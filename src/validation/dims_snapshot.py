"""Validate dims snapshot partitions."""

from __future__ import annotations

import argparse
import os
import warnings
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from src.observability import get_logger
from src.specs import load_spec_safe
from src.validation.base_silver_schemas import REQUIRED_BASE_SILVER_COLUMNS
from src.validation.common import (
    handle_exit,
    join_path,
    read_parquet_safe,
    resolve_reports_enabled,
)

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate dims snapshots.")
    parser.add_argument(
        "--dims-path",
        default=None,
        help="Path to dims snapshot root (overrides env/spec).",
    )
    parser.add_argument(
        "--run-date",
        required=True,
        help="Snapshot date (YYYY-MM-DD) to validate.",
    )
    parser.add_argument(
        "--run-id",
        help="Run ID for this validation (auto-generated if not provided).",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="Comma-separated list of tables to validate.",
    )
    parser.add_argument(
        "--output-report",
        default="docs/validation_reports/DIMS_SNAPSHOT.md",
        help="Path to write Markdown report.",
    )
    parser.add_argument(
        "--enforce-quality",
        action="store_true",
        help="Exit non-zero when any table fails.",
    )
    parser.add_argument(
        "--spec-path",
        default=None,
        help="Path to spec directory or file (overrides ECOM_SPEC_PATH).",
    )
    return parser.parse_args()


def resolve_dims_path(args: argparse.Namespace) -> str:
    if args.dims_path:
        return args.dims_path
    spec = load_spec_safe()
    if spec and spec.dims.base_path:
        path = os.path.expandvars(spec.dims.base_path)
    else:
        path = os.getenv("SILVER_DIMS_PATH", "data/silver/dims")
    return path


def list_tables(args: argparse.Namespace) -> list[tuple[str, str]]:
    if args.tables:
        return [(t.strip(), "snapshot_dt") for t in args.tables.split(",") if t.strip()]
    spec = load_spec_safe()
    if spec:
        return [(t.name, t.partition_key) for t in spec.dims.tables]
    return [("customers", "snapshot_dt"), ("product_catalog", "snapshot_dt")]


def validate_snapshot(table: str, path: str, partition_key: str, run_date: str) -> dict:
    """Validate a single dimension snapshot for existence, schema, and null PKs."""
    snapshot_path = join_path(path, f"{partition_key}={run_date}")

    # 1. Existence & Readability Check (using robust reader)
    df = read_parquet_safe(snapshot_path)

    if df is None:
        return {
            "status": "FAIL",
            "rows": 0,
            "message": "Partition not found or unreadable",
        }

    row_count = df.height
    if row_count == 0:
        return {"status": "FAIL", "rows": 0, "message": "Empty snapshot (0 rows)"}

    # 2. Schema Check (Required Columns)
    required_cols = REQUIRED_BASE_SILVER_COLUMNS.get(table, [])
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        return {
            "status": "FAIL",
            "rows": row_count,
            "message": f"Missing columns: {', '.join(missing_cols)}",
        }

    # 3. Primary Key Null Check
    if required_cols:
        pk_col = required_cols[0]
        null_pk_count = df.select(pl.col(pk_col).is_null().sum()).item()

        if null_pk_count > 0:
            return {
                "status": "FAIL",
                "rows": row_count,
                "message": f"Found {null_pk_count} nulls in PK '{pk_col}'",
            }

    return {"status": "PASS", "rows": row_count, "message": "OK"}


def main() -> int:
    args = parse_args()
    if args.spec_path:
        os.environ["ECOM_SPEC_PATH"] = args.spec_path

    root_path = resolve_dims_path(args)
    tables = list_tables(args)

    # Use provided run-id or generate one
    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    results = []
    overall_status = "PASS"

    for table, partition_key in tables:
        table_path = join_path(root_path, table)
        res = validate_snapshot(table, str(table_path), partition_key, args.run_date)
        res["table"] = table
        res["path"] = str(table_path)
        results.append(res)
        if res["status"] == "FAIL":
            overall_status = "FAIL"

    # ... (inside main function)

    report_lines = [
        "# Dims Snapshot Quality Report",
        "",
        f"**Run ID:** `{run_id}`",
        f"**Snapshot Date:** `{args.run_date}`",
        f"**Overall Status:** {'✅' if overall_status == 'PASS' else '❌'} "
        f"{overall_status}",
        "",
        "## Summary",
        "",
        "| Table | Status | Rows | Message | Path |",
        "|-------|--------|-----:|---------|------|",
    ]

    for r in results:
        status_emoji = "✅" if r["status"] == "PASS" else "❌"
        # Escape pipe characters in message/path if necessary
        path_display = f"`{r['path']}`"
        report_lines.append(
            f"| {r['table']} | {status_emoji} {r['status']} | "
            f"{r['rows']:,} | {r['message']} | {path_display} |"
        )

    report_lines.extend(
        [
            "",
            "---",
            "",
            "## Metadata",
            "",
            "- **Generated by:** `src/validation/dims_snapshot.py`",
            "- **Validation Framework:** 1.1.0",
            "",
            "<!-- AUTO-GENERATED - DO NOT EDIT MANUALLY -->",
        ]
    )

    report_content = "\n".join(report_lines)

    if resolve_reports_enabled(args.spec_path):
        from src.observability.config import get_config

        obs_config = get_config()

        if not obs_config.use_cloud_reports():
            # Local: write to filesystem
            local_path = Path(args.output_report)
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_text(report_content, encoding="utf-8")
            logger.info(f"Wrote Markdown report to: {local_path}")
        else:
            # Cloud: write to GCS using observability config
            import fsspec

            # Determine GCS path
            report_filename = Path(args.output_report).name
            gcs_path = obs_config.get_run_report_path(run_id, report_filename)

            # Ensure gs:// prefix for fsspec if missing
            if not str(gcs_path).startswith("gs://"):
                gcs_path = f"gs://{gcs_path}"

            try:
                fs = fsspec.filesystem("gcs")
                # fsspec gcs filesystem expects path without gs:// prefix
                # Safest is to strip if we used filesystem("gcs")
                write_path = str(gcs_path).replace("gs://", "")

                with fs.open(write_path, "w") as f:
                    f.write(report_content)
                logger.info(f"Wrote Markdown report to GCS: {gcs_path}")
            except Exception as e:
                logger.error(f"Failed to write report to GCS: {e}")
                # Fallback to local if GCS fails? Or just log.

    logger.info(
        f"Dims snapshot validation {overall_status}",
        tables=[r["table"] for r in results],
    )

    return handle_exit(
        overall_status, args.enforce_quality, os.getenv("PIPELINE_ENV", "dev")
    )


if __name__ == "__main__":
    if not os.getenv("ECOM_CLI_SUPPRESS_DEPRECATION"):
        warnings.warn(
            (
                "Deprecated: use `ecomlake dim validate` instead of "
                "python -m src.validation.dims_snapshot"
            ),
            UserWarning,
            stacklevel=2,
        )
    raise SystemExit(main())
