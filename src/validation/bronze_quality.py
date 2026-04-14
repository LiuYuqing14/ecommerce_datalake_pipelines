#!/usr/bin/env python3
"""Bronze Layer Quality Validation.

Validates Bronze inputs by:
1. Scanning manifest files per partition
2. Aggregating row counts per table
3. Reporting missing manifests / empty partitions

Outputs:
- JSON metrics: data/metrics/data_quality/bronze_quality_{run_id}.json
- Markdown report: docs/validation_reports/BRONZE_QUALITY.md
"""

from __future__ import annotations

import argparse
import json
import os
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.observability import get_logger
from src.observability.metrics import write_data_quality_metric
from src.settings import load_settings
from src.specs import load_spec_safe
from src.validation.common import (
    ValidationStatus,
    handle_exit,
    is_gcs_path,
    resolve_layer_paths,
    resolve_reports_enabled,
)

logger = get_logger(__name__)


def get_bronze_partitions() -> dict[str, str | None]:
    spec = load_spec_safe()
    if spec:
        return {table.name: table.partition_key for table in spec.bronze.tables}
    return load_settings().pipeline.table_partitions


def get_partition_glob(table: str) -> str:
    """Get the partition glob for a table from centralized config."""
    key = get_bronze_partitions().get(table, "ingest_dt")
    return f"{key}=*"


@dataclass
class TableMetrics:
    table: str
    partitions: int
    manifests: int
    rows: int
    missing_manifests: int
    empty_partitions: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Bronze layer inputs.")
    parser.add_argument(
        "--bronze-path",
        default="samples/bronze",
        help="Path to Bronze data (local path or gs:// bucket).",
    )
    parser.add_argument(
        "--tables",
        default=None,
        help="Comma-separated list of tables to validate (default: all).",
    )
    parser.add_argument(
        "--partition-date",
        default=None,
        help="Partition date (YYYY-MM-DD) to validate (optional).",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=0,
        help="Number of days before partition-date to include (default: 0).",
    )
    parser.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Legacy: use --enforce-quality instead.",
    )
    parser.add_argument(
        "--enforce-quality",
        action="store_true",
        help="Exit with non-zero code on any failures (standard gate).",
    )
    parser.add_argument(
        "--run-id",
        help="Run ID for this validation (auto-generated if not provided).",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.yml (optional)",
    )
    parser.add_argument(
        "--output-report",
        default="docs/validation_reports/BRONZE_QUALITY.md",
        help="Path to write Markdown report.",
    )
    parser.add_argument(
        "--spec-path",
        default=None,
        help="Path to spec directory or file (overrides ECOM_SPEC_PATH).",
    )
    return parser.parse_args()


def bronze_qa_required() -> bool:
    env_override = os.getenv("BRONZE_QA_REQUIRED")
    if env_override is not None:
        return env_override.lower() in {"true", "1", "yes"}
    pipeline_env = os.getenv("PIPELINE_ENV", "local").lower()
    return pipeline_env in {"dev", "prod"}


def bronze_qa_fail() -> bool:
    env_override = os.getenv("BRONZE_QA_FAIL")
    if env_override is not None:
        return env_override.lower() in {"true", "1", "yes"}
    pipeline_env = os.getenv("PIPELINE_ENV", "local").lower()
    return pipeline_env in {"prod"}


def list_tables(root: str) -> list[str]:
    if is_gcs_path(root):
        try:
            import fsspec
        except ModuleNotFoundError as exc:
            if not bronze_qa_required():
                logger.warning("gcsfs not installed; skipping bronze QA for GCS path")
                return []
            raise RuntimeError("gcsfs is required for GCS bronze QA") from exc

        fs = fsspec.filesystem("gcs")
        entries = fs.ls(root, detail=True)
        return sorted(
            [
                e["name"].rstrip("/").split("/")[-1]
                for e in entries
                if e["type"] == "directory"
            ]
        )

    return sorted([p.name for p in Path(root).iterdir() if p.is_dir()])


def list_partitions(
    root: str, table: str, partition_values: list[str] | None
) -> list[str]:
    partition_glob = get_partition_glob(table)
    if is_gcs_path(root):
        import fsspec

        key = get_bronze_partitions().get(table, "ingest_dt")
        fs = fsspec.filesystem("gcs")
        if partition_values:
            found_prefixes = []
            for value in partition_values:
                partition_path_str = f"{root}/{table}/{key}={value}"
                if fs.exists(partition_path_str):
                    found_prefixes.append(partition_path_str)
            return sorted(found_prefixes)

        # Optimization: Just list the partition directories, not every file inside them.
        # This prevents hangs on large GCS buckets.
        matches = fs.glob(f"{root}/{table}/{partition_glob}")
        prefixes: set[str] = set()
        for match in matches:
            path = match
            if path.startswith("gs://"):
                path = path[len("gs://") :]
            parts = path.split("/")
            for idx, part in enumerate(parts):
                if part.startswith(f"{key}="):
                    prefix = "/".join(parts[: idx + 1])
                    if not prefix.startswith("gs://"):
                        prefix = f"gs://{prefix}"
                    prefixes.add(prefix)
                    break
        return sorted(prefixes)

    if partition_values:
        paths = []
        key = get_bronze_partitions().get(table, "ingest_dt")
        for value in partition_values:
            local_partition_path = Path(root, table, f"{key}={value}")
            if local_partition_path.is_dir():
                paths.append(str(local_partition_path))
        return sorted(paths)

    return sorted(str(p) for p in Path(root, table).glob(partition_glob) if p.is_dir())


def read_manifest(path: str) -> dict[str, Any] | None:
    manifest_name = "_MANIFEST.json"
    if is_gcs_path(path):
        import fsspec

        fs = fsspec.filesystem("gcs")
        manifest_path = f"{path}/{manifest_name}"
        if not fs.exists(manifest_path):
            return None
        with fs.open(manifest_path) as handle:
            return json.load(handle)

    manifest_file = Path(path) / manifest_name
    if not manifest_file.exists():
        return None
    try:
        return json.loads(manifest_file.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(f"Failed to read manifest {manifest_file}: {exc}")
        return None


def validate_table(
    root: str, table: str, partition_values: list[str] | None
) -> TableMetrics:
    partitions = list_partitions(root, table, partition_values)
    manifests = 0
    rows = 0
    missing_manifests = 0
    empty_partitions = 0

    for partition_path in partitions:
        manifest = read_manifest(partition_path)
        if manifest is None:
            missing_manifests += 1
            continue
        manifests += 1
        rows += int(manifest.get("total_rows", 0))
        if int(manifest.get("total_rows", 0)) == 0:
            empty_partitions += 1

    return TableMetrics(
        table=table,
        partitions=len(partitions),
        manifests=manifests,
        rows=rows,
        missing_manifests=missing_manifests,
        empty_partitions=empty_partitions,
    )


def generate_report(metrics: list[TableMetrics], output_path: str, run_id: str) -> None:
    """Generate validation report and write to local or GCS path.

    Args:
        metrics: List of table metrics to report
        output_path: Report path (local or gs://)
        run_id: Run identifier
    """
    from src.observability.config import get_config

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines = [
        "# Bronze Quality Report",
        "",
        f"**Last Updated:** {timestamp}",
        f"**Run ID:** `{run_id}`",
        "",
        "## Summary",
        "",
        (
            "| Table | Partitions | Manifests | Rows | Missing Manifests | "
            "Empty Partitions |"
        ),
        "| --- | --- | --- | --- | --- | --- |",
    ]

    for m in metrics:
        lines.append(
            f"| {m.table} | {m.partitions} | {m.manifests} | {m.rows:,} | "
            f"{m.missing_manifests} | {m.empty_partitions} |"
        )

    total_missing = sum(m.missing_manifests for m in metrics)
    total_empty = sum(m.empty_partitions for m in metrics)
    lines.extend(
        [
            "",
            "## Checks",
            "",
            f"- **Missing manifests:** {total_missing}",
            f"- **Empty partitions:** {total_empty}",
            "",
            "---",
            "",
            "## Metadata",
            "",
            "- **Generated by:** `src/validation/bronze_quality.py`",
            "- **Report Format Version:** 1.0",
            "",
            "<!-- AUTO-GENERATED - DO NOT EDIT MANUALLY -->",
        ]
    )

    report_content = "\n".join(lines)

    # Write to local or GCS based on environment
    obs_config = get_config()
    if not obs_config.use_cloud_reports():
        # Local: write to filesystem
        local_path = Path(output_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_text(report_content)
        logger.info(f"Wrote Markdown report to: {local_path}")
    else:
        # Dev/Prod: write to GCS
        import fsspec

        fs = fsspec.filesystem("gcs")
        with fs.open(output_path, "w") as f:
            f.write(report_content)
        logger.info(f"Wrote Markdown report to GCS: {output_path}")


def main() -> int:
    args = parse_args()
    if args.spec_path:
        os.environ["ECOM_SPEC_PATH"] = args.spec_path

    from src.observability.config import get_config
    from src.settings import load_settings

    settings = load_settings(args.config)
    pipeline_env = os.getenv("PIPELINE_ENV", settings.pipeline.environment).lower()

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    partition_values: list[str] | None = None
    if args.partition_date:
        try:
            from datetime import date, timedelta

            base_date = date.fromisoformat(args.partition_date)
            lookback_days = max(args.lookback_days, 0)
            partition_values = [
                (base_date - timedelta(days=offset)).isoformat()
                for offset in range(lookback_days + 1)
            ]
        except ValueError:
            logger.error("Invalid partition-date; expected YYYY-MM-DD")
            return 1

    # Resolve paths using shared library
    paths = resolve_layer_paths(
        args.config,
        bronze_over=args.bronze_path,
        spec_path=args.spec_path,
    )
    bronze_root = str(paths["bronze"])

    logger.info(f"Starting Bronze quality validation (run_id={run_id})")

    expected_tables = list(get_bronze_partitions().keys())
    available_tables: list[str] | None = None

    if not is_gcs_path(bronze_root):
        available_tables = list_tables(bronze_root)
    else:
        logger.info("Skipping GCS table listing; using configured table list")

    if args.tables:
        requested = [t.strip() for t in args.tables.split(",") if t.strip()]
        if available_tables is None:
            tables = requested
        else:
            tables = [t for t in requested if t in available_tables]
    else:
        if available_tables is None:
            tables = expected_tables
        else:
            tables = [t for t in expected_tables if t in available_tables]

    metrics = [validate_table(bronze_root, table, partition_values) for table in tables]

    # Overall status: FAIL if any critical issues found (missing manifests/empty)
    total_missing = sum(m.missing_manifests for m in metrics)
    total_empty = sum(m.empty_partitions for m in metrics)

    if total_missing > 0 or total_empty > 0:
        overall_status = ValidationStatus.FAIL
    else:
        overall_status = ValidationStatus.PASS

    metric_payloads = [
        {
            "table": m.table,
            "row_count": {
                "actual": m.rows,
                "status": "PASS" if m.rows > 0 else "FAIL",
            },
            "manifest_check": {
                "missing": m.missing_manifests,
                "status": "PASS" if m.missing_manifests == 0 else "FAIL",
            },
            "empty_partitions": {
                "count": m.empty_partitions,
                "status": "PASS" if m.empty_partitions == 0 else "FAIL",
            },
        }
        for m in metrics
    ]

    write_data_quality_metric(
        run_id=run_id,
        validation_type="bronze_quality",
        table_metrics=metric_payloads,
        overall_status=overall_status,
    )

    # Determine report path (use observability config for GCS in dev/prod)
    obs_config = get_config()
    if not obs_config.use_cloud_reports():
        report_path = args.output_report
    else:
        # GCS: Use observability config path with run folder
        report_filename = Path(args.output_report).name
        report_path = obs_config.get_run_report_path(run_id, report_filename)

    if resolve_reports_enabled(args.spec_path):
        generate_report(metrics, report_path, run_id)
    else:
        report_path = "(reports disabled)"
    logger.info(f"✅ Bronze quality validation completed: {overall_status}")

    return handle_exit(
        overall_status=overall_status,
        enforce=args.enforce_quality or args.fail_on_issues,
        env=pipeline_env,
    )


if __name__ == "__main__":
    if not os.getenv("ECOM_CLI_SUPPRESS_DEPRECATION"):
        warnings.warn(
            (
                "Deprecated: use `ecomlake bronze validate` instead of "
                "python -m src.validation.bronze_quality"
            ),
            UserWarning,
            stacklevel=2,
        )
    raise SystemExit(main())
