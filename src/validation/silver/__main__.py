#!/usr/bin/env python3
"""Silver Layer Quality Validation (Refactored)."""

from __future__ import annotations

import argparse
import os
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.observability import get_logger
from src.observability.metrics import write_silver_quality_metric
from src.runners.enriched.shared import (
    get_silver_table_partitions,
    get_table_partitions,
)
from src.settings import load_settings
from src.specs import load_spec_safe
from src.validation.base_silver_schemas import REQUIRED_BASE_SILVER_COLUMNS
from src.validation.common import (
    get_overall_status,
    handle_exit,
    is_gcs_path,
    join_path,
    resolve_layer_paths,
    resolve_reports_enabled,
)
from src.validation.silver.data import (
    check_required_columns,
    compute_key_cardinality,
    list_partitions_by_key,
)
from src.validation.silver.metrics import compute_fk_mismatch_summary, validate_table
from src.validation.silver.models import SilverQualityReport
from src.validation.silver.report import build_profile_report, generate_markdown_report

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Validate Silver layer transformation quality."
    )
    parser.add_argument(
        "--config", default="config/config.yml", help="Path to pipeline config YAML."
    )
    parser.add_argument(
        "--bronze-path",
        default=None,
        help="Path to Bronze layer data (overrides env/config).",
    )
    parser.add_argument(
        "--silver-path",
        default=None,
        help="Path to Silver layer data (overrides env/config).",
    )
    parser.add_argument(
        "--quarantine-path",
        default=None,
        help="Path to quarantine data (overrides env/config).",
    )
    parser.add_argument(
        "--run-id", help="Run ID for this validation (auto-generated if not provided)."
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
        "--fail-on-sla-breach",
        action="store_true",
        help="Legacy: use --enforce-quality instead.",
    )
    parser.add_argument(
        "--enforce-quality",
        action="store_true",
        help="Exit with non-zero code on any failures (standard gate).",
    )
    parser.add_argument(
        "--output-report",
        default="docs/validation_reports/SILVER_QUALITY.md",
        help="Path to write Markdown report.",
    )
    parser.add_argument(
        "--spec-path",
        default=None,
        help="Path to spec directory or file (overrides ECOM_SPEC_PATH).",
    )
    return parser.parse_args()


def expand_partition_ranges(values: list[str]) -> list[str]:
    """Expand YYYY-MM-DD or YYYY-MM-DD..YYYY-MM-DD range strings into dates."""
    from datetime import date

    expanded: list[str] = []
    for value in values:
        item = value.strip()
        if not item:
            continue
        if ".." not in item:
            expanded.append(item)
            continue
        start_str, end_str = item.split("..", 1)
        try:
            start = date.fromisoformat(start_str.strip())
            end = date.fromisoformat(end_str.strip())
        except ValueError:
            logger.warning("Invalid partition range ignored", range=item)
            continue
        if end < start:
            logger.warning("Partition range end before start", range=item)
            continue
        current = start
        while current <= end:
            expanded.append(current.isoformat())
            current = current.fromordinal(current.toordinal() + 1)
    return expanded


def build_partition_values(partition_date: str, lookback_days: int) -> list[str]:
    from datetime import date, timedelta

    base_date = date.fromisoformat(partition_date)
    days = max(lookback_days, 0)
    return [
        (base_date - timedelta(days=offset)).isoformat() for offset in range(days + 1)
    ]


def main() -> int:
    """Run Silver quality validation."""
    args = parse_args()
    if args.spec_path:
        os.environ["ECOM_SPEC_PATH"] = args.spec_path
    settings = load_settings(args.config)
    sla_thresholds = settings.pipeline.sla_thresholds or {}
    pipeline_env = os.getenv("PIPELINE_ENV", settings.pipeline.environment).lower()

    paths = resolve_layer_paths(
        args.config,
        bronze_over=args.bronze_path,
        silver_over=args.silver_path,
        spec_path=args.spec_path,
    )
    bronze_path = paths["bronze"]
    silver_path = paths["silver"]
    quarantine_path = paths["quarantine"]

    if not any(
        is_gcs_path(str(path)) for path in (bronze_path, silver_path, quarantine_path)
    ):
        bronze_path = Path(bronze_path)
        silver_path = Path(silver_path)
        quarantine_path = Path(quarantine_path)

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"Starting Silver quality validation (run_id={run_id})")
    partition_values: list[str] | None = None
    if args.partition_date:
        try:
            partition_values = build_partition_values(
                args.partition_date, args.lookback_days
            )
        except ValueError:
            logger.error("Invalid partition-date; expected YYYY-MM-DD")
            return 1

    spec = load_spec_safe()
    allow_empty_map: dict[str, bool] = {}
    if spec:
        for table_spec in spec.silver_base.tables:
            if table_spec.quality and table_spec.quality.sla is not None:
                sla_thresholds[table_spec.name] = table_spec.quality.sla
            if table_spec.quality and table_spec.quality.allow_empty:
                allow_empty_map[table_spec.name] = True
        available_tables = {table.name for table in spec.silver_base.tables}
    else:
        available_tables = set(sla_thresholds.keys())

    if args.tables:
        requested_tables = [t.strip() for t in args.tables.split(",") if t.strip()]
        tables = [t for t in requested_tables if t in available_tables]
    else:
        tables = sorted(available_tables)

    table_metrics = []
    for table in tables:
        silver_partition_key = get_silver_table_partitions().get(table, "ingest_dt")
        bronze_partition_key = get_table_partitions().get(table, "ingest_dt")

        # Only apply date partition filter to Bronze if it uses standard
        # ingestion dates. For dimensions (signup_date, category),
        # we want to compare the snapshot against the total bronze volume.
        bronze_partitions = partition_values
        if bronze_partition_key not in {"ingest_dt"}:
            bronze_partitions = None

        metrics = validate_table(
            table,
            bronze_path,
            silver_path,
            quarantine_path,
            sla_thresholds,
            allow_empty=allow_empty_map.get(table, False),
            partition_key=silver_partition_key,
            partitions=partition_values,
            bronze_partition_key=bronze_partition_key,
            bronze_partitions=bronze_partitions,
        )
        table_metrics.append(metrics)
    metrics_by_table = {m.table: m for m in table_metrics}

    overall_status = get_overall_status([m.status for m in table_metrics])

    total_processed = sum(m.silver_rows + m.quarantine_rows for m in table_metrics)
    total_quarantined = sum(m.quarantine_rows for m in table_metrics)
    total_bronze_rows = sum(m.bronze_rows for m in table_metrics)
    total_row_loss = sum(m.row_loss for m in table_metrics)
    total_quarantine_pct = (
        (total_quarantined / total_processed * 100) if total_processed > 0 else 0.0
    )
    total_row_loss_pct = (
        (total_row_loss / total_bronze_rows * 100) if total_bronze_rows > 0 else 0.0
    )

    contract_issues: list[dict[str, Any]] = []
    if total_quarantine_pct > settings.pipeline.max_quarantine_pct:
        contract_issues.append(
            {
                "check": "max_quarantine_pct",
                "message": (
                    f"Quarantine rate {total_quarantine_pct:.2f}% exceeds "
                    f"threshold {settings.pipeline.max_quarantine_pct:.2f}%"
                ),
            }
        )
    if total_row_loss_pct > settings.pipeline.max_row_loss_pct:
        contract_issues.append(
            {
                "check": "max_row_loss_pct",
                "message": (
                    f"Row loss {total_row_loss_pct:.2f}% exceeds "
                    f"threshold {settings.pipeline.max_row_loss_pct:.2f}%"
                ),
            }
        )

    expected_source = "none"
    if partition_values:
        expected_set = set(partition_values)
        expected_source = "partition_date"
    elif settings.pipeline.expected_bronze_partitions:
        expected_set = set(
            expand_partition_ranges(settings.pipeline.expected_bronze_partitions)
        )
        expected_source = "config"
    else:
        expected_set = set()
    if expected_set:
        date_partition_keys = {"ingest_dt", "signup_date"}
        for table in tables:
            bronze_partition_key = get_table_partitions().get(table, "ingest_dt")
            if bronze_partition_key not in date_partition_keys:
                continue
            if expected_source == "config" and bronze_partition_key != "ingest_dt":
                continue
            bronze_parts = list_partitions_by_key(
                join_path(bronze_path, table), bronze_partition_key
            )
            missing = sorted(expected_set - bronze_parts)
            if missing:
                sample = ", ".join(missing[:5])
                contract_issues.append(
                    {
                        "check": "missing_bronze_partitions",
                        "table": table,
                        "message": (
                            f"Missing {len(missing)} {bronze_partition_key} partitions "
                            f"for {table}: {sample}"
                        ),
                    }
                )

    if settings.pipeline.min_table_rows:
        for metrics in table_metrics:
            min_rows = settings.pipeline.min_table_rows.get(metrics.table)
            if min_rows is None:
                continue
            processed_count = metrics.silver_rows + metrics.quarantine_rows
            if processed_count < min_rows:
                contract_issues.append(
                    {
                        "check": "min_table_rows",
                        "table": metrics.table,
                        "message": (
                            f"Processed {processed_count:,} rows for {metrics.table}, "
                            f"below minimum {min_rows:,}"
                        ),
                    }
                )

        # Note: Cardinality checks logic could also be moved to metrics.py,
        # but for brevity keeping it here or in metrics.
        # The original monolith had compute_key_cardinality inside itself.
        # I moved compute_key_cardinality to data.py, so we can use it here.

    sample_rows_env = os.getenv("SILVER_LINEAGE_SAMPLE_ROWS", "0").strip()
    sample_rows_val = int(sample_rows_env) if sample_rows_env.isdigit() else 0
    sample_rows: int | None = None if sample_rows_val <= 0 else sample_rows_val
    for table in tables:
        metrics_item = metrics_by_table.get(table)
        if allow_empty_map.get(table) and metrics_item:
            processed = metrics_item.silver_rows + metrics_item.quarantine_rows
            if processed == 0:
                continue
        required_cols = REQUIRED_BASE_SILVER_COLUMNS.get(table, [])
        if not required_cols:
            continue
        partition_key = get_silver_table_partitions().get(table, "ingest_dt")
        checks = check_required_columns(
            join_path(silver_path, table),
            required_cols,
            partition_key=partition_key,
            partitions=partition_values,
            sample_rows=sample_rows,
        )
        missing_item = checks.get("missing", [])
        nulls = checks.get("nulls", {})
        if missing_item and isinstance(missing_item, list):
            contract_issues.append(
                {
                    "check": "missing_required_columns",
                    "table": table,
                    "message": f"Missing columns: {', '.join(missing_item)}",
                }
            )
        if nulls and isinstance(nulls, dict):
            detail = ", ".join(f"{col}={count}" for col, count in nulls.items())
            contract_issues.append(
                {
                    "check": "null_required_columns",
                    "table": table,
                    "message": f"Null required values detected: {detail}",
                }
            )

    if "returns" in tables:
        returns_cardinality = compute_key_cardinality(
            join_path(silver_path, "returns"),
            "return_id",
            partition_key=get_silver_table_partitions().get("returns", "ingest_dt"),
            partitions=partition_values,
        )
        if (
            returns_cardinality["non_null_rows"] > 0
            and returns_cardinality["distinct_ratio"]
            < settings.pipeline.min_return_id_distinct_ratio
        ):
            contract_issues.append(
                {
                    "check": "returns_return_id_distinct_ratio",
                    "message": (
                        f"Distinct ratio {returns_cardinality['distinct_ratio']:.6f} "
                        f"< min {settings.pipeline.min_return_id_distinct_ratio:.6f}"
                    ),
                }
            )

    if "return_items" in tables:
        return_items_cardinality = compute_key_cardinality(
            join_path(silver_path, "return_items"),
            "return_id",
            partition_key=get_silver_table_partitions().get(
                "return_items", "ingest_dt"
            ),
            partitions=partition_values,
        )
        if (
            return_items_cardinality["non_null_rows"] > 0
            and return_items_cardinality["distinct_ratio"]
            < settings.pipeline.min_return_id_distinct_ratio
        ):
            contract_issues.append(
                {
                    "check": "return_items_return_id_distinct_ratio",
                    "message": (
                        f"Distinct ratio "
                        f"{return_items_cardinality['distinct_ratio']:.6f}"
                        f" < min {settings.pipeline.min_return_id_distinct_ratio:.6f}"
                    ),
                }
            )

    if contract_issues:
        overall_status = "FAIL" if pipeline_env == "prod" else "WARN"

    tables_passing = sum(1 for m in table_metrics if m.status == "PASS")
    tables_warning = sum(1 for m in table_metrics if m.status == "WARN")
    tables_failing = sum(1 for m in table_metrics if m.status == "FAIL")

    report = SilverQualityReport(
        run_id=run_id,
        timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        table_metrics=table_metrics,
        fk_mismatch_summary=(
            compute_fk_mismatch_summary(silver_path)
            if not args.tables and not partition_values
            else []
        ),
        contract_issues=contract_issues,
        overall_status=overall_status,
        tables_passing=tables_passing,
        tables_warning=tables_warning,
        tables_failing=tables_failing,
        total_quarantined=total_quarantined,
        total_processed=total_processed,
    )

    metrics_dicts = [
        {
            "table": m.table,
            "row_counts": {
                "bronze_input": m.bronze_rows,
                "silver_output": m.silver_rows,
                "quarantine_output": m.quarantine_rows,
                "total_processed": m.silver_rows + m.quarantine_rows,
                "row_loss": m.row_loss,
                "row_loss_pct": m.row_loss_pct,
            },
            "pass_rate": {
                "rate": m.pass_rate,
                "sla_threshold": m.sla_threshold,
                "status": m.status,
            },
            "quarantine_breakdown": m.quarantine_breakdown,
        }
        for m in table_metrics
    ]

    write_silver_quality_metric(
        run_id=run_id,
        table_metrics=metrics_dicts,
        overall_status=overall_status,
        contract_issues=contract_issues,
    )

    # Determine report path (use observability config for GCS in dev/prod)
    from src.observability.config import get_config

    if resolve_reports_enabled(args.spec_path):
        obs_config = get_config()
        if not obs_config.use_cloud_reports():
            report_path = args.output_report
        else:
            # GCS: Use observability config path with run folder
            report_filename = Path(args.output_report).name
            report_path = obs_config.get_run_report_path(run_id, report_filename)

        generate_markdown_report(report, report_path)
    else:
        report_path = "(reports disabled)"

    profile_enabled = os.getenv("SILVER_PROFILE_ENABLED", "false").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if profile_enabled:
        profile_path = Path(
            os.getenv(
                "SILVER_PROFILE_REPORT", "docs/validation_reports/SILVER_PROFILE.md"
            )
        )
        build_profile_report(tables, Path(silver_path), profile_path)

    logger.info("=" * 70)
    logger.info(f"Silver Quality Validation Summary (run_id={run_id})")
    logger.info("=" * 70)
    logger.info(f"Overall Status: {overall_status}")
    logger.info(f"Tables Passing: {tables_passing}/{len(tables)}")
    if tables_warning > 0:
        logger.warning(f"Tables Warning: {tables_warning}")
    if tables_failing > 0:
        logger.error(f"Tables Failing: {tables_failing}")
    logger.info(f"Detailed report: {report_path}")
    logger.info("=" * 70)

    return handle_exit(
        overall_status=overall_status,
        enforce=args.enforce_quality or args.fail_on_sla_breach,
        env=pipeline_env,
    )


if __name__ == "__main__":
    if not os.getenv("ECOM_CLI_SUPPRESS_DEPRECATION"):
        warnings.warn(
            (
                "Deprecated: use `ecomlake silver validate` instead of "
                "python -m src.validation.silver"
            ),
            UserWarning,
            stacklevel=2,
        )
    raise SystemExit(main())
