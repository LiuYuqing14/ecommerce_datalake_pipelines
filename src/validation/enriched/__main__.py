#!/usr/bin/env python3
"""Enriched Silver Layer Quality Validation (Refactored)."""

from __future__ import annotations

import argparse
import os
import warnings
from datetime import datetime, timezone
from pathlib import Path

from src.observability import get_logger
from src.observability.metrics import write_enriched_quality_metric
from src.settings import load_settings
from src.specs import load_spec_safe
from src.validation.common import (
    handle_exit,
    is_gcs_path,
    resolve_layer_paths,
    resolve_reports_enabled,
)
from src.validation.enriched.metrics import validate_table
from src.validation.enriched.report import generate_markdown_report

logger = get_logger(__name__)

DEFAULT_ENRICHED_TABLES = [
    "int_attributed_purchases",
    "int_cart_attribution",
    "int_inventory_risk",
    "int_customer_retention_signals",
    "int_customer_lifetime_value",
    "int_daily_business_metrics",
    "int_product_performance",
    "int_sales_velocity",
    "int_regional_financials",
    "int_shipping_economics",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Enriched Silver outputs.")
    parser.add_argument(
        "--config", default="config/config.yml", help="Path to pipeline config YAML."
    )
    parser.add_argument(
        "--enriched-path",
        default=None,
        help="Path to Enriched Silver data (overrides env/config).",
    )
    parser.add_argument(
        "--run-id", help="Run ID for this validation (auto-generated if not provided)."
    )
    parser.add_argument(
        "--ingest-dt", default=None, help="Target partition date (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Days of partitions to include when validating business-date tables.",
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
        "--output-report",
        default="docs/validation_reports/ENRICHED_QUALITY.md",
        help="Path to write Markdown report.",
    )
    parser.add_argument(
        "--spec-path",
        default=None,
        help="Path to spec directory or file (overrides ECOM_SPEC_PATH).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.spec_path:
        os.environ["ECOM_SPEC_PATH"] = args.spec_path
    settings = load_settings(args.config)
    pipeline_env = os.getenv("PIPELINE_ENV", settings.pipeline.environment).lower()

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    paths = resolve_layer_paths(
        config_path=args.config,
        enriched_over=args.enriched_path,
        spec_path=args.spec_path,
    )

    enriched_path = paths["enriched"]
    if not is_gcs_path(str(enriched_path)):
        enriched_path = Path(enriched_path)

    spec = load_spec_safe()
    if spec:
        tables = [table.name for table in spec.silver_enriched.tables]
        min_rows_map = {
            table.name: table.min_rows
            for table in spec.silver_enriched.tables
            if table.min_rows is not None
        }
        partition_map = {
            table.name: table.partition_key for table in spec.silver_enriched.tables
        }
    else:
        tables = settings.pipeline.enriched_tables or DEFAULT_ENRICHED_TABLES
        min_rows_map = settings.pipeline.enriched_min_table_rows or {}
        partition_map = settings.pipeline.enriched_partitions

    logger.info(f"Starting Enriched Silver validation (run_id={run_id})")

    if args.lookback_days is not None:
        lookback_days = args.lookback_days
    elif spec:
        lookback_days = spec.silver_enriched.lookback_days
    else:
        lookback_days = settings.pipeline.enriched_lookback_days

    table_metrics = [
        validate_table(
            table=table,
            enriched_path=enriched_path,
            ingest_dt=args.ingest_dt,
            min_rows=min_rows_map.get(table),
            partition_key=partition_map.get(
                table, settings.pipeline.enriched_partitions.get(table, "ingest_dt")
            ),
            pipeline_env=pipeline_env,
            settings=settings.pipeline,
            lookback_days=lookback_days,
        )
        for table in tables
    ]

    overall_status = "PASS"
    if any(m.status == "FAIL" for m in table_metrics):
        overall_status = "FAIL"
    elif any(m.status == "WARN" for m in table_metrics):
        overall_status = "WARN"

    metrics_payloads = [
        {
            "table": m.table,
            "partition_key": m.partition_key,
            "ingest_dt": m.ingest_dt,
            "row_count": m.row_count,
            "min_rows": m.min_rows,
            "prior_row_count": m.prior_row_count,
            "row_delta_pct": m.row_delta_pct,
            "schema_snapshot": m.schema_snapshot,
            "null_rates": m.null_rates,
            "sanity_issues": m.sanity_issues,
            "semantic_issues": m.semantic_issues,
            "status": m.status,
            "notes": m.notes,
        }
        for m in table_metrics
    ]

    write_enriched_quality_metric(
        run_id=run_id,
        table_metrics=metrics_payloads,
        overall_status=overall_status,
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

        generate_markdown_report(
            run_id=run_id,
            timestamp=timestamp,
            table_metrics=table_metrics,
            overall_status=overall_status,
            output_path=report_path,
        )
    else:
        report_path = "(reports disabled)"

    failing = [m for m in table_metrics if m.status == "FAIL"]
    if failing:
        details = []
        for metric in failing:
            issues = []
            if metric.semantic_issues:
                issues.append(f"semantic={'; '.join(metric.semantic_issues)}")
            if metric.sanity_issues:
                issues.append(f"sanity={'; '.join(metric.sanity_issues)}")
            if metric.notes:
                issues.append(f"notes={'; '.join(metric.notes)}")
            if not issues:
                issues.append("no_issue_details")
            details.append(f"{metric.table} ({' | '.join(issues)})")
        logger.error(
            "Enriched validation failed",
            details="; ".join(details),
        )

    return handle_exit(
        overall_status=overall_status,
        enforce=args.enforce_quality or args.fail_on_issues,
        env=pipeline_env,
    )


if __name__ == "__main__":
    if not os.getenv("ECOM_CLI_SUPPRESS_DEPRECATION"):
        warnings.warn(
            (
                "Deprecated: use `ecomlake enriched validate` instead of "
                "python -m src.validation.enriched"
            ),
            UserWarning,
            stacklevel=2,
        )
    raise SystemExit(main())
