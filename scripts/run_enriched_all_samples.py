#!/usr/bin/env python3
"""Run enriched transforms + validation for all available sample dates."""

from __future__ import annotations

import argparse
import os
import sys
import warnings
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.runners import enriched  # noqa: E402
from src.specs import load_spec_safe  # noqa: E402
from src.validation.enriched import __main__ as enriched_quality  # noqa: E402

DEFAULT_PARTITION_MAP = {
    "orders": "order_dt",
    "order_items": "order_dt",
    "cart_items": "added_dt",
    "shopping_carts": "created_dt",
    "customers": "signup_dt",
    "returns": "return_dt",
    "return_items": "return_dt",
}


def list_dates(base_path: Path, table: str, key: str) -> set[str]:
    table_path = base_path / table
    if not table_path.exists():
        return set()
    return {
        part.name.split("=", 1)[-1]
        for part in table_path.glob(f"{key}=*")
        if part.is_dir()
    }


def resolve_valid_dates(base_path: Path) -> list[str]:
    spec = load_spec_safe()
    if spec:
        partition_map = {
            table.name: table.partition_key for table in spec.silver_base.tables
        }
    else:
        partition_map = DEFAULT_PARTITION_MAP
    date_sets = [
        list_dates(base_path, table, key) for table, key in partition_map.items()
    ]
    if not date_sets:
        return []
    return sorted(set.intersection(*date_sets))


def run_enriched_for_date(base_path: Path, output_path: str, ingest_dt: str) -> None:
    functions = [
        enriched.run_cart_attribution,
        enriched.run_cart_attribution_summary,
        enriched.run_inventory_risk,
        enriched.run_customer_retention,
        enriched.run_sales_velocity,
        enriched.run_product_performance,
        enriched.run_regional_financials,
        enriched.run_customer_lifetime_value,
        enriched.run_daily_business_metrics,
        enriched.run_shipping_economics,
    ]

    for func in functions:
        result = func(
            base_silver_path=str(base_path),
            output_path=output_path,
            ingest_dt=ingest_dt,
        )
        print(f"{result['table']}: rows={result['output_rows']}")


def write_enriched_report(
    output_path: str, ingest_dt: str, report_path: Path, enforce_quality: bool
) -> None:
    args = [
        "enriched_quality",
        "--enriched-path",
        output_path,
        "--ingest-dt",
        ingest_dt,
        "--output-report",
        str(report_path),
    ]
    if enforce_quality:
        args.append("--enforce-quality")
    # Reuse argparse via sys.argv for the module entrypoint.
    import sys

    sys.argv = args
    enriched_quality.main()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run enriched transforms + validation for all sample dates."
    )
    parser.add_argument(
        "--base-path",
        default="data/silver/base",
        help="Base Silver path.",
    )
    parser.add_argument(
        "--output-path",
        default="data/silver/enriched",
        help="Enriched Silver output path.",
    )
    parser.add_argument(
        "--report-path",
        default="docs/validation_reports/ENRICHED_QUALITY.md",
        help="Validation report path (overwritten per date).",
    )
    parser.add_argument(
        "--per-date",
        action="store_true",
        help="Write report per date (suffixes report filename).",
    )
    parser.add_argument(
        "--ingest-dt",
        default=None,
        help="Run enriched transforms for a single date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--enforce-quality",
        action="store_true",
        help="Exit non-zero on any validation failures.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_path = Path(args.base_path)
    output_path = args.output_path
    report_path = Path(args.report_path)

    if args.ingest_dt:
        ingest_dates = [args.ingest_dt]
    else:
        ingest_dates = resolve_valid_dates(base_path)
        if not ingest_dates:
            print("No shared partition dates found across base tables.")
            return
        print(f"Found {len(ingest_dates)} valid dates: {', '.join(ingest_dates)}")

    for ingest_dt in ingest_dates:
        print(f"\n=== Enriched run for {ingest_dt} ===")
        run_enriched_for_date(base_path, output_path, ingest_dt)

        if args.per_date:
            dated_report = report_path.with_name(
                f"{report_path.stem}_{ingest_dt}{report_path.suffix}"
            )
        else:
            dated_report = report_path

        write_enriched_report(
            output_path, ingest_dt, dated_report, args.enforce_quality
        )


if __name__ == "__main__":
    if not os.getenv("ECOM_CLI_SUPPRESS_DEPRECATION"):
        warnings.warn(
            (
                "Deprecated: use `ecomlake enriched run` instead of "
                "scripts/run_enriched_all_samples.py"
            ),
            UserWarning,
            stacklevel=2,
        )
    main()
