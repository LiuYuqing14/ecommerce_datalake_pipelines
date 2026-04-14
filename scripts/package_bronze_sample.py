#!/usr/bin/env python3
"""Package Bronze sample data for 2020-01-01 through 2020-01-05.

Creates a complete Bronze sample with:
- All customers with signup_date <= 2020-01-05 (includes 2019 signups)
- Complete product catalog (all 5 categories)
- 5 consecutive days of fact tables (orders, carts, returns, etc.)

Usage:
    python scripts/package_bronze_sample.py \
        --source data/bronze \
        --output samples/bronze_samples.zip \
        --start-date 2020-01-01 \
        --end-date 2020-01-05
"""

from __future__ import annotations

import argparse
import os
import sys
import warnings
import zipfile
from datetime import date, timedelta
from pathlib import Path

# Configuration
DATE_START = "2020-01-01"
DATE_END = "2020-01-05"

# Tables to include
FACT_TABLES = [
    "orders",
    "order_items",
    "shopping_carts",
    "cart_items",
    "returns",
    "return_items",
]

DIM_TABLES = {
    "customers": "signup_date",
    "product_catalog": "category",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Package Bronze sample data for demo.")
    parser.add_argument(
        "--source",
        default="data/bronze",
        help="Source Bronze data directory",
    )
    parser.add_argument(
        "--output",
        default="samples/bronze_samples.zip",
        help="Output zip file path",
    )
    parser.add_argument(
        "--start-date",
        default=DATE_START,
        help="Start date for fact tables (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        default=DATE_END,
        help="End date for fact tables (YYYY-MM-DD)",
    )
    return parser.parse_args()


def date_range(start: str, end: str) -> list[str]:
    """Generate list of dates between start and end (inclusive)."""
    start_dt = date.fromisoformat(start)
    end_dt = date.fromisoformat(end)
    current = start_dt
    dates = []
    while current <= end_dt:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


def should_exclude_file(file_path: Path) -> bool:
    """Check if file should be excluded from the zip."""
    exclude_patterns = [".DS_Store", ".gitkeep", "Thumbs.db"]
    return any(pattern in file_path.name for pattern in exclude_patterns)


def collect_customer_partitions(source_dir: Path, end_date: str) -> list[Path]:
    """Collect all customer signup_date partitions up to end_date."""
    customers_dir = source_dir / "customers"
    if not customers_dir.exists():
        print(f"ERROR: Customers directory not found: {customers_dir}")
        return []

    partitions = []
    # Get all signup_date partitions
    for partition in sorted(customers_dir.glob("signup_date=*")):
        signup_date = partition.name.split("=")[1]
        if signup_date <= end_date:
            partitions.append(partition)

    return partitions


def collect_product_catalog_partitions(source_dir: Path) -> list[Path]:
    """Collect all product catalog category partitions."""
    catalog_dir = source_dir / "product_catalog"
    if not catalog_dir.exists():
        print(f"ERROR: Product catalog directory not found: {catalog_dir}")
        return []

    return list(catalog_dir.glob("category=*"))


def collect_fact_table_partitions(
    source_dir: Path, table: str, dates: list[str]
) -> list[Path]:
    """Collect ingest_dt partitions for a fact table."""
    table_dir = source_dir / table
    if not table_dir.exists():
        print(f"WARNING: Table directory not found: {table_dir}")
        return []

    partitions = []
    for dt in dates:
        partition_dir = table_dir / f"ingest_dt={dt}"
        if partition_dir.exists():
            partitions.append(partition_dir)
        else:
            print(f"  → {table} (ingest_dt={dt}): NOT FOUND (skipping)")

    return partitions


def add_partition_to_zip(
    zip_file: zipfile.ZipFile,
    partition_dir: Path,
    table_name: str,
    partition_name: str,
    source_root: Path,
):
    """Add all files from a partition directory to the zip."""
    files_added = 0

    for file_path in partition_dir.rglob("*"):
        if file_path.is_file() and not should_exclude_file(file_path):
            # Create archive path: bronze/<table>/<partition>/<file>
            rel_path = file_path.relative_to(source_root)
            arc_name = f"bronze/{rel_path}"

            zip_file.write(file_path, arc_name)
            files_added += 1

    return files_added


def main() -> int:
    args = parse_args()

    source_dir = Path(args.source)
    output_path = Path(args.output)

    if not source_dir.exists():
        print(f"ERROR: Source directory not found: {source_dir}")
        return 1

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Generate date range for fact tables
    dates = date_range(args.start_date, args.end_date)
    print(f"Packaging Bronze sample for {args.start_date} through {args.end_date}")
    print(f"Date range: {len(dates)} days")
    print(f"Source: {source_dir}")
    print(f"Output: {output_path}")
    print()

    total_files = 0
    total_partitions = 0

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        # 1. Package customers (all signups up to end date)
        print("==> Packaging customers (complete history)")
        customer_partitions = collect_customer_partitions(source_dir, args.end_date)
        print(f"  Found {len(customer_partitions)} customer partitions")

        for partition in customer_partitions:
            partition_name = partition.name
            files = add_partition_to_zip(
                zf, partition, "customers", partition_name, source_dir
            )
            total_files += files
            total_partitions += 1

        print(f"  ✓ Packaged {len(customer_partitions)} customer partitions")
        print()

        # 2. Package product catalog (all categories)
        print("==> Packaging product_catalog (all categories)")
        catalog_partitions = collect_product_catalog_partitions(source_dir)
        print(f"  Found {len(catalog_partitions)} category partitions")

        for partition in catalog_partitions:
            partition_name = partition.name
            files = add_partition_to_zip(
                zf, partition, "product_catalog", partition_name, source_dir
            )
            total_files += files
            total_partitions += 1

        print(f"  ✓ Packaged {len(catalog_partitions)} category partitions")
        print()

        # 3. Package fact tables (ingest_dt partitions for date range)
        print("==> Packaging fact tables")
        for table in FACT_TABLES:
            print(f"  → {table}")
            partitions = collect_fact_table_partitions(source_dir, table, dates)

            for partition in partitions:
                partition_name = partition.name
                ingest_dt = partition_name.split("=")[1]
                files = add_partition_to_zip(
                    zf, partition, table, partition_name, source_dir
                )
                total_files += files
                total_partitions += 1
                print(f"    → ingest_dt={ingest_dt}: {files} files")

            print(f"  ✓ Packaged {len(partitions)} partitions for {table}")

        print()

    # Report summary
    zip_size_mb = output_path.stat().st_size / (1024 * 1024)
    print("=" * 60)
    print("✅ Packaging complete!")
    print(f"  Total partitions: {total_partitions}")
    print(f"  Total files: {total_files}")
    print(f"  Zip size: {zip_size_mb:.2f} MB")
    print(f"  Output: {output_path}")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    if not os.getenv("ECOM_CLI_SUPPRESS_DEPRECATION"):
        warnings.warn(
            (
                "Deprecated: use `ecomlake sample package` instead of "
                "scripts/package_bronze_sample.py"
            ),
            UserWarning,
            stacklevel=2,
        )
    sys.exit(main())
