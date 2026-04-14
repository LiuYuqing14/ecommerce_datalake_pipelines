#!/usr/bin/env python3
"""Test Gold layer logic (BigQuery SQL) on DuckDB using local Silver data."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import duckdb

# Ensure src is in path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.settings import load_settings  # noqa: E402


def resolve_source_path(table_name: str, settings) -> str:
    """Map source('silver', '...') to local parquet path."""
    silver_path = os.getenv("SILVER_ENRICHED_PATH", "data/silver/enriched")
    # Note: Enriched tables are partitioned. We'll pick the latest for testing.
    # Structure: silver_path/table_name/partition=val/*.parquet
    table_dir = Path(silver_path) / table_name

    if not table_dir.exists():
        return f"MISSING_{table_name}"

    # Find all parquet files recursively
    parquet_files = list(table_dir.glob("**/*.parquet"))
    if not parquet_files:
        return f"EMPTY_{table_name}"

    return str(table_dir / "**" / "*.parquet")


def transpile_bq_to_duckdb(sql: str, settings) -> str:
    """Convert BQ dbt SQL to DuckDB compatible SQL for local logic testing."""
    # 1. Replace {{ source('silver', 'table') }} with read_parquet(...)
    source_pattern = (
        r"\{\{\s*source\s*\(\s*['\"]silver['\"]\s*,\s*['\"](.+?)['\"]\s*\)\s*\}\}"
    )

    def replace_source(match):
        table_name = match.group(1)
        path = resolve_source_path(table_name, settings)
        return f"read_parquet('{path}')"

    sql = re.sub(source_pattern, replace_source, sql)

    # 2. BQ -> DuckDB transpilation rules
    # Replace safe_cast(x as type) -> try_cast(x as type)
    sql = re.sub(r"safe_cast\s*\(", "try_cast(", sql, flags=re.IGNORECASE)

    return sql


def main():
    settings = load_settings()
    models_dir = PROJECT_ROOT / "dbt_bigquery" / "models" / "gold_marts"

    print("--- Gold Logic Pre-flight (DuckDB) ---")
    print(f"Models directory: {models_dir}")

    con = duckdb.connect(database=":memory:")

    # Workaround for DuckDB 1.1.x 'INTERNAL Error: Unsupported type for
    # NumericValueUnionToValue'
    # This error occurs during statistics propagation for certain Decimal types
    # in aggregations.
    con.execute("SET disabled_optimizers = 'statistics_propagation'")

    # Register BQ compatibility macros
    con.execute("CREATE MACRO safe_divide(a, b) AS a / NULLIF(b, 0)")
    con.execute("CREATE MACRO countif(x) AS sum(CASE WHEN x THEN 1 ELSE 0 END)")
    con.execute("CREATE MACRO current_date() AS today()")

    success_count = 0
    fail_count = 0

    for model_file in models_dir.glob("*.sql"):
        model_name = model_file.stem
        print(f"\nChecking model: {model_name}")

        bq_sql = model_file.read_text()
        duck_sql = transpile_bq_to_duckdb(bq_sql, settings)

        if "MISSING_" in duck_sql or "EMPTY_" in duck_sql:
            print(f"  ⚠️  Skipped: Source data missing for {model_name}")
            continue

        try:
            # Create a view to test the logic
            con.execute(f"CREATE OR REPLACE VIEW {model_name} AS {duck_sql}")
            # Verify we can select from it
            res = con.execute(f"SELECT * FROM {model_name} LIMIT 5").pl()
            print(f"  ✅ SUCCESS: Logic valid. Sample rows: {len(res)}")
            success_count += 1
        except Exception as e:
            print(f"  ❌ FAILED: Logic error in {model_name}")
            print(f"     Error: {str(e)}")
            fail_count += 1

    print("\n--- Summary ---")
    print(f"Passed: {success_count}")
    print(f"Failed: {fail_count}")

    if fail_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
