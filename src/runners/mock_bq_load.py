"""Mock BigQuery loader for local testing."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl

from src.validation.enriched_schemas import ENRICHED_SCHEMAS

logger = logging.getLogger(__name__)


def mock_bigquery_load(
    enriched_path: str,
    table: str,
    partition_key: str,
    partition_value: str,
    project_id: str = "mock-project",
    dataset: str = "silver",
) -> dict[str, Any]:
    """Mock BigQuery load that validates Parquet files and their schemas.

    Args:
        enriched_path: Path to enriched silver data
        table: Table name to load
        partition_key: Partition column name
        partition_value: Partition value (e.g., "2020-01-01")
        project_id: GCP project ID (ignored in mock)
        dataset: BigQuery dataset name (ignored in mock)

    Returns:
        Dictionary with mock load statistics
    """
    # Build the partition path
    partition_path = Path(enriched_path) / table / f"{partition_key}={partition_value}"

    if not partition_path.exists():
        logger.warning(
            f"Mock BQ Load: Partition not found: {partition_path}",
            extra={
                "table": table,
                "partition_key": partition_key,
                "partition_value": partition_value,
            },
        )
        return {
            "table": f"{project_id}.{dataset}.{table}",
            "partition": f"{partition_key}={partition_value}",
            "status": "SKIPPED",
            "rows": 0,
            "files": 0,
        }

    # Find Parquet files
    parquet_files = list(partition_path.glob("*.parquet"))

    if not parquet_files:
        logger.warning(
            f"Mock BQ Load: No Parquet files in {partition_path}",
            extra={"table": table, "partition_path": str(partition_path)},
        )
        return {
            "table": f"{project_id}.{dataset}.{table}",
            "partition": f"{partition_key}={partition_value}",
            "status": "NO_FILES",
            "rows": 0,
            "files": 0,
        }

    # Read Parquet files to get row counts and validate schema
    try:
        # Use scan to get schema without loading all data
        lf = pl.scan_parquet(parquet_files)
        observed_schema = lf.schema

        # 1. Basic Counts
        df = lf.collect()
        row_count = df.height
        column_count = len(df.columns)
        file_count = len(parquet_files)

        # 2. Schema Validation (New)
        target_schema = ENRICHED_SCHEMAS.get(table)
        schema_issues = []

        if target_schema:
            for col, expected_type in target_schema.items():
                if col not in observed_schema:
                    schema_issues.append(f"Missing column: {col}")
                    continue

                observed_type = observed_schema[col]
                # Compare types - convert to string for simple comparison if needed,
                # or handle complex type matching.
                if str(observed_type) != str(expected_type):
                    schema_issues.append(
                        f"Type mismatch for {col}: Expected {expected_type}, "
                        f"got {observed_type}"
                    )

        if schema_issues:
            logger.error(
                f"Mock BQ Load SCHEMA ERROR: {table}",
                extra={
                    "table": table,
                    "issues": schema_issues,
                },
            )
            return {
                "table": f"{project_id}.{dataset}.{table}",
                "status": "SCHEMA_ERROR",
                "issues": schema_issues,
                "rows": row_count,
                "files": file_count,
            }

        logger.info(
            f"Mock BQ Load SUCCESS: {table}",
            extra={
                "destination": f"{project_id}.{dataset}.{table}",
                "partition": f"{partition_key}={partition_value}",
                "rows": row_count,
                "columns": column_count,
                "files": file_count,
                "write_disposition": "WRITE_APPEND",
            },
        )

        return {
            "table": f"{project_id}.{dataset}.{table}",
            "partition": f"{partition_key}={partition_value}",
            "status": "SUCCESS",
            "rows": row_count,
            "columns": column_count,
            "files": file_count,
        }

    except Exception as exc:
        logger.error(
            f"Mock BQ Load FAILED: {table}",
            extra={
                "table": table,
                "partition": f"{partition_key}={partition_value}",
                "error": str(exc),
            },
        )
        return {
            "table": f"{project_id}.{dataset}.{table}",
            "partition": f"{partition_key}={partition_value}",
            "status": "ERROR",
            "error": str(exc),
            "rows": 0,
            "files": 0,
        }
