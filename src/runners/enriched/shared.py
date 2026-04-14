"""Shared helpers for Enriched Silver runners."""

from __future__ import annotations

import logging
import os
from datetime import date, datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, Sequence

import polars as pl
import pyarrow.parquet as pq

from src.runners.manifest import generate_manifest
from src.settings import PipelineConfig, load_settings
from src.specs import load_spec_safe
from src.validation.base_silver_schemas import BASE_SILVER_SCHEMAS

logger = logging.getLogger(__name__)


def normalize_schema(
    table: str, schema: dict[str, pl.DataType] | None
) -> dict[str, pl.DataType] | None:
    if schema is None:
        return None
    if table == "product_catalog" and "category" not in schema:
        schema = {**schema, "category": pl.String()}
    if table in get_dims_partitions() and "as_of_dt" not in schema:
        return {**schema, "as_of_dt": pl.Date()}
    return schema


def enriched_runner(
    output_table: str,
    input_tables: Sequence[str],
):
    """Decorator to standardize Enriched Silver runner boilerplate."""

    def decorator(
        func: Callable[[Dict[str, pl.LazyFrame], PipelineConfig, str], pl.LazyFrame],
    ):
        @wraps(func)
        def wrapper(
            base_silver_path: str,
            output_path: str,
            ingest_dt: str = "2020-01-01",
        ) -> Dict[str, Any]:
            start_time = datetime.now()
            settings = load_settings()
            lookback_days = settings.pipeline.enriched_lookback_days

            # 1. Read input tables
            tables = {
                table: read_partitioned(
                    base_silver_path, table, ingest_dt, lookback_days
                )
                for table in input_tables
            }

            # 2. Execute transform
            result_lazy = func(tables, settings.pipeline, ingest_dt)

            # 3. Add lineage and materialize
            result_lazy = result_lazy.with_columns(ingest_dt=pl.lit(ingest_dt))
            result = result_lazy.collect()

            # 4. Write output
            partition_col = get_enriched_partitions()[output_table]
            write_partitioned_shards(
                result,
                output_path,
                output_table,
                partition_col,
                settings.pipeline.enriched_max_rows_per_file,
            )

            elapsed = (datetime.now() - start_time).total_seconds()
            return {
                "table": output_table,
                "output_rows": len(result),
                "processing_time_seconds": elapsed,
                "output_path": f"{output_path}/{output_table}",
            }

        return wrapper

    return decorator


def get_table_partitions() -> dict[str, str | None]:
    """Get bronze table partitions (deprecated for enriched runners)."""
    spec = load_spec_safe()
    if spec:
        return {table.name: table.partition_key for table in spec.bronze.tables}
    return load_settings().pipeline.table_partitions


def get_silver_table_partitions() -> dict[str, str | None]:
    """Get silver table partitions for enriched runners."""
    spec = load_spec_safe()
    if spec:
        return {table.name: table.partition_key for table in spec.silver_base.tables}
    return load_settings().pipeline.silver_table_partitions


def get_enriched_partitions() -> dict[str, str]:
    spec = load_spec_safe()
    if spec:
        return {
            table.name: table.partition_key for table in spec.silver_enriched.tables
        }
    return load_settings().pipeline.enriched_partitions


def get_dims_partitions() -> dict[str, str | None]:
    spec = load_spec_safe()
    if spec:
        return {table.name: table.partition_key for table in spec.dims.tables}
    return {}


def is_gcs_path(path: str) -> bool:
    return path.startswith("gs://")


def list_partitions(
    base_path: str,
    table: str,
    partition_key: str,
) -> list[str]:
    if is_gcs_path(base_path):
        try:
            import fsspec
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "fsspec is required for GCS partition discovery"
            ) from exc
        fs = fsspec.filesystem("gcs")
        matches = fs.glob(f"{base_path}/{table}/{partition_key}=*")
        return sorted({m.split(f"{partition_key}=")[-1].rstrip("/") for m in matches})

    table_path = os.path.join(base_path, table)
    if not os.path.isdir(table_path):
        return []
    partitions = []
    for entry in os.listdir(table_path):
        if entry.startswith(f"{partition_key}="):
            partitions.append(entry.split("=", 1)[-1])
    return sorted(partitions)


def partition_range(ingest_dt: str, lookback_days: int) -> list[str]:
    end = date.fromisoformat(ingest_dt)
    start = end - timedelta(days=max(lookback_days, 0))
    current = start
    partitions = []
    while current <= end:
        partitions.append(current.isoformat())
        current += timedelta(days=1)
    return partitions


def read_partitioned(
    base_path: str,
    table: str,
    ingest_dt: str,
    lookback_days: int,
) -> pl.LazyFrame:
    dims_partitions = get_dims_partitions()
    dims_base = os.getenv("SILVER_DIMS_PATH")
    if table in dims_partitions:
        if not dims_base:
            spec = load_spec_safe()
            dims_base = spec.dims.base_path if spec and spec.dims.base_path else ""
        if dims_base:
            dims_base = os.path.expandvars(dims_base)
            if is_gcs_path(dims_base):
                dims_base = os.getenv(
                    "SILVER_DIMS_LOCAL_PATH", "/opt/airflow/data/silver/dims"
                )
            elif not os.path.isabs(dims_base):
                dims_base = os.path.join("/opt/airflow", dims_base)
            base_path = dims_base
        partition_key = dims_partitions.get(table)
    else:
        partition_key = get_silver_table_partitions().get(table)
    if partition_key is None:
        schema = normalize_schema(table, BASE_SILVER_SCHEMAS.get(table))
        return pl.scan_parquet(
            f"{base_path}/{table}/**/*.parquet",
            hive_partitioning=True,
            schema=schema,
        )

    date_partitions = {
        "ingest_dt",
        "ingestion_dt",
        "order_dt",
        "return_dt",
        "added_dt",
        "created_dt",
    }
    if partition_key not in date_partitions:
        schema = normalize_schema(table, BASE_SILVER_SCHEMAS.get(table))
        return pl.scan_parquet(
            f"{base_path}/{table}/{partition_key}=*/**/*.parquet",
            hive_partitioning=True,
            schema=schema,
        )

    available = set(list_partitions(base_path, table, partition_key))
    desired = [
        dt for dt in partition_range(ingest_dt, lookback_days) if dt in available
    ]
    if not desired:
        demo_mode = os.getenv("DEMO_MODE", "").lower() in {"1", "true", "yes", "on"}
        if demo_mode and table == "product_catalog" and available:
            latest = sorted(available)[-1]
            paths = [f"{base_path}/{table}/{partition_key}={latest}/**/*.parquet"]
            schema = normalize_schema(table, BASE_SILVER_SCHEMAS.get(table))
            logger.warning(
                "Demo mode: falling back to latest %s partition %s for %s.",
                partition_key,
                latest,
                table,
            )
            return pl.scan_parquet(
                paths,
                hive_partitioning=True,
                schema=schema,
            )
        # If no partitions match the requested date range, return an empty
        # DataFrame with the correct schema. This prevents pipeline failure
        # when data is legitimately missing (e.g. gaps in source data).
        logger.warning(
            f"No {partition_key} partitions found for {table} at {base_path} "
            f"matching range for {ingest_dt}. Returning empty DataFrame."
        )
        schema = normalize_schema(table, BASE_SILVER_SCHEMAS.get(table))
        if schema:
            return pl.DataFrame(schema=schema).lazy()
        # Fallback if schema is missing (should not happen given imports)
        raise FileNotFoundError(
            f"No {partition_key} partitions found for {table} at {base_path} "
            f"matching range for {ingest_dt}"
        )

    paths = [f"{base_path}/{table}/{partition_key}={dt}/**/*.parquet" for dt in desired]
    schema = normalize_schema(table, BASE_SILVER_SCHEMAS.get(table))
    return pl.scan_parquet(
        paths,
        hive_partitioning=True,
        schema=schema,
    )


def ensure_output_dir(path: str) -> None:
    if path.startswith("gs://"):
        return
    os.makedirs(path, exist_ok=True)


def output_file(path: str) -> str:
    path = path.rstrip("/")
    return f"{path}/part-00000.parquet"


def write_sharded_parquet(
    df: pl.DataFrame,
    output_uri: str,
    max_rows_per_file: int,
) -> None:
    def _write_parquet_compat(frame: pl.DataFrame, path: str) -> None:
        try:
            frame.write_parquet(path)
        except TypeError:
            pq.write_table(frame.to_arrow(), path, use_dictionary=False)

    output_uri = output_uri.rstrip("/")
    ensure_output_dir(output_uri)

    if max_rows_per_file <= 0:
        _write_parquet_compat(df, output_file(output_uri))
        return

    if df.height == 0:
        _write_parquet_compat(df.head(0), output_file(output_uri))
        return

    if df.height <= max_rows_per_file:
        _write_parquet_compat(df, output_file(output_uri))
        return

    shard_index = 0
    for offset in range(0, df.height, max_rows_per_file):
        chunk = df.slice(offset, max_rows_per_file)
        filename = f"{output_uri}/part-{shard_index:05d}.parquet"
        _write_parquet_compat(chunk, filename)
        shard_index += 1


def normalize_partition_values(df: pl.DataFrame, partition_col: str) -> pl.DataFrame:
    dtype = df.schema.get(partition_col)
    if dtype is None:
        return df
    if dtype == pl.Date:
        return df
    if dtype == pl.Datetime:
        return df.with_columns(pl.col(partition_col).cast(pl.Date).alias(partition_col))
    if dtype == pl.Utf8:
        parsed = df.select(
            pl.col(partition_col)
            .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            .alias("parsed"),
            pl.col(partition_col).alias("orig"),
        )
        invalid = parsed.filter(
            pl.col("orig").is_not_null() & pl.col("parsed").is_null()
        ).height
        has_parsed = parsed.select(pl.col("parsed").drop_nulls()).height > 0
        if invalid == 0 and has_parsed:
            return df.with_columns(
                pl.col(partition_col)
                .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                .alias(partition_col)
            )
        return df
    return df.with_columns(pl.col(partition_col).cast(pl.Utf8))


def write_partitioned_shards(
    df: pl.LazyFrame | pl.DataFrame,
    output_path: str,
    table: str,
    partition_col: str,
    max_rows_per_file: int,
) -> None:
    """Write partitioned parquet data supporting both LazyFrame and DataFrame inputs.

    If a LazyFrame is provided, prefer fully streaming sink_parquet operations.
    If a DataFrame is provided, fall back to the existing eager sharding logic.
    """

    # Handle LazyFrame path – collect then use eager partitioned write
    if isinstance(df, pl.LazyFrame):
        # Collect LazyFrame to DataFrame for partitioned write
        # Note: Polars sink_parquet doesn't support partition_by
        # parameter in current version
        df = df.collect()
        # Fall through to eager DataFrame logic below

    # ---- Existing eager fallback for DataFrame inputs ----
    df = normalize_partition_values(df, partition_col)

    if df.height == 0:
        # No rows to write for this run; avoid failing on missing partition cols.
        logger.warning("No rows produced for %s; skipping partitioned write.", table)
        ensure_output_dir(f"{output_path}/{table}")
        return

    if partition_col not in df.columns:
        raise ValueError(f"Missing partition column: {partition_col}")

    partitions = (
        df.select(pl.col(partition_col).drop_nulls().unique()).to_series().to_list()
    )

    if not partitions:
        partitions = ["unknown"]
        df = df.with_columns(pl.lit("unknown").alias(partition_col))

    for value in partitions:
        chunk = df.filter(pl.col(partition_col) == value)
        output_uri = f"{output_path}/{table}/{partition_col}={value}/"
        write_sharded_parquet(chunk, output_uri, max_rows_per_file)

    # Generate manifest for the table (scans all partitions we just wrote)
    generate_manifest(Path(output_path) / table)


__all__ = [
    "get_table_partitions",
    "get_enriched_partitions",
    "read_partitioned",
    "write_partitioned_shards",
    "enriched_runner",
]
