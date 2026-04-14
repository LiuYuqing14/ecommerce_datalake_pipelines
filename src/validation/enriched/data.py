from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid, ArrowTypeError

from src.observability import get_logger
from src.validation.common import (
    collect_parquet_files,
    count_parquet_rows,
    get_gcs_filesystem,
    join_path,
    list_partitions,
    path_exists,
)

logger = get_logger(__name__)


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def resolve_partition(
    table_path: Path | str,
    partition_key: str,
    ingest_dt: str | None,
    lookback_days: int,
) -> tuple[str | None, Path | str | None]:
    if not path_exists(table_path):
        return ingest_dt, None
    partitions = list_partitions(table_path, partition_key)
    if not partitions:
        return ingest_dt, None
    if partition_key == "ingest_dt":
        if ingest_dt:
            partition_path = join_path(table_path, f"{partition_key}={ingest_dt}")
            return ingest_dt, partition_path if path_exists(partition_path) else None
        latest = partitions[-1]
        return latest, join_path(table_path, f"{partition_key}={latest}")
    if ingest_dt:
        run_date = _parse_iso_date(ingest_dt)
        if run_date is None:
            return ingest_dt, None
        start = run_date - timedelta(days=max(lookback_days, 0))
        desired = []
        for value in partitions:
            part_date = _parse_iso_date(value)
            if part_date and start <= part_date <= run_date:
                desired.append(value)
        if not desired:
            return ingest_dt, None
        latest = desired[-1]
        return latest, join_path(table_path, f"{partition_key}={latest}")
    latest = partitions[-1]
    return latest, join_path(table_path, f"{partition_key}={latest}")


def get_schema_snapshot(path: Path | str) -> dict[str, str]:
    parquet_files = collect_parquet_files(path)
    if not parquet_files:
        return {}
    try:
        if isinstance(parquet_files[0], Path):
            parquet_file = pq.ParquetFile(parquet_files[0])
        else:
            fs = get_gcs_filesystem()
            with fs.open(parquet_files[0], "rb") as handle:
                parquet_file = pq.ParquetFile(handle)
    except (ArrowInvalid, ArrowTypeError, OSError, ValueError) as exc:
        logger.warning(
            "Failed to read parquet schema",
            path=str(parquet_files[0]),
            error_type=type(exc).__name__,
            error=str(exc),
        )
        return {}
    schema = parquet_file.schema_arrow
    return {field.name: str(field.type) for field in schema}


def compute_row_delta(
    table_path: Path | str,
    partition_key: str,
    partition_value: str | None,
    current_rows: int,
) -> tuple[int | None, float | None]:
    partitions = list_partitions(table_path, partition_key)
    if not partitions or not partition_value or partition_value not in partitions:
        return None, None
    idx = partitions.index(partition_value)
    if idx == 0:
        return None, None
    prior_partition = join_path(table_path, f"{partition_key}={partitions[idx - 1]}")
    prior_rows = count_parquet_rows(prior_partition)
    if prior_rows == 0:
        return prior_rows, None
    delta_pct = round(((current_rows - prior_rows) / prior_rows) * 100, 2)
    return prior_rows, delta_pct
