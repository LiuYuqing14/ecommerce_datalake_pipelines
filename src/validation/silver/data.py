from __future__ import annotations

import logging
from pathlib import Path

import polars as pl
from polars.exceptions import ColumnNotFoundError, ComputeError, SchemaError

from src.validation.common import is_gcs_path, path_exists, read_parquet_safe

logger = logging.getLogger(__name__)


def get_quarantine_breakdown(
    quarantine_path: Path | str,
    top_n: int = 5,
    partition_key: str | None = None,
    partitions: list[str] | None = None,
) -> list[dict]:
    """Analyze quarantine reasons and return top failures."""
    if not path_exists(quarantine_path):
        return []

    # get_quarantine_breakdown

    # Use read_parquet_safe to handle GCS paths and pyarrow issues
    # Note: We don't pass partitions logic to read_parquet_safe currently
    # in its simplest form, but we can filter by path if needed.
    # For now, we trust read_parquet_safe to handle the path.
    # If explicit partition filtering is needed, we might need to
    # enhance read_parquet_safe or pre-filter.
    # Assuming quarantine_path points to the specific partition or base dir.

    # Actually, read_parquet_safe calls collect_parquet_files internally
    # but without partition args. We should update read_parquet_safe
    # or just rely on path. For now, let's just pass the path.

    df = read_parquet_safe(quarantine_path)

    if df is None:
        return []

    try:
        if "invalid_reason" not in df.columns:
            logger.warning(f"No invalid_reason column in {quarantine_path}")
            return []

        if "row_num" in df.columns:
            df = df.filter(pl.col("row_num").is_not_null())

        if df.height == 0:
            return []

        reason_counts = (
            df.group_by("invalid_reason")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .head(top_n)
        )

        total_quarantined = df.height
        breakdown = []
        for row in reason_counts.iter_rows(named=True):
            breakdown.append(
                {
                    "reason": row["invalid_reason"] or "unknown",
                    "count": row["count"],
                    "percentage": round((row["count"] / total_quarantined) * 100, 1),
                }
            )

        return breakdown

    except (ColumnNotFoundError, SchemaError, ComputeError) as e:
        logger.error(
            f"Failed to analyze quarantine schema for {quarantine_path}",
            extra={
                "error_type": type(e).__name__,
                "error": str(e),
            },
        )
        return []


def compute_key_cardinality(
    table_path: Path | str,
    key: str,
    partition_key: str | None = None,
    partitions: list[str] | None = None,
) -> dict[str, float]:
    if not path_exists(table_path):
        return {
            "total_rows": 0,
            "non_null_rows": 0,
            "distinct_count": 0,
            "distinct_ratio": 0.0,
        }

    # compute_key_cardinality

    # Use read_parquet_safe to handle GCS paths and pyarrow issues
    df = read_parquet_safe(
        table_path,
        columns=[key],
    )

    if df is None:
        logger.warning(
            "Failed key cardinality scan",
            extra={
                "table": str(table_path),
                "key": key,
            },
        )
        return {
            "total_rows": 0,
            "non_null_rows": 0,
            "distinct_count": 0,
            "distinct_ratio": 0.0,
        }

    # Check if key column exists (read_parquet_safe returns None or DF with columns)
    if key not in df.columns:
        logger.warning(
            f"Key column '{key}' not found in {table_path}",
            extra={"error_type": "ColumnNotFoundError"},
        )
        return {
            "total_rows": 0,
            "non_null_rows": 0,
            "distinct_count": 0,
            "distinct_ratio": 0.0,
        }

    total_rows = df.height
    non_null_rows = df.select(pl.col(key).is_not_null().sum()).item()
    distinct_count = df.select(pl.col(key).drop_nulls().n_unique()).item()
    distinct_ratio = distinct_count / non_null_rows if non_null_rows > 0 else 0.0

    return {
        "total_rows": total_rows,
        "non_null_rows": non_null_rows,
        "distinct_count": distinct_count,
        "distinct_ratio": distinct_ratio,
    }


def list_partitions_by_key(path: Path | str, partition_key: str) -> set[str]:
    """Return partition values for a table path keyed by partition_key."""
    path_str = str(path)
    partitions = set()
    if is_gcs_path(path_str):
        try:
            import fsspec
        except ModuleNotFoundError as exc:
            raise RuntimeError("gcsfs is required for gs:// validation reads") from exc
        fs = fsspec.filesystem("gcs")
        matches = fs.glob(f"{path_str.rstrip('/')}/{partition_key}=*")
        for match in matches:
            name = match.rstrip("/").split("/")[-1]
            if name.startswith(f"{partition_key}="):
                partitions.add(name.split("=", 1)[-1])
        return partitions

    for part_dir in Path(path_str).glob(f"{partition_key}=*"):
        if not part_dir.is_dir():
            continue
        partitions.add(part_dir.name.split("=", 1)[-1])
    return partitions


def check_required_columns(
    table_path: Path | str,
    required_cols: list[str],
    partition_key: str | None = None,
    partitions: list[str] | None = None,
    sample_rows: int | None = None,
) -> dict[str, list[str] | dict[str, int]]:
    """Check required columns exist and are non-null."""
    from src.validation.common import read_parquet_safe

    df = read_parquet_safe(
        table_path,
        columns=required_cols,
        n_rows=sample_rows,
    )
    if df is None:
        logger.warning(
            "Failed required-column scan",
            extra={
                "table": str(table_path),
            },
        )
        return {"missing": required_cols, "nulls": {}}

    missing_cols = [col for col in required_cols if col not in df.columns]
    null_counts: dict[str, int] = {}
    for col in required_cols:
        if col not in df.columns:
            continue
        nulls = df.select(pl.col(col).is_null().sum()).item()
        if nulls:
            null_counts[col] = int(nulls)

    return {"missing": missing_cols, "nulls": null_counts}
