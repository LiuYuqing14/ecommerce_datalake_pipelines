from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl
from polars.exceptions import ColumnNotFoundError, ComputeError, SchemaError
from pyarrow.lib import ArrowInvalid, ArrowTypeError

from src.validation.common import (
    count_parquet_rows,
    join_path,
    path_exists,
    read_parquet_safe,
)
from src.validation.silver.data import get_quarantine_breakdown
from src.validation.silver.models import TableQualityMetrics

logger = logging.getLogger(__name__)

_NOT_SET = object()


def validate_table(
    table: str,
    bronze_path: Path | str,
    silver_path: Path | str,
    quarantine_path: Path | str,
    sla_thresholds: dict[str, float],
    allow_empty: bool = False,
    partition_key: str | None = None,
    partitions: list[str] | None = None,
    bronze_partition_key: str | None = None,
    bronze_partitions: list[str] | None | object = _NOT_SET,
) -> TableQualityMetrics:
    """Validate quality for a single table."""
    logger.info(f"Validating {table}...")

    # Use bronze_partition_key if provided, otherwise fall back to partition_key
    bronze_pk = (
        bronze_partition_key if bronze_partition_key is not None else partition_key
    )

    # Resolve bronze partitions:
    # 1. If explicit argument provided (including None), use it.
    # 2. If not provided (Sentinel), fallback to shared 'partitions'.
    bronze_parts_final: list[str] | None
    if bronze_partitions is _NOT_SET:
        bronze_parts_final = partitions
    else:
        # It's either list[str] or None at this point if not Sentinel
        bronze_parts_final = bronze_partitions  # type: ignore

    bronze_rows = count_parquet_rows(
        join_path(bronze_path, table),
        partition_key=bronze_pk,
        partitions=bronze_parts_final,
    )
    silver_rows = count_parquet_rows(
        join_path(silver_path, table),
        partition_key=partition_key,
        partitions=partitions,
    )
    quarantine_rows = count_parquet_rows(
        join_path(quarantine_path, table),
        partition_key=partition_key,
        partitions=partitions,
    )

    total_processed = silver_rows + quarantine_rows

    if allow_empty and bronze_rows == 0 and total_processed == 0:
        pass_rate = 1.0
        status = "WARN"
        logger.warning("%s: No rows processed (allowed empty source)", table)
    elif total_processed > 0:
        pass_rate = silver_rows / total_processed
        status = None
    else:
        pass_rate = 0.0
        status = None
        logger.warning(f"{table}: No rows processed!")

    sla_threshold = sla_thresholds.get(table, 0.95)

    if status is None:
        if pass_rate >= sla_threshold:
            status = "PASS"
        elif pass_rate >= (sla_threshold * 0.9):
            status = "WARN"
        else:
            status = "FAIL"

    quarantine_breakdown = get_quarantine_breakdown(
        join_path(quarantine_path, table),
        partition_key=partition_key,
        partitions=partitions,
    )
    row_loss = bronze_rows - total_processed
    row_loss_pct = (row_loss / bronze_rows * 100) if bronze_rows > 0 else 0.0

    logger.info(
        f"{table}: {status}",
        extra={
            "bronze_rows": bronze_rows,
            "silver_rows": silver_rows,
            "quarantine_rows": quarantine_rows,
            "pass_rate": f"{pass_rate:.2%}",
            "sla": f"{sla_threshold:.2%}",
        },
    )

    if status in ("WARN", "FAIL"):
        logger.warning(
            f"{table}: Pass rate {pass_rate:.2%} "
            f"{'below' if status == 'FAIL' else 'near'} "
            f"SLA {sla_threshold:.2%}"
        )

    if row_loss_pct > 1.0:
        logger.warning(f"{table}: Lost {row_loss_pct:.2f}% of rows ({row_loss:,} rows)")

    return TableQualityMetrics(
        table=table,
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        quarantine_rows=quarantine_rows,
        pass_rate=pass_rate,
        sla_threshold=sla_threshold,
        status=status,
        quarantine_breakdown=quarantine_breakdown,
        row_loss=row_loss,
        row_loss_pct=row_loss_pct,
    )


def compute_fk_mismatch_summary(silver_path: Path | str) -> list[dict[str, Any]]:
    fk_pairs = [
        ("order_items", "order_id", "orders", "order_id"),
        ("return_items", "order_id", "orders", "order_id"),
        ("return_items", "return_id", "returns", "return_id"),
        ("cart_items", "cart_id", "shopping_carts", "cart_id"),
    ]

    summary: list[dict[str, Any]] = []

    for child_table, child_key, parent_table, parent_key in fk_pairs:
        child_path = join_path(silver_path, child_table)
        parent_path = join_path(silver_path, parent_table)
        if not path_exists(child_path) or not path_exists(parent_path):
            continue

        try:
            child_keys_df = read_parquet_safe(
                child_path,
            )
            parent_keys_df = read_parquet_safe(
                parent_path,
            )
            if child_keys_df is None or parent_keys_df is None:
                continue

            child_keys = child_keys_df.select(pl.col(child_key).alias("key"))
            parent_keys = (
                parent_keys_df.select(pl.col(parent_key).alias("key"))
                .filter(pl.col("key").is_not_null())
                .unique()
            )
            missing_rows = (
                child_keys.filter(pl.col("key").is_not_null())
                .join(parent_keys, on="key", how="anti")
                .select(pl.len())
                .item()
            )
        except (ArrowInvalid, ArrowTypeError, OSError, ValueError) as exc:
            logger.warning(
                "Failed FK mismatch summary: parquet read error",
                extra={
                    "child_table": child_table,
                    "parent_table": parent_table,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            )
            continue
        except (ColumnNotFoundError, SchemaError, ComputeError) as exc:
            logger.warning(
                "Failed FK mismatch summary: schema/column error",
                extra={
                    "child_table": child_table,
                    "parent_table": parent_table,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            )
            continue

        if missing_rows > 0:
            summary.append(
                {
                    "child_table": child_table,
                    "child_key": child_key,
                    "parent_table": parent_table,
                    "parent_key": parent_key,
                    "missing_rows": missing_rows,
                }
            )

    return summary
