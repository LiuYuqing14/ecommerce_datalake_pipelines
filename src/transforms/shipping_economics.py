"""Shipping economics calculations."""

from __future__ import annotations

import polars as pl

from src.transforms.common import _resolve_date_column


def compute_shipping_economics(
    orders: pl.LazyFrame,
) -> pl.LazyFrame:
    """Analyze shipping costs, margins, and performance."""
    # Ensure inputs are lazy
    orders = orders.lazy() if isinstance(orders, pl.DataFrame) else orders
    date_col = _resolve_date_column(orders, ["order_dt", "order_date"])

    result = orders.with_columns(
        order_dt=pl.col(date_col).cast(pl.Date),
        shipping_margin=pl.col("shipping_cost") - pl.col("actual_shipping_cost"),
        shipping_margin_pct=pl.when(pl.col("shipping_cost") > 0)
        .then(
            (pl.col("shipping_cost") - pl.col("actual_shipping_cost"))
            / pl.col("shipping_cost")
        )
        .otherwise(None),
        is_expedited=pl.when(pl.col("shipping_speed").is_in(["two-day", "overnight"]))
        .then(pl.lit(True))
        .otherwise(pl.lit(False)),
    )

    return result.select(
        [
            "order_id",
            "order_dt",
            "shipping_speed",
            "shipping_cost",
            "actual_shipping_cost",
            "shipping_margin",
            "shipping_margin_pct",
            "is_expedited",
            "order_channel",
        ]
    )
