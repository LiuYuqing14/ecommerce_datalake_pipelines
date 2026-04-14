"""Sales velocity calculations."""

from __future__ import annotations

import polars as pl


def compute_sales_velocity(
    orders: pl.LazyFrame,
    order_items: pl.LazyFrame,
    window_days: int = 7,
) -> pl.LazyFrame:
    """Compute rolling sales velocity and inventory depletion rates."""
    # Ensure inputs are lazy
    orders = orders.lazy() if isinstance(orders, pl.DataFrame) else orders
    order_items = (
        order_items.lazy() if isinstance(order_items, pl.DataFrame) else order_items
    )

    sales = order_items.join(
        orders.select(["order_id", "order_date"]),
        on="order_id",
        how="inner",
    ).with_columns(order_dt=pl.col("order_date").cast(pl.Date))

    daily = (
        sales.group_by(["product_id", "order_dt"])
        .agg(pl.col("quantity").sum().alias("daily_quantity"))
        .sort(["product_id", "order_dt"])
    )

    return daily.with_columns(
        velocity_avg=pl.col("daily_quantity")
        .rolling_mean(window_size=window_days)
        .over("product_id")
    ).with_columns(
        trend_signal=pl.when(pl.col("daily_quantity") > pl.col("velocity_avg") * 1.2)
        .then(pl.lit("UP"))
        .when(pl.col("daily_quantity") < pl.col("velocity_avg") * 0.8)
        .then(pl.lit("DOWN"))
        .otherwise(pl.lit("STABLE"))
    )
