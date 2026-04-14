"""Churn risk detection logic."""

from __future__ import annotations

from datetime import date

import polars as pl


def compute_customer_retention_signals(
    customers: pl.LazyFrame,
    orders: pl.LazyFrame,
    lookback_days: list[int] | None = None,
    reference_date: date | None = None,
    bronze_nudge_days: int = 14,
) -> pl.LazyFrame:
    """Identify churn candidates based on order history."""
    if lookback_days is None:
        lookback_days = [30, 90]

    customers_lazy = (
        customers.lazy() if isinstance(customers, pl.DataFrame) else customers
    )
    customers_lazy = customers_lazy.unique(subset=["customer_id"], keep="last")
    orders = orders.lazy() if isinstance(orders, pl.DataFrame) else orders

    if not lookback_days:
        lookback_days = [30, 90]
    window_min = min(lookback_days)
    window_max = max(lookback_days)
    current_date = reference_date or date.today()

    order_rollup = (
        orders.select(["customer_id", "order_date"])
        .with_columns(order_dt=pl.col("order_date").cast(pl.Date))
        .group_by("customer_id")
        .agg(
            pl.col("order_dt").min().alias("first_purchase_date"),
            pl.col("order_dt").max().alias("last_purchase_date"),
            pl.len().alias("total_orders"),
        )
    )

    enriched = customers_lazy.join(
        order_rollup, on="customer_id", how="left"
    ).with_columns(pl.col("total_orders").fill_null(0))

    return enriched.with_columns(
        days_since_first_buy=(
            pl.lit(current_date) - pl.col("first_purchase_date")
        ).dt.total_days(),
        days_since_last_buy=(
            pl.lit(current_date) - pl.col("last_purchase_date")
        ).dt.total_days(),
    ).with_columns(
        is_in_danger_zone=(
            pl.col("days_since_first_buy").is_between(window_min, window_max)
        )
        & (pl.col("total_orders") == 1),
        needs_bronze_nudge=(pl.col("loyalty_tier") == "Bronze")
        & (pl.col("days_since_last_buy") > bronze_nudge_days),
    )
