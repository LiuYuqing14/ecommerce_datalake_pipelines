"""Customer lifetime value calculations."""

from __future__ import annotations

from datetime import date, datetime

import polars as pl


def compute_customer_lifetime_value(
    customers: pl.LazyFrame,
    orders: pl.LazyFrame,
    returns: pl.LazyFrame,
    reference_date: date | None = None,
    churn_days: int = 90,
    whale_clv: float = 500.0,
    whale_orders: int = 5,
    medium_clv: float = 200.0,
) -> pl.LazyFrame:
    """Calculate historical CLV and engagement metrics."""
    # Ensure inputs are lazy
    customers = customers.lazy() if isinstance(customers, pl.DataFrame) else customers
    orders = orders.lazy() if isinstance(orders, pl.DataFrame) else orders
    returns = returns.lazy() if isinstance(returns, pl.DataFrame) else returns

    orders_dates = orders.with_columns(order_dt=pl.col("order_date").cast(pl.Date))
    returns_dates = returns.with_columns(return_dt=pl.col("return_date").cast(pl.Date))

    if reference_date is None:
        # Note: height is not available on LazyFrame without collecting.
        # We'll use current date as default if not provided.
        reference_date = datetime.utcnow().date()

    orders_agg = orders_dates.group_by("customer_id").agg(
        pl.len().alias("order_count"),
        pl.col("net_total").sum().alias("total_spent"),
        pl.col("net_total").mean().alias("avg_order_value"),
        pl.col("order_dt").min().alias("first_order_date"),
        pl.col("order_dt").max().alias("last_order_date"),
    )

    returns_agg = returns_dates.group_by("customer_id").agg(
        pl.len().alias("return_count"),
        pl.col("refunded_amount").sum().alias("total_refunded"),
    )

    result = (
        customers.unique(subset=["customer_id"], keep="last")
        .select(["customer_id", "clv_bucket"])
        .join(orders_agg, on="customer_id", how="left")
        .join(returns_agg, on="customer_id", how="left")
        .with_columns(
            pl.col("order_count").fill_null(0),
            pl.col("return_count").fill_null(0),
            pl.col("total_spent").fill_null(0.0),
            pl.col("total_refunded").fill_null(0.0),
            pl.col("avg_order_value").fill_null(0.0),
        )
        .with_columns(
            net_clv=pl.col("total_spent") - pl.col("total_refunded"),
            days_since_last_order=pl.when(pl.col("last_order_date").is_not_null())
            .then((pl.lit(reference_date) - pl.col("last_order_date")).dt.total_days())
            .otherwise(None),
        )
        .with_columns(
            customer_segment=pl.when(
                (pl.col("days_since_last_order") >= churn_days)
                & (pl.col("order_count") > 0)
            )
            .then(pl.lit("churned"))
            .when(pl.col("order_count") <= 1)
            .then(pl.lit("one-timer"))
            .when(
                (pl.col("net_clv") >= whale_clv)
                | (pl.col("order_count") >= whale_orders)
            )
            .then(pl.lit("whale"))
            .otherwise(pl.lit("regular")),
            actual_clv_bucket=pl.when(pl.col("net_clv") >= whale_clv)
            .then(pl.lit("high_value"))
            .when(pl.col("net_clv") >= medium_clv)
            .then(pl.lit("medium_value"))
            .otherwise(pl.lit("low_value")),
        )
        .rename({"clv_bucket": "predicted_clv_bucket"})
    )

    return result.select(
        [
            "customer_id",
            "total_spent",
            "total_refunded",
            "net_clv",
            "order_count",
            "return_count",
            "avg_order_value",
            "first_order_date",
            "last_order_date",
            "days_since_last_order",
            "customer_segment",
            "predicted_clv_bucket",
            "actual_clv_bucket",
        ]
    )
