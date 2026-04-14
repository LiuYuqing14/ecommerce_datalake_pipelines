"""Daily business KPI calculations."""

from __future__ import annotations

import polars as pl

from src.transforms.common import _resolve_date_column


def compute_daily_business_metrics(
    orders: pl.LazyFrame,
    returns: pl.LazyFrame,
    carts: pl.LazyFrame,
    ratio_epsilon: float = 0.0001,
    rate_precision: int = 6,
    rate_cap_min: float = 0.0,
    rate_cap_max: float = 1.0,
    rate_cap_enabled: bool = True,
) -> pl.LazyFrame:
    """Aggregate high-level business performance daily."""
    # Ensure inputs are lazy
    orders = orders.lazy() if isinstance(orders, pl.DataFrame) else orders
    returns = returns.lazy() if isinstance(returns, pl.DataFrame) else returns
    carts = carts.lazy() if isinstance(carts, pl.DataFrame) else carts

    order_date_col = _resolve_date_column(orders, ["order_dt", "order_date"])
    cart_date_col = _resolve_date_column(carts, ["created_dt", "created_at"])
    return_date_col = _resolve_date_column(returns, ["return_dt", "return_date"])

    orders_daily = (
        orders.with_columns(date=pl.col(order_date_col).cast(pl.Date))
        .group_by("date")
        .agg(
            pl.len().alias("orders_count"),
            pl.col("gross_total").sum().alias("gross_revenue"),
            pl.col("net_total").sum().alias("net_revenue"),
        )
    )

    carts_daily = (
        carts.with_columns(date=pl.col(cart_date_col).cast(pl.Date))
        .group_by("date")
        .agg(pl.len().alias("carts_created"))
    )

    returns_daily = (
        returns.with_columns(date=pl.col(return_date_col).cast(pl.Date))
        .group_by("date")
        .agg(
            pl.len().alias("returns_count"),
            pl.col("refunded_amount").sum().alias("refund_total"),
        )
    )

    combined = (
        orders_daily.join(carts_daily, on="date", how="full", coalesce=True)
        .join(returns_daily, on="date", how="full", coalesce=True)
        .filter(pl.col("date").is_not_null())
        .sort("date")
        .with_columns(
            pl.col("orders_count").fill_null(0),
            pl.col("carts_created").fill_null(0),
            pl.col("returns_count").fill_null(0),
            pl.col("gross_revenue").fill_null(0.0),
            pl.col("net_revenue").fill_null(0.0),
            pl.col("refund_total").fill_null(0.0),
        )
    )

    orders_count_f = pl.col("orders_count").cast(pl.Float64)
    carts_created_f = pl.col("carts_created").cast(pl.Float64)
    returns_count_f = pl.col("returns_count").cast(pl.Float64)

    cart_rate_expr = orders_count_f / carts_created_f
    return_rate_expr = returns_count_f / orders_count_f
    if rate_cap_enabled:
        cart_rate_expr = cart_rate_expr.clip(rate_cap_min, rate_cap_max)
        return_rate_expr = return_rate_expr.clip(rate_cap_min, rate_cap_max)

    combined = combined.with_columns(
        avg_order_value=pl.when(pl.col("orders_count") > 0)
        .then(pl.col("net_revenue") / orders_count_f)
        .otherwise(0.0),
        cart_conversion_rate=pl.when(pl.col("carts_created") > 0)
        .then(cart_rate_expr.round(rate_precision))
        .otherwise(0.0),
        return_rate=pl.when(pl.col("orders_count") > 0)
        .then(return_rate_expr.round(rate_precision))
        .otherwise(0.0),
    )

    combined = combined.with_columns(
        orders_7d_avg=pl.col("orders_count").rolling_mean(window_size=7),
        revenue_7d_avg=pl.col("net_revenue").rolling_mean(window_size=7),
        revenue_30d_avg=pl.col("net_revenue").rolling_mean(window_size=30),
        revenue_30d_std=pl.col("net_revenue").rolling_std(window_size=30),
    )

    combined = combined.with_columns(
        revenue_anomaly_flag=pl.when(
            pl.col("net_revenue")
            > (pl.col("revenue_30d_avg") + pl.col("revenue_30d_std") * 2)
        )
        .then(pl.lit("HIGH"))
        .when(
            pl.col("net_revenue")
            < (pl.col("revenue_30d_avg") - pl.col("revenue_30d_std") * 2)
        )
        .then(pl.lit("LOW"))
        .otherwise(pl.lit("NORMAL"))
    )

    return combined.select(
        [
            "date",
            "orders_count",
            "gross_revenue",
            "net_revenue",
            "avg_order_value",
            "carts_created",
            "cart_conversion_rate",
            "returns_count",
            "return_rate",
            "refund_total",
            "orders_7d_avg",
            "revenue_7d_avg",
            "revenue_30d_avg",
            "revenue_30d_std",
            "revenue_anomaly_flag",
        ]
    )
