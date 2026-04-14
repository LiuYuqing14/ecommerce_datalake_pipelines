"""Cart attribution logic using Polars."""

from __future__ import annotations

import polars as pl


def compute_cart_attribution(
    carts: pl.LazyFrame,
    orders: pl.LazyFrame,
    tolerance_hours: int = 48,
) -> pl.LazyFrame:
    """Link orders to most recent carts within a tolerance window."""
    # Ensure inputs are lazy
    carts_lazy = carts.lazy() if isinstance(carts, pl.DataFrame) else carts
    orders_lazy = orders.lazy() if isinstance(orders, pl.DataFrame) else orders

    carts_ts = carts_lazy.with_columns(
        event_ts=pl.col("created_at").cast(pl.Datetime)
    ).sort(["customer_id", "event_ts"])
    orders_ts = orders_lazy.with_columns(
        event_ts=pl.col("order_date").cast(pl.Datetime)
    ).sort(["customer_id", "event_ts"])

    # join_asof currently requires eager DataFrames in older Polars,
    # but modern Polars supports it on LazyFrames if sorted.
    # We will return a LazyFrame.
    return orders_ts.join_asof(
        carts_ts,
        on="event_ts",
        by="customer_id",
        strategy="backward",
        tolerance=f"{tolerance_hours}h",
    ).with_columns(is_recovered=pl.col("cart_id").is_not_null())


def compute_cart_attribution_summary(
    carts: pl.LazyFrame,
    cart_items: pl.LazyFrame,
    orders: pl.LazyFrame,
    tolerance_hours: int = 48,
    abandoned_min_value: float = 0.0,
) -> pl.LazyFrame:
    """Summarize cart conversion and abandonment at cart grain."""
    carts_lazy = carts.lazy() if isinstance(carts, pl.DataFrame) else carts
    items_lazy = (
        cart_items.lazy() if isinstance(cart_items, pl.DataFrame) else cart_items
    )
    orders_lazy = orders.lazy() if isinstance(orders, pl.DataFrame) else orders

    cart_items_agg = items_lazy.group_by("cart_id").agg(
        item_count=pl.len(),
        category_count=pl.col("category").n_unique(),
        cart_value_items=(pl.col("quantity") * pl.col("unit_price")).sum(),
    )

    carts_ts = carts_lazy.with_columns(
        cart_ts=pl.col("created_at").cast(pl.Datetime)
    ).sort(["customer_id", "cart_ts"])
    orders_ts = orders_lazy.with_columns(
        order_ts=pl.col("order_date").cast(pl.Datetime)
    ).sort(["customer_id", "order_ts"])

    attributed = carts_ts.join_asof(
        orders_ts,
        left_on="cart_ts",
        right_on="order_ts",
        by="customer_id",
        strategy="forward",
        tolerance=f"{tolerance_hours}h",
    )

    cart_value_expr = pl.coalesce([pl.col("cart_total"), pl.col("cart_value_items")])
    has_value_expr = cart_value_expr.is_not_null() & (
        cart_value_expr > abandoned_min_value
    )

    enriched = attributed.join(cart_items_agg, on="cart_id", how="left").with_columns(
        cart_value=cart_value_expr,
        cart_status=pl.when(pl.col("order_id").is_not_null())
        .then(pl.lit("converted"))
        .when(has_value_expr)
        .then(pl.lit("abandoned"))
        .otherwise(pl.lit("empty")),
        time_to_purchase_hours=pl.when(pl.col("order_ts").is_not_null())
        .then((pl.col("order_ts") - pl.col("cart_ts")).dt.total_hours())
        .otherwise(None),
        abandoned_value=pl.when(pl.col("order_id").is_null() & has_value_expr)
        .then(cart_value_expr)
        .otherwise(None),
        order_date=pl.col("order_ts"),
    )

    return enriched.select(
        "cart_id",
        "customer_id",
        "created_at",
        "updated_at",
        "cart_value",
        "item_count",
        "category_count",
        "cart_status",
        "time_to_purchase_hours",
        "order_id",
        "order_date",
        "order_channel",
        "abandoned_value",
    )
