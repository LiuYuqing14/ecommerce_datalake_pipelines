"""Inventory risk scoring logic."""

from __future__ import annotations

import polars as pl


def compute_inventory_risk(
    products: pl.LazyFrame,
    order_items: pl.LazyFrame,
    return_items: pl.LazyFrame,
) -> pl.LazyFrame:
    """Score products based on stock-out risk and return rates."""
    # Ensure inputs are lazy
    products = products.lazy() if isinstance(products, pl.DataFrame) else products
    order_items = (
        order_items.lazy() if isinstance(order_items, pl.DataFrame) else order_items
    )
    return_items = (
        return_items.lazy() if isinstance(return_items, pl.DataFrame) else return_items
    )

    sales = order_items.group_by("product_id").agg(
        pl.col("quantity").sum().alias("sales_volume")
    )
    returns = return_items.group_by("product_id").agg(
        pl.col("quantity_returned").sum().alias("return_volume")
    )

    inventory = (
        products.join(sales, on="product_id", how="left")
        .join(returns, on="product_id", how="left")
        .with_columns(
            pl.col("sales_volume").fill_null(0),
            pl.col("return_volume").fill_null(0),
            pl.col("inventory_quantity").fill_null(0),
            pl.col("cost_price").fill_null(0),
        )
    )

    with_metrics = inventory.with_columns(
        utilization_ratio=pl.when(pl.col("inventory_quantity") > 0)
        .then(pl.col("sales_volume") / pl.col("inventory_quantity"))
        .otherwise(0.0),
        return_signal=pl.when(pl.col("sales_volume") > 0)
        .then(pl.col("return_volume") / pl.col("sales_volume"))
        .otherwise(0.0),
        locked_capital=pl.col("cost_price") * pl.col("inventory_quantity"),
    )

    with_attention = with_metrics.with_columns(
        attention_score=(pl.col("utilization_ratio") + pl.col("return_signal")).clip(
            0, 1
        )
    )

    return with_attention.with_columns(
        risk_tier=pl.when(pl.col("attention_score") >= 0.8)
        .then(pl.lit("HIGH"))
        .when(pl.col("attention_score") >= 0.5)
        .then(pl.lit("MODERATE"))
        .otherwise(pl.lit("HEALTHY"))
    )
