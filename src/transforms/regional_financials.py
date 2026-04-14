"""Regional financial enrichment."""

from __future__ import annotations

import polars as pl


def compute_regional_financials(
    orders: pl.LazyFrame,
    customers: pl.LazyFrame,
) -> pl.LazyFrame:
    """Analyze financial performance by region and customer tier."""
    # Ensure inputs are lazy
    orders = orders.lazy() if isinstance(orders, pl.DataFrame) else orders
    customers = customers.lazy() if isinstance(customers, pl.DataFrame) else customers

    enriched = orders
    order_cols = orders.collect_schema().names()
    customer_cols = customers.collect_schema().names()

    if "region" not in order_cols and "region" in customer_cols:
        enriched = enriched.join(
            customers.select(["customer_id", "region"]),
            on="customer_id",
            how="left",
        )

    current_cols = enriched.collect_schema().names()

    address_parts = []
    if "shipping_address" in current_cols:
        address_parts.append(pl.col("shipping_address"))
    if "billing_address" in current_cols:
        address_parts.append(pl.col("billing_address"))

    if address_parts:
        address = pl.coalesce(address_parts)
        state = address.str.extract(r",\s*([A-Z]{2})\s\d{5}", 1)
        region_expr = pl.concat_str([pl.lit("US-"), state])
    else:
        region_expr = pl.lit("UNKNOWN")

    if "region" in current_cols:
        enriched = enriched.with_columns(
            pl.when(pl.col("region").is_null())
            .then(region_expr)
            .otherwise(pl.col("region"))
            .alias("region")
        )
    else:
        enriched = enriched.with_columns(region_expr.alias("region"))

    # Add financial metrics
    enriched = (
        enriched.with_columns(
            tax_rate=pl.lit(0.0),
        )
        .with_columns(
            tax_amount=pl.col("gross_total") * pl.col("tax_rate"),
        )
        .with_columns(net_revenue=pl.col("gross_total") - pl.col("tax_amount"))
    )

    return enriched
