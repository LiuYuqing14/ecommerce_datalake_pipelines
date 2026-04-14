"""Commerce-focused Enriched Silver runners."""

from __future__ import annotations

import polars as pl

from src.settings import PipelineConfig
from src.transforms.cart_attribution import (
    compute_cart_attribution,
    compute_cart_attribution_summary,
)
from src.transforms.inventory_risk import compute_inventory_risk
from src.transforms.product_performance import compute_product_performance
from src.transforms.sales_velocity import compute_sales_velocity

from .shared import enriched_runner


@enriched_runner(
    output_table="int_attributed_purchases",
    input_tables=["shopping_carts", "orders"],
)
def run_cart_attribution(
    tables: dict[str, pl.LazyFrame], settings: PipelineConfig, ingest_dt: str
) -> pl.LazyFrame:
    """Cart attribution transform."""
    return compute_cart_attribution(
        carts=tables["shopping_carts"],
        orders=tables["orders"],
        tolerance_hours=settings.attribution_tolerance_hours,
    ).with_columns(
        order_dt=pl.col("order_date").cast(pl.Date),
    )


@enriched_runner(
    output_table="int_cart_attribution",
    input_tables=["shopping_carts", "cart_items", "orders"],
)
def run_cart_attribution_summary(
    tables: dict[str, pl.LazyFrame], settings: PipelineConfig, ingest_dt: str
) -> pl.LazyFrame:
    """Cart attribution summary (cart-level)."""
    return compute_cart_attribution_summary(
        carts=tables["shopping_carts"],
        cart_items=tables["cart_items"],
        orders=tables["orders"],
        tolerance_hours=settings.attribution_tolerance_hours,
        abandoned_min_value=settings.cart_abandoned_min_value,
    ).with_columns(
        cart_dt=pl.col("created_at").cast(pl.Date),
    )


@enriched_runner(
    output_table="int_inventory_risk",
    input_tables=["product_catalog", "order_items", "return_items"],
)
def run_inventory_risk(
    tables: dict[str, pl.LazyFrame], settings: PipelineConfig, ingest_dt: str
) -> pl.LazyFrame:
    """Inventory risk scoring transform."""
    return compute_inventory_risk(
        products=tables["product_catalog"],
        order_items=tables["order_items"],
        return_items=tables["return_items"],
    )


@enriched_runner(
    output_table="int_product_performance",
    input_tables=["product_catalog", "order_items", "return_items", "cart_items"],
)
def run_product_performance(
    tables: dict[str, pl.LazyFrame], settings: PipelineConfig, ingest_dt: str
) -> pl.LazyFrame:
    """Product performance (profitability) transform."""
    return compute_product_performance(
        products=tables["product_catalog"],
        order_items=tables["order_items"],
        return_items=tables["return_items"],
        cart_items=tables["cart_items"],
        rate_precision=settings.rate_precision,
        rate_cap_min=settings.rate_cap_min,
        rate_cap_max=settings.rate_cap_max,
        rate_cap_enabled=settings.rate_cap_enabled,
    )


@enriched_runner(
    output_table="int_sales_velocity",
    input_tables=["orders", "order_items"],
)
def run_sales_velocity(
    tables: dict[str, pl.LazyFrame], settings: PipelineConfig, ingest_dt: str
) -> pl.LazyFrame:
    """Sales velocity transform (rolling windows)."""
    return compute_sales_velocity(
        orders=tables["orders"],
        order_items=tables["order_items"],
        window_days=settings.sales_velocity_window_days,
    )
