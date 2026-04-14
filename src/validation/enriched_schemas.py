"""Polars schemas for Enriched Silver parquet files (for BQ compatibility)."""

from __future__ import annotations

from typing import Dict

import polars as pl

# Schema for int_attributed_purchases
ATTRIBUTED_PURCHASES_SCHEMA: Dict[str, pl.DataType] = {
    "cart_id": pl.String(),
    "customer_id": pl.String(),
    "product_id": pl.Int64(),
    "order_id": pl.String(),
    "added_at": pl.Datetime(),
    "order_date": pl.Datetime(),
    "quantity": pl.Int64(),
    "unit_price": pl.Decimal(18, 2),
    "cost_price": pl.Decimal(18, 2),
    "attribution_window_hours": pl.Float64(),
    "is_within_window": pl.Boolean(),
    "ingest_dt": pl.String(),
}

# Schema for int_customer_lifetime_value
CUSTOMER_LIFETIME_VALUE_SCHEMA: Dict[str, pl.DataType] = {
    "customer_id": pl.String(),
    "total_spent": pl.Float64(),
    "total_refunded": pl.Float64(),
    "net_clv": pl.Float64(),
    "order_count": pl.Int64(),
    "return_count": pl.Int64(),
    "first_order_date": pl.Date(),
    "last_order_date": pl.Date(),
    "customer_segment": pl.String(),
    "is_active": pl.Boolean(),
    "ingest_dt": pl.String(),
}

# Schema for int_daily_business_metrics
DAILY_BUSINESS_METRICS_SCHEMA: Dict[str, pl.DataType] = {
    "business_dt": pl.Date(),
    "orders_count": pl.Int64(),
    "gross_revenue": pl.Float64(),
    "net_revenue": pl.Float64(),
    "carts_created": pl.Int64(),
    "cart_conversion_rate": pl.Float64(),
    "returns_count": pl.Int64(),
    "return_rate": pl.Float64(),
    "avg_order_value": pl.Float64(),
    "ingest_dt": pl.String(),
}

# Map of all enriched table schemas
ENRICHED_SCHEMAS: Dict[str, Dict[str, pl.DataType]] = {
    "int_attributed_purchases": ATTRIBUTED_PURCHASES_SCHEMA,
    "int_customer_lifetime_value": CUSTOMER_LIFETIME_VALUE_SCHEMA,
    "int_daily_business_metrics": DAILY_BUSINESS_METRICS_SCHEMA,
    # Add other schemas as needed, or use these as primary validation examples
}
