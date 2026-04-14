"""Polars schemas for Base Silver parquet reads."""

from __future__ import annotations

from typing import Dict

import polars as pl

ORDERS_SCHEMA: Dict[str, pl.DataType] = {
    "order_id": pl.String(),
    "total_items": pl.Int64(),
    "order_date": pl.Datetime(),
    "customer_id": pl.String(),
    "email": pl.String(),
    "order_channel": pl.String(),
    "is_expedited": pl.Boolean(),
    "customer_tier": pl.String(),
    "gross_total": pl.Decimal(18, 2),
    "net_total": pl.Decimal(18, 2),
    "total_discount_amount": pl.Decimal(18, 2),
    "payment_method": pl.String(),
    "shipping_speed": pl.String(),
    "shipping_cost": pl.Decimal(18, 2),
    "agent_id": pl.String(),
    "actual_shipping_cost": pl.Decimal(18, 2),
    "payment_processing_fee": pl.Decimal(18, 2),
    "shipping_address": pl.String(),
    "billing_address": pl.String(),
    "clv_bucket": pl.String(),
    "is_reactivated": pl.Boolean(),
    "batch_id": pl.String(),
    "ingestion_ts": pl.Datetime(),
    "ingestion_dt": pl.Date(),
    "event_id": pl.String(),
    "source_file": pl.String(),
    "order_dt": pl.Date(),
}

ORDER_ITEMS_SCHEMA: Dict[str, pl.DataType] = {
    "order_id": pl.String(),
    "product_id": pl.Int64(),
    "product_name": pl.String(),
    "category": pl.String(),
    "quantity": pl.Int64(),
    "unit_price": pl.Decimal(18, 2),
    "discount_amount": pl.Decimal(18, 2),
    "cost_price": pl.Decimal(18, 2),
    "batch_id": pl.String(),
    "ingestion_ts": pl.Datetime(),
    "ingestion_dt": pl.Date(),
    "event_id": pl.String(),
    "source_file": pl.String(),
    "order_dt": pl.Date(),
}

CART_ITEMS_SCHEMA: Dict[str, pl.DataType] = {
    "cart_item_id": pl.Int64(),
    "cart_id": pl.String(),
    "product_id": pl.Int64(),
    "product_name": pl.String(),
    "category": pl.String(),
    "added_at": pl.Datetime(),
    "quantity": pl.Int64(),
    "unit_price": pl.Decimal(18, 2),
    "batch_id": pl.String(),
    "ingestion_ts": pl.Datetime(),
    "ingestion_dt": pl.Date(),
    "event_id": pl.String(),
    "source_file": pl.String(),
    "added_dt": pl.Date(),
}

CUSTOMERS_SCHEMA: Dict[str, pl.DataType] = {
    "customer_id": pl.String(),
    "email": pl.String(),
    "signup_date": pl.Date(),
    "first_name": pl.String(),
    "last_name": pl.String(),
    "phone_number": pl.String(),
    "gender": pl.String(),
    "age": pl.Decimal(10, 2),
    "is_guest": pl.Boolean(),
    "customer_status": pl.String(),
    "signup_channel": pl.String(),
    "loyalty_tier": pl.String(),
    "initial_loyalty_tier": pl.String(),
    "email_verified": pl.Boolean(),
    "marketing_opt_in": pl.Boolean(),
    "mailing_address": pl.String(),
    "billing_address": pl.String(),
    "loyalty_enrollment_date": pl.Date(),
    "clv_bucket": pl.String(),
    "batch_id": pl.String(),
    "ingestion_ts": pl.Datetime(),
    "ingestion_dt": pl.Date(),
    "event_id": pl.String(),
    "source_file": pl.String(),
    "signup_dt": pl.Date(),
}

PRODUCT_CATALOG_SCHEMA: Dict[str, pl.DataType] = {
    "product_id": pl.Int64(),
    "product_name": pl.String(),
    "category": pl.String(),
    "unit_price": pl.Decimal(18, 2),
    "cost_price": pl.Decimal(18, 2),
    "inventory_quantity": pl.Int64(),
    "batch_id": pl.String(),
    "ingestion_ts": pl.Datetime(),
    "ingestion_dt": pl.Date(),
    "event_id": pl.String(),
    "source_file": pl.String(),
}

SHOPPING_CARTS_SCHEMA: Dict[str, pl.DataType] = {
    "cart_id": pl.String(),
    "customer_id": pl.String(),
    "created_at": pl.Datetime(),
    "updated_at": pl.Datetime(),
    "cart_total": pl.Decimal(18, 2),
    "status": pl.String(),
    "batch_id": pl.String(),
    "ingestion_ts": pl.Datetime(),
    "ingestion_dt": pl.Date(),
    "event_id": pl.String(),
    "source_file": pl.String(),
    "created_dt": pl.Date(),
}

RETURNS_SCHEMA: Dict[str, pl.DataType] = {
    "return_id": pl.String(),
    "order_id": pl.String(),
    "customer_id": pl.String(),
    "email": pl.String(),
    "return_date": pl.Datetime(),
    "reason": pl.String(),
    "return_type": pl.String(),
    "refunded_amount": pl.Decimal(18, 2),
    "return_channel": pl.String(),
    "agent_id": pl.String(),
    "refund_method": pl.String(),
    "batch_id": pl.String(),
    "ingestion_ts": pl.Datetime(),
    "ingestion_dt": pl.Date(),
    "event_id": pl.String(),
    "source_file": pl.String(),
    "return_dt": pl.Date(),
}

RETURN_ITEMS_SCHEMA: Dict[str, pl.DataType] = {
    "return_item_id": pl.Int64(),
    "return_id": pl.String(),
    "order_id": pl.String(),
    "product_id": pl.Int64(),
    "product_name": pl.String(),
    "category": pl.String(),
    "quantity_returned": pl.Int64(),
    "unit_price": pl.Decimal(18, 2),
    "cost_price": pl.Decimal(18, 2),
    "refunded_amount": pl.Decimal(18, 2),
    "batch_id": pl.String(),
    "ingestion_ts": pl.Datetime(),
    "ingestion_dt": pl.Date(),
    "event_id": pl.String(),
    "source_file": pl.String(),
    "return_dt": pl.Date(),
}

BASE_SILVER_SCHEMAS: Dict[str, Dict[str, pl.DataType]] = {
    "orders": ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "cart_items": CART_ITEMS_SCHEMA,
    "customers": CUSTOMERS_SCHEMA,
    "product_catalog": PRODUCT_CATALOG_SCHEMA,
    "shopping_carts": SHOPPING_CARTS_SCHEMA,
    "returns": RETURNS_SCHEMA,
    "return_items": RETURN_ITEMS_SCHEMA,
}

LINEAGE_REQUIRED_COLUMNS = [
    "batch_id",
    "ingestion_ts",
    "event_id",
    "source_file",
]

REQUIRED_BASE_SILVER_COLUMNS = {
    "orders": [
        "order_id",
        "customer_id",
        "order_date",
        "net_total",
        "gross_total",
        *LINEAGE_REQUIRED_COLUMNS,
    ],
    "order_items": [
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        *LINEAGE_REQUIRED_COLUMNS,
    ],
    "customers": [
        "customer_id",
        "email",
        "signup_date",
        *LINEAGE_REQUIRED_COLUMNS,
    ],
    "product_catalog": [
        "product_id",
        "product_name",
        "unit_price",
        *LINEAGE_REQUIRED_COLUMNS,
    ],
    "shopping_carts": [
        "cart_id",
        "customer_id",
        "created_at",
        *LINEAGE_REQUIRED_COLUMNS,
    ],
    "cart_items": [
        "cart_item_id",
        "cart_id",
        "product_id",
        "quantity",
        "unit_price",
        *LINEAGE_REQUIRED_COLUMNS,
    ],
    "returns": [
        "return_id",
        "order_id",
        "customer_id",
        "return_date",
        *LINEAGE_REQUIRED_COLUMNS,
    ],
    "return_items": [
        "return_item_id",
        "return_id",
        "order_id",
        "product_id",
        "quantity_returned",
        *LINEAGE_REQUIRED_COLUMNS,
    ],
}
