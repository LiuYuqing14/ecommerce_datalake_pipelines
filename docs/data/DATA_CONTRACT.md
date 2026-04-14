# Data Contract (Bronze -> Base Silver, Enriched Silver)

## Purpose
Define the expected schema, required fields, and change policy for silver tables.
This is the single source of truth for required fields, data types, and nullability.

## Scope
Base Silver tables map 1:1 with bronze tables. No split/merge.
Enriched Silver introduces business-aligned tables derived from Base Silver.
See `docs/data/BRONZE_SCHEMA_MAP.json` for current bronze schemas.

## Schema Versioning
- Current version: v1
- Additive changes (new nullable columns) -> minor version bump.
- Breaking changes (type changes, removed columns, renamed columns) -> major version bump.
- All schema changes must be documented in this file.

## Lineage Columns (Base Silver Only)
All Base Silver tables must include these columns (not null):
- batch_id (string)
- ingestion_ts (timestamp)
- event_id (string)
- source_file (string)

## Observed Bronze Types vs Base Silver Targets
Bronze source fields are string-heavy (timestamps and dates are strings). Base Silver must
explicitly cast to target types below. Use the profile report for observed types and
validate that casts are applied in dbt-duckdb models.

### cart_items
- cart_item_id: Bronze `int64` -> Base Silver `int64`
- cart_id: Bronze `string` -> Base Silver `string`
- product_id: Bronze `int64` -> Base Silver `int64`
- product_name: Bronze `string` -> Base Silver `string`
- category: Bronze `string` -> Base Silver `string`
- added_at: Bronze `string` -> Base Silver `timestamp`
- quantity: Bronze `int64` -> Base Silver `int64`
- unit_price: Bronze `float64` -> Base Silver `float64`
- batch_id: Bronze `string` -> Base Silver `string`
- ingestion_ts: Bronze `string` -> Base Silver `timestamp`
- event_id: Bronze `string` -> Base Silver `string`
- source_file: Bronze `string` -> Base Silver `string`

### customers
- customer_id: Bronze `string` -> Base Silver `string`
- first_name: Bronze `string` -> Base Silver `string`
- last_name: Bronze `string` -> Base Silver `string`
- email: Bronze `string` -> Base Silver `string`
- phone_number: Bronze `string` -> Base Silver `string`
- signup_date: Bronze `string` -> Base Silver `date`
- gender: Bronze `string` -> Base Silver `string`
- age: Bronze `float64` -> Base Silver `float64`
- is_guest: Bronze `boolean` -> Base Silver `bool`
- customer_status: Bronze `string` -> Base Silver `string`
- signup_channel: Bronze `string` -> Base Silver `string`
- loyalty_tier: Bronze `string` -> Base Silver `string`
- initial_loyalty_tier: Bronze `string` -> Base Silver `string`
- email_verified: Bronze `boolean` -> Base Silver `bool`
- marketing_opt_in: Bronze `boolean` -> Base Silver `bool`
- mailing_address: Bronze `string` -> Base Silver `string`
- billing_address: Bronze `string` -> Base Silver `string`
- loyalty_enrollment_date: Bronze `string` -> Base Silver `date`
- clv_bucket: Bronze `string` -> Base Silver `string`
- batch_id: Bronze `string` -> Base Silver `string`
- ingestion_ts: Bronze `string` -> Base Silver `timestamp`
- event_id: Bronze `string` -> Base Silver `string`
- source_file: Bronze `string` -> Base Silver `string`

### order_items
- order_id: Bronze `string` -> Base Silver `string`
- product_id: Bronze `int64` -> Base Silver `int64`
- product_name: Bronze `string` -> Base Silver `string`
- category: Bronze `string` -> Base Silver `string`
- quantity: Bronze `int64` -> Base Silver `int64`
- unit_price: Bronze `float64` -> Base Silver `float64`
- discount_amount: Bronze `float64` -> Base Silver `float64`
- cost_price: Bronze `float64` -> Base Silver `float64`
- batch_id: Bronze `string` -> Base Silver `string`
- ingestion_ts: Bronze `string` -> Base Silver `timestamp`
- event_id: Bronze `string` -> Base Silver `string`
- source_file: Bronze `string` -> Base Silver `string`

### orders
- order_id: Bronze `string` -> Base Silver `string`
- total_items: Bronze `int64` -> Base Silver `int64`
- order_date: Bronze `string` -> Base Silver `timestamp`
- customer_id: Bronze `string` -> Base Silver `string`
- email: Bronze `string` -> Base Silver `string`
- order_channel: Bronze `string` -> Base Silver `string`
- is_expedited: Bronze `boolean` -> Base Silver `bool`
- customer_tier: Bronze `string` -> Base Silver `string`
- gross_total: Bronze `float64` -> Base Silver `float64`
- net_total: Bronze `float64` -> Base Silver `float64`
- total_discount_amount: Bronze `float64` -> Base Silver `float64`
- payment_method: Bronze `string` -> Base Silver `string`
- shipping_speed: Bronze `string` -> Base Silver `string`
- shipping_cost: Bronze `float64` -> Base Silver `float64`
- agent_id: Bronze `string` -> Base Silver `string`
- actual_shipping_cost: Bronze `float64` -> Base Silver `float64`
- payment_processing_fee: Bronze `float64` -> Base Silver `float64`
- shipping_address: Bronze `string` -> Base Silver `string`
- billing_address: Bronze `string` -> Base Silver `string`
- clv_bucket: Bronze `string` -> Base Silver `string`
- is_reactivated: Bronze `boolean` -> Base Silver `bool`
- batch_id: Bronze `string` -> Base Silver `string`
- ingestion_ts: Bronze `string` -> Base Silver `timestamp`
- event_id: Bronze `string` -> Base Silver `string`
- source_file: Bronze `string` -> Base Silver `string`

### product_catalog
- product_id: Bronze `int64` -> Base Silver `int64`
- product_name: Bronze `string` -> Base Silver `string`
- category: Bronze `string` -> Base Silver `string`
- unit_price: Bronze `float64` -> Base Silver `float64`
- cost_price: Bronze `float64` -> Base Silver `float64`
- inventory_quantity: Bronze `int64` -> Base Silver `int64`
- batch_id: Bronze `string` -> Base Silver `string`
- ingestion_ts: Bronze `string` -> Base Silver `timestamp`
- event_id: Bronze `string` -> Base Silver `string`
- source_file: Bronze `string` -> Base Silver `string`

### return_items
- return_item_id: Bronze `int64` -> Base Silver `int64`
- return_id: Bronze `string` -> Base Silver `string`
- order_id: Bronze `string` -> Base Silver `string`
- product_id: Bronze `int64` -> Base Silver `int64`
- product_name: Bronze `string` -> Base Silver `string`
- category: Bronze `string` -> Base Silver `string`
- quantity_returned: Bronze `int64` -> Base Silver `int64`
- unit_price: Bronze `float64` -> Base Silver `float64`
- cost_price: Bronze `float64` -> Base Silver `float64`
- refunded_amount: Bronze `float64` -> Base Silver `float64`
- batch_id: Bronze `string` -> Base Silver `string`
- ingestion_ts: Bronze `string` -> Base Silver `timestamp`
- event_id: Bronze `string` -> Base Silver `string`
- source_file: Bronze `string` -> Base Silver `string`

### returns
- return_id: Bronze `string` -> Base Silver `string`
- order_id: Bronze `string` -> Base Silver `string`
- customer_id: Bronze `string` -> Base Silver `string`
- email: Bronze `string` -> Base Silver `string`
- return_date: Bronze `string` -> Base Silver `timestamp`
- reason: Bronze `string` -> Base Silver `string`
- return_type: Bronze `string` -> Base Silver `string`
- refunded_amount: Bronze `float64` -> Base Silver `float64`
- return_channel: Bronze `string` -> Base Silver `string`
- agent_id: Bronze `string` -> Base Silver `string`
- refund_method: Bronze `string` -> Base Silver `string`
- batch_id: Bronze `string` -> Base Silver `string`
- ingestion_ts: Bronze `string` -> Base Silver `timestamp`
- event_id: Bronze `string` -> Base Silver `string`
- source_file: Bronze `string` -> Base Silver `string`

### shopping_carts
- cart_id: Bronze `string` -> Base Silver `string`
- customer_id: Bronze `string` -> Base Silver `string`
- created_at: Bronze `string` -> Base Silver `timestamp`
- updated_at: Bronze `string` -> Base Silver `timestamp`
- cart_total: Bronze `float64` -> Base Silver `float64`
- status: Bronze `string` -> Base Silver `string`
- batch_id: Bronze `string` -> Base Silver `string`
- ingestion_ts: Bronze `string` -> Base Silver `timestamp`
- event_id: Bronze `string` -> Base Silver `string`
- source_file: Bronze `string` -> Base Silver `string`

## Base Silver Tables (Required Fields)
Columns not listed below are optional and preserved as-is.

### orders
- order_id (string, not null)
- customer_id (string, not null)
- order_date (timestamp, not null)
- net_total (float64, not null)
- gross_total (float64, not null)

### order_items
- order_id (string, not null)
- product_id (int64, not null)
- quantity (int64, not null)
- unit_price (float64, not null)

### customers
- customer_id (string, not null)
- email (string, not null)
- signup_date (date, not null)

### product_catalog
- product_id (int64, not null)
- product_name (string, not null)
- unit_price (float64, not null)

### shopping_carts
- cart_id (string, not null)
- customer_id (string, not null)
- created_at (timestamp, not null)

### cart_items
- cart_item_id (int64, not null)
- cart_id (string, not null)
- product_id (int64, not null)
- quantity (int64, not null)
- unit_price (float64, not null)

### returns
- return_id (string, not null)
- order_id (string, not null)
- customer_id (string, not null)
- return_date (timestamp, not null)

### return_items
- return_item_id (int64, not null)
- return_id (string, not null)
- order_id (string, not null)
- product_id (int64, not null)
- quantity_returned (int64, not null)

## Enriched Silver Tables (Required Fields)
Enriched Silver tables must include stable business keys and event timestamps.

### int_attributed_purchases
- order_id (string, not null)
- customer_id (string, not null)
- order_date (timestamp, not null)
- cart_id (string, nullable)
- is_recovered (bool, not null)

### int_inventory_risk
- product_id (int64, not null)
- attention_score (float64, not null)
- risk_tier (string, not null)
- locked_capital (float64, not null)

### int_customer_retention_signals
- customer_id (string, not null)
- days_since_first_buy (int64, not null)
- days_since_last_buy (int64, not null)
- is_in_danger_zone (bool, not null)
- needs_bronze_nudge (bool, not null)

### int_sales_velocity
- product_id (int64, not null)
- order_date (timestamp, not null)
- velocity_avg (float64, not null)
- trend_signal (string, not null)

### int_regional_financials
- order_id (string, not null)
- region (string, not null)
- gross_total (float64, not null)
- net_total (float64, not null)

## Change Policy
- Upstream changes must be communicated before deployment.
- Silver transforms will reject partitions with missing required columns.
- If a breaking change is detected, publish a new schema version and update downstream consumers.

## Contract Enforcement
- Base Silver validation uses Pydantic schemas and dbt tests.
- Enriched Silver validation uses unit tests for Polars transforms and dbt tests for output constraints.
- Contract violations block downstream publish steps.

## Ownership
- Drafted by: Data Engineering
- Reviewers: Business Intelligence, Data Science, Platform Engineering


---

<p align="center">
  <a href="../../README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="../../RESOURCE_HUB.md">📚 <b>Resource Hub</b></a>
</p>

<p align="center">
  <sub>Last updated: 2026-01-24</sub><br>
  <sub>✨ Transform the data. Tell the story. Build the future. ✨</sub>
</p>
<!-- GENERATED META -->
Last updated (UTC): 2026-01-24T20:59:59Z
Content hash (SHA-256): f261d206358c5de79133699eda713e32ce85be624dabda70a96f8baaf4e5471f
<!-- END GENERATED META -->
