# Transformation Summary

## Overview

This document provides a comprehensive overview of all transformations in the medallion lakehouse pipeline, organized by layer (Bronze → Silver → Gold). Each transformation's purpose, inputs, outputs, and business logic are documented to support debugging, extension, and architectural decisions.

**Pipeline Architecture**:

```
Bronze (Raw Parquet)
    ↓
Base Silver (dbt-duckdb) - 8 tables
    ↓
Dimension Snapshots (dbt-duckdb) - 2 tables
    ↓
Enriched Silver (Polars) - 10 transforms
    ↓
Gold Marts (dbt-bigquery) - 8 fact tables
```

**Quick Navigation**:
- [Base Silver (dbt)](#base-silver-dbt-duckdb) - Clean and conform raw data
- [Dimension Snapshots](#dimension-snapshots) - Daily reference data snapshots
- [Enriched Silver (Polars)](#enriched-silver-polars) - Business logic and behavioral signals
- [Gold Marts (dbt)](#gold-marts-dbt-bigquery) - Aggregated BI-ready facts

---

## Base Silver (dbt-duckdb)

**Goal**: Clean and conform raw Bronze into trusted, typed tables with quarantine handling.

**Why**: Every downstream transform depends on consistent keys, timestamps, and numeric types. Base Silver enforces data contracts, validates integrity, and isolates bad data.

**Technology**: dbt-duckdb running on local DuckDB database

**Location**: [dbt_duckdb/models/base_silver/](../../dbt_duckdb/models/base_silver/)

### Transformation Pattern

Each Base Silver model follows this pattern:

1. **Source Bronze data** via [sources.yml](../../dbt_duckdb/models/sources.yml)
2. **Type casting & normalization** - Standardize timestamps, numeric fields, and strings
3. **Deduplication** - Latest record wins by `ingestion_ts` and `event_id` per primary key
4. **Business date derivation** - Derive `order_dt`, `created_dt`, `added_dt`, `return_dt`, `signup_dt`
5. **Primary key validation** - Flag missing or duplicate PKs
6. **Foreign key validation** - Optional FK checks against dims when enabled (guest IDs may bypass)
7. **Quarantine split** - Invalid rows → quarantine table with `invalid_reason`
8. **Add metadata** - Append `batch_id`, `ingestion_ts`, `ingestion_dt`, `event_id`, `source_file`

### Base Silver Tables

#### 1. `stg_ecommerce__orders`

**Source**: [bronze.orders](../../samples/bronze/orders/)

**Primary Key**: `order_id`

**Foreign Keys**:
- `customer_id` → `stg_ecommerce__customers`

**Key Transformations**:
- Normalize identifiers/strings and parse `order_date`, `ingestion_ts`
- Cast monetary fields (`gross_total`, `net_total`, `total_discount_amount`, shipping costs/fees)
- Derive `order_dt` from `order_date` (fallback to `ingestion_ts`)
- Deduplicate by `order_id` using latest `ingestion_ts`/`event_id`

**Quarantine Rules**:
- Missing/invalid `order_id`, `customer_id`, or `order_date`
- Missing/negative `gross_total` or `net_total`, or `net_total > gross_total`
- Negative `total_discount_amount`
- FK invalid for `customer_id` when strict FK enforcement is enabled (guest IDs exempt)
- Duplicate `order_id` (keeps most recent)

**Output Columns**:
```
order_id
total_items
order_date
customer_id
email
order_channel
is_expedited
customer_tier
gross_total
net_total
total_discount_amount
payment_method
shipping_speed
shipping_cost
agent_id
actual_shipping_cost
payment_processing_fee
shipping_address
billing_address
clv_bucket
is_reactivated
batch_id
ingestion_ts
ingestion_dt
event_id
source_file
order_dt
```

---

#### 2. `stg_ecommerce__order_items`

**Source**: [bronze.order_items](../../samples/bronze/order_items/)

**Primary Key**: `order_id` + `product_id`

**Foreign Keys**:
- `order_id` → `stg_ecommerce__orders`
- `product_id` → `stg_ecommerce__product_catalog`

**Key Transformations**:
- Normalize identifiers/strings and cast `quantity`, `unit_price`, `discount_amount`, `cost_price`
- Derive `order_dt` from the orders dimension or `ingestion_ts`
- Deduplicate by (`order_id`, `product_id`) using latest `ingestion_ts`/`event_id`

**Quarantine Rules**:
- Missing/invalid `order_id` or `product_id`
- Non-positive `quantity` or negative pricing fields
- FK invalid for `order_id` or `product_id` when strict FK enforcement is enabled
- Duplicate (`order_id`, `product_id`) rows (keeps most recent)

**Output Columns**:
```
order_id
product_id
product_name
category
quantity
unit_price
discount_amount
cost_price
batch_id
ingestion_ts
ingestion_dt
event_id
source_file
order_dt
```

---

#### 3. `stg_ecommerce__customers`

**Source**: [bronze.customers](../../samples/bronze/customers/)

**Primary Key**: `customer_id`

**Key Transformations**:
- Normalize identifiers/strings (including lowercase email)
- Parse `signup_date` and derive `signup_dt`
- Cast profile fields (age, booleans) and deduplicate by `customer_id`

**Quarantine Rules**:
- Missing/invalid `customer_id` or `signup_date`
- Invalid email format
- Duplicate `customer_id` (keeps most recent)

**Output Columns**:
```
customer_id
email
signup_date
first_name
last_name
phone_number
gender
age
is_guest
customer_status
signup_channel
loyalty_tier
initial_loyalty_tier
email_verified
marketing_opt_in
mailing_address
billing_address
loyalty_enrollment_date
clv_bucket
batch_id
ingestion_ts
ingestion_dt
event_id
source_file
signup_dt
```

**Note**: Partitioned by `signup_dt` (not `ingestion_dt`)

---

#### 4. `stg_ecommerce__product_catalog`

**Source**: [bronze.product_catalog](../../samples/bronze/product_catalog/)

**Primary Key**: `product_id`

**Key Transformations**:
- Normalize identifiers/strings and cast `unit_price`, `cost_price`, `inventory_quantity`
- Validate `product_id` and pricing fields

**Quarantine Rules**:
- Missing/invalid `product_id` or `product_name`
- Missing/negative `unit_price`
- Negative `cost_price` or `inventory_quantity`
- `unit_price < cost_price` when both present
- Duplicate `product_id` (keeps most recent)

**Output Columns**:
```
product_id
product_name
category
unit_price
cost_price
inventory_quantity
batch_id
ingestion_ts
event_id
source_file
ingestion_dt
```

**Note**: Reference data - partitioned by `category` in Bronze, by `ingestion_dt` in Silver

---

#### 5. `stg_ecommerce__shopping_carts`

**Source**: [bronze.shopping_carts](../../samples/bronze/shopping_carts/)

**Primary Key**: `cart_id`

**Foreign Keys**:
- `customer_id` → `stg_ecommerce__customers`

**Key Transformations**:
- Parse `created_at`/`updated_at` and derive `created_dt`
- Cast `cart_total` and normalize status fields
- Deduplicate by `cart_id` using latest `ingestion_ts`/`event_id`

**Quarantine Rules**:
- Missing/invalid `cart_id`, `customer_id`, or `created_at`
- `cart_total < 0` or `updated_at < created_at`
- FK invalid for `customer_id` when strict FK enforcement is enabled (guest IDs exempt)
- Duplicate `cart_id` (keeps most recent)

**Output Columns**:
```
cart_id
customer_id
created_at
updated_at
cart_total
status
batch_id
ingestion_ts
ingestion_dt
event_id
source_file
created_dt
```

---

#### 6. `stg_ecommerce__cart_items`

**Source**: [bronze.cart_items](../../samples/bronze/cart_items/)

**Primary Key**: `cart_item_id`

**Foreign Keys**:
- `cart_id` → `stg_ecommerce__shopping_carts`
- `product_id` → `stg_ecommerce__product_catalog`

**Key Transformations**:
- Normalize identifiers/strings and cast `quantity`, `unit_price`
- Parse `added_at` and derive `added_dt`
- Deduplicate by (`cart_id`, `product_id`, `added_at`) using latest `ingestion_ts`/`event_id`

**Quarantine Rules**:
- Missing/invalid `cart_item_id`, `cart_id`, or `product_id`
- Non-positive `quantity` or negative `unit_price`
- FK invalid for `cart_id` or `product_id` when strict FK enforcement is enabled
- Duplicate cart item lines (keeps most recent)

**Output Columns**:
```
cart_item_id
cart_id
product_id
product_name
category
added_at
quantity
unit_price
batch_id
ingestion_ts
ingestion_dt
event_id
source_file
added_dt
```

---

#### 7. `stg_ecommerce__returns`

**Source**: [bronze.returns](../../samples/bronze/returns/)

**Primary Key**: `return_id`

**Foreign Keys**:
- `order_id` → `stg_ecommerce__orders`

**Key Transformations**:
- Parse `return_date` and derive `return_dt`
- Cast `refunded_amount` and normalize reason/channel fields
- Deduplicate by `return_id` using latest `ingestion_ts`/`event_id`

**Quarantine Rules**:
- Missing/invalid `return_id`, `order_id`, or `customer_id`
- Invalid `return_date` or negative `refunded_amount`
- FK invalid for `order_id` or `customer_id` when strict FK enforcement is enabled (guest IDs exempt)
- Duplicate `return_id` (keeps most recent)

**Output Columns**:
```
return_id
order_id
customer_id
email
return_date
reason
return_type
refunded_amount
return_channel
agent_id
refund_method
batch_id
ingestion_ts
ingestion_dt
event_id
source_file
return_dt
```

**Note**: Can have zero rows per partition (returns are optional)

---

#### 8. `stg_ecommerce__return_items`

**Source**: [bronze.return_items](../../samples/bronze/return_items/)

**Primary Key**: `return_item_id`

**Foreign Keys**:
- `return_id` → `stg_ecommerce__returns`
- `product_id` → `stg_ecommerce__product_catalog`

**Key Transformations**:
- Normalize identifiers/strings and cast `quantity_returned`, `unit_price`, `cost_price`, `refunded_amount`
- Derive `return_dt` and deduplicate by `return_item_id`

**Quarantine Rules**:
- Missing/invalid `return_item_id`, `return_id`, `order_id`, or `product_id`
- Non-positive `quantity_returned` or negative pricing fields
- FK invalid for `return_id`, `order_id`, or `product_id` when strict FK enforcement is enabled
- Duplicate `return_item_id` (keeps most recent)

**Output Columns**:
```
return_item_id
return_id
order_id
product_id
product_name
category
quantity_returned
unit_price
cost_price
refunded_amount
batch_id
ingestion_ts
ingestion_dt
event_id
source_file
return_dt
```

---

### Base Silver Summary

| Table | Bronze Source | PK | FKs | Partition Key | Allow Empty |
|-------|---------------|----|----|---------------|-------------|
| `stg_ecommerce__orders` | `orders` | `order_id` | `customer_id` | `ingestion_dt` | No |
| `stg_ecommerce__order_items` | `order_items` | `order_id` + `product_id` | `order_id`, `product_id` | `ingestion_dt` | No |
| `stg_ecommerce__customers` | `customers` | `customer_id` | None | `signup_dt` | No |
| `stg_ecommerce__product_catalog` | `product_catalog` | `product_id` | None | `ingestion_dt` | No |
| `stg_ecommerce__shopping_carts` | `shopping_carts` | `cart_id` | `customer_id` | `ingestion_dt` | No |
| `stg_ecommerce__cart_items` | `cart_items` | `cart_item_id` | `cart_id`, `product_id` | `ingestion_dt` | No |
| `stg_ecommerce__returns` | `returns` | `return_id` | `order_id` | `ingestion_dt` | Yes |
| `stg_ecommerce__return_items` | `return_items` | `return_item_id` | `return_id`, `product_id` | `ingestion_dt` | Yes |

---

## Dimension Snapshots

**Goal**: Create daily snapshots of slowly changing dimensions to avoid re-reading Bronze for every enriched transform.

**Why**: 60% reduction in Bronze reads, faster DAG execution, prevents stale dimension joins.

**Technology**: Python snapshot runner (Polars)

**Location**: [src/runners/dims_snapshot.py](../../src/runners/dims_snapshot.py)

### Snapshot Pattern

1. **Read Base Silver** - Load base tables from `SILVER_BASE_PATH`
2. **Filter & annotate** - Filter by run date and add `as_of_dt`
3. **Write snapshot** - Create `snapshot_dt=YYYY-MM-DD/data_0.parquet`
4. **Generate manifest** - Write `_MANIFEST.json` per snapshot partition
5. **Publish latest (optional)** - Write `_latest.json` via `publish_dims_latest`

### Dimension Tables

#### 1. `dims/customers`

**Source**: [data/silver/base/customers](../../data/silver/base/customers/)

**Snapshot Frequency**: Daily

**Structure**:
```
data/silver/dims/customers/
  snapshot_dt=2025-10-15/
    data_0.parquet
    _MANIFEST.json
  snapshot_dt=2025-10-16/
    snapshot.parquet
    _MANIFEST.json
  _latest.json  # {"run_date": "2025-10-16", "run_id": "...", "published_at": "..."}
```

**Schema**: Same as `stg_ecommerce__customers` plus `snapshot_dt`

---

#### 2. `dims/product_catalog`

**Source**: [data/silver/base/product_catalog](../../data/silver/base/product_catalog/)

**Snapshot Frequency**: Daily

**Structure**:
```
data/silver/dims/product_catalog/
  snapshot_dt=2025-10-15/
    data_0.parquet
    _MANIFEST.json
  _latest.json  # {"run_date": "2025-10-15", "run_id": "...", "published_at": "..."}
```

**Schema**: Same as `stg_ecommerce__product_catalog` plus `snapshot_dt`

---

## Enriched Silver (Polars)

**Goal**: Join Base Silver entities and compute behavioral/business signals using Polars for fast, memory-efficient processing.

**Why**: Provide domain-ready, interpretable metrics while keeping grain near source. Polars enables complex transformations with lazy evaluation and columnar optimizations.

**Technology**: Polars (lazy evaluation) with Parquet I/O

**Location**:
- **Transforms**: [src/transforms/](../../src/transforms/)
- **Runners**: [src/runners/enriched/](../../src/runners/enriched/)

### Transformation Pattern

Each enriched transform follows this pattern:

1. **Lazy load** Base Silver/dims tables via `pl.scan_parquet()`
2. **Apply business logic** using Polars expressions
3. **Compute metrics** (aggregations, window functions, joins)
4. **Add lineage** - Append `ingest_dt` for partitioning
5. **Write output** partitioned Parquet with `_MANIFEST.json`

### Enriched Transforms

#### 1. `int_attributed_purchases`

**Transform**: [src/transforms/cart_attribution.py](../../src/transforms/cart_attribution.py)

**Runner**: [src/runners/enriched/commerce.py](../../src/runners/enriched/commerce.py)

**Inputs**:
- `shopping_carts` (Base Silver)
- `orders` (Base Silver)

**Business Logic**:

Links each order to its most recent cart session within a configurable attribution window (default: 48 hours). Produces order-level cart recovery signals.

**Key Calculations**:
- `is_recovered` - True if a cart was matched within the attribution window
- `order_dt` - Date derived from `order_date` (added in runner)

**Partition Key**: `order_dt`

**Output Columns**:
```
order_id
customer_id
order_date
order_dt
cart_id
created_at
cart_total
order_channel
is_recovered
ingest_dt
```

**Notes**:
- Output retains most order/cart fields from the join; columns above are the key attribution fields.

---

#### 2. `int_cart_attribution`

**Transform**: [src/transforms/cart_attribution.py](../../src/transforms/cart_attribution.py)

**Runner**: [src/runners/enriched/commerce.py](../../src/runners/enriched/commerce.py)

**Inputs**:
- `shopping_carts` (Base Silver)
- `cart_items` (Base Silver)
- `orders` (Base Silver)

**Business Logic**:

Cart-level conversion/abandonment analysis. Flags abandoned carts, calculates lost value, and measures time-to-purchase for converted carts.

**Key Calculations**:
- `cart_status` - `converted`, `abandoned`, or `empty`
- `time_to_purchase_hours` - Hours from cart creation to order (if converted)
- `abandoned_value` - Cart value if abandoned
- `item_count` / `category_count` - Item and category counts per cart

**Partition Key**: `cart_dt`

**Output Columns**:
```
cart_id
customer_id
created_at
updated_at
cart_dt
cart_value
item_count
category_count
cart_status
time_to_purchase_hours
order_id
order_date
order_channel
abandoned_value
ingest_dt
```

**Semantic Checks**:
- `cart_status = 'converted'` → `order_id` is non-null
- `cart_status = 'abandoned'` → `order_id` is null
- `cart_status = 'abandoned'` → `abandoned_value = cart_value`
- `time_to_purchase_hours >= 0`

---

#### 3. `int_product_performance`

**Transform**: [src/transforms/product_performance.py](../../src/transforms/product_performance.py)

**Runner**: [src/runners/enriched/commerce.py](../../src/runners/enriched/commerce.py)

**Inputs**:
- `product_catalog` (Dimension snapshot)
- `order_items` (Base Silver)
- `return_items` (Base Silver)
- `cart_items` (Base Silver)

**Business Logic**:

Product-level profitability, return rate, and cart intent signals per business date.

**Key Calculations**:
- `units_sold` - Total quantity ordered
- `units_returned` - Total quantity returned
- `units_in_carts` - Total quantity in carts
- `gross_revenue` / `net_revenue` - Revenue before/after refunds
- `gross_profit` / `net_margin` - Profit before/after refunds
- `return_rate` - `units_returned / units_sold` (capped if configured)
- `cart_to_order_rate` - `units_sold / effective_units_in_carts` (capped if configured)
- `margin_pct` - `net_margin / gross_revenue`

**Partition Key**: `product_dt`

**Output Columns**:
```
product_id
product_name
category
product_dt
units_sold
units_returned
units_in_carts
gross_revenue
net_revenue
gross_margin
gross_profit
net_margin
refunded_amount
return_rate
cart_to_order_rate
margin_pct
catalog_unit_price
catalog_cost_price
inventory_quantity
ingest_dt
```

**Semantic Checks**:
- `return_rate <= 1.0` (when capped)
- `cart_to_order_rate <= 1.0` (when capped)

---

#### 4. `int_sales_velocity`

**Transform**: [src/transforms/sales_velocity.py](../../src/transforms/sales_velocity.py)

**Runner**: [src/runners/enriched/commerce.py](../../src/runners/enriched/commerce.py)

**Inputs**:
- `orders` (Base Silver)
- `order_items` (Base Silver)

**Business Logic**:

Rolling daily demand velocity with trend signals for inventory planning.

**Key Calculations**:
- `daily_quantity` - Units sold per product per day
- `velocity_avg` - Rolling mean over `window_days` (default 7)
- `trend_signal` - `UP`, `DOWN`, or `STABLE` vs rolling mean

**Partition Key**: `order_dt`

**Lookback**: Rolling windows benefit from `enriched_lookback_days` when backfilling.

**Output Columns**:
```
product_id
order_dt
daily_quantity
velocity_avg
trend_signal
ingest_dt
```

**Validation**:
- `daily_quantity >= 0`
- `trend_signal` in {`UP`, `DOWN`, `STABLE`}

---

#### 5. `int_customer_retention_signals`

**Transform**: [src/transforms/churn_detection.py](../../src/transforms/churn_detection.py)

**Runner**: [src/runners/enriched/customer.py](../../src/runners/enriched/customer.py)

**Inputs**:
- `customers` (Dimension snapshot)
- `orders` (Base Silver)

**Business Logic**:

Churn risk flags based on recency and single-purchase behavior.

**Key Calculations**:
- `days_since_first_buy` / `days_since_last_buy` - Based on reference date
- `is_in_danger_zone` - Single-purchase customers within lookback window
- `needs_bronze_nudge` - Bronze tier customers inactive beyond threshold

**Partition Key**: `ingest_dt`

**Output Columns**:
```
customer_id
first_purchase_date
last_purchase_date
total_orders
days_since_first_buy
days_since_last_buy
is_in_danger_zone
needs_bronze_nudge
ingest_dt
```

**Notes**:
- Output retains customer attributes from the dimension snapshot.

---

#### 6. `int_customer_lifetime_value`

**Transform**: [src/transforms/customer_lifetime_value.py](../../src/transforms/customer_lifetime_value.py)

**Runner**: [src/runners/enriched/customer.py](../../src/runners/enriched/customer.py)

**Inputs**:
- `customers` (Dimension snapshot)
- `orders` (Base Silver)
- `returns` (Base Silver)

**Business Logic**:

CLV calculation with segment bucketing for marketing and finance.

**Key Calculations**:
- `total_spent` - Lifetime net revenue
- `total_refunded` - Lifetime refund amount
- `net_clv` - `total_spent - total_refunded`
- `customer_segment` - `churned`, `one-timer`, `whale`, `regular`
- `predicted_clv_bucket` / `actual_clv_bucket` - Bucketed CLV
- `order_count` / `avg_order_value` - Lifetime order metrics

**Partition Key**: `ingest_dt`

**Output Columns**:
```
customer_id
total_spent
total_refunded
net_clv
order_count
return_count
avg_order_value
first_order_date
last_order_date
days_since_last_order
customer_segment
predicted_clv_bucket
actual_clv_bucket
ingest_dt
```

**Semantic Checks**:
- `net_clv = total_spent - total_refunded` (within tolerance)

---

#### 7. `int_regional_financials`

**Transform**: [src/transforms/regional_financials.py](../../src/transforms/regional_financials.py)

**Runner**: [src/runners/enriched/finance.py](../../src/runners/enriched/finance.py)

**Inputs**:
- `orders` (Base Silver)
- `customers` (Dimension snapshot)

**Business Logic**:

Regional enrichment for finance and ops reporting.

**Key Calculations**:
- `region` - From customer region or inferred from address
- `tax_rate` / `tax_amount` - Currently set to 0.0 and computed from `gross_total`
- `net_revenue` - `gross_total - tax_amount`

**Partition Key**: `order_dt`

**Output Columns**:
```
order_id
customer_id
order_date
order_dt
order_channel
region
gross_total
tax_rate
tax_amount
net_revenue
ingest_dt
```

**Notes**:
- Output retains the full order record with added region/tax fields.

---

#### 8. `int_shipping_economics`

**Transform**: [src/transforms/shipping_economics.py](../../src/transforms/shipping_economics.py)

**Runner**: [src/runners/enriched/finance.py](../../src/runners/enriched/finance.py)

**Inputs**:
- `orders` (Base Silver)

**Business Logic**:

Shipping margin and cost efficiency per order.

**Key Calculations**:
- `shipping_cost` - Charged to customer
- `actual_shipping_cost` - Incurred by business
- `shipping_margin` - `shipping_cost - actual_shipping_cost`
- `shipping_margin_pct` - `shipping_margin / shipping_cost`

**Partition Key**: `order_dt`

**Output Columns**:
```
order_id
order_dt
shipping_speed
shipping_cost
actual_shipping_cost
shipping_margin
shipping_margin_pct
is_expedited
order_channel
ingest_dt
```

**Semantic Checks**:
- `shipping_margin = shipping_cost - actual_shipping_cost` (within $0.01)
- `shipping_margin_pct` is null when `shipping_cost = 0`

---

#### 9. `int_inventory_risk`

**Transform**: [src/transforms/inventory_risk.py](../../src/transforms/inventory_risk.py)

**Runner**: [src/runners/enriched/commerce.py](../../src/runners/enriched/commerce.py)

**Inputs**:
- `product_catalog` (Dimension snapshot)
- `order_items` (Base Silver)
- `return_items` (Base Silver)

**Business Logic**:

Risk tiers and locked capital from inventory utilization and returns.

**Key Calculations**:
- `utilization_ratio` - `sales_volume / inventory_quantity`
- `return_signal` - `return_volume / sales_volume`
- `locked_capital` - `cost_price * inventory_quantity`
- `attention_score` - Clipped sum of utilization and return signals
- `risk_tier` - `HIGH`, `MODERATE`, `HEALTHY`

**Partition Key**: `ingest_dt`

**Output Columns**:
```
product_id
inventory_quantity
sales_volume
return_volume
utilization_ratio
return_signal
locked_capital
attention_score
risk_tier
ingest_dt
```

**Validation**:
- `risk_tier` in {`HIGH`, `MODERATE`, `HEALTHY`}
- `locked_capital >= 0`

**Notes**:
- Output retains product attributes from the catalog snapshot.

---

#### 10. `int_daily_business_metrics`

**Transform**: [src/transforms/daily_business_metrics.py](../../src/transforms/daily_business_metrics.py)

**Runner**: [src/runners/enriched/ops.py](../../src/runners/enriched/ops.py)

**Inputs**:
- `orders` (Base Silver)
- `returns` (Base Silver)
- `shopping_carts` (Base Silver)

**Business Logic**:

Daily KPI rollup for executive dashboards.

**Key Calculations**:
- `orders_count` - Total orders
- `gross_revenue` - Total order revenue
- `net_revenue` - Total net revenue
- `avg_order_value` - `net_revenue / orders_count`
- `returns_count` - Total returns
- `refund_total` - Total refund amount
- `return_rate` - `returns_count / orders_count`
- `carts_created` - Total carts created
- `cart_conversion_rate` - `orders_count / carts_created`
- `orders_7d_avg` / `revenue_7d_avg` - Rolling 7-day averages
- `revenue_30d_avg` / `revenue_30d_std` - Rolling 30-day stats
- `revenue_anomaly_flag` - `HIGH`, `LOW`, `NORMAL`

**Partition Key**: `date`

**Output Columns**:
```
date
orders_count
gross_revenue
net_revenue
avg_order_value
carts_created
cart_conversion_rate
returns_count
return_rate
refund_total
orders_7d_avg
revenue_7d_avg
revenue_30d_avg
revenue_30d_std
revenue_anomaly_flag
ingest_dt
```

**Semantic Checks**:
- `return_rate = returns_count / orders_count` (within epsilon)
- `cart_conversion_rate = orders_count / carts_created` (within epsilon)

---

### Enriched Silver Summary

| Transform | Partition Key | Inputs | Business Domain |
|-----------|---------------|--------|-----------------|
| `int_attributed_purchases` | `order_dt` | shopping_carts, orders | Commerce |
| `int_cart_attribution` | `cart_dt` | shopping_carts, cart_items, orders | Commerce |
| `int_product_performance` | `product_dt` | product_catalog, order_items, return_items, cart_items | Commerce |
| `int_sales_velocity` | `order_dt` | orders, order_items | Commerce |
| `int_customer_retention_signals` | `ingest_dt` | customers, orders | Customer |
| `int_customer_lifetime_value` | `ingest_dt` | customers, orders, returns | Customer |
| `int_regional_financials` | `order_dt` | orders, customers | Finance & Ops |
| `int_shipping_economics` | `order_dt` | orders | Finance & Ops |
| `int_inventory_risk` | `ingest_dt` | product_catalog, order_items, return_items | Finance & Ops |
| `int_daily_business_metrics` | `date` | orders, returns, shopping_carts | Executive |

---

## Gold Marts (dbt-bigquery)

**Goal**: Aggregated, domain-friendly marts for BI and analytics.

**Why**: BI tools need stable, summarized facts instead of granular transforms. Gold marts provide pre-aggregated, denormalized tables optimized for query performance.

**Technology**: dbt-bigquery running on BigQuery warehouse

**Location**: [dbt_bigquery/models/gold_marts/](../../dbt_bigquery/models/gold_marts/)

### Mart Pattern

Each Gold mart follows this pattern:

1. **Source Enriched Silver** - Read from BigQuery Silver dataset
2. **Aggregate** - Group by time/dimension keys
3. **Join dimensions** - Enrich with dimension attributes
4. **Calculate KPIs** - Compute business metrics
5. **Materialize** - Write to Gold dataset with partitioning/clustering

### Gold Mart Tables

#### 1. `fct_finance_revenue`

**Sources**:
- `int_regional_financials` (Enriched Silver)
- `int_shipping_economics` (Enriched Silver)

**Grain**: Daily revenue by order channel

**Key Metrics**:
- `gross_revenue`
- `net_revenue`
- `shipping_revenue`
- `shipping_cost`
- `shipping_margin`

**Business Use**: Finance dashboards, order-channel performance tracking

---

#### 2. `fct_marketing_attribution`

**Sources**:
- `int_attributed_purchases` (Enriched Silver)
- `int_cart_attribution` (Enriched Silver)
- `int_customer_retention_signals` (Enriched Silver)

**Grain**: Daily attribution metrics by channel

**Key Metrics**:
- `recovered_orders` / `total_orders`
- `abandoned_carts` / `converted_carts`
- `avg_time_to_purchase_hours`
- `abandoned_value`
- `at_risk_customers` / `total_customers`

**Business Use**: Marketing campaign analysis, funnel optimization

---

#### 3. `fct_sales_operations`

**Sources**:
- `int_sales_velocity` (Enriched Silver)
- `int_inventory_risk` (Enriched Silver)
- `int_product_performance` (Enriched Silver)

**Grain**: Daily product operations metrics

**Key Metrics**:
- `sales_velocity_7d` (avg of `velocity_avg`)
- `trend_signal`
- `inventory_quantity` / `inventory_risk_tier`
- `gross_profit`, `net_margin`, `return_rate`

**Business Use**: Inventory planning, pricing strategy

---

#### 4. `fct_cart_abandonment`

**Sources**:
- `int_cart_attribution` (Enriched Silver)

**Grain**: Daily cart abandonment vs conversion by channel

**Key Metrics**:
- `abandoned_carts`, `converted_carts`, `conversion_rate`
- `abandoned_value`
- `avg_time_to_purchase_hours`

**Business Use**: Remarketing campaigns, checkout optimization

---

#### 5. `fct_product_profitability`

**Sources**:
- `int_product_performance` (Enriched Silver)

**Grain**: Daily profitability by product

**Key Metrics**:
- `units_sold`, `units_returned`
- `gross_revenue`, `net_revenue`
- `gross_profit`, `net_margin`
- `return_rate`, `margin_pct`

**Business Use**: Product line optimization, pricing decisions

---

#### 6. `fct_customer_segments`

**Sources**:
- `int_customer_lifetime_value` (Enriched Silver)

**Grain**: Customer segment + CLV bucket combinations

**Key Metrics**:
- `customer_count`
- `avg_net_clv`
- `avg_order_value`
- `avg_total_spent`

**Business Use**: Customer segmentation, loyalty programs

---

#### 7. `fct_daily_dashboard`

**Sources**:
- `int_daily_business_metrics` (Enriched Silver)

**Grain**: Daily executive KPI rollup

**Key Metrics**:
- `orders_count`, `gross_revenue`, `net_revenue`, `avg_order_value`
- `carts_created`, `cart_conversion_rate`
- `returns_count`, `return_rate`, `refund_total`
- Rolling revenue stats and `revenue_anomaly_flag`

**Business Use**: Executive dashboards, daily reporting

---

#### 8. `fct_shipping_analysis`

**Sources**:
- `int_shipping_economics` (Enriched Silver)

**Grain**: Daily shipping metrics by speed/channel

**Key Metrics**:
- `orders`
- `shipping_revenue`, `shipping_cost`, `shipping_margin`
- `shipping_margin_pct`

**Business Use**: Logistics optimization, carrier negotiations

---

### Gold Marts Summary

| Mart | Enriched Sources | Grain Key | Business Domain |
|------|------------------|-----------|-----------------|
| `fct_finance_revenue` | regional_financials, shipping_economics | `order_date` | Finance |
| `fct_marketing_attribution` | attributed_purchases, cart_attribution, retention_signals | `metric_date` | Marketing |
| `fct_sales_operations` | sales_velocity, inventory_risk, product_performance | `order_date` | Sales Ops |
| `fct_cart_abandonment` | cart_attribution | `cart_date` | Commerce |
| `fct_product_profitability` | product_performance | `product_date` | Product |
| `fct_customer_segments` | customer_lifetime_value | `customer_segment + predicted_clv_bucket + actual_clv_bucket` | Customer |
| `fct_daily_dashboard` | daily_business_metrics | `metric_date` | Executive |
| `fct_shipping_analysis` | shipping_economics | `order_date` | Logistics |

---

## How to Use This Document

### For Debugging

**Trace data lineage**: Follow transform chain for the table in question

```
Bronze → Base Silver → Enriched → Gold
```

**Example**: Debug cart conversion metrics

1. Check Bronze `shopping_carts` partition exists
2. Validate Base Silver `stg_ecommerce__shopping_carts` row counts
3. Review Enriched `int_cart_attribution` logic
4. Verify Gold `fct_cart_abandonment` aggregations

### For Adding New Transforms

**Enriched Silver**:

1. Identify inputs from Base Silver or Dims
2. Define business logic in [src/transforms/](../../src/transforms/)
3. Create runner in [src/runners/enriched/](../../src/runners/enriched/)
4. Add table spec to [config/specs/enriched.yml](../../config/specs/enriched.yml)
5. Update this document with transform details

**Gold Marts**:

1. Identify Enriched Silver sources
2. Define aggregation grain and KPIs
3. Create dbt model in [dbt_bigquery/models/gold_marts/](../../dbt_bigquery/models/gold_marts/)
4. Add partitioning/clustering strategy
5. Update this document with mart details

### For QA Focus

**Layer-specific validation**:

- **Base Silver**: Validates data integrity (PK/FK, types, ranges)
- **Enriched Silver**: Validates business logic (semantic checks, sanity checks)
- **Gold Marts**: Validates aggregation logic (totals, averages, counts)

**Use validation modules**:

```bash
# Base Silver validation
ecomlake silver validate --partition-date 2025-10-15

# Enriched Silver validation
ecomlake enriched validate --ingest-dt 2025-10-15
```

---

## Related Documentation

- **[Architecture Overview](ARCHITECTURE.md)** - Complete system architecture with data flow
- **[Spec Overview](SPEC_OVERVIEW.md)** - Spec-driven orchestration and table metadata
- **[Data Contract](DATA_CONTRACT.md)** - Bronze → Silver type mappings
- **[Validation Guide](VALIDATION_GUIDE.md)** - Three-layer validation framework
- **[CLI Usage Guide](CLI_USAGE_GUIDE.md)** - Running transforms and validation

---

**Last Updated**: 2026-02-04
**Base Silver Tables**: 8 (dbt-duckdb)
**Dimension Snapshots**: 2 (customers, products)
**Enriched Transforms**: 10 (Polars)
**Gold Marts**: 8 (dbt-bigquery)

---

<p align="center">
  <a href="../../README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="../../RESOURCE_HUB.md">📚 <b>Resource Hub</b></a>
</p>

<p align="center">
  <sub>Last updated: 2026-02-04</sub><br>
  <sub>✨ Transform the data. Tell the story. Build the future. ✨</sub>
</p>
