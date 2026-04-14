# Enriched Silver Strategy: Polars-Powered Business Metrics

> **📌 HISTORICAL DOCUMENT - Original Enriched Silver Strategy (2026-01-11)**
>
> This document represents the initial strategy for Polars-powered business metrics in the Enriched Silver layer.
> For **current implementation**, see:
> - [TRANSFORMATION_SUMMARY.md](../resources/TRANSFORMATION_SUMMARY.md) - Current transform catalog
> - `src/transforms/` - Production Polars transform modules
> - `src/runners/enriched/` - Production enriched runners
>
> **Status**: ✅ Phase 1-3 Implemented - 10 enriched transforms in production

## Purpose

Define the Enriched Silver layer - complex business metrics computed via Polars that feed Gold marts. This layer transforms Base Silver's clean data into business-aligned analytics tables.

---

## Architecture

```
Base Silver (dbt-DuckDB, 8 tables)
    ↓ Polars Runners (Airflow Python operators)
Enriched Silver (Parquet, 5 tables)
    ↓ bq load (one-time cost)
BigQuery Enriched Silver
    ↓ dbt-BigQuery SQL (Gold marts)
Gold (BigQuery, 3 marts)
```

**Why Polars?**
- Complex temporal joins (cart → order attribution)
- Fast aggregations on 1M+ rows
- Local compute (no BigQuery scan costs)
- ~$200/month cheaper than dbt-BigQuery Python models

---

## Enriched Silver Tables (5 Tables)

### 1. `int_cart_attribution` - Cart Abandonment & Conversion

**Source Tables:**
- Base Silver: `shopping_carts` (262K rows)
- Base Silver: `cart_items` (1.1M rows)
- Base Silver: `orders` (35K rows)

**Grain:** One row per cart

**Business Question:** Which carts converted to orders, which were abandoned, and why?

**Key Metrics:**
- `cart_status` (converted | abandoned)
- `time_to_purchase` (duration between cart creation and order)
- `abandoned_value` (potential revenue lost)
- `cart_value`, `item_count`, `category_count`
- `conversion_channel` (Web, Mobile, Phone)

**Polars Features Used:**
- `join_asof()` - Temporal join (cart created → order placed within 48h)
- Window functions - Cart item aggregations
- Complex expressions - Cart abandonment classification

**Partitioning & Incremental (Polars):**
- **Partition column:** `ingest_dt` (Airflow run date, `{{ ds }}`)
- **Incremental scope:** Recompute carts created in the last 3 days and any orders placed in the last 3 days
- **Backfill policy:** Partition-by-partition recompute; use a configurable lookback window for late orders

**Gold Marts Fed:**
- `fct_marketing_attribution` - Conversion rates by channel
- New: `fct_cart_abandonment` - Abandonment trends

**Sample Output:**
| cart_id | customer_id | created_at | cart_value | cart_status | time_to_purchase | order_id |
|---------|-------------|------------|------------|-------------|------------------|----------|
| CART-1  | CUST-100    | 2023-01-14 | 245.50     | converted   | 2h 15m           | ORD-500  |
| CART-2  | CUST-101    | 2023-01-14 | 890.00     | abandoned   | null             | null     |

---

### 2. `int_product_performance` - Product Profitability Scorecard

**Source Tables:**
- Base Silver: `product_catalog` (3K rows)
- Base Silver: `order_items` (175K rows)
- Base Silver: `return_items` (8.7K rows)
- Base Silver: `cart_items` (1.1M rows)

**Grain:** One row per product per day

**Business Question:** What's the true profitability of each product after accounting for returns and abandonment?

**Key Metrics:**
- `units_sold`, `units_returned`, `units_in_carts`
- `gross_revenue` = sum(unit_price × quantity_sold)
- `gross_margin` = sum((unit_price - cost_price) × quantity_sold)
- `net_margin` = gross_margin - sum(refunded_amount)
- `return_rate` = units_returned / units_sold
- `cart_to_order_rate` = units_sold / units_in_carts
- `margin_pct` = net_margin / gross_revenue

**Polars Features Used:**
- Multi-table joins (product → order_items → return_items → cart_items)
- Group-by aggregations with complex expressions
- Per-product profitability calculations

**Partitioning & Incremental (Polars):**
- **Partition column:** `ingest_dt` (Airflow run date, `{{ ds }}`)
- **Incremental scope:** Recompute product-day metrics for the last 7 days to capture late returns
- **Backfill policy:** Partition-by-partition recompute; window size configurable

**Gold Marts Fed:**
- `fct_sales_operations` - Product velocity and trends
- New: `fct_product_profitability` - Margin analysis

**Sample Output:**
| product_id | product_dt | units_sold | units_returned | gross_revenue | net_margin | return_rate | margin_pct |
|------------|------------|------------|----------------|---------------|------------|-------------|------------|
| 2752       | 2023-01-14 | 145        | 12             | 7,250.00      | 3,125.40   | 8.3%        | 43.1%      |

---

### 3. `int_customer_lifetime_value` - Customer Value Tracker

**Source Tables:**
- Base Silver: `customers` (6.4K rows)
- Base Silver: `orders` (35K rows)
- Base Silver: `returns` (2.2K rows)

**Grain:** One row per customer (snapshot)

**Business Question:** What's each customer's actual lifetime value and segment?

**Key Metrics:**
- `total_spent` = sum(net_total) from orders
- `total_refunded` = sum(refunded_amount) from returns
- `net_clv` = total_spent - total_refunded
- `order_count`, `return_count`, `avg_order_value`
- `first_order_date`, `last_order_date`, `days_since_last_order`
- `customer_segment` (whale | regular | one-timer | churned)
- `predicted_clv_bucket` (from Base Silver) vs `actual_net_clv`

**Polars Features Used:**
- Customer-level aggregations
- Recency calculations (last order date)
- Segmentation logic (tiered bucketing)

**Partitioning & Incremental (Polars):**
- **Partition column:** `ingest_dt` (Airflow run date, `{{ ds }}`)
- **Incremental scope:** Recompute customers with new orders or returns in the last 7 days
- **Backfill policy:** Periodic full recompute (weekly) or incremental by changed customer_id

**Gold Marts Fed:**
- `fct_marketing_attribution` - CLV by acquisition channel
- New: `fct_customer_segments` - Retention cohorts

**Sample Output:**
| customer_id | total_spent | net_clv | order_count | days_since_last_order | customer_segment | predicted_clv_bucket | actual_clv_bucket |
|-------------|-------------|---------|-------------|-----------------------|------------------|----------------------|-------------------|
| CUST-100    | 12,450.00   | 11,980  | 28          | 5                     | whale            | high_value           | high_value        |
| CUST-101    | 450.00      | 450     | 1           | 180                   | churned          | medium_value         | low_value         |

---

### 4. `int_daily_business_metrics` - Daily Operational Dashboard

**Source Tables:**
- Base Silver: `orders` (35K rows)
- Base Silver: `shopping_carts` (262K rows)
- Base Silver: `returns` (2.2K rows)

**Grain:** One row per date

**Business Question:** What are our daily operational KPIs and trends?

**Key Metrics:**
- `date`, `orders_count`, `gross_revenue`, `net_revenue`, `avg_order_value`
- `carts_created`, `cart_conversion_rate` = orders / carts
- `returns_count`, `return_rate` = returns / orders
- `orders_7d_avg`, `revenue_7d_avg` (rolling averages)
- `revenue_anomaly_flag` (> 2 std devs from 30-day mean)

**Polars Features Used:**
- Daily aggregations across multiple tables
- Rolling window calculations (7-day, 30-day averages)
- Statistical anomaly detection

**Partitioning & Incremental (Polars):**
- **Partition column:** `ingest_dt` (Airflow run date, `{{ ds }}`)
- **Incremental scope:** Recompute last 14 days to keep rolling windows accurate
- **Backfill policy:** Partition-by-partition recompute; rolling windows use lookback >= 30 days

**Gold Marts Fed:**
- All Gold marts (provides daily dimension)
- New: `fct_daily_dashboard` - Executive KPI view

**Sample Output:**
| date       | orders_count | gross_revenue | cart_conversion_rate | revenue_7d_avg | anomaly_flag |
|------------|--------------|---------------|----------------------|----------------|--------------|
| 2023-01-14 | 1,689        | 245,890.00    | 13.4%                | 198,450.00     | HIGH         |

---

### 5. `int_shipping_economics` - Shipping Profitability

**Source Tables:**
- Base Silver: `orders` (35K rows)

**Grain:** One row per order

**Business Question:** Are we making or losing money on shipping?

**Key Metrics:**
- `order_id`, `order_date`, `shipping_speed`
- `shipping_cost` (charged to customer)
- `actual_shipping_cost` (what we paid)
- `shipping_margin` = shipping_cost - actual_shipping_cost
- `shipping_margin_pct` = shipping_margin / shipping_cost
- `is_expedited`, `order_channel`

**Polars Features Used:**
- Simple transformations and aggregations
- Margin calculations

**Partitioning & Incremental (Polars):**
- **Partition column:** `ingest_dt` (Airflow run date, `{{ ds }}`)
- **Incremental scope:** Recompute orders from the last 7 days
- **Backfill policy:** Partition-by-partition recompute; lookback window configurable

**Gold Marts Fed:**
- `fct_finance_revenue` - Shipping P&L
- New: `fct_shipping_analysis` - Cost optimization

**Sample Output:**
| order_id | order_date | shipping_speed | shipping_cost | actual_cost | shipping_margin | margin_pct |
|----------|------------|----------------|---------------|-------------|-----------------|------------|
| ORD-500  | 2023-01-14 | Standard       | 8.99          | 6.50        | 2.49            | 27.7%      |
| ORD-501  | 2023-01-14 | Expedited      | 15.99         | 18.25       | -2.26           | -14.1%     |

---

## Implementation Priority

### Phase 1: Core Business Metrics (Week 1)
**Build first, highest value:**

1. **`int_cart_attribution`** - Cart abandonment (87% abandonment = huge opportunity)
2. **`int_product_performance`** - Product profitability (margin > revenue)

**Deliverables:**
- Transform functions in `src/transforms/`
- Runner scripts in `src/runners/enriched/` (domain modules)
- Unit tests in `tests/unit/`
- Update Gold marts to consume these tables

### Phase 2: Customer & Operations (Week 2)
**Extend analytics:**

3. **`int_customer_lifetime_value`** - Customer segmentation
4. **`int_daily_business_metrics`** - Daily KPI dashboard

**Deliverables:**
- Additional transform functions
- Additional Gold marts (if needed)

### Phase 3: Shipping (Week 3)
**Nice-to-have:**

5. **`int_shipping_economics`** - Shipping P&L

---

## Alignment with Current DAG Outputs

The current DAG runs these Polars outputs:
- `int_attributed_purchases`
- `int_inventory_risk`
- `int_customer_retention_signals`
- `int_sales_velocity`
- `int_regional_financials`

**Proposed alignment options:**
1. **Rename current outputs to match the plan** and update downstream Gold marts accordingly.
2. **Keep current outputs as-is** and add the new planned tables in parallel, then migrate Gold marts when validated.

Given the plan focuses on business metrics, option 2 is lower-risk: add new enriched tables, validate in parallel, then deprecate older outputs.

## Gold Mart Mapping

### Existing Gold Marts (Update to Use Enriched Silver)

| Gold Mart | Current Source | New Enriched Silver Source | New Metrics Available |
|-----------|----------------|----------------------------|----------------------|
| `fct_marketing_attribution` | `int_attributed_purchases` | `int_cart_attribution` | Conversion rates, time-to-purchase, abandonment by channel |
| `fct_sales_operations` | `int_sales_velocity` | `int_product_performance` | Margin%, return rates, true profitability |
| `fct_finance_revenue` | `int_regional_financials` | `int_product_performance` + `int_shipping_economics` | Gross margin, net margin, shipping P&L |

### New Gold Marts (To Be Created)

| Gold Mart | Enriched Silver Source | Purpose |
|-----------|------------------------|---------|
| `fct_cart_abandonment` | `int_cart_attribution` | Daily/weekly abandonment trends, high-value abandoners |
| `fct_product_profitability` | `int_product_performance` | Product margin analysis, return risk scoring |
| `fct_customer_segments` | `int_customer_lifetime_value` | Customer cohorts, CLV accuracy, churn prediction |
| `fct_daily_dashboard` | `int_daily_business_metrics` | Executive KPI dashboard |
| `fct_shipping_analysis` | `int_shipping_economics` | Shipping cost optimization |

---

## Migration Plan (Gold)

1. **Run in parallel:** Keep current enriched outputs while adding the planned tables.
2. **Backfill + compare:** Validate Gold mart metrics side-by-side for 2-4 weeks.
3. **Switch sources:** Update Gold marts to use the new enriched tables once deltas are understood.
4. **Deprecate old outputs:** Remove legacy enriched tables and DAG tasks.

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ Base Silver (dbt-DuckDB)                                         │
│ - shopping_carts (262K)                                          │
│ - cart_items (1.1M)                                              │
│ - orders (35K)                                                   │
│ - order_items (175K)                                             │
│ - returns (2.2K)                                                 │
│ - return_items (8.7K)                                            │
│ - customers (6.4K)                                               │
│ - product_catalog (3K)                                           │
└───────────────────┬─────────────────────────────────────────────┘
                    │
                    ▼ Polars Runners (Airflow)
┌─────────────────────────────────────────────────────────────────┐
│ Enriched Silver (Parquet → BigQuery)                            │
│                                                                   │
│ 1. int_cart_attribution (262K rows)                             │
│    ├─ Conversion analysis                                        │
│    └─ Abandonment tracking                                       │
│                                                                   │
│ 2. int_product_performance (3K products × dates)                │
│    ├─ Margin calculations                                        │
│    ├─ Return rate analysis                                       │
│    └─ Cart-to-order conversion                                   │
│                                                                   │
│ 3. int_customer_lifetime_value (6.4K customers)                 │
│    ├─ Spending history                                           │
│    ├─ Segmentation                                               │
│    └─ Churn detection                                            │
│                                                                   │
│ 4. int_daily_business_metrics (~93 days)                        │
│    ├─ Daily KPIs                                                 │
│    ├─ Rolling averages                                           │
│    └─ Anomaly flags                                              │
│                                                                   │
│ 5. int_shipping_economics (35K orders)                          │
│    └─ Shipping margin per order                                  │
└───────────────────┬─────────────────────────────────────────────┘
                    │
                    ▼ dbt-BigQuery SQL
┌─────────────────────────────────────────────────────────────────┐
│ Gold Marts (BigQuery)                                            │
│                                                                   │
│ Existing:                                                         │
│ - fct_marketing_attribution (updated)                            │
│ - fct_sales_operations (updated)                                 │
│ - fct_finance_revenue (updated)                                  │
│                                                                   │
│ New:                                                              │
│ - fct_cart_abandonment                                           │
│ - fct_product_profitability                                      │
│ - fct_customer_segments                                          │
│ - fct_daily_dashboard                                            │
│ - fct_shipping_analysis                                          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Cost & Performance

### Current Approach (If Using dbt-BigQuery Python)
- Cost: ~$307/month (daily runs)
- Performance: ~85 seconds per model
- BigQuery scans: 1TB+ per day

### Polars Approach (Recommended)
- Cost: ~$114/month (daily runs)
- Performance: ~12 seconds per model (7x faster)
- BigQuery scans: Only final Gold queries

**Savings: $193/month (~$2,300/year)**

See [docs/local_only/COST_ANALYSIS.md](../local_only/COST_ANALYSIS.md) for detailed breakdown.

---

## Testing Strategy

### Unit Tests (pytest)
- Test each transform function in isolation
- Mock data matching actual schema
- Validate output columns and types
- Test edge cases (nulls, empty data, orphaned FKs)

### Integration Tests
- Run on sample Bronze data (93 days, 1.5M+ rows)
- Validate row counts match expectations
- Check FK integrity (cart → order joins)
- Profile performance on full dataset

### Data Quality Tests (dbt)
- Gold mart tests in `dbt_bigquery/models/gold_marts/schema.yml`
- Test FK relationships, uniqueness, not_null
- Accepted values for categorical fields
- Reconciliation tests (totals match between layers)

---

## Next Steps

1. **Validate enriched outputs** and write sample metrics to `docs/validation_reports/`
2. **Update Gold marts** to use new Enriched Silver tables
3. **Create new Gold marts** (fct_cart_abandonment, fct_product_profitability)
4. **Document** in README and validation guides

## Implementation Notes

- Transforms live in `src/transforms/`:
  - `cart_attribution.py`
  - `product_performance.py`
  - `customer_lifetime_value.py`
  - `daily_business_metrics.py`
  - `shipping_economics.py`
- Runner wiring is in `src/runners/enriched/` (exports via `src/runners/enriched/__init__.py`)
- Airflow tasks and BigQuery loads are in `airflow/dags/ecom_silver_to_gold.py`
- Unit tests live in `tests/unit/` (see `test_cart_attribution.py`, `test_product_performance.py`, `test_customer_lifetime_value.py`, `test_daily_business_metrics.py`, `test_shipping_economics.py`)

---

## References

- [Data Dictionary](../data/DATA_DICTIONARY.md) - Base Silver schema
- [Silver Quality Report](../validation_reports/SILVER_QUALITY.md) - Data quality baseline
- [Cost Analysis](../local_only/COST_ANALYSIS.md) - Polars vs dbt-BigQuery comparison
- [Architecture Summary](ARCHITECTURE_SUMMARY.md) - Overall pipeline design

<!-- GENERATED META -->
Last updated (UTC): 2026-01-11T23:45:00Z
<!-- END GENERATED META -->

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
