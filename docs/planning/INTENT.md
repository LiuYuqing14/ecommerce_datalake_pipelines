# ECOM_DATALAKE_PIPELINES

**Modern Lakehouse Pipeline: Bronze → Rich Silver → BigQuery Gold Marts**

> **📌 HISTORICAL DOCUMENT - Original Planning & Vision (Pre-Implementation)**
>
> This document represents the initial architectural vision and planning for this project.
> For **current project status, implementation details, and actual features**, see the main [README.md](../../README.md).
>
> **Project Status**: ✅ Feature Complete - All Phase 1 & 2 objectives achieved (as of January 2026)

## Project Overview

This repository represents the evolution of my data engineering practice, implementing a production-grade medallion architecture pipeline. The project leverages a **hybrid local-cloud compute strategy** to process 6 years of synthetic e-commerce data (~20GB) while minimizing warehouse costs and maximizing transformation performance.

### The Learning Journey

This is iteration 2 of my data pipeline work. **Iteration 1** ([ecom-datalake-exten](https://github.com/G-Schumacher44/ecom-datalake-exten)) focused on Bronze layer hydration—building a robust ingestion system with partitioning, manifests, and lineage tracking. This iteration shifts focus to **modern transformation patterns**, introducing dbt, DuckDB, and Polars to create a "Rich Silver" layer that pre-computes behavioral attributes before data reaches the cloud warehouse.

---

## Architecture Philosophy: "Rich Silver" Intermediate Modeling

### The Problem This Solves

Traditional pipelines either:
1. Dump raw data into BigQuery and run expensive transformations repeatedly
2. Create simple "cleaned" Silver layers without business logic

This project implements a **third path**: the **Rich Silver pattern**, where:
- **Base Silver** (`/base`): Source-aligned, cleaned, conformed data (1:1 with Bronze)
- **Enriched Silver** (`/enriched`): Business-aligned, attribute-enriched data with pre-computed behavioral signals

### Why This Matters

**Cost Efficiency**: Row-level calculations (e.g., "days since last purchase", churn risk flags) are computed once in Polars and materialized to GCS. BigQuery never re-computes them—it just reads the pre-tagged attributes.

**Performance**: Complex temporal joins (abandoned cart attribution, sessionization) that require expensive WINDOW functions in SQL become 5-line `join_asof` operations in Polars.

**Architectural Flexibility**: Base Silver remains stable and source-aligned for data scientists. Enriched Silver evolves independently as business requirements change.

---

## Data Sources

### Synthetic Data Generator
**Repository**: [ecom_sales_data_generator](https://github.com/G-Schumacher44/ecom_sales_data_generator)

**Entities Generated** (8 tables):
- `orders` & `order_items`: Transaction records with customer/product links
- `returns` & `return_items`: Refund data tied to orders
- `shopping_carts` & `cart_items`: Pre-purchase browsing activity (cart abandonment modeling)
- `product_catalog`: SKU definitions with pricing
- `customers`: User profiles with acquisition channels and loyalty tiers

**Data Characteristics**:
- 6 years of daily-partitioned Parquet files
- Realistic behavioral patterns: cart abandonment, loyalty tier progression, seasonal spikes
- Configurable "messiness" (nulls, typos) for validation testing
- ~20GB total dataset in GCS Bronze bucket

---

## Technical Stack

### Layer Responsibilities

| Layer | Storage | Compute | Responsibility |
|-------|---------|---------|----------------|
| **Bronze** | GCS (`gs://bucket/bronze/`) | Python Generator | Raw ingestion with partitioning, manifests, lineage metadata |
| **Base Silver** | GCS (`gs://bucket/silver/base/`) | dbt + DuckDB | Technical cleaning: deduplication, type casting, schema validation (Pydantic) |
| **Enriched Silver** | GCS (`gs://bucket/silver/enriched/`) | dbt + Polars | Behavioral enrichment: temporal joins, attribution, risk scoring, sessionization |
| **Gold Marts** | BigQuery | dbt-bigquery | Department-specific aggregations and star schemas |

### Orchestration
- **Airflow**: Single DAG managing the full pipeline: `base_silver` → `enriched_silver` → `bigquery_load` → `gold_marts`
- **Bronze layer**: Already populated by [ecom-datalake-exten](https://github.com/G-Schumacher44/ecom-datalake-exten) (static 6-year dataset in GCS)
- **Deployment**: Local Airflow instance (learning/development environment)
- **Future extensibility**: The modular design allows for plugin-based ingestion (e.g., daily batch loads, streaming connectors) without touching transformation logic

---

## Transformation Logic: The Polars "Power Moves"

### 1. Abandoned Cart Attribution (Marketing Mart)
**Business Question**: Which purchases were influenced by abandoned cart recovery emails?

**The Challenge**: Traditional SQL requires cross-joins or expensive WINDOW functions to link every purchase back to preceding cart events.

**The Polars Solution**: `join_asof` performs temporal "as-of" joins—linking each purchase to the most recent abandoned cart within a 48-hour window.

```python
recovered_orders = purchases.join_asof(
    carts,
    on="timestamp",
    by="cust_id",
    strategy="backward",
    tolerance="48h"
)
```

**Output Table**: `attributed_purchases` with `is_recovered` flag and `cart_id` linkage.

---

### 2. Inventory Risk Scoring (Operations Mart)
**Business Question**: Which SKUs are locking up capital and should be rationalized or discounted?

**The Challenge**: Calculating composite risk scores (utilization ratio + return rate + demand velocity) requires row-level weighted logic that's verbose in SQL.

**The Polars Solution**: Modular expression-based scoring with `.with_columns()` chaining.

```python
inventory_df = inventory_df.with_columns([
    (pl.col("sales_volume") / pl.col("stock_level")).alias("utilization_ratio"),
    (pl.col("unit_cost") * pl.col("stock_level")).alias("locked_capital")
]).with_columns(
    attention_score = (utilization_signal + return_signal).clip(0, 1)
).with_columns(
    risk_tier = pl.when(pl.col("attention_score") >= 0.8).then("🔴 High")
                .when(pl.col("attention_score") >= 0.5).then("🟡 Moderate")
                .otherwise("✅ Healthy")
)
```

**Output Table**: `inventory_risk` with daily `attention_score` and `locked_capital` calculations.

---

### 3. Churn Risk Detection (Customer Retention Pipeline)
**Business Question**: Which customers are entering the "Month 1-3 Cliff" danger zone or stuck in Bronze tier with zero repeat purchases?

**The Polars Solution**: Date-based windowing to flag at-risk segments.

```python
retention_df = retention_df.with_columns(
    days_since_first_buy = (pl.lit(current_date) - pl.col("first_purchase_date")).dt.total_days()
).with_columns(
    is_in_danger_zone = (pl.col("days_since_first_buy").is_between(30, 90)) & (pl.col("total_orders") == 1),
    needs_bronze_nudge = (pl.col("loyalty_tier") == "Bronze") & (pl.col("days_since_last_buy") > 14)
)
```

**Output Table**: `customer_retention_signals` with daily churn probability flags.

---

### 4. Sales Velocity Trending (Sales Mart)
**Business Question**: Which products are trending up/down compared to their 7-day average?

**The Polars Solution**: Rolling windows with `rolling_mean()`.

```python
sales_df = sales_df.with_columns(
    velocity_7d_avg = pl.col("quantity").rolling_mean(window_size="7d", by="product_id")
).with_columns(
    trend_signal = pl.when(pl.col("quantity") > pl.col("velocity_7d_avg") * 1.2).then("⬆️ Up")
                    .when(pl.col("quantity") < pl.col("velocity_7d_avg") * 0.8).then("⬇️ Down")
                    .otherwise("➡️ Stable")
)
```

**Output Table**: `sales_velocity` with daily trend indicators.

---

### 5. Regional Financial Localization (Finance Mart)
**Business Question**: What is net revenue by channel and shipping performance?

**The Polars Solution**: Enriched regional financials carry gross + net totals, and shipping economics adds margin context.

**Output Table**: `regional_financials` with gross and net revenue fields.

---

## Airflow DAG Structure

### Single End-to-End DAG: `ecom_silver_to_gold_pipeline`

**Note**: Bronze layer is pre-populated by the [ecom-datalake-exten](https://github.com/G-Schumacher44/ecom-datalake-exten) project. This DAG assumes the static 6-year dataset already exists in GCS Bronze bucket.

```
Task 1: validate_bronze_schema
  └─ Pydantic models validate sample of existing Bronze data

Task 2: base_silver_transform (dbt-duckdb)
  └─ Read: gs://bucket/bronze/*.parquet (pre-populated static dataset)
  └─ Write: gs://bucket/silver/base/*.parquet
  └─ Logic: Deduplication, type casting, column renaming

Task 3: enriched_silver_transform (dbt + Polars Python models)
  └─ Read: gs://bucket/silver/base/*.parquet
  └─ Write: gs://bucket/silver/enriched/*.parquet
  └─ Logic: Attribution, sessionization, risk scoring, velocity calcs

Task 4: load_to_bigquery (GCSToBigQueryOperator)
  └─ Load: gs://bucket/silver/enriched/*.parquet → BigQuery `silver` dataset

Task 5: gold_marts_build (dbt-bigquery)
  └─ Read: BigQuery `silver` dataset
  └─ Write: BigQuery `gold_marts` dataset
  └─ Logic: Departmental aggregations and star schemas
```

---

## Gold Marts

### 1. Finance Mart (`fct_finance_revenue`)
**Consumers**: Finance team, CFO dashboards

**Grain**: Daily revenue by channel

**Key Metrics**:
- `gross_revenue`: Sum of order totals
- `net_revenue`: Net totals from orders
- `shipping_revenue`, `shipping_cost`, `shipping_margin`

**Source Tables**: `int_regional_financials`, `int_shipping_economics`

---

### 2. Marketing Mart (`fct_marketing_attribution`)
**Consumers**: Marketing team, growth analysts

**Grain**: Daily channel performance

**Key Metrics**:
- `channel` (order_channel proxy)
- `recovered_orders` / `total_orders`
- `abandoned_carts` / `converted_carts`
- `avg_time_to_purchase_hours`
- `at_risk_customers` (from retention signals)

**Source Tables**: `int_attributed_purchases`, `int_cart_attribution`, `int_customer_retention_signals`

---

### 3. Sales/Operations Mart (`fct_sales_operations`)
**Consumers**: Sales team, operations, inventory planners

**Grain**: Daily product performance with inventory health signals

**Key Metrics**:
- `sales_velocity_7d`: Rolling 7-day average units sold
- `trend_signal`: (Up/Down/Stable) from Sales Velocity table
- `inventory_risk_tier`: (High/Moderate/Healthy) from Inventory Risk table
- `inventory_quantity`
- `gross_profit`, `net_margin`, `return_rate`

**Source Tables**: `int_sales_velocity`, `int_inventory_risk`, `int_product_performance`

---

## Additional Gold Marts

### `fct_cart_abandonment`
**Grain**: Daily channel-level cart outcomes  
**Source Tables**: `int_cart_attribution`

### `fct_product_profitability`
**Grain**: Daily product profitability  
**Source Tables**: `int_product_performance`

### `fct_customer_segments`
**Grain**: Customer segment snapshots  
**Source Tables**: `int_customer_lifetime_value`

### `fct_daily_dashboard`
**Grain**: Daily KPI rollup  
**Source Tables**: `int_daily_business_metrics`

### `fct_shipping_analysis`
**Grain**: Daily shipping performance by speed/channel  
**Source Tables**: `int_shipping_economics`

---

## Data Quality & Validation

### Pydantic Schema Validation
Before Base Silver transformation, Pydantic models validate:
- `loyalty_tier` is in `['Bronze', 'Silver', 'Gold', 'Platinum']`
- `unit_cost` and `stock_level` are non-negative
- `signup_channel` matches expected values
- Timestamps are valid ISO format

**Purpose**: Ensures the generator hasn't drifted and prevents corrupted data from poisoning Silver layer.

---

## Storage & Lineage Strategy

### GCS Bucket Structure
```
gs://ecom-datalake/
├── bronze/
│   ├── orders/ingest_dt=2020-01-01/*.parquet
│   ├── customers/ingest_dt=2020-01-01/*.parquet
│   └── [7 other tables...]
├── silver/
│   ├── base/
│   │   ├── stg_ecommerce__orders/*.parquet
│   │   ├── stg_ecommerce__customers/*.parquet
│   │   └── [7 other base tables...]
│   └── enriched/
│       ├── int_ecommerce__attributed_purchases/*.parquet
│       ├── int_ecommerce__inventory_risk/*.parquet
│       ├── int_ecommerce__customer_retention_signals/*.parquet
│       ├── int_ecommerce__sales_velocity/*.parquet
│       └── int_ecommerce__regional_financials/*.parquet
```

### Lineage Preservation
- **Bronze → Base Silver**: 1:1 mapping preserves source structure
- **Base Silver → Enriched Silver**: Modular Polars transformations create new attribute tables
- **Enriched Silver → Gold**: BigQuery dbt models aggregate enriched attributes

**Why This Matters**: If Polars logic has a bug, re-run only the `enriched_silver_transform` task without re-processing Bronze → Base Silver.

---

## BigQuery Strategy

### Loading Pattern
**Method**: `GCSToBigQueryOperator` loads Parquet directly from `gs://bucket/silver/enriched/`

**Dataset Structure**:
- `silver_base` dataset: Mirrors Base Silver tables (cleaned source entities)
- `silver_enriched` dataset: Contains enriched attribute tables (separate, joinable tables)
- `gold_marts` dataset: Departmental mart tables

**Enriched Silver Design Philosophy**: Keep enriched tables **separate and modular** rather than creating wide tables:
- `attributed_purchases`: Cart attribution signals
- `inventory_risk`: SKU risk scores
- `customer_retention_signals`: Churn flags
- `sales_velocity`: Product trend indicators
- `regional_financials`: Tax/revenue calculations

**Why Separate Tables?**
- **Flexibility**: Gold Marts can join only the enriched tables they need
- **Maintainability**: Update attribution logic without touching inventory logic
- **Reusability**: Data scientists can use individual enriched tables for ML features
- **Clear ownership**: Each enriched table has a single, well-defined purpose

### Cost Optimization
By pre-computing row-level attributes in Polars:
- **Avoided**: BigQuery scanning 20GB daily to calculate `days_since_last_purchase`
- **Avoided**: Expensive WINDOW functions for rolling averages
- **Result**: Gold Mart queries become simple `SELECT SUM(...)` aggregations

**Portfolio Talking Point**: "By shifting computational complexity left into the Rich Silver layer, I reduced BigQuery scan costs by an estimated 40% while ensuring all downstream marts share a single, consistent definition of customer behavior."

---

## Portfolio Narrative

### From Audit to Automation
This project operationalizes insights from analytical audits:
- **Inventory Audit**: Identified $19.1M in locked capital → Built daily `inventory_risk` pipeline
- **Retention Analysis**: Found Month 1-3 Cliff → Built `customer_retention_signals` churn detector
- **Attribution Study**: Proved cart recovery impact → Built `attributed_purchases` engine

### The "Full Circle" Story
1. **Generate**: Synthetic data with realistic behavioral patterns ([ecom_sales_data_generator](https://github.com/G-Schumacher44/ecom_sales_data_generator))
2. **Ingest**: Bronze layer with partitioning and lineage ([ecom-datalake-exten](https://github.com/G-Schumacher44/ecom-datalake-exten) - Iteration 1, complete)
3. **Clean**: Base Silver with validation and standardization (dbt + DuckDB) - **This project**
4. **Enrich**: Rich Silver with behavioral attributes (dbt + Polars) - **This project**
5. **Hydrate**: BigQuery Gold Marts for stakeholder consumption - **This project**
6. **Operationalize**: Airflow DAG orchestrates Silver → Gold transformation - **This project**

---

## Modern Stack Justification (2026 Edition)

### Why DuckDB for Base Silver?
- In-process OLAP engine—no warehouse spin-up costs
- Native Parquet support with predicate pushdown
- Can read directly from GCS via `httpfs` extension
- Perfect for 20GB-scale technical cleaning

### Why Polars for Enriched Silver?
- **10-100x faster** than Pandas for 20GB datasets
- Lazy evaluation with query optimization
- Native support for temporal joins (`join_asof`), rolling windows, and expression chaining
- Rust-based—production-grade performance without Spark overhead

### Why BigQuery for Gold?
- Massive-scale aggregations (when Enriched Silver grows beyond local compute)
- Native BI tool integrations (Looker, Tableau)
- Separation of concerns: Local compute for transformations, cloud for serving

---

## Key Learnings & Trade-offs

### What I'm Learning
- **dbt Intermediate Modeling**: Separating `stg_` (Base) from `int_` (Enriched) layers
- **Polars Expression API**: Building complex row-level logic with readable Python
- **Cost-Conscious Architecture**: Doing expensive work locally before cloud upload
- **Idempotent Pipelines**: Each DAG task can re-run independently without full reprocessing

### Architectural Trade-offs
- **Local vs. Cloud Compute**: This works for 20GB. At 200GB+, would need distributed Polars (via Dask) or Spark
- **Static vs. Incremental**: Current design processes full dataset, but architecture supports incremental patterns:
  - Plugin-based ingestion could add daily batch loads or streaming data
  - Date-partitioned Bronze structure already supports incremental processing
  - dbt incremental models could process only new/changed data
- **Complexity vs. Flexibility**: More pipeline stages = more to maintain, but this modularity gives flexibility to:
  - Re-run only failed stages without full reprocessing
  - Swap out tools (e.g., replace Polars with Spark) without touching other layers
  - Iterate on business logic (Enriched Silver) independently from data cleaning (Base Silver)
  - Debug issues at specific layers rather than in monolithic transformations
  - Add new enriched tables without modifying existing ones

---

## Related Repositories

1. **[ecom_sales_data_generator](https://github.com/G-Schumacher44/ecom_sales_data_generator)**: Synthetic data engine (8 tables, 6 years)
2. **[ecom-datalake-exten](https://github.com/G-Schumacher44/ecom-datalake-exten)**: Bronze hydrator (Iteration 1)
3. **ecom-datalake-silver** (local): Lightweight validation framework (Great Expectations alternative)

---

## Success Metrics

This project is successful if it demonstrates:
1. ✅ **Modern tooling proficiency**: dbt, DuckDB, Polars, Airflow, BigQuery
2. ✅ **Architectural thinking**: Rich Silver pattern, lineage preservation, modular DAG design
3. ✅ **Cost awareness**: Local compute for transformations, cloud for serving
4. ✅ **Business impact**: Operationalized analytics (churn prevention, inventory optimization, attribution)
5. ✅ **Production readiness**: Pydantic validation, idempotent tasks, error handling

---

## Next Steps (Original Planning Roadmap)

> **📝 Implementation Note**: These items have been completed. This section is preserved to show the original project roadmap and demonstrate planning-to-execution progression.

### ✅ Phase 1: Core Pipeline (MVP) - COMPLETED

- ✅ Implement Pydantic validation schemas for all 8 Bronze tables
- ✅ Build dbt Base Silver models (deduplication, type casting)
- ✅ Develop Polars enrichment logic (attribution, risk scoring, sessionization)
- ✅ Configure Airflow DAG with GCS/BigQuery operators
- ✅ Create dbt Gold Mart models (8 fact tables - exceeded original 3-mart target)

### ✅ Phase 2: Observability & Quality - COMPLETED

- ✅ Build monitoring/alerting for pipeline failures (structured observability with audit trails)
- ✅ Document cost savings analysis (documented in architecture docs)
- ✅ Add data quality dashboard (three-layer validation framework: Bronze, Silver, Enriched)

### 🚧 Phase 3: Extensibility (Future Enhancements)

- [ ] Design plugin architecture for incremental ingestion
- [ ] Explore streaming connector (e.g., Kafka → Bronze append pattern)
- [ ] Implement dbt incremental models for changed-data-only processing
- [ ] Add custom enriched tables based on new business requirements

**Current Status & Next Steps**: See [README.md - Future Enhancements](../../README.md#-future-enhancements) for the active roadmap.

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
