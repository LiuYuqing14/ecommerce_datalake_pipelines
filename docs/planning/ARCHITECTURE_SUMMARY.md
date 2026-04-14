# Architecture Summary: Local-First Hybrid Pattern

> **📌 HISTORICAL DOCUMENT - Original Architecture Overview (Early Planning)**
>
> This document represents the initial architectural vision for the local-first hybrid pattern.
> For **current architecture documentation**, see:
> - [ARCHITECTURE.md](../resources/ARCHITECTURE.md) - Complete current architecture
> - [SPEC_OVERVIEW.md](../resources/SPEC_OVERVIEW.md) - Spec-driven orchestration pattern
> - [CONFIG_STRATEGY.md](../resources/CONFIG_STRATEGY.md) - Configuration architecture
>
> **Status**: ✅ Implemented - Core pattern validated and enhanced with spec-driven orchestration

## Overview

This pipeline uses a **local-first hybrid architecture** where heavy compute happens locally (DuckDB + Polars) and only final results are loaded to BigQuery for serving.

## Data Flow

```
Bronze (GCS parquet)
    ↓ [dbt-duckdb, local DuckDB]
Base Silver (GCS parquet)
    ↓ [Polars runners, local Python]
Enriched Silver (GCS parquet)
    ↓ [bq load, Airflow task]
Enriched Silver (BigQuery tables)
    ↓ [dbt-bigquery SQL, in BigQuery]
Gold Marts (BigQuery tables)
```

## Component Responsibilities

### 1. dbt_duckdb (Base Silver)
- **Purpose**: Data cleaning and schema enforcement
- **Runs**: Locally (or on Airflow worker)
- **Reads from**: Bronze parquet in GCS via DuckDB httpfs
- **Writes to**: Base Silver parquet in GCS
- **8 models** (one per Bronze table): `stg_ecommerce__orders`, `stg_ecommerce__customers`, etc.

### 2. src/transforms/ (Pure Polars Logic)
- **Purpose**: Business logic for Enriched Silver (cart attribution, churn, velocity, etc.)
- **Design**: Pure functions, no I/O, fully testable
- **Example**: `compute_cart_attribution(carts: pl.DataFrame, orders: pl.DataFrame) -> pl.DataFrame`

### 3. src/runners/ (I/O Wrappers)
- **Purpose**: Read Base Silver from GCS, call transforms, write Enriched Silver to GCS
- **Design**: Thin wrappers around src/transforms/ functions
- **Example**: `run_cart_attribution(base_silver_path, output_path, ingest_dt)`
- **5 runners** (one per Enriched table)

### 4. Airflow DAG (Orchestration)
- **Phase 1**: Base Silver TaskGroup (8 parallel dbt-duckdb tasks)
- **Phase 2**: Enriched Silver TaskGroup (5 parallel PythonOperator tasks calling runners)
- **Phase 3**: Load to BigQuery TaskGroup (5 parallel `bq load` tasks)
- **Phase 4**: Gold Marts (dbt-bigquery SQL models)

### 5. dbt_bigquery (Gold Layer Only)
- **Purpose**: SQL aggregations for BI consumption
- **Runs**: In BigQuery
- **Reads from**: Enriched Silver BigQuery tables
- **Writes to**: Gold Marts BigQuery tables
- **No Python models** - pure SQL only

## Key Design Decisions

1. **No dbt Python models for Enriched Silver**
   - dbt Python models are designed to run IN BigQuery
   - We want to run Polars locally to avoid BQ compute costs
   - Solution: Pure Polars runners called directly by Airflow

2. **GCS as persistence layer**
   - Both Base and Enriched Silver live as parquet in GCS
   - BigQuery is just a "view" for SQL consumption
   - Allows for re-processing without re-computing expensive transforms

3. **Table-level parallelism**
   - 8 Base Silver tasks run in parallel (largest: cart_items 9.6GB)
   - 5 Enriched Silver tasks run in parallel (largest: attributed_purchases 3.7GB)
   - Peak memory: ~9.6GB (fits 16GB laptop)

4. **Cost optimization**
   - All heavy compute is local (free)
   - BigQuery only used for:
     - Storing Enriched Silver (storage quota, minimal cost)
     - Running Gold Marts (lightweight SQL aggregations)
   - Total estimated cost: ~$2-3/month

## File Structure

```
src/
├── transforms/           # Pure Polars logic (no I/O)
│   ├── cart_attribution.py
│   ├── inventory_risk.py
│   ├── churn_detection.py
│   ├── sales_velocity.py
│   └── regional_financials.py
├── runners/              # I/O wrappers (read GCS → transform → write GCS)
│   └── enriched/
│       ├── commerce.py
│       ├── customer.py
│       ├── finance.py
│       ├── ops.py
│       └── shared.py
├── validation/           # Pydantic schemas
└── observability/        # Audit logging

dbt_duckdb/
└── models/
    └── base_silver/      # 8 SQL models for cleaning
        ├── stg_ecommerce__orders.sql
        └── ...

dbt_bigquery/
└── models/
    └── gold_marts/       # SQL models only (no Python)
        ├── fct_finance_revenue.sql
        └── ...

airflow/dags/
└── ecom_silver_to_gold.py  # Main orchestration DAG
```

## Why This Works

- **Local-first**: No BigQuery compute costs for heavy transforms
- **Production-realistic**: Simulates on-prem → cloud hybrid architectures
- **Testable**: Pure Polars logic in src/transforms/ is fully unit-testable
- **Scalable**: Table-level parallelism allows processing 20GB on laptop
- **Student-friendly**: ~$2-3/month total cost

## Next Steps

1. Implement Base Silver dbt models (data cleaning)
2. Implement Polars transform logic in src/transforms/
3. Add unit tests for Polars transforms
4. Test end-to-end: Airflow → dbt → Polars → GCS → BigQuery → Gold

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
