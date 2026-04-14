# Silver Transform Framework (Hybrid)

> **📌 HISTORICAL DOCUMENT - Original Framework Design (Pre-Implementation)**
>
> This document represents the initial framework design for the hybrid dbt + Polars transformation approach.
> For **current implementation and patterns**, see:
> - [ARCHITECTURE.md](../resources/ARCHITECTURE.md) - Current system architecture
> - [SPEC_OVERVIEW.md](../resources/SPEC_OVERVIEW.md) - Spec-driven orchestration
> - [VALIDATION_GUIDE.md](../resources/VALIDATION_GUIDE.md) - Three-layer validation framework
>
> **Status**: ✅ Implemented - Framework executed with production enhancements

## Purpose
Define how Base Silver and Enriched Silver transformations are organized and executed.
This document reflects the hybrid dbt + Polars approach used in this repo.

## Design Goals
- Keep orchestration in Airflow with clear task boundaries.
- Use dbt-duckdb for Base Silver cleaning and conformance.
- Use reusable Polars modules for enrichment logic, called by Python runner scripts.
- Keep data quality checks visible and traceable.
- All heavy compute runs locally; BigQuery only for Gold SQL aggregations.

## High-Level Flow
1) Validate bronze partition(s) and required schema.
2) Base Silver (dbt-duckdb) cleans and standardizes bronze tables, writes to GCS.
3) Enriched Silver (Polars runners in `src/runners/`) reads Base Silver from GCS, applies `src/transforms/`, writes to GCS.
4) Load enriched parquet from GCS to BigQuery `silver` dataset.
5) Build Gold marts (dbt-bigquery SQL models only).
6) Emit audit metrics per table and partition.

## Module Layout
```
src/
  transforms/              # Pure Polars logic (no I/O)
    cart_attribution.py
    inventory_risk.py
    churn_detection.py
    sales_velocity.py
    regional_financials.py
  runners/                 # I/O wrappers (read GCS → transform → write GCS)
    enriched/
      commerce.py
      customer.py
      finance.py
      ops.py
      shared.py
  validation/
    base_silver_schemas.py
  observability/
    audit.py

dbt_duckdb/
  models/base_silver/
    stg_ecommerce__*.sql

dbt_bigquery/
  models/gold_marts/       # SQL only (no Python models)
    fct_*.sql
```

## Base Silver (dbt-duckdb)
- Bronze tables are referenced as external Parquet sources.
- Base Silver models apply schema casting, deduplication, and integrity rules.
- Output tables remain 1:1 with bronze entities.

## Enriched Silver (Polars Runners)
- Domain runner scripts in `src/runners/enriched/` read Base Silver parquet from GCS.
- Enrichment logic lives in `src/transforms/` (pure functions, no I/O).
- Runners write Enriched Silver parquet to GCS.
- Output tables are business-aligned (attribution, risk, retention, velocity).
- Called directly by Airflow PythonOperator tasks (not dbt).

## Validation and Quality
- Pydantic schemas in `src/validation/schemas.py` validate key fields.
- SLA and quality thresholds live in `docs/planning/SLA_AND_QUALITY.md`.
- Audit logs written via `src/observability/audit.py`.

## Outputs
- Base Silver parquet: `gs://<silver-bucket>/silver/ecom/base/<table>/...`
- Enriched Silver parquet: `gs://<silver-bucket>/silver/ecom/enriched/<table>/...`
- Audit logs: `gs://<silver-bucket>/silver/ecom/_audit/run_id=.../summary.json`

## Notes
- Keep table list aligned with `docs/resources/DATA_CONTRACT.md`.
- Keep quality checks aligned with `docs/planning/SLA_AND_QUALITY.md`.
- dbt lineage covers both Base and Enriched Silver outputs.

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
