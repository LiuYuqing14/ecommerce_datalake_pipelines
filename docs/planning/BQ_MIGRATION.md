# BigQuery Migration (Downstream Readiness)

## Purpose
Define how silver data lands in BigQuery for analytics use (Looker/BI).
This plan reflects production-grade patterns for analytics readiness.

## Options

### Option A: External Tables (GCS Parquet)
- Pros: fast to set up, no load jobs.
- Cons: slower queries, limited performance tuning.
- Use when: early exploration or low query volume.

### Option B: Native BQ Tables (Load Jobs)
- Pros: faster queries, better performance and cost control.
- Cons: requires scheduled load or streaming jobs.
- Use when: analytics team needs consistent performance.

## Recommended Path
1) Start with external tables for quick access.
2) Move to native tables once schema is stable and usage grows.

## Default Dataset
- Dataset: `silver`
- Location: `US`

## Partitioning and Clustering
- Partition by business date (e.g., `order_dt`, `return_dt`).
- Cluster by high-cardinality filters (e.g., `customer_id`, `order_id`).
- For reference tables (product_catalog), consider non-partitioned or `ingest_dt`.

## Data Refresh Strategy
- Daily partitions appended for current data.
- Backfill runs in weekly batches.
- Keep a `run_id` and `schema_version` column for traceability.

## Metadata (Audit) Table
Add a lightweight audit table for pipeline observability.

Suggested schema:
- run_id (STRING)
- table_name (STRING)
- ingest_dt (DATE)
- partition_key (STRING)
- partition_value (STRING)
- schema_version (STRING)
- input_rows (INT64)
- output_rows (INT64)
- reject_rows (INT64)
- started_at (TIMESTAMP)
- ended_at (TIMESTAMP)
- duration_sec (FLOAT64)
- quality_failures (ARRAY<STRING>)

Load strategy:
- Append per run from audit JSON logs.
- Optional view for daily SLA status by table.
 - Local dev: persist audit records to a DuckDB table instead of BigQuery.

## Validation Before Publish
- Row count drift check vs prior day/7-day average.
- Schema version and required columns check.
- FK integrity checks on critical tables (orders -> customers).

## Governance and Access
- Use dataset-level IAM roles for Business Intelligence and Data Science.
- Service accounts used by Airflow/dbt have least privilege.
- Audit metadata table is restricted to Data Engineering by default.

## Example Setup

### Create Dataset
```bash
bq --location=US mk -d ecom_silver
```

### External Table (orders)
```sql
CREATE OR REPLACE EXTERNAL TABLE `ecom_silver.orders_ext`
WITH PARTITION COLUMNS (order_dt DATE)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://<silver-bucket>/<prefix>/orders/order_dt=*/*.parquet']
);
```

### Native Table Load (orders)
```bash
bq load \
  --source_format=PARQUET \
  --time_partitioning_field=order_dt \
  ecom_silver.orders \
  gs://<silver-bucket>/<prefix>/orders/order_dt=*/part-*.parquet
```

## Open Decisions
- BQ dataset name and location.
- External vs native table choice (initially external is fine).
- Access control model for analytics users.

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
