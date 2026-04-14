# SLAs and Data Quality Checks

## Purpose
Define availability expectations and quality checks for the silver layer.
This is a lightweight SLA/quality spec for the simulated pipeline.

## SLAs (Draft)
- Backfill availability: full historical run completes within the table-level SLA window.
- Incremental availability (future): daily silver availability by 07:00 local time for prior day partitions.
- Data quality checks must pass before publishing silver partitions.

## Quality Checks (General)
- Required columns present in each table.
- Primary keys are non-null and unique.
- Foreign keys reference existing parent records.
- Date/time columns parseable; invalid values quarantined or nullified.
- Numeric values that represent counts or prices are non-negative.

## Table-Specific Checks

### orders
- order_id unique and non-null.
- customer_id exists in customers.
- order_date parseable.
- net_total <= gross_total.

### order_items
- (order_id, product_id) unique per partition.
- order_id exists in orders.
- quantity > 0, unit_price >= 0.

### customers
- customer_id unique and non-null.
- email normalized to lowercase; invalid emails flagged.

### product_catalog
- product_id unique and non-null.
- unit_price >= 0, cost_price >= 0.

### shopping_carts
- cart_id unique and non-null.
- customer_id exists in customers.

### cart_items
- cart_item_id unique and non-null.
- cart_id exists in shopping_carts.
- product_id exists in product_catalog.

### returns
- return_id unique and non-null.
- order_id exists in orders.
- customer_id exists in customers.

### return_items
- return_item_id unique and non-null.
- return_id exists in returns.
- order_id exists in orders.
- product_id exists in product_catalog.

## Table-Specific SLAs (Draft)

### orders
- Freshness: silver available by 07:00 daily for prior day partition.
- Completeness: row count within +/- 10% of 7-day average.
- Integrity: `order_id` non-null >= 99.9%; `net_total <= gross_total`.
- FK: `customer_id` present in customers >= 99%.

### order_items
- Freshness: silver available by 07:00 daily for prior day partition.
- Completeness: items per order within +/- 15% of 7-day average.
- Integrity: `quantity > 0`; `unit_price >= 0`.

### customers
- Freshness: silver available by 09:00 daily for prior day partition.
- Completeness: `email` non-null >= 98%.
- Integrity: `customer_id` unique and non-null >= 99.9%.

### product_catalog
- Freshness: silver available by 09:00 daily (or weekly refresh if desired).
- Completeness: row count within +/- 20% of 30-day average.
- Integrity: `unit_price >= 0`, `inventory_quantity >= 0`.

### shopping_carts
- Freshness: silver available by 09:00 daily for prior day partition.
- Completeness: row count within +/- 15% of 7-day average.
- Integrity: `cart_id` unique and non-null >= 99.5%.

### cart_items
- Freshness: silver available by 09:00 daily for prior day partition.
- Completeness: row count within +/- 15% of 7-day average.
- Integrity: `quantity > 0`, `unit_price >= 0`.

### returns
- Freshness: silver available by 10:00 daily for prior day partition.
- Completeness: return rate within +/- 20% of 30-day average.
- Integrity: `return_id` non-null >= 99.5%; `refunded_amount >= 0`.

### return_items
- Freshness: silver available by 10:00 daily for prior day partition.
- Completeness: return items per return within +/- 20% of 30-day average.
- Integrity: `quantity_returned > 0`, `refunded_amount >= 0`.

## Failure Handling
- **Fail Fast:** The pipeline now strictly enforces quality gates. If a critical validation check fails (e.g., missing manifests, SLA breach), the DAG task returns a non-zero exit code, stopping the pipeline immediately in Production environments.
- **Quarantine:** Invalid rows (FK violations, type errors) are automatically routed to quarantine tables.
- **Configuration:** All thresholds and checks are defined in `config/config.yml` rather than hardcoded in scripts.

## Processing Time SLAs (Table-Level)

### Base Silver (dbt-duckdb)

| Table | Size | Target Time | Memory |
| --- | --- | --- | --- |
| cart_items | 9.6GB | 15-20 min | 9.6GB |
| shopping_carts | 3.0GB | 8-12 min | 3.0GB |
| customers | 2.2GB | 1-2 min* | 2.2GB |
| order_items | 1.3GB | 4-6 min | 1.3GB |
| orders | 717MB | 2-3 min | 717MB |
| product_catalog | 80MB | 10-20 sec* | 80MB |
| return_items | 45MB | 15-30 sec | 45MB |
| returns | 33MB | 15-30 sec | 33MB |

*\*Optimized via local manifest generation and batched GCS rsync.*

**Total Base Silver**: 15-25 minutes (parallel execution)

**Peak memory**: ~9.6GB (cart_items table)

### Enriched Silver (Polars runners)

| Table | Inputs | Target Time | Memory |
| --- | --- | --- | --- |
| int_attributed_purchases | carts (3GB), orders (717MB) | 8-12 min | 3.7GB |
| int_inventory_risk | products (80MB), order_items (1.3GB), returns (33MB) | 5-8 min | 1.4GB |
| int_customer_retention_signals | customers (2.2GB), orders (717MB) | 6-10 min | 2.9GB |
| int_sales_velocity | orders (717MB), order_items (1.3GB) | 6-10 min | 2.0GB |
| int_regional_financials | orders (717MB), customers (2.2GB) | 6-10 min | 2.9GB |

**Total Enriched Silver**: 30-50 minutes (parallel execution)

**Peak memory**: ~3.7GB (attributed_purchases: carts + orders)

### Overall Pipeline SLA

- **Total processing time**: 45-75 minutes (Base + Enriched in sequence)
- **Peak memory usage**: ~9.6GB (Base Silver cart_items table)
- **Temp storage**: <10GB (ephemeral `/tmp/` files deleted after upload)
- **GCS egress cost**: ~$2-3/month for read/write operations

## Monitoring Expectations

- Audit logs are produced per table and partition.
- SLA dashboards consume the audit table in BigQuery.
- Alert when freshness or integrity thresholds are breached.
- Alert when processing time exceeds target by >25%.

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
