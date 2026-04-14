# Operations Runbook

This runbook covers common failure scenarios and their resolution for the ecom-datalake-pipelines system.

---

## Quick Start - Local Demo

**2-minute demo workflow:**

```bash
# 1. Unzip Bronze sample data
unzip samples/bronze_samples.zip -d samples/

# 2. Run fast demo (pre-cooked dims + single-day processing)
ecomlake local demo-fast

# 3. Verify outputs
ls data/silver/base/orders/ingestion_dt=2023-01-01
ls data/silver/enriched/int_cart_attribution/cart_dt=2023-01-01
cat docs/validation_reports/ENRICHED_QUALITY_2023-01-01.md
```

**Expected Results:**

- ✅ Silver: 6/6 fact tables passing validation
- ✅ Enriched: 10/10 business tables with data
- ✅ dbt: 147 data quality tests pass

**What the demo validates:**

- Bronze → Silver transformation via dbt
- Dimension snapshot loading from pre-cooked archive
- Enriched layer transformations with dims dependencies
- Returns data flow through the pipeline
- Quality gates at each layer

---

## Table of Contents

1. [Bronze Ingestion Failures](#1-bronze-ingestion-failures)
2. [Dimension Snapshot Failures](#2-dimension-snapshot-failures)
3. [Base Silver dbt Failures](#3-base-silver-dbt-failures)
4. [Enriched Silver Validation Failures](#4-enriched-silver-validation-failures)
5. [Quarantine Data Management](#5-quarantine-data-management)
6. [Airflow DAG Failures](#6-airflow-dag-failures)
7. [GCS Connectivity Issues](#7-gcs-connectivity-issues)
8. [Docker & Environment Issues](#8-docker--environment-issues)

---

## 1. Bronze Ingestion Failures

### Symptoms
- Missing `ingest_dt` partitions in Bronze layer
- Manifest file missing or row count mismatch
- Parquet files with invalid magic bytes

### Diagnosis

```bash
# Check Bronze partition exists
ls -la data/bronze/orders/ingest_dt=2025-01-15/

# Validate Parquet file integrity
python -c "
import pyarrow.parquet as pq
pf = pq.ParquetFile('data/bronze/orders/ingest_dt=2025-01-15/part-0.parquet')
print(f'Rows: {pf.metadata.num_rows}')
print(f'Schema: {pf.schema_arrow}')
"

# Check manifest
cat data/bronze/orders/ingest_dt=2025-01-15/_manifest.json
```

### Resolution

**Missing partition:**
1. Re-run the upstream ingestion job for the missing date
2. Verify source system data availability
3. Check ingestion logs for errors

**Corrupt Parquet file:**
1. Delete the corrupt file: `rm data/bronze/orders/ingest_dt=2025-01-15/part-0.parquet`
2. Re-run ingestion for that partition
3. If persists, check source data quality

**Manifest mismatch:**
1. Regenerate manifest: Run Bronze validation to create new manifest
2. If row counts don't match, investigate source system

### Prevention
- Enable Bronze quality checks in Airflow DAG
- Set up alerts for missing partitions
- Monitor ingestion job success rates

---

## 2. Dimension Snapshot Failures

### Symptoms
- dbt models fail with "No files found that match the pattern" errors
- Foreign key validation failures in Base Silver
- Missing dimension tables (product_catalog, customer_snapshot)
- CI/CD pipeline fails on `ecomlake local silver` step

### Diagnosis

```bash
# Check if dimension snapshots exist
ls -la data/silver/dims/product_catalog/
ls -la data/silver/dims/customer_snapshot/

# Verify SILVER_DIMS_PATH is set
echo $SILVER_DIMS_PATH

# Check dimension snapshot creation
ecomlake dim run --run-date 2024-01-03
```

### Resolution

**Missing dimension snapshots:**
1. **Root cause**: Base Silver dbt models perform FK validation by reading dimension snapshots, but those snapshots must be created first.
2. **Correct execution order**:
   ```bash
   # Step 1: Create dimension snapshots from Bronze
   ecomlake local dims

   # Step 2: Run Base Silver (reads dims for FK validation)
   ecomlake local silver

   # Step 3: Run Enriched Silver
   ecomlake local enriched
   ```
3. **In CI/CD**: Ensure `SILVER_DIMS_PATH` environment variable is set:
   ```yaml
   env:
     SILVER_DIMS_PATH: ${{ github.workspace }}/data/silver/dims
   ```

**Stale dimension snapshots:**
1. Dimension snapshots are daily snapshots created from Bronze data
2. If Bronze data changes but dims aren't refreshed, FK validation will fail
3. Re-run: `ecomlake local dims` to refresh snapshots

**Performance benefit:**
- Dimension snapshots provide 60% performance improvement
- Avoids re-reading Bronze data for every Base Silver run
- Trade-off: Snapshots must be created before Base Silver

### Prevention
- Always run `ecomlake local dims` before `ecomlake local silver`
- In Airflow DAGs, ensure dim snapshot tasks are upstream dependencies
- Monitor dimension snapshot freshness (should match `ingest_dt`)

---

## 3. Base Silver dbt Failures

### Symptoms
- dbt run exits with non-zero code
- Quarantine tables have high row counts
- Foreign key validation failures
- Duplicate primary key errors

### Diagnosis

```bash
# Run dbt with verbose output
cd dbt_duckdb && dbt run --target local --full-refresh 2>&1 | tee dbt_run.log

# Check specific model
dbt run --select stg_orders --target local

# Inspect quarantine table
python -c "
import polars as pl
df = pl.read_parquet('data/silver/base/quarantine/orders/*.parquet')
print(df.head(10))
print(f'Quarantine rows: {df.height}')
"

# Check Silver validation report
ecomlake silver validate --silver-path data/silver/base --env local
```

### Resolution

**High quarantine rate (>5%):**
1. Inspect quarantine records for patterns:
   ```python
   import polars as pl
   df = pl.read_parquet('data/silver/base/quarantine/orders/*.parquet')
   # Check failure reasons
   print(df.group_by('_quarantine_reason').len())
   ```
2. Common causes:
   - **Null primary keys**: Check Bronze data quality
   - **Invalid foreign keys**: Ensure reference tables loaded first
   - **Type casting failures**: Review Bronze schema changes
3. Fix upstream data or adjust validation rules in dbt models

**Duplicate primary keys:**
1. Check if source system has duplicates
2. Review deduplication logic in `stg_*` models
3. Ensure `ROW_NUMBER()` window function uses correct ordering

**Foreign key failures:**
1. Verify reference table exists and is populated
2. Check join conditions in `int_*` models
3. Consider adding missing reference data

### Prevention
- Run `dbt test` after each `dbt run`
- Set `max_quarantine_pct` threshold in config
- Monitor quarantine table growth over time

---

## 4. Enriched Silver Validation Failures

### Symptoms
- Validation status: FAIL or WARN
- Sanity check violations (negative values, rates > 1)
- Semantic check failures (business rule violations)
- Missing enriched tables

### Diagnosis

```bash
# In prod/prod-sim, validation runs post-sync and reads GCS outputs.
# Confirm the partition exists before validating.
gcloud storage ls gs://<silver-bucket>/data/silver/enriched/**/ingest_dt=YYYY-MM-DD/

# Run enriched validation
ecomlake enriched validate --enriched-path data/silver/enriched --env local

# Check specific table metrics
python -c "
import polars as pl
df = pl.read_parquet('data/silver/enriched/int_customer_lifetime_value/*.parquet')
print(df.describe())

# Check for negative CLV
negatives = df.filter(pl.col('net_clv') < 0)
print(f'Negative CLV records: {negatives.height}')
"

# Review validation report
cat data/metrics/enriched_quality_*.json | jq .
```

### Resolution

**Sanity check: negative values**
1. Identify affected records:
   ```python
   df.filter(pl.col('gross_revenue') < 0).select(['product_id', 'gross_revenue'])
   ```
2. Trace back to source data in Base Silver
3. Common causes:
   - Incorrect aggregation logic
   - Sign errors in refund calculations
   - Missing null handling

**Semantic check: `net_clv_matches_components`**
1. This means `total_spent - total_refunded != net_clv`
2. Check for floating-point precision issues
3. Review CLV calculation in `src/transforms/customer_lifetime_value.py`

**Semantic check: `return_rate_le_one`**
1. Returns exceed sales for a product
2. Check if returns are being double-counted
3. Verify join conditions in product_performance transform

**Missing enriched table:**
1. Verify Base Silver tables exist
2. Check runner logs for errors
3. Advanced: run enriched for a single date, then inspect customer output table
   `ecomlake enriched run --ingest-dt 2024-01-03`

### Prevention
- Set `enriched_ratio_epsilon` for floating-point tolerance
- Review semantic check expressions in `config/config.yml`
- Add unit tests for edge cases in transforms

---

## 5. Quarantine Data Management

### What is Quarantined Data?

Records that fail validation checks during Silver processing are written to `quarantine/` subdirectories instead of main Silver tables. This **prevents bad data from propagating to Gold marts** while preserving raw inputs for debugging and recovery.

### Quarantine Architecture

**Location Pattern:**
```
{silver_base_path}/quarantine/{table_name}/ingest_dt={YYYY-MM-DD}/*.parquet
```

**Example:**
```
data/silver/base/quarantine/orders/ingest_dt=2025-01-15/part-0.parquet
```

**Metadata:** Each quarantined record includes:
- `_quarantine_reason`: Validation failure description
- `_quarantine_timestamp`: When record was quarantined
- All original columns from Bronze layer

### Retention Policy

| Environment | TTL | Review Cadence | Alert Threshold |
|-------------|-----|----------------|-----------------|
| **Production** | 30 days | Weekly | >5% of valid records |
| **Development** | 7 days | Manual | >10% |
| **Local** | Manual cleanup | N/A | None |

### Common Quarantine Reasons

1. **Null Primary Keys**
   - **Reason:** `customer_id IS NULL` or `order_id IS NULL`
   - **Impact:** Records cannot be uniquely identified
   - **Fix:** Investigate Bronze data quality; contact source system team

2. **Invalid Foreign Keys**
   - **Reason:** `product_id` not in `product_catalog` dimension
   - **Impact:** Cannot join to reference data
   - **Fix:** Ensure dimension snapshots are current; check for orphaned records

3. **Type Casting Failures**
   - **Reason:** Cannot cast `order_total` from VARCHAR to DECIMAL
   - **Impact:** Numeric operations fail
   - **Fix:** Review Bronze schema changes; add explicit type handling

4. **Out-of-Range Values**
   - **Reason:** `order_date` in future or `price < 0`
   - **Impact:** Violates business logic
   - **Fix:** Check source system data entry validation

5. **Duplicate Primary Keys**
   - **Reason:** Multiple records with same PK after deduplication
   - **Impact:** Cannot load to Silver (unique constraint)
   - **Fix:** Review deduplication logic in `stg_*` models

### Diagnosis

**Check Quarantine Size:**
```bash
# Generate quarantine breakdown (see report)
ecomlake silver validate --silver-path data/silver/base --quarantine-path data/silver/base/quarantine

# Manual check (local)
find data/silver/base/quarantine -name "*.parquet" -exec wc -l {} \; | awk '{sum+=$1} END {print sum}'

# GCS (production)
gsutil du -sh gs://${GCS_BUCKET}/silver/base/quarantine/
```

**Inspect Quarantine Records:**
```python
import polars as pl

# Read quarantined orders
df = pl.read_parquet('data/silver/base/quarantine/orders/ingest_dt=2025-01-15/*.parquet')

# Group by failure reason
df.group_by('_quarantine_reason').agg([
    pl.count().alias('record_count'),
    pl.col('order_id').first().alias('example_order_id')
])

# Export sample for analysis
df.head(100).write_csv('quarantine_sample.csv')
```

**Check Quarantine Rate:**
```bash
# Run Silver validation
ecomlake silver validate --silver-path data/silver/base --env local

# Check metrics output
cat data/metrics/silver_quality_*.json | jq '.table_metrics[] | select(.table_name=="orders") | .quarantine_pct'
```

### Resolution

**High Quarantine Rate (>5%):**

1. **Identify Pattern:**
   ```python
   import polars as pl
   df = pl.read_parquet('data/silver/base/quarantine/orders/**/*.parquet')

   # Most common failure reasons
   reasons = df.group_by('_quarantine_reason').len().sort('len', descending=True)
   print(reasons)
   ```

2. **Fix Upstream:**
   - **Preferred:** Fix Bronze data quality at source
   - **Alternative:** Update validation rules in dbt models if business logic changed

3. **Reprocess:**
   ```bash
   # After fixing upstream, reprocess partition
   ecomlake local silver --date 2025-01-15

   # Verify quarantine reduced
   ecomlake silver validate --silver-path data/silver/base --env local
   ```

**Recover Quarantined Records:**

If quarantined records are determined to be valid:

1. **Manual Fix:**
   ```python
   import polars as pl

   # Read quarantine
   df = pl.read_parquet('data/silver/base/quarantine/orders/ingest_dt=2025-01-15/*.parquet')

   # Fix issue (example: fill nulls)
   df_fixed = df.with_columns([
       pl.col('customer_id').fill_null('UNKNOWN')
   ]).drop(['_quarantine_reason', '_quarantine_timestamp'])

   # Write to valid Silver path
   df_fixed.write_parquet('data/silver/base/orders/ingest_dt=2025-01-15/recovered.parquet')
   ```

2. **Update Validation Rules:**
   - Edit `dbt_duckdb/models/base_silver/stg_orders.sql`
   - Adjust `WHERE` clause to allow previously quarantined records
   - Reprocess partition with `--full-refresh`

### Monitoring & Alerts

**Airflow Alerting:**

The Silver DAG will fail if quarantine rate exceeds threshold:

```python
# Configurable via environment variable
MAX_QUARANTINE_RATE = float(os.getenv('MAX_QUARANTINE_RATE', '0.05'))  # 5%

# DAG task checks validation metrics
if quarantine_pct > MAX_QUARANTINE_RATE:
    raise AirflowException(f"Quarantine rate {quarantine_pct:.2%} exceeds {MAX_QUARANTINE_RATE:.2%}")
```

**Weekly Review (Production):**

```bash
# Generate quarantine report
ecomlake silver validate --silver-path data/silver/base --output-report docs/validation_reports/SILVER_QUALITY_2025_W03.md

# Review top reasons
cat docs/validation_reports/quarantine_summary_2025W03.json | jq
```

**Metrics Dashboard:**

Key metrics to track:
- `quarantine_pct` by table and date
- Top 5 `_quarantine_reason` patterns
- Quarantine growth rate (week-over-week)

### Cleanup (Manual)

**Local Development:**
```bash
# Remove all quarantine data older than 7 days
find data/silver/base/quarantine -type f -mtime +7 -delete
```

**Production (GCS):**
```bash
# List old quarantine partitions
gsutil ls gs://${GCS_BUCKET}/silver/base/quarantine/orders/ | grep -v $(date -d '30 days ago' +%Y-%m-%d)

# Delete (dry-run first)
gsutil -m rm -r gs://${GCS_BUCKET}/silver/base/quarantine/orders/ingest_dt=2024-12-*
```

### Escalation

**When to Escalate:**

1. **Quarantine rate >10%** for 3+ consecutive days
   - Indicates systemic issue in Bronze data or validation logic
   - **Action:** Engage source system team and data platform lead

2. **New quarantine reason appears**
   - Could indicate schema change or new data quality issue
   - **Action:** Review Bronze schema changes; update validation specs

3. **Quarantine storage >1TB**
   - Cost/performance impact
   - **Action:** Expedite root cause fix or adjust retention policy

### Prevention

- **Set Alerts:** Configure `MAX_QUARANTINE_RATE` in production Airflow
- **Pre-Bronze Validation:** Add quality checks at ingestion time
- **Schema Contracts:** Use dbt contracts to enforce column presence/types
- **Regular Reviews:** Weekly quarantine report analysis
- **Document Patterns:** Update runbook with new failure patterns

### Reference: Quarantine vs. Validation Failure

| Scenario | Quarantine? | Pipeline Fails? | Gold Impact |
|----------|-------------|-----------------|-------------|
| 1% null PKs | ✅ Yes | ❌ No | None (records excluded) |
| 15% null PKs | ✅ Yes | ✅ Yes (exceeds threshold) | Blocked |
| Invalid JSON | ❌ No | ✅ Yes (parsing error) | Blocked |
| Schema mismatch | ❌ No | ✅ Yes (dbt fails) | Blocked |

---

## 6. Airflow DAG Failures

### Symptoms
- DAG tasks marked as failed in Airflow UI
- Sensor timeouts waiting for upstream data
- Task retries exhausted

### Diagnosis

```bash
# Check Airflow logs
docker-compose logs airflow-worker | grep ERROR

# Check specific task logs in Airflow UI
# Navigate to: DAG > Task Instance > Logs

# Verify DAG is parsed correctly
docker-compose exec airflow-scheduler airflow dags list-import-errors
```

### Resolution

**Sensor timeout (waiting for Bronze data):**
1. Check if upstream ingestion completed
2. Verify sensor is checking correct path/partition
3. Increase `timeout` or `poke_interval` if data arrives late

**Task failure with OOM:**
1. Increase worker memory in `docker-compose.yml`
2. For large tables, enable partitioned processing
3. Consider using `enriched_max_rows_per_file` setting

**dbt task failure:**
1. Check dbt logs in task output
2. Common issues: DuckDB lock contention, missing dependencies
3. Ensure `dbt deps` ran before `dbt run`

**Retry exhaustion:**
1. Review root cause of initial failure
2. Manually clear task state: `airflow tasks clear <dag_id> -t <task_id>`
3. Re-trigger DAG run

### Prevention
- Set appropriate `retries` and `retry_delay` in DAG
- Use `on_failure_callback` for alerting
- Monitor DAG SLAs in Airflow

---

## 7. GCS Connectivity Issues

### Symptoms
- `google.auth.exceptions.DefaultCredentialsError`
- `gcsfs` connection timeouts
- Permission denied errors on bucket operations

### Diagnosis

```bash
# Verify credentials are set
echo $GOOGLE_APPLICATION_CREDENTIALS
cat $GOOGLE_APPLICATION_CREDENTIALS | jq .client_email

# Test GCS access
gsutil ls gs://your-bucket-name/

# Check service account permissions
gcloud projects get-iam-policy your-project-id \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:your-sa@your-project.iam.gserviceaccount.com"
```

### Resolution

**Missing credentials:**
1. Set environment variable:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
   ```
2. For Airflow, add to `docker-compose.yml` environment
3. For local dev, use `gcloud auth application-default login`

**Permission denied:**
1. Required roles for service account:
   - `roles/storage.objectViewer` (read Bronze)
   - `roles/storage.objectCreator` (write Silver)
   - `roles/bigquery.dataEditor` (Gold layer)
2. Grant permissions:
   ```bash
   gcloud storage buckets add-iam-policy-binding gs://your-bucket \
     --member="serviceAccount:your-sa@project.iam.gserviceaccount.com" \
     --role="roles/storage.objectAdmin"
   ```

**Connection timeouts:**
1. Check network connectivity to GCS
2. Verify VPC/firewall rules if running in GCP
3. Increase timeout in `fsspec` configuration

### Staging Publish Checks (staging mode)

If `SILVER_PUBLISH_MODE`, `ENRICHED_PUBLISH_MODE`, or `DIMS_PUBLISH_MODE` is set to
`staging`, confirm data lands in the staging prefix before promotion:

```bash
gsutil ls gs://your-bucket/silver/base/_staging/<run_id>/
gsutil ls gs://your-bucket/silver/enriched/_staging/<run_id>/
gsutil ls gs://your-bucket/silver/dims/_staging/<run_id>/
```

If staging exists but canonical is missing, check the promote task logs (e.g.
`promote_silver_base`, `promote_enriched`, `promote_dims_snapshot`).

### Prevention
- Use Workload Identity in GKE instead of key files
- Rotate service account keys regularly
- Set up IAM alerts for permission changes

---

## 8. Docker & Environment Issues

### Symptoms
- `ModuleNotFoundError` when running Airflow tasks
- `Errno 35: Resource temporarily unavailable` (file locking)
- Dependencies missing in container

### Diagnosis

```bash
# Check installed packages
docker-compose exec airflow-scheduler pip list | grep ecom-datalake

# Verify mounts
docker inspect airflow-scheduler | jq .[0].Mounts
```

### Resolution

**File locking (Errno 35 on macOS):**
1. This is a known issue with VirtioFS on macOS.
2. Fix: Configure writable paths to `/tmp` in `docker-compose.yml`:
   ```yaml
   DBT_TARGET_PATH: "/tmp/dbt_target"
   DBT_LOG_PATH: "/tmp/dbt_logs"
   ```
3. Restart Docker Desktop and switch file sharing implementation to gRPC FUSE if persistent.

**Missing dependencies:**
1. Rebuild the image: `docker-compose build`
2. Ensure `setup.py` or `pyproject.toml` is copied in Dockerfile
3. Check `.dockerignore` isn't excluding source code

**Airflow warning: "empty cryptography key":**
1. Generate a Fernet key (one-time) and store it securely.
2. Set `AIRFLOW__CORE__FERNET_KEY` via env or secret manager.
3. Recreate containers so Airflow picks it up.

**Production auth guidance (GCP):**
1. Prefer Workload Identity (no key files) for GKE/Composer.
2. If Workload Identity is unavailable, use a dedicated service account key.
3. Mount the key and set `GOOGLE_APPLICATION_CREDENTIALS` via your orchestrator
   or secret manager (never commit the key).

**BigQuery load error: missing `google_cloud_default` connection:**
1. Create the connection in Airflow (one-time):
   ```bash
   docker-compose exec airflow-scheduler \
     airflow connections add google_cloud_default \
     --conn-type google_cloud_platform \
     --conn-extra '{"extra__google_cloud_platform__project":"<your-project>"}'
   ```
2. Or set `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT` via env/secret manager and recreate.

---

## Quick Reference: Validation Exit Codes

| Exit Code | Meaning | Action Required |
|-----------|---------|-----------------|
| 0 | All checks passed | None |
| 1 | FAIL status in prod | Fix data quality issues before proceeding |
| 1 | FAIL with `--enforce` | Validation gate blocked pipeline |

## Quick Reference: Key Configuration

| Setting | Location | Purpose |
|---------|----------|---------|
| `max_quarantine_pct` | config/config.yml | Max allowed quarantine rate |
| `enriched_ratio_epsilon` | config/config.yml | Float comparison tolerance |
| `min_table_rows` | config/config.yml | Minimum rows per table |
| `sla_thresholds` | config/config.yml | Pass rate thresholds by table |

## Contact & Escalation

1. **L1 (Self-service)**: Follow this runbook
2. **L2 (Team)**: Check `#data-platform` Slack channel
3. **L3 (Escalation)**: Page on-call via PagerDuty

---

*Last updated: 2025-01-17*

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
