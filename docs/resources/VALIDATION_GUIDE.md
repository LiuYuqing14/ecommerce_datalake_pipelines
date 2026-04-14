# Data Validation Guide

Complete guide to data quality validation in the e-commerce data pipeline.

## Overview

The pipeline has **3 validation gates**:

```
┌──────────────┐
│ Bronze Layer │  (GCS - Raw Data)
└──────┬───────┘
       │
       ▼
┌─────────────────────────────────────────┐
│ GATE 1: Bronze Validation               │
│ - Metadata checks (row counts, schema)  │
│ - Pydantic sample validation            │
│ Output: docs/validation_reports/        │
│         BRONZE_*.md                      │
└──────┬──────────────────────────────────┘
       │ PASS
       ▼
┌──────────────┐
│ Base Silver  │  (dbt transformations)
└──────┬───────┘
       │
       ▼
┌─────────────────────────────────────────┐
│ GATE 2: Silver Quality Validation       │
│ - Pass rate calculation                 │
│ - Quarantine analysis                   │
│ Output: docs/validation_reports/        │
│         SILVER_QUALITY_<run_id>.md       │
└──────┬──────────────────────────────────┘
       │ PASS
       ▼
┌──────────────────┐
│ Enriched Silver  │  (Polars transformations)
└──────┬───────────┘
       │
       ▼
┌─────────────────────────────────────────┐
│ GATE 3: Enriched Validation             │
│ - Ingest partition checks               │
│ - Minimum row count checks              │
│ Output: docs/validation_reports/        │
│         ENRICHED_QUALITY_<run_id>.md    │
└─────────────────────────────────────────┘
```

**Dev/Prod report storage:**  
When `PIPELINE_ENV=dev|prod`, reports are written to GCS at:
`gs://$REPORTS_BUCKET/validation_reports/<run_id>/`

**GCS validation reads (no rsync):**  
Validation reads `gs://` paths directly via `fsspec/gcsfs` in dev/prod.  
`gcloud storage rsync` is used only for publish/sync steps (not for validation).

**Staging-aware validation:**  
When a publish mode is set to `staging`, validation targets the staging prefix
(`.../_staging/<run_id>/`) and **promotion to canonical happens only after the
validation gate passes**.

---

## Gate 1.5: Dimension Quality Validation (✅ IMPLEMENTED)

### What It Does

Immediately after dimension snapshots are refreshed (and before fact processing), this lightweight gate:

1. **Verifies snapshot existence** for the current run date (`snapshot_dt`).
2. **Checks readability** (opens parquet files to ensure validity).
3. **Validates schema** (checks required columns like `customer_id`).
4. **Checks for critical nulls** (e.g., null primary keys).
5. **Prevents full scans**: Unlike Gate 2, this only checks the daily snapshot, avoiding expensive historical scans of the Bronze layer.

### Output Files

- **Markdown Report:** `docs/validation_reports/DIMS_QUALITY_<run_id>.md` (or GCS equivalent).

### Integration with Airflow

```python
validate_dims_quality = BashOperator(
    task_id="validate_dims_quality",
    bash_command="ecomlake dim validate --run-date {{ ds }} ..."
)
```

---

## Gate 2: Silver Quality Validation (✅ IMPLEMENTED)

### What It Does

After dbt processes Bronze → Silver, this validator checks **Fact Tables Only** (Orders, Items, etc.):

1. **Counts rows** in Bronze, Silver, and Quarantine
2. **Calculates pass rates** (Silver / (Silver + Quarantine))
3. **Compares to SLA thresholds** (from `docs/planning/SLA_AND_QUALITY.md`)
4. **Analyzes quarantine reasons** (what failed, how often)
5. **Detects row loss** (Bronze vs. total processed)
6. **Flags contract-level anomalies** (distinct ID ratios, overall quarantine/row loss thresholds)
6. **Generates dual output**:
   - JSON metrics → `data/metrics/silver_quality/`
   - Markdown report → `docs/validation_reports/SILVER_QUALITY_<run_id>.md`

### Usage

#### Basic Usage

```bash
# Run after dbt completes
ecomlake silver validate
```

#### With Custom Paths

```bash
ecomlake silver validate \
  --bronze-path samples/bronze \
  --silver-path data/silver/base \
  --quarantine-path data/silver/base/quarantine \
  --run-id "20260111_143022"
```

#### In Hard Fail Mode (Stops Pipeline on SLA Breach)

```bash
ecomlake silver validate \
  --enforce-quality
```

### Output Files

#### 1. JSON Metrics (Machine-Readable)

**Location:** `data/metrics/silver_quality/silver_quality_{run_id}.json`

```json
{
  "metadata": {
    "written_at": "2026-01-11T14:47:15Z",
    "environment": "local",
    "metric_type": "silver_quality"
  },
  "transformation_metadata": {
    "run_id": "20260111_143022",
    "timestamp": "2026-01-11T14:47:15Z",
    "dbt_project_version": "1.0.5"
  },
  "table_metrics": [
    {
      "table": "orders",
      "row_counts": {
        "bronze_input": 2456789,
        "silver_output": 2398456,
        "quarantine_output": 58333,
        "total_processed": 2456789,
        "row_loss": 0,
        "row_loss_pct": 0.0
      },
      "pass_rate": {
        "rate": 0.9762,
        "sla_threshold": 0.95,
        "status": "PASS"
      },
      "quarantine_breakdown": [...]
    }
  ],
  "overall_status": "PASS"
}
```

#### 2. Markdown Report (Human-Readable)

**Location:** `docs/validation_reports/SILVER_QUALITY_<run_id>.md`

See example in `docs/validation_reports/README.md`

### SLA Thresholds

Configured in `config/config.yml` under `pipeline.sla_thresholds` (loaded via `src/settings.py`):

| Table | Pass Rate SLA |
|-------|--------------|
| orders | 95% |
| customers | 98% |
| product_catalog | 99% |
| shopping_carts | 95% |
| cart_items | 95% |
| order_items | 95% |
| returns | 95% |
| return_items | 95% |

### Status Logic

- **PASS**: Pass rate >= SLA threshold
- **WARN**: Pass rate >= 90% of SLA (within 10%)
- **FAIL**: Pass rate < 90% of SLA

### Contract-Level Thresholds (Gate 2)

These are configurable in `config/config.yml` and used by `src/validation/silver_quality.py`:

| Setting | Purpose | Default |
|---------|---------|---------|
| `max_quarantine_pct` | Max total quarantine % before flagging | 5.0 |
| `max_row_loss_pct` | Max total row loss % before flagging | 1.0 |
| `min_return_id_distinct_ratio` | Minimum distinct ratio for returns/return_items | 0.001 |
| `expected_bronze_partitions` | Required bronze ingest_dt partitions (supports YYYY-MM-DD and ranges like YYYY-MM-DD..YYYY-MM-DD) | [] |
| `min_table_rows` | Minimum processed rows per table | 1 |

### Integration with Airflow

Add to DAG as **Phase 1.5** (between Base Silver and Enriched Silver):

```python
# In airflow/dags/ecom_silver_to_gold.py

from airflow.operators.bash import BashOperator

validate_silver_quality = BashOperator(
    task_id="validate_silver_quality",
    bash_command=(
        "python -m src.validation.silver "
        "--bronze-path samples/bronze "
        "--silver-path data/silver/base "
        "--quarantine-path data/silver/base/quarantine "
        "--run-id {{ run_id }} "
        # Optional: Fail pipeline on SLA breach
        # "--enforce-quality"
    ),
)

# Set dependencies
base_silver_group >> validate_silver_quality >> promote_silver_base >> enriched_silver_group
```

---

## Gate 3: Enriched Silver Quality Validation (✅ IMPLEMENTED)

### What It Does

After Polars creates Enriched Silver outputs, this validator:

1. **Checks partitioned outputs** (business date or ingest_dt)
2. **Counts rows** in the target partition (per table)
3. **Validates minimum row counts** (configurable)
4. **Captures schema snapshot** (columns + dtypes)
5. **Computes key field null rates**
6. **Runs sanity checks** (e.g., negative counts, rates outside 0..1)
7. **Runs semantic checks** (business-rule validations per table)
4. **Generates dual output**:
   - JSON metrics → `data/metrics/enriched_silver/`
   - Markdown report → `docs/validation_reports/ENRICHED_QUALITY_<run_id>.md`

### Usage

```bash
python -m src.validation.enriched \
  --enriched-path data/silver/enriched \
  --ingest-dt 2026-01-12 \
  --run-id "20260112_120000"
```

### Configuration

Configured in `config/config.yml` under:

- `pipeline.enriched_tables`
- `pipeline.enriched_min_table_rows`

### Integration with Airflow

```python
# In airflow/dags/ecom_silver_to_gold.py

validate_enriched_quality = BashOperator(
    task_id="validate_enriched_quality",
    bash_command=(
        "ecomlake enriched validate_quality "
        "--enriched-path data/silver/enriched "
        "--ingest-dt {{ ds }} "
        "--run-id {{ run_id }}"
    ),
)

enriched_silver_group >> sync_silver_enriched_to_gcs >> validate_enriched_quality >> promote_enriched >> load_bigquery_group
```

## Gate 1: Bronze Validation (✅ IMPLEMENTED)

### Components

#### 1. Bronze Metadata Validation

**Script:** `src/validation/bronze_quality.py`

**Purpose:** Fast pre-checks before expensive processing

**Checks:**
- Row counts via manifests
- Partition completeness
- Missing/empty partitions

**Output:**
- `data/metrics/data_quality/bronze_quality_{run_id}.json`
- `docs/validation_reports/BRONZE_QUALITY_<run_id>.md`

#### 2. Pydantic Sample Validation (Enhanced)

**Script:** `ecomlake bronze validate-samples` (optional enhancement)

---

## Viewing Validation Results

### Latest Run

```bash
# View latest Silver quality report (pick a run_id)
ls -t docs/validation_reports/SILVER_QUALITY_*.md | head -n 1
```

### Historical Trends

```bash
# See how quality has changed over time (replace with chosen run_id)
git log -p docs/validation_reports/SILVER_QUALITY_<run_id>.md

# Compare to previous run
git diff HEAD~1 docs/validation_reports/SILVER_QUALITY_<run_id>.md
```

### Query Metrics (Local)

```python
from src/observability import get_metrics_writer

# Get last 10 Silver quality runs
writer = get_metrics_writer("silver_quality")
recent_runs = writer.read_metrics(limit=10)

# Analyze trends
for run in recent_runs:
    for table in run["table_metrics"]:
        print(f"{table['table']}: {table['pass_rate']['rate']:.2%}")
```

### Query Metrics (Production - BigQuery)

```sql
-- Load metrics from GCS
CREATE EXTERNAL TABLE `my-project.observability.silver_quality`
OPTIONS (
  format = 'JSON',
  uris = ['gs://ecom-datalake-metrics/pipeline_metrics/silver_quality/*.json']
);

-- Track pass rate trends
SELECT
  DATE(metadata.written_at) as date,
  table_metric.table,
  table_metric.pass_rate.rate as pass_rate,
  table_metric.pass_rate.sla_threshold as sla
FROM `my-project.observability.silver_quality`,
  UNNEST(table_metrics) as table_metric
WHERE table_metric.table = 'orders'
ORDER BY date DESC
LIMIT 30;
```

---

## Troubleshooting

### Low Pass Rates

If a table shows low pass rate:

1. **Check quarantine reasons**
   ```bash
   # View top quarantine reasons in report
   grep -A 10 "Top Quarantine Reasons" docs/validation_reports/SILVER_QUALITY_<run_id>.md
   ```

2. **Inspect quarantine data**
   ```python
   import polars as pl
   df = pl.read_parquet("data/silver/quarantine/orders/**/*.parquet")
   print(df.select("invalid_reason").value_counts())
   ```

3. **Compare to Bronze**
   - High FK failures → Check upstream Bronze table quality
   - High duplicates → Check Bronze deduplication logic
   - Invalid dates → Check Bronze timestamp parsing

### Row Loss

If Bronze rows != (Silver + Quarantine):

1. **Check for filtering** in dbt models
2. **Verify all partitions processed**
3. **Check for dbt failures** (partial runs)

### Unexpected Quarantine Spikes

If quarantine rate suddenly increases:

1. **Check upstream data quality** (Bronze validation)
2. **Review recent Bronze schema changes**
3. **Look for new error patterns** in quarantine breakdown

---

## Best Practices

### 1. Always Review Reports After Changes

```bash
# After modifying dbt models
dbt run --select stg_ecommerce__orders
python -m src.validation.silver --run-id <run_id>
git diff docs/validation_reports/SILVER_QUALITY_<run_id>.md
```

### 2. Commit Validation Reports

```bash
# Reports are self-documenting - commit them!
git add docs/validation_reports/SILVER_QUALITY_<run_id>.md
git commit -m "chore: update Silver quality report"
```

### 3. Monitor Trends, Not Just Point-in-Time

```python
# Track pass rate trend over last 7 runs
writer = get_metrics_writer("silver_quality")
runs = writer.read_metrics(limit=7)

for run in runs:
    orders_metric = [m for m in run["table_metrics"] if m["table"] == "orders"][0]
    print(f"{run['transformation_metadata']['run_id']}: {orders_metric['pass_rate']['rate']:.2%}")
```

### 4. Set Appropriate SLA Thresholds

- **Critical tables** (orders, customers): 95-98%
- **Lookup tables** (product_catalog): 99%+
- **Denormalized tables** (order_items): 95%

### 5. Use Soft Fails in Development

```bash
# Development: Warn but don't fail
python -m src.validation.silver
```

```bash
# Production: Fail pipeline on SLA breach
python -m src.validation.silver --enforce-quality
```

---

## Roadmap

### Implemented ✅
- [x] Silver quality validation
- [x] Dual output (JSON + Markdown)
- [x] Observability framework integration
- [x] Quarantine analysis
- [x] SLA threshold validation

### Next Steps ⏭️
- [x] Bronze metadata validation
- [x] Enhanced Pydantic validation (all 8 tables)
- [ ] Historical baseline comparison
- [ ] Anomaly detection
- [ ] Alerting integration (Slack/PagerDuty)

---

## Files Reference

### Validation Scripts
- `src/validation/silver/__main__.py` - Silver quality validator
- `ecomlake bronze validate-samples` - Bronze Pydantic validation
- `ecomlake bronze profile` - Bronze schema profiling (legacy script: `scripts/describe_parquet_samples.py`)

### Validation Reports (Auto-Generated)
- `docs/validation_reports/SILVER_QUALITY_<run_id>.md` - Silver quality report
- `docs/validation_reports/README.md` - Reports documentation

### Metrics (JSON)
- `data/metrics/silver_quality/` - Silver quality metrics
- `data/metrics/data_quality/` - Bronze validation metrics

### Configuration
- `docs/planning/SLA_AND_QUALITY.md` - SLA thresholds and quality requirements
- `docs/resources/DATA_CONTRACT.md` - Schema definitions

---

<p align="center">
  <a href="../../README.md">🏠 <b>Home</b></a>
  &nbsp;·&nbsp;
  <a href="../../RESOURCE_HUB.md">📚 <b>Resource Hub</b></a>
</p>

<p align="center">
  <sub>Last updated: 2026-01-28</sub><br>
  <sub>✨ Transform the data. Tell the story. Build the future. ✨</sub>
</p>
