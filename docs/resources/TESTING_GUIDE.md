# Testing Guide - Bronze → Silver → Gold Pipeline

## Overview

This guide covers testing strategies for the complete medallion lakehouse pipeline, from Bronze validation through Gold mart testing. The project maintains **61.81% test coverage** with unit tests, integration tests, and full E2E pipeline validation.

## Test Categories

### 1. Unit Tests (`tests/unit/`)

**Coverage**: Transform logic, validation modules, runners

```bash
# Run all unit tests
pytest tests/unit/ -v

# Run with coverage
pytest tests/unit/ --cov=src --cov-report=term-missing

# Run specific test module
pytest tests/unit/test_transforms.py -v
```

**Key Test Modules:**
- `test_transforms.py` - Polars transform logic (cart attribution, customer retention, etc.)
- `test_validation.py` - Bronze/Silver/Enriched validation modules
- `test_runners_base.py` - Base silver runner and manifest generation
- `test_runners_dims.py` - Dimension snapshot runner
- `test_bronze_quality.py` - Bronze quality validation
- `test_settings.py` - Configuration and settings validation

### 2. Integration Tests

**E2E Pipeline Check** (CI/CD):

The CI workflow validates the pipeline in a single end-to-end job:

```bash
for day in 2020-01-01 2020-01-02 2020-01-03 2020-01-04 2020-01-05; do
  ecomlake dim run --run-date "$day"
  ecomlake dim validate --run-date "$day" --run-id "ci_$RUN_ID"
done

ecomlake silver run \
  --select "base_silver.*" \
  --vars "{run_date: '2020-01-05', lookback_days: 4}"

ecomlake silver validate \
  --partition-date 2020-01-05 \
  --lookback-days 4

ecomlake dbt test
ecomlake local enriched --date 2020-01-05

ecomlake enriched validate --ingest-dt 2020-01-05
python tests/integration/test_gold_logic_duckdb.py
```

**What gets validated:**
- Dims snapshot generation
- Base Silver dbt models + data tests
- Silver quality gate over a 3-day window
- Enriched business tables
- Gold layer transformation logic

### 3. dbt Tests

**Base Silver (dbt-duckdb)**:

```bash
cd dbt_duckdb
dbt test --target dev --select base_silver.*
```

Tests:
- Schema enforcement (required columns, data types)
- Primary key uniqueness and non-null constraints
- Foreign key relationships
- Not-null constraints on critical fields

**Gold Marts (dbt-bigquery)**:

```bash
cd dbt_bigquery
dbt test --target dev --select gold.*
```

Tests:
- Aggregation sanity checks (non-negative totals)
- Row count drift detection
- Referential integrity with Silver layer

## Bronze Data Profiling

### Purpose

Understand Bronze data quality before building Silver transforms:
- Schema structure and data types
- Null rates and cardinality
- Volume characteristics
- Temporal schema drift detection

### Profiling Script

```bash
# Basic profile with quality report
ecomlake bronze profile \
  --date-range 2020-01-01..2020-12-31

# Generate schema JSON map
ecomlake bronze profile \
  --date-range 2020-01-01..2020-12-31 \
  --schema-json docs/data/BRONZE_SCHEMA_MAP.json

# Auto-update data contract
ecomlake bronze profile \
  --date-range 2020-01-01..2020-12-31 \
  --update-contract docs/resources/DATA_CONTRACT.md

# Generate data dictionary
ecomlake bronze profile \
  --date-range 2020-01-01..2020-12-31 \
  --data-dictionary docs/data/DATA_DICTIONARY.md
```

### Sampling Strategy: Stratified Temporal Sampling

Sample 3 months from different periods to detect drift:
- Early period: `2020-03` (first year)
- Middle period: `2023-01` (mid-dataset)
- Recent period: `2025-10-01` (latest year, single-day sample)

**Why this works:**
- Fast feedback loop (seconds vs minutes)
- Detects schema drift over time
- Identifies quality degradation patterns
- Representative of dataset characteristics

### Quality Check Logic

**Primary Entity IDs** (customer_id, order_id, product_id):
- Expected: High cardinality (thousands to millions)
- Flagged if: <10 distinct values (data corruption)

**Lookup IDs** (agent_id, region_id, tier_id):
- Expected: Low cardinality (3-50 distinct values)
- NOT flagged (normal for reference data)

**Metadata Columns** (batch_id, ingestion_ts):
- Expected: Low cardinality for batch metadata
- Excluded from quality checks

### Interpreting the Profile Report

- ✅ **No quality flags**: Bronze data looks healthy
- ⚠️ **High null rate**: Investigate if column is optional or upstream issue
- ⚠️ **Low cardinality on entity ID**: Data corruption or incomplete load
- ⚠️ **Cardinality mismatch**: FK relationships might be broken

## Validation Framework Testing

### Three-Layer Validation

The pipeline implements validation at each layer with distinct quality requirements:

**1. Bronze Validation** (`src/validation/bronze_quality.py`):

```bash
ecomlake bronze validate \
  --bronze-path samples/bronze \
  --partition-date 2020-01-05 \
  --lookback-days 0 \
  --output-report docs/validation_reports/bronze_quality.md
```

Validates:
- Manifest completeness and row counts
- Schema conformance
- Partition coverage
- File integrity

**2. Silver Validation** (`src/validation/silver`):

```bash
ecomlake silver validate \
  --bronze-path samples/bronze \
  --silver-path data/silver/base \
  --partition-date 2020-01-05 \
  --tables orders,customers,product_catalog \
  --output-report docs/validation_reports/silver_quality.md \
  --enforce-quality
```

Validates:
- Bronze-to-Silver row count reconciliation
- Primary key uniqueness
- Foreign key referential integrity
- Quarantine analysis
- Schema consistency

**3. Enriched Validation** (`src/validation/enriched`):

```bash
ecomlake enriched validate \
  --enriched-path data/silver/enriched \
  --ingest-dt 2020-01-05 \
  --output-report docs/validation_reports/enriched_quality.md \
  --enforce-quality
```

Validates:
- Business rule enforcement
- Required column presence
- Minimum row count thresholds
- Schema snapshot consistency
- Null rate analysis

## Test Matrix (Progressive Testing)

### Phase 1: Small Batch Validation

**Scope**: Single partition window across all tables (one `ingest_dt`)

**Goals**:
- Schema enforcement
- Date parsing
- FK checks
- Partition write path

**Pass Criteria**:
- All tables write Silver output
- No missing required columns
- Reject rate within expected bounds (<5%)

**Example**:

```bash
ecomlake silver run \
  --select "base_silver.*" \
  --vars "{run_date: '2020-01-05', lookback_days: 0}"
ecomlake dim run --run-date 2020-01-05
ecomlake local enriched --date 2020-01-05
```

### Phase 2: Medium Batch

**Scope**: Short batch window across all tables (7 days)

**Goals**:
- Batching logic
- Idempotency
- Performance baseline

**Pass Criteria**:
- Each day produces partitions
- No duplicate partitions on rerun
- Runtime within expected SLA

**Example**:

```bash
for date in 2020-01-01 2020-01-02 2020-01-03 2020-01-04 2020-01-05; do
  ecomlake silver run \
    --select "base_silver.*" \
    --vars "{run_date: '${date}', lookback_days: 0}"
  ecomlake local enriched --date $date
done
```

### Phase 3: Large Batch

**Scope**: Extended batch window across all tables (30+ days)

**Goals**:
- Stability under longer windows
- Row count drift checks
- Memory footprint validation

**Pass Criteria**:
- Row counts within thresholds
- Audit logs generated for all partitions
- Memory usage <10GB peak

## CI/CD Pipeline Testing

### GitHub Actions Workflow

The project includes automated E2E pipeline validation:

```yaml
# .github/workflows/pipeline-e2e.yml
- name: Run E2E Pipeline Check
  run: |
    for day in 2020-01-01 2020-01-02 2020-01-03 2020-01-04 2020-01-05; do
      ecomlake dim run --run-date "$day"
      ecomlake dim validate --run-date "$day"
    done
    ecomlake silver run \
      --select "base_silver.*" \
      --vars "{run_date: '2020-01-05', lookback_days: 2}"
    ecomlake local enriched --date 2020-01-05
```

**What This Validates**:
- Full Bronze → Silver → Enriched flow
- Schema consistency across layers
- Transform logic integrity
- Configuration loading
- Sample data compatibility

## Performance Testing

### Benchmarking Transforms

```bash
# Benchmark specific transform
pytest tests/unit/test_transforms.py::test_cart_attribution -v --durations=10

# Profile memory usage
mprof run ecomlake enriched run --ingest-dt 2020-01-05
```

### Expected Performance

**Base Silver** (dbt-duckdb):
- 8 tables, ~194k total rows across sample partitions
- Runtime: <2 minutes
- Memory: <2GB

**Enriched Silver** (Polars):
- 10 transforms on sample partitions (single-day window)
- Runtime: <5 minutes
- Memory: <6GB peak

**Gold Marts** (dbt-bigquery):
- 8 fact tables
- Runtime: <3 minutes (warehouse execution)

## Troubleshooting

### Missing Partitions

**Symptom**: No Silver output for expected partition

**Debug**:

```bash
# Check Bronze input
ls -lh samples/bronze/orders/ingest_dt=2020-01-05/

# Verify manifest
cat samples/bronze/orders/ingest_dt=2020-01-05/_MANIFEST.json

# Check dbt run output
cat /tmp/dbt_logs/dbt.log
```

### FK Failures

**Symptom**: Foreign key validation failures in Silver

**Debug**:

```bash
# Verify parent tables loaded for same window
ls -lh data/silver/base/customers/ingestion_dt=2020-01-05/
ls -lh data/silver/base/product_catalog/ingestion_dt=2020-01-05/

# Check dimension snapshots
ls -lh data/silver/dims/customers/
cat data/silver/dims/_latest.json
```

### Date Parsing Errors

**Symptom**: Failed date/timestamp parsing in transforms

**Debug**:

```bash
# Profile Bronze data types
ecomlake bronze profile \
  --tables orders \
  --date-range 2020-01-05..2020-01-05

# Check data contract expectations
cat docs/data/DATA_CONTRACT.md | grep -A 5 "order_date"
```

### Transform Failures

**Symptom**: Enriched transforms fail with Polars errors

**Debug**:

```bash
# Run enriched for a single date (then inspect specific output table)
ecomlake enriched run --ingest-dt 2020-01-05

# Check input schemas
ecomlake bronze profile \
  --bronze-path data/silver/base \
  --tables orders,shopping_carts
```

## Test Data Management

### Sample Data

The project includes Bronze Parquet samples in `samples/bronze/`:
- **Tables**: 8 tables (orders, customers, product_catalog, shopping_carts, cart_items, order_items, returns, return_items)
- **Partitions**:
  - `ingest_dt`: 2020-01-01 through 2020-01-05 (5 consecutive days)
  - `signup_date`: 2019-01-01 through 2020-01-05 (370 partitions - complete customer history)
  - `category`: Books, Clothing, Electronics, Home, Toys (all 5 categories)
- **Size**: ~16MB compressed, ~20MB extracted
- **Rows**: ~194k total rows across 8 tables (see `docs/data/BRONZE_PROFILE_REPORT.md`)
- **Format**: Hive-partitioned Parquet with manifests
- **Use Case**: Full local testing including dims snapshot generation

### Refreshing Samples

```bash
# Generate new samples from data generator
# (requires ecom_sales_data_generator repo)
python generate_samples.py --start-date 2020-01-01 --end-date 2020-01-05

# Or sync from GCS
gsutil -m rsync -r gs://your-bucket/bronze/orders/ingest_dt=2020-01-05 \
  samples/bronze/orders/ingest_dt=2020-01-05
```

## Observability Checks

### Audit Logs

**Verify audit logs generated**:

```bash
# Check local audit JSON files
ls -lh data/metrics/audit/

# View audit summary
cat data/metrics/audit/run_2020-01-05_*.json | jq .
```

**Audit Schema** (see [AUDIT_SCHEMA.md](../planning/AUDIT_SCHEMA.md)):
- `run_id`, `table_name`, `partition_value`
- `input_rows`, `output_rows`, `quarantine_rows`
- `quality_checks_passed`, `execution_time_seconds`

### Validation Reports

**Check validation report outputs**:

```bash
# Bronze quality report
cat docs/validation_reports/BRONZE_QUALITY_*.md

# Silver quality report
cat docs/validation_reports/SILVER_QUALITY_*.md

# Enriched quality report
cat docs/validation_reports/ENRICHED_QUALITY_*.md
```

## Success Criteria

### Test Coverage Goals

- ✅ **Unit Tests**: >60% coverage (current: 61.81%)
- ✅ **Integration Tests**: E2E pipeline passes on sample data
- ✅ **dbt Tests**: 100% pass rate on schema/relationship tests
- ✅ **Validation Framework**: All three layers tested

### Quality Gates

From [SLA_AND_QUALITY.md](../resources/SLA_AND_QUALITY.md):

**Bronze**:
- Manifest row counts match actual
- No missing required partitions
- Schema drift detected and reported

**Silver**:
- Row loss <1% (Bronze → Silver)
- Quarantine rate <5%
- PK uniqueness 100%
- FK integrity >99%

**Enriched**:
- All business rules enforced
- Required columns present
- Minimum row thresholds met
- Schema consistent with specifications

## Best Practices

### 1. Test Early and Often

```bash
# Run unit tests before committing
pytest tests/unit/ -v

# Run pre-commit hooks
pre-commit run --all-files
```

### 2. Use Stratified Sampling

Don't profile the full 6-year dataset—use temporal sampling for fast feedback.

### 3. Validate at Each Layer

Run validation after each layer transformation to catch issues early:
- Bronze → validate manifests and schemas
- Silver → validate row counts and integrity
- Enriched → validate business rules

### 4. Monitor Test Coverage

```bash
# Generate coverage report
pytest --cov=src --cov-report=html tests/

# Open in browser
open htmlcov/index.html
```

### 5. Document Failures

When tests fail, document:
- Root cause analysis
- Fix implemented
- Preventive measures added

## Related Documentation

- **[Validation Guide](VALIDATION_GUIDE.md)** - Three-layer validation framework details
- **[SLA & Quality Gates](SLA_AND_QUALITY.md)** - Quality thresholds and acceptance criteria
- **[Data Contract](DATA_CONTRACT.md)** - Bronze → Silver schema expectations
- **[Audit Schema](../planning/AUDIT_SCHEMA.md)** - Audit trail structure and fields

---

**Last Updated**: 2026-01-24
**Test Coverage**: 62%
**CI/CD Status**: ✅ Automated E2E pipeline validation enabled

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
