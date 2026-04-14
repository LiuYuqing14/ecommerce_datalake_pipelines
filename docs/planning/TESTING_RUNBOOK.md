# Testing Runbook (Bronze -> Silver)

> **📌 HISTORICAL DOCUMENT - Original Testing Runbook (Pre-Implementation)**
>
> This document represents the initial testing strategy for the Bronze → Silver pipeline.
> For **current testing practices and comprehensive guide**, see:
> - **[TESTING_GUIDE.md](../resources/TESTING_GUIDE.md)** - Complete testing guide with unit tests, E2E validation, and CI/CD
>
> **Status**: ✅ Implemented and enhanced - Now includes 51% test coverage, E2E CI pipeline, and three-layer validation framework

Provide a repeatable testing sequence for the bronze -> silver pipeline.

## Preconditions
- Bronze samples or live partitions are available for the test dates.
- Data contract and SLA checks are defined.

## Bronze Data Profiling Strategy

### Why Profile Bronze Data?
Before building Silver transforms, we need to understand:
- Actual schema structure and data types
- Data quality issues (nulls, cardinality, outliers)
- Volume characteristics per table
- Temporal schema drift across the 6-year dataset

### Profiling Script
Location: `ecomlake bronze profile` (legacy script: [scripts/describe_parquet_samples.py](../../../scripts/describe_parquet_samples.py))

**Usage**:
```bash
ecomlake bronze profile --output docs/data/BRONZE_PROFILE_REPORT.md
```

### Sampling Strategy: Stratified Temporal Sampling
Instead of profiling the full 6-year dataset (~17GB) or a single random sample, we use **stratified temporal sampling**:

- **Sample 3 months** from different periods:
  - Early period: `2020-06` (first year)
  - Middle period: `2023-01` (mid-dataset)
  - Recent period: `2025-12` (latest year)

- **Why this works**:
  - Fast feedback loop (seconds vs minutes)
  - Detects schema drift over time (new columns, type changes)
  - Identifies quality degradation patterns
  - Representative of dataset characteristics

### Profile Output Structure
The generated [BRONZE_PROFILE_REPORT.md](../data/BRONZE_PROFILE_REPORT.md) includes:

1. **Table Summaries**: Row counts, partition counts, column counts per table
2. **Column Statistics**: Per column:
   - Data type, null %, distinct count
   - Min/max values
   - Percentiles (p25, p50, p75, p95) for numeric columns
   - Top 5 values for string/categorical columns
3. **Data Quality Flags**: Automated checks for:
   - High null rates (>50%)
   - Suspiciously low cardinality on primary entity IDs (customer_id, order_id, product_id, etc.)
   - Cardinality mismatches between related tables
   - Volume anomalies (sudden spikes/drops)
4. **Partition Coverage**: Shows which months were sampled per table

### Quality Check Logic
The profiling script includes context-aware data quality checks:

- **Primary Entity IDs** (customer_id, order_id, product_id, cart_id, return_id, item_id):
  - Expected: High cardinality (thousands to millions of distinct values)
  - Flagged if: <10 distinct values (indicates data corruption or test data)

- **Lookup IDs** (agent_id, region_id, tier_id, status_id, warehouse_id):
  - Expected: Low cardinality (3-50 distinct values)
  - NOT flagged (this is normal for reference data)

- **Metadata Columns** (batch_id, ingestion_ts, event_id, source_file):
  - Expected: Low cardinality for batch metadata
  - Excluded from quality checks

### Interpreting the Report
- ✅ **No quality flags**: Bronze data looks healthy
- ⚠️ **High null rate**: Investigate if the column is truly optional or if there's an upstream data quality issue
- ⚠️ **Low cardinality on entity ID**: Likely data corruption or incomplete load (should have thousands+ distinct values)
- ⚠️ **Cardinality mismatch**: FK relationships might be broken (e.g., 10k orders but only 5 customers)

### When to Re-run Profiling
- After Bronze data reloads
- When investigating data quality issues
- Before designing new Silver transforms
- When schema drift is suspected

## Test Matrix (Progressive)

### Phase 1: Small Batch Validation
- Scope: single partition window across all tables (e.g., one `ingest_dt`).
- Goals: schema enforcement, date parsing, FK checks, partition write path.
- Pass criteria:
  - All tables write silver output.
  - No missing required columns.
  - Reject rate within expected bounds.

### Phase 2: Medium Batch
- Scope: short batch window across all tables.
- Goals: batching logic, idempotency, performance baseline.
- Pass criteria:
  - Each day produces partitions.
  - No duplicate partitions on rerun.
  - Runtime within expected SLA.

### Phase 3: Large Batch
- Scope: extended batch window across all tables.
- Goals: stability under longer windows, row count drift checks.
- Pass criteria:
  - Row counts within thresholds.
  - Audit logs generated for all partitions.

## Execution Steps (Template)
1) Select batch window and tables.
2) Run pipeline with logging enabled.
3) Inspect outputs:
   - Partition paths exist for each table.
   - `_audit` logs present.
4) Validate quality checks:
   - Required columns present.
   - PK/FK checks pass or quarantined.
5) Record metrics:
   - Input rows, output rows, rejects.

## Troubleshooting
- Missing partitions: verify input `ingest_dt` and source path.
- FK failures: confirm parent tables loaded for the same window.
- Date parsing errors: check source string formats.

## Outputs to Collect
- Audit summary JSON per run.
- Sample row counts per table.
- Any error logs or rejected row samples.

## Test Categories and Success Criteria
Use the SLAs and quality thresholds from `docs/planning/SLA_AND_QUALITY.md` as pass/fail gates.

Schema/Contract
- Required columns present.
- Types parseable and consistent with the contract.
- Success: 100% compliance with required columns and type parsing.

Data Quality
- Null rate thresholds for critical fields.
- Numeric bounds (non-negative amounts, positive quantities).
- Success: all thresholds within SLA limits.

Integrity
- PK uniqueness and non-null.
- FK existence (e.g., order_items -> orders).
- Success: integrity thresholds met or rows quarantined per SLA.

Volume/Drift
- Row count within baseline thresholds (daily or weekly average).
- Success: within SLA-defined tolerance.

End-to-End
- Full partition processed and silver outputs written.
- Audit metrics emitted and accessible.
- Success: outputs present, metrics logged, and checks pass.

## dbt Tests (Base + Gold)
- Base Silver: schema tests for required fields, not-null, and relationships.
- Gold Marts: aggregation sanity checks (non-negative totals, row count drift).
- Success: dbt test suite passes with zero failures.

## Observability Checks
- Audit logs written for each table + partition.
- Audit table in BigQuery updates within SLA window.
 - Local dev: DuckDB `audit_runs` table updated with latest run metrics.

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
