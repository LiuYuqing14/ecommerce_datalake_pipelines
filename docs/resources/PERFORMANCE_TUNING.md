# Performance Tuning Guide

This guide covers performance optimization strategies for the ecom_datalake_pipelines project, from Bronze ingestion through Gold marts.

---

## Table of Contents

1. [Polars Optimization](#polars-optimization)
2. [DuckDB Optimization](#duckdb-optimization)
3. [BigQuery Optimization](#bigquery-optimization)
4. [Airflow Optimization](#airflow-optimization)
5. [Storage Optimization](#storage-optimization)
6. [Monitoring & Profiling](#monitoring--profiling)

---

## Polars Optimization

### LazyFrame vs DataFrame

**Always prefer LazyFrames for transformations:**

```python
# Good - Lazy evaluation with query optimization
df = pl.scan_parquet("data/bronze/orders/*.parquet")
result = (
    df
    .filter(pl.col("order_dt") >= "2020-01-01")
    .group_by("customer_id")
    .agg(pl.col("total_amount").sum())
    .collect()  # Execute optimized query plan
)

# Avoid - Eager evaluation, no optimization
df = pl.read_parquet("data/bronze/orders/*.parquet")
result = (
    df
    .filter(pl.col("order_dt") >= "2020-01-01")
    .group_by("customer_id")
    .agg(pl.col("total_amount").sum())
)
```

**Benefits of LazyFrames:**
- Query optimization (predicate pushdown, projection pushdown)
- Parallel execution planning
- Memory-efficient streaming for large datasets
- Automatic filter/projection reordering

### Predicate Pushdown

Filters should be applied as early as possible to reduce data scanned:

```python
# Good - Filter pushed down to scan
df = (
    pl.scan_parquet("data/bronze/orders/*.parquet")
    .filter(pl.col("order_dt").is_between("2020-01-01", "2020-01-31"))
    .select(["order_id", "customer_id", "total_amount"])
    .collect()
)

# Avoid - Filter applied after loading all data
df = (
    pl.scan_parquet("data/bronze/orders/*.parquet")
    .select(["order_id", "customer_id", "total_amount", "order_dt"])
    .collect()
    .filter(pl.col("order_dt").is_between("2020-01-01", "2020-01-31"))
)
```

### Projection Pushdown

Select only required columns early:

```python
# Good - Select columns early
df = (
    pl.scan_parquet("data/bronze/orders/*.parquet")
    .select(["order_id", "customer_id", "total_amount"])
    .filter(pl.col("total_amount") > 100)
    .collect()
)

# Avoid - Reading all columns
df = (
    pl.scan_parquet("data/bronze/orders/*.parquet")
    .filter(pl.col("total_amount") > 100)
    .select(["order_id", "customer_id", "total_amount"])
    .collect()
)
```

### Partition Pruning

Use Hive-style partitioning for date-based filtering:

```python
# Partitioned structure: data/bronze/orders/ingest_dt=2020-01-01/*.parquet
df = pl.scan_parquet(
    "data/bronze/orders/**/*.parquet",
    hive_partitioning=True
)
# Filter on partition column - only scans matching partitions
.filter(pl.col("ingest_dt") == "2020-01-01")
```

### Join Optimization

**Choose the right join strategy:**

```python
# Small table on right (broadcast join)
large_df.join(small_df, on="key", how="left")

# For very large joins, ensure both sides are sorted if possible
left = df1.sort("join_key")
right = df2.sort("join_key")
result = left.join(right, on="join_key")
```

**Use `join_asof` for time-series joins:**

```python
# Efficient temporal join
carts.join_asof(
    orders,
    left_on="created_at",
    right_on="order_dt",
    by="customer_id",
    strategy="backward",
    tolerance="48h"
)
```

### Streaming Mode

For datasets too large for memory:

```python
df = pl.scan_parquet("huge_dataset/*.parquet")
result = (
    df
    .filter(...)
    .select(...)
    .collect(streaming=True)  # Process in chunks
)
```

### Parallel Processing

Control thread count for CPU-bound operations:

```python
# Adjust based on available cores (default is num_cores)
pl.Config.set_streaming_chunk_size(100_000)
pl.Config.set_global_string_cache(True)  # For categorical operations
```

---

## DuckDB Optimization

### Database Configuration

Optimize DuckDB settings in dbt profiles:

```yaml
# dbt/profiles.yml
dbt:
  outputs:
    dev:
      type: duckdb
      path: 'dev.duckdb'
      threads: 4
      extensions:
        - httpfs
        - parquet
      settings:
        memory_limit: '8GB'
        max_memory: '8GB'
        threads: 4
        preserve_insertion_order: false
```

### Query Optimization

**Use CTEs wisely:**

```sql
-- Good - CTEs for readability, but not excessive nesting
WITH base_orders AS (
    SELECT * FROM {{ ref('stg_ecommerce__orders') }}
    WHERE order_dt >= '2020-01-01'
),
aggregated AS (
    SELECT
        customer_id,
        SUM(total_amount) as total_spent
    FROM base_orders
    GROUP BY customer_id
)
SELECT * FROM aggregated;

-- Consider materialization for reused CTEs
{{ config(materialized='table') }}
```

**Avoid SELECT \*:**

```sql
-- Good - Explicit columns
SELECT order_id, customer_id, total_amount
FROM {{ ref('stg_ecommerce__orders') }}

-- Avoid - Reads all columns
SELECT * FROM {{ ref('stg_ecommerce__orders') }}
```

**Use efficient aggregations:**

```sql
-- Good - Single pass aggregation
SELECT
    customer_id,
    COUNT(*) as order_count,
    SUM(total_amount) as total_spent,
    AVG(total_amount) as avg_order_value
FROM orders
GROUP BY customer_id;

-- Avoid - Multiple passes
SELECT
    customer_id,
    (SELECT COUNT(*) FROM orders o2 WHERE o2.customer_id = o1.customer_id) as order_count
FROM orders o1;
```

### Partition Filters

DuckDB supports Hive partitioning for Parquet:

```sql
-- Reads only ingest_dt=2020-01-01 partition
SELECT * FROM read_parquet(
    'data/bronze/orders/**/*.parquet',
    hive_partitioning=true
)
WHERE ingest_dt = '2020-01-01';
```

### Parallel Scans

DuckDB automatically parallelizes Parquet scans. Ensure files are:
- Reasonably sized (10-100MB per file)
- Row group size ~100K-1M rows
- Properly partitioned by date

---

## BigQuery Optimization

### Table Partitioning

Use date partitioning for temporal queries:

```sql
-- Create partitioned table
CREATE TABLE IF NOT EXISTS `project.silver.int_attributed_purchases`
PARTITION BY order_dt
CLUSTER BY customer_id, product_id
AS SELECT * FROM ...
```

### Clustering

Cluster on commonly filtered/joined columns:

```sql
-- Good clustering choices
CLUSTER BY customer_id, order_dt  -- For customer analysis queries
CLUSTER BY product_id, order_dt   -- For product analysis queries
```

### Query Optimization

**Avoid SELECT \*:**

```sql
-- Good - Specify columns
SELECT order_id, customer_id, total_amount
FROM `project.silver.orders`

-- Avoid - Scans all columns (costly in BQ)
SELECT * FROM `project.silver.orders`
```

**Use partition filters:**

```sql
-- Good - Scans only one partition
SELECT * FROM `project.silver.orders`
WHERE order_dt = '2020-01-01'

-- Avoid - Full table scan
SELECT * FROM `project.silver.orders`
WHERE EXTRACT(YEAR FROM order_dt) = 2020
```

**Optimize joins:**

```sql
-- Good - Join on clustered columns
SELECT o.*, c.name
FROM `project.silver.orders` o
JOIN `project.silver.customers` c
  ON o.customer_id = c.customer_id
WHERE o.order_dt >= '2020-01-01'  -- Partition filter

-- Avoid - Non-partitioned/clustered joins
SELECT o.*, c.name
FROM `project.silver.orders` o
JOIN `project.silver.customers` c
  ON o.customer_email = c.email  -- Not indexed
```

### Cost Control

**Use LIMIT for development:**

```sql
-- Development queries
SELECT * FROM `project.silver.orders`
WHERE order_dt = '2020-01-01'
LIMIT 1000;
```

**Monitor bytes processed:**

```python
# In dbt or Python BigQuery client
job_config = bigquery.QueryJobConfig(
    dry_run=True  # Check bytes before running
)
```

---

## Airflow Optimization

### Task Parallelism

Configure DAG concurrency:

```python
with DAG(
    dag_id="ecom_silver_to_gold_pipeline",
    max_active_runs=1,  # Prevent overlapping runs
    max_active_tasks=10,  # Parallel task limit
    concurrency=10,  # DEPRECATED: use max_active_tasks
) as dag:
    ...
```

### Task Groups for Organization

```python
with TaskGroup("enriched_silver") as enriched_group:
    # 10 tasks run in parallel (up to max_active_tasks)
    task1 = PythonOperator(...)
    task2 = PythonOperator(...)
    # ...
```

### Pool Management

Define resource pools in Airflow UI or config:

```python
BashOperator(
    task_id="heavy_computation",
    pool="bigquery_pool",  # Limit concurrent BQ queries
    pool_slots=2,
)
```

### XCom Optimization

**Avoid large XCom payloads:**

```python
# Good - Pass paths/metadata
def load_config(**kwargs):
    return {"silver_path": "gs://bucket/silver"}

# Avoid - Passing large data
def load_data(**kwargs):
    df = pl.read_parquet("huge.parquet")
    return df.to_dict()  # Too large for XCom
```

### Retry Configuration

Use environment-appropriate retry settings (see [config.yml](../config/config.yml)):

```yaml
retry_config:
  prod:
    retries: 3
    retry_delay_minutes: 5
    retry_exponential_backoff: true
    max_retry_delay_minutes: 30
```

---

## Storage Optimization

### Parquet Settings

**Optimize for read performance:**

```python
# Writing Parquet files
df.write_parquet(
    "output.parquet",
    compression="zstd",  # Good compression + speed
    row_group_size=100_000,  # Balance parallelism & overhead
    statistics=True,  # Enable min/max stats for filtering
)
```

**Compression comparison:**
- `snappy`: Fast, moderate compression (default)
- `zstd`: Best compression ratio, good speed
- `gzip`: High compression, slower
- `lz4`: Fastest, lowest compression

### File Sizing

**Optimal file sizes:**
- **Bronze/Silver**: 10-100MB per file
- **Enriched**: 50-200MB per file
- **Gold**: Depends on query patterns

**Partition by date, limit files per partition:**

```python
# Configure in enriched runners
enriched_max_rows_per_file: 1000  # ~50-100MB Parquet files
```

### Hive Partitioning

Use Hive-style partitioning for temporal data:

```
data/
  bronze/
    orders/
      ingest_dt=2020-01-01/
        part-001.parquet
        part-002.parquet
      ingest_dt=2020-01-02/
        part-001.parquet
```

Benefits:
- Partition pruning during reads
- Easy retention management
- Clear data organization

---

## GCS & I/O Optimization

### Manifest-Based File Discovery

**Avoid recursive globbing on GCS (`**/*.parquet`):**
GCS is an object store, not a filesystem. Recursive listing is an `O(N)` operation where N is the number of files, leading to severe latency as the lake grows.

**Optimization Strategy:**
- **Produce Manifests:** The `base_silver` runner now auto-generates a `_MANIFEST.json` containing the list of files and row counts for every partition.
- **Read Manifests:** Validation scripts (`src.validation.common`) prioritize reading the manifest over listing objects. This turns a multi-minute "directory scan" into a sub-second JSON read.

### Batched Uploads vs. Sequential Copies

**Avoid loops calling `gcloud storage cp`:**
Executing a subprocess for every file or partition introduces massive Python and OS overhead.

**Good - Batched Sync:**
```python
# Generate manifests locally first, then sync once
subprocess.run(["gcloud", "storage", "rsync", "-r", local_path, remote_path])
```

**Impact:**
Refactoring the Dimension Refresh pipeline to use manifests and `rsync` reduced stage duration from **30+ minutes to ~2 minutes**.

---

## Monitoring & Profiling

### Polars Query Plans

Inspect LazyFrame query plans:

```python
lf = pl.scan_parquet("data.parquet").filter(...).select(...)
print(lf.explain())  # See optimized query plan
```

### DuckDB EXPLAIN

Profile query execution:

```sql
EXPLAIN ANALYZE
SELECT customer_id, COUNT(*)
FROM orders
WHERE order_dt >= '2020-01-01'
GROUP BY customer_id;
```

### BigQuery Query Plan

Use BigQuery Console or API:

```python
job = client.query(query)
for stage in job.query_plan:
    print(f"Stage {stage.name}: {stage.shuffle_output_bytes} bytes")
```

### Pipeline Metrics

Monitor execution times via observability layer:

```python
from src.observability.metrics import track_table_metrics

track_table_metrics(
    table_name="int_attributed_purchases",
    row_count=df.height,
    run_id="manual_2020-01-01",
    metrics={
        "execution_time_seconds": 45.2,
        "bytes_written": 1024 * 1024 * 50,
    }
)
```

### Validation Reports

Check validation report metrics:

```markdown
## Validation Summary
- Processing Time: 2.34s
- Rows Processed: 125,432
- Throughput: 53,594 rows/sec
```


---

## GCS & I/O Optimization

### Manifest-Based File Discovery

**Avoid recursive globbing on GCS (`**/*.parquet`):**
GCS is an object store, not a filesystem. Recursive listing is an `O(N)` operation where N is the number of files, leading to severe latency as the lake grows.

**Optimization Strategy:**
- **Produce Manifests:** The `base_silver` runner now auto-generates a `_MANIFEST.json` containing the list of files and row counts for every partition.
- **Read Manifests:** Validation scripts (`src.validation.common`) prioritize reading the manifest over listing objects. This turns a multi-minute "directory scan" into a sub-second JSON read.

### Batched Uploads vs. Sequential Copies

**Avoid loops calling `gcloud storage cp`:**
Executing a subprocess for every file or partition introduces massive Python and OS overhead.

**Good - Batched Sync:**
```python
# Generate manifests locally first, then sync once
subprocess.run(["gcloud", "storage", "rsync", "-r", local_path, remote_path])
```

**Impact:**
Refactoring the Dimension Refresh pipeline to use manifests and `rsync` reduced stage duration from **30+ minutes to ~2 minutes**.

---

## Monitoring & Profiling

### Polars Query Plans

Inspect LazyFrame query plans:

```python
lf = pl.scan_parquet("data.parquet").filter(...).select(...)
print(lf.explain())  # See optimized query plan
```

### DuckDB EXPLAIN

Profile query execution:

```sql
EXPLAIN ANALYZE
SELECT customer_id, COUNT(*)
FROM orders
WHERE order_dt >= '2020-01-01'
GROUP BY customer_id;
```

### BigQuery Query Plan

Use BigQuery Console or API:

```python
job = client.query(query)
for stage in job.query_plan:
    print(f"Stage {stage.name}: {stage.shuffle_output_bytes} bytes")
```

### Pipeline Metrics

Monitor execution times via observability layer:

```python
from src.observability.metrics import track_table_metrics

track_table_metrics(
    table_name="int_attributed_purchases",
    row_count=df.height,
    run_id="manual_2020-01-01",
    metrics={
        "execution_time_seconds": 45.2,
        "bytes_written": 1024 * 1024 * 50,
    }
)
```

### Validation Reports

Check validation report metrics:

```markdown
## Validation Summary
- Processing Time: 2.34s
- Rows Processed: 125,432
- Throughput: 53,594 rows/sec
```

---

## Benchmarking Checklist

When optimizing a pipeline stage:

1. **Profile before optimizing**
   - Measure current execution time
   - Identify bottleneck (I/O, CPU, memory)

2. **Apply targeted optimization**
   - LazyFrames for Polars
   - Partition pruning
   - Query pushdown
   - Parallel processing

3. **Measure improvement**
   - Compare execution time
   - Check memory usage
   - Validate correctness

4. **Document changes**
   - Add comments explaining optimization
   - Update metrics tracking

---

## Common Performance Issues

### Issue: Out of Memory (OOM)

**Symptoms:**
- Python process killed
- DuckDB "out of memory" errors

**Solutions:**
1. Use streaming in Polars: `.collect(streaming=True)`
2. Reduce DuckDB memory_limit
3. Process data in date partitions
4. Increase worker memory allocation

### Issue: Slow Joins

**Symptoms:**
- Join operations taking >1 minute
- High CPU usage during joins

**Solutions:**
1. Ensure join keys are sorted
2. Use appropriate join type (left, inner, semi)
3. Filter before joining
4. Consider broadcasting small tables

### Issue: Excessive I/O

**Symptoms:**
- High read/write times
- Network bottlenecks

**Solutions:**
1. Use columnar formats (Parquet)
2. Enable compression (zstd)
3. Optimize file sizes (50-100MB)
4. Use local storage for intermediate results

### Issue: Airflow Task Delays

**Symptoms:**
- Tasks queued but not running
- Long gaps between tasks

**Solutions:**
1. Increase `max_active_tasks`
2. Use task pools for resource management
3. Optimize XCom usage
4. Check worker capacity

---

## Docker Performance

### macOS VirtioFS Optimization

On macOS with Docker Desktop, file locking can cause `Errno 35` (Resource temporarily unavailable) errors. The pipeline redirects writable paths to `/tmp` inside the container:

```yaml
# docker-compose.yml - Environment variables
DBT_TARGET_PATH: "/tmp/dbt_target"       # dbt compiled artifacts
DBT_LOG_PATH: "/tmp/dbt_logs"            # dbt log files
DBT_DUCKDB_PATH: "/tmp/dbt_duckdb/ecom.duckdb"  # DuckDB database
METRICS_BASE_PATH: "/tmp/metrics"        # Metrics output
LOGS_BASE_PATH: "/tmp/logs"              # Log output
DBT_PARTIAL_PARSE: "false"               # Disable partial parsing (cache conflicts)
```

### Volume Mount Performance

**Use named volumes for data directories** (not host paths):

```yaml
# Good - Named volume (fast)
volumes:
  - airflow-data:/opt/airflow/data

# Avoid - Host mount (slow on macOS)
# - ./data:/opt/airflow/data
```

**Exception**: Mount source code for hot-reload during development:
```yaml
# Development only - for live code changes
volumes:
  - ./src:/opt/airflow/src
  - ./config:/opt/airflow/config
```

### DuckDB Single-Writer Constraint

DuckDB uses file-level locks. In Docker:
- Base Silver runs as a single dbt task (not per-model)
- Avoids concurrent write conflicts
- In warehouse-backed prod (BigQuery), split into per-model tasks

See [DEPLOYMENT_GUIDE.md - Limitations](DEPLOYMENT_GUIDE.md#limitations--constraints-portfolio-scope) for details.

---

## Environment-Specific Tuning

### Local Development

```yaml
# config/config.yml
environment: local
enriched_lookback_days: 0  # Process only current date
enriched_max_rows_per_file: 1000  # Smaller files for faster iteration
```

```python
# Limit data for faster testing
df = pl.scan_parquet("bronze/orders/**/*.parquet")
  .filter(pl.col("ingest_dt") == "2020-01-01")  # Single date
  .head(10000)  # Limit rows
  .collect()
```

### Development Environment

```yaml
environment: dev
enriched_lookback_days: 7
threads: 4
memory_limit: '8GB'
```

### Production Environment

```yaml
environment: prod
enriched_lookback_days: 30  # Full historical context
threads: 8
memory_limit: '16GB'
retry_config:
  prod:
    retries: 3
    retry_exponential_backoff: true
```

---

---

## Related Documentation

- **[SLA_AND_QUALITY.md](SLA_AND_QUALITY.md)** - Processing time SLAs and table-level performance targets
- **[OBSERVABILITY_STRATEGY.md](OBSERVABILITY_STRATEGY.md)** - Metrics tracking for performance monitoring
- **[CONFIG_STRATEGY.md](CONFIG_STRATEGY.md)** - Environment-specific configuration tuning
- **[ENVIRONMENT_VARIABLE_STRATEGY.md](ENVIRONMENT_VARIABLE_STRATEGY.md)** - Performance-related env vars

---

## Further Reading

- [Polars Performance Guide](https://pola-rs.github.io/polars-book/user-guide/lazy/query-plan/)
- [DuckDB Performance Guide](https://duckdb.org/docs/guides/performance/overview)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices-performance-overview)
- [Airflow Performance Tuning](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

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
