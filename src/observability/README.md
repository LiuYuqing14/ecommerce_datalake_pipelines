# Observability Framework

Comprehensive metrics collection and structured logging for the e-commerce data pipeline.

## Features

- **Environment-Aware**: Automatically adapts storage based on `PIPELINE_ENV`
  - Local: Writes to `data/metrics/` and `data/logs/`
  - Production: Writes to GCS buckets
- **Structured Metrics**: JSON-formatted metrics for easy analysis
- **Structured Logging**: JSONL (JSON Lines) logs for aggregation systems
- **Type-Safe**: Full type hints for IDE autocomplete
- **Zero Configuration**: Works out of the box in local mode
- **Run Context**: Auto-binds `run_id`, `dag_id`, and `task_id` from Airflow env vars

**Portfolio note:** Metrics are stored as JSON artifacts; in production you would
also emit key metrics to a time-series backend (Prometheus/StatsD/Cloud Monitoring)
for alerting.

## Directory Structure

```
data/
├── metrics/
│   ├── pipeline_runs/          # End-to-end pipeline execution metrics
│   ├── data_quality/           # Bronze validation metrics
│   ├── silver_quality/         # Silver transformation metrics
│   ├── enriched_silver/        # Enriched Silver (Polars) metrics
│   └── bronze_metadata/        # Bronze metadata validation
└── logs/
    ├── errors/                 # Error logs (JSONL)
    ├── audit/                  # Audit trail (JSONL)
    └── debug/                  # Debug logs (text)
```

## Usage Examples

### 1. Writing Metrics

#### Pipeline Run Metrics

```python
from datetime import datetime
from src.observability.metrics import write_pipeline_run_metric

# At start of pipeline
start_time = datetime.now()
run_id = f"{start_time:%Y%m%d_%H%M%S}_{dag_run.run_id}"

# ... run pipeline phases ...

# At end of pipeline
end_time = datetime.now()
write_pipeline_run_metric(
    run_id=run_id,
    status="SUCCESS",
    start_time=start_time,
    end_time=end_time,
    phase_timings=[
        {
            "phase": "bronze_validation",
            "duration_seconds": 83,
            "status": "SUCCESS"
        },
        {
            "phase": "base_silver",
            "duration_seconds": 625,
            "status": "SUCCESS",
            "parallel_tasks": 8
        }
    ],
    # Optional: Add custom fields
    airflow_dag_id=dag.dag_id,
    triggered_by="scheduler"
)
```

#### Data Quality Metrics

```python
from src.observability.metrics import write_data_quality_metric

table_metrics = [
    {
        "table": "orders",
        "row_count": {
            "actual": 2456789,
            "expected": 2500000,
            "deviation_pct": -1.73,
            "status": "PASS"
        },
        "schema_validation": {
            "status": "PASS",
            "missing_columns": [],
            "extra_columns": []
        }
    }
]

write_data_quality_metric(
    run_id=run_id,
    validation_type="bronze_metadata",
    table_metrics=table_metrics,
    overall_status="PASS",
    validator_version="1.2.0"
)
```

#### Silver Quality Metrics

```python
from src.observability.metrics import write_silver_quality_metric

table_metrics = [
    {
        "table": "orders",
        "row_counts": {
            "bronze_input": 2456789,
            "silver_output": 2398456,
            "quarantine_output": 58333
        },
        "pass_rate": {
            "rate": 0.9762,
            "sla_threshold": 0.95,
            "status": "PASS"
        },
        "quarantine_breakdown": [
            {"reason": "customer_fk_invalid", "count": 32145, "percentage": 55.1},
            {"reason": "duplicate_order_id", "count": 15678, "percentage": 26.9}
        ]
    }
]

write_silver_quality_metric(
    run_id=run_id,
    table_metrics=table_metrics,
    overall_status="PASS"
)
```

### 2. Custom Metrics Writers

```python
from src.observability import get_metrics_writer

# Create a custom metrics writer
writer = get_metrics_writer("custom_metrics")

# Write any metric
writer.write_metric({
    "custom_field_1": "value1",
    "custom_field_2": 12345,
    "nested": {
        "data": "here"
    }
}, run_id="my_run_id")

# Read metrics back
recent_metrics = writer.read_metrics(limit=10)
latest = writer.get_latest_metric()
```

### 3. Structured Logging

```python
from src.observability import get_logger

logger = get_logger(__name__)

# Info logging
logger.info("Starting Bronze validation", table="orders", row_count=1000000)

# Warning logging
logger.warning("Pass rate below baseline", table="cart_items", pass_rate=0.92)

# Error logging (automatically writes to errors JSONL)
try:
    # ... some operation ...
    pass
except Exception as e:
    logger.error(
        "Failed to process table",
        error_type="DuckDBException",
        table="orders",
        context={"row_count": 2456789, "memory_limit_mb": 4096}
    )

# Metric logging
logger.metric("rows_processed", 1000000, table="orders", phase="silver")
```

**Production note:** GCS is append-unfriendly, so logs are written as small, unique
JSONL objects per event in dev/prod. In real deployments, prefer stdout logging
with a collector (Cloud Logging/Datadog).

### 4. Convenience Functions

```python
from src.observability import log_metric, log_error

# Quick metric logging
log_metric("quarantine_rate", 0.024, table="orders")
log_metric("processing_time_seconds", 127.5, table="orders", phase="dbt")

# Quick error logging
log_error(
    "Memory limit exceeded during transformation",
    error_type="MemoryError",
    table="orders",
    memory_used_mb=4200,
    memory_limit_mb=4096
)
```

## Environment Configuration

### Local Development (Default)

```bash
# No configuration needed!
# Metrics written to: data/metrics/
# Logs written to: data/logs/
```

### Production Deployment

```bash
# Set environment variables
export PIPELINE_ENV=prod
export METRICS_BUCKET=ecom-datalake-metrics
export LOGS_BUCKET=ecom-datalake-logs  # Optional, defaults to METRICS_BUCKET

# Metrics written to: gs://ecom-datalake-metrics/pipeline_metrics/
# Logs written to: gs://ecom-datalake-logs/pipeline_logs/
```

## Metric Schema Reference

### Pipeline Run Metric

```json
{
  "metadata": {
    "written_at": "2026-01-11T14:50:00Z",
    "environment": "prod",
    "metric_type": "pipeline_runs"
  },
  "run_metadata": {
    "run_id": "20260111_143022_abc123",
    "pipeline_name": "ecom_silver_to_gold",
    "environment": "prod",
    "start_time": "2026-01-11T14:30:22Z",
    "end_time": "2026-01-11T14:47:15Z",
    "duration_seconds": 1013,
    "status": "SUCCESS"
  },
  "phase_timings": [...]
}
```

### Data Quality Metric

```json
{
  "metadata": {...},
  "validation_metadata": {
    "run_id": "...",
    "validation_type": "bronze_metadata",
    "timestamp": "...",
    "validator_version": "1.2.0"
  },
  "table_metrics": [...],
  "overall_status": "PASS"
}
```

### Error Log Entry (JSONL)

```json
{"timestamp": "2026-01-11T14:35:12Z", "level": "ERROR", "component": "dbt.orders", "error_type": "DuckDBException", "message": "Memory limit exceeded", "context": {...}, "traceback": "..."}
```

## Integration with Airflow

### Example DAG Integration

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.observability import get_logger, write_pipeline_run_metric

logger = get_logger(__name__)

def track_phase_timing(**context):
    """Track phase timing and write metric."""
    task_instance = context['task_instance']
    start_time = task_instance.start_date
    end_time = datetime.now()

    logger.info(
        f"Phase {task_instance.task_id} completed",
        duration_seconds=(end_time - start_time).total_seconds()
    )

with DAG(...) as dag:
    task = PythonOperator(
        task_id='my_task',
        python_callable=my_function,
        on_success_callback=track_phase_timing
    )
```

## Querying Metrics

### Local (Python)

```python
from src.observability import get_metrics_writer

writer = get_metrics_writer("data_quality")

# Get last 10 runs
recent_runs = writer.read_metrics(limit=10)

# Analyze pass rates over time
for metric in recent_runs:
    for table in metric["table_metrics"]:
        print(f"{table['table']}: {table['row_count']['status']}")
```

### Production (BigQuery)

```sql
-- Load metrics from GCS to BigQuery
LOAD DATA OVERWRITE my_dataset.pipeline_metrics
FROM FILES (
  format = 'JSON',
  uris = ['gs://ecom-datalake-metrics/pipeline_metrics/data_quality_*.json']
);

-- Query pass rates over time
SELECT
  DATE(metadata.written_at) as date,
  table_metric.table,
  table_metric.row_count.status,
  COUNT(*) as runs
FROM my_dataset.pipeline_metrics,
  UNNEST(table_metrics) as table_metric
GROUP BY 1, 2, 3
ORDER BY date DESC;
```

## Best Practices

1. **Always include run_id**: Ties all metrics to a specific pipeline execution
2. **Use structured context**: Pass dictionaries to logger, not formatted strings
3. **Write metrics at key points**: Start, end, and after each major phase
4. **Include SLA thresholds**: Store expected values alongside actuals for easy comparison
5. **Persist before failing**: Write metrics even if pipeline fails (use try/finally)

## Testing

```python
# Test local writing
import os
os.environ["PIPELINE_ENV"] = "local"

from src.observability import get_config

config = get_config()
assert config.is_local
assert "data/metrics" in config.metrics_base_path

# Initialize directories
config.ensure_local_dirs()

# Write a test metric
from src.observability import get_metrics_writer
writer = get_metrics_writer("test_metrics")
path = writer.write_metric({"test": "data"}, run_id="test_123")
print(f"Wrote metric to: {path}")
```

## Future Enhancements

- [ ] Prometheus integration for real-time monitoring
- [ ] Grafana dashboard templates
- [ ] Automatic anomaly detection (ML-based)
- [ ] Slack/PagerDuty alerting integration
- [ ] dbt artifacts parser (read manifest.json, run_results.json)
- [ ] Historical baseline tracking with statistical deviation alerts

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
