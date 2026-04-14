# Audit Schema

## Purpose
Define the canonical audit log record for Silver and Enriched Silver runs.
These records are written as JSON to GCS and optionally loaded into BigQuery.

## Storage
- GCS path: `gs://<silver-bucket>/silver/ecom/_audit/run_id=<run_id>/<table>/<partition_key>=YYYY-MM-DD/summary.json`
- BigQuery table: `silver.audit_runs`
- Local dev: `duckdb` file with `audit_runs` table

## Schema
| Field | Type | Required | Description |
| --- | --- | --- | --- |
| run_id | STRING | Yes | Unique run identifier for a pipeline execution. |
| table_name | STRING | Yes | Table name being processed. |
| ingest_dt | DATE | Yes | Ingest date of the source partition. |
| partition_key | STRING | Yes | Partition key used for output. |
| partition_value | STRING | Yes | Partition value (YYYY-MM-DD). |
| schema_version | STRING | Yes | Data contract schema version. |
| input_rows | INT64 | Yes | Input row count. |
| output_rows | INT64 | Yes | Output row count. |
| reject_rows | INT64 | Yes | Rejected row count. |
| started_at | TIMESTAMP | Yes | Run start timestamp (UTC). |
| ended_at | TIMESTAMP | Yes | Run end timestamp (UTC). |
| duration_sec | FLOAT64 | Yes | Duration in seconds. |
| quality_failures | ARRAY<STRING> | No | Summary list of quality failures. |

## Notes
- Audit logs are emitted per table and partition.
- The audit table is append-only.
- SLA dashboards and alerting read from the BigQuery audit table.

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
