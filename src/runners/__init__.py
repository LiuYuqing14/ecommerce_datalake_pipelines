"""
Enriched Silver runners.

These modules handle I/O for Enriched Silver transforms:
- Read Base Silver parquet from GCS
- Call pure Polars transform logic from src/transforms/
- Write Enriched Silver parquet to GCS

Design:
- Pure logic lives in src/transforms/ (testable, no I/O)
- I/O wrappers live here (read/write GCS, error handling)
- Called directly by Airflow tasks (not via dbt)
"""
