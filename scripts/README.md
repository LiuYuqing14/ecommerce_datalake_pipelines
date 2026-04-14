## Scripts Index

Short guide to the scripts in this folder. The **core pipeline** is under `src/`; these scripts are helpers, runners, and utilities.

### Core local runners

- `run_dims_from_spec.py` — Runs dbt dims models and builds dimension snapshots.
- `run_enriched_all_samples.py` — Runs Polars enriched transforms for sample dates.
- `run_dev_pipeline.sh` — Full local pipeline while reading/writing to GCS (dev-style).
- `run_sim_prod_gcs.sh` — Simulate prod run against GCS with quality gates.
- `bootstrap_airflow.sh` — Initializes local Airflow instance.

### Validation & profiling

- `describe_parquet_samples.py` — Profiles local Bronze samples and writes docs/metrics.
- `validate_bronze_samples.py` — Validates Bronze samples against schemas.
- `report_bronze_sizes.sh` — Storage/size report for Bronze samples.

### Data acquisition / utilities

- `pull_bronze_sample.sh` — Pulls Bronze sample partitions from GCS.
- `version.py` — Prints pipeline version metadata.

### Adhoc utilities (not part of the pipeline)

`adhoc/` contains one-off inspection scripts used during data debugging:

- `expand_bronze_samples.sh` — Expands/normalizes sample layout.
- `inspect_cart_items_spikes.py` / `inspect_cart_items_spikes_all.py` — Spot checks for cart anomalies.
- `inspec_returns.py` — Returns data spot checks.
- `parquet_run_scan.py` / `parquet_sanity_checks.py` — Lightweight parquet scans.
- `spot_check_bronze_profiles.py` — Quick checks on profiling outputs.
- `update_footers.py` — Updates doc footers/metadata.

### Usage notes

- Most scripts assume repo root as CWD.
- Local demo helpers rely on `samples/bronze` being present (run `unzip samples/bronze_samples.zip -d samples/`).
- GCS-facing scripts require `gcloud` auth and appropriate env vars (see `docker.env.gcs.example` and `.env.example`).
