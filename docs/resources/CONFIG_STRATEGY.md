# Configuration Strategy

## Design Philosophy: Config-First with Environment Overrides

**Primary Source:** `config/config.yml` (version controlled, discoverable)  
**Override Mechanism:** Environment variables (deployment-specific)

This hybrid approach gives you:
- ✅ Single source of truth in Git
- ✅ Deployment flexibility via env vars
- ✅ Clear precedence rules
- ✅ Self-documenting defaults

---

## Configuration Hierarchy (Priority Order)

```
┌─────────────────────────────────────┐
│ 1. Environment Variables (HIGHEST) │  ← Deployment-specific overrides
├─────────────────────────────────────┤
│ 2. config/config.yml                │  ← Version-controlled defaults
├─────────────────────────────────────┤
│ 3. Code Defaults (LOWEST)           │  ← Fallback if config missing
└─────────────────────────────────────┘
```

### Example: `metrics_bucket`

**Lookup order:**
1. Check `METRICS_BUCKET` env var → Use if set
2. Check `config.yml` → `pipeline.metrics_bucket`
3. Fall back to hardcoded default: `"ecom-datalake-metrics"`

---

## Observability Configuration

### Local Development (Default)

**config/config.yml:**
```yaml
pipeline:
  environment: "local"
  metrics_bucket: "ecom-datalake-metrics"  # Unused in local mode
  logs_bucket: "ecom-datalake-logs"        # Unused in local mode
```

**Result:**
- Metrics → `./data/metrics/`
- Logs → `./data/logs/`
- No GCS access needed

**No .env file required!** ✅

---

### Development Environment

**Option 1: Update config.yml (Recommended)**

Create `config/config.dev.yml`:
```yaml
pipeline:
  environment: "dev"
  metrics_bucket: "ecom-datalake-dev-metrics"
  logs_bucket: "ecom-datalake-dev-logs"
  project_id: "my-dev-project"
  # ... other dev settings
```

Then run: `python -m src.validation.silver --config config/config.dev.yml`

**Option 2: Environment Variables**

Keep `config.yml` unchanged, override via env:
```bash
export PIPELINE_ENV=dev
export GOOGLE_CLOUD_PROJECT=my-dev-project
export METRICS_BUCKET=ecom-datalake-dev-metrics
python -m src.validation.silver
```

**Dev intent:** Same validation behavior as `local`, but pointing at cloud paths (GCS/BQ) for server-based testing.

---

### Production Environment

**Option 1: Update config.yml on deployment**

Production repo has different `config.yml`:
```yaml
pipeline:
  environment: "prod"
  project_id: "my-prod-project"
  bronze_bucket: "ecom-prod-bronze"
  silver_bucket: "ecom-prod-silver"
  metrics_bucket: "ecom-prod-metrics"
  logs_bucket: "ecom-prod-logs"
```

**Option 2: Environment Variables (Recommended for Airflow)**

Set in Airflow environment:
```bash
PIPELINE_ENV=prod
GOOGLE_CLOUD_PROJECT=my-prod-project
METRICS_BUCKET=ecom-prod-metrics
LOGS_BUCKET=ecom-prod-logs
```

This keeps `config.yml` generic across environments.

---

## All Configuration Options

### In config/config.yml

```yaml
pipeline:
  # GCP Configuration
  project_id: "your-gcp-project"

  # Data Lake Buckets (use "local" for local filesystem)
  bronze_bucket: "local"
  bronze_prefix: "samples/bronze"
  silver_bucket: "local"
  silver_base_prefix: "data/silver/base"
  silver_enriched_prefix: "data/silver/enriched"

  # BigQuery Datasets
  bigquery_dataset: "silver"
  gold_dataset: "gold_marts"

  # Observability & Metrics
  environment: "local"                      # local, dev, or prod
  metrics_bucket: "ecom-datalake-metrics"   # Used when environment=dev/prod
  logs_bucket: "ecom-datalake-logs"         # Used when environment=dev/prod

  # Business Logic Configuration
  default_ingest_dt: "2020-01-01"
  attribution_tolerance_hours: 48
  churn_danger_window_days: [30, 90]
  sales_velocity_window_days: 7
  max_quarantine_pct: 5.0
  max_row_loss_pct: 1.0
  min_return_id_distinct_ratio: 0.001
```

### Environment Variable Overrides

**See `.envrc` and `.env.example` for all options.**

Key overrides:

#### Pipeline & Environment

- `PIPELINE_ENV` → Overrides `pipeline.environment` (local, dev, prod)
- `GOOGLE_CLOUD_PROJECT` → Overrides `pipeline.project_id`
- `GCS_BUCKET` → Bronze data bucket for GCS operations
- `ECOM_SPEC_PATH` → Overrides the spec directory or YAML file (default: `config/specs`)

#### Observability

- `METRICS_BUCKET` → Overrides observability metrics bucket (observability only)
- `LOGS_BUCKET` → Overrides observability logs bucket (observability only)
- `OBSERVABILITY_ENV` → Overrides observability environment (observability only)
- `METRICS_BASE_PATH` → Local path for metrics output (default: `/tmp/metrics` in Docker)
- `LOGS_BASE_PATH` → Local path for logs output (default: `/tmp/logs` in Docker)

#### Data Paths

- `BRONZE_BASE_PATH` → Overrides dbt var (for dbt models)
- `SILVER_BASE_PATH` → Overrides dbt var (for dbt models)
- `SILVER_QUARANTINE_PATH` → Overrides silver quarantine output path
- `SILVER_ENRICHED_PATH` → Overrides silver enriched output path
- `SILVER_DIMS_PATH` → Overrides dims snapshot base path
- `SILVER_DIMS_LOCAL_PATH` → Local dims snapshot path (used when `SILVER_DIMS_PATH` is gs://)
- `SILVER_GCS_TARGET` → Overrides silver base GCS sync target
- `SILVER_ENRICHED_GCS_TARGET` → Overrides silver enriched GCS sync target
- `SILVER_STAGING_PATH` → Override Base Silver staging prefix (optional)
- `DIMS_STAGING_PATH` → Override Dims staging prefix (optional)
- `ENRICHED_STAGING_PATH` → Override Enriched staging prefix (optional)

#### GCP Auth (Secrets)

- `USE_SA_AUTH` → Use service-account auth instead of ADC (default: false)
- `CLOUDSDK_CONFIG` → ADC config path (default: `/home/airflow/.config/gcloud`)
- `GCP_SA_KEY_PATH` → Host path for the service account key (docker-compose mount)
- `GOOGLE_APPLICATION_CREDENTIALS` → In-container path to the mounted key

#### dbt / DuckDB (Docker paths)

- `DBT_TARGET_PATH` → dbt compiled artifacts (default: `/tmp/dbt_target`)
- `DBT_LOG_PATH` → dbt log files (default: `/tmp/dbt_logs`)
- `DBT_DUCKDB_PATH` → DuckDB database file (default: `/tmp/dbt_duckdb/ecom.duckdb`)
- `DBT_PARTIAL_PARSE` → Enable/disable dbt partial parsing (default: `false`)

#### Feature Flags

- `GOLD_PIPELINE_ENABLED` → Enable BigQuery Gold layer (default: `false`)
- `BQ_LOAD_ENABLED` → Enable Enriched Silver BigQuery loads (default: `false`)
- `BRONZE_QA_REQUIRED` → Require Bronze QA phase (default: `true`)
- `BRONZE_QA_FAIL` → Fail pipeline on Bronze issues (default: `false`)
- `SILVER_PUBLISH_MODE` → Export mode for Base Silver (direct or staging) (default: `direct`)
- `ENRICHED_PUBLISH_MODE` → Export mode for Enriched Silver (direct or staging) (default: `direct`)
- `DIMS_PUBLISH_MODE` → Export mode for Dims snapshots (direct or staging) (default: `direct`)

When `SILVER_PUBLISH_MODE=staging`, exports go to:
- `gs://.../silver/base/_staging/<run_id>/...`
- `_MANIFEST.json` written under the staging prefix
- Canonical publish happens via **explicit promote** after validation
- `STRICT_FK` → Enforce FK validation in Silver (default: `false`)

---

## When to Use Each Approach

### Use config.yml When:
- ✅ Setting applies to all developers (default paths, business logic)
- ✅ Value should be version controlled
- ✅ You want changes tracked in Git
- ✅ Configuration is environment-independent

### Use Environment Variables When:
- ✅ Value differs per deployment (dev vs prod buckets)
- ✅ Value contains secrets (API keys, credentials)
- ✅ You need quick local overrides during debugging
- ✅ Running in CI/CD or Airflow

**Secret examples (env-only):**
- `GOOGLE_APPLICATION_CREDENTIALS` (in-container path)
- `GCP_SA_KEY_PATH` (host path for docker-compose mount)

---

## Recommended Local vs Docker Split

For day-to-day development, keep your local shell in **local** mode and run
Docker/Airflow in **dev** mode so GCS paths and staging behavior can be tested
without changing your laptop defaults.

**Local shell (direnv defaults):**
- `PIPELINE_ENV=local`
- local filesystem paths in `.envrc` (e.g. `data/bronze`, `data/silver/*`)
- `SILVER_PUBLISH_MODE=direct`
- `ENRICHED_PUBLISH_MODE=direct`
- `DIMS_PUBLISH_MODE=direct`

**Docker/Airflow (docker-compose defaults):**
- `PIPELINE_ENV=dev`
- GCS paths/buckets for Bronze/Silver/Enriched/Dims
- `SILVER_PUBLISH_MODE=staging`
- `ENRICHED_PUBLISH_MODE=staging`
- `DIMS_PUBLISH_MODE=staging`

This keeps local iterations fast while Docker validates staging + manifest flow.

---

## Migration Guide: Local → Production

### Step 1: Update config.yml

**Before (local dev):**
```yaml
pipeline:
  environment: "local"
  bronze_bucket: "local"
  silver_bucket: "local"
```

**After (production-ready):**
```yaml
pipeline:
  environment: "prod"
  project_id: "my-prod-project"
  bronze_bucket: "ecom-prod-bronze"
  silver_bucket: "ecom-prod-silver"
  metrics_bucket: "ecom-prod-metrics"
  logs_bucket: "ecom-prod-logs"
```

### Step 2: Create GCS Buckets

```bash
gsutil mb -p my-prod-project gs://ecom-prod-metrics
gsutil mb -p my-prod-project gs://ecom-prod-logs
```

### Step 3: Grant Permissions

```bash
gsutil iam ch serviceAccount:airflow@my-prod-project.iam.gserviceaccount.com:roles/storage.objectAdmin \
  gs://ecom-prod-metrics

gsutil iam ch serviceAccount:airflow@my-prod-project.iam.gserviceaccount.com:roles/storage.objectAdmin \
  gs://ecom-prod-logs
```

### Step 4: Deploy

No environment variables needed - config.yml has everything!

---

## Best Practices

### ✅ DO

- **Commit config.yml** with sensible defaults
- **Use env vars** for deployment-specific overrides
- **Document** any required env vars in README
- **Version control** production config separately if needed
- **Use `config.prod.yml`** and pass `--config` flag in production

### ❌ DON'T

- Don't hardcode production bucket names in code
- Don't commit `.env` files (use `.env.example`)
- Don't duplicate config across files
- Don't use env vars for business logic (use config.yml)

---

## Troubleshooting

### "Metrics writing to wrong location"

Check config precedence:
```python
from src.observability import get_config
config = get_config()
print(f"Environment: {config.environment}")
print(f"Metrics path: {config.metrics_base_path}")
```

### "Can't find config.yml"

Pass explicit path:
```bash
python -m src.validation.silver --config /path/to/config.yml
```

### "Env vars not taking effect"

Verify they're exported:
```bash
env | grep PIPELINE_ENV
env | grep METRICS_BUCKET
```

---

## Summary: Your Question Answered

> **Q:** Should `METRICS_BUCKET` and `LOGS_BUCKET` be env or config driven?

**A:** **Config-driven with env overrides** (hybrid approach)

**What we implemented:**
1. ✅ **Primary:** `config/config.yml` contains default values
2. ✅ **Override:** Environment variables can override config
3. ✅ **Fallback:** Code has hardcoded defaults if both missing

**Precedence:** `ENV VAR > config.yml > code default`

**Result:** 
- Local dev works with **zero configuration**
- Production can use **config.yml OR env vars** (your choice!)
- Maximum flexibility with clear, predictable behavior

---

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
