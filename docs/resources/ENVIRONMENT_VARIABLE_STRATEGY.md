# Environment Variable Strategy

## Three Categories of Configuration

### 1. Infrastructure Config (Config-First with Env Overrides)

**Pattern:** `config.yml` → env var → code default

**When to use:**
- Deployment-specific settings (buckets, datasets, regions)
- Values that differ between local/dev/prod
- Non-sensitive infrastructure settings

**Example:**
```yaml
# config/config.yml
pipeline:
  environment: "local"
  metrics_bucket: "ecom-datalake-metrics"
  bronze_bucket: "local"
```

**Override via env:**
```bash
export PIPELINE_ENV=prod
export METRICS_BUCKET=ecom-prod-metrics
```

**✅ Good for:** Buckets, datasets, regions, environment names  
**❌ Not for:** Secrets, per-developer settings, business logic

---

### 2. Secrets & Credentials (Env-Only)

**Pattern:** env var → error (no default)

**When to use:**
- API keys, passwords, tokens
- Service account credentials
- Any security-sensitive value

**Example:**
```bash
# .env (NEVER commit)
# Optional service account credentials (prod-style)
USE_SA_AUTH=true
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
GCP_SA_KEY_PATH=/path/to/service-account.json
DBT_BIGQUERY_KEYFILE=/path/to/service-account.json
OPENAI_API_KEY=sk-...
DATABASE_PASSWORD=secret
```

**In code:**
```python
import os

# No default - will raise error if not set
api_key = os.environ["OPENAI_API_KEY"]

# Or with explicit check (only when SA auth is enabled)
if os.getenv("USE_SA_AUTH", "").lower() in {"1", "true", "yes", "on"}:
    if not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        raise ValueError("GOOGLE_APPLICATION_CREDENTIALS must be set")
```

**✅ Good for:** Credentials, keys, passwords  
**❌ Not for:** Non-sensitive config, business logic

**Note:** ADC (`gcloud auth application-default login`) is the default for local/dev
and does not require `GOOGLE_APPLICATION_CREDENTIALS` unless `USE_SA_AUTH=true`.

#### Airflow Fernet Key (Required for Encrypted Secrets)

Airflow uses a Fernet key to encrypt connections and variables stored in the
metadata database. Without it, Airflow stores secrets in plaintext and logs a warning.

**Required env var:**
```bash
AIRFLOW__CORE__FERNET_KEY=<32-byte base64 key>
```

**Storage guidance:**
- Local dev: keep in a git-ignored `docker.env` or shell env.
- Production: inject from a secrets manager (recommended) or your orchestrator's
  secret store. Do not commit to git.

**Rotation note:** rotating the Fernet key makes existing encrypted values unreadable.
Plan to re-save connections/variables if you rotate.

**Airflow secrets backend:** Airflow can load connections/variables from a secrets
backend (e.g., GCP Secret Manager, HashiCorp Vault, AWS Secrets Manager), which is
recommended for production.

**Production auth (GCP):** Use Workload Identity (preferred) so no key files are
needed. If Workload Identity isn’t available, use a dedicated service account
with a key file mounted into the runtime and `GOOGLE_APPLICATION_CREDENTIALS`
set via your orchestrator or secret manager.

#### Example Files

- `.env.example`: non-secret placeholders for local/dev.
- `.envrc`: direnv defaults; reads `.env` if present (never commit secrets).

If you enable a secrets backend locally, set these in `.env` (or shell env) only:
```bash
AIRFLOW__SECRETS__BACKEND=airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend
AIRFLOW__SECRETS__BACKEND_KWARGS={"connections_prefix":"airflow-connections","variables_prefix":"airflow-variables"}
```

#### GCP Authentication Patterns

**Local Development (ADC - Default)**:
```bash
# docker.env
USE_SA_AUTH=false
CLOUDSDK_CONFIG=/home/airflow/.config/gcloud

# Your local ~/.config/gcloud is mounted via docker-compose.yml
# No service account key needed!
```

**Production (Service Account)**:
```bash
# docker.env (or Cloud Composer env vars)
USE_SA_AUTH=true
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/.gcp/sa.json
GCP_SA_KEY_PATH=/path/to/host/service-account.json  # For docker-compose mount
```

**Testing Prod-like Locally**:
```bash
# 1. Download service account key
gcloud iam service-accounts keys create ~/my-sa-key.json \
  --iam-account=airflow@my-project.iam.gserviceaccount.com

# 2. Update docker.env
USE_SA_AUTH=true
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/.gcp/sa.json
GCP_SA_KEY_PATH=/Users/you/my-sa-key.json

# 3. Uncomment SA volume mount in docker-compose.yml
# - ${GCP_SA_KEY_PATH}:/opt/airflow/.gcp/sa.json:ro

# 4. Restart docker-compose
docker-compose down && docker-compose up -d
```

---

### 3. Business Logic (Config-Only)

**Pattern:** config.yml → code default (no env override)

**When to use:**
- Business rules and constants
- Algorithm parameters
- SLA thresholds
- Feature behavior

**Example:**
```yaml
# config/config.yml
pipeline:
  attribution_tolerance_hours: 48
  churn_danger_window_days: [30, 90]
  min_order_value: 10.00
  max_retry_attempts: 3
```

**In code:**
```python
from src.settings import load_settings

settings = load_settings()
# Use directly - no env var override
tolerance = settings.pipeline.attribution_tolerance_hours
```

**✅ Good for:** Business rules, algorithm params, thresholds  
**❌ Not for:** Deployment settings, secrets

---

## Quick Reference Table

| Category | Examples | Config.yml? | Env Var? | Default? | Commit? |
|----------|----------|-------------|----------|----------|---------|
| **Infrastructure** | Buckets, datasets, regions | ✅ Primary | ✅ Override | ✅ Yes | ✅ Yes |
| **Secrets** | API keys, passwords | ❌ Never | ✅ Required | ❌ No | ❌ Never |
| **Business Logic** | SLAs, tolerances, rules | ✅ Only | ❌ No override | ✅ Yes | ✅ Yes |
| **Per-Dev** | Debug flags, local paths | ❌ No | ✅ Optional | ✅ Yes | ❌ .env only |

---

## Examples from Your Pipeline

### Feature Flags (Env-Only)
```bash
# Gate optional stages without changing code
GOLD_PIPELINE_ENABLED=true
BQ_LOAD_ENABLED=true
SILVER_PUBLISH_MODE=direct|staging
ENRICHED_PUBLISH_MODE=direct|staging
DIMS_PUBLISH_MODE=direct|staging
DIMS_SNAPSHOT_ALLOW_BOOTSTRAP=true  # backfill-only: bootstrap earliest product_catalog partition
DIMS_CUSTOMERS_IGNORE_SIGNUP_DATE=true  # backfill-only: skip signup_date cutoff
```

### Path Overrides (Per-Dev / Dev)
```bash
BRONZE_BASE_PATH=samples/bronze
SILVER_BASE_PATH=data/silver/base
SILVER_QUARANTINE_PATH=data/silver/base/quarantine
SILVER_ENRICHED_PATH=data/silver/enriched
SILVER_DIMS_PATH=data/silver/dims
SILVER_DIMS_LOCAL_PATH=/opt/airflow/data/silver/dims
SILVER_GCS_TARGET=gs://your-silver-bucket/silver/base
SILVER_ENRICHED_GCS_TARGET=gs://your-silver-bucket/silver/enriched
SILVER_STAGING_PATH=gs://your-silver-bucket/silver/base/_staging/<run_id>
DIMS_STAGING_PATH=gs://your-silver-bucket/silver/dims/_staging/<run_id>
ENRICHED_STAGING_PATH=gs://your-silver-bucket/silver/enriched/_staging/<run_id>
```

### Complete Environment Variable List (Current)

**Core pipeline**
```bash
PIPELINE_ENV=local|dev|prod
ECOM_CONFIG_PATH=/opt/airflow/config/config.yml
ECOM_SPEC_PATH=/opt/airflow/config/specs
BASE_SILVER_LOOKBACK_DAYS=0
BRONZE_VALIDATION_LOOKBACK_DAYS=0
SILVER_VALIDATION_LOOKBACK_DAYS=0
```

**Docker image versioning (baked at build time)**
```bash
GIT_COMMIT=a1b2c3d                    # Git commit SHA (short)
GIT_BRANCH=main                       # Git branch name
PIPELINE_VERSION=main-a1b2c3d         # Full version string (tag or branch-commit)
```

**Paths / storage**
```bash
BRONZE_BASE_PATH=samples/bronze
SILVER_BASE_PATH=data/silver/base
SILVER_QUARANTINE_PATH=data/silver/base/quarantine
SILVER_ENRICHED_PATH=data/silver/enriched
SILVER_DIMS_PATH=data/silver/dims
SILVER_DIMS_LOCAL_PATH=/opt/airflow/data/silver/dims
SILVER_GCS_TARGET=gs://bucket/silver/base
SILVER_ENRICHED_GCS_TARGET=gs://bucket/silver/enriched
SILVER_STAGING_PATH=gs://bucket/silver/base/_staging/<run_id>
DIMS_STAGING_PATH=gs://bucket/silver/dims/_staging/<run_id>
ENRICHED_STAGING_PATH=gs://bucket/silver/enriched/_staging/<run_id>
SILVER_PUBLISH_MODE=direct|staging
ENRICHED_PUBLISH_MODE=direct|staging
DIMS_PUBLISH_MODE=direct|staging
```

**GCP / BigQuery**
```bash
GOOGLE_CLOUD_PROJECT=your-project
GCS_BUCKET=your-bronze-bucket
BQ_LOCATION=US

# GCP Authentication (two modes - ADC or Service Account)
USE_SA_AUTH=false                                        # false=ADC (default), true=SA
CLOUDSDK_CONFIG=/home/airflow/.config/gcloud             # ADC config path (when USE_SA_AUTH=false)
CLOUDSDK_CONFIG_PATH=$HOME/.config/gcloud                # Host path for docker-compose mount
GCP_SA_KEY_PATH=/path/to/service-account.json           # Host path (when USE_SA_AUTH=true)
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/.gcp/sa.json # Container path (when USE_SA_AUTH=true)

# BigQuery / dbt
DBT_BIGQUERY_KEYFILE=/path/to/sa.json
DBT_BQ_METHOD=oauth|service-account
BQ_LOAD_ENABLED=true|false
GOLD_PIPELINE_ENABLED=true|false

# Legacy (deprecated - use GOOGLE_CLOUD_PROJECT instead)
ECOM_PROJECT_ID=your-project
ECOM_BRONZE_BUCKET=your-bronze-bucket
ECOM_SILVER_BUCKET=your-silver-bucket
```

**Observability**
```bash
OBSERVABILITY_ENV=local|dev|prod
METRICS_BUCKET=ecom-datalake-metrics     # Metrics bucket (GCS or local path)
LOGS_BUCKET=ecom-datalake-logs           # Logs bucket (GCS or local path)
REPORTS_BUCKET=data-reporting            # Validation reports bucket (GCS or local)
METRICS_BASE_PATH=/tmp/metrics           # Local metrics output (Docker)
LOGS_BASE_PATH=/tmp/logs                 # Local logs output (Docker)
REPORTS_BASE_PATH=docs/validation_reports # Local reports output
```

**Feature flags**
```bash
GOLD_PIPELINE_ENABLED=false              # Enable/disable Gold layer pipeline
BQ_LOAD_ENABLED=false                    # Enable/disable BigQuery loads
SILVER_PUBLISH_MODE=direct|staging       # direct=immediate, staging=versioned runs
DIMS_SNAPSHOT_ALLOW_BOOTSTRAP=false      # backfill-only: bootstrap earliest product_catalog
DIMS_CUSTOMERS_IGNORE_SIGNUP_DATE=false  # backfill-only: skip signup_date cutoff
```

**Quality gates**
```bash
STRICT_FK=false                          # Enforce FK validation failures
BRONZE_QA_REQUIRED=true                  # Require Bronze QA phase
BRONZE_QA_FAIL=false                     # Fail pipeline on Bronze issues (vs warn)
SILVER_PROFILE_ENABLED=false             # Enable profiling for Silver tables
SILVER_PROFILE_REPORT=docs/validation_reports/SILVER_PROFILE.md
```

**dbt / DuckDB (Docker paths)**
```bash
DBT_TARGET_PATH=/tmp/dbt_target
DBT_LOG_PATH=/tmp/dbt_logs
DBT_DUCKDB_PATH=/tmp/dbt_duckdb/ecom.duckdb
DBT_PARTIAL_PARSE=false
```

**Airflow user configuration**
```bash
AIRFLOW_USERNAME=airflow
AIRFLOW_PASSWORD=airflow
AIRFLOW_EMAIL=airflow@example.com
AIRFLOW_FIRSTNAME=Airflow
AIRFLOW_LASTNAME=Admin
```

**Airflow / runtime (set by environment)**
```bash
AIRFLOW_HOME=/opt/airflow
AIRFLOW_UID=50000
PYTHONPATH=/opt/airflow
PATH=/usr/local/sbin:/usr/local/bin:...
HOME=/home/airflow
AIRFLOW_CTX_DAG_RUN_ID=...
AIRFLOW_CTX_DAG_ID=...
AIRFLOW_CTX_TASK_ID=...
AIRFLOW_CTX_EXECUTION_DATE=...
ECOM_RUN_ID=...
```

---

### Docker/macOS VirtioFS Workaround

On macOS with Docker Desktop using VirtioFS, mounted volumes can experience `Errno 35` (Resource temporarily unavailable) file locking issues. To avoid this, the following paths are redirected to `/tmp` inside the container:

| Variable             | Default Value                   | Purpose                                          |
| -------------------- | ------------------------------- | ------------------------------------------------ |
| `DBT_TARGET_PATH`    | `/tmp/dbt_target`               | dbt compiled artifacts                           |
| `DBT_LOG_PATH`       | `/tmp/dbt_logs`                 | dbt log files                                    |
| `DBT_DUCKDB_PATH`    | `/tmp/dbt_duckdb/ecom.duckdb`   | DuckDB database file                             |
| `METRICS_BASE_PATH`  | `/tmp/metrics`                  | Pipeline metrics output                          |
| `LOGS_BASE_PATH`     | `/tmp/logs`                     | Structured log output                            |
| `DBT_PARTIAL_PARSE`  | `false`                         | Disable partial parsing to avoid cache conflicts |

These are configured in `docker-compose.yml` and should not be modified unless necessary.

### Infrastructure (Config + Env)
```yaml
# config.yml
pipeline:
  environment: "local"          # Override: PIPELINE_ENV
  metrics_bucket: "..."         # Override: METRICS_BUCKET (observability only)
  observability_env: "local"    # Override: OBSERVABILITY_ENV (observability only)
  bronze_bucket: "local"        # Override: ECOM_BRONZE_BUCKET
  project_id: "my-project"      # Override: GOOGLE_CLOUD_PROJECT
```

### Secrets (Env Only)
```bash
# .env (not in config.yml)
USE_SA_AUTH=true
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
GCP_SA_KEY_PATH=/path/to/service-account.json
DBT_BIGQUERY_KEYFILE=/path/to/sa.json
```

### Business Logic (Config Only)
```yaml
# config.yml (no env override)
pipeline:
  attribution_tolerance_hours: 48
  churn_danger_window_days: [30, 90]
  sales_velocity_window_days: 7
  default_ingest_dt: "2020-01-01"
```

---

## Anti-Patterns to Avoid

### ❌ DON'T: Put secrets in config.yml
```yaml
# BAD - Never do this
pipeline:
  api_key: "sk-secret123"
  database_password: "admin123"
```

### ❌ DON'T: Allow env override for business logic
```python
# BAD - Business rule shouldn't vary per deployment
attribution_hours = int(os.getenv("ATTRIBUTION_HOURS", "48"))
```

### ❌ DON'T: Hardcode deployment-specific values
```python
# BAD - Should be in config.yml
METRICS_BUCKET = "ecom-prod-metrics"  # Hardcoded!
```

---

## When to Make Exceptions

**Sometimes you need flexibility for debugging:**

```python
# OK for debug-only settings
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
```

**Emergency overrides (use sparingly):**
```python
# Allow env override for emergencies, but warn
sla_threshold = float(
    os.getenv("OVERRIDE_SLA_THRESHOLD") or 
    settings.pipeline.sla_threshold
)
if os.getenv("OVERRIDE_SLA_THRESHOLD"):
    logger.warning("SLA threshold overridden by environment variable!")
```

---

## Summary

**Not every env variable should work like observability settings.**

Use this decision tree:

```
Is it a secret/credential?
├─ YES → Env-only (no config.yml, no default)
└─ NO → Is it deployment-specific?
    ├─ YES → Config-first with env override
    └─ NO → Config-only (no env override)
```

**Your current setup:**
- ✅ Infrastructure: Config-first with env overrides (buckets, datasets, project IDs)
- ✅ Secrets: Env-only (GCP credentials via ADC or service account)
- ✅ Business logic: Config-only (SLA thresholds, attribution windows, etc.)
- ✅ Feature flags: Env-only (GOLD_PIPELINE_ENABLED, BQ_LOAD_ENABLED, etc.)
- ✅ Docker versioning: Build-time args (GIT_COMMIT, GIT_BRANCH, PIPELINE_VERSION)

---

## Related Documentation

- **[CONFIG_STRATEGY.md](CONFIG_STRATEGY.md)** - Configuration hierarchy and precedence rules
- **[CREDENTIALS_SAFETY_AUDIT.md](CREDENTIALS_SAFETY_AUDIT.md)** - GCP credential security and Docker safety
- **[DOCKER_VERSIONING.md](DOCKER_VERSIONING.md)** - Git-based image versioning with env vars
- **[DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)** - Local, dev, and production deployment patterns
- **[SPEC_OVERVIEW.md](SPEC_OVERVIEW.md)** - Spec-driven orchestration and environment variable support

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
