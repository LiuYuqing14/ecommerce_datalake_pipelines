# Deployment Guide

Complete guide for deploying the e-commerce data pipeline in containerized environments.

---

## 🎯 Quick Start (New Users)

**Want to run the pipeline immediately?** Jump to the [One-Click Quickstart](#-one-click-quickstart) with sample data included.

```bash
# Complete setup in 3 commands:
unzip samples/bronze_samples.zip -d samples/
docker-compose build && docker-compose up airflow-init
docker-compose up -d
# ✅ Access Airflow at http://localhost:8080 (airflow/airflow)
```

---

## Limitations & Constraints (Portfolio Scope)

- **DuckDB single-writer**: Base Silver runs as a single dbt task to avoid file locks. In a warehouse-backed prod setup, split into per-model tasks for retries and observability.
- **GCS sync idempotency**: `gsutil rsync` is not atomic. For production, sync to a staging prefix, validate, then **promote to canonical**.
- **Batch-only assumptions**: The pipeline expects static Bronze partitions per run. Streaming/async ingestion could introduce FK misses unless you snapshot or pin partitions.

## Future Improvements

- Replace the DuckDB single-task run with per-model dbt tasks when using BigQuery/Snowflake (better retries and lineage).
- Add a staging + manifest publish step for GCS syncs, then promote after validation.
- Introduce enriched-level validation severity (warn vs drop) for nuanced business rules.
- Document and optionally wire Workload Identity for production-grade auth.

## Table of Contents

1. [Local Development](#local-development)
2. [Production Deployment](#production-deployment)
3. [Environment Variables](#environment-variables)
4. [Troubleshooting](#troubleshooting)

---

## Local Development

### Prerequisites

- Docker & Docker Compose installed
- 8GB RAM minimum (16GB recommended)
- Git (for cloning the repository)

### 🚀 One-Click Quickstart

**Complete local setup in 3 commands** (with sample data included):

```bash
# 1. Extract bronze sample data (included in repo)
unzip samples/bronze_samples.zip -d samples/

# 2. Build and initialize Airflow
docker-compose build && docker-compose up airflow-init

# 3. Start the pipeline
docker-compose up -d

# ✅ Done! Access Airflow at http://localhost:8080
# Username: airflow | Password: airflow
```

**What this does**:
- ✅ Extracts multi-period Bronze Parquet samples (2020-03, 2023-01, 2024-01, 2025-10)
- ✅ Builds custom Airflow image with dbt + Polars + all dependencies
- ✅ Initializes Airflow database and creates admin user
- ✅ Starts Airflow scheduler, webserver, and PostgreSQL services
- ✅ Mounts sample data at `samples/bronze/` for immediate pipeline execution

**Next steps**:

**Env files**:
- **Local samples**: `docker.env.local.example` → `docker.env.local`
- **GCS/BigQuery**: `docker.env.gcs.example` → `docker.env`

**Option A: Use Airflow UI** (Recommended - handles dependencies automatically):
1. Open Airflow UI: http://localhost:8080
2. Navigate to **DAGs** → `ecom_silver_to_gold_pipeline`

---

### Run Locally with the Published Image (Sample Data)

If you want to use the **versioned Docker image** instead of building locally:

```bash
# 1) Unzip sample data
unzip samples/bronze_samples.zip -d samples/

# 2) Create a local env file
cp docker.env.local.example docker.env.local

# 3) Run the published image (replace TAG)
DOCKER_ENV_FILE=docker.env.local \
PIPELINE_IMAGE=ghcr.io/g-schumacher44/ecom_datalake_pipelines \
PIPELINE_TAG=YOUR_TAG \
docker compose up -d --no-build
```

**Notes**:
- The compose file defaults to `ecom-datalake-pipeline:latest`. Override with `PIPELINE_TAG`.
- GitHub releases (tags like `v1.0.0`) automatically build/push to **GHCR** via GitHub Actions.
  If you need **GCP Artifact Registry**, use `ecomlake deploy push-image-versioned PROJECT_ID=...`.
- Some enriched tables may be empty for a given sample date because the sample archive does not include every table for every day.
3. Click **Trigger DAG** (play button)
4. Watch the pipeline execute: Dims → Base Silver → Enriched Silver

**Option B: Fast Local Demo** (recommended for quick validation):
```bash
# Single command that runs the complete pipeline with pre-cooked dims
ecomlake local demo-fast

# Validates:
# ✅ Silver: 6/6 fact tables (orders, carts, returns, etc.)
# ✅ Enriched: 10/10 business tables with full data
# ✅ dbt: 147 data quality tests
# ✅ All dims-dependent enriched tables produce data

# Check validation reports:
cat docs/validation_reports/SILVER_QUALITY_FULL.md
cat docs/validation_reports/ENRICHED_QUALITY_2020-01-05.md
```

**Option C: Manual CLI execution** (for testing individual steps):
```bash
# Run in correct order:
ecomlake local dims --date 2020-01-05      # 1. Create dimension snapshots
ecomlake local silver --date 2020-01-05    # 2. Run Base Silver (needs dims for FK checks)
ecomlake local enriched --date 2020-01-05  # 3. Run Enriched transforms
```

**Notes**:

- Base Silver models perform FK validation against dimension snapshots, so dims must be created first
- `local-demo-fast` processes 2020-01-05 with complete Bronze data (370 customer partitions + 5 product categories + 5 days of fact tables)
- Sample includes complete customer history (2019-01-01 through 2020-01-05) allowing honest dims snapshot generation
- For testing individual dates, use the manual workflow above

**Validate outputs**:
```bash
# Check Silver outputs
ls -lh data/silver/base/orders/
ls -lh data/silver/enriched/int_cart_attribution/

# View validation reports
cat docs/validation_reports/BRONZE_QUALITY.md
cat docs/validation_reports/SILVER_QUALITY.md
cat docs/validation_reports/ENRICHED_QUALITY.md
```

**What's included in `bronze_samples.zip`**:

The sample archive contains representative Bronze Parquet data for testing and development:

- **8 tables**: orders, customers, product_catalog, shopping_carts, cart_items, order_items, returns, return_items
- **Partitions**:
  - `ingest_dt`: 2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05 (5 consecutive days)
  - `signup_date`: 2019-01-01 through 2020-01-05 (370 partitions - complete customer history)
  - `category`: Books, Clothing, Electronics, Home, Toys (all 5 categories)
- **Format**: Hive-partitioned Parquet with `_MANIFEST.json` metadata
- **Size**: ~16MB compressed, ~20MB extracted
- **Rows**: ~194k total rows across 8 tables (see `docs/data/BRONZE_PROFILE_REPORT.md`)
- **Use case**: Full end-to-end pipeline testing including dims snapshot generation

**Sample data structure**:
```
samples/bronze/
  orders/
    ingest_dt=2020-01-01/
      part-00000.parquet
      _MANIFEST.json
    ingest_dt=2020-01-05/
      part-00000.parquet
      _MANIFEST.json
  customers/
    signup_date=2019-01-01/
      part-00000.parquet
      _MANIFEST.json
    signup_date=2020-01-05/
      part-00000.parquet
      _MANIFEST.json
  product_catalog/
    category=Electronics/
      part-00000.parquet
      _MANIFEST.json
  # ... 5 more tables
```

---

### Alternative: Step-by-Step Setup

If you prefer manual control over each step:

```bash
# 1. Extract sample data
unzip samples/bronze_samples.zip -d samples/

# 2. Build the custom Airflow image
docker-compose build

# 3. Initialize Airflow database + admin user
docker-compose up airflow-init

# 4. Start Airflow services
docker-compose up -d

# 5. Access Airflow UI
open http://localhost:8080
# Username: airflow
# Password: airflow

# 6. Trigger the DAG
# In Airflow UI: DAGs -> ecom_silver_to_gold_pipeline -> Trigger DAG
```

### Local Configuration

**Default behavior** (no `.env` file needed):
- `PIPELINE_ENV=local`
- Bronze: `samples/bronze/`
- Silver: `data/silver/base/`
- Enriched: `data/silver/enriched/`
- Gold pipeline: **disabled** (no BigQuery)

**Create `.env` to override** (optional):

```bash
# Local development with custom paths
PIPELINE_ENV=local
BRONZE_BASE_PATH=samples/bronze
SILVER_BASE_PATH=data/silver/base
GOLD_PIPELINE_ENABLED=false
```

### Development Workflow

**Option A: Code baked into image** (current default)
- Edit code locally
- Rebuild image: `docker-compose build`
- Restart services: `docker-compose up -d`
- Slower iteration, matches production

**Option B: Live code reload** (faster iteration)
1. Edit `docker-compose.yml` - uncomment these volume mounts:
   ```yaml
   - ./src:/opt/airflow/src
   - ./config:/opt/airflow/config
   - ./dbt_duckdb:/opt/airflow/dbt_duckdb
   ```
2. Restart: `docker-compose restart airflow-scheduler airflow-webserver`
3. Code changes reflected immediately (no rebuild)

### Logs & Debugging

```bash
# View scheduler logs (task execution)
docker-compose logs -f airflow-scheduler

# View webserver logs
docker-compose logs -f airflow-webserver

# Check validation reports
cat docs/validation_reports/BRONZE_QUALITY.md
cat docs/validation_reports/SILVER_QUALITY.md

# Shell into container
docker-compose exec airflow-scheduler bash
python -c "from src.settings import load_settings; print(load_settings())"
```

### Teardown

```bash
# Stop services
docker-compose down

# Remove volumes (reset database)
docker-compose down -v

# Remove image
docker rmi ecom-datalake-pipeline:latest
```

---

## Production Deployment

### Architecture Options

**Option A: Cloud Composer (Recommended)**
- Managed Airflow on GCP
- Auto-scaling workers
- Integrated with GCS/BigQuery
- $300-500/month (small environment)

**Option B: Self-Hosted Kubernetes**
- Use official Airflow Helm chart
- More control, more complexity
- Good for multi-cloud or on-prem

**Option C: Docker on VM**
- Single GCE instance with docker-compose
- Cheapest option (~$50/month)
- No auto-scaling, manual maintenance

### Dimension Refresh Strategy

- **Customers**: full refresh on a daily cadence (simple, reliable for small/medium volumes).
- **Product catalog**: full refresh on change (or daily if the source is small and stable).
- **Facts**: partitioned by business date and backfilled by date ranges.

### Cloud Composer Deployment

#### 1. Build & Push Image

```bash
# Set variables
export PROJECT_ID="your-gcp-project"
export REGION="us-central1"
export IMAGE_NAME="ecom-datalake-pipeline"
export IMAGE_TAG="v1.0.5"
export ARTIFACT_REPO="airflow-images"

# Create Artifact Registry repository (one-time)
gcloud artifacts repositories create $ARTIFACT_REPO \
  --repository-format=docker \
  --location=$REGION \
  --project=$PROJECT_ID

# Build and tag image
docker build -t $IMAGE_NAME:$IMAGE_TAG .
docker tag $IMAGE_NAME:$IMAGE_TAG \
  $REGION-docker.pkg.dev/$PROJECT_ID/$ARTIFACT_REPO/$IMAGE_NAME:$IMAGE_TAG

# Authenticate and push
gcloud auth configure-docker $REGION-docker.pkg.dev
docker push $REGION-docker.pkg.dev/$PROJECT_ID/$ARTIFACT_REPO/$IMAGE_NAME:$IMAGE_TAG
```

#### 2. Create Cloud Composer Environment

```bash
# Create environment with custom image
gcloud composer environments create ecom-pipeline \
  --location=$REGION \
  --image-version=composer-2.9.3-airflow-2.9.3 \
  --environment-size=small \
  --python-version=3.12 \
  --service-account=composer-sa@$PROJECT_ID.iam.gserviceaccount.com
```

#### 3. Configure Environment Variables

```bash
# Set pipeline environment to prod
gcloud composer environments update ecom-pipeline \
  --location=$REGION \
  --update-env-variables=PIPELINE_ENV=prod,\
GOOGLE_CLOUD_PROJECT=$PROJECT_ID,\
GCS_BUCKET=ecom-datalake-bronze,\
BRONZE_BASE_PATH=gs://ecom-datalake-bronze/bronze,\
SILVER_BASE_PATH=gs://ecom-datalake-silver/base,\
SILVER_ENRICHED_PATH=gs://ecom-datalake-silver/enriched,\
GOLD_PIPELINE_ENABLED=true,\
BRONZE_QA_FAIL=true
```

#### 4. Upload DAG

```bash
# Get DAG bucket
export DAG_BUCKET=$(gcloud composer environments describe ecom-pipeline \
  --location=$REGION \
  --format="value(config.dagGcsPrefix)")

# Upload DAG
gsutil cp airflow/dags/ecom_silver_to_gold.py $DAG_BUCKET/
```

#### 5. Create GCS Buckets

```bash
# Bronze bucket (input data)
gsutil mb -l $REGION gs://ecom-datalake-bronze
gsutil cp -r samples/bronze/* gs://ecom-datalake-bronze/bronze/

# Silver bucket (output data)
gsutil mb -l $REGION gs://ecom-datalake-silver

# Metrics bucket (observability)
gsutil mb -l $REGION gs://ecom-datalake-metrics
```

#### 6. Create BigQuery Datasets

```bash
# Silver dataset (for enriched tables)
bq mk --location=$REGION --dataset $PROJECT_ID:silver

# Gold dataset (for marts)
bq mk --location=$REGION --dataset $PROJECT_ID:gold_marts
```

#### 7. Grant Service Account Permissions

```bash
export SA_EMAIL="composer-sa@$PROJECT_ID.iam.gserviceaccount.com"

# GCS buckets
gsutil iam ch serviceAccount:$SA_EMAIL:roles/storage.objectAdmin \
  gs://ecom-datalake-bronze
gsutil iam ch serviceAccount:$SA_EMAIL:roles/storage.objectAdmin \
  gs://ecom-datalake-silver
gsutil iam ch serviceAccount:$SA_EMAIL:roles/storage.objectAdmin \
  gs://ecom-datalake-metrics

# BigQuery datasets
bq add-iam-policy-binding --member=serviceAccount:$SA_EMAIL \
  --role=roles/bigquery.dataEditor $PROJECT_ID:silver
bq add-iam-policy-binding --member=serviceAccount:$SA_EMAIL \
  --role=roles/bigquery.dataEditor $PROJECT_ID:gold_marts
```

### Self-Hosted Docker Deployment

#### 1. Provision GCE Instance

```bash
# Create VM with Docker
gcloud compute instances create airflow-vm \
  --zone=us-central1-a \
  --machine-type=n2-standard-4 \
  --boot-disk-size=100GB \
  --image-family=cos-stable \
  --image-project=cos-cloud \
  --scopes=cloud-platform \
  --service-account=airflow-vm-sa@$PROJECT_ID.iam.gserviceaccount.com
```

#### 2. SSH and Setup

```bash
# SSH into VM
gcloud compute ssh airflow-vm --zone=us-central1-a

# Clone repo
git clone https://github.com/your-org/ecom-datalake-pipelines.git
cd ecom-datalake-pipelines

# Create production .env
cat > .env <<EOF
PIPELINE_ENV=prod
GOOGLE_CLOUD_PROJECT=$PROJECT_ID
GCS_BUCKET=ecom-datalake-bronze
BRONZE_BASE_PATH=gs://ecom-datalake-bronze/bronze
SILVER_BASE_PATH=gs://ecom-datalake-silver/base
SILVER_ENRICHED_PATH=gs://ecom-datalake-silver/enriched
GOLD_PIPELINE_ENABLED=true
BRONZE_QA_FAIL=true
AIRFLOW_UID=1000
EOF

# Build and start
docker-compose build
docker-compose up airflow-init
docker-compose up -d
```

#### 3. Access Airflow UI

```bash
# Create firewall rule (one-time)
gcloud compute firewall-rules create allow-airflow \
  --allow tcp:8080 \
  --source-ranges 0.0.0.0/0 \
  --target-tags airflow-vm

# Get external IP
gcloud compute instances describe airflow-vm \
  --zone=us-central1-a \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'

# Access UI at http://<EXTERNAL_IP>:8080
```

---

## Environment Variables

### Required for Production

| Variable               | Description                      | Example                              |
| ---------------------- | -------------------------------- | ------------------------------------ |
| `PIPELINE_ENV`         | Environment: local, dev, prod    | `prod`                               |
| `GOOGLE_CLOUD_PROJECT` | GCP project ID                   | `my-project-123`                     |
| `GCS_BUCKET`           | Bronze data bucket               | `ecom-datalake-bronze`               |
| `BRONZE_BASE_PATH`     | Path to Bronze data              | `gs://ecom-datalake-bronze/bronze`   |
| `SILVER_BASE_PATH`     | Path to Base Silver output       | `gs://ecom-datalake-silver/base`     |
| `SILVER_ENRICHED_PATH` | Path to Enriched Silver output   | `gs://ecom-datalake-silver/enriched` |

### Optional Feature Flags

| Variable                 | Default                         | Description                              |
| ------------------------ | ------------------------------- | ---------------------------------------- |
| `GOLD_PIPELINE_ENABLED`  | `false` (local), `true` (prod)  | Enable BigQuery Gold layer               |
| `BQ_LOAD_ENABLED`        | `false` (local), `true` (prod)  | Enable Enriched Silver BigQuery loads    |
| `BRONZE_QA_REQUIRED`     | `true`                          | Require Bronze QA phase                  |
| `BRONZE_QA_FAIL`         | `false` (local), `true` (prod)  | Fail pipeline on Bronze issues           |
| `STRICT_FK`              | `false` (local), `true` (prod)  | Enforce FK validation in Silver          |
| `SILVER_PROFILE_ENABLED` | `false`                         | Generate Silver profiling reports        |
| `BQ_LOCATION`            | `US`                            | BigQuery dataset location                |
| `SILVER_PUBLISH_MODE`    | `direct`                        | Base Silver publish mode (direct|staging) |
| `ENRICHED_PUBLISH_MODE`  | `direct`                        | Enriched publish mode (direct|staging)   |
| `DIMS_PUBLISH_MODE`      | `direct`                        | Dims publish mode (direct|staging)       |
| `DIMS_SNAPSHOT_ALLOW_BOOTSTRAP` | `false`                  | Backfill-only: bootstrap earliest product_catalog partition |
| `DIMS_CUSTOMERS_IGNORE_SIGNUP_DATE` | `false`            | Backfill-only: include all customers in snapshot |

### Docker/macOS VirtioFS Issues

If you encounter `Resource deadlock avoided` (Errno 35) errors on macOS with Docker Desktop:

1. **Switch file sharing backend**: Docker Desktop → Settings → General → Change **VirtioFS** to **gRPC FUSE**
2. **Avoid cloud-synced folders**: Ensure your project is NOT in iCloud, OneDrive, Dropbox, or Google Drive synced directories
3. **Restart Docker Desktop** after making changes

These variables redirect writable paths to `/tmp` inside the container to avoid write-related file locking issues:

| Variable             | Default Value                   | Description                              |
| -------------------- | ------------------------------- | ---------------------------------------- |
| `DBT_TARGET_PATH`    | `/tmp/dbt_target`               | dbt compiled artifacts                   |
| `DBT_LOG_PATH`       | `/tmp/dbt_logs`                 | dbt log files                            |
| `DBT_DUCKDB_PATH`    | `/tmp/dbt_duckdb/ecom.duckdb`   | DuckDB database file                     |
| `METRICS_BASE_PATH`  | `/tmp/metrics`                  | Pipeline metrics output                  |
| `LOGS_BASE_PATH`     | `/tmp/logs`                     | Structured log output                    |
| `DBT_PARTIAL_PARSE`  | `false`                         | Disable dbt partial parsing              |

### Airflow User Configuration

| Variable             | Default                    | Description                |
| -------------------- | -------------------------- | -------------------------- |
| `AIRFLOW_USERNAME`   | `airflow`                  | Airflow admin username     |
| `AIRFLOW_PASSWORD`   | `airflow`                  | Airflow admin password     |
| `AIRFLOW_EMAIL`      | `airflow@example.com`      | Airflow admin email        |
| `AIRFLOW_FIRSTNAME`  | `Airflow`                  | Airflow admin first name   |
| `AIRFLOW_LASTNAME`   | `Admin`                    | Airflow admin last name    |
| `AIRFLOW_UID`        | `50000`                    | Airflow user ID in Docker  |

### Authentication

**Cloud Composer**: Uses environment's service account (automatic)

**Docker local/VM**: Two options:

1. **Application Default Credentials** (recommended):
   ```bash
   gcloud auth application-default login
   # Or on GCE: attach service account to VM
   ```

2. **Service Account Key** (not recommended):
   ```yaml
   # In docker-compose.yml
   volumes:
     - ./service-account-key.json:/opt/airflow/service-account-key.json:ro
   environment:
     GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/service-account-key.json
   ```

---

## Troubleshooting

### "Module 'src' not found"

**Cause**: Image not built with custom code

**Fix**:
```bash
docker-compose build --no-cache
docker-compose up -d
```

### "Permission denied: gs://..."

**Cause**: Service account lacks GCS permissions

**Fix**:
```bash
# Check current SA
gcloud composer environments describe ecom-pipeline \
  --location=us-central1 \
  --format="value(config.nodeConfig.serviceAccount)"

# Grant storage.objectAdmin
gsutil iam ch serviceAccount:SA_EMAIL:roles/storage.objectAdmin gs://BUCKET
```

### "BQ load failed: Not found: Dataset"

**Cause**: BigQuery dataset doesn't exist

**Fix**:
```bash
bq mk --location=US --dataset PROJECT_ID:silver
bq mk --location=US --dataset PROJECT_ID:gold_marts
```

### "dbt deps failed: packages not found"

**Cause**: dbt packages not installed during build

**Fix**: Check Dockerfile includes:
```dockerfile
RUN cd dbt_duckdb && dbt deps --project-dir . --profiles-dir .
```

### DAG import errors

**Check logs**:
```bash
# Local
docker-compose logs airflow-scheduler | grep "ecom_silver_to_gold"

# Cloud Composer
gcloud composer environments run ecom-pipeline \
  --location=us-central1 dags list
```

**Validate DAG syntax**:
```bash
docker-compose exec airflow-scheduler bash
python airflow/dags/ecom_silver_to_gold.py
```

### Out of memory

**Cause**: Polars processing large data on small instance

**Fix**:
- Increase instance size (Cloud Composer: `--environment-size=medium`)
- Use streaming: `pl.scan_parquet()` instead of `pl.read_parquet()`
- Partition data by ingest_dt

### Slow builds

**Cause**: Large build context or cache issues

**Fix**:
```bash
# Check context size
du -sh .

# Verify .dockerignore excludes data/
cat .dockerignore | grep "^data/"

# Clean build cache
docker builder prune
docker-compose build --no-cache
```

---

## Cost Optimization

### Local Development
- **Cost**: $0 (runs on your machine)
- **Best for**: Testing, development, learning

### Docker on GCE VM
- **Compute**: n2-standard-4 = ~$120/month
- **Storage**: 100GB disk = ~$10/month
- **Total**: ~$130/month
- **Best for**: Small production workloads

### Cloud Composer
- **Small environment**: ~$300/month base + usage
- **GCS storage**: ~$8/month (400GB)
- **BigQuery**: Pay per query (~$30/month for daily runs)
- **Total**: ~$340/month
- **Best for**: Production with SLA requirements

**Savings tip**: Use Polars runners instead of dbt-BigQuery Python models (saves ~$200/month). See [docs/local_only/COST_ANALYSIS.md](../local_only/COST_ANALYSIS.md) for details.

---

For questions or issues, see [CONTRIBUTING.md](../CONTRIBUTING.md).

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
