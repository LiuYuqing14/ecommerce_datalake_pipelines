#!/bin/bash
if [[ -z "${ECOM_CLI_SUPPRESS_DEPRECATION:-}" ]]; then
  echo "DEPRECATED: use \`ecomlake pipeline sim-prod-gcs\` instead of scripts/run_sim_prod_gcs.sh" >&2
fi
set -euo pipefail

# ==============================================================================
# Simulated Prod Pipeline Runner - GCS-backed, no Docker
# ==============================================================================
# Usage: ./scripts/run_sim_prod_gcs.sh [INGEST_DATE]
# Example: ./scripts/run_sim_prod_gcs.sh 2020-01-01
# ==============================================================================

INGEST_DATE=${1:-"2020-01-01"}
RUN_ID="manual_$(date +%Y%m%d_%H%M%S)"

: "${GOOGLE_CLOUD_PROJECT:=gcs-automation-project}"
: "${BRONZE_BASE_PATH:=gs://gcs-automation-project-raw/ecom/raw}"
: "${SILVER_BASE_PATH:=gs://gcs-automation-project-silver/data/silver/base}"
: "${SILVER_ENRICHED_PATH:=gs://gcs-automation-project-silver/data/silver/enriched}"
: "${SILVER_DIMS_PATH:=gs://gcs-automation-project-silver/data/silver/dims}"
: "${SILVER_ENRICHED_LOCAL_PATH:=./data/silver/enriched}"
: "${SILVER_LOCAL_BASE_PATH:=./data/silver/base}"
: "${SILVER_EXPORT_BASE_PATH:=$SILVER_BASE_PATH}"
: "${METRICS_BUCKET:=data-reporting}"
: "${LOGS_BUCKET:=data-reporting}"
: "${REPORTS_BUCKET:=data-reporting}"

export PIPELINE_ENV="${PIPELINE_ENV:-prod}"
export OBSERVABILITY_ENV="${OBSERVABILITY_ENV:-prod}"
export BQ_LOAD_ENABLED="${BQ_LOAD_ENABLED:-false}"
export GOLD_PIPELINE_ENABLED="${GOLD_PIPELINE_ENABLED:-false}"
export SILVER_PUBLISH_MODE="${SILVER_PUBLISH_MODE:-staging}"
export ENRICHED_PUBLISH_MODE="${ENRICHED_PUBLISH_MODE:-staging}"
export DIMS_PUBLISH_MODE="${DIMS_PUBLISH_MODE:-staging}"
export RUN_ID="${RUN_ID}"
export SILVER_STAGING_PATH="${SILVER_BASE_PATH}/_staging/${RUN_ID}"
export ENRICHED_STAGING_PATH="${SILVER_ENRICHED_PATH}/_staging/${RUN_ID}"
export DIMS_STAGING_PATH="${SILVER_DIMS_PATH}/_staging/${RUN_ID}"

export GOOGLE_CLOUD_PROJECT
export BRONZE_BASE_PATH
export SILVER_BASE_PATH
export SILVER_ENRICHED_PATH
export SILVER_DIMS_PATH
export SILVER_ENRICHED_LOCAL_PATH
export SILVER_LOCAL_BASE_PATH
export SILVER_EXPORT_BASE_PATH
export METRICS_BUCKET
export LOGS_BUCKET
export REPORTS_BUCKET

if [[ "${SILVER_LOCAL_BASE_PATH}" == /opt/airflow/* ]]; then
  SILVER_LOCAL_BASE_PATH="./data/silver/base"
  export SILVER_LOCAL_BASE_PATH
fi
if [[ "${SILVER_ENRICHED_LOCAL_PATH}" == /opt/airflow/* ]]; then
  SILVER_ENRICHED_LOCAL_PATH="./data/silver/enriched"
  export SILVER_ENRICHED_LOCAL_PATH
fi

mkdir -p "${SILVER_LOCAL_BASE_PATH}" "${SILVER_ENRICHED_LOCAL_PATH}"

echo "=========================================="
echo "Starting Simulated Prod Pipeline"
echo "Ingest Date: ${INGEST_DATE}"
echo "Run ID: ${RUN_ID}"
echo "Env: ${PIPELINE_ENV}"
echo "Bronze: ${BRONZE_BASE_PATH}"
echo "Silver: ${SILVER_BASE_PATH}"
echo "Enriched: ${SILVER_ENRICHED_PATH}"
echo "Reports Bucket: ${REPORTS_BUCKET}"
echo "Metrics Bucket: ${METRICS_BUCKET}"
echo "Logs Bucket: ${LOGS_BUCKET}"
echo "=========================================="

# Authenticate with GCP (ADC)
echo "→ Authenticating with GCP (ADC)..."
gcloud auth application-default login --quiet || echo "Already authenticated"

echo ""
echo "→ Phase 1: Dimension Refresh"
python -m src.validation.bronze_quality \
    --bronze-path "${BRONZE_BASE_PATH}" \
    --tables customers,product_catalog \
    --output-report "docs/validation_reports/BRONZE_DIMS_${RUN_ID}.md" \
    --run-id "${RUN_ID}" \
    --enforce-quality

export SILVER_BASE_PATH="${SILVER_BASE_PATH}"
python -m src.runners.base_silver \
    --select stg_ecommerce__customers stg_ecommerce__customers_quarantine

python -m src.runners.base_silver \
    --select stg_ecommerce__product_catalog stg_ecommerce__product_catalog_quarantine

python -m src.validation.silver \
    --bronze-path "${BRONZE_BASE_PATH}" \
    --silver-path "${SILVER_LOCAL_BASE_PATH}" \
    --quarantine-path "${SILVER_LOCAL_BASE_PATH}/quarantine" \
    --tables customers,product_catalog \
    --run-id "${RUN_ID}" \
    --output-report "docs/validation_reports/SILVER_DIMS_${RUN_ID}.md" \
    --enforce-quality

echo ""
echo "→ Phase 2: Bronze Quality Validation"
python -m src.validation.bronze_quality \
    --bronze-path "${BRONZE_BASE_PATH}" \
    --output-report "docs/validation_reports/BRONZE_QUALITY_${RUN_ID}.md" \
    --run-id "${RUN_ID}" \
    --enforce-quality

echo ""
echo "→ Phase 3: Base Silver Transforms (dbt)"
python -m src.runners.base_silver \
    --vars "{\"run_date\": \"${INGEST_DATE}\", \"lookback_days\": 0}" \
    --select path:models/base_silver \
    --exclude stg_ecommerce__customers \
        stg_ecommerce__customers_quarantine \
        stg_ecommerce__product_catalog \
        stg_ecommerce__product_catalog_quarantine

echo ""
echo "→ Phase 4: Silver Quality Validation"
python -m src.validation.silver \
    --bronze-path "${BRONZE_BASE_PATH}" \
    --silver-path "${SILVER_LOCAL_BASE_PATH}" \
    --quarantine-path "${SILVER_LOCAL_BASE_PATH}/quarantine" \
    --run-id "${RUN_ID}" \
    --output-report "docs/validation_reports/SILVER_QUALITY_${RUN_ID}.md" \
    --enforce-quality

echo ""
echo "→ Phase 5: Publish Silver Base (staging)"
if [[ "${SILVER_PUBLISH_MODE}" == "staging" ]]; then
  echo "→ Promote Base Silver to canonical"
  gcloud storage rsync -r "${SILVER_STAGING_PATH}" "${SILVER_BASE_PATH}"
fi


echo ""
echo "→ Phase 6: Enriched Silver Transforms (Polars)"
export SILVER_ENRICHED_PATH="${SILVER_ENRICHED_LOCAL_PATH}"

python -m src.runners.enriched.cart_attribution \
    --base-silver-path "${SILVER_LOCAL_BASE_PATH}" \
    --output-path "${SILVER_ENRICHED_LOCAL_PATH}" \
    --ingest-dt "${INGEST_DATE}"

python -m src.runners.enriched.cart_attribution_summary \
    --base-silver-path "${SILVER_LOCAL_BASE_PATH}" \
    --output-path "${SILVER_ENRICHED_LOCAL_PATH}" \
    --ingest-dt "${INGEST_DATE}"

python -m src.runners.enriched.inventory_risk \
    --base-silver-path "${SILVER_LOCAL_BASE_PATH}" \
    --output-path "${SILVER_ENRICHED_LOCAL_PATH}" \
    --ingest-dt "${INGEST_DATE}"

python -m src.runners.enriched.product_performance \
    --base-silver-path "${SILVER_LOCAL_BASE_PATH}" \
    --output-path "${SILVER_ENRICHED_LOCAL_PATH}" \
    --ingest-dt "${INGEST_DATE}"

python -m src.runners.enriched.customer_retention_signals \
    --base-silver-path "${SILVER_LOCAL_BASE_PATH}" \
    --output-path "${SILVER_ENRICHED_LOCAL_PATH}" \
    --ingest-dt "${INGEST_DATE}"

python -m src.runners.enriched.customer_lifetime_value \
    --base-silver-path "${SILVER_LOCAL_BASE_PATH}" \
    --output-path "${SILVER_ENRICHED_LOCAL_PATH}" \
    --ingest-dt "${INGEST_DATE}"

python -m src.runners.enriched.daily_business_metrics \
    --base-silver-path "${SILVER_LOCAL_BASE_PATH}" \
    --output-path "${SILVER_ENRICHED_LOCAL_PATH}" \
    --ingest-dt "${INGEST_DATE}"

python -m src.runners.enriched.sales_velocity \
    --base-silver-path "${SILVER_LOCAL_BASE_PATH}" \
    --output-path "${SILVER_ENRICHED_LOCAL_PATH}" \
    --ingest-dt "${INGEST_DATE}"

python -m src.runners.enriched.regional_financials \
    --base-silver-path "${SILVER_LOCAL_BASE_PATH}" \
    --output-path "${SILVER_ENRICHED_LOCAL_PATH}" \
    --ingest-dt "${INGEST_DATE}"

python -m src.runners.enriched.shipping_economics \
    --base-silver-path "${SILVER_LOCAL_BASE_PATH}" \
    --output-path "${SILVER_ENRICHED_LOCAL_PATH}" \
    --ingest-dt "${INGEST_DATE}"

echo ""
echo "→ Phase 7: Sync Enriched Silver to GCS"
if [[ "${ENRICHED_PUBLISH_MODE}" == "staging" ]]; then
  gcloud storage rsync -r --delete-unmatched-destination-objects \
      "${SILVER_ENRICHED_LOCAL_PATH}" \
      "${ENRICHED_STAGING_PATH}"
else
  gcloud storage rsync -r --delete-unmatched-destination-objects \
      "${SILVER_ENRICHED_LOCAL_PATH}" \
      "${SILVER_ENRICHED_PATH}"
fi

echo ""
echo "→ Phase 8: Enriched Quality Validation"
ENRICHED_VALIDATE_PATH="${SILVER_ENRICHED_PATH}"
if [[ "${ENRICHED_PUBLISH_MODE}" == "staging" ]]; then
  ENRICHED_VALIDATE_PATH="${ENRICHED_STAGING_PATH}"
fi
python -m src.validation.enriched \
    --enriched-path "${ENRICHED_VALIDATE_PATH}" \
    --run-id "${RUN_ID}" \
    --ingest-dt "${INGEST_DATE}" \
    --output-report "docs/validation_reports/ENRICHED_QUALITY_${RUN_ID}.md" \
    --enforce-quality

echo ""
echo "→ Phase 9: Promote Enriched Silver to canonical"
if [[ "${ENRICHED_PUBLISH_MODE}" == "staging" ]]; then
  gcloud storage rsync -r "${ENRICHED_STAGING_PATH}" "${SILVER_ENRICHED_PATH}"
fi

echo ""
echo "=========================================="
echo "✓ Simulated Prod Pipeline Complete"
echo "=========================================="
echo "Local Silver: ${SILVER_LOCAL_BASE_PATH}"
echo "Local Enriched: ${SILVER_ENRICHED_LOCAL_PATH}"
echo "GCS Silver: ${SILVER_BASE_PATH}"
echo "GCS Enriched: ${SILVER_ENRICHED_PATH}"
echo "Run ID: ${RUN_ID}"
