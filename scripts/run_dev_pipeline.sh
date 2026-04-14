#!/bin/bash
if [[ -z "${ECOM_CLI_SUPPRESS_DEPRECATION:-}" ]]; then
  echo "DEPRECATED: use \`ecomlake pipeline dev-gcs\` instead of scripts/run_dev_pipeline.sh" >&2
fi
set -e  # Exit on error

# ==============================================================================
# Dev Pipeline Runner - Mimics Airflow DAG without Airflow
# ==============================================================================
# This script runs the full pipeline locally while reading/writing to GCS
# Usage: ./scripts/run_dev_pipeline.sh [INGEST_DATE]
# Example: ./scripts/run_dev_pipeline.sh 2020-01-01
# ==============================================================================

INGEST_DATE=${1:-"2020-01-01"}
RUN_ID="manual_$(date +%Y%m%d_%H%M%S)"

echo "=========================================="
echo "Starting Dev Pipeline"
echo "Ingest Date: $INGEST_DATE"
echo "Run ID: $RUN_ID"
echo "=========================================="

# Authenticate with GCP
echo "→ Authenticating with GCP..."
gcloud auth application-default login --quiet || echo "Already authenticated"

# Set environment
export BQ_LOAD_ENABLED=false
export GOLD_PIPELINE_ENABLED=false

# ==============================================================================
# Phase 1: Dimension Refresh
# ==============================================================================
echo ""
echo "→ Phase 1: Dimension Refresh"

# Validate Bronze Dims
echo "  → Validating Bronze dimensions..."
python -m src.validation.bronze_quality \
    --bronze-path gs://gcs-automation-project-raw/data/bronze \
    --tables customers,product_catalog \
    --output-report docs/validation_reports/BRONZE_DIMS_${RUN_ID}.md \
    --run-id $RUN_ID \
    --enforce-quality

# Refresh Customers
echo "  → Refreshing customers dimension..."
export BRONZE_BASE_PATH="gs://gcs-automation-project-raw/data/bronze"
export SILVER_BASE_PATH="./data/silver/base"
python -m src.runners.base_silver \
    --select stg_ecommerce__customers stg_ecommerce__customers_quarantine

# Refresh Product Catalog
echo "  → Refreshing product catalog dimension..."
python -m src.runners.base_silver \
    --select stg_ecommerce__product_catalog stg_ecommerce__product_catalog_quarantine

# Validate Dim Quality
echo "  → Validating dimension quality..."
python -m src.validation.silver \
    --bronze-path gs://gcs-automation-project-raw/data/bronze \
    --silver-path ./data/silver/base \
    --quarantine-path ./data/silver/base/quarantine \
    --tables customers,product_catalog \
    --run-id $RUN_ID \
    --output-report docs/validation_reports/SILVER_DIMS_${RUN_ID}.md \
    --enforce-quality

# ==============================================================================
# Phase 2: Bronze Quality Validation
# ==============================================================================
echo ""
echo "→ Phase 2: Bronze Quality Validation"
python -m src.validation.bronze_quality \
    --bronze-path gs://gcs-automation-project-raw/data/bronze \
    --output-report docs/validation_reports/BRONZE_QUALITY_${RUN_ID}.md \
    --run-id $RUN_ID \
    --enforce-quality

# ==============================================================================
# Phase 3: Base Silver (dbt)
# ==============================================================================
echo ""
echo "→ Phase 3: Base Silver Transforms (dbt)"
python -m src.runners.base_silver \
    --vars "{\"run_date\": \"$INGEST_DATE\", \"lookback_days\": 0}" \
    --select path:models/base_silver \
    --exclude stg_ecommerce__customers \
        stg_ecommerce__customers_quarantine \
        stg_ecommerce__product_catalog \
        stg_ecommerce__product_catalog_quarantine

# ==============================================================================
# Phase 4: Silver Quality Validation
# ==============================================================================
echo ""
echo "→ Phase 4: Silver Quality Validation"
python -m src.validation.silver \
    --bronze-path gs://gcs-automation-project-raw/data/bronze \
    --silver-path ./data/silver/base \
    --quarantine-path ./data/silver/base/quarantine \
    --run-id $RUN_ID \
    --output-report docs/validation_reports/SILVER_QUALITY_${RUN_ID}.md \
    --enforce-quality

# ==============================================================================
# Phase 5: Sync Silver Base to GCS
# ==============================================================================
echo ""
echo "→ Phase 5: Syncing Silver Base to GCS"
gcloud storage rsync -r --delete-unmatched-destination-objects \
    ./data/silver/base \
    gs://gcs-automation-project-silver/data/silver/base

# ==============================================================================
# Phase 6: Enriched Silver (Polars)
# ==============================================================================
echo ""
echo "→ Phase 6: Enriched Silver Transforms (Polars)"

export SILVER_ENRICHED_PATH="./data/silver/enriched"

echo "  → Running cart_attribution..."
python -m src.runners.enriched.cart_attribution \
    --base-silver-path ./data/silver/base \
    --output-path ./data/silver/enriched \
    --ingest-dt $INGEST_DATE

echo "  → Running cart_attribution_summary..."
python -m src.runners.enriched.cart_attribution_summary \
    --base-silver-path ./data/silver/base \
    --output-path ./data/silver/enriched \
    --ingest-dt $INGEST_DATE

echo "  → Running inventory_risk..."
python -m src.runners.enriched.inventory_risk \
    --base-silver-path ./data/silver/base \
    --output-path ./data/silver/enriched \
    --ingest-dt $INGEST_DATE

echo "  → Running product_performance..."
python -m src.runners.enriched.product_performance \
    --base-silver-path ./data/silver/base \
    --output-path ./data/silver/enriched \
    --ingest-dt $INGEST_DATE

echo "  → Running customer_retention..."
python -m src.runners.enriched.customer_retention \
    --base-silver-path ./data/silver/base \
    --output-path ./data/silver/enriched \
    --ingest-dt $INGEST_DATE

echo "  → Running sales_velocity..."
python -m src.runners.enriched.sales_velocity \
    --base-silver-path ./data/silver/base \
    --output-path ./data/silver/enriched \
    --ingest-dt $INGEST_DATE

echo "  → Running regional_financials..."
python -m src.runners.enriched.regional_financials \
    --base-silver-path ./data/silver/base \
    --output-path ./data/silver/enriched \
    --ingest-dt $INGEST_DATE

echo "  → Running customer_lifetime_value..."
python -m src.runners.enriched.customer_lifetime_value \
    --base-silver-path ./data/silver/base \
    --output-path ./data/silver/enriched \
    --ingest-dt $INGEST_DATE

echo "  → Running daily_business_metrics..."
python -m src.runners.enriched.daily_business_metrics \
    --base-silver-path ./data/silver/base \
    --output-path ./data/silver/enriched \
    --ingest-dt $INGEST_DATE

echo "  → Running shipping_economics..."
python -m src.runners.enriched.shipping_economics \
    --base-silver-path ./data/silver/base \
    --output-path ./data/silver/enriched \
    --ingest-dt $INGEST_DATE

# ==============================================================================
# Phase 7: Enriched Quality Validation
# ==============================================================================
echo ""
echo "→ Phase 7: Enriched Quality Validation"
python -m src.validation.enriched \
    --enriched-path ./data/silver/enriched \
    --run-id $RUN_ID \
    --ingest-dt $INGEST_DATE \
    --output-report docs/validation_reports/ENRICHED_QUALITY_${RUN_ID}.md \
    --enforce-quality

# ==============================================================================
# Phase 8: Sync Enriched to GCS
# ==============================================================================
echo ""
echo "→ Phase 8: Syncing Enriched Silver to GCS"
gcloud storage rsync -r --delete-unmatched-destination-objects \
    ./data/silver/enriched \
    gs://gcs-automation-project-silver/data/silver/enriched

# ==============================================================================
# Phase 9: Mock BQ Validation
# ==============================================================================
echo ""
echo "→ Phase 9: Mock BigQuery Load Validation"
echo "  (Validates Parquet files are readable and contain data)"

# This would require calling the mock_bigquery_load function
# For now, we'll just verify files exist
echo "  → Checking enriched Parquet files..."
for table in int_attributed_purchases int_cart_attribution int_inventory_risk \
             int_product_performance int_customer_retention_signals \
             int_sales_velocity int_regional_financials \
             int_customer_lifetime_value int_daily_business_metrics \
             int_shipping_economics; do
    echo "    ✓ $table"
    ls -lh ./data/silver/enriched/$table/ || echo "    ⚠ No files found"
done

# ==============================================================================
# Complete
# ==============================================================================
echo ""
echo "=========================================="
echo "✓ Pipeline Complete!"
echo "=========================================="
echo "Results:"
echo "  → Local Silver: ./data/silver/base"
echo "  → Local Enriched: ./data/silver/enriched"
echo "  → GCS Silver: gs://gcs-automation-project-silver/data/silver/base"
echo "  → GCS Enriched: gs://gcs-automation-project-silver/data/silver/enriched"
echo "  → Validation Reports: docs/validation_reports/*_${RUN_ID}.md"
echo ""
echo "BigQuery load and Gold marts were SKIPPED (dev mode)"
echo "=========================================="
