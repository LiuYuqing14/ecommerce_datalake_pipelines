#!/usr/bin/env bash
if [[ -z "${ECOM_CLI_SUPPRESS_DEPRECATION:-}" ]]; then
  echo "DEPRECATED: use \`ecomlake sample pull\` instead of scripts/pull_bronze_sample.sh" >&2
fi
set -euo pipefail

# Pull sample partitions per table for schema discovery.
# Usage: ./scripts/pull_bronze_sample.sh [INGEST_DTS] [DEST_DIR]
# INGEST_DTS can be a single date (YYYY-MM-DD), a month (YYYY-MM),
# or a comma-separated list of dates/months.
# Set MAX_DAYS to limit how many days are pulled per month.
#
# Notes:
# - Transactional tables (orders, carts, returns) use ingest_dt partitioning
# - Customers table uses signup_date partitioning
# - Product catalog uses category partitioning (pulls all categories)

INGEST_DTS="${1:-2020-06,2023-01,2025-12}"
DEST_DIR="${2:-samples/bronze}"
MAX_DAYS="${MAX_DAYS:-0}"

# Tables partitioned by ingest_dt
INGEST_DT_TABLES=(
  "gcs-automation-project-raw/ecom/raw/cart_items"
  "gcs-automation-project-raw/ecom/raw/order_items"
  "gcs-automation-project-raw/ecom/raw/orders"
  "gcs-automation-project-raw/ecom/raw/return_items"
  "gcs-automation-project-raw/ecom/raw/returns"
  "gcs-automation-project-raw/ecom/raw/shopping_carts"
)

# Tables with alternative partitioning schemes
SIGNUP_DATE_TABLES=(
  "gcs-automation-project-raw/ecom/raw/customers"
)

CATEGORY_TABLES=(
  "gcs-automation-project-raw/ecom/raw/product_catalog"
)

mkdir -p "${DEST_DIR}"

IFS=',' read -r -a ingest_dates <<< "${INGEST_DTS}"

# Process ingest_dt partitioned tables
for ingest_dt in "${ingest_dates[@]}"; do
  ingest_dt="$(echo "${ingest_dt}" | xargs)"
  if [[ "${ingest_dt}" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
    echo "==> Sampling month=${ingest_dt}"
  else
    echo "==> Sampling ingest_dt=${ingest_dt}"
  fi

  for table_path in "${INGEST_DT_TABLES[@]}"; do
    table_name="${table_path##*/}"

    if [[ "${ingest_dt}" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
      # Month mode: discover matching partition directories for this table.
      partitions=$(gsutil ls -d "gs://${table_path}/ingest_dt=${ingest_dt}-*/" 2>/dev/null || true)
      if [[ -z "${partitions}" ]]; then
        echo "  -> ${table_name} (no ingest_dt partitions for ${ingest_dt})"
        continue
      fi
      if [[ "${MAX_DAYS}" -gt 0 ]]; then
        partitions=$(echo "${partitions}" | head -n "${MAX_DAYS}")
      fi
      while read -r partition_path; do
        [[ -z "${partition_path}" ]] && continue
        day_value="${partition_path##*ingest_dt=}"
        day_value="${day_value%/}"
        local_dir="${DEST_DIR}/${table_name}/ingest_dt=${day_value}"

        echo "  -> ${table_name} (ingest_dt=${day_value})"
        mkdir -p "${local_dir}"

        gsutil -q cp "${partition_path}_MANIFEST.json" "${local_dir}/" || true
        gsutil -q ls "${partition_path}*.parquet" | head -n 3 | \
          xargs -I {} gsutil -q cp {} "${local_dir}/" || true
      done <<< "${partitions}"
    else
      partition_path="gs://${table_path}/ingest_dt=${ingest_dt}"
      local_dir="${DEST_DIR}/${table_name}/ingest_dt=${ingest_dt}"

      echo "  -> ${table_name} (ingest_dt=${ingest_dt})"
      mkdir -p "${local_dir}"

      gsutil -q cp "${partition_path}/_MANIFEST.json" "${local_dir}/" || true
      gsutil -q ls "${partition_path}/*.parquet" | head -n 3 | \
        xargs -I {} gsutil -q cp {} "${local_dir}/" || true
    fi
  done

  # Process signup_date partitioned tables (customers)
  for table_path in "${SIGNUP_DATE_TABLES[@]}"; do
    table_name="${table_path##*/}"

    if [[ "${ingest_dt}" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
      # Month mode: discover matching signup_date partitions
      partitions=$(gsutil ls -d "gs://${table_path}/signup_date=${ingest_dt}-*/" 2>/dev/null || true)
      if [[ -z "${partitions}" ]]; then
        echo "  -> ${table_name} (no signup_date partitions for ${ingest_dt})"
        continue
      fi
      if [[ "${MAX_DAYS}" -gt 0 ]]; then
        partitions=$(echo "${partitions}" | head -n "${MAX_DAYS}")
      fi
      while read -r partition_path; do
        [[ -z "${partition_path}" ]] && continue
        day_value="${partition_path##*signup_date=}"
        day_value="${day_value%/}"
        local_dir="${DEST_DIR}/${table_name}/signup_date=${day_value}"

        echo "  -> ${table_name} (signup_date=${day_value})"
        mkdir -p "${local_dir}"

        gsutil -q cp "${partition_path}_MANIFEST.json" "${local_dir}/" || true
        gsutil -q ls "${partition_path}*.parquet" | head -n 3 | \
          xargs -I {} gsutil -q cp {} "${local_dir}/" || true
      done <<< "${partitions}"
    else
      partition_path="gs://${table_path}/signup_date=${ingest_dt}"
      local_dir="${DEST_DIR}/${table_name}/signup_date=${ingest_dt}"

      echo "  -> ${table_name} (signup_date=${ingest_dt})"
      mkdir -p "${local_dir}"

      gsutil -q cp "${partition_path}/_MANIFEST.json" "${local_dir}/" || true
      gsutil -q ls "${partition_path}/*.parquet" | head -n 3 | \
        xargs -I {} gsutil -q cp {} "${local_dir}/" || true
    fi
  done
done

# Process category partitioned tables (product_catalog)
# These pull all categories regardless of date filters
echo "==> Sampling category-partitioned tables"
for table_path in "${CATEGORY_TABLES[@]}"; do
  table_name="${table_path##*/}"

  # Discover all category partitions
  categories=$(gsutil ls -d "gs://${table_path}/category=*/" 2>/dev/null || true)
  if [[ -z "${categories}" ]]; then
    echo "  -> ${table_name} (no category partitions found)"
    continue
  fi

  while read -r partition_path; do
    [[ -z "${partition_path}" ]] && continue
    category_value="${partition_path##*category=}"
    category_value="${category_value%/}"
    local_dir="${DEST_DIR}/${table_name}/category=${category_value}"

    echo "  -> ${table_name} (category=${category_value})"
    mkdir -p "${local_dir}"

    gsutil -q cp "${partition_path}_MANIFEST.json" "${local_dir}/" || true
    gsutil -q ls "${partition_path}*.parquet" | head -n 3 | \
      xargs -I {} gsutil -q cp {} "${local_dir}/" || true
  done <<< "${categories}"
done

echo "Done. Samples in ${DEST_DIR}."
