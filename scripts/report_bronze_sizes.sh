#!/usr/bin/env bash
if [[ -z "${ECOM_CLI_SUPPRESS_DEPRECATION:-}" ]]; then
  echo "DEPRECATED: use \`ecomlake bucket report\` instead of scripts/report_bronze_sizes.sh" >&2
fi
set -euo pipefail

# Generate a Markdown report of bucket/prefix sizes and per-table sizes.
# Usage: ./scripts/report_bronze_sizes.sh [BUCKET] [PREFIX] [OUTPUT]
# Defaults read from config/config.yml when present.
#
# Optional environment variables:
#   SHOW_COSTS=1              Enable cost estimation table
#   STANDARD_PRICE=0.020      Price per GB/month for Standard storage (default: $0.020)
#   NEARLINE_PRICE=0.010      Price per GB/month for Nearline storage (default: $0.010)
#   COLDLINE_PRICE=0.004      Price per GB/month for Coldline storage (default: $0.004)

CONFIG_PATH="${CONFIG_PATH:-config/config.yml}"

default_bucket="gcs-automation-project-raw"
default_prefix="ecom/raw"

# Cost estimation defaults (USD per GB per month)
SHOW_COSTS="${SHOW_COSTS:-0}"
STANDARD_PRICE="${STANDARD_PRICE:-0.020}"
NEARLINE_PRICE="${NEARLINE_PRICE:-0.010}"
COLDLINE_PRICE="${COLDLINE_PRICE:-0.004}"

if [[ -f "${CONFIG_PATH}" ]]; then
  read -r cfg_bucket cfg_prefix < <(
    python - <<'PY'
import yaml
from pathlib import Path

cfg = yaml.safe_load(Path("config/config.yml").read_text()) or {}
pipeline = cfg.get("pipeline", {})
print(pipeline.get("bronze_bucket", ""))
print(pipeline.get("bronze_prefix", ""))
PY
  )
  if [[ -n "${cfg_bucket}" ]]; then
    default_bucket="${cfg_bucket}"
  fi
  if [[ -n "${cfg_prefix}" ]]; then
    default_prefix="${cfg_prefix}"
  fi
fi

BUCKET="${1:-${default_bucket}}"
PREFIX="${2:-${default_prefix}}"
OUTPUT="${3:-docs/data/BRONZE_BUCKET_SIZES.md}"

bucket_uri="gs://${BUCKET}"
prefix_uri="${bucket_uri}/${PREFIX}"

# Convert bytes to human-readable format (KB, MB, GB, TB)
human_size() {
  local bytes="$1"
  local kb=$(echo "scale=2; $bytes / 1024" | bc)
  local mb=$(echo "scale=2; $bytes / 1048576" | bc)
  local gb=$(echo "scale=2; $bytes / 1073741824" | bc)
  local tb=$(echo "scale=2; $bytes / 1099511627776" | bc)

  if (( $(echo "$tb >= 1" | bc -l) )); then
    echo "${tb} TB"
  elif (( $(echo "$gb >= 1" | bc -l) )); then
    echo "${gb} GB"
  elif (( $(echo "$mb >= 1" | bc -l) )); then
    echo "${mb} MB"
  elif (( $(echo "$kb >= 1" | bc -l) )); then
    echo "${kb} KB"
  else
    echo "${bytes} bytes"
  fi
}

# Convert bytes to GB (for cost calculations)
bytes_to_gb() {
  local bytes="$1"
  echo "scale=4; $bytes / 1073741824" | bc
}

# Calculate monthly storage cost
calculate_cost() {
  local gb="$1"
  local price_per_gb="$2"
  echo "scale=2; $gb * $price_per_gb" | bc
}

echo "Scanning full bucket: ${bucket_uri}"
bucket_total_bytes=$(gsutil du -s "${bucket_uri}" | awk '{print $1}')

echo "Scanning prefix: ${prefix_uri}"
prefix_total_bytes=$(gsutil du -s "${prefix_uri}" | awk '{print $1}')

report_date=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

{
  echo "# Bronze Bucket Size Report"
  echo ""
  echo "Generated: ${report_date}"
  echo ""
  echo "## Full Bucket Totals"
  echo ""
  echo "- Bucket: \`${bucket_uri}\`"
  echo "  - Total: **$(human_size "${bucket_total_bytes}")**"
  echo "  - Raw bytes: ${bucket_total_bytes}"
  echo ""
  echo "## Prefix Summary"
  echo ""
  echo "- Prefix: \`${prefix_uri}\`"
  echo "  - Total: **$(human_size "${prefix_total_bytes}")**"
  echo "  - Raw bytes: ${prefix_total_bytes}"
  echo ""
  echo "## Per-Table Breakdown"
  echo ""
  echo "| Table | Size | Raw Bytes |"
  echo "| --- | --- | --- |"

  gsutil ls "${prefix_uri}/" | while read -r table_path; do
    table_name=$(basename "${table_path}")
    echo "Scanning table: ${table_name}" >&2
    table_bytes=$(gsutil du -s "${table_path}" | awk '{print $1}')
    echo "| ${table_name} | **$(human_size "${table_bytes}")** | ${table_bytes} |"
  done

  # Optional cost estimation section
  if [[ "${SHOW_COSTS}" == "1" ]]; then
    echo ""
    echo "---"
    echo ""
    echo "## Cost Estimation"
    echo ""
    echo "**Note**: Prices shown are approximate and may vary by region, storage class, and time. Verify current pricing at the [official GCS pricing page](https://cloud.google.com/storage/pricing)."
    echo ""

    # Calculate GB sizes
    bucket_gb=$(bytes_to_gb "${bucket_total_bytes}")
    prefix_gb=$(bytes_to_gb "${prefix_total_bytes}")

    # Calculate costs for different storage classes
    bucket_standard=$(calculate_cost "${bucket_gb}" "${STANDARD_PRICE}")
    bucket_nearline=$(calculate_cost "${bucket_gb}" "${NEARLINE_PRICE}")
    bucket_coldline=$(calculate_cost "${bucket_gb}" "${COLDLINE_PRICE}")

    prefix_standard=$(calculate_cost "${prefix_gb}" "${STANDARD_PRICE}")
    prefix_nearline=$(calculate_cost "${prefix_gb}" "${NEARLINE_PRICE}")
    prefix_coldline=$(calculate_cost "${prefix_gb}" "${COLDLINE_PRICE}")

    echo "### Full Bucket Monthly Costs"
    echo ""
    echo "| Storage Class | Price/GB/Month | Total GB | Estimated Monthly Cost |"
    echo "| --- | --- | --- | --- |"
    echo "| Standard | \$${STANDARD_PRICE} | ${bucket_gb} GB | **\$${bucket_standard}** |"
    echo "| Nearline | \$${NEARLINE_PRICE} | ${bucket_gb} GB | **\$${bucket_nearline}** |"
    echo "| Coldline | \$${COLDLINE_PRICE} | ${bucket_gb} GB | **\$${bucket_coldline}** |"
    echo ""
    echo "### Prefix Monthly Costs"
    echo ""
    echo "| Storage Class | Price/GB/Month | Total GB | Estimated Monthly Cost |"
    echo "| --- | --- | --- | --- |"
    echo "| Standard | \$${STANDARD_PRICE} | ${prefix_gb} GB | **\$${prefix_standard}** |"
    echo "| Nearline | \$${NEARLINE_PRICE} | ${prefix_gb} GB | **\$${prefix_nearline}** |"
    echo "| Coldline | \$${COLDLINE_PRICE} | ${prefix_gb} GB | **\$${prefix_coldline}** |"
    echo ""
    echo "_Pricing based on Standard (\$${STANDARD_PRICE}/GB), Nearline (\$${NEARLINE_PRICE}/GB), and Coldline (\$${COLDLINE_PRICE}/GB) storage classes._"
  fi
} > "${OUTPUT}"

echo "Wrote ${OUTPUT}"
