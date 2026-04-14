#!/usr/bin/env bash
if [[ -z "${ECOM_CLI_SUPPRESS_DEPRECATION:-}" ]]; then
  echo "DEPRECATED: use \`ecomlake airflow bootstrap\` instead of scripts/bootstrap_airflow.sh" >&2
fi
set -euo pipefail

mkdir -p airflow/dags airflow/logs airflow/plugins

read -r -p "Full reset (docker compose down -v)? [y/N] " do_reset
if [[ "${do_reset}" =~ ^[Yy]$ ]]; then
  echo "Stopping containers and removing volumes..."
  docker compose down -v
fi

echo "Initializing Airflow services..."
docker compose up airflow-init

echo "Starting Airflow webserver and scheduler..."
docker compose up -d airflow-webserver airflow-scheduler

echo "Airflow UI: http://localhost:8080"
