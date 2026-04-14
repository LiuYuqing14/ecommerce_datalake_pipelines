from __future__ import annotations

import json
import logging
import os
import subprocess

import pendulum

from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
from common import (
    AIRFLOW_HOME,
    COMMON_ENV,
    PIPELINE_ENV,
    get_dim_specs,
    get_dim_table_names,
    get_enriched_table_names,
    get_retry_config,
    get_silver_base_table_names,
    make_runner_callable,
    promote_staged_prefix,
    resolve_bool,
    resolve_dims_base_path,
    resolve_run_config,
)
from src.runners.enriched import (
    run_cart_attribution,
    run_cart_attribution_summary,
    run_customer_lifetime_value,
    run_customer_retention,
    run_daily_business_metrics,
    run_inventory_risk,
    run_product_performance,
    run_regional_financials,
    run_sales_velocity,
    run_shipping_economics,
)
from src.runners.mock_bq_load import mock_bigquery_load

logger = logging.getLogger(__name__)

# --- Task Callables ---


def load_config_to_xcom(**kwargs):
    """Loads configuration and pushes paths to XCom."""
    return resolve_run_config(kwargs.get("run_id"))


def _read_dims_latest_payload(path: str) -> dict | None:
    if path.startswith("gs://"):
        result = subprocess.run(
            ["gcloud", "storage", "cat", path],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            return None
        payload = result.stdout
    else:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = handle.read()
        except OSError:
            return None

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return None

    return data if isinstance(data, dict) else None


def _extract_dims_snapshot_date(payload: dict) -> str | None:
    for key in ("run_date", "snapshot_dt", "as_of_dt", "ds"):
        value = payload.get(key)
        if value:
            return str(value)
    return None


def choose_dim_refresh_task(**context) -> str:
    latest_base = resolve_dims_base_path()
    if not latest_base:
        return "trigger_dim_refresh"

    latest_path = f"{latest_base.rstrip('/')}/_latest.json"
    payload = _read_dims_latest_payload(latest_path)
    latest_date = _extract_dims_snapshot_date(payload or {})
    if latest_date == context["ds"]:
        return "skip_dim_refresh"
    return "trigger_dim_refresh"


def sync_enriched_partitions_to_gcs(
    enriched_path: str,
    bucket: str,
    env: str,
    ingest_dt: str,
    local_path: str = "/opt/airflow/data/silver/enriched",
    **kwargs,
):
    """Syncs only the relevant partitions for the current run to GCS."""
    if env not in ("dev", "prod"):
        logger.info(f"Skipping sync for env: {env}")
        return
    if not enriched_path.startswith("gs://"):
        logger.info(f"Skipping sync for non-GCS target: {enriched_path}")
        return
    if bucket == "local":
        logger.info("Skipping sync for local bucket")
        return

    from src.runners.enriched.shared import get_enriched_partitions

    partitions = get_enriched_partitions()

    # Override local path from env var if set (matching previous bash logic)
    local_path = os.getenv("SILVER_ENRICHED_LOCAL_PATH", local_path)

    for table, partition_key in partitions.items():
        # Source: /opt/airflow/data/silver/enriched/table/key=date
        local_dir = f"{local_path.rstrip('/')}/{table}/{partition_key}={ingest_dt}"

        # Target: gs://bucket/.../table/key=date
        remote_dir = f"{enriched_path.rstrip('/')}/{table}/{partition_key}={ingest_dt}"

        if not os.path.exists(local_dir):
            logger.warning(f"Local partition not found, skipping: {local_dir}")
            continue

        logger.info(f"Syncing {local_dir} -> {remote_dir}")
        # Construct cmd for gcloud storage rsync
        cmd = ["gcloud", "storage", "rsync", "-r", local_dir, remote_dir]
        subprocess.run(cmd, check=True)


def promote_enriched_partitions_to_gcs(
    staging_path: str,
    canonical_path: str,
    env: str,
    ingest_dt: str,
    **kwargs,
) -> None:
    """Promote only the current ingest_dt partitions from staging to canonical."""
    if env not in ("dev", "prod"):
        logger.info(f"Skipping promote for env: {env}")
        return
    if not staging_path:
        logger.warning("Skipping promote: staging path not set")
        return
    if not staging_path.startswith("gs://") or not canonical_path.startswith("gs://"):
        logger.warning("Skipping promote: non-GCS paths")
        return

    from src.runners.enriched.shared import get_enriched_partitions

    partitions = get_enriched_partitions()

    for table, partition_key in partitions.items():
        staging_dir = f"{staging_path.rstrip('/')}/{table}/{partition_key}={ingest_dt}"
        canonical_dir = (
            f"{canonical_path.rstrip('/')}/{table}/{partition_key}={ingest_dt}"
        )
        logger.info(f"Promoting {staging_dir} -> {canonical_dir}")
        subprocess.run(
            ["gcloud", "storage", "rsync", "-r", staging_dir, canonical_dir],
            check=True,
        )


_run_cart_attribution = make_runner_callable(run_cart_attribution)
_run_cart_attribution_summary = make_runner_callable(run_cart_attribution_summary)
_run_inventory_risk = make_runner_callable(run_inventory_risk)
_run_customer_retention = make_runner_callable(run_customer_retention)
_run_sales_velocity = make_runner_callable(run_sales_velocity)
_run_product_performance = make_runner_callable(run_product_performance)
_run_regional_financials = make_runner_callable(run_regional_financials)
_run_customer_lifetime_value = make_runner_callable(run_customer_lifetime_value)
_run_daily_business_metrics = make_runner_callable(run_daily_business_metrics)
_run_shipping_economics = make_runner_callable(run_shipping_economics)


# --- DAG Definition ---

with DAG(
    dag_id="ecom_silver_to_gold_pipeline",
    start_date=pendulum.datetime(2018, 12, 31, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=get_retry_config(),
    tags=["ecom", "silver", "gold"],
) as dag:
    dim_specs = get_dim_specs()
    dim_table_names = set(get_dim_table_names())
    silver_base_tables = get_silver_base_table_names()
    # Include only fact tables in the main pipeline validation (exclude dimensions)
    fact_tables = [t for t in silver_base_tables if t not in dim_table_names]
    silver_quality_csv = ",".join(fact_tables)

    dim_excludes = []
    for dim in dim_specs:
        model = dim["dbt_model"]
        dim_excludes.extend([model, f"{model}_quarantine"])
    dim_exclude_args = " ".join(dim_excludes)
    dim_exclude_clause = f"--exclude {dim_exclude_args}" if dim_exclude_args else ""

    enriched_table_names = get_enriched_table_names()
    enriched_callable_map = {
        "int_attributed_purchases": _run_cart_attribution,
        "int_cart_attribution": _run_cart_attribution_summary,
        "int_inventory_risk": _run_inventory_risk,
        "int_product_performance": _run_product_performance,
        "int_customer_retention_signals": _run_customer_retention,
        "int_sales_velocity": _run_sales_velocity,
        "int_regional_financials": _run_regional_financials,
        "int_customer_lifetime_value": _run_customer_lifetime_value,
        "int_daily_business_metrics": _run_daily_business_metrics,
        "int_shipping_economics": _run_shipping_economics,
    }

    # 1. Setup Config Task (Push paths to XCom)
    setup_config = PythonOperator(
        task_id="setup_pipeline_config", python_callable=load_config_to_xcom
    )

    trigger_dim_refresh = TriggerDagRunOperator(
        task_id="trigger_dim_refresh",
        trigger_dag_id="ecom_dim_refresh_pipeline",
        execution_date="{{ data_interval_start }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )
    skip_dim_refresh = EmptyOperator(task_id="skip_dim_refresh")
    dims_refresh_done = EmptyOperator(
        task_id="dims_refresh_done",
        trigger_rule="none_failed_min_one_success",
    )
    dims_freshness_gate = BranchPythonOperator(
        task_id="dims_freshness_gate",
        python_callable=choose_dim_refresh_task,
    )

    # 1.5: Validate Dims Quality (Snapshot only, no Bronze comparison)
    validate_dims_quality = BashOperator(
        task_id="validate_dims_quality",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && "
            f'DIMS_PATH="${{SILVER_DIMS_PATH:-data/silver/dims}}" '
            f'&& if [[ "$DIMS_PATH" == gs://* ]]; then '
            f'DIMS_PATH="${{SILVER_DIMS_LOCAL_PATH:-{AIRFLOW_HOME}/data/silver/dims}}"; fi '
            f'&& export SILVER_DIMS_PATH="$DIMS_PATH" '
            f"&& python -m src.validation.dims_snapshot "
            f"--run-date {{{{ ds }}}} "
            f"--run-id {{{{ run_id }}}} "
            f"--output-report docs/validation_reports/DIMS_QUALITY_{{{{ run_id | replace(':', '') }}}}.md "
            + (" --enforce-quality" if PIPELINE_ENV in {"dev", "prod"} else "")
        ),
    )

    # 2. Phase 0: Bronze Quality
    validate_bronze_quality = BashOperator(
        task_id="validate_bronze_quality",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && python -m src.validation.bronze_quality "
            f"--bronze-path {{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}} "
            f"--partition-date {{{{ ds }}}} "
            f"--lookback-days {os.getenv('BRONZE_VALIDATION_LOOKBACK_DAYS', '0')} "
            f"--output-report docs/validation_reports/BRONZE_QUALITY_{{{{ run_id | replace(':', '') }}}}.md "
            f"--run-id {{{{ run_id }}}} "
            + (" --fail-on-issues" if PIPELINE_ENV in {"dev", "prod"} else "")
        ),
    )

    # 3. Phase 1: Base Silver (dbt)
    base_silver_group = BashOperator(
        task_id="base_silver",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && "
            f"export BRONZE_BASE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}}\" "
            f"&& export SILVER_BASE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}}}\" "
            f"&& export SILVER_ENRICHED_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['enriched'] }}}}\" "
            f"&& export RUN_ID=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['run_id_clean'] }}}}\" "
            f"&& export SILVER_PUBLISH_MODE=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver_publish_mode'] }}}}\" "
            f"&& export SILVER_STAGING_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver_staging'] }}}}\" "
            f'&& DIMS_PATH="${{SILVER_DIMS_PATH:-data/silver/dims}}" '
            f'&& if [[ "$DIMS_PATH" == gs://* ]]; then '
            f'DIMS_PATH="${{SILVER_DIMS_LOCAL_PATH:-{AIRFLOW_HOME}/data/silver/dims}}"; fi '
            f'&& export SILVER_DIMS_PATH="$DIMS_PATH" '
            f"&& export DBT_DUCKDB_PATH=\"/tmp/dbt_duckdb/ecom_base_silver_{{{{ run_id | replace(':', '') }}}}.duckdb\" "
            f"&& python -m src.runners.base_silver "
            f"--vars '{{\"run_date\": \"{{{{ ds }}}}\", \"lookback_days\": {os.getenv('BASE_SILVER_LOOKBACK_DAYS', '0')}}}' "
            "--select path:models/base_silver "
            f"{dim_exclude_clause}"
        ),
    )

    # 4. Phase 1.5: Silver Quality
    validate_silver_quality = BashOperator(
        task_id="validate_silver_quality",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && "
            f"BRONZE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}}\" "
            f"SILVER_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver_validate'] }}}}\" "
            f"&& python -m src.validation.silver "
            f'--bronze-path "$BRONZE_PATH" '
            f'--silver-path "$SILVER_PATH" '
            f'--quarantine-path "$SILVER_PATH/quarantine" '
            f"--partition-date {{{{ ds }}}} "
            f"--lookback-days {os.getenv('SILVER_VALIDATION_LOOKBACK_DAYS', '0')} "
            f"--run-id {{{{ run_id }}}} "
            f"--tables {silver_quality_csv} "
            f"--output-report docs/validation_reports/SILVER_QUALITY_{{{{ run_id | replace(':', '') }}}}.md "
            + (" --enforce-quality" if PIPELINE_ENV == "prod" else "")
        ),
    )

    # 5. Sync Silver Base
    # Removed: sync_silver_base_to_gcs (Handled by base_silver runner)
    promote_silver_base = PythonOperator(
        task_id="promote_silver_base",
        python_callable=promote_staged_prefix,
        op_kwargs={
            "staging_path": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver_staging'] }}",
            "canonical_path": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}",
            "env": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['env'] }}",
        },
    )

    # 6. Phase 2: Enriched Silver
    with TaskGroup("enriched_silver") as enriched_silver_group:
        for table in enriched_table_names:
            runner = enriched_callable_map.get(table)
            if not runner:
                continue
            PythonOperator(
                task_id=table,
                python_callable=runner,
                op_kwargs={"ingest_dt": "{{ ds }}"},
            )

    # 7. Validate Enriched
    validate_enriched_quality = BashOperator(
        task_id="validate_enriched_quality",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && "
            f"ENRICHED_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['enriched_validate'] }}}}\" "
            "&& python -m src.validation.enriched "
            f'--enriched-path "$ENRICHED_PATH" '
            f"--run-id {{{{ run_id }}}} "
            f"--ingest-dt {{{{ ds }}}} "
            f"--output-report docs/validation_reports/ENRICHED_QUALITY_{{{{ run_id | replace(':', '') }}}}.md "
            + (" --enforce-quality" if PIPELINE_ENV == "prod" else "")
        ),
    )

    # 8. Sync Enriched
    sync_silver_enriched = PythonOperator(
        task_id="sync_silver_enriched_to_gcs",
        python_callable=sync_enriched_partitions_to_gcs,
        op_kwargs={
            "enriched_path": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['enriched_validate'] }}",
            "bucket": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bucket'] }}",
            "env": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['env'] }}",
            "ingest_dt": "{{ ds }}",
        },
    )

    promote_enriched = PythonOperator(
        task_id="promote_enriched",
        python_callable=promote_enriched_partitions_to_gcs,
        op_kwargs={
            "staging_path": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['enriched_staging'] }}",
            "canonical_path": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['enriched'] }}",
            "env": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['env'] }}",
            "ingest_dt": "{{ ds }}",
        },
    )

    # 9. Validate Enriched Parquet Files (Mock BQ Load)
    with TaskGroup("validate_enriched_parquet") as validate_parquet_group:
        from src.runners.enriched.shared import get_enriched_partitions

        enriched_partitions = get_enriched_partitions()

        for table in enriched_partitions.keys():
            partition_key = enriched_partitions.get(table, "ingest_dt")
            PythonOperator(
                task_id=f"validate_{table}",
                python_callable=mock_bigquery_load,
                op_kwargs={
                    "enriched_path": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['enriched'] }}",
                    "table": table,
                    "partition_key": partition_key,
                    "partition_value": "{{ ds }}",
                    "project_id": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['project_id'] }}",
                    "dataset": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bq_dataset'] }}",
                },
            )

    # 10. Gates
    should_run_gold = ShortCircuitOperator(
        task_id="should_run_gold",
        python_callable=lambda: resolve_bool(
            "GOLD_PIPELINE_ENABLED", PIPELINE_ENV in {"dev", "prod"}
        ),
    )

    should_load_bigquery = ShortCircuitOperator(
        task_id="should_load_bigquery",
        python_callable=lambda: resolve_bool(
            "BQ_LOAD_ENABLED", PIPELINE_ENV in {"dev", "prod"}
        ),
    )

    # 11. Load to BQ
    with TaskGroup("load_to_bigquery") as load_bigquery_group:
        from src.runners.enriched.shared import get_enriched_partitions

        enriched_partitions = get_enriched_partitions()
        enriched_tables = list(enriched_partitions.keys())

        for table in enriched_tables:
            partition_key = enriched_partitions.get(table, "ingest_dt")
            BigQueryInsertJobOperator(
                task_id=f"load_{table}",
                location=os.getenv("BQ_LOCATION", "US"),
                configuration={
                    "load": {
                        "destinationTable": {
                            "projectId": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['project_id'] }}",
                            "datasetId": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bq_dataset'] }}",
                            "tableId": table,
                        },
                        "sourceUris": [
                            "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['enriched'] }}/"
                            + f"{table}/{partition_key}={{{{ ds }}}}/*.parquet"
                        ],
                        "sourceFormat": "PARQUET",
                        "writeDisposition": "WRITE_APPEND",
                        "createDisposition": "CREATE_IF_NEEDED",
                        "timePartitioning": {
                            "type": "DAY",
                            "field": partition_key,
                        },
                    }
                },
            )

    # 12. Gold Marts
    gold_marts_build = BashOperator(
        task_id="gold_marts_build",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && dbt run --project-dir dbt_bigquery --profiles-dir dbt_bigquery "
            f"--select tag:gold"
        ),
    )

    # Flow
    (
        setup_config
        >> dims_freshness_gate
        >> [trigger_dim_refresh, skip_dim_refresh]
        >> dims_refresh_done
        >> validate_dims_quality
        >> validate_bronze_quality
        >> base_silver_group
        >> validate_silver_quality
        >> promote_silver_base
        >> enriched_silver_group
        >> sync_silver_enriched
        >> validate_enriched_quality
        >> promote_enriched
        >> validate_parquet_group
        >> should_load_bigquery
        >> load_bigquery_group
        >> should_run_gold
        >> gold_marts_build
    )
