from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timezone

import pendulum

from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from common import (
    AIRFLOW_HOME,
    COMMON_ENV,
    PIPELINE_ENV,
    get_dim_specs,
    get_dim_table_names,
    get_retry_config,
    promote_staged_prefix,
    resolve_dims_base_path,
    resolve_run_config,
)
from src.runners.dims_snapshot import snapshot_dims

# --- Task Callables ---


def load_config_to_xcom(**kwargs):
    """Loads configuration and pushes paths to XCom."""
    return resolve_run_config(kwargs.get("run_id"))


def publish_dims_latest(**context) -> None:
    """Persist dims freshness pointer for the current run_date."""
    latest_base = resolve_dims_base_path()
    if not latest_base:
        return

    payload = {
        "run_date": context["ds"],
        "run_id": context["run_id"],
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    latest_path = f"{latest_base.rstrip('/')}/_latest.json"

    if latest_base.startswith("gs://"):
        tmp_file = "/tmp/dims_latest.json"
        with open(tmp_file, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        subprocess.run(
            ["gcloud", "storage", "cp", tmp_file, latest_path],
            check=True,
        )
        return

    os.makedirs(latest_base, exist_ok=True)
    with open(latest_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def run_snapshot_dims(
    run_date: str,
    silver_base_path: str,
    dims_publish_mode: str,
    dims_staging_path: str,
    run_id_clean: str,
) -> None:
    """Run dims snapshot with optional staging configuration."""
    if run_id_clean:
        os.environ["RUN_ID"] = run_id_clean
    if dims_publish_mode:
        os.environ["DIMS_PUBLISH_MODE"] = dims_publish_mode
    if dims_staging_path:
        os.environ["DIMS_STAGING_PATH"] = dims_staging_path
    snapshot_dims(run_date, silver_base_path)


# --- DAG Definition ---

with DAG(
    dag_id="ecom_dim_refresh_pipeline",
    start_date=pendulum.datetime(2020, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=get_retry_config(),
    tags=["ecom", "silver", "dims"],
) as dag:
    dim_specs = get_dim_specs()
    dim_tables = get_dim_table_names()
    dim_table_csv = ",".join(dim_tables)
    dim_tables_arg = f"--tables {dim_table_csv} " if dim_table_csv else ""

    # 1. Setup Config
    setup_config = PythonOperator(
        task_id="setup_pipeline_config", python_callable=load_config_to_xcom
    )

    # 2. Validate Bronze Dims
    validate_bronze_dims = BashOperator(
        task_id="validate_bronze_dims",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && python -m src.validation.bronze_quality "
            f"--bronze-path {{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}} "
            f"{dim_tables_arg}"
            f"--partition-date {{{{ ds }}}} "
            f"--lookback-days {os.getenv('BRONZE_VALIDATION_LOOKBACK_DAYS', '0')} "
            f"--output-report docs/validation_reports/BRONZE_DIMS_{{{{ run_id | replace(':', '') }}}}.md "
            f"--run-id {{{{ run_id }}}} "
            + (" --enforce-quality" if PIPELINE_ENV in {"dev", "prod"} else "")
        ),
    )

    # 3. Refresh Dims
    # DBT_DUCKDB_PATH unique per task to avoid lock contention
    with TaskGroup("refresh_dims") as refresh_dims_group:
        for dim in dim_specs:
            table = dim["name"]
            model = dim["dbt_model"]
            BashOperator(
                task_id=f"refresh_{table}",
                env=COMMON_ENV,
                bash_command=(
                    f"cd {AIRFLOW_HOME} && "
                    f"export BRONZE_BASE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['bronze'] }}}}\" "
                    f'&& export BRONZE_SYNC_TABLES="{table}" '
                    f"&& export SILVER_BASE_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}}}\" "
                    f"&& export RUN_ID=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['run_id_clean'] }}}}\" "
                    f"&& export SILVER_PUBLISH_MODE=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver_publish_mode'] }}}}\" "
                    f"&& export SILVER_STAGING_PATH=\"{{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver_staging'] }}}}\" "
                    f"&& export DBT_DUCKDB_PATH=\"/tmp/dbt_duckdb/ecom_{table}_{{{{ run_id | replace(':', '') }}}}.duckdb\" "
                    f"&& python -m src.runners.base_silver "
                    f"--select {model} {model}_quarantine"
                ),
            )

    validate_dims_snapshot = BashOperator(
        task_id="validate_dims_snapshot",
        env=COMMON_ENV,
        bash_command=(
            f"cd {AIRFLOW_HOME} && python -m src.validation.dims_snapshot "
            f"--dims-path {{{{ ti.xcom_pull(task_ids='setup_pipeline_config')['dims_validate'] }}}} "
            f"--run-date {{{{ ds }}}} "
            f"--output-report docs/validation_reports/DIMS_SNAPSHOT_{{{{ run_id | replace(':', '') }}}}.md "
            + (" --enforce-quality" if PIPELINE_ENV == "prod" else "")
        ),
    )
    snapshot_dims_task = PythonOperator(
        task_id="snapshot_dims",
        python_callable=run_snapshot_dims,
        op_kwargs={
            "run_date": "{{ ds }}",
            "silver_base_path": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}",
            "dims_publish_mode": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['dims_publish_mode'] }}",
            "dims_staging_path": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['dims_staging'] }}",
            "run_id_clean": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['run_id_clean'] }}",
        },
    )
    promote_dims_snapshot = PythonOperator(
        task_id="promote_dims_snapshot",
        python_callable=promote_staged_prefix,
        op_kwargs={
            "staging_path": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['dims_staging'] }}",
            "canonical_path": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['dims'] }}",
            "env": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['env'] }}",
        },
    )
    promote_silver_base = PythonOperator(
        task_id="promote_silver_base",
        python_callable=promote_staged_prefix,
        op_kwargs={
            "staging_path": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver_staging'] }}",
            "canonical_path": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['silver'] }}",
            "env": "{{ ti.xcom_pull(task_ids='setup_pipeline_config')['env'] }}",
        },
    )
    publish_dims_latest_task = PythonOperator(
        task_id="publish_dims_latest",
        python_callable=publish_dims_latest,
    )

    # Flow
    (
        setup_config
        >> validate_bronze_dims
        >> refresh_dims_group
        >> snapshot_dims_task
        >> validate_dims_snapshot
        >> promote_silver_base
        >> promote_dims_snapshot
        >> publish_dims_latest_task
    )
