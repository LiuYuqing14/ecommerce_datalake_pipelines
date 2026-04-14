"""Click-based CLI for ecom-datalake-pipelines."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Iterable, Sequence

import click


def _ensure_list(args: Iterable[str] | None) -> list[str]:
    return list(args) if args else []


def _compose_base_cmd() -> list[str]:
    if shutil.which("docker-compose"):
        return ["docker-compose"]
    return ["docker", "compose"]


def _run_cmd(
    cmd: Sequence[str],
    *,
    env_overrides: dict[str, str] | None = None,
    cwd: str | Path | None = None,
    check: bool = True,
) -> int:
    env = os.environ.copy()
    env["ECOM_CLI_SUPPRESS_DEPRECATION"] = "1"
    if env_overrides:
        for key, value in env_overrides.items():
            if value is not None:
                env[key] = value
    result = subprocess.run(cmd, env=env, cwd=cwd)
    if check and result.returncode != 0:
        raise SystemExit(result.returncode)
    return result.returncode


def _run_python_script(
    script_path: str,
    args: Iterable[str] | None = None,
    *,
    env_overrides: dict[str, str] | None = None,
    cwd: str | Path | None = None,
) -> None:
    _run_cmd(
        [sys.executable, script_path, *_ensure_list(args)],
        env_overrides=env_overrides,
        cwd=cwd,
    )


def _run_python_module(
    module: str,
    args: Iterable[str] | None = None,
    *,
    env_overrides: dict[str, str] | None = None,
    cwd: str | Path | None = None,
) -> None:
    _run_cmd(
        [sys.executable, "-m", module, *_ensure_list(args)],
        env_overrides=env_overrides,
        cwd=cwd,
    )


def _inject_config_arg(args: Iterable[str], config_path: str | None) -> list[str]:
    arg_list = _ensure_list(args)
    if not config_path:
        return arg_list
    for arg in arg_list:
        if arg == "--config" or arg.startswith("--config="):
            return arg_list
    return ["--config", config_path, *arg_list]


def _extract_arg_value(args: Sequence[str], name: str) -> str | None:
    flag = f"--{name}"
    for idx, arg in enumerate(args):
        if arg == flag and idx + 1 < len(args):
            return args[idx + 1]
        if arg.startswith(f"{flag}="):
            return arg.split("=", 1)[1]
    return None


def _clear_dir(path: Path) -> None:
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


@click.group(
    context_settings={"help_option_names": ["-h", "--help"]},
    help="E-commerce data lake CLI. Use action-style commands like `dim run-strict`.",
    epilog=textwrap.dedent(
        """\
        Common examples
        ---------------

          Bronze profiling:
            ecomlake bronze profile --date-range 2025-10-01..2025-10-01

          Bronze validation (strict):
            ecomlake bronze validate --partition-date 2024-01-03 --enforce-quality

          Dims (strict):
            ecomlake dim run-strict --run-date 2024-01-03

          Silver run + validate:
            ecomlake silver run --select base_silver.*
            ecomlake silver validate --partition-date 2024-01-03

          Enriched run:
            ecomlake enriched run --ingest-dt 2024-01-03

          Pipeline (native GCS):
            ecomlake pipeline dev-gcs 2025-10-04

          Airflow:
            ecomlake airflow up

          Local demo:
            ecomlake local demo
        """
    ),
    invoke_without_command=True,
)
@click.option(
    "--config",
    "config_path",
    default=None,
    envvar="ECOM_CONFIG_PATH",
    help="Path to config.yml (defaults to config/config.yml).",
)
@click.pass_context
def cli(ctx: click.Context, config_path: str | None) -> None:
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path or "config/config.yml"
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@cli.command(help="Show pipeline version information.")
def version() -> None:
    _run_python_script("scripts/version.py")


@cli.group(
    help="Bronze layer operations (profile + validate).",
    epilog=textwrap.dedent(
        """\
        Examples
        --------

          Profile:
            ecomlake bronze profile --months 2025-10

          Validate:
            ecomlake bronze validate --partition-date 2024-01-03

          Validate samples:
            ecomlake bronze validate-samples --date 2024-01-03
        """
    ),
)
def bronze() -> None:
    pass


@bronze.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    help="Profile Bronze parquet samples (self-documenting artifacts).",
)
@click.pass_context
def profile(ctx: click.Context) -> None:
    _run_python_script("scripts/describe_parquet_samples.py", ctx.args)


@bronze.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    help="Validate Bronze inputs (manifest, schema, partitions).",
)
@click.pass_context
def validate(ctx: click.Context) -> None:
    args = _inject_config_arg(ctx.args, ctx.obj.get("config_path"))
    _run_python_module("src.validation.bronze_quality", args)


@bronze.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    name="validate-samples",
    help="Quick Bronze validation against local samples.",
)
@click.pass_context
def validate_samples(ctx: click.Context) -> None:
    _run_python_script("scripts/validate_bronze_samples.py", ctx.args)


@cli.group(
    help="Dimension snapshot operations.",
    epilog=textwrap.dedent(
        """\
        Examples
        --------

          Run:
            ecomlake dim run --run-date 2024-01-03

          Run (strict):
            ecomlake dim run-strict --run-date 2024-01-03

          Validate:
            ecomlake dim validate --run-date 2024-01-03 --enforce-quality
        """
    ),
)
def dim() -> None:
    pass


@dim.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    name="run",
    help="Run dimension snapshots from spec.",
)
@click.pass_context
def dim_run(ctx: click.Context) -> None:
    _run_python_script("scripts/run_dims_from_spec.py", ctx.args)


@dim.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    name="validate",
    help="Validate dimension snapshots.",
)
@click.pass_context
def dim_validate(ctx: click.Context) -> None:
    args = _inject_config_arg(ctx.args, ctx.obj.get("config_path"))
    _run_python_module("src.validation.dims_snapshot", args)


@dim.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    name="run-strict",
    help="Run dims, then validate with --enforce-quality.",
)
@click.pass_context
def dim_run_strict(ctx: click.Context) -> None:
    run_args = _ensure_list(ctx.args)
    _run_python_script("scripts/run_dims_from_spec.py", run_args)
    validate_args = _inject_config_arg([], ctx.obj.get("config_path"))
    if "--enforce-quality" not in validate_args:
        validate_args.append("--enforce-quality")
    run_date = _extract_arg_value(run_args, "run-date")
    if run_date and "--run-date" not in validate_args:
        validate_args.extend(["--run-date", run_date])
    _run_python_module("src.validation.dims_snapshot", validate_args)


@cli.group(
    help="Base Silver operations (dbt-duckdb + validation).",
    epilog=textwrap.dedent(
        """\
        Examples
        --------

          Run:
            ecomlake silver run --select base_silver.*

          Validate:
            ecomlake silver validate --partition-date 2024-01-03

          Run (strict):
            ecomlake silver run-strict --select base_silver.*
        """
    ),
)
def silver() -> None:
    pass


@silver.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    name="run",
    help="Run Base Silver transformations (dbt-duckdb runner).",
)
@click.pass_context
def silver_run(ctx: click.Context) -> None:
    _run_python_module("src.runners.base_silver", ctx.args)


@silver.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    name="validate",
    help="Validate Base Silver outputs.",
)
@click.pass_context
def silver_validate(ctx: click.Context) -> None:
    args = _inject_config_arg(ctx.args, ctx.obj.get("config_path"))
    _run_python_module("src.validation.silver", args)


@silver.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    name="run-strict",
    help="Run Base Silver then validate with --enforce-quality.",
)
@click.pass_context
def silver_run_strict(ctx: click.Context) -> None:
    run_args = _ensure_list(ctx.args)
    _run_python_module("src.runners.base_silver", run_args)
    validate_args = _inject_config_arg([], ctx.obj.get("config_path"))
    if "--enforce-quality" not in validate_args:
        validate_args.append("--enforce-quality")
    _run_python_module("src.validation.silver", validate_args)


@cli.group(
    help="Enriched Silver operations.",
    epilog=textwrap.dedent(
        """\
        Examples
        --------

          Run:
            ecomlake enriched run --ingest-dt 2024-01-03

          Run (strict):
            ecomlake enriched run-strict --ingest-dt 2024-01-03

          Validate:
            ecomlake enriched validate --ingest-dt 2024-01-03
        """
    ),
)
def enriched() -> None:
    pass


@enriched.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    name="run",
    help="Run Enriched Silver transforms.",
)
@click.pass_context
def enriched_run(ctx: click.Context) -> None:
    _run_python_script("scripts/run_enriched_all_samples.py", ctx.args)


@enriched.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    name="validate",
    help="Validate Enriched Silver outputs.",
)
@click.pass_context
def enriched_validate(ctx: click.Context) -> None:
    args = _inject_config_arg(ctx.args, ctx.obj.get("config_path"))
    _run_python_module("src.validation.enriched", args)


@enriched.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    name="run-strict",
    help="Run Enriched Silver then validate with --enforce-quality.",
)
@click.pass_context
def enriched_run_strict(ctx: click.Context) -> None:
    run_args = _ensure_list(ctx.args)
    if "--enforce-quality" not in run_args:
        run_args.append("--enforce-quality")
    _run_python_script("scripts/run_enriched_all_samples.py", run_args)
    validate_args = _inject_config_arg([], ctx.obj.get("config_path"))
    if "--enforce-quality" not in validate_args:
        validate_args.append("--enforce-quality")
    ingest_dt = _extract_arg_value(run_args, "ingest-dt")
    if ingest_dt and "--ingest-dt" not in validate_args:
        validate_args.extend(["--ingest-dt", ingest_dt])
    _run_python_module("src.validation.enriched", validate_args)


@cli.group(
    name="sample",
    help="Sample data operations (pull/package).",
    epilog=textwrap.dedent(
        """\
        Examples
        --------

          Pull:
            ecomlake sample pull 2020-01

          Package:
            ecomlake sample package --months 2020-01,2020-02
        """
    ),
)
def sample_group() -> None:
    pass


@sample_group.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    name="pull",
    help="Pull Bronze samples from GCS.",
)
@click.pass_context
def sample_pull(ctx: click.Context) -> None:
    _run_cmd(["./scripts/pull_bronze_sample.sh", *_ensure_list(ctx.args)])


@sample_group.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    name="package",
    help="Package Bronze samples for demo distribution.",
)
@click.pass_context
def sample_package(ctx: click.Context) -> None:
    _run_python_script("scripts/package_bronze_sample.py", ctx.args)


@cli.group(
    help="Storage reporting tools.",
    epilog=textwrap.dedent(
        """\
        Examples
        --------

          Default report:
            ecomlake bucket report

          Custom report:
            ecomlake bucket report gcs-automation-project-raw ecom/raw \\
              docs/data/BRONZE_SIZES.md
        """
    ),
)
def bucket() -> None:
    pass


@bucket.command(
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
    name="report",
    help="Report Bronze bucket sizes.",
)
@click.pass_context
def bucket_report(ctx: click.Context) -> None:
    _run_cmd(["./scripts/report_bronze_sizes.sh", *_ensure_list(ctx.args)])


@cli.group(
    help="Airflow lifecycle and DAG controls.",
    epilog=textwrap.dedent(
        """\
        Examples
        --------

          Start:
            ecomlake airflow up

          Tail a task log:
            ecomlake airflow log-task --dag ecom_silver_to_gold_pipeline \\
              --run-id manual_20260123 --task validate_bronze

          Strict mode:
            ecomlake airflow strict-mode
        """
    ),
)
def airflow() -> None:
    pass


@airflow.command(name="bootstrap", help="Initialize Airflow with Docker Compose.")
def airflow_bootstrap() -> None:
    _run_cmd(["./scripts/bootstrap_airflow.sh"])


@airflow.command(name="up", help="Start Airflow services.")
def airflow_up() -> None:
    base = _compose_base_cmd()
    _run_cmd([*base, "up", "airflow-init"])
    _run_cmd([*base, "up", "-d"])


@airflow.command(name="down", help="Stop Airflow services.")
def airflow_down() -> None:
    _run_cmd([*_compose_base_cmd(), "down"])


@airflow.command(name="restart", help="Restart scheduler + webserver.")
def airflow_restart() -> None:
    _run_cmd(
        [
            *_compose_base_cmd(),
            "restart",
            "airflow-scheduler",
            "airflow-webserver",
        ]
    )


@airflow.command(name="logs", help="Tail scheduler logs.")
def airflow_logs() -> None:
    _run_cmd([*_compose_base_cmd(), "logs", "-f", "airflow-scheduler"])


@airflow.command(name="log-task", help="Tail a specific task log.")
@click.option("--dag", required=True, help="Airflow DAG id.")
@click.option("--run-id", required=True, help="Airflow run id.")
@click.option("--task", "task_id", required=True, help="Airflow task id.")
@click.option("--try", "try_num", default=1, show_default=True, type=int)
@click.option("--lines", default=200, show_default=True, type=int)
def airflow_log_task(
    dag: str, run_id: str, task_id: str, try_num: int, lines: int
) -> None:
    log_path = (
        f"/opt/airflow/logs/dag_id={dag}/run_id={run_id}/task_id={task_id}/"
        f"attempt={try_num}.log"
    )
    _run_cmd(
        [
            *_compose_base_cmd(),
            "exec",
            "airflow-scheduler",
            "tail",
            "-n",
            str(lines),
            log_path,
        ]
    )


@airflow.command(name="shell", help="Open a bash shell in the scheduler container.")
def airflow_shell() -> None:
    _run_cmd([*_compose_base_cmd(), "exec", "airflow-scheduler", "bash"])


@airflow.command(name="clean-data", help="Remove local data/metrics outputs.")
def airflow_clean_data() -> None:
    for path in (
        Path("data/silver/base"),
        Path("data/silver/dims"),
        Path("data/silver/enriched"),
        Path("data/metrics"),
    ):
        _clear_dir(path)
    click.echo("Local data cleared.")


@airflow.command(
    name="clean",
    help="Destroy containers, volumes, image, and local data.",
)
def airflow_clean() -> None:
    if not click.confirm(
        "This will remove containers, volumes, images, and local data. Continue?",
        default=False,
    ):
        raise SystemExit(1)
    _run_cmd([*_compose_base_cmd(), "down", "-v"])
    _run_cmd(["docker", "rmi", "ecom-datalake-pipeline:latest"], check=False)
    airflow_clean_data()


def _airflow_mode_env(
    *,
    pipeline_env: str,
    observability_env: str,
    bronze_qa_fail: str | None,
    bq_load_enabled: str,
    gold_pipeline_enabled: str,
) -> dict[str, str]:
    env = {
        "PIPELINE_ENV": pipeline_env,
        "OBSERVABILITY_ENV": observability_env,
        "BQ_LOAD_ENABLED": bq_load_enabled,
        "GOLD_PIPELINE_ENABLED": gold_pipeline_enabled,
    }
    if bronze_qa_fail is not None:
        env["BRONZE_QA_FAIL"] = bronze_qa_fail
    return env


@airflow.command(name="strict-mode", help="Restart Airflow in strict (prod) mode.")
def airflow_strict_mode() -> None:
    env = _airflow_mode_env(
        pipeline_env="prod",
        observability_env="local",
        bronze_qa_fail="true",
        bq_load_enabled="false",
        gold_pipeline_enabled="false",
    )
    _run_cmd(
        [
            *_compose_base_cmd(),
            "up",
            "-d",
            "--force-recreate",
            "airflow-scheduler",
            "airflow-webserver",
        ],
        env_overrides=env,
    )


@airflow.command(name="easy-mode", help="Restart Airflow in easy (local) mode.")
def airflow_easy_mode() -> None:
    env = _airflow_mode_env(
        pipeline_env="local",
        observability_env="local",
        bronze_qa_fail="false",
        bq_load_enabled="false",
        gold_pipeline_enabled="false",
    )
    _run_cmd(
        [
            *_compose_base_cmd(),
            "up",
            "-d",
            "--force-recreate",
            "airflow-scheduler",
            "airflow-webserver",
        ],
        env_overrides=env,
    )


@airflow.command(
    name="dev-mode",
    help="Start Airflow in dev mode (GCS buckets, no BQ).",
)
def airflow_dev_mode() -> None:
    env = _airflow_mode_env(
        pipeline_env="dev",
        observability_env="dev",
        bronze_qa_fail=None,
        bq_load_enabled="false",
        gold_pipeline_enabled="false",
    )
    _run_cmd(
        [
            "docker",
            "compose",
            "--env-file",
            "docker.env",
            "up",
            "-d",
            "--force-recreate",
            "airflow-scheduler",
            "airflow-webserver",
        ],
        env_overrides=env,
    )


@airflow.command(
    name="prod-sim-mode",
    help="Start Airflow in prod-sim mode (GCS buckets, no BQ).",
)
def airflow_prod_sim_mode() -> None:
    env = _airflow_mode_env(
        pipeline_env="prod",
        observability_env="prod",
        bronze_qa_fail=None,
        bq_load_enabled="false",
        gold_pipeline_enabled="false",
    )
    _run_cmd(
        [
            "docker",
            "compose",
            "--env-file",
            "docker.env",
            "up",
            "-d",
            "--force-recreate",
            "airflow-scheduler",
            "airflow-webserver",
        ],
        env_overrides=env,
    )


@cli.group(
    help="Pipeline execution helpers.",
    epilog=textwrap.dedent(
        """\
        Examples
        --------

          Trigger sample:
            ecomlake pipeline run-sample --date 2024-01-03

          Backfill (easy):
            ecomlake pipeline backfill-easy --start 2025-10-01 --end 2025-10-31

          Docker (dev):
            ecomlake pipeline dev-docker 2025-10-04
        """
    ),
)
def pipeline() -> None:
    pass


@pipeline.command(name="run-sample", help="Trigger main DAG (soft mode).")
@click.option("--date", required=True, help="Execution date (YYYY-MM-DD).")
@click.option("--run-id", default=None, help="Optional Airflow run id.")
def pipeline_run_sample(date: str, run_id: str | None) -> None:
    cmd = [
        *_compose_base_cmd(),
        "exec",
        "airflow-scheduler",
        "airflow",
        "dags",
        "trigger",
        "ecom_silver_to_gold_pipeline",
        "--exec-date",
        date,
    ]
    if run_id:
        cmd.extend(["--run-id", run_id])
    _run_cmd(cmd)


@pipeline.command(name="run-sample-strict", help="Strict mode + trigger main DAG.")
@click.option("--date", required=True, help="Execution date (YYYY-MM-DD).")
@click.option("--run-id", default=None, help="Optional Airflow run id.")
def pipeline_run_sample_strict(date: str, run_id: str | None) -> None:
    airflow_strict_mode.callback()
    pipeline_run_sample.callback(date=date, run_id=run_id)


@pipeline.command(
    name="run-sample-bq",
    help="Strict mode + trigger main DAG with BQ load.",
)
@click.option("--date", required=True, help="Execution date (YYYY-MM-DD).")
def pipeline_run_sample_bq(date: str) -> None:
    env = _airflow_mode_env(
        pipeline_env="prod",
        observability_env="local",
        bronze_qa_fail="true",
        bq_load_enabled="true",
        gold_pipeline_enabled="false",
    )
    _run_cmd(
        [
            *_compose_base_cmd(),
            "up",
            "-d",
            "--force-recreate",
            "airflow-scheduler",
            "airflow-webserver",
        ],
        env_overrides=env,
    )
    _run_cmd(
        [
            *_compose_base_cmd(),
            "exec",
            "airflow-scheduler",
            "airflow",
            "dags",
            "trigger",
            "ecom_silver_to_gold_pipeline",
            "--exec-date",
            date,
        ]
    )


@pipeline.command(name="run-dims", help="Trigger dimension refresh DAG.")
@click.option("--date", required=True, help="Execution date (YYYY-MM-DD).")
@click.option("--run-id", default=None, help="Optional Airflow run id.")
def pipeline_run_dims(date: str, run_id: str | None) -> None:
    cmd = [
        *_compose_base_cmd(),
        "exec",
        "airflow-scheduler",
        "airflow",
        "dags",
        "trigger",
        "ecom_dim_refresh_pipeline",
        "--exec-date",
        date,
    ]
    if run_id:
        cmd.extend(["--run-id", run_id])
    _run_cmd(cmd)


@pipeline.command(name="backfill-dims", help="Backfill dimension DAG.")
@click.option("--start", required=True, help="Start date (YYYY-MM-DD).")
@click.option("--end", required=True, help="End date (YYYY-MM-DD).")
def pipeline_backfill_dims(start: str, end: str) -> None:
    _run_cmd(
        [
            *_compose_base_cmd(),
            "exec",
            "airflow-scheduler",
            "airflow",
            "dags",
            "backfill",
            "ecom_dim_refresh_pipeline",
            "-s",
            start,
            "-e",
            end,
        ]
    )


@pipeline.command(name="backfill-easy", help="Easy mode + backfill main DAG.")
@click.option("--start", required=True, help="Start date (YYYY-MM-DD).")
@click.option("--end", required=True, help="End date (YYYY-MM-DD).")
def pipeline_backfill_easy(start: str, end: str) -> None:
    airflow_easy_mode.callback()
    _run_cmd(
        [
            *_compose_base_cmd(),
            "exec",
            "airflow-scheduler",
            "airflow",
            "dags",
            "backfill",
            "ecom_silver_to_gold_pipeline",
            "-s",
            start,
            "-e",
            end,
        ]
    )


@pipeline.command(name="backfill-strict", help="Strict mode + backfill main DAG.")
@click.option("--start", required=True, help="Start date (YYYY-MM-DD).")
@click.option("--end", required=True, help="End date (YYYY-MM-DD).")
def pipeline_backfill_strict(start: str, end: str) -> None:
    airflow_strict_mode.callback()
    _run_cmd(
        [
            *_compose_base_cmd(),
            "exec",
            "airflow-scheduler",
            "airflow",
            "dags",
            "backfill",
            "ecom_silver_to_gold_pipeline",
            "-s",
            start,
            "-e",
            end,
        ]
    )


@pipeline.command(name="dev-gcs", help="Run pipeline against GCS (native, no Docker).")
@click.argument("date")
def pipeline_dev_gcs(date: str) -> None:
    _run_cmd(["./scripts/run_dev_pipeline.sh", date])


@pipeline.command(
    name="sim-prod-gcs",
    help="Simulate prod pipeline against GCS (native).",
)
@click.argument("date")
def pipeline_sim_prod_gcs(date: str) -> None:
    _run_cmd(["./scripts/run_sim_prod_gcs.sh", date])


@pipeline.command(name="dev-docker", help="Start dev Airflow + trigger main DAG.")
@click.argument("date")
def pipeline_dev_docker(date: str) -> None:
    airflow_dev_mode.callback()
    time.sleep(5)
    pipeline_run_sample.callback(date=date, run_id=None)


@pipeline.command(
    name="prod-sim-docker",
    help="Start prod-sim Airflow + trigger main DAG.",
)
@click.argument("date")
def pipeline_prod_sim_docker(date: str) -> None:
    airflow_prod_sim_mode.callback()
    time.sleep(5)
    pipeline_run_sample.callback(date=date, run_id=None)


@cli.group(
    help="dbt helpers.",
    epilog=textwrap.dedent(
        """\
        Examples
        --------

          ecomlake dbt deps
          ecomlake dbt build
          ecomlake dbt test
        """
    ),
)
def dbt() -> None:
    pass


@dbt.command(name="deps", help="Install dbt packages.")
def dbt_deps() -> None:
    _run_cmd(
        ["dbt", "deps", "--project-dir", ".", "--profiles-dir", "."],
        cwd="dbt_duckdb",
        env_overrides=_dbt_env(),
    )


@dbt.command(name="build", help="Build all Base Silver models.")
def dbt_build() -> None:
    _run_cmd(
        ["dbt", "build", "--project-dir", ".", "--profiles-dir", "."],
        cwd="dbt_duckdb",
        env_overrides=_dbt_env(),
    )


@dbt.command(name="test", help="Run dbt data tests.")
def dbt_test() -> None:
    _run_cmd(
        ["dbt", "test", "--project-dir", ".", "--profiles-dir", "."],
        cwd="dbt_duckdb",
        env_overrides=_dbt_env(),
    )


@cli.group(
    help="Developer workflows.",
    epilog=textwrap.dedent(
        """\
        Examples
        --------

          ecomlake dev test
          ecomlake dev lint
          ecomlake dev format
        """
    ),
)
def dev() -> None:
    pass


@dev.command(name="test", help="Run unit tests.")
def dev_test() -> None:
    _run_cmd(["pytest", "tests/unit/", "-v"])


@dev.command(name="lint", help="Run ruff + yamllint.")
def dev_lint() -> None:
    _run_cmd(["ruff", "check", "src/", "tests/", "airflow/"])
    _run_cmd(["yamllint", "."])


@dev.command(name="format", help="Run black + isort.")
def dev_format() -> None:
    _run_cmd(["black", "src/", "tests/", "airflow/"])
    _run_cmd(["isort", "src/", "tests/", "airflow/"])


@dev.command(name="type-check", help="Run mypy.")
def dev_type_check() -> None:
    _run_cmd(["mypy", "src/"])


@cli.group(
    help="Local (no Docker) pipeline helpers.",
    epilog=textwrap.dedent(
        """\
        Examples
        --------

          Silver:
            ecomlake local silver --date 2024-01-03

          Dims:
            ecomlake local dims --date 2024-01-03

          Demo:
            ecomlake local demo
        """
    ),
)
def local() -> None:
    pass


def _local_env() -> dict[str, str]:
    cwd = Path.cwd()
    return {
        "PIPELINE_ENV": "local",
        "BRONZE_BASE_PATH": str(cwd / "samples/bronze"),
        "SILVER_BASE_PATH": str(cwd / "data/silver/base"),
        "SILVER_DIMS_PATH": str(cwd / "data/silver/dims"),
    }


def _local_metrics_env() -> dict[str, str]:
    return {
        "LOGS_BASE_PATH": "/tmp/ecom_logs",
        "METRICS_BASE_PATH": "/tmp/ecom_metrics",
    }


def _dbt_env() -> dict[str, str]:
    return {
        "DBT_LOG_PATH": "/tmp/dbt_logs",
        "DBT_TARGET_PATH": "/tmp/dbt_target",
    }


def _run_dbt_deps_locked() -> None:
    lock_path = Path("/tmp/ecomlake_dbt_deps.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w", encoding="utf-8") as lock_file:
        import fcntl

        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            packages_dir = Path("dbt_duckdb") / "dbt_packages"
            if packages_dir.exists():
                for entry in packages_dir.iterdir():
                    if (
                        entry.is_dir()
                        and entry.name.startswith("dbt_utils ")
                        and entry.name[len("dbt_utils ") :].isdigit()
                    ):
                        shutil.rmtree(entry, ignore_errors=True)

            for entry in Path("dbt_duckdb").iterdir():
                if entry.is_dir() and entry.name.startswith(
                    "{{ env_var('DBT_LOG_PATH'"
                ):
                    shutil.rmtree(entry, ignore_errors=True)

            _run_cmd(
                ["dbt", "deps", "--project-dir", ".", "--profiles-dir", "."],
                cwd="dbt_duckdb",
                env_overrides={**_local_env(), **_dbt_env()},
            )
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


@local.command(name="silver", help="Run local Base Silver + validation.")
@click.option("--date", default=None, help="Optional run date (YYYY-MM-DD).")
def local_silver(date: str | None) -> None:
    env = {**_local_env()}
    base_cmd = [
        sys.executable,
        "-m",
        "src.runners.base_silver",
        "--select",
        "base_silver.*",
    ]
    if date:
        base_cmd.extend(
            [
                "--vars",
                f'{{"run_date": "{date}", "lookback_days": 0}}',
                "--exclude",
                "stg_ecommerce__customers",
                "stg_ecommerce__customers_quarantine",
                "stg_ecommerce__product_catalog",
                "stg_ecommerce__product_catalog_quarantine",
            ]
        )
    _run_cmd(base_cmd, env_overrides=env)

    validation_cmd = [
        sys.executable,
        "-m",
        "src.validation.silver",
        "--bronze-path",
        "samples/bronze",
        "--silver-path",
        "data/silver/base",
        "--quarantine-path",
        "data/silver/base/quarantine",
        "--output-report",
        "docs/validation_reports/SILVER_QUALITY_FULL.md",
    ]
    if date:
        validation_cmd.extend(
            [
                "--partition-date",
                date,
                "--tables",
                "orders,order_items,shopping_carts,cart_items,returns,return_items",
            ]
        )
    _run_cmd(validation_cmd)


@local.command(name="silver-strict", help="Run local Base Silver + strict validation.")
@click.option("--date", default=None, help="Optional run date (YYYY-MM-DD).")
def local_silver_strict(date: str | None) -> None:
    env = {**_local_env()}
    base_cmd = [
        sys.executable,
        "-m",
        "src.runners.base_silver",
        "--select",
        "base_silver.*",
    ]
    if date:
        base_cmd.extend(
            [
                "--vars",
                f'{{"run_date": "{date}", "lookback_days": 0}}',
                "--exclude",
                "stg_ecommerce__customers",
                "stg_ecommerce__customers_quarantine",
                "stg_ecommerce__product_catalog",
                "stg_ecommerce__product_catalog_quarantine",
            ]
        )
    _run_cmd(base_cmd, env_overrides=env)

    validation_cmd = [
        sys.executable,
        "-m",
        "src.validation.silver",
        "--bronze-path",
        "samples/bronze",
        "--silver-path",
        "data/silver/base",
        "--quarantine-path",
        "data/silver/base/quarantine",
        "--output-report",
        "docs/validation_reports/SILVER_QUALITY_FULL.md",
        "--enforce-quality",
    ]
    if date:
        validation_cmd.extend(
            [
                "--partition-date",
                date,
                "--tables",
                "orders,order_items,shopping_carts,cart_items,returns,return_items",
            ]
        )
    _run_cmd(validation_cmd)


@local.command(name="dims", help="Run local dims + validation.")
@click.option(
    "--date",
    "run_date",
    default=None,
    help="Optional run date (YYYY-MM-DD).",
)
def local_dims(run_date: str | None) -> None:
    env = {**_local_env(), **_local_metrics_env()}
    cmd = [sys.executable, "scripts/run_dims_from_spec.py"]
    if run_date:
        cmd.extend(["--run-date", run_date])
    _run_cmd(cmd, env_overrides=env)

    validation_cmd = [
        sys.executable,
        "-m",
        "src.validation.silver",
        "--bronze-path",
        "samples/bronze",
        "--silver-path",
        "data/silver/base",
        "--quarantine-path",
        "data/silver/base/quarantine",
        "--output-report",
        "docs/validation_reports/SILVER_QUALITY.md",
        "--tables",
        "customers,product_catalog",
    ]
    _run_cmd(validation_cmd)


@local.command(name="dims-strict", help="Run local dims + strict validation.")
@click.option(
    "--date",
    "run_date",
    default=None,
    help="Optional run date (YYYY-MM-DD).",
)
def local_dims_strict(run_date: str | None) -> None:
    env = {**_local_env()}
    cmd = [sys.executable, "scripts/run_dims_from_spec.py"]
    if run_date:
        cmd.extend(["--run-date", run_date])
    _run_cmd(cmd, env_overrides=env)

    validation_cmd = [
        sys.executable,
        "-m",
        "src.validation.silver",
        "--bronze-path",
        "samples/bronze",
        "--silver-path",
        "data/silver/base",
        "--quarantine-path",
        "data/silver/base/quarantine",
        "--output-report",
        "docs/validation_reports/SILVER_QUALITY.md",
        "--tables",
        "customers,product_catalog",
        "--enforce-quality",
    ]
    _run_cmd(validation_cmd)


@local.command(name="enriched", help="Run local Enriched transforms.")
@click.option(
    "--date",
    "ingest_dt",
    default=None,
    help="Optional ingest date (YYYY-MM-DD).",
)
def local_enriched(ingest_dt: str | None) -> None:
    cwd = Path.cwd()
    env = {
        **_local_env(),
        "SILVER_ENRICHED_PATH": str(cwd / "data/silver/enriched"),
        **_local_metrics_env(),
    }
    cmd = [
        sys.executable,
        "scripts/run_enriched_all_samples.py",
        "--base-path",
        str(cwd / "data/silver/base"),
        "--output-path",
        str(cwd / "data/silver/enriched"),
        "--per-date",
    ]
    if ingest_dt:
        cmd.extend(["--ingest-dt", ingest_dt])
    _run_cmd(cmd, env_overrides=env)


@local.command(
    name="enriched-strict",
    help="Run local Enriched transforms with strict checks.",
)
@click.option(
    "--date",
    "ingest_dt",
    default=None,
    help="Optional ingest date (YYYY-MM-DD).",
)
def local_enriched_strict(ingest_dt: str | None) -> None:
    cwd = Path.cwd()
    env = {
        **_local_env(),
        "SILVER_ENRICHED_PATH": str(cwd / "data/silver/enriched"),
    }
    cmd = [
        sys.executable,
        "scripts/run_enriched_all_samples.py",
        "--base-path",
        str(cwd / "data/silver/base"),
        "--output-path",
        str(cwd / "data/silver/enriched"),
        "--per-date",
        "--enforce-quality",
    ]
    if ingest_dt:
        cmd.extend(["--ingest-dt", ingest_dt])
    _run_cmd(cmd, env_overrides=env)


_DEMO_DEFAULT_DATE = "2020-01-05"
_DEMO_DEFAULT_END_DATE = "2020-01-05"
_DEMO_DEFAULT_LOOKBACK = 4
_DEMO_DEFAULT_DATES = "2020-01-01 2020-01-02 2020-01-03 2020-01-04 2020-01-05"


@local.command(
    name="demo",
    help="Run local end-to-end demo (dims + silver + enriched).",
)
@click.option("--date", "demo_date", default=_DEMO_DEFAULT_DATE, show_default=True)
@click.option(
    "--end-date",
    "demo_end_date",
    default=_DEMO_DEFAULT_END_DATE,
    show_default=True,
)
@click.option("--lookback", default=_DEMO_DEFAULT_LOOKBACK, show_default=True, type=int)
@click.option(
    "--dates",
    default=_DEMO_DEFAULT_DATES,
    show_default=True,
    help="Space-separated dates for the dims loop.",
)
def local_demo(demo_date: str, demo_end_date: str, lookback: int, dates: str) -> None:
    _run_dbt_deps_locked()

    env = {**_local_env(), **_local_metrics_env()}
    for day in dates.split():
        _run_cmd(
            [sys.executable, "scripts/run_dims_from_spec.py", "--run-date", day],
            env_overrides=env,
        )
        _run_cmd(
            [
                sys.executable,
                "-m",
                "src.validation.dims_snapshot",
                "--run-date",
                day,
                "--run-id",
                f"local_demo_{day}",
            ],
            env_overrides=env,
        )

    _run_cmd(
        [
            sys.executable,
            "-m",
            "src.runners.base_silver",
            "--select",
            "base_silver.*",
            "--vars",
            f"{{run_date: '{demo_end_date}', lookback_days: {lookback}}}",
        ],
        env_overrides={
            **env,
            "DBT_LOG_PATH": "/tmp/dbt_logs",
            "DBT_TARGET_PATH": "/tmp/dbt_target",
        },
    )

    _run_cmd(
        [
            sys.executable,
            "-m",
            "src.validation.silver",
            "--bronze-path",
            "samples/bronze",
            "--silver-path",
            "data/silver/base",
            "--quarantine-path",
            "data/silver/base/quarantine",
            "--partition-date",
            demo_end_date,
            "--lookback-days",
            str(lookback),
            "--tables",
            "orders,order_items,shopping_carts,cart_items,returns,return_items",
            "--output-report",
            "docs/validation_reports/SILVER_QUALITY_FULL.md",
        ],
        env_overrides=env,
    )

    _run_cmd(
        ["dbt", "test", "--project-dir", ".", "--profiles-dir", "."],
        cwd="dbt_duckdb",
        env_overrides={**_local_env(), **_dbt_env()},
    )

    local_enriched.callback(ingest_dt=demo_end_date)


@local.command(name="demo-fast", help="Run fast local demo (single-day pipeline).")
@click.option("--date", "demo_date", default="2020-01-05", show_default=True)
def local_demo_fast(demo_date: str) -> None:
    _run_dbt_deps_locked()

    env = {**_local_env(), **_local_metrics_env()}
    Path("data/silver/dims").mkdir(parents=True, exist_ok=True)
    local_dims.callback(run_date=demo_date)

    _run_cmd(
        [
            sys.executable,
            "-m",
            "src.runners.base_silver",
            "--select",
            "base_silver.*",
            "--vars",
            f"{{run_date: '{demo_date}', lookback_days: 0}}",
        ],
        env_overrides={
            **env,
            "DBT_LOG_PATH": "/tmp/dbt_logs",
            "DBT_TARGET_PATH": "/tmp/dbt_target",
        },
    )

    _run_cmd(
        [
            sys.executable,
            "-m",
            "src.validation.silver",
            "--bronze-path",
            "samples/bronze",
            "--silver-path",
            "data/silver/base",
            "--quarantine-path",
            "data/silver/base/quarantine",
            "--partition-date",
            demo_date,
            "--lookback-days",
            "0",
            "--tables",
            "orders,order_items,shopping_carts,cart_items,returns,return_items",
            "--output-report",
            "docs/validation_reports/SILVER_QUALITY_FULL.md",
        ],
        env_overrides=env,
    )

    _run_cmd(
        ["dbt", "test", "--project-dir", ".", "--profiles-dir", "."],
        cwd="dbt_duckdb",
        env_overrides={**_local_env(), **_dbt_env()},
    )
    local_enriched.callback(ingest_dt=demo_date)


@local.command(name="demo-full", help="Alias for demo.")
def local_demo_full() -> None:
    local_demo.callback(
        demo_date=_DEMO_DEFAULT_DATE,
        demo_end_date=_DEMO_DEFAULT_END_DATE,
        lookback=_DEMO_DEFAULT_LOOKBACK,
        dates=_DEMO_DEFAULT_DATES,
    )


@cli.group(
    help="Deployment helpers.",
    epilog=textwrap.dedent(
        """\
        Examples
        --------

          ecomlake deploy build-versioned
          ecomlake deploy push-image --project-id my-project-123
        """
    ),
)
def deploy() -> None:
    pass


def _git_output(args: Sequence[str]) -> str:
    result = subprocess.run(
        ["git", *args], stdout=subprocess.PIPE, text=True, check=True
    )
    return result.stdout.strip()


@deploy.command(
    name="build-versioned",
    help="Build Docker image with Git version tags.",
)
def deploy_build_versioned() -> None:
    git_commit = _git_output(["rev-parse", "--short", "HEAD"])
    git_branch = _git_output(["rev-parse", "--abbrev-ref", "HEAD"])
    try:
        git_tag = _git_output(["describe", "--tags", "--exact-match"])
    except subprocess.CalledProcessError:
        git_tag = ""
    version = git_tag or f"{git_branch}-{git_commit}"

    _run_cmd(
        [
            "docker",
            "build",
            "--build-arg",
            f"GIT_COMMIT={git_commit}",
            "--build-arg",
            f"GIT_BRANCH={git_branch}",
            "--build-arg",
            f"VERSION={version}",
            "-t",
            f"ecom-datalake-pipeline:{version}",
            "-t",
            "ecom-datalake-pipeline:latest",
            ".",
        ]
    )


@deploy.command(name="push-image", help="Build + push Docker image (latest).")
@click.option("--project-id", required=True, help="GCP project id.")
def deploy_push_image(project_id: str) -> None:
    _run_cmd(["docker", "build", "-t", "ecom-datalake-pipeline:latest", "."])
    _run_cmd(
        [
            "docker",
            "tag",
            "ecom-datalake-pipeline:latest",
            f"us-central1-docker.pkg.dev/{project_id}/airflow-images/ecom-datalake-pipeline:latest",
        ]
    )
    _run_cmd(["gcloud", "auth", "configure-docker", "us-central1-docker.pkg.dev"])
    _run_cmd(
        [
            "docker",
            "push",
            f"us-central1-docker.pkg.dev/{project_id}/airflow-images/ecom-datalake-pipeline:latest",
        ]
    )


@deploy.command(
    name="push-image-versioned",
    help="Build + push Docker image with version tags.",
)
@click.option("--project-id", required=True, help="GCP project id.")
def deploy_push_image_versioned(project_id: str) -> None:
    git_commit = _git_output(["rev-parse", "--short", "HEAD"])
    git_branch = _git_output(["rev-parse", "--abbrev-ref", "HEAD"])
    try:
        git_tag = _git_output(["describe", "--tags", "--exact-match"])
    except subprocess.CalledProcessError:
        git_tag = ""
    version = git_tag or f"{git_branch}-{git_commit}"

    deploy_build_versioned.callback()
    _run_cmd(
        [
            "docker",
            "tag",
            f"ecom-datalake-pipeline:{version}",
            f"us-central1-docker.pkg.dev/{project_id}/airflow-images/ecom-datalake-pipeline:{version}",
        ]
    )
    _run_cmd(
        [
            "docker",
            "tag",
            f"ecom-datalake-pipeline:{version}",
            f"us-central1-docker.pkg.dev/{project_id}/airflow-images/ecom-datalake-pipeline:latest",
        ]
    )
    _run_cmd(["gcloud", "auth", "configure-docker", "us-central1-docker.pkg.dev"])
    _run_cmd(
        [
            "docker",
            "push",
            f"us-central1-docker.pkg.dev/{project_id}/airflow-images/ecom-datalake-pipeline:{version}",
        ]
    )
    _run_cmd(
        [
            "docker",
            "push",
            f"us-central1-docker.pkg.dev/{project_id}/airflow-images/ecom-datalake-pipeline:latest",
        ]
    )
