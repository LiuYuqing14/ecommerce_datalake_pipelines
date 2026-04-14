"""Create point-in-time dims snapshots from silver base tables."""

from __future__ import annotations

import json
import os
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

from src.observability import get_logger
from src.runners.manifest import generate_manifest
from src.specs import load_spec_safe

logger = get_logger(__name__)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")


def _resolve_run_id() -> str:
    for key in ("RUN_ID", "AIRFLOW_CTX_DAG_RUN_ID", "AIRFLOW_CTX_EXECUTION_DATE"):
        value = os.getenv(key, "").strip()
        if value:
            return value.replace(":", "").replace("+", "").replace(" ", "_")
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _resolve_local_base_path(path: str, fallback: str) -> str:
    if not path:
        path = fallback
    if path.startswith("gs://"):
        return fallback
    if os.path.isabs(path):
        return path
    return str(Path(AIRFLOW_HOME) / path)


def _resolve_dims_paths(dims_base_path: str) -> tuple[str, str | None]:
    dims_base_path = os.path.expandvars(dims_base_path)
    if dims_base_path.startswith("gs://"):
        local_path = _resolve_local_base_path(
            os.getenv("SILVER_DIMS_LOCAL_PATH", ""),
            str(Path(AIRFLOW_HOME) / "data/silver/dims"),
        )
        return local_path, dims_base_path
    return _resolve_local_base_path(dims_base_path, dims_base_path), None


def _load_table(base_path: str, table: str) -> pl.DataFrame:
    glob_path = str(Path(base_path) / table / "**" / "*.parquet")
    if not Path(base_path).exists():
        raise FileNotFoundError(f"Base silver path not found: {base_path}")
    try:
        return pl.scan_parquet(glob_path, hive_partitioning=True).collect()
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Missing parquet for {table}: {glob_path}") from exc


def _write_snapshot(
    df: pl.DataFrame, dims_path: str, table: str, run_date: str
) -> None:
    if df.is_empty():
        raise ValueError(f"Snapshot for {table} is empty on {run_date}")
    output_dir = Path(dims_path) / table / f"snapshot_dt={run_date}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "data_0.parquet"
    df.write_parquet(str(output_path))


def snapshot_dims(run_date: str, silver_base_path: str | None = None) -> None:
    run_dt = date.fromisoformat(run_date)
    spec = load_spec_safe()
    dims_tables = (
        [table.name for table in spec.dims.tables]
        if spec and spec.dims.tables
        else ["customers", "product_catalog"]
    )
    dims_base_path = (
        spec.dims.base_path if spec and spec.dims.base_path else "data/silver/dims"
    )
    dims_local_path, dims_gcs_path = _resolve_dims_paths(dims_base_path)
    dims_publish_mode = os.getenv("DIMS_PUBLISH_MODE", "direct").lower()
    dims_staging_path = os.getenv("DIMS_STAGING_PATH", "").strip()
    if dims_gcs_path and dims_publish_mode == "staging":
        if not dims_staging_path:
            run_id = _resolve_run_id()
            dims_staging_path = f"{dims_gcs_path.rstrip('/')}/_staging/{run_id}"
        dims_gcs_path = dims_staging_path

    if silver_base_path is None:
        silver_base_path = os.getenv("SILVER_BASE_PATH", "data/silver/base")
    silver_base_path_raw = silver_base_path
    silver_base_path = _resolve_local_base_path(
        silver_base_path,
        os.getenv(
            "SILVER_LOCAL_BASE_PATH", str(Path(AIRFLOW_HOME) / "data/silver/base")
        ),
    )

    logger.info("Building dims snapshot", run_date=run_date)
    pipeline_env = os.getenv("PIPELINE_ENV", "local").lower()
    allow_fallback = pipeline_env in {"local", "dev"} or not str(
        silver_base_path_raw
    ).startswith("gs://")
    ignore_customer_signup = os.getenv(
        "DIMS_CUSTOMERS_IGNORE_SIGNUP_DATE", ""
    ).lower() in {"1", "true", "yes"}
    # Explicit opt-in for backfill bootstrap when run_date predates the first
    # available product_catalog partition (kept off by default for prod).
    bootstrap_allowed = os.getenv("DIMS_SNAPSHOT_ALLOW_BOOTSTRAP", "").lower() in {
        "1",
        "true",
        "yes",
    }

    for table in dims_tables:
        df = _load_table(silver_base_path, table)
        if table == "customers":
            df = df.with_columns(
                pl.col("signup_date").cast(pl.Date, strict=False).alias("signup_date"),
                pl.lit(run_dt).cast(pl.Date).alias("as_of_dt"),
            )
            if not ignore_customer_signup:
                df = df.filter(
                    pl.col("signup_date").is_not_null()
                    & (pl.col("signup_date") <= run_dt)
                )
        elif table == "product_catalog":
            # Cast ingestion_dt to Date if it exists
            if "ingestion_dt" in df.columns:
                base_df = df.with_columns(
                    pl.col("ingestion_dt")
                    .cast(pl.Date, strict=False)
                    .alias("ingestion_dt"),
                )
            else:
                logger.warning(
                    f"ingestion_dt not found in {table} source; "
                    "hive partitioning might have failed or table is unpartitioned"
                )
                base_df = df

            # Filter for data valid ON or BEFORE the run date
            df = base_df.with_columns(
                pl.lit(run_dt).cast(pl.Date).alias("as_of_dt"),
            )

            if "ingestion_dt" in df.columns:
                df = df.filter(
                    pl.col("ingestion_dt").is_not_null()
                    & (pl.col("ingestion_dt") <= run_dt)
                )

            if df.is_empty() and allow_fallback:
                max_dt = None
                if "ingestion_dt" in base_df.columns:
                    # Collect min/max to find latest available
                    max_dt_series = base_df.select(
                        pl.col("ingestion_dt").max()
                    ).to_series()
                    if len(max_dt_series) > 0 and max_dt_series[0] is not None:
                        max_dt = max_dt_series[0]

                if max_dt:
                    logger.warning(
                        f"Product catalog empty for {run_date}; "
                        f"falling back to latest available partition: {max_dt}",
                        run_date=run_date,
                        fallback_date=str(max_dt),
                    )
                    df = base_df.filter(pl.col("ingestion_dt") == max_dt).with_columns(
                        pl.lit(run_dt).cast(pl.Date).alias("as_of_dt")
                    )
                else:
                    logger.warning(
                        f"No fallback possible for {table}: "
                        "source is empty or has no ingestion_dt"
                    )
            elif df.is_empty() and bootstrap_allowed:
                # Backfill bootstrap: if run_date is before the first partition,
                # use the earliest available data but keep the snapshot date.
                if "ingestion_dt" in base_df.columns:
                    stats = base_df.select(
                        pl.col("ingestion_dt").min().alias("min_dt"),
                        pl.col("ingestion_dt").max().alias("max_dt"),
                    ).row(0)
                    min_dt, max_dt = stats
                    if min_dt and run_dt < min_dt:
                        logger.warning(
                            f"Product catalog empty for {run_date}; "
                            "bootstrapping from earliest available partition: "
                            f"{min_dt}",
                            run_date=run_date,
                            bootstrap_date=str(min_dt),
                        )
                        df = base_df.filter(
                            pl.col("ingestion_dt") == min_dt
                        ).with_columns(pl.lit(run_dt).cast(pl.Date).alias("as_of_dt"))
                    else:
                        logger.warning(
                            f"Bootstrap not applied for {table}: "
                            f"run_date={run_date} min_dt={min_dt} max_dt={max_dt}"
                        )
                else:
                    logger.warning(
                        f"Bootstrap not possible for {table}: "
                        "source is empty or has no ingestion_dt"
                    )
        else:
            df = df.with_columns(pl.lit(run_dt).cast(pl.Date).alias("as_of_dt"))

        if df.is_empty():
            logger.error(
                f"Snapshot for {table} is empty for {run_date}. "
                f"allow_fallback={allow_fallback}, pipeline_env={pipeline_env}"
            )

        _write_snapshot(df, dims_local_path, table, run_date)
        generate_manifest(Path(dims_local_path) / table)

    if dims_gcs_path:
        logger.info("Syncing dims snapshot to GCS", dims_path=dims_gcs_path)
        result = subprocess.run(
            ["gcloud", "storage", "rsync", "-r", dims_local_path, dims_gcs_path],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logger.error(
                f"GCS rsync failed with exit code {result.returncode}",
                extra={"stderr": result.stderr},
            )
            raise RuntimeError(
                f"Failed to sync dims to GCS: {result.stderr or 'Unknown error'}"
            )


def publish_dims_latest(run_date: str, run_id: str) -> None:
    spec = load_spec_safe()
    dims_base_path = (
        spec.dims.base_path if spec and spec.dims.base_path else "data/silver/dims"
    )
    dims_local_path, dims_gcs_path = _resolve_dims_paths(dims_base_path)
    payload = {
        "run_date": run_date,
        "run_id": run_id,
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    latest_path = Path(dims_local_path) / "_latest.json"
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if dims_gcs_path:
        result = subprocess.run(
            [
                "gcloud",
                "storage",
                "cp",
                str(latest_path),
                f"{dims_gcs_path}/_latest.json",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            logger.error(
                f"GCS copy failed with exit code {result.returncode}",
                extra={"stderr": result.stderr},
            )
            raise RuntimeError(
                f"Failed to copy _latest.json to GCS: "
                f"{result.stderr or 'Unknown error'}"
            )
