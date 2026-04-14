from __future__ import annotations

import logging
import os
import subprocess
from datetime import timedelta

from src.settings import RetryConfig, load_settings
from src.specs import load_spec_safe

logger = logging.getLogger(__name__)

# --- Configuration Helper (Lazy Loading) ---


class SettingsConfig:
    """Lazy configuration loader for Airflow DAGs."""

    def __init__(self):
        self._settings = None
        self._airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
        self._config_path = os.getenv(
            "ECOM_CONFIG_PATH", f"{self._airflow_home}/config/config.yml"
        )

    @property
    def settings(self):
        if self._settings is None:
            self._settings = load_settings(self._config_path)
        return self._settings

    @property
    def pipeline(self):
        return self.settings.pipeline

    @property
    def airflow_home(self):
        return self._airflow_home

    def resolve_pipeline_env(self) -> str:
        env_override = os.getenv("PIPELINE_ENV")
        return (env_override or self.pipeline.environment or "local").lower()

    def resolve_path(self, bucket: str, prefix: str, env_key: str | None = None) -> str:
        pipeline_env = self.resolve_pipeline_env()
        if env_key:
            override = os.getenv(env_key)
            if override:
                return (
                    self._resolve_local_path(override)
                    if bucket == "local" or pipeline_env == "local"
                    else override
                )
        if bucket == "local" or pipeline_env == "local":
            return self._resolve_local_path(prefix)
        return f"gs://{bucket}/{prefix}"

    def _resolve_local_path(self, path: str) -> str:
        if path.startswith("gs://") or os.path.isabs(path):
            return path
        return os.path.join(self.airflow_home, path)


# --- Top-Level Constants ---

PIPELINE_ENV = os.getenv("PIPELINE_ENV", "local").lower()
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")

COMMON_ENV = {
    "PIPELINE_ENV": PIPELINE_ENV,
    "OBSERVABILITY_ENV": os.getenv("OBSERVABILITY_ENV", ""),
    "PYTHONPATH": os.getenv("PYTHONPATH", AIRFLOW_HOME),
    "PATH": f"{os.getenv('PATH', '')}:/home/airflow/.local/bin",
    "HOME": os.getenv("HOME", "/home/airflow"),
    "CLOUDSDK_CONFIG": os.getenv("CLOUDSDK_CONFIG", ""),
    "USE_SA_AUTH": os.getenv("USE_SA_AUTH", ""),
    "GOOGLE_APPLICATION_CREDENTIALS": os.getenv("GOOGLE_APPLICATION_CREDENTIALS", ""),
    "GOOGLE_CLOUD_PROJECT": os.getenv("GOOGLE_CLOUD_PROJECT", ""),
    "BQ_LOCATION": os.getenv("BQ_LOCATION", ""),
    "BQ_GOLD_DATASET": os.getenv("BQ_GOLD_DATASET", ""),
    "BQ_LOAD_ENABLED": os.getenv("BQ_LOAD_ENABLED", ""),
    "GOLD_PIPELINE_ENABLED": os.getenv("GOLD_PIPELINE_ENABLED", ""),
    "BRONZE_BASE_PATH": os.getenv("BRONZE_BASE_PATH", ""),
    "SILVER_BASE_PATH": os.getenv("SILVER_BASE_PATH", ""),
    "SILVER_ENRICHED_PATH": os.getenv("SILVER_ENRICHED_PATH", ""),
    "SILVER_ENRICHED_LOCAL_PATH": os.getenv("SILVER_ENRICHED_LOCAL_PATH", ""),
    "SILVER_DIMS_PATH": os.getenv("SILVER_DIMS_PATH", ""),
    "SILVER_DIMS_LOCAL_PATH": os.getenv("SILVER_DIMS_LOCAL_PATH", ""),
    "SILVER_QUARANTINE_PATH": os.getenv("SILVER_QUARANTINE_PATH", ""),
    "SILVER_PUBLISH_MODE": os.getenv("SILVER_PUBLISH_MODE", ""),
    "ENRICHED_PUBLISH_MODE": os.getenv("ENRICHED_PUBLISH_MODE", ""),
    "DIMS_PUBLISH_MODE": os.getenv("DIMS_PUBLISH_MODE", ""),
    "METRICS_BUCKET": os.getenv("METRICS_BUCKET", ""),
    "LOGS_BUCKET": os.getenv("LOGS_BUCKET", ""),
    "REPORTS_BUCKET": os.getenv("REPORTS_BUCKET", ""),
    "REPORTS_BASE_PATH": os.getenv("REPORTS_BASE_PATH", ""),
}

# Avoid passing empty env vars that can break ADC or cloud auth detection.
COMMON_ENV = {key: value for key, value in COMMON_ENV.items() if value != ""}

# --- Helpers ---


def resolve_bool(env_key: str, default: bool = False) -> bool:
    """Resolve a boolean environment variable."""
    env_override = os.getenv(env_key)
    if env_override is None:
        return default
    return env_override.lower() in {"true", "1", "yes"}


def resolve_dims_base_path() -> str | None:
    """Resolve the dims snapshot base path, honoring env and spec defaults."""
    env_override = os.getenv("SILVER_DIMS_PATH")
    if env_override:
        base_path = env_override
    else:
        spec = load_spec_safe()
        if not spec or not spec.dims or not spec.dims.base_path:
            return None
        base_path = spec.dims.base_path

    base_path = os.path.expandvars(base_path)
    if base_path.startswith("gs://") or os.path.isabs(base_path):
        return base_path

    config = SettingsConfig()
    return os.path.join(config.airflow_home, base_path)


def sanitize_run_id(run_id: str) -> str:
    """Normalize run IDs for filesystem-safe staging paths."""
    return run_id.replace(":", "").replace("+", "").replace(" ", "_")


def resolve_run_config(run_id: str | None = None) -> dict[str, str]:
    """Resolve pipeline paths and configuration for a specific run."""
    config = SettingsConfig()
    pl = config.pipeline
    p_env = config.resolve_pipeline_env()

    run_id_clean = sanitize_run_id(run_id) if run_id else ""

    # Helper to resolve staging path
    def _resolve_staging(
        canonical: str,
        publish_mode_env_key: str,
        staging_path_env_key: str,
        config_attr: str,
    ) -> tuple[str, str, str]:
        # Get publish mode from env or config
        # env var priority: ENV_VAR > config.yml > "direct"
        default_mode = getattr(pl, config_attr, "direct")
        mode = (os.getenv(publish_mode_env_key) or default_mode).lower()

        staging_path = ""
        # Only enable staging if mode is 'staging' AND canonical path is GCS
        if mode == "staging" and canonical.startswith("gs://"):
            override = os.getenv(staging_path_env_key, "").strip()
            if override:
                staging_path = override
            elif run_id_clean:
                staging_path = f"{canonical.rstrip('/')}/_staging/{run_id_clean}"

        return mode, staging_path, (staging_path or canonical)

    # Resolve Bronze
    bronze_path = config.resolve_path(
        pl.bronze_bucket, pl.bronze_prefix, "BRONZE_BASE_PATH"
    )

    # Resolve Silver Base
    silver_path = config.resolve_path(
        pl.silver_bucket, pl.silver_base_prefix, "SILVER_BASE_PATH"
    )
    silver_mode, silver_staging, silver_validate = _resolve_staging(
        silver_path,
        "SILVER_PUBLISH_MODE",
        "SILVER_STAGING_PATH",
        "silver_publish_mode",
    )

    # Resolve Silver Enriched
    enriched_path = config.resolve_path(
        pl.silver_bucket, pl.silver_enriched_prefix, "SILVER_ENRICHED_PATH"
    )
    enriched_mode, enriched_staging, enriched_validate = _resolve_staging(
        enriched_path,
        "ENRICHED_PUBLISH_MODE",
        "ENRICHED_STAGING_PATH",
        "silver_enriched_publish_mode",
    )

    # Resolve Dims
    dims_path = resolve_dims_base_path() or "data/silver/dims"
    dims_mode, dims_staging, dims_validate = _resolve_staging(
        dims_path, "DIMS_PUBLISH_MODE", "DIMS_STAGING_PATH", "dims_publish_mode"
    )

    return {
        "env": p_env,
        "run_id_clean": run_id_clean,
        "bucket": pl.silver_bucket,
        "project_id": pl.project_id,
        "bq_dataset": pl.bigquery_dataset,
        "bronze": bronze_path,
        "silver": silver_path,
        "silver_staging": silver_staging,
        "silver_validate": silver_validate,
        "silver_publish_mode": silver_mode,
        "enriched": enriched_path,
        "enriched_staging": enriched_staging,
        "enriched_validate": enriched_validate,
        "enriched_publish_mode": enriched_mode,
        "dims": dims_path,
        "dims_staging": dims_staging,
        "dims_validate": dims_validate,
        "dims_publish_mode": dims_mode,
    }


def promote_staged_prefix(
    staging_path: str,
    canonical_path: str,
    env: str,
) -> None:
    """Promote staged data to canonical GCS prefix after validation."""
    if env not in ("dev", "prod"):
        logger.info(f"Skipping promote for env: {env}")
        return
    if not staging_path:
        logger.warning("Skipping promote: staging path not set")
        return
    if not staging_path.startswith("gs://") or not canonical_path.startswith("gs://"):
        logger.warning("Skipping promote: non-GCS paths")
        return
    logger.info(f"Promoting {staging_path} -> {canonical_path}")
    subprocess.run(
        ["gcloud", "storage", "rsync", "-r", staging_path, canonical_path],
        check=True,
    )


_resolved_dims_path = resolve_dims_base_path()
if _resolved_dims_path:
    COMMON_ENV.setdefault("SILVER_DIMS_PATH", _resolved_dims_path)
    if _resolved_dims_path.startswith("gs://"):
        COMMON_ENV.setdefault(
            "SILVER_DIMS_LOCAL_PATH", f"{AIRFLOW_HOME}/data/silver/dims"
        )


def run_enriched_runner(func, **kwargs):
    """Wrapper to run enriched runners with resolved paths."""
    config = SettingsConfig()
    pl = config.pipeline

    base_silver_path = config.resolve_path(
        pl.silver_bucket, pl.silver_base_prefix, "SILVER_BASE_PATH"
    )
    output_path = config.resolve_path(
        pl.silver_bucket, pl.silver_enriched_prefix, "SILVER_ENRICHED_PATH"
    )
    if base_silver_path.startswith("gs://"):
        base_silver_path = os.getenv(
            "SILVER_LOCAL_BASE_PATH", f"{config.airflow_home}/data/silver/base"
        )
    if output_path.startswith("gs://"):
        output_path = os.getenv(
            "SILVER_ENRICHED_LOCAL_PATH", f"{config.airflow_home}/data/silver/enriched"
        )

    if not os.path.isdir(base_silver_path):
        raise FileNotFoundError(
            "Enriched runner missing local silver base path. "
            f"Expected directory: {base_silver_path}"
        )
    if not output_path.startswith("gs://"):
        os.makedirs(output_path, exist_ok=True)

    kwargs["base_silver_path"] = base_silver_path
    kwargs["output_path"] = output_path

    allowed_keys = {"base_silver_path", "output_path", "ingest_dt"}
    filtered = {key: value for key, value in kwargs.items() if key in allowed_keys}
    return func(**filtered)


def make_runner_callable(runner_func):
    """Factory to create Airflow-compatible callables for runners."""

    def wrapper(**kwargs):
        return run_enriched_runner(runner_func, **kwargs)

    return wrapper


def get_retry_config(environment: str | None = None) -> dict:
    """Get Airflow retry configuration for the current environment.

    Args:
        environment: Environment name (local/dev/prod). If None, resolves
                    from PIPELINE_ENV or config.

    Returns:
        Dictionary with Airflow default_args retry settings:
        - retries: Number of retry attempts
        - retry_delay: timedelta for initial delay
        - retry_exponential_backoff: Whether to use exponential backoff
        - max_retry_delay: timedelta for maximum retry delay
    """
    config = SettingsConfig()
    env = environment or config.resolve_pipeline_env()

    # Get retry config for this environment
    retry_cfg: RetryConfig = config.pipeline.retry_config.get(env)
    if not retry_cfg:
        raise ValueError(
            f"No retry_config found for environment '{env}'. "
            f"Available: {list(config.pipeline.retry_config.keys())}"
        )

    return {
        "retries": retry_cfg.retries,
        "retry_delay": timedelta(minutes=retry_cfg.retry_delay_minutes),
        "retry_exponential_backoff": retry_cfg.retry_exponential_backoff,
        "max_retry_delay": timedelta(minutes=retry_cfg.max_retry_delay_minutes),
    }


def get_dim_specs() -> list[dict[str, str]]:
    """Return dim table specs (name + dbt_model) from spec or defaults."""
    spec = load_spec_safe()
    if spec:
        return [{"name": t.name, "dbt_model": t.dbt_model} for t in spec.dims.tables]
    return [
        {"name": "customers", "dbt_model": "stg_ecommerce__customers"},
        {"name": "product_catalog", "dbt_model": "stg_ecommerce__product_catalog"},
    ]


def get_dim_table_names() -> list[str]:
    return [t["name"] for t in get_dim_specs()]


def get_silver_base_table_names() -> list[str]:
    spec = load_spec_safe()
    if spec:
        return [t.name for t in spec.silver_base.tables]
    config = SettingsConfig()
    return list(config.pipeline.sla_thresholds.keys())


def get_enriched_table_names() -> list[str]:
    spec = load_spec_safe()
    if spec:
        return [t.name for t in spec.silver_enriched.tables]
    config = SettingsConfig()
    return list(config.pipeline.enriched_tables)
