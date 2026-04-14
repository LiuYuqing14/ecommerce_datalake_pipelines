"""Environment-aware configuration for observability."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path


def _truthy_env(key: str) -> bool:
    value = os.getenv(key, "")
    return value.lower() in {"1", "true", "yes", "on"}


def _adc_credentials_path() -> Path:
    config_dir = os.getenv("CLOUDSDK_CONFIG")
    if config_dir:
        return Path(config_dir) / "application_default_credentials.json"
    return Path.home() / ".config" / "gcloud" / "application_default_credentials.json"


def _cloud_auth_available() -> bool:
    use_sa_auth = _truthy_env("USE_SA_AUTH")
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if use_sa_auth:
        return bool(sa_path) and Path(sa_path).exists()

    adc_path = _adc_credentials_path()
    if adc_path.exists():
        return True
    if sa_path and Path(sa_path).exists():
        return True
    return False


class Environment(Enum):
    """Deployment environment."""

    LOCAL = "local"
    DEV = "dev"
    PROD = "prod"


@dataclass(frozen=True)
class ObservabilityConfig:
    """Configuration for metrics and logging storage.

    Adapts to local development vs production environments:
    - Local: Writes to data/metrics/ and data/logs/
    - Production: Writes to GCS buckets

    Environment is determined by PIPELINE_ENV environment variable.
    """

    environment: Environment
    project_root: Path
    metrics_bucket: str | None = None
    logs_bucket: str | None = None
    reports_bucket: str | None = None
    cloud_auth_available: bool = False

    @classmethod
    def from_env(cls) -> ObservabilityConfig:
        """Create config from environment variables with fallback to config.yml.

        Priority:
        1. PIPELINE_ENV environment variable
        2. config.yml pipeline.environment
        3. Default: "local"
        """
        project_root = Path(__file__).parent.parent.parent

        # Try loading from config.yml first
        config_env = "local"
        config_metrics_bucket = "ecom-datalake-metrics"
        config_logs_bucket = "ecom-datalake-logs"
        config_reports_bucket = "ecom-datalake-reports"

        try:
            from ..settings import load_settings

            config_path = os.getenv("ECOM_CONFIG_PATH")
            if not config_path:
                airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
                config_path = f"{airflow_home}/config/config.yml"
            settings = load_settings(config_path)
            config_env = settings.pipeline.environment
            config_metrics_bucket = settings.pipeline.metrics_bucket
            config_logs_bucket = settings.pipeline.logs_bucket
            config_reports_bucket = getattr(
                settings.pipeline, "reports_bucket", "ecom-datalake-reports"
            )
        except (OSError, ValueError, ImportError) as exc:
            # Fallback to local defaults if config.yml is missing or invalid
            import sys

            print(
                f"WARNING: Observability falling back to defaults. Reason: {exc}",
                file=sys.stderr,
            )
        # Environment variable overrides config file
        env_override = os.getenv("OBSERVABILITY_ENV")
        if env_override:
            env_str = env_override.lower()
        else:
            env_str = os.getenv("PIPELINE_ENV", config_env).lower()
        environment = Environment(env_str)

        cloud_auth_available = _cloud_auth_available()

        # In production, use GCS buckets (when auth is available)
        if environment in (Environment.DEV, Environment.PROD):
            metrics_bucket = os.getenv("METRICS_BUCKET", config_metrics_bucket)
            logs_bucket = os.getenv("LOGS_BUCKET", config_logs_bucket)
            reports_bucket = os.getenv("REPORTS_BUCKET", config_reports_bucket)
        else:
            metrics_bucket = None
            logs_bucket = None
            reports_bucket = None

        return cls(
            environment=environment,
            project_root=project_root,
            metrics_bucket=metrics_bucket,
            logs_bucket=logs_bucket,
            reports_bucket=reports_bucket,
            cloud_auth_available=cloud_auth_available,
        )

    @property
    def metrics_base_path(self) -> str:
        """Base path for metrics storage."""
        if not self.use_cloud_metrics():
            # Use /tmp in Docker to avoid macOS bind mount Errno 35 locking issues
            return os.getenv(
                "METRICS_BASE_PATH", str(self.project_root / "data" / "metrics")
            )
        return f"gs://{self.metrics_bucket}/pipeline_metrics"

    @property
    def logs_base_path(self) -> str:
        """Base path for logs storage."""
        if not self.use_cloud_logs():
            # Use /tmp in Docker to avoid macOS bind mount Errno 35 locking issues
            return os.getenv("LOGS_BASE_PATH", str(self.project_root / "data" / "logs"))
        return f"gs://{self.logs_bucket}/pipeline_logs"

    @property
    def reports_base_path(self) -> str:
        """Base path for validation reports storage."""
        if not self.use_cloud_reports():
            return os.getenv(
                "REPORTS_BASE_PATH",
                str(self.project_root / "docs" / "validation_reports"),
            )
        return f"gs://{self.reports_bucket}/validation_reports"

    def get_metrics_path(self, metric_type: str) -> str:
        """Get path for specific metric type.

        Args:
            metric_type: Type of metric (e.g., 'pipeline_runs', 'data_quality')

        Returns:
            Full path for storing metrics of this type
        """
        return f"{self.metrics_base_path}/{metric_type}"

    def get_run_metrics_path(self, run_id: str, metric_type: str) -> str:
        """Get path for metrics grouped under a run folder."""
        if not self.use_cloud_metrics():
            return self.get_metrics_path(metric_type)
        return f"{self.metrics_base_path}/{run_id}/{metric_type}"

    def get_logs_path(self, log_type: str) -> str:
        """Get path for specific log type.

        Args:
            log_type: Type of log (e.g., 'errors', 'audit')

        Returns:
            Full path for storing logs of this type
        """
        return f"{self.logs_base_path}/{log_type}"

    def get_report_path(self, report_name: str) -> str:
        """Get full path for validation report.

        Args:
            report_name: Name of the report file (e.g.,
                'BRONZE_QUALITY_manual_20250117.md')

        Returns:
            Full path for storing the validation report
        """
        return f"{self.reports_base_path}/{report_name}"

    def get_run_report_path(self, run_id: str, report_name: str) -> str:
        """Get full path for validation report under a run folder."""
        return f"{self.reports_base_path}/{run_id}/{report_name}"

    @property
    def is_local(self) -> bool:
        """Whether running in local development mode."""
        return self.environment == Environment.LOCAL

    def use_cloud_metrics(self) -> bool:
        """Whether to use cloud storage for metrics."""
        return (
            self.environment in (Environment.DEV, Environment.PROD)
            and bool(self.metrics_bucket)
            and self.cloud_auth_available
        )

    def use_cloud_logs(self) -> bool:
        """Whether to use cloud storage for logs."""
        return (
            self.environment in (Environment.DEV, Environment.PROD)
            and bool(self.logs_bucket)
            and self.cloud_auth_available
        )

    def use_cloud_reports(self) -> bool:
        """Whether to use cloud storage for reports."""
        return (
            self.environment in (Environment.DEV, Environment.PROD)
            and bool(self.reports_bucket)
            and self.cloud_auth_available
        )

    @property
    def use_cloud_logging(self) -> bool:
        """Whether to send logs to Cloud Logging (Stackdriver)."""
        return self.use_cloud_logs()

    def ensure_local_dirs(self) -> None:
        """Create local directories if in local mode."""
        # Create metrics subdirectories when writing locally
        if not self.use_cloud_metrics():
            metrics_types = [
                "pipeline_runs",
                "data_quality",
                "silver_quality",
                "enriched_silver",
            ]
            for metric_type in metrics_types:
                path = Path(self.get_metrics_path(metric_type))
                path.mkdir(parents=True, exist_ok=True)

        # Create logs subdirectories when writing locally
        if not self.use_cloud_logs():
            log_types = ["errors", "audit", "debug"]
            for log_type in log_types:
                path = Path(self.get_logs_path(log_type))
                path.mkdir(parents=True, exist_ok=True)


def get_config() -> ObservabilityConfig:
    """Get the observability configuration for current environment."""
    return ObservabilityConfig.from_env()


def get_metrics_writer(metric_type: str):
    """Get a metrics writer for the current environment.

    Args:
        metric_type: Type of metric to write

    Returns:
        MetricsWriter instance
    """
    from .metrics import MetricsWriter

    config = get_config()
    return MetricsWriter(config, metric_type)
