"""Metrics collection and persistence."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

import yaml

from .config import ObservabilityConfig


class MetricType(Enum):
    """Types of metrics we track."""

    PIPELINE_RUN = "pipeline_runs"
    DATA_QUALITY = "data_quality"
    SILVER_QUALITY = "silver_quality"
    ENRICHED_SILVER = "enriched_silver"
    BRONZE_METADATA = "bronze_metadata"
    PYDANTIC_VALIDATION = "pydantic_validation"


def _read_dbt_version() -> str:
    """Read dbt project version from dbt_project.yml.

    Returns:
        Version string from dbt_project.yml, or "unknown" if not found.
    """
    dbt_project_path = Path(__file__).parents[2] / "dbt_bigquery" / "dbt_project.yml"
    if not dbt_project_path.exists():
        return "unknown"

    try:
        with open(dbt_project_path) as f:
            config = yaml.safe_load(f)
        return config.get("version", "unknown")
    except Exception:
        return "unknown"


class MetricsWriter:
    """Write metrics to environment-appropriate storage.

    In local mode: Writes JSON files to data/metrics/
    In production: Writes to GCS buckets

    Usage:
        writer = MetricsWriter(config, "data_quality")
        writer.write_metric({
            "table": "orders",
            "row_count": 1000000,
            ...
        }, run_id="20260111_143022")
    """

    def __init__(self, config: ObservabilityConfig, metric_type: str):
        self.config = config
        self.metric_type = metric_type
        self.base_path = config.get_metrics_path(metric_type)
        self.use_cloud = config.use_cloud_metrics()

        # Ensure local directories exist
        if not self.use_cloud:
            Path(self.base_path).mkdir(parents=True, exist_ok=True)

    def write_metric(
        self,
        data: dict[str, Any],
        run_id: str | None = None,
        filename: str | None = None,
    ) -> str:
        """Write a metric to storage.

        Args:
            data: Metric data to write (will be JSON-serialized)
            run_id: Optional run ID (used in filename if provided)
            filename: Optional explicit filename (overrides auto-generation)

        Returns:
            Path where metric was written
        """
        # Auto-generate filename if not provided
        if filename is None:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            if run_id:
                filename = f"{self.metric_type}_{run_id}.json"
            else:
                filename = f"{self.metric_type}_{timestamp}.json"

        # Add metadata
        enriched_data = {
            "metadata": {
                "written_at": datetime.now(timezone.utc).isoformat(),
                "environment": self.config.environment.value,
                "metric_type": self.metric_type,
            },
            **data,
        }

        base_path = self.base_path
        if run_id and self.use_cloud:
            base_path = self.config.get_run_metrics_path(run_id, self.metric_type)

        # Write to storage
        file_path = f"{base_path}/{filename}"

        if not self.use_cloud:
            # Local: Write to filesystem
            with open(file_path, "w") as f:
                json.dump(enriched_data, f, indent=2, default=str)
        else:
            # Production: Write to GCS
            # Note: Requires fsspec[gcs] or gcsfs to be installed
            import fsspec

            fs = fsspec.filesystem("gcs")
            with fs.open(file_path, "w") as f:
                json.dump(enriched_data, f, indent=2, default=str)

        return file_path

    def read_metrics(
        self, pattern: str = "*.json", limit: int | None = None
    ) -> list[dict]:
        """Read metrics from storage.

        Args:
            pattern: Glob pattern to match files
            limit: Maximum number of files to read (most recent first)

        Returns:
            List of metric dictionaries
        """
        if not self.use_cloud:
            # Local: Read from filesystem
            files = sorted(
                Path(self.base_path).glob(pattern),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if limit:
                files = files[:limit]

            metrics = []
            for file_path in files:
                with open(file_path) as f:
                    metrics.append(json.load(f))
            return metrics
        else:
            # Production: Read from GCS
            import fsspec

            fs = fsspec.filesystem("gcs")
            file_pattern = f"{self.base_path}/{pattern}"
            files = sorted(fs.glob(file_pattern), reverse=True)
            if limit:
                files = files[:limit]

            metrics = []
            for file_path in files:
                with fs.open(f"gs://{file_path}") as f:
                    metrics.append(json.load(f))
            return metrics

    def get_latest_metric(self) -> dict | None:
        """Get the most recently written metric."""
        metrics = self.read_metrics(limit=1)
        return metrics[0] if metrics else None


def write_pipeline_run_metric(
    run_id: str,
    status: str,
    start_time: datetime,
    end_time: datetime,
    phase_timings: list[dict],
    **extra_fields,
) -> str:
    """Write a pipeline run metric.

    Args:
        run_id: Unique run identifier
        status: Run status (SUCCESS, FAILED, etc.)
        start_time: When pipeline started
        end_time: When pipeline ended
        phase_timings: List of phase timing dicts
        **extra_fields: Additional fields to include

    Returns:
        Path where metric was written
    """
    config = ObservabilityConfig.from_env()
    writer = MetricsWriter(config, MetricType.PIPELINE_RUN.value)

    duration = (end_time - start_time).total_seconds()

    data = {
        "run_metadata": {
            "run_id": run_id,
            "pipeline_name": "ecom_silver_to_gold",
            "environment": config.environment.value,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "status": status,
            **extra_fields,
        },
        "phase_timings": phase_timings,
    }

    return writer.write_metric(data, run_id=run_id)


def write_data_quality_metric(
    run_id: str,
    validation_type: str,
    table_metrics: list[dict],
    overall_status: str,
    **extra_fields,
) -> str:
    """Write a data quality metric.

    Args:
        run_id: Unique run identifier
        validation_type: Type of validation (e.g., 'bronze_metadata')
        table_metrics: List of per-table metric dicts
        overall_status: Overall validation status
        **extra_fields: Additional fields to include

    Returns:
        Path where metric was written
    """
    config = ObservabilityConfig.from_env()
    writer = MetricsWriter(config, MetricType.DATA_QUALITY.value)

    data = {
        "validation_metadata": {
            "run_id": run_id,
            "validation_type": validation_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **extra_fields,
        },
        "table_metrics": table_metrics,
        "overall_status": overall_status,
    }

    filename = f"{validation_type}_{run_id}.json"
    return writer.write_metric(data, run_id=run_id, filename=filename)


def write_silver_quality_metric(
    run_id: str, table_metrics: list[dict], overall_status: str, **extra_fields
) -> str:
    """Write Silver layer quality metrics.

    Args:
        run_id: Unique run identifier
        table_metrics: List of per-table quality metrics
        overall_status: Overall quality status

    Returns:
        Path where metric was written
    """
    config = ObservabilityConfig.from_env()
    writer = MetricsWriter(config, MetricType.SILVER_QUALITY.value)

    data = {
        "transformation_metadata": {
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "dbt_project_version": _read_dbt_version(),
            **extra_fields,
        },
        "table_metrics": table_metrics,
        "overall_status": overall_status,
    }

    return writer.write_metric(data, run_id=run_id)


def write_enriched_quality_metric(
    run_id: str, table_metrics: list[dict], overall_status: str, **extra_fields
) -> str:
    """Write Enriched Silver layer quality metrics."""
    config = ObservabilityConfig.from_env()
    writer = MetricsWriter(config, MetricType.ENRICHED_SILVER.value)

    data = {
        "transformation_metadata": {
            "run_id": run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **extra_fields,
        },
        "table_metrics": table_metrics,
        "overall_status": overall_status,
    }

    return writer.write_metric(data, run_id=run_id)
