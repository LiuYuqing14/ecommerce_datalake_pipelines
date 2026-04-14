"""Structured logging for pipeline observability."""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import ObservabilityConfig


class StructuredLogger:
    """Logger that writes JSON-formatted log entries.

    Supports both standard logging and JSONL (JSON Lines) files for
    easy ingestion by log aggregation systems.
    """

    def __init__(
        self,
        name: str,
        config: ObservabilityConfig,
        base_context: dict[str, Any] | None = None,
    ):
        self.name = name
        self.config = config
        self.logger = logging.getLogger(name)
        self.base_context = base_context or {}

        # Set up handlers
        self._setup_handlers()

    def _setup_handlers(self) -> None:
        """Set up logging handlers based on environment."""
        # Console handler (always present)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        # Use simple format for console
        console_formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
        )
        console_handler.setFormatter(console_formatter)

        self.logger.addHandler(console_handler)
        self.logger.setLevel(logging.INFO)

        # File handler for local development
        if not self.config.use_cloud_logs():
            log_dir = Path(self.config.logs_base_path) / "debug"
            log_dir.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(log_dir / f"{self.name}.log")
            file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d: %(message)s"
            )
            file_handler.setFormatter(file_formatter)
            self.logger.addHandler(file_handler)

    def _write_jsonl(self, log_type: str, level: str, message: str, **context) -> None:
        """Write a JSONL log entry.

        Args:
            log_type: Type of log (errors, audit, etc.)
            level: Log level (ERROR, WARN, INFO, etc.)
            message: Log message
            **context: Additional context fields
        """
        resolved_context = {**self.base_context, **context}
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "component": self.name,
            "message": message,
            "environment": self.config.environment.value,
            **resolved_context,
        }

        # Determine file path
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        run_id = resolved_context.get("run_id")
        token = uuid.uuid4().hex[:8]
        timestamp = datetime.now(timezone.utc).strftime("%H%M%S%f")
        if run_id:
            filename = f"{log_type}_{date_str}_{run_id}_{timestamp}_{token}.jsonl"
        else:
            filename = f"{log_type}_{date_str}_{timestamp}_{token}.jsonl"

        try:
            if not self.config.use_cloud_logs():
                # Local: Append to file
                log_path = Path(self.config.get_logs_path(log_type)) / filename
                log_path.parent.mkdir(parents=True, exist_ok=True)

                with open(log_path, "a") as f:
                    f.write(json.dumps(log_entry, default=str) + "\n")
            else:
                # Production: Write to GCS
                import fsspec

                log_uri = f"{self.config.get_logs_path(log_type)}/{filename}"
                fs = fsspec.filesystem("gcs")
                with fs.open(log_uri, "w") as f:
                    f.write(json.dumps(log_entry, default=str) + "\n")
        except OSError as exc:
            self.logger.warning(
                "Failed to write structured log",
                exc_info=exc,
                extra={
                    **self.base_context,
                    "log_type": log_type,
                    "log_destination": self.config.get_logs_path(log_type),
                },
            )

    def info(self, message: str, **context) -> None:
        """Log info message."""
        self.logger.info(message, extra={**self.base_context, **context})

    def warning(self, message: str, **context) -> None:
        """Log warning message."""
        self.logger.warning(message, extra={**self.base_context, **context})

    def error(
        self,
        message: str,
        error_type: str | None = None,
        exc_info: bool = True,
        **context,
    ) -> None:
        """Log error message and write to errors JSONL.

        Args:
            message: Error message
            error_type: Type of error (optional)
            exc_info: Whether to include exception info
            **context: Additional context
        """
        self.logger.error(
            message,
            exc_info=exc_info,
            extra={**self.base_context, **context},
        )

        # Also write to structured error log
        error_context = {"error_type": error_type, **context}

        if exc_info and sys.exc_info()[0] is not None:
            import traceback

            error_context["traceback"] = traceback.format_exc()

        self._write_jsonl("errors", "ERROR", message, **error_context)

    def metric(self, metric_name: str, value: Any, **tags) -> None:
        """Log a metric value.

        Args:
            metric_name: Name of the metric
            value: Metric value
            **tags: Additional tags/dimensions
        """
        message = f"{metric_name}={value}"
        self.logger.info(
            message,
            extra={
                **self.base_context,
                "metric_name": metric_name,
                "value": value,
                **tags,
            },
        )

        # Write to audit log
        self._write_jsonl(
            "audit",
            "INFO",
            message,
            metric_name=metric_name,
            value=value,
            **tags,
        )


# Global logger cache
_loggers: dict[str, StructuredLogger] = {}


def _context_from_env() -> dict[str, str]:
    context: dict[str, str] = {}
    run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or os.getenv("ECOM_RUN_ID")
    if run_id:
        context["run_id"] = run_id
    dag_id = os.getenv("AIRFLOW_CTX_DAG_ID")
    if dag_id:
        context["dag_id"] = dag_id
    task_id = os.getenv("AIRFLOW_CTX_TASK_ID")
    if task_id:
        context["task_id"] = task_id
    execution_date = os.getenv("AIRFLOW_CTX_EXECUTION_DATE")
    if execution_date:
        context["execution_date"] = execution_date
    return context


def get_logger(name: str, **context) -> StructuredLogger:
    """Get or create a structured logger.

    Args:
        name: Logger name (usually __name__)
        **context: Optional context fields (run_id, dag_id, task_id, etc.)

    Returns:
        StructuredLogger instance
    """
    if name not in _loggers:
        config = ObservabilityConfig.from_env()
        base_context = {**_context_from_env(), **context}
        _loggers[name] = StructuredLogger(name, config, base_context)
        return _loggers[name]

    logger = _loggers[name]
    if context:
        logger.base_context.update(context)
    return logger


def log_metric(metric_name: str, value: Any, **tags) -> None:
    """Convenience function to log a metric.

    Args:
        metric_name: Name of the metric
        value: Metric value
        **tags: Additional tags/dimensions
    """
    logger = get_logger("metrics")
    logger.metric(metric_name, value, **tags)


def log_error(message: str, error_type: str | None = None, **context) -> None:
    """Convenience function to log an error.

    Args:
        message: Error message
        error_type: Type of error
        **context: Additional context
    """
    logger = get_logger("errors")
    logger.error(message, error_type=error_type, **context)
