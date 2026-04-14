"""
Observability framework for pipeline metrics and logging.

Provides environment-aware storage (local filesystem vs GCS) and
structured logging for tracking pipeline execution, data quality,
and performance metrics.
"""

from .config import ObservabilityConfig, get_metrics_writer
from .metrics import MetricsWriter, MetricType
from .structured_logging import get_logger, log_error, log_metric

__all__ = [
    "ObservabilityConfig",
    "get_metrics_writer",
    "MetricsWriter",
    "MetricType",
    "get_logger",
    "log_metric",
    "log_error",
]
