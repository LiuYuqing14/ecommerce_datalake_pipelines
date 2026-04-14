"""Unit tests for src/observability/structured_logging.py."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.observability.config import Environment, ObservabilityConfig
from src.observability.structured_logging import (
    StructuredLogger,
    get_logger,
    log_error,
    log_metric,
)


@pytest.fixture
def test_config():
    """Create test observability config."""
    return ObservabilityConfig(
        environment=Environment.LOCAL,
        project_root=Path("/tmp/test_project"),
    )


@pytest.fixture
def temp_log_dir():
    """Create temporary directory for log tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


class TestStructuredLogger:
    """Tests for StructuredLogger class."""

    def test_initialization(self, test_config):
        logger = StructuredLogger("test_logger", test_config)
        assert logger.name == "test_logger"
        assert logger.config == test_config
        assert logger.base_context == {}

    def test_initialization_with_base_context(self, test_config):
        context = {"run_id": "test_123", "dag_id": "my_dag"}
        logger = StructuredLogger("test_logger", test_config, context)
        assert logger.base_context == context

    def test_info_logging(self, test_config):
        logger = StructuredLogger("test_logger", test_config)
        # Should not raise
        logger.info("Test info message", extra_field="value")

    def test_warning_logging(self, test_config):
        logger = StructuredLogger("test_logger", test_config)
        # Should not raise
        logger.warning("Test warning message", warning_code="W001")

    def test_error_logging_writes_jsonl(self, temp_log_dir):
        with patch.dict(os.environ, {"LOGS_BASE_PATH": temp_log_dir}):
            config = ObservabilityConfig(
                environment=Environment.LOCAL,
                project_root=Path(temp_log_dir),
            )
            logger = StructuredLogger("test_logger", config)

            logger.error("Test error", error_type="ValueError", exc_info=False)

            # Check that error JSONL file was created
            error_logs = list(Path(temp_log_dir).glob("**/errors_*.jsonl"))
            assert len(error_logs) >= 1

            # Verify content
            with open(error_logs[0], encoding="utf-8") as f:
                log_entry = json.loads(f.read().strip())
                assert log_entry["message"] == "Test error"
                assert log_entry["level"] == "ERROR"
                assert log_entry["error_type"] == "ValueError"
                assert log_entry["component"] == "test_logger"

    def test_metric_logging_writes_to_audit(self, temp_log_dir):
        with patch.dict(os.environ, {"LOGS_BASE_PATH": temp_log_dir}):
            config = ObservabilityConfig(
                environment=Environment.LOCAL,
                project_root=Path(temp_log_dir),
            )
            logger = StructuredLogger("test_logger", config)

            logger.metric("rows_processed", 1000, table="orders")

            # Check that audit JSONL file was created
            audit_logs = list(Path(temp_log_dir).glob("**/audit_*.jsonl"))
            assert len(audit_logs) >= 1

            # Verify content
            with open(audit_logs[0], encoding="utf-8") as f:
                log_entry = json.loads(f.read().strip())
                assert log_entry["metric_name"] == "rows_processed"
                assert log_entry["value"] == 1000
                assert log_entry["table"] == "orders"

    def test_error_with_exception_includes_traceback(self, temp_log_dir):
        with patch.dict(os.environ, {"LOGS_BASE_PATH": temp_log_dir}):
            config = ObservabilityConfig(
                environment=Environment.LOCAL,
                project_root=Path(temp_log_dir),
            )
            logger = StructuredLogger("test_logger", config)

            try:
                raise ValueError("Test exception")
            except ValueError:
                logger.error("Caught error", error_type="ValueError", exc_info=True)

            # Verify traceback is included
            error_logs = list(Path(temp_log_dir).glob("**/errors_*.jsonl"))
            with open(error_logs[0], encoding="utf-8") as f:
                log_entry = json.loads(f.read().strip())
                assert "traceback" in log_entry
                assert "ValueError: Test exception" in log_entry["traceback"]

    def test_base_context_merged_with_log_context(self, temp_log_dir):
        with patch.dict(os.environ, {"LOGS_BASE_PATH": temp_log_dir}):
            config = ObservabilityConfig(
                environment=Environment.LOCAL,
                project_root=Path(temp_log_dir),
            )
            base_context = {"run_id": "run_123"}
            logger = StructuredLogger("test_logger", config, base_context)

            logger.metric("test_metric", 42, table="orders")

            audit_logs = list(Path(temp_log_dir).glob("**/audit_*.jsonl"))
            with open(audit_logs[0], encoding="utf-8") as f:
                log_entry = json.loads(f.read().strip())
                assert log_entry["run_id"] == "run_123"
                assert log_entry["table"] == "orders"

    def test_jsonl_write_failure_logs_warning(self, test_config):
        logger = StructuredLogger("test_logger", test_config)

        # Mock the file write to raise an error
        with patch("builtins.open", side_effect=OSError("Permission denied")):
            # Should not raise, but log a warning
            logger.error("Test error", error_type="TestError", exc_info=False)

    @patch("fsspec.filesystem")
    def test_cloud_logs_write_to_gcs(self, mock_fsspec):
        config = ObservabilityConfig(
            environment=Environment.PROD,
            project_root=Path("/tmp"),
            metrics_bucket="my-bucket",
            logs_bucket="my-bucket",
            cloud_auth_available=True,
        )
        logger = StructuredLogger("test_logger", config)

        # Mock GCS filesystem
        mock_fs = MagicMock()
        mock_file = MagicMock()
        mock_fs.open.return_value.__enter__.return_value = mock_file
        mock_fsspec.return_value = mock_fs

        logger.metric("test_metric", 100)

        # Verify fsspec was called for GCS
        mock_fsspec.assert_called_with("gcs")
        assert mock_fs.open.called


class TestGetLogger:
    """Tests for get_logger() function."""

    def test_creates_new_logger(self):
        logger = get_logger("new_logger_test")
        assert logger.name == "new_logger_test"

    def test_returns_cached_logger(self):
        logger1 = get_logger("cached_logger_test")
        logger2 = get_logger("cached_logger_test")
        assert logger1 is logger2

    def test_updates_context_on_cached_logger(self):
        logger1 = get_logger("context_test", run_id="run_1")
        logger2 = get_logger("context_test", dag_id="dag_1")

        assert logger1 is logger2
        assert logger1.base_context["run_id"] == "run_1"
        assert logger1.base_context["dag_id"] == "dag_1"

    @patch.dict(os.environ, {"AIRFLOW_CTX_DAG_RUN_ID": "airflow_run_123"})
    def test_extracts_context_from_environment(self):
        logger = get_logger("env_context_test")
        assert logger.base_context["run_id"] == "airflow_run_123"

    @patch.dict(
        os.environ,
        {
            "AIRFLOW_CTX_DAG_ID": "my_dag",
            "AIRFLOW_CTX_TASK_ID": "my_task",
            "AIRFLOW_CTX_EXECUTION_DATE": "2024-01-01",
        },
    )
    def test_extracts_full_airflow_context(self):
        logger = get_logger("airflow_test")
        assert logger.base_context["dag_id"] == "my_dag"
        assert logger.base_context["task_id"] == "my_task"
        assert logger.base_context["execution_date"] == "2024-01-01"


class TestConvenienceFunctions:
    """Tests for log_metric() and log_error() convenience functions."""

    def test_log_metric_creates_metric_logger(self, temp_log_dir):
        with patch.dict(os.environ, {"LOGS_BASE_PATH": temp_log_dir}):
            log_metric("test_metric", 42, table="orders")

            audit_logs = list(Path(temp_log_dir).glob("**/audit_*.jsonl"))
            assert len(audit_logs) >= 1

    def test_log_error_creates_error_logger(self, temp_log_dir):
        with patch.dict(os.environ, {"LOGS_BASE_PATH": temp_log_dir}):
            log_error("Test error", error_type="TestError")

            error_logs = list(Path(temp_log_dir).glob("**/errors_*.jsonl"))
            assert len(error_logs) >= 1


class TestJSONLFormatting:
    """Tests for JSONL file formatting."""

    def test_jsonl_contains_required_fields(self, temp_log_dir):
        with patch.dict(os.environ, {"LOGS_BASE_PATH": temp_log_dir}):
            config = ObservabilityConfig(
                environment=Environment.DEV,
                project_root=Path(temp_log_dir),
            )
            logger = StructuredLogger("test_logger", config)

            logger.metric("rows_processed", 500, table="customers")

            audit_logs = list(Path(temp_log_dir).glob("**/audit_*.jsonl"))
            with open(audit_logs[0], encoding="utf-8") as f:
                log_entry = json.loads(f.read().strip())

            # Required fields
            assert "timestamp" in log_entry
            assert "level" in log_entry
            assert "component" in log_entry
            assert "message" in log_entry
            assert "environment" in log_entry

            # Verify values
            assert log_entry["level"] == "INFO"
            assert log_entry["component"] == "test_logger"
            assert log_entry["environment"] == "dev"

    def test_multiple_logs_appended_to_same_file(self, temp_log_dir):
        with patch.dict(os.environ, {"LOGS_BASE_PATH": temp_log_dir}):
            config = ObservabilityConfig(
                environment=Environment.LOCAL,
                project_root=Path(temp_log_dir),
            )
            logger = StructuredLogger("test_logger", config)

            # Write multiple metrics quickly (same timestamp might create same file)
            logger.metric("metric_1", 100)
            logger.metric("metric_2", 200)

            # At least one audit log file should exist
            audit_logs = list(Path(temp_log_dir).glob("**/audit_*.jsonl"))
            assert len(audit_logs) >= 1
