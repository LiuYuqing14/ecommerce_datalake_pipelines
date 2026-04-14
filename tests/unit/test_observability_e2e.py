import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.observability import audit, config


def test_audit_record_serialization():
    record = audit.AuditRecord(
        run_id="test_run",
        table="orders",
        ingest_dt="2025-10-01",
        partition_key="ingest_dt",
        partition_value="2025-10-01",
        schema_version="1.0",
        input_rows=100,
        output_rows=95,
        reject_rows=5,
        started_at=datetime.now(timezone.utc).isoformat(),
        ended_at=datetime.now(timezone.utc).isoformat(),
        duration_sec=1.5,
        quality_failures=["null_id"],
    )

    js = record.to_json()
    data = json.loads(js)
    assert data["run_id"] == "test_run"
    assert data["table"] == "orders"
    assert "quality_failures" in data


def test_write_audit_file_local(tmp_path):
    record = audit.AuditRecord(
        run_id="test_run",
        table="orders",
        ingest_dt="2025-10-01",
        partition_key="ingest_dt",
        partition_value="2025-10-01",
        schema_version="1.0",
        input_rows=10,
        output_rows=10,
        reject_rows=0,
        started_at="2025-10-01T00:00:00",
        ended_at="2025-10-01T00:00:01",
        duration_sec=1.0,
        quality_failures=[],
    )

    audit_file = tmp_path / "audit.json"
    audit.write_audit_file(audit_file, record)

    assert audit_file.exists()
    content = json.loads(audit_file.read_text())
    assert content["table"] == "orders"


def test_observability_config_resolution(monkeypatch):
    monkeypatch.setenv("PIPELINE_ENV", "prod")
    monkeypatch.setenv("OBSERVABILITY_ENV", "prod")
    monkeypatch.setenv("REPORTS_BUCKET", "my-reports")
    monkeypatch.setenv("USE_SA_AUTH", "false")  # Skip real auth checks

    # Mock load_settings to return a config that matches our environment variables
    with (
        patch("src.settings.load_settings") as mock_load,
        patch("src.observability.config._cloud_auth_available", return_value=True),
    ):
        mock_config = MagicMock()
        mock_config.pipeline.environment = "prod"
        mock_config.pipeline.reports_bucket = "my-reports"
        mock_load.return_value = mock_config

        cfg = config.ObservabilityConfig.from_env()
        assert cfg.environment == config.Environment.PROD
        assert cfg.reports_bucket == "my-reports"

        path = cfg.get_run_report_path("run123", "report.md")
        assert "gs://my-reports/validation_reports/run123/report.md" == path


def test_metrics_writer_local(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPELINE_ENV", "local")
    monkeypatch.setenv("METRICS_BASE_PATH", str(tmp_path))

    # Correct import location
    writer = config.get_metrics_writer("data_quality")
    # Fix argument order: data (dict) comes first, then run_id (str)
    writer.write_metric({"status": "PASS"}, "run123")

    # Check if file exists in the expected subfolder
    # Base path / metric_type / metric_type_runid.json
    expected_file = tmp_path / "data_quality" / "data_quality_run123.json"
    assert expected_file.exists()
