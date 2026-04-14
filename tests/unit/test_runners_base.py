import os
import sys
from unittest.mock import MagicMock, patch

import pytest

from src.runners import base_silver


@pytest.fixture
def mock_dbt_run():
    with patch("src.runners.base_silver.subprocess.run") as mock:
        mock.return_value = MagicMock(returncode=0)
        yield mock


@pytest.fixture
def mock_ensure_dirs():
    with patch("src.runners.base_silver.ensure_local_directories") as mock:
        yield mock


@pytest.fixture
def mock_gcloud_rsync():
    with patch("src.runners.base_silver.gcloud_rsync") as mock:
        yield mock


def test_base_silver_main_local_flow(
    mock_dbt_run, mock_ensure_dirs, monkeypatch, tmp_path
):
    """Test the base_silver runner in local mode."""
    # Set up dummy environment
    monkeypatch.setenv("PIPELINE_ENV", "local")
    monkeypatch.setenv("BRONZE_BASE_PATH", str(tmp_path / "bronze"))
    monkeypatch.setenv("SILVER_BASE_PATH", str(tmp_path / "silver"))

    # Mock sys.argv
    monkeypatch.setattr(sys, "argv", ["base_silver.py", "--select", "stg_orders"])

    # Run main
    base_silver.main()

    # Verify dbt was called with correct args
    # subprocess.run is called inside run_dbt
    args, kwargs = mock_dbt_run.call_args
    cmd = args[0]
    assert "dbt" in cmd
    assert "run" in cmd
    assert "stg_orders" in cmd

    # Verify environment variables were set
    assert os.environ["BRONZE_BASE_PATH"] == str(tmp_path / "bronze")
    assert os.environ["SILVER_BASE_PATH"] == str(tmp_path / "silver")


def test_base_silver_manifest_generation(tmp_path):
    """Test the _MANIFEST.json generation logic."""
    table_path = tmp_path / "orders" / "ingestion_dt=2025-10-01"
    table_path.mkdir(parents=True)

    # Create a dummy parquet file
    import polars as pl

    df = pl.DataFrame({"id": [1, 2, 3]})
    df.write_parquet(table_path / "data.parquet")

    # Run manifest generation
    base_silver.generate_manifest(tmp_path / "orders")

    # Verify manifest exists
    manifest_file = table_path / "_MANIFEST.json"
    assert manifest_file.exists()

    import json

    content = json.loads(manifest_file.read_text())
    assert content["total_rows"] == 3
    assert content["file_count"] == 1
    assert content["files"] == [{"path": "data.parquet", "rows": 3}]


def test_extract_spec_path():
    argv = ["--select", "model1", "--spec-path", "my/spec.yml", "--vars", "{}"]
    spec_path, cleaned = base_silver.extract_spec_path(argv)
    assert spec_path == "my/spec.yml"
    assert "--spec-path" not in cleaned
    assert "my/spec.yml" not in cleaned
    assert "model1" in cleaned


def test_base_silver_staging_publish_uses_staging_path(
    mock_dbt_run, mock_ensure_dirs, mock_gcloud_rsync, monkeypatch, tmp_path
):
    monkeypatch.setenv("PIPELINE_ENV", "prod")
    monkeypatch.setenv("BRONZE_BASE_PATH", str(tmp_path / "bronze"))
    monkeypatch.setenv("SILVER_BASE_PATH", "gs://bucket/silver/base")
    monkeypatch.setenv("SILVER_EXPORT_BASE_PATH", "gs://bucket/silver/base")
    monkeypatch.setenv("SILVER_LOCAL_BASE_PATH", str(tmp_path / "silver"))
    monkeypatch.setenv("SILVER_PUBLISH_MODE", "staging")
    monkeypatch.setenv("SILVER_STAGING_PATH", "gs://bucket/silver/base/_staging/test")
    monkeypatch.setenv("RUN_ID", "test")
    monkeypatch.setenv("BRONZE_SYNC_TABLES", "orders")
    monkeypatch.setattr(sys, "argv", ["base_silver.py", "--select", "stg_orders"])

    table_path = tmp_path / "silver" / "orders" / "ingestion_dt=2020-01-01"
    table_path.mkdir(parents=True)
    (table_path / "data.parquet").write_text("stub")

    with patch("src.runners.base_silver.gcloud_copy_file") as mock_copy:
        base_silver.main()

    assert mock_gcloud_rsync.call_count == 1
    _, kwargs = mock_gcloud_rsync.call_args
    assert kwargs["delete"] is True
    assert mock_gcloud_rsync.call_args.args[1].endswith("/_staging/test/orders")
    assert mock_copy.call_count == 1


def test_base_silver_always_generates_manifest(
    mock_dbt_run, mock_ensure_dirs, monkeypatch, tmp_path
):
    """Test that manifests are generated even for local runs."""
    monkeypatch.setenv("SILVER_BASE_PATH", str(tmp_path / "silver"))
    monkeypatch.setattr(sys, "argv", ["base_silver.py", "--select", "stg_orders"])

    # Simulate output directory
    (tmp_path / "silver" / "orders").mkdir(parents=True)

    with patch("src.runners.base_silver.generate_manifest") as mock_gen:
        base_silver.main()

        # Check if generate_manifest was called for orders
        # Note: STANDARD_TABLES includes "orders"
        mock_gen.assert_any_call(tmp_path / "silver" / "orders")
