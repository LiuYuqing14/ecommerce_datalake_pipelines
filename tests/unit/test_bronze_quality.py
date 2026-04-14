from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.validation import bronze_quality
from src.validation.bronze_quality import (
    bronze_qa_fail,
    bronze_qa_required,
    get_bronze_partitions,
    get_partition_glob,
    list_partitions,
    list_tables,
)


def _write_manifest(path: Path, rows: int = 1) -> None:
    manifest_path = path / "_MANIFEST.json"
    manifest_path.write_text(json.dumps({"total_rows": rows}))


def test_bronze_quality_passes_with_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table_path = tmp_path / "orders" / "ingest_dt=2020-01-01"
    table_path.mkdir(parents=True)
    _write_manifest(table_path, rows=10)

    monkeypatch.setenv("PIPELINE_ENV", "local")
    monkeypatch.setattr(
        bronze_quality,
        "parse_args",
        lambda: bronze_quality.argparse.Namespace(
            config="config/config.yml",
            bronze_path=str(tmp_path),
            fail_on_issues=True,
            partition_date=None,
            lookback_days=0,
            run_id="test",
            output_report=str(tmp_path / "report.md"),
            tables=None,
            enforce_quality=True,
            spec_path=None,
        ),
    )
    assert bronze_quality.main() == 0


def test_bronze_quality_fails_without_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    table_path = tmp_path / "orders" / "ingest_dt=2020-01-01"
    table_path.mkdir(parents=True)

    monkeypatch.setenv("PIPELINE_ENV", "local")
    monkeypatch.setattr(
        bronze_quality,
        "parse_args",
        lambda: bronze_quality.argparse.Namespace(
            config="config/config.yml",
            bronze_path=str(tmp_path),
            fail_on_issues=True,
            partition_date=None,
            lookback_days=0,
            run_id="test",
            output_report=str(tmp_path / "report.md"),
            tables=None,
            enforce_quality=True,
            spec_path=None,
        ),
    )
    assert bronze_quality.main() == 1


class TestGetBronzePartitions:
    """Tests for get_bronze_partitions()."""

    def test_returns_dict_from_spec_or_settings(self):
        result = get_bronze_partitions()
        assert isinstance(result, dict)
        assert len(result) >= 0


class TestGetPartitionGlob:
    """Tests for get_partition_glob()."""

    def test_returns_glob_pattern_for_orders(self):
        result = get_partition_glob("orders")
        assert "=" in result
        assert "*" in result

    def test_defaults_to_ingest_dt_for_unknown_table(self):
        result = get_partition_glob("nonexistent_table")
        assert result == "ingest_dt=*"


class TestBronzeQaRequired:
    """Tests for bronze_qa_required()."""

    def test_returns_true_when_env_override_set_to_true(self):
        with patch.dict(os.environ, {"BRONZE_QA_REQUIRED": "true"}, clear=False):
            assert bronze_qa_required() is True

    def test_returns_false_when_env_override_set_to_false(self):
        with patch.dict(os.environ, {"BRONZE_QA_REQUIRED": "false"}, clear=False):
            assert bronze_qa_required() is False

    def test_returns_true_for_prod_environment(self):
        with patch.dict(
            os.environ, {"PIPELINE_ENV": "prod", "BRONZE_QA_REQUIRED": ""}, clear=False
        ):
            os.environ.pop("BRONZE_QA_REQUIRED", None)
            result = bronze_qa_required()
            assert result is True


class TestBronzeQaFail:
    """Tests for bronze_qa_fail()."""

    def test_returns_true_when_env_override_set(self):
        with patch.dict(os.environ, {"BRONZE_QA_FAIL": "true"}, clear=False):
            assert bronze_qa_fail() is True

    def test_returns_false_when_env_override_not_set(self):
        with patch.dict(os.environ, {"BRONZE_QA_FAIL": "false"}, clear=False):
            assert bronze_qa_fail() is False


class TestListTables:
    """Tests for list_tables() with local paths."""

    def test_lists_table_directories(self, tmp_path: Path):
        (tmp_path / "orders").mkdir()
        (tmp_path / "customers").mkdir()
        (tmp_path / "products").mkdir()

        result = list_tables(str(tmp_path))
        assert result == ["customers", "orders", "products"]

    def test_returns_empty_list_for_empty_directory(self, tmp_path: Path):
        result = list_tables(str(tmp_path))
        assert result == []

    def test_ignores_files_in_root(self, tmp_path: Path):
        (tmp_path / "orders").mkdir()
        (tmp_path / "README.md").touch()

        result = list_tables(str(tmp_path))
        assert result == ["orders"]


class TestListPartitions:
    """Tests for list_partitions() with local paths."""

    def test_lists_partition_directories(self, tmp_path: Path):
        table_path = tmp_path / "orders"
        table_path.mkdir()
        (table_path / "ingest_dt=2024-01-01").mkdir()
        (table_path / "ingest_dt=2024-01-02").mkdir()
        (table_path / "ingest_dt=2024-01-03").mkdir()

        result = list_partitions(str(tmp_path), "orders", None)
        assert len(result) >= 3

    def test_filters_by_partition_values(self, tmp_path: Path):
        table_path = tmp_path / "orders"
        table_path.mkdir()
        (table_path / "ingest_dt=2024-01-01").mkdir()
        (table_path / "ingest_dt=2024-01-02").mkdir()
        (table_path / "ingest_dt=2024-01-03").mkdir()

        result = list_partitions(str(tmp_path), "orders", ["2024-01-01", "2024-01-03"])
        assert len(result) == 2
        assert any("2024-01-01" in p for p in result)
        assert any("2024-01-03" in p for p in result)

    def test_returns_empty_for_nonexistent_table(self, tmp_path: Path):
        result = list_partitions(str(tmp_path), "nonexistent", None)
        assert result == []

    def test_returns_empty_when_no_matching_values(self, tmp_path: Path):
        table_path = tmp_path / "orders"
        table_path.mkdir()
        (table_path / "ingest_dt=2024-01-01").mkdir()

        result = list_partitions(str(tmp_path), "orders", ["2024-12-31"])
        assert result == []
