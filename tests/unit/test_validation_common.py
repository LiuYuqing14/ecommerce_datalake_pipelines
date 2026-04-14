"""Tests for validation common utilities."""

from __future__ import annotations

import io
import json
from pathlib import Path

import polars as pl

from src.validation.common import (
    ValidationStatus,
    collect_parquet_files,
    count_parquet_rows,
    get_overall_status,
    handle_exit,
    is_gcs_path,
    is_parquet_file,
    list_partitions,
    read_parquet_safe,
)


class TestValidationStatus:
    def test_status_constants(self) -> None:
        assert ValidationStatus.PASS == "PASS"
        assert ValidationStatus.WARN == "WARN"
        assert ValidationStatus.FAIL == "FAIL"


class TestIsGcsPath:
    def test_gcs_path_returns_true(self) -> None:
        assert is_gcs_path("gs://my-bucket/path/to/file") is True

    def test_local_path_returns_false(self) -> None:
        assert is_gcs_path("/local/path/to/file") is False
        assert is_gcs_path("relative/path") is False

    def test_s3_path_returns_false(self) -> None:
        assert is_gcs_path("s3://bucket/path") is False


class TestIsParquetFile:
    def test_valid_parquet_file(self, tmp_path: Path) -> None:
        # Create a valid parquet file
        df = pl.DataFrame({"a": [1, 2, 3]})
        parquet_path = tmp_path / "valid.parquet"
        df.write_parquet(parquet_path)

        assert is_parquet_file(parquet_path) is True

    def test_invalid_file_returns_false(self, tmp_path: Path) -> None:
        # Create a non-parquet file
        invalid_path = tmp_path / "invalid.parquet"
        invalid_path.write_text("not a parquet file")

        assert is_parquet_file(invalid_path) is False

    def test_nonexistent_file_returns_false(self, tmp_path: Path) -> None:
        nonexistent = tmp_path / "does_not_exist.parquet"
        assert is_parquet_file(nonexistent) is False


class TestCollectParquetFiles:
    def test_collects_valid_parquet_files(self, tmp_path: Path) -> None:
        # Create valid parquet files
        df = pl.DataFrame({"x": [1, 2]})
        (tmp_path / "subdir").mkdir()
        df.write_parquet(tmp_path / "file1.parquet")
        df.write_parquet(tmp_path / "subdir" / "file2.parquet")

        files = collect_parquet_files(tmp_path)
        assert len(files) == 2

    def test_skips_invalid_parquet_files(self, tmp_path: Path) -> None:
        # Create one valid and one invalid
        df = pl.DataFrame({"x": [1]})
        df.write_parquet(tmp_path / "valid.parquet")
        (tmp_path / "invalid.parquet").write_text("not parquet")

        files = collect_parquet_files(tmp_path)
        assert len(files) == 1
        assert Path(files[0]).name == "valid.parquet"

    def test_empty_directory_returns_empty(self, tmp_path: Path) -> None:
        files = collect_parquet_files(tmp_path)
        assert files == []

    def test_collects_from_gcs_manifest(self, monkeypatch) -> None:
        manifest = {
            "files": ["ingest_dt=2020-01-01/part-0000.parquet"],
        }

        class FakeFS:
            def exists(self, path: str) -> bool:
                return path.endswith("/_MANIFEST.json")

            def open(self, path: str, mode: str = "r"):
                return io.StringIO(json.dumps(manifest))

            def glob(self, pattern: str):
                raise AssertionError("glob should not be called when manifest exists")

        monkeypatch.setattr(
            "src.validation.common.get_gcs_filesystem", lambda: FakeFS()
        )

        files = collect_parquet_files("gs://bucket/table")
        assert files == ["gs://bucket/table/ingest_dt=2020-01-01/part-0000.parquet"]


class TestCountParquetRows:
    def test_counts_rows_across_files(self, tmp_path: Path) -> None:
        df1 = pl.DataFrame({"a": [1, 2, 3]})
        df2 = pl.DataFrame({"a": [4, 5]})
        df1.write_parquet(tmp_path / "part1.parquet")
        df2.write_parquet(tmp_path / "part2.parquet")

        count = count_parquet_rows(tmp_path)
        assert count == 5

    def test_nonexistent_path_returns_zero(self, tmp_path: Path) -> None:
        count = count_parquet_rows(tmp_path / "nonexistent")
        assert count == 0

    def test_empty_directory_returns_zero(self, tmp_path: Path) -> None:
        count = count_parquet_rows(tmp_path)
        assert count == 0


class TestReadParquetSafe:
    def test_reads_valid_parquet(self, tmp_path: Path) -> None:
        df = pl.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        df.write_parquet(tmp_path / "data.parquet")

        result = read_parquet_safe(tmp_path)
        assert result is not None
        assert result.height == 2
        assert set(result.columns) == {"col1", "col2"}

    def test_reads_specific_columns(self, tmp_path: Path) -> None:
        df = pl.DataFrame({"col1": [1, 2], "col2": ["a", "b"], "col3": [True, False]})
        df.write_parquet(tmp_path / "data.parquet")

        result = read_parquet_safe(tmp_path, columns=["col1", "col3"])
        assert result is not None
        assert set(result.columns) == {"col1", "col3"}

    def test_empty_directory_returns_none(self, tmp_path: Path) -> None:
        result = read_parquet_safe(tmp_path)
        assert result is None

    def test_fallback_reads_per_file(self, tmp_path: Path, monkeypatch) -> None:
        df = pl.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
        df.write_parquet(tmp_path / "data.parquet")

        original_read = pl.read_parquet

        def fake_read_parquet(source, *args, **kwargs):
            if isinstance(source, list):
                raise pl.exceptions.ComputeError("forced")
            return original_read(source, *args, **kwargs)

        monkeypatch.setattr(pl, "read_parquet", fake_read_parquet)

        result = read_parquet_safe(tmp_path)
        assert result is not None
        assert result.height == 2


class TestListPartitions:
    def test_lists_partition_values(self, tmp_path: Path) -> None:
        # Create partition directories
        (tmp_path / "date=2024-01-01").mkdir()
        (tmp_path / "date=2024-01-02").mkdir()
        (tmp_path / "date=2024-01-03").mkdir()

        partitions = list_partitions(tmp_path, "date")
        assert partitions == ["2024-01-01", "2024-01-02", "2024-01-03"]

    def test_ignores_files(self, tmp_path: Path) -> None:
        (tmp_path / "date=2024-01-01").mkdir()
        (tmp_path / "date=2024-01-02").write_text("not a dir")

        partitions = list_partitions(tmp_path, "date")
        assert partitions == ["2024-01-01"]

    def test_empty_returns_empty_list(self, tmp_path: Path) -> None:
        partitions = list_partitions(tmp_path, "date")
        assert partitions == []

    def test_different_partition_key(self, tmp_path: Path) -> None:
        (tmp_path / "ingest_dt=2024-01-01").mkdir()
        (tmp_path / "date=2024-01-02").mkdir()

        partitions = list_partitions(tmp_path, "ingest_dt")
        assert partitions == ["2024-01-01"]


class TestGetOverallStatus:
    def test_all_pass_returns_pass(self) -> None:
        statuses = ["PASS", "PASS", "PASS"]
        assert get_overall_status(statuses) == "PASS"

    def test_any_fail_returns_fail(self) -> None:
        statuses = ["PASS", "FAIL", "PASS"]
        assert get_overall_status(statuses) == "FAIL"

    def test_warn_without_fail_returns_warn(self) -> None:
        statuses = ["PASS", "WARN", "PASS"]
        assert get_overall_status(statuses) == "WARN"

    def test_fail_takes_precedence_over_warn(self) -> None:
        statuses = ["WARN", "FAIL", "PASS"]
        assert get_overall_status(statuses) == "FAIL"

    def test_empty_list_returns_pass(self) -> None:
        assert get_overall_status([]) == "PASS"


class TestHandleExit:
    def test_fail_with_enforce_returns_1(self) -> None:
        assert handle_exit("FAIL", enforce=True, env="dev") == 1

    def test_fail_in_prod_returns_1(self) -> None:
        assert handle_exit("FAIL", enforce=False, env="prod") == 1

    def test_fail_without_enforce_in_dev_returns_0(self) -> None:
        assert handle_exit("FAIL", enforce=False, env="dev") == 0

    def test_pass_returns_0(self) -> None:
        assert handle_exit("PASS", enforce=True, env="prod") == 0

    def test_warn_returns_0(self) -> None:
        assert handle_exit("WARN", enforce=True, env="prod") == 0
