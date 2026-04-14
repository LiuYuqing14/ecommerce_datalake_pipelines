"""Unit tests for src/runners/enriched/shared.py."""

import os
import tempfile
from datetime import date, datetime
from pathlib import Path

import polars as pl

from src.runners.enriched.shared import (
    ensure_output_dir,
    get_enriched_partitions,
    get_silver_table_partitions,
    is_gcs_path,
    list_partitions,
    normalize_partition_values,
    normalize_schema,
    output_file,
    partition_range,
    write_sharded_parquet,
)


class TestIsGcsPath:
    """Tests for is_gcs_path()."""

    def test_gcs_path_returns_true(self):
        assert is_gcs_path("gs://bucket/path/to/data")

    def test_local_path_returns_false(self):
        assert not is_gcs_path("/local/path/to/data")

    def test_relative_path_returns_false(self):
        assert not is_gcs_path("relative/path")

    def test_s3_path_returns_false(self):
        assert not is_gcs_path("s3://bucket/key")


class TestNormalizeSchema:
    """Tests for normalize_schema()."""

    def test_none_schema_returns_none(self):
        assert normalize_schema("orders", None) is None

    def test_product_catalog_adds_category(self):
        schema = {"product_id": pl.String(), "name": pl.String()}
        result = normalize_schema("product_catalog", schema)
        assert "category" in result
        assert result["category"] == pl.String()

    def test_product_catalog_preserves_existing_category(self):
        schema = {
            "product_id": pl.String(),
            "category": pl.String(),
        }
        result = normalize_schema("product_catalog", schema)
        assert result["category"] == pl.String()

    def test_non_product_catalog_preserves_schema(self):
        schema = {"order_id": pl.String(), "total": pl.Float64()}
        result = normalize_schema("orders", schema)
        assert result == schema


class TestPartitionRange:
    """Tests for partition_range()."""

    def test_single_day_range(self):
        result = partition_range("2024-01-01", 0)
        assert result == ["2024-01-01"]

    def test_three_day_range(self):
        result = partition_range("2024-01-03", 2)
        assert result == ["2024-01-01", "2024-01-02", "2024-01-03"]

    def test_seven_day_range(self):
        result = partition_range("2024-01-07", 6)
        expected = [
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "2024-01-06",
            "2024-01-07",
        ]
        assert result == expected

    def test_negative_lookback_treated_as_zero(self):
        result = partition_range("2024-01-01", -5)
        assert result == ["2024-01-01"]


class TestOutputFile:
    """Tests for output_file()."""

    def test_appends_part_filename(self):
        assert output_file("/path/to/output") == "/path/to/output/part-00000.parquet"

    def test_strips_trailing_slash(self):
        assert output_file("/path/to/output/") == "/path/to/output/part-00000.parquet"


class TestEnsureOutputDir:
    """Tests for ensure_output_dir()."""

    def test_gcs_path_does_nothing(self):
        # Should not raise
        ensure_output_dir("gs://bucket/path")

    def test_creates_local_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            test_path = os.path.join(tmpdir, "nested", "directory")
            ensure_output_dir(test_path)
            assert os.path.isdir(test_path)

    def test_idempotent_on_existing_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ensure_output_dir(tmpdir)
            assert os.path.isdir(tmpdir)


class TestNormalizePartitionValues:
    """Tests for normalize_partition_values()."""

    def test_date_column_converts_to_string(self):
        df = pl.DataFrame(
            {
                "order_id": ["A", "B"],
                "ingest_dt": [date(2024, 1, 1), date(2024, 1, 2)],
            }
        )
        result = normalize_partition_values(df, "ingest_dt")
        assert result.schema["ingest_dt"] == pl.Date
        assert result["ingest_dt"].to_list() == [date(2024, 1, 1), date(2024, 1, 2)]

    def test_string_column_unchanged(self):
        df = pl.DataFrame({"order_id": ["A", "B"], "region": ["US", "EU"]})
        result = normalize_partition_values(df, "region")
        assert result["region"].to_list() == ["US", "EU"]

    def test_datetime_column_casts_to_date(self):
        df = pl.DataFrame(
            {
                "order_id": ["A", "B"],
                "ingest_dt": [
                    datetime(2024, 1, 1, 8, 30, 0),
                    datetime(2024, 1, 2, 12, 0, 0),
                ],
            }
        )
        result = normalize_partition_values(df, "ingest_dt")
        assert result.schema["ingest_dt"] == pl.Date
        assert result["ingest_dt"].to_list() == [date(2024, 1, 1), date(2024, 1, 2)]

    def test_utf8_date_strings_parse_when_valid(self):
        df = pl.DataFrame(
            {
                "order_id": ["A", "B"],
                "ingest_dt": ["2024-01-01", "2024-01-02"],
            }
        )
        result = normalize_partition_values(df, "ingest_dt")
        assert result.schema["ingest_dt"] == pl.Date
        assert result["ingest_dt"].to_list() == [date(2024, 1, 1), date(2024, 1, 2)]

    def test_utf8_with_invalid_dates_returns_unchanged(self):
        df = pl.DataFrame(
            {
                "order_id": ["A", "B"],
                "ingest_dt": ["2024-01-01", "not-a-date"],
            }
        )
        result = normalize_partition_values(df, "ingest_dt")
        assert result.schema["ingest_dt"] == pl.Utf8
        assert result["ingest_dt"].to_list() == ["2024-01-01", "not-a-date"]

    def test_missing_partition_column_returns_unchanged(self):
        df = pl.DataFrame({"order_id": ["A", "B"]})
        result = normalize_partition_values(df, "nonexistent")
        assert result.equals(df)


class TestWriteShardedParquet:
    """Tests for write_sharded_parquet()."""

    def test_writes_single_file_when_below_threshold(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
            write_sharded_parquet(df, tmpdir, max_rows_per_file=10)

            files = list(Path(tmpdir).glob("*.parquet"))
            assert len(files) == 1
            assert files[0].name == "part-00000.parquet"

            # Verify data integrity
            result = pl.read_parquet(files[0])
            assert result.equals(df)

    def test_shards_when_exceeds_threshold(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pl.DataFrame({"id": range(10), "value": list("abcdefghij")})
            write_sharded_parquet(df, tmpdir, max_rows_per_file=3)

            files = sorted(Path(tmpdir).glob("*.parquet"))
            assert len(files) == 4  # 3 + 3 + 3 + 1 = 10 rows
            assert files[0].name == "part-00000.parquet"
            assert files[3].name == "part-00003.parquet"

            # Verify all data preserved
            result = pl.concat([pl.read_parquet(f) for f in files])
            assert result.height == 10

    def test_handles_empty_dataframe(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pl.DataFrame(schema={"id": pl.Int64, "value": pl.String})
            write_sharded_parquet(df, tmpdir, max_rows_per_file=10)

            files = list(Path(tmpdir).glob("*.parquet"))
            assert len(files) == 1

            result = pl.read_parquet(files[0])
            assert result.height == 0
            assert result.schema == df.schema

    def test_max_rows_zero_writes_single_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pl.DataFrame({"id": range(100)})
            write_sharded_parquet(df, tmpdir, max_rows_per_file=0)

            files = list(Path(tmpdir).glob("*.parquet"))
            assert len(files) == 1

    def test_strips_trailing_slash_from_output_uri(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            df = pl.DataFrame({"id": [1]})
            write_sharded_parquet(df, f"{tmpdir}/", max_rows_per_file=10)

            files = list(Path(tmpdir).glob("*.parquet"))
            assert len(files) == 1


class TestListPartitions:
    """Tests for list_partitions() with local paths."""

    def test_returns_empty_for_nonexistent_table(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = list_partitions(tmpdir, "nonexistent", "ingest_dt")
            assert result == []

    def test_lists_local_partitions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create partition directories
            os.makedirs(f"{tmpdir}/orders/ingest_dt=2024-01-01")
            os.makedirs(f"{tmpdir}/orders/ingest_dt=2024-01-03")
            os.makedirs(f"{tmpdir}/orders/ingest_dt=2024-01-02")

            result = list_partitions(tmpdir, "orders", "ingest_dt")
            assert result == ["2024-01-01", "2024-01-02", "2024-01-03"]

    def test_ignores_non_partition_directories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            os.makedirs(f"{tmpdir}/orders/ingest_dt=2024-01-01")
            os.makedirs(f"{tmpdir}/orders/other_dir")
            os.makedirs(f"{tmpdir}/orders/ingest_dt=2024-01-02")

            result = list_partitions(tmpdir, "orders", "ingest_dt")
            assert result == ["2024-01-01", "2024-01-02"]


class TestGetEnrichedPartitions:
    """Tests for get_enriched_partitions()."""

    def test_returns_dict_from_spec_or_settings(self):
        # This test validates the function runs without error
        # Actual values depend on spec/settings configuration
        result = get_enriched_partitions()
        assert isinstance(result, dict)


class TestGetSilverTablePartitions:
    """Tests for get_silver_table_partitions()."""

    def test_returns_dict_from_spec_or_settings(self):
        result = get_silver_table_partitions()
        assert isinstance(result, dict)
