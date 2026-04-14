"""Unit tests for src/validation/enriched/metrics.py."""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from src.settings import PipelineConfig, ValidationConfig
from src.validation.enriched.metrics import (
    compute_null_rates,
    evaluate_sanity_checks,
    evaluate_semantic_checks,
    validate_table,
)


class TestComputeNullRates:
    """Tests for compute_null_rates()."""

    def test_no_nulls_returns_zero_rates(self):
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        result = compute_null_rates(df, ["a", "b"])
        assert result == {"a": 0.0, "b": 0.0}

    def test_all_nulls_returns_one(self):
        df = pl.DataFrame({"a": [None, None, None], "b": [None, None, None]})
        result = compute_null_rates(df, ["a", "b"])
        assert result == {"a": 1.0, "b": 1.0}

    def test_partial_nulls_calculates_rate(self):
        df = pl.DataFrame({"a": [1, None, 3], "b": ["x", None, None]})
        result = compute_null_rates(df, ["a", "b"])
        assert result["a"] == pytest.approx(0.333333, rel=1e-5)
        assert result["b"] == pytest.approx(0.666667, rel=1e-5)

    def test_empty_dataframe_returns_zero_rates(self):
        df = pl.DataFrame(schema={"a": pl.Int64, "b": pl.String})
        result = compute_null_rates(df, ["a", "b"])
        assert result == {"a": 0.0, "b": 0.0}

    def test_missing_column_skipped(self):
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = compute_null_rates(df, ["a", "nonexistent"])
        assert result == {"a": 0.0}

    def test_empty_column_list_returns_empty_dict(self):
        df = pl.DataFrame({"a": [1, 2, 3]})
        result = compute_null_rates(df, [])
        assert result == {}


class TestEvaluateSanityChecks:
    """Tests for evaluate_sanity_checks()."""

    def test_no_issues_when_all_valid(self):
        df = pl.DataFrame({"price": [10.0, 20.0, 30.0], "rate": [0.1, 0.5, 0.9]})
        config = ValidationConfig(
            sanity_checks={
                "non_negative": ["price"],
                "rate_0_1": ["rate"],
            }
        )
        result = evaluate_sanity_checks(df, config)
        assert result == []

    def test_detects_negative_values(self):
        df = pl.DataFrame({"price": [10.0, -5.0, 30.0]})
        config = ValidationConfig(
            sanity_checks={"non_negative": ["price"], "rate_0_1": []}
        )
        result = evaluate_sanity_checks(df, config)
        assert len(result) == 1
        assert "price: 1 negative" in result

    def test_detects_rate_out_of_range(self):
        df = pl.DataFrame({"rate": [0.5, 1.2, -0.1]})
        config = ValidationConfig(
            sanity_checks={"non_negative": [], "rate_0_1": ["rate"]}
        )
        result = evaluate_sanity_checks(df, config)
        assert len(result) == 1
        assert "rate: 2 outside_0_1" in result

    def test_skips_missing_columns(self):
        df = pl.DataFrame({"price": [10.0, 20.0]})
        config = ValidationConfig(
            sanity_checks={"non_negative": ["nonexistent"], "rate_0_1": []}
        )
        result = evaluate_sanity_checks(df, config)
        assert result == []

    def test_multiple_issues_reported(self):
        df = pl.DataFrame({"price": [-10.0, 20.0], "rate": [0.5, 1.5]})
        config = ValidationConfig(
            sanity_checks={
                "non_negative": ["price"],
                "rate_0_1": ["rate"],
            }
        )
        result = evaluate_sanity_checks(df, config)
        assert len(result) == 2


class TestEvaluateSemanticChecks:
    """Tests for evaluate_semantic_checks()."""

    @pytest.fixture
    def pipeline_config(self):
        return PipelineConfig(
            project_id="test-project",
            bronze_bucket="test-bronze",
            silver_bucket="test-silver",
            cart_abandoned_min_value=100.0,
            rate_cap_min=0.0,
            rate_cap_max=1.0,
            return_units_max_ratio=2.0,
        )

    def test_no_issues_when_all_pass(self, pipeline_config):
        df = pl.DataFrame({"price": [100.0, 200.0], "quantity": [1, 2]})
        config = ValidationConfig(
            semantic_checks={
                "test_table": [
                    {"name": "price_check", "expr": "price < 0"},
                ]
            }
        )
        result = evaluate_semantic_checks(
            df, "test_table", config, pipeline_config, ratio_epsilon=0.01
        )
        assert result == []

    def test_detects_semantic_violations(self, pipeline_config):
        df = pl.DataFrame({"price": [-10.0, 200.0]})
        config = ValidationConfig(
            semantic_checks={
                "test_table": [
                    {"name": "negative_price", "expr": "price < 0"},
                ]
            }
        )
        result = evaluate_semantic_checks(
            df, "test_table", config, pipeline_config, ratio_epsilon=0.01
        )
        assert len(result) == 1
        assert "negative_price: 1 rows" in result

    def test_handles_sql_syntax_error(self, pipeline_config):
        # Test with an expression that causes syntax error (division operator issue)
        df = pl.DataFrame({"a": [1.0], "b": [2.0]})
        config = ValidationConfig(
            semantic_checks={
                "test_table": [
                    {
                        "name": "syntax_check",
                        "expr": "a / / b > 1",
                    },  # Double division operator
                ]
            }
        )
        result = evaluate_semantic_checks(
            df, "test_table", config, pipeline_config, ratio_epsilon=0.01
        )
        # Should handle the error gracefully
        assert len(result) >= 1
        assert any("syntax_check:" in r for r in result)

    def test_empty_checks_returns_empty_list(self, pipeline_config):
        df = pl.DataFrame({"price": [100.0]})
        config = ValidationConfig(semantic_checks={})
        result = evaluate_semantic_checks(
            df, "test_table", config, pipeline_config, ratio_epsilon=0.01
        )
        assert result == []

    def test_uses_ratio_epsilon_in_expr(self, pipeline_config):
        df = pl.DataFrame({"a": [1.0], "b": [1.01]})
        config = ValidationConfig(
            semantic_checks={
                "test_table": [
                    {
                        "name": "ratio_check",
                        "expr": "ABS(a - b) > {ratio_epsilon}",
                    },
                ]
            }
        )
        result = evaluate_semantic_checks(
            df, "test_table", config, pipeline_config, ratio_epsilon=0.001
        )
        assert len(result) == 1
        assert "ratio_check: 1 rows" in result


class TestValidateTable:
    """Tests for validate_table()."""

    @pytest.fixture
    def temp_enriched_path(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def pipeline_config(self):
        return PipelineConfig(
            project_id="test-project",
            bronze_bucket="test-bronze",
            silver_bucket="test-silver",
            cart_abandoned_min_value=100.0,
            rate_cap_min=0.0,
            rate_cap_max=1.0,
            return_units_max_ratio=2.0,
            enriched_ratio_epsilon=0.01,
            rate_cap_enabled=False,
            rate_precision=4,
            validation=ValidationConfig(
                key_fields={"test_table": ["id"]},
                sanity_checks={"non_negative": [], "rate_0_1": []},
                semantic_checks={},
            ),
        )

    def test_missing_partition_returns_fail_in_prod(
        self, temp_enriched_path, pipeline_config
    ):
        result = validate_table(
            table="test_table",
            enriched_path=temp_enriched_path,
            ingest_dt="2024-01-01",
            min_rows=1,
            partition_key="ingest_dt",
            pipeline_env="prod",
            settings=pipeline_config,
            lookback_days=0,
        )
        assert result.status == "FAIL"
        assert "missing_partition" in result.notes
        assert result.row_count == 0

    def test_missing_partition_returns_warn_in_local(
        self, temp_enriched_path, pipeline_config
    ):
        result = validate_table(
            table="test_table",
            enriched_path=temp_enriched_path,
            ingest_dt="2024-01-01",
            min_rows=1,
            partition_key="ingest_dt",
            pipeline_env="local",
            settings=pipeline_config,
            lookback_days=0,
        )
        assert result.status == "WARN"
        assert "missing_partition" in result.notes

    def test_empty_partition_returns_warn(self, temp_enriched_path, pipeline_config):
        # Create empty partition
        table_path = temp_enriched_path / "test_table"
        partition_path = table_path / "ingest_dt=2024-01-01"
        partition_path.mkdir(parents=True)

        # Write empty parquet
        df = pl.DataFrame(schema={"id": pl.Int64, "value": pl.String})
        df.write_parquet(partition_path / "part-00000.parquet")

        result = validate_table(
            table="test_table",
            enriched_path=temp_enriched_path,
            ingest_dt="2024-01-01",
            min_rows=1,
            partition_key="ingest_dt",
            pipeline_env="local",
            settings=pipeline_config,
            lookback_days=0,
        )
        assert result.status == "WARN"
        assert "empty_partition" in result.notes
        assert result.row_count == 0

    def test_below_min_rows_returns_warn(self, temp_enriched_path, pipeline_config):
        table_path = temp_enriched_path / "test_table"
        partition_path = table_path / "ingest_dt=2024-01-01"
        partition_path.mkdir(parents=True)

        df = pl.DataFrame({"id": [1], "value": ["a"]})
        df.write_parquet(partition_path / "part-00000.parquet")

        result = validate_table(
            table="test_table",
            enriched_path=temp_enriched_path,
            ingest_dt="2024-01-01",
            min_rows=10,
            partition_key="ingest_dt",
            pipeline_env="local",
            settings=pipeline_config,
            lookback_days=0,
        )
        assert result.status == "WARN"
        assert any("below_min_rows" in note for note in result.notes)
        assert result.row_count == 1

    def test_valid_partition_returns_pass(self, temp_enriched_path, pipeline_config):
        table_path = temp_enriched_path / "test_table"
        partition_path = table_path / "ingest_dt=2024-01-01"
        partition_path.mkdir(parents=True)

        df = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        df.write_parquet(partition_path / "part-00000.parquet")

        result = validate_table(
            table="test_table",
            enriched_path=temp_enriched_path,
            ingest_dt="2024-01-01",
            min_rows=1,
            partition_key="ingest_dt",
            pipeline_env="local",
            settings=pipeline_config,
            lookback_days=0,
        )
        assert result.status == "PASS"
        assert result.row_count == 3
        assert result.notes == []
