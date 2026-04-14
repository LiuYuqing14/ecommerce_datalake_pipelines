"""Tests for settings and config validation."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from src.settings import (
    ConfigValidationError,
    PipelineConfig,
    SemanticCheck,
    Settings,
    ValidationConfig,
    load_settings,
    validate_config,
)


class TestSemanticCheck:
    def test_valid_semantic_check(self) -> None:
        check = SemanticCheck(name="valid_check", expr="column > 0")
        assert check.name == "valid_check"
        assert check.expr == "column > 0"

    def test_name_must_be_lowercase(self) -> None:
        with pytest.raises(ValueError, match="must be lowercase"):
            SemanticCheck(name="InvalidName", expr="x > 0")

    def test_name_must_start_with_letter(self) -> None:
        with pytest.raises(ValueError, match="must be lowercase"):
            SemanticCheck(name="123invalid", expr="x > 0")

    def test_expr_rejects_drop(self) -> None:
        with pytest.raises(ValueError, match="forbidden keyword: drop"):
            SemanticCheck(name="bad_check", expr="drop table users")

    def test_expr_rejects_delete(self) -> None:
        with pytest.raises(ValueError, match="forbidden keyword: delete"):
            SemanticCheck(name="bad_check", expr="delete from users")

    def test_expr_allows_safe_sql(self) -> None:
        check = SemanticCheck(name="rate_check", expr="rate > 0 and rate <= 1")
        assert "rate" in check.expr


class TestValidationConfig:
    def test_valid_sanity_checks(self) -> None:
        config = ValidationConfig(
            sanity_checks={
                "non_negative": ["col1", "col2"],
                "rate_0_1": ["rate_col"],
            }
        )
        assert "non_negative" in config.sanity_checks

    def test_invalid_sanity_check_type_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid sanity check type"):
            ValidationConfig(
                sanity_checks={
                    "invalid_type": ["col1"],
                }
            )

    def test_semantic_check_missing_name(self) -> None:
        with pytest.raises(ValueError, match="missing 'name' field"):
            ValidationConfig(semantic_checks={"table1": [{"expr": "x > 0"}]})

    def test_semantic_check_missing_expr(self) -> None:
        with pytest.raises(ValueError, match="missing 'expr' field"):
            ValidationConfig(semantic_checks={"table1": [{"name": "check1"}]})


class TestPipelineConfig:
    def test_valid_environment(self) -> None:
        config = PipelineConfig(
            project_id="test-project",
            bronze_bucket="bronze",
            silver_bucket="silver",
            environment="prod",
        )
        assert config.environment == "prod"

    def test_invalid_environment_rejected(self) -> None:
        with pytest.raises(ValueError, match="must be one of"):
            PipelineConfig(
                project_id="test-project",
                bronze_bucket="bronze",
                silver_bucket="silver",
                environment="staging",
            )

    def test_valid_ingest_dt_format(self) -> None:
        config = PipelineConfig(
            project_id="test-project",
            bronze_bucket="bronze",
            silver_bucket="silver",
            default_ingest_dt="2024-01-15",
        )
        assert config.default_ingest_dt == "2024-01-15"

    def test_invalid_ingest_dt_format_rejected(self) -> None:
        with pytest.raises(ValueError, match="YYYY-MM-DD format"):
            PipelineConfig(
                project_id="test-project",
                bronze_bucket="bronze",
                silver_bucket="silver",
                default_ingest_dt="01-15-2024",
            )

    def test_sla_threshold_in_range(self) -> None:
        config = PipelineConfig(
            project_id="test-project",
            bronze_bucket="bronze",
            silver_bucket="silver",
            sla_thresholds={"orders": 0.95},
        )
        assert config.sla_thresholds["orders"] == 0.95

    def test_sla_threshold_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="between 0 and 1"):
            PipelineConfig(
                project_id="test-project",
                bronze_bucket="bronze",
                silver_bucket="silver",
                sla_thresholds={"orders": 1.5},
            )

    def test_percentage_in_range(self) -> None:
        config = PipelineConfig(
            project_id="test-project",
            bronze_bucket="bronze",
            silver_bucket="silver",
            max_quarantine_pct=5.0,
        )
        assert config.max_quarantine_pct == 5.0

    def test_percentage_out_of_range_rejected(self) -> None:
        with pytest.raises(ValueError, match="between 0 and 100"):
            PipelineConfig(
                project_id="test-project",
                bronze_bucket="bronze",
                silver_bucket="silver",
                max_quarantine_pct=150.0,
            )

    def test_invalid_publish_mode_rejected(self) -> None:
        with pytest.raises(ValueError, match="must be one of"):
            PipelineConfig(
                project_id="test-project",
                bronze_bucket="bronze",
                silver_bucket="silver",
                silver_publish_mode="invalid_mode",
            )


class TestSettingsFromYaml:
    def test_loads_valid_yaml(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yml"
        config_file.write_text(
            yaml.dump(
                {
                    "pipeline": {
                        "project_id": "test-project",
                        "bronze_bucket": "bronze-bucket",
                        "silver_bucket": "silver-bucket",
                        "environment": "local",
                    }
                }
            )
        )

        settings = Settings.from_yaml(config_file)
        assert settings.pipeline.project_id == "test-project"

    def test_strict_mode_raises_on_invalid(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yml"
        config_file.write_text(
            yaml.dump(
                {
                    "pipeline": {
                        "project_id": "test-project",
                        "bronze_bucket": "bronze",
                        "silver_bucket": "silver",
                        "environment": "invalid_env",
                    }
                }
            )
        )

        with pytest.raises(ConfigValidationError):
            Settings.from_yaml(config_file, strict=True)

    def test_non_strict_mode_falls_back(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yml"
        config_file.write_text(
            yaml.dump(
                {
                    "pipeline": {
                        "project_id": "test-project",
                        "bronze_bucket": "bronze",
                        "silver_bucket": "silver",
                        "environment": "invalid_env",
                    }
                }
            )
        )

        # Should not raise, falls back to defaults
        settings = Settings.from_yaml(config_file, strict=False)
        assert settings.pipeline.environment == "local"

    def test_missing_file_strict_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigValidationError, match="Failed to read"):
            Settings.from_yaml(tmp_path / "nonexistent.yml", strict=True)


class TestValidateConfig:
    def test_valid_config_returns_empty_list(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yml"
        config_file.write_text(
            yaml.dump(
                {
                    "pipeline": {
                        "project_id": "test-project",
                        "bronze_bucket": "bronze",
                        "silver_bucket": "silver",
                    }
                }
            )
        )

        issues = validate_config(config_file)
        assert issues == []

    def test_invalid_config_returns_issues(self, tmp_path: Path) -> None:
        config_file = tmp_path / "config.yml"
        config_file.write_text(
            yaml.dump(
                {
                    "pipeline": {
                        "project_id": "test-project",
                        "bronze_bucket": "bronze",
                        "silver_bucket": "silver",
                        "environment": "bad_env",
                    }
                }
            )
        )

        issues = validate_config(config_file)
        assert len(issues) == 1
        assert "must be one of" in issues[0]


class TestLoadSettings:
    def test_loads_from_default_path(self) -> None:
        # This tests the actual config/config.yml
        settings = load_settings()
        assert settings.pipeline.project_id is not None

    def test_loads_from_custom_path(self, tmp_path: Path) -> None:
        config_file = tmp_path / "custom.yml"
        config_file.write_text(
            yaml.dump(
                {
                    "pipeline": {
                        "project_id": "custom-project",
                        "bronze_bucket": "bronze",
                        "silver_bucket": "silver",
                    }
                }
            )
        )

        settings = load_settings(config_file)
        assert settings.pipeline.project_id == "custom-project"
