"""Typed settings loaded from config files and environment."""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class ConfigValidationError(Exception):
    """Raised when config.yml fails schema validation."""

    pass


class SemanticCheck(BaseModel):
    """Schema for a single semantic check expression."""

    name: str = Field(..., min_length=1, description="Unique name for the check")
    expr: str = Field(..., min_length=1, description="SQL expression to evaluate")

    @field_validator("name")
    @classmethod
    def name_is_valid_identifier(cls, v: str) -> str:
        if not re.match(r"^[a-z][a-z0-9_]*$", v):
            raise ValueError(
                f"Semantic check name '{v}' must be lowercase with underscores"
            )
        return v

    @field_validator("expr")
    @classmethod
    def expr_has_no_dangerous_keywords(cls, v: str) -> str:
        dangerous = ["drop", "delete", "truncate", "insert", "update", "alter"]
        lower_expr = v.lower()
        for keyword in dangerous:
            if re.search(rf"\b{keyword}\b", lower_expr):
                raise ValueError(
                    f"Semantic check expression contains forbidden keyword: {keyword}"
                )
        return v


class RetryConfig(BaseModel):
    """Schema for environment-specific retry configuration."""

    retries: int = Field(ge=0, le=10, description="Number of retry attempts")
    retry_delay_minutes: int = Field(
        ge=1, le=60, description="Initial delay between retries in minutes"
    )
    retry_exponential_backoff: bool = Field(
        default=False, description="Enable exponential backoff for retries"
    )
    max_retry_delay_minutes: int = Field(
        ge=1, le=120, description="Maximum retry delay in minutes"
    )

    @model_validator(mode="after")
    def max_delay_gte_initial_delay(self) -> "RetryConfig":
        """Ensure max_retry_delay_minutes >= retry_delay_minutes."""
        if self.max_retry_delay_minutes < self.retry_delay_minutes:
            raise ValueError(
                f"max_retry_delay_minutes ({self.max_retry_delay_minutes}) must be >= "
                f"retry_delay_minutes ({self.retry_delay_minutes})"
            )
        return self


class ValidationConfig(BaseModel):
    """Schema for validation configuration section."""

    key_fields: dict[str, list[str]] = Field(default_factory=dict)
    sanity_checks: dict[str, list[str]] = Field(default_factory=dict)
    semantic_checks: dict[str, list[dict[str, str]]] = Field(default_factory=dict)

    @field_validator("sanity_checks")
    @classmethod
    def sanity_check_types_valid(cls, v: dict[str, list[str]]) -> dict[str, list[str]]:
        allowed_types = {"non_negative", "rate_0_1"}
        for check_type in v.keys():
            if check_type not in allowed_types:
                raise ValueError(
                    f"Invalid sanity check type '{check_type}'. "
                    f"Allowed: {allowed_types}"
                )
        return v

    @model_validator(mode="after")
    def validate_semantic_checks_schema(self) -> "ValidationConfig":
        """Validate each semantic check has required fields."""
        for table, checks in self.semantic_checks.items():
            for i, check in enumerate(checks):
                if "name" not in check:
                    raise ValueError(
                        f"Semantic check {i} for '{table}' missing 'name' field"
                    )
                if "expr" not in check:
                    raise ValueError(
                        f"Semantic check '{check.get('name', i)}' for '{table}' "
                        "missing 'expr' field"
                    )
                # Validate as SemanticCheck
                SemanticCheck.model_validate(check)
        return self


class PipelineConfig(BaseModel):
    # GCP Configuration
    project_id: str = Field(..., description="GCP project id")

    # Data Lake Buckets
    bronze_bucket: str = Field(..., description="Bronze GCS bucket name")
    bronze_prefix: str = "bronze"
    silver_bucket: str = Field(..., description="Silver GCS bucket name")
    silver_base_prefix: str = "silver/base"
    silver_enriched_prefix: str = "silver/enriched"
    silver_publish_mode: str = Field(
        default="direct",
        description="Silver publish mode: direct or staging",
    )
    silver_enriched_publish_mode: str = Field(
        default="direct",
        description="Enriched publish mode: direct or staging",
    )
    dims_publish_mode: str = Field(
        default="direct",
        description="Dims publish mode: direct or staging",
    )

    # BigQuery Datasets
    bigquery_dataset: str = "silver"
    gold_dataset: str = "gold_marts"

    # Observability & Metrics
    environment: str = Field(
        default="local",
        description="Deployment environment: local, dev, or prod",
    )
    metrics_bucket: str = Field(
        default="ecom-datalake-metrics",
        description="GCS bucket for metrics",
    )
    logs_bucket: str = Field(
        default="ecom-datalake-logs",
        description="GCS bucket for logs",
    )
    reports_bucket: str = Field(
        default="ecom-datalake-reports",
        description="GCS bucket for validation reports",
    )
    retry_config: dict[str, RetryConfig] = Field(
        default_factory=dict,
        description="Environment-specific retry configuration for Airflow tasks",
    )

    # Business Logic Configuration
    default_ingest_dt: str = "2020-01-01"
    attribution_tolerance_hours: int = 48
    churn_danger_window_days: list[int] = Field(default_factory=lambda: [30, 90])
    sales_velocity_window_days: int = 7
    sla_thresholds: dict[str, float] = Field(
        default_factory=lambda: {
            "orders": 0.95,
            "customers": 0.98,
            "product_catalog": 0.99,
            "shopping_carts": 0.95,
            "cart_items": 0.95,
            "order_items": 0.95,
            "returns": 0.95,
            "return_items": 0.95,
        }
    )
    max_quarantine_pct: float = Field(
        default=5.0,
        description="Max overall quarantine percentage before flagging in validation.",
    )
    max_row_loss_pct: float = Field(
        default=1.0,
        description="Max overall row loss percentage before flagging in validation.",
    )
    min_return_id_distinct_ratio: float = Field(
        default=0.001,
        description="Minimum distinct return_id ratio for contract-level checks.",
    )
    expected_bronze_partitions: list[str] = Field(
        default_factory=list,
        description="Expected bronze ingest_dt partitions for completeness checks.",
    )
    min_table_rows: dict[str, int] = Field(
        default_factory=dict,
        description="Minimum processed row counts per table before enriched runs.",
    )
    enriched_tables: list[str] = Field(
        default_factory=lambda: [
            "int_attributed_purchases",
            "int_cart_attribution",
            "int_inventory_risk",
            "int_customer_retention_signals",
            "int_customer_lifetime_value",
            "int_daily_business_metrics",
            "int_product_performance",
            "int_sales_velocity",
            "int_regional_financials",
            "int_shipping_economics",
        ],
        description="Expected Enriched Silver tables for validation.",
    )
    enriched_min_table_rows: dict[str, int] = Field(
        default_factory=dict,
        description="Minimum row counts per enriched table.",
    )
    enriched_lookback_days: int = Field(
        default=0,
        description="Days of ingest_dt partitions to include in enriched reads.",
    )
    bronze_validation_lookback_days: int = Field(
        default=0,
        description="Days of partitions to include in bronze validation.",
    )
    silver_validation_lookback_days: int = Field(
        default=0,
        description="Days of partitions to include in silver validation.",
    )
    enriched_max_rows_per_file: int = Field(
        default=500_000,
        description="Max rows per Parquet file for enriched outputs.",
    )
    enriched_ratio_epsilon: float = Field(
        default=0.0001,
        description="Tolerance for enriched ratio checks (floating-point drift).",
    )
    cart_abandoned_min_value: float = Field(
        default=0.0,
        description="Minimum cart value required to label a cart as abandoned.",
    )
    rate_precision: int = Field(
        default=6,
        description="Decimal precision for derived rate metrics.",
    )
    rate_cap_min: float = Field(
        default=0.0,
        description="Minimum cap for rate metrics when rate_cap_enabled is true.",
    )
    rate_cap_max: float = Field(
        default=1.0,
        description="Maximum cap for rate metrics when rate_cap_enabled is true.",
    )
    rate_cap_enabled: bool = Field(
        default=True,
        description="Whether to cap rate metrics within [rate_cap_min, rate_cap_max].",
    )
    return_units_max_ratio: float = Field(
        default=2.0,
        description=(
            "Maximum allowed ratio of units_returned to units_sold before flagging."
        ),
    )
    table_partitions: dict[str, str | None] = Field(default_factory=dict)
    silver_table_partitions: dict[str, str | None] = Field(default_factory=dict)
    enriched_partitions: dict[str, str] = Field(default_factory=dict)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)

    @field_validator("environment")
    @classmethod
    def environment_is_valid(cls, v: str) -> str:
        allowed = {"local", "dev", "prod"}
        if v not in allowed:
            raise ValueError(f"environment must be one of {allowed}, got '{v}'")
        return v

    @field_validator(
        "silver_publish_mode",
        "silver_enriched_publish_mode",
        "dims_publish_mode",
    )
    @classmethod
    def silver_publish_mode_is_valid(cls, v: str) -> str:
        allowed = {"direct", "staging"}
        if v not in allowed:
            raise ValueError(f"publish_mode must be one of {allowed}, got '{v}'")
        return v

    @field_validator("default_ingest_dt")
    @classmethod
    def ingest_dt_is_valid_date(cls, v: str) -> str:
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", v):
            raise ValueError(f"default_ingest_dt must be YYYY-MM-DD format, got '{v}'")
        return v

    @field_validator("sla_thresholds")
    @classmethod
    def sla_thresholds_in_range(cls, v: dict[str, float]) -> dict[str, float]:
        for table, threshold in v.items():
            if not 0.0 <= threshold <= 1.0:
                raise ValueError(
                    f"SLA threshold for '{table}' must be between 0 and 1, "
                    f"got {threshold}"
                )
        return v

    @field_validator("max_quarantine_pct", "max_row_loss_pct")
    @classmethod
    def percentage_in_range(cls, v: float) -> float:
        if not 0.0 <= v <= 100.0:
            raise ValueError(f"Percentage must be between 0 and 100, got {v}")
        return v

    @field_validator("rate_precision")
    @classmethod
    def rate_precision_is_non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("rate_precision must be non-negative")
        return v

    @model_validator(mode="after")
    def validate_rate_caps(self) -> "PipelineConfig":
        """Ensure rate_cap_max >= rate_cap_min."""
        if self.rate_cap_enabled and self.rate_cap_max < self.rate_cap_min:
            raise ValueError(
                f"rate_cap_max ({self.rate_cap_max}) must be >= "
                f"rate_cap_min ({self.rate_cap_min})"
            )
        return self

    @field_validator("return_units_max_ratio")
    @classmethod
    def return_units_max_ratio_gte_one(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("return_units_max_ratio must be >= 1.0")
        return v

    @field_validator("retry_config")
    @classmethod
    def retry_config_has_required_envs(
        cls, v: dict[str, RetryConfig]
    ) -> dict[str, RetryConfig]:
        """Validate that retry_config contains entries for all expected environments."""
        required_envs = {"local", "dev", "prod"}
        missing = required_envs - set(v.keys())
        if missing:
            raise ValueError(
                f"retry_config missing required environments: {missing}. "
                f"Expected: {required_envs}"
            )
        return v

    @model_validator(mode="after")
    def enriched_tables_have_partitions(self) -> "PipelineConfig":
        """Warn if enriched tables are missing partition definitions."""
        missing = []
        for table in self.enriched_tables:
            if table not in self.enriched_partitions:
                missing.append(table)
        if missing:
            logger.warning(f"Enriched tables missing partition definitions: {missing}")
        return self


class Settings(BaseSettings):
    """Root settings container."""

    pipeline: PipelineConfig

    model_config = SettingsConfigDict(env_prefix="ECOM_", extra="ignore")

    @classmethod
    def from_yaml(cls, path: str | Path, strict: bool = False) -> "Settings":
        """Load settings from YAML file.

        Args:
            path: Path to config YAML file
            strict: If True, raise ConfigValidationError on validation failure.
                    If False (default), fall back to defaults with a warning.

        Returns:
            Validated Settings instance

        Raises:
            ConfigValidationError: If strict=True and validation fails
        """
        try:
            payload = yaml.safe_load(Path(path).read_text()) or {}
        except OSError as exc:
            if strict:
                raise ConfigValidationError(
                    f"Failed to read config {path}: {exc}"
                ) from exc
            payload = {}
            logger.warning(f"Failed to read config {path}: {exc}")

        try:
            return cls.model_validate(payload)
        except ValidationError as exc:
            if strict:
                raise ConfigValidationError(
                    f"Config validation failed for {path}:\n{exc}"
                ) from exc
            logger.warning(f"Invalid config in {path}; falling back to defaults: {exc}")
            return cls(
                pipeline=PipelineConfig(
                    project_id=os.getenv(
                        "GOOGLE_CLOUD_PROJECT",
                        os.getenv("ECOM_PROJECT_ID", "local"),
                    ),
                    bronze_bucket=os.getenv("ECOM_BRONZE_BUCKET", "local"),
                    silver_bucket=os.getenv("ECOM_SILVER_BUCKET", "local"),
                )
            )

    def resolve_path(self, bucket: str, prefix: str) -> str:
        """Resolve a path to gs:// or local filesystem based on bucket config.

        Args:
            bucket: Bucket name or 'local'
            prefix: Path prefix within the bucket

        Returns:
            Resolved path string (gs://... or /path/to/...)
        """
        env = (
            os.getenv("PIPELINE_ENV") or self.pipeline.environment or "local"
        ).lower()

        if bucket == "local" or env == "local":
            # For local paths, we assume absolute paths or relative to execution context
            # We don't verify existence here, just formatting
            return str(Path(prefix))

        return f"gs://{bucket}/{prefix}"


def load_settings(
    config_path: str | Path | None = None, strict: bool = False
) -> Settings:
    """Load and validate settings from config file.

    Args:
        config_path: Path to config YAML file. Defaults to config/config.yml
        strict: If True, raise ConfigValidationError on validation failure

    Returns:
        Validated Settings instance
    """
    if config_path is None:
        config_path = os.getenv("ECOM_CONFIG_PATH", "config/config.yml")
    return Settings.from_yaml(config_path, strict=strict)


def validate_config(config_path: str | Path | None = None) -> list[str]:
    """Validate config file and return list of issues.

    This function is useful for CI/CD pipelines to validate config before deploy.

    Args:
        config_path: Path to config YAML file. Defaults to config/config.yml

    Returns:
        List of validation error messages. Empty list if valid.

    Example:
        >>> issues = validate_config("config/config.yml")
        >>> if issues:
        ...     print("Config errors:", issues)
        ...     sys.exit(1)
    """
    if config_path is None:
        config_path = "config/config.yml"

    issues: list[str] = []

    try:
        Settings.from_yaml(config_path, strict=True)
    except ConfigValidationError as exc:
        issues.append(str(exc))

    return issues
