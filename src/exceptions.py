"""Custom exception hierarchy for the pipeline.

Exception Hierarchy:
    PipelineError (base)
    ├── ConfigError
    │   └── ConfigValidationError
    ├── DataError
    │   ├── SchemaError
    │   ├── ValidationError
    │   └── TransformError
    └── StorageError
        ├── ReadError
        └── WriteError

Usage:
    from src.exceptions import TransformError, ValidationError

    try:
        result = compute_clv(customers, orders)
    except TransformError as e:
        logger.error(f"Transform failed: {e}")
        raise
"""

from __future__ import annotations


class PipelineError(Exception):
    """Base exception for all pipeline errors.

    All custom exceptions inherit from this class, making it easy to catch
    any pipeline-related error with a single except clause.

    Example:
        try:
            run_pipeline()
        except PipelineError as e:
            logger.error(f"Pipeline failed: {e}")
            sys.exit(1)
    """

    def __init__(self, message: str, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            detail_str = ", ".join(f"{k}={v}" for k, v in self.details.items())
            return f"{self.message} ({detail_str})"
        return self.message


# --- Configuration Errors ---


class ConfigError(PipelineError):
    """Raised when configuration loading or parsing fails."""

    pass


class ConfigValidationError(ConfigError):
    """Raised when config.yml fails schema validation.

    Example:
        if environment not in ("local", "dev", "prod"):
            raise ConfigValidationError(
                "Invalid environment",
                details={
                    "environment": environment,
                    "allowed": ["local", "dev", "prod"],
                },
            )
    """

    pass


# --- Data Errors ---


class DataError(PipelineError):
    """Base class for data-related errors."""

    pass


class SchemaError(DataError):
    """Raised when data schema doesn't match expectations.

    Example:
        if "customer_id" not in df.columns:
            raise SchemaError(
                "Missing required column",
                details={"column": "customer_id", "table": "orders"}
            )
    """

    pass


class ValidationError(DataError):
    """Raised when data fails quality validation checks.

    Example:
        if null_rate > 0.05:
            raise ValidationError(
                "Null rate exceeds threshold",
                details={"column": "email", "null_rate": null_rate, "threshold": 0.05}
            )
    """

    pass


class TransformError(DataError):
    """Raised when a data transformation fails.

    Example:
        try:
            result = compute_customer_lifetime_value(customers, orders, returns)
        except Exception as e:
            raise TransformError(
                "CLV computation failed",
                details={"transform": "customer_lifetime_value", "error": str(e)}
            ) from e
    """

    pass


# --- Storage Errors ---


class StorageError(PipelineError):
    """Base class for storage-related errors (GCS, local filesystem)."""

    pass


class ReadError(StorageError):
    """Raised when reading from storage fails.

    Example:
        try:
            df = pl.read_parquet(path)
        except Exception as e:
            raise ReadError(
                "Failed to read parquet file",
                details={"path": str(path), "error": str(e)}
            ) from e
    """

    pass


class WriteError(StorageError):
    """Raised when writing to storage fails.

    Example:
        try:
            df.write_parquet(output_path)
        except Exception as e:
            raise WriteError(
                "Failed to write parquet file",
                details={"path": str(output_path), "rows": df.height, "error": str(e)}
            ) from e
    """

    pass


# --- Convenience exports ---

__all__ = [
    "PipelineError",
    "ConfigError",
    "ConfigValidationError",
    "DataError",
    "SchemaError",
    "ValidationError",
    "TransformError",
    "StorageError",
    "ReadError",
    "WriteError",
]
