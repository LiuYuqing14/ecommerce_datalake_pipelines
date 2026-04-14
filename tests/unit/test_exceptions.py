from src.exceptions import (
    ConfigError,
    ConfigValidationError,
    DataError,
    PipelineError,
    ReadError,
    SchemaError,
    StorageError,
    TransformError,
    ValidationError,
    WriteError,
)


def test_pipeline_error_base():
    error = PipelineError("Test message")
    assert str(error) == "Test message"
    assert error.details == {}


def test_pipeline_error_with_details():
    error = PipelineError("Test message", details={"key": "value"})
    assert str(error) == "Test message (key=value)"
    assert error.details == {"key": "value"}


def test_exception_hierarchy():
    # Test that all inherit from PipelineError
    assert isinstance(ConfigError("test"), PipelineError)
    assert isinstance(ConfigValidationError("test"), ConfigError)
    assert isinstance(DataError("test"), PipelineError)
    assert isinstance(SchemaError("test"), DataError)
    assert isinstance(ValidationError("test"), DataError)
    assert isinstance(TransformError("test"), DataError)
    assert isinstance(StorageError("test"), PipelineError)
    assert isinstance(ReadError("test"), StorageError)
    assert isinstance(WriteError("test"), StorageError)


def test_read_error_details():
    error = ReadError("Failed to read", details={"path": "/tmp/test.parquet"})
    assert "path=/tmp/test.parquet" in str(error)


def test_write_error_details():
    error = WriteError(
        "Failed to write", details={"path": "/tmp/test.parquet", "rows": 100}
    )
    assert "path=/tmp/test.parquet" in str(error)
    assert "rows=100" in str(error)
