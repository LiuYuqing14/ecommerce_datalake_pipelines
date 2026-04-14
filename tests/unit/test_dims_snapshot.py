from unittest.mock import patch

import polars as pl
import pytest

from src.validation import dims_snapshot


@pytest.fixture
def mock_read_parquet_safe():
    with patch("src.validation.dims_snapshot.read_parquet_safe") as mock:
        yield mock


def test_validate_snapshot_missing_partition(mock_read_parquet_safe):
    mock_read_parquet_safe.return_value = None

    result = dims_snapshot.validate_snapshot(
        "customers", "path/to/dims", "snapshot_dt", "2025-10-01"
    )

    assert result["status"] == "FAIL"
    assert "Partition not found" in result["message"]


def test_validate_snapshot_empty_dataframe(mock_read_parquet_safe):
    mock_read_parquet_safe.return_value = pl.DataFrame({})

    result = dims_snapshot.validate_snapshot(
        "customers", "path/to/dims", "snapshot_dt", "2025-10-01"
    )

    assert result["status"] == "FAIL"
    assert "Empty snapshot" in result["message"]


def test_validate_snapshot_missing_columns(mock_read_parquet_safe):
    # customers requires customer_id, email, etc.
    df = pl.DataFrame({"customer_id": ["C1"]})  # Missing email
    mock_read_parquet_safe.return_value = df

    result = dims_snapshot.validate_snapshot(
        "customers", "path/to/dims", "snapshot_dt", "2025-10-01"
    )

    assert result["status"] == "FAIL"
    assert "Missing columns" in result["message"]


def test_validate_snapshot_null_pk(mock_read_parquet_safe):
    # customers PK is customer_id (first col in schema)
    df = pl.DataFrame(
        {
            "customer_id": [None, "C2"],
            "email": ["a@b.com", "b@c.com"],
            "signup_date": ["2020-01-01", "2020-01-02"],
            "batch_id": ["b1", "b1"],
            "ingestion_ts": ["2020-01-01", "2020-01-01"],
            "event_id": ["e1", "e2"],
            "source_file": ["f1", "f1"],
        }
    )
    mock_read_parquet_safe.return_value = df

    result = dims_snapshot.validate_snapshot(
        "customers", "path/to/dims", "snapshot_dt", "2025-10-01"
    )

    assert result["status"] == "FAIL"
    assert "Found 1 nulls in PK" in result["message"]


def test_validate_snapshot_success(mock_read_parquet_safe):
    df = pl.DataFrame(
        {
            "customer_id": ["C1", "C2"],
            "email": ["a@b.com", "b@c.com"],
            "signup_date": ["2020-01-01", "2020-01-02"],
            "batch_id": ["b1", "b1"],
            "ingestion_ts": ["2020-01-01", "2020-01-01"],
            "event_id": ["e1", "e2"],
            "source_file": ["f1", "f1"],
        }
    )
    mock_read_parquet_safe.return_value = df

    result = dims_snapshot.validate_snapshot(
        "customers", "path/to/dims", "snapshot_dt", "2025-10-01"
    )

    assert result["status"] == "PASS"
    assert result["message"] == "OK"
    assert result["rows"] == 2
