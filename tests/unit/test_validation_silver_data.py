from __future__ import annotations

from pathlib import Path

import polars as pl

from src.validation.silver import data as silver_data


def _write_parquet(path: Path, rows: list[dict]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)
    return path


def test_get_quarantine_breakdown_empty_path(tmp_path: Path):
    assert silver_data.get_quarantine_breakdown(tmp_path / "missing") == []


def test_get_quarantine_breakdown_counts(tmp_path: Path):
    parquet_path = tmp_path / "quarantine" / "part-0.parquet"
    _write_parquet(
        parquet_path,
        [
            {"invalid_reason": "bad_price", "row_num": 1},
            {"invalid_reason": "bad_price", "row_num": 2},
            {"invalid_reason": "bad_sku", "row_num": 3},
        ],
    )
    breakdown = silver_data.get_quarantine_breakdown(tmp_path / "quarantine", top_n=5)
    assert breakdown[0]["reason"] == "bad_price"
    assert breakdown[0]["count"] == 2
    assert breakdown[0]["percentage"] == 66.7


def test_get_quarantine_breakdown_missing_column(tmp_path: Path):
    parquet_path = tmp_path / "quarantine" / "part-0.parquet"
    _write_parquet(parquet_path, [{"foo": 1}])
    assert silver_data.get_quarantine_breakdown(tmp_path / "quarantine") == []


def test_compute_key_cardinality(tmp_path: Path):
    parquet_path = tmp_path / "orders" / "part-0.parquet"
    _write_parquet(
        parquet_path,
        [
            {"order_id": "o1"},
            {"order_id": "o1"},
            {"order_id": None},
            {"order_id": "o2"},
        ],
    )
    result = silver_data.compute_key_cardinality(tmp_path / "orders", "order_id")
    assert result["total_rows"] == 4
    assert result["non_null_rows"] == 3
    assert result["distinct_count"] == 2
    assert result["distinct_ratio"] == 2 / 3


def test_list_partitions_by_key_local(tmp_path: Path):
    base = tmp_path / "orders"
    (base / "ingest_dt=2024-01-01").mkdir(parents=True)
    (base / "ingest_dt=2024-01-02").mkdir(parents=True)
    partitions = silver_data.list_partitions_by_key(base, "ingest_dt")
    assert partitions == {"2024-01-01", "2024-01-02"}


def test_check_required_columns(tmp_path: Path):
    parquet_path = tmp_path / "orders" / "part-0.parquet"
    _write_parquet(
        parquet_path,
        [
            {"order_id": "o1", "total": 10, "extra": 1},
            {"order_id": None, "total": 5, "extra": 2},
        ],
    )
    result = silver_data.check_required_columns(
        tmp_path / "orders",
        ["order_id", "total"],
    )
    nulls = result["nulls"]
    assert isinstance(nulls, dict)
    assert nulls["order_id"] == 1


def test_check_required_columns_missing_path(tmp_path: Path):
    result = silver_data.check_required_columns(
        tmp_path / "missing",
        ["order_id", "total"],
    )
    assert result["missing"] == ["order_id", "total"]
