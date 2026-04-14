from pathlib import Path

from src.validation.enriched.data import resolve_partition


def _make_partition(table_path: Path, key: str, value: str) -> Path:
    partition = table_path / f"{key}={value}"
    partition.mkdir(parents=True, exist_ok=True)
    return partition


def test_resolve_partition_ingest_dt_exact(tmp_path: Path) -> None:
    table_path = tmp_path / "int_inventory_risk"
    _make_partition(table_path, "ingest_dt", "2025-10-08")
    _make_partition(table_path, "ingest_dt", "2025-10-07")

    resolved, path = resolve_partition(
        table_path, "ingest_dt", "2025-10-08", lookback_days=0
    )

    assert resolved == "2025-10-08"
    assert path == table_path / "ingest_dt=2025-10-08"


def test_resolve_partition_business_date_range(tmp_path: Path) -> None:
    table_path = tmp_path / "int_sales_velocity"
    _make_partition(table_path, "order_dt", "2020-01-01")
    _make_partition(table_path, "order_dt", "2020-01-02")

    resolved, path = resolve_partition(
        table_path, "order_dt", "2020-01-02", lookback_days=1
    )

    assert resolved == "2020-01-02"
    assert path == table_path / "order_dt=2020-01-02"


def test_resolve_partition_business_date_missing(tmp_path: Path) -> None:
    table_path = tmp_path / "int_sales_velocity"
    _make_partition(table_path, "order_dt", "2020-01-01")

    resolved, path = resolve_partition(
        table_path, "order_dt", "2025-10-08", lookback_days=0
    )

    assert resolved == "2025-10-08"
    assert path is None


def test_resolve_partition_business_date_latest_fallback(tmp_path: Path) -> None:
    table_path = tmp_path / "int_sales_velocity"
    _make_partition(table_path, "order_dt", "2020-01-01")
    _make_partition(table_path, "order_dt", "2020-01-05")

    resolved, path = resolve_partition(table_path, "order_dt", None, lookback_days=0)

    assert resolved == "2020-01-05"
    assert path == table_path / "order_dt=2020-01-05"
