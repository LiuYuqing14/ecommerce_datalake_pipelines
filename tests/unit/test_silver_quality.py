from __future__ import annotations

from pathlib import Path

import polars as pl

from src.validation.silver.data import get_quarantine_breakdown


def test_get_quarantine_breakdown_counts_reasons(tmp_path: Path) -> None:
    table_path = tmp_path / "orders"
    table_path.mkdir(parents=True)
    df = pl.DataFrame(
        {
            "invalid_reason": [
                "missing_customer_id",
                "missing_customer_id",
                "negative_net_total",
            ]
        }
    )
    df.write_parquet(table_path / "part-0000.parquet")

    breakdown = get_quarantine_breakdown(table_path, top_n=5)

    assert breakdown[0]["reason"] == "missing_customer_id"
    assert breakdown[0]["count"] == 2
    assert breakdown[1]["reason"] == "negative_net_total"
    assert breakdown[1]["count"] == 1
