from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from src.transforms.churn_detection import compute_customer_retention_signals


def test_compute_customer_retention_signals_flags_danger_and_bronze() -> None:
    today = date(2020, 3, 10)
    customers = pl.DataFrame(
        {
            "customer_id": ["c-1", "c-2"],
            "loyalty_tier": ["Bronze", "Gold"],
        }
    )
    orders = pl.DataFrame(
        {
            "customer_id": ["c-1", "c-2", "c-2"],
            "order_date": [
                today - timedelta(days=45),
                today - timedelta(days=10),
                today - timedelta(days=5),
            ],
        }
    )

    result = compute_customer_retention_signals(
        customers.lazy(), orders.lazy(), reference_date=date(2020, 3, 10)
    ).collect()

    row_c1 = result.filter(pl.col("customer_id") == "c-1").row(0, named=True)
    row_c2 = result.filter(pl.col("customer_id") == "c-2").row(0, named=True)

    assert row_c1["is_in_danger_zone"] is True
    assert row_c1["needs_bronze_nudge"] is True
    assert row_c1["total_orders"] == 1

    assert row_c2["is_in_danger_zone"] is False
    assert row_c2["needs_bronze_nudge"] is False
