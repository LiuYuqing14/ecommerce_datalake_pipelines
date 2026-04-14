from datetime import date

import polars as pl

from src.transforms.shipping_economics import compute_shipping_economics


def test_compute_shipping_economics_margins() -> None:
    orders = pl.DataFrame(
        {
            "order_id": ["ORD-1", "ORD-2"],
            "order_date": [date(2020, 1, 1), date(2020, 1, 2)],
            "shipping_speed": ["standard", "overnight"],
            "shipping_cost": [10.0, 0.0],
            "actual_shipping_cost": [7.0, 12.0],
            "order_channel": ["web", "phone"],
        }
    )

    result = compute_shipping_economics(orders.lazy()).collect()
    row1 = result.filter(pl.col("order_id") == "ORD-1").row(0, named=True)
    row2 = result.filter(pl.col("order_id") == "ORD-2").row(0, named=True)

    assert row1["shipping_margin"] == 3.0
    assert row1["shipping_margin_pct"] == 0.3
    assert row1["is_expedited"] is False

    assert row2["shipping_margin"] == -12.0
    assert row2["shipping_margin_pct"] is None
    assert row2["is_expedited"] is True
