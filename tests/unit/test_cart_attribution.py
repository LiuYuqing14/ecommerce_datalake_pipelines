from datetime import datetime

import polars as pl

from src.transforms.cart_attribution import (
    compute_cart_attribution,
    compute_cart_attribution_summary,
)


def test_compute_cart_attribution_flags_recovered() -> None:
    carts = pl.DataFrame(
        {
            "cart_id": ["c1"],
            "customer_id": ["cust-1"],
            "created_at": [datetime(2020, 1, 1, 10, 0)],
        }
    )
    orders = pl.DataFrame(
        {
            "order_id": ["o1"],
            "customer_id": ["cust-1"],
            "order_date": [datetime(2020, 1, 1, 20, 0)],
        }
    )

    result = compute_cart_attribution(
        carts.lazy(), orders.lazy(), tolerance_hours=48
    ).collect()

    assert result.shape[0] == 1
    assert result[0, "is_recovered"] is True
    assert result[0, "cart_id"] == "c1"


def test_compute_cart_attribution_summary_cart_status() -> None:
    carts = pl.DataFrame(
        {
            "cart_id": ["c1", "c2"],
            "customer_id": ["cust-1", "cust-2"],
            "created_at": [datetime(2020, 1, 1, 10, 0), datetime(2020, 1, 1, 11, 0)],
            "updated_at": [datetime(2020, 1, 1, 10, 30), datetime(2020, 1, 1, 11, 45)],
            "cart_total": [100.0, 50.0],
        }
    )
    cart_items = pl.DataFrame(
        {
            "cart_id": ["c1", "c1", "c2"],
            "product_id": [1, 2, 3],
            "quantity": [1, 2, 1],
            "unit_price": [20.0, 30.0, 50.0],
            "category": ["a", "b", "c"],
        }
    )
    orders = pl.DataFrame(
        {
            "order_id": ["o1"],
            "customer_id": ["cust-1"],
            "order_date": [datetime(2020, 1, 1, 12, 0)],
            "order_channel": ["web"],
        }
    )

    result = compute_cart_attribution_summary(
        carts.lazy(), cart_items.lazy(), orders.lazy()
    ).collect()

    status_map = dict(zip(result["cart_id"], result["cart_status"], strict=False))
    assert status_map["c1"] == "converted"
    assert status_map["c2"] == "abandoned"
