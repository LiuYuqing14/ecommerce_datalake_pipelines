from datetime import date

import polars as pl

from src.transforms.product_performance import compute_product_performance


def test_compute_product_performance_basic_metrics() -> None:
    products = pl.DataFrame(
        {
            "product_id": [1],
            "product_name": ["Widget"],
            "category": ["tools"],
            "unit_price": [20.0],
            "cost_price": [5.0],
            "inventory_quantity": [10],
        }
    )
    order_items = pl.DataFrame(
        {
            "order_id": ["O1"],
            "product_id": [1],
            "quantity": [2],
            "unit_price": [20.0],
            "cost_price": [5.0],
            "order_dt": [date(2020, 1, 1)],
        }
    )
    return_items = pl.DataFrame(
        {
            "return_id": ["R1"],
            "order_id": ["O1"],
            "product_id": [1],
            "quantity_returned": [1],
            "refunded_amount": [20.0],
            "return_dt": [date(2020, 1, 1)],
        }
    )
    cart_items = pl.DataFrame(
        {
            "product_id": [1],
            "quantity": [3],
            "unit_price": [20.0],
            "added_dt": [date(2020, 1, 1)],
        }
    )

    result = compute_product_performance(
        products=products.lazy(),
        order_items=order_items.lazy(),
        return_items=return_items.lazy(),
        cart_items=cart_items.lazy(),
    ).collect()

    assert result.shape[0] == 1
    row = result.row(0, named=True)
    assert row["units_sold"] == 2
    assert row["units_returned"] == 1
    assert row["units_in_carts"] == 3
    assert row["gross_revenue"] == 40.0
    assert row["gross_margin"] == 30.0
    assert row["net_margin"] == 10.0
