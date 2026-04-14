from datetime import date

import polars as pl

from src.transforms.sales_velocity import compute_sales_velocity


def test_compute_sales_velocity_flags_trend() -> None:
    orders = pl.DataFrame(
        {
            "order_id": ["o-1", "o-2"],
            "order_date": [date(2020, 1, 1), date(2020, 1, 2)],
        }
    )
    order_items = pl.DataFrame(
        {
            "order_id": ["o-1", "o-2"],
            "product_id": [101, 101],
            "quantity": [10, 20],
        }
    )

    result = compute_sales_velocity(
        orders.lazy(), order_items.lazy(), window_days=2
    ).collect()
    row = result.filter(pl.col("order_dt") == date(2020, 1, 2)).row(0, named=True)

    assert row["velocity_avg"] == 15
    assert row["trend_signal"] == "UP"
