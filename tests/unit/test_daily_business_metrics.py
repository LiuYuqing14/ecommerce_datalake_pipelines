from datetime import date

import polars as pl

from src.transforms.daily_business_metrics import compute_daily_business_metrics


def test_compute_daily_business_metrics_basic_kpis() -> None:
    orders = pl.DataFrame(
        {
            "order_dt": [date(2020, 1, 1), date(2020, 1, 1), date(2020, 1, 2)],
            "gross_total": [120.0, 80.0, 120.0],
            "net_total": [100.0, 80.0, 100.0],
        }
    )
    carts = pl.DataFrame(
        {
            "created_dt": [
                date(2020, 1, 1),
                date(2020, 1, 1),
                date(2020, 1, 1),
                date(2020, 1, 1),
                date(2020, 1, 2),
                date(2020, 1, 2),
            ]
        }
    )
    returns = pl.DataFrame(
        {
            "return_dt": [date(2020, 1, 1)],
            "refunded_amount": [20.0],
        }
    )

    result = compute_daily_business_metrics(
        orders=orders.lazy(), carts=carts.lazy(), returns=returns.lazy()
    ).collect()

    day1 = result.filter(pl.col("date") == date(2020, 1, 1)).row(0, named=True)

    assert day1["orders_count"] == 2
    assert day1["gross_revenue"] == 200.0
    assert day1["net_revenue"] == 180.0
    assert day1["avg_order_value"] == 90.0
    assert day1["carts_created"] == 4
    assert day1["cart_conversion_rate"] == 0.5
    assert day1["returns_count"] == 1
    assert day1["return_rate"] == 0.5
    assert day1["refund_total"] == 20.0
