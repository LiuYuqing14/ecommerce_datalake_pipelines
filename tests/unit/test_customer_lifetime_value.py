from datetime import date

import polars as pl

from src.transforms.customer_lifetime_value import compute_customer_lifetime_value


def test_compute_customer_lifetime_value_segments() -> None:
    customers = pl.DataFrame(
        {
            "customer_id": ["CUST-1", "CUST-2"],
            "clv_bucket": ["medium_value", "high_value"],
        }
    )
    orders = pl.DataFrame(
        {
            "customer_id": ["CUST-1", "CUST-1", "CUST-2"],
            "net_total": [100.0, 200.0, 1000.0],
            "order_date": [date(2020, 1, 1), date(2020, 1, 5), date(2020, 1, 1)],
        }
    )
    returns = pl.DataFrame(
        {
            "customer_id": ["CUST-1"],
            "refunded_amount": [50.0],
            "return_date": [date(2020, 1, 10)],
        }
    )

    result = compute_customer_lifetime_value(
        customers=customers.lazy(),
        orders=orders.lazy(),
        returns=returns.lazy(),
        reference_date=date(2020, 3, 1),
    ).collect()

    cust1 = result.filter(pl.col("customer_id") == "CUST-1").row(0, named=True)
    cust2 = result.filter(pl.col("customer_id") == "CUST-2").row(0, named=True)

    assert cust1["net_clv"] == 250.0
    assert cust1["order_count"] == 2
    assert cust1["return_count"] == 1
    assert cust1["customer_segment"] == "regular"
    assert cust1["predicted_clv_bucket"] == "medium_value"
    assert cust1["actual_clv_bucket"] == "medium_value"
    assert cust2["order_count"] == 1
    assert cust2["customer_segment"] == "one-timer"
