import polars as pl

from src.transforms.regional_financials import compute_regional_financials


def test_compute_regional_financials_defaults_tax_rate() -> None:
    orders = pl.DataFrame(
        {
            "order_id": ["o-1"],
            "customer_id": ["c-1"],
            "gross_total": [100.0],
        }
    )
    customers = pl.DataFrame(
        {
            "customer_id": ["c-1"],
            "region": ["west"],
        }
    )

    result = compute_regional_financials(orders.lazy(), customers.lazy()).collect()
    row = result.row(0, named=True)

    assert row["region"] == "west"
    assert row["tax_rate"] == 0.0
    assert row["tax_amount"] == 0.0
    assert row["net_revenue"] == 100.0


def test_compute_regional_financials_extracts_from_address() -> None:
    orders = pl.DataFrame(
        {
            "order_id": ["o-1", "o-2"],
            "customer_id": ["c-1", "c-2"],
            "gross_total": [100.0, 200.0],
            "shipping_address": [
                "123 Main St, New York, NY 10001",
                None,
            ],
            "billing_address": [
                None,
                "456 Oak Rd, Los Angeles, CA 90210",
            ],
        }
    )
    customers = pl.DataFrame(
        {
            "customer_id": ["c-1", "c-2"],
            # region is missing here, should extract from address
        }
    )

    result = compute_regional_financials(orders.lazy(), customers.lazy()).collect()

    rows = result.sort("order_id").to_dicts()

    assert rows[0]["region"] == "US-NY"
    assert rows[1]["region"] == "US-CA"
