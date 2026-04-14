import polars as pl
import pytest

from src.transforms.inventory_risk import compute_inventory_risk


def test_compute_inventory_risk_outputs_expected_tier() -> None:
    products = pl.DataFrame(
        {
            "product_id": [101],
            "inventory_quantity": [10],
            "cost_price": [2.0],
        }
    )
    order_items = pl.DataFrame(
        {
            "product_id": [101],
            "quantity": [5],
        }
    )
    return_items = pl.DataFrame(
        {
            "product_id": [101],
            "quantity_returned": [1],
        }
    )

    result = compute_inventory_risk(
        products.lazy(), order_items.lazy(), return_items.lazy()
    ).collect()
    row = result.row(0, named=True)

    assert row["utilization_ratio"] == 0.5
    assert row["return_signal"] == 0.2
    assert row["locked_capital"] == 20.0
    assert row["risk_tier"] == "MODERATE"


@pytest.mark.parametrize(
    ("inventory_quantity", "expected_tier"),
    [(10, "MODERATE"), (5, "HIGH")],
)
def test_inventory_risk_tiers(inventory_quantity: int, expected_tier: str) -> None:
    products = pl.DataFrame(
        {
            "product_id": [101],
            "inventory_quantity": [inventory_quantity],
            "cost_price": [2.0],
        }
    )
    order_items = pl.DataFrame(
        {
            "product_id": [101],
            "quantity": [5],
        }
    )
    return_items = pl.DataFrame(
        {
            "product_id": [101],
            "quantity_returned": [1],
        }
    )

    result = compute_inventory_risk(
        products.lazy(), order_items.lazy(), return_items.lazy()
    ).collect()
    row = result.row(0, named=True)

    assert row["risk_tier"] == expected_tier
