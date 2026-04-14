from datetime import date
from typing import Literal

import pytest
from pydantic import ValidationError

pytest.importorskip("email_validator")

from src.validation import schemas


def test_customer_record_validates():
    record = schemas.CustomerRecord(
        customer_id="c_1",
        first_name="Ada",
        last_name="Lovelace",
        email="ada@example.com",
        signup_date=date(2020, 1, 1),
        loyalty_tier="Gold",
        signup_channel="email",
    )
    assert record.customer_id == "c_1"


CustomerTier = Literal["Bronze", "Silver", "Gold", "Platinum"]


@pytest.mark.parametrize("tier", ["Bronze", "Silver", "Gold", "Platinum"])
def test_customer_record_tier_allowed(tier: CustomerTier):
    record = schemas.CustomerRecord(
        customer_id="c_2",
        first_name="Grace",
        last_name="Hopper",
        email="grace@example.com",
        signup_date=date(2020, 1, 2),
        loyalty_tier=tier,
        signup_channel="referral",
    )
    assert record.loyalty_tier == tier


def test_customer_record_rejects_bad_email():
    with pytest.raises(ValidationError):
        schemas.CustomerRecord(
            customer_id="c_3",
            first_name="Linus",
            last_name="Torvalds",
            email="not-an-email",
            signup_date=date(2020, 1, 3),
            loyalty_tier="Silver",
            signup_channel="social",
        )


def test_order_record_enforces_non_negative():
    with pytest.raises(ValidationError):
        schemas.OrderRecord(
            order_id="o_1",
            customer_id="c_1",
            order_date=date(2020, 1, 4),
            gross_total=-1.0,
            net_total=10.0,
            payment_method="card",
        )


def test_product_record_enforces_inventory_non_negative():
    with pytest.raises(ValidationError):
        schemas.ProductRecord(
            product_id=1,
            product_name="Widget",
            category="Tools",
            unit_price=1.0,
            cost_price=0.5,
            inventory_quantity=-1,
        )
