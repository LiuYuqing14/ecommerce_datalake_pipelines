from unittest.mock import MagicMock

import polars as pl

from src.validation.enriched.metrics import evaluate_semantic_checks


def test_semantic_checks_cart_attribution() -> None:
    df = pl.DataFrame(
        {
            "cart_status": ["converted", "abandoned", "abandoned"],
            "order_id": [None, "ORD-1", None],
            "abandoned_value": [0.0, 0.0, 0.0],
            "time_to_purchase_hours": [1, -1, None],
        }
    )

    config = MagicMock()
    settings = MagicMock()
    settings.cart_abandoned_min_value = 0.0
    settings.rate_cap_min = 0.0
    settings.rate_cap_max = 1.0
    settings.return_units_max_ratio = 2.0

    config.semantic_checks = {
        "int_cart_attribution": [
            {
                "name": "converted_requires_order_id",
                "expr": "cart_status = 'converted' and order_id is null",
            },
            {
                "name": "abandoned_requires_null_order_id",
                "expr": "cart_status = 'abandoned' and order_id is not null",
            },
            {
                "name": "abandoned_value_positive_for_abandoned",
                "expr": "cart_status = 'abandoned' and abandoned_value <= 0",
            },
            {
                "name": "time_to_purchase_hours_non_negative",
                "expr": "time_to_purchase_hours < 0",
            },
        ]
    }

    issues = evaluate_semantic_checks(
        df, "int_cart_attribution", config, settings, 0.0001
    )

    assert "converted_requires_order_id: 1 rows" in issues
    assert "abandoned_requires_null_order_id: 1 rows" in issues
    assert "abandoned_value_positive_for_abandoned: 2 rows" in issues
    assert "time_to_purchase_hours_non_negative: 1 rows" in issues


def test_semantic_checks_product_performance() -> None:
    df = pl.DataFrame(
        {
            "units_sold": [10],
            "units_returned": [12],
            "return_rate": [1.2],
            "units_in_carts": [5],
            "cart_to_order_rate": [2.0],
            "gross_margin": [100.0],
            "net_margin": [120.0],
        }
    )

    config = MagicMock()
    settings = MagicMock()
    settings.cart_abandoned_min_value = 0.0
    settings.rate_cap_min = 0.0
    settings.rate_cap_max = 1.0
    settings.return_units_max_ratio = 2.0

    config.semantic_checks = {
        "int_product_performance": [
            {
                "name": "units_returned_le_units_sold",
                "expr": "units_returned > units_sold",
            },
            {
                "name": "return_rate_le_one",
                "expr": "return_rate > 1.0 + {ratio_epsilon}",
            },
            {
                "name": "cart_to_order_rate_le_one",
                "expr": "cart_to_order_rate > 1.0 + {ratio_epsilon}",
            },
            {
                "name": "net_margin_le_gross_margin",
                "expr": "net_margin > gross_margin + {ratio_epsilon}",
            },
        ]
    }

    issues = evaluate_semantic_checks(
        df, "int_product_performance", config, settings, 0.0001
    )

    assert "units_returned_le_units_sold: 1 rows" in issues
    assert "return_rate_le_one: 1 rows" in issues
    assert "cart_to_order_rate_le_one: 1 rows" in issues
    assert "net_margin_le_gross_margin: 1 rows" in issues


def test_semantic_checks_shipping_economics() -> None:
    df = pl.DataFrame(
        {
            "shipping_cost": [0.0, 10.0],
            "actual_shipping_cost": [5.0, 3.0],
            "shipping_margin": [-5.0, 8.0],
            "shipping_margin_pct": [0.1, 0.7],
        }
    )

    config = MagicMock()
    settings = MagicMock()
    settings.cart_abandoned_min_value = 0.0
    settings.rate_cap_min = 0.0
    settings.rate_cap_max = 1.0
    settings.return_units_max_ratio = 2.0

    config.semantic_checks = {
        "int_shipping_economics": [
            {
                "name": "shipping_margin_matches_components",
                "expr": (
                    "abs(shipping_margin - (shipping_cost - actual_shipping_cost)) "
                    "> {ratio_epsilon}"
                ),
            },
            {
                "name": "shipping_margin_pct_null_when_zero_cost",
                "expr": "shipping_cost = 0 and shipping_margin_pct is not null",
            },
        ]
    }

    issues = evaluate_semantic_checks(
        df, "int_shipping_economics", config, settings, 0.0001
    )

    assert "shipping_margin_matches_components: 1 rows" in issues
    assert "shipping_margin_pct_null_when_zero_cost: 1 rows" in issues
