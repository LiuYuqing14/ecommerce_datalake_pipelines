import polars as pl

from src.validation import enriched_schemas


def test_attributed_purchases_schema_has_expected_types():
    schema = enriched_schemas.ATTRIBUTED_PURCHASES_SCHEMA
    assert schema["cart_id"] == pl.String()
    assert schema["product_id"] == pl.Int64()
    assert schema["unit_price"] == pl.Decimal(18, 2)
    assert schema["is_within_window"] == pl.Boolean()


def test_enriched_schemas_map_contains_core_tables():
    schemas_map = enriched_schemas.ENRICHED_SCHEMAS
    assert "int_attributed_purchases" in schemas_map
    assert "int_customer_lifetime_value" in schemas_map
    assert "int_daily_business_metrics" in schemas_map
