"""Integration test: Bronze → Silver → Enriched pipeline flow.

This test validates the complete data pipeline from raw Bronze data
through Base Silver transformations to Enriched Silver outputs.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

from src.settings import ValidationConfig
from src.transforms.customer_lifetime_value import compute_customer_lifetime_value
from src.transforms.product_performance import compute_product_performance
from src.validation.enriched.metrics import compute_null_rates, evaluate_sanity_checks


@pytest.fixture
def bronze_customers() -> pl.DataFrame:
    """Simulated Bronze customers data (raw ingestion format)."""
    return pl.DataFrame(
        {
            "customer_id": ["CUST-001", "CUST-002", "CUST-003", "CUST-004"],
            "email": [
                "alice@example.com",
                "bob@example.com",
                "carol@example.com",
                "dave@example.com",
            ],
            "first_name": ["Alice", "Bob", "Carol", "Dave"],
            "last_name": ["Smith", "Jones", "Williams", "Brown"],
            "signup_date": [
                date(2020, 1, 1),
                date(2020, 2, 15),
                date(2020, 3, 20),
                date(2020, 4, 10),
            ],
            "loyalty_tier": ["Gold", "Silver", "Bronze", "Platinum"],
            "signup_channel": ["web", "mobile", "web", "referral"],
            "batch_id": ["batch-001"] * 4,
            "ingestion_ts": [datetime(2020, 5, 1, 12, 0)] * 4,
        }
    )


@pytest.fixture
def bronze_orders() -> pl.DataFrame:
    """Simulated Bronze orders data."""
    return pl.DataFrame(
        {
            "order_id": ["ORD-001", "ORD-002", "ORD-003", "ORD-004", "ORD-005"],
            "customer_id": ["CUST-001", "CUST-001", "CUST-002", "CUST-003", "CUST-004"],
            "order_date": [
                date(2020, 1, 15),
                date(2020, 2, 20),
                date(2020, 3, 10),
                date(2020, 4, 5),
                date(2020, 5, 1),
            ],
            "gross_total": [150.00, 250.00, 75.00, 500.00, 1200.00],
            "net_total": [140.00, 230.00, 70.00, 480.00, 1150.00],
            "payment_method": [
                "credit_card",
                "credit_card",
                "paypal",
                "credit_card",
                "bank_transfer",
            ],
            "batch_id": ["batch-001"] * 5,
            "ingestion_ts": [datetime(2020, 5, 1, 12, 0)] * 5,
        }
    )


@pytest.fixture
def bronze_returns() -> pl.DataFrame:
    """Simulated Bronze returns data."""
    return pl.DataFrame(
        {
            "return_id": ["RET-001", "RET-002"],
            "order_id": ["ORD-001", "ORD-003"],
            "customer_id": ["CUST-001", "CUST-002"],
            "return_date": [date(2020, 1, 25), date(2020, 3, 20)],
            "refunded_amount": [30.00, 25.00],
            "reason": ["defective", "wrong_size"],
            "batch_id": ["batch-001"] * 2,
            "ingestion_ts": [datetime(2020, 5, 1, 12, 0)] * 2,
        }
    )


@pytest.fixture
def silver_customers(bronze_customers: pl.DataFrame) -> pl.DataFrame:
    """Base Silver customers - cleaned and typed from Bronze."""
    return bronze_customers.with_columns(
        [
            pl.col("customer_id").alias("customer_id"),
            pl.lit("medium_value").alias("clv_bucket"),
        ]
    )


@pytest.fixture
def silver_orders(bronze_orders: pl.DataFrame) -> pl.DataFrame:
    """Base Silver orders - cleaned and typed from Bronze."""
    return bronze_orders.with_columns(
        [
            pl.col("order_date").alias("order_dt"),
        ]
    )


@pytest.fixture
def silver_returns(bronze_returns: pl.DataFrame) -> pl.DataFrame:
    """Base Silver returns - cleaned and typed from Bronze."""
    return bronze_returns.with_columns(
        [
            pl.col("return_date").alias("return_dt"),
        ]
    )


@pytest.fixture
def silver_products() -> pl.DataFrame:
    """Base Silver product catalog."""
    return pl.DataFrame(
        {
            "product_id": [1, 2, 3, 4, 5],
            "product_name": ["Widget A", "Widget B", "Gadget X", "Gadget Y", "Tool Z"],
            "category": ["widgets", "widgets", "gadgets", "gadgets", "tools"],
            "unit_price": [25.00, 50.00, 75.00, 100.00, 150.00],
            "cost_price": [10.00, 20.00, 30.00, 40.00, 60.00],
            "inventory_quantity": [100, 50, 75, 30, 20],
            "batch_id": ["batch-001"] * 5,
            "ingestion_ts": [datetime(2020, 5, 1, 12, 0)] * 5,
        }
    )


@pytest.fixture
def silver_order_items() -> pl.DataFrame:
    """Base Silver order items - line items per order."""
    return pl.DataFrame(
        {
            "order_id": [
                "ORD-001",
                "ORD-001",
                "ORD-002",
                "ORD-003",
                "ORD-004",
                "ORD-005",
            ],
            "product_id": [1, 2, 3, 1, 4, 5],
            "product_name": [
                "Widget A",
                "Widget B",
                "Gadget X",
                "Widget A",
                "Gadget Y",
                "Tool Z",
            ],
            "category": [
                "widgets",
                "widgets",
                "gadgets",
                "widgets",
                "gadgets",
                "tools",
            ],
            "quantity": [2, 1, 1, 3, 2, 4],
            "unit_price": [25.00, 50.00, 75.00, 25.00, 100.00, 150.00],
            "discount_amount": [5.00, 10.00, 5.00, 0.00, 20.00, 50.00],
            "cost_price": [10.00, 20.00, 30.00, 10.00, 40.00, 60.00],
            "order_dt": [
                date(2020, 1, 15),
                date(2020, 1, 15),
                date(2020, 2, 20),
                date(2020, 3, 10),
                date(2020, 4, 5),
                date(2020, 5, 1),
            ],
            "batch_id": ["batch-001"] * 6,
            "ingestion_ts": [datetime(2020, 5, 1, 12, 0)] * 6,
        }
    )


@pytest.fixture
def silver_return_items() -> pl.DataFrame:
    """Base Silver return items - line items per return.

    Note: return_dt must match order_dt for product_performance grouping,
    since product_performance groups by product_dt derived from order/return dates.
    """
    return pl.DataFrame(
        {
            "return_item_id": [1, 2],
            "return_id": ["RET-001", "RET-002"],
            "order_id": ["ORD-001", "ORD-003"],
            "product_id": [1, 1],
            "product_name": ["Widget A", "Widget A"],
            "category": ["widgets", "widgets"],
            "quantity_returned": [1, 1],
            "unit_price": [25.00, 25.00],
            "cost_price": [10.00, 10.00],
            "refunded_amount": [25.00, 25.00],
            # Match order_dt: ORD-001 was 2020-01-15, ORD-003 was 2020-03-10
            "return_dt": [date(2020, 1, 15), date(2020, 3, 10)],
            "batch_id": ["batch-001"] * 2,
            "ingestion_ts": [datetime(2020, 5, 1, 12, 0)] * 2,
        }
    )


@pytest.fixture
def silver_cart_items() -> pl.DataFrame:
    """Base Silver cart items for cart attribution."""
    return pl.DataFrame(
        {
            "cart_item_id": [1, 2, 3, 4],
            "cart_id": ["CART-001", "CART-001", "CART-002", "CART-003"],
            "product_id": [1, 2, 3, 4],
            "product_name": ["Widget A", "Widget B", "Gadget X", "Gadget Y"],
            "category": ["widgets", "widgets", "gadgets", "gadgets"],
            "added_at": [
                datetime(2020, 1, 14, 10, 0),
                datetime(2020, 1, 14, 10, 5),
                datetime(2020, 2, 19, 15, 0),
                datetime(2020, 3, 5, 9, 0),
            ],
            "added_dt": [
                date(2020, 1, 14),
                date(2020, 1, 14),
                date(2020, 2, 19),
                date(2020, 3, 5),
            ],
            "quantity": [2, 1, 1, 2],
            "unit_price": [25.00, 50.00, 75.00, 100.00],
            "batch_id": ["batch-001"] * 4,
            "ingestion_ts": [datetime(2020, 5, 1, 12, 0)] * 4,
        }
    )


@pytest.fixture
def silver_shopping_carts() -> pl.DataFrame:
    """Base Silver shopping carts."""
    return pl.DataFrame(
        {
            "cart_id": ["CART-001", "CART-002", "CART-003"],
            "customer_id": ["CUST-001", "CUST-001", "CUST-003"],
            "created_at": [
                datetime(2020, 1, 14, 10, 0),
                datetime(2020, 2, 19, 15, 0),
                datetime(2020, 3, 5, 9, 0),
            ],
            "updated_at": [
                datetime(2020, 1, 15, 12, 0),
                datetime(2020, 2, 20, 10, 0),
                datetime(2020, 3, 6, 11, 0),
            ],
            "cart_total": [100.00, 75.00, 200.00],
            "status": ["converted", "converted", "abandoned"],
            "batch_id": ["batch-001"] * 3,
            "ingestion_ts": [datetime(2020, 5, 1, 12, 0)] * 3,
        }
    )


class TestBronzeToSilverFlow:
    """Test Bronze to Base Silver transformation quality."""

    def test_bronze_to_silver_customers_preserves_records(
        self, bronze_customers: pl.DataFrame, silver_customers: pl.DataFrame
    ) -> None:
        """Verify all Bronze customer records flow to Silver."""
        assert silver_customers.height == bronze_customers.height
        assert set(silver_customers["customer_id"]) == set(
            bronze_customers["customer_id"]
        )

    def test_bronze_to_silver_orders_adds_partition_column(
        self, bronze_orders: pl.DataFrame, silver_orders: pl.DataFrame
    ) -> None:
        """Verify Silver orders have partition date column."""
        assert "order_dt" in silver_orders.columns
        assert silver_orders.height == bronze_orders.height

    def test_bronze_to_silver_no_null_primary_keys(
        self, silver_customers: pl.DataFrame, silver_orders: pl.DataFrame
    ) -> None:
        """Verify primary keys are never null after Silver transformation."""
        assert silver_customers.filter(pl.col("customer_id").is_null()).height == 0
        assert silver_orders.filter(pl.col("order_id").is_null()).height == 0


class TestSilverToEnrichedFlow:
    """Test Base Silver to Enriched Silver transformation."""

    @pytest.mark.integration
    def test_customer_lifetime_value_enrichment(
        self,
        silver_customers: pl.DataFrame,
        silver_orders: pl.DataFrame,
        silver_returns: pl.DataFrame,
    ) -> None:
        """Verify CLV enrichment produces valid metrics."""
        enriched_clv = compute_customer_lifetime_value(
            customers=silver_customers.lazy(),
            orders=silver_orders.lazy(),
            returns=silver_returns.lazy(),
            reference_date=date(2020, 6, 1),
            churn_days=90,
        ).collect()

        # All customers should be in output
        assert enriched_clv.height == silver_customers.height

        # Required columns present
        required_cols = [
            "customer_id",
            "total_spent",
            "total_refunded",
            "net_clv",
            "order_count",
            "return_count",
            "customer_segment",
        ]
        for col in required_cols:
            assert col in enriched_clv.columns, f"Missing column: {col}"

        # Verify CLV calculation: net_clv = total_spent - total_refunded
        for row in enriched_clv.iter_rows(named=True):
            expected_net = row["total_spent"] - row["total_refunded"]
            assert abs(row["net_clv"] - expected_net) < 0.01, (
                f"CLV mismatch for {row['customer_id']}: "
                f"{row['net_clv']} != {expected_net}"
            )

        # Customer with orders should have positive metrics
        cust_001 = enriched_clv.filter(pl.col("customer_id") == "CUST-001")
        assert cust_001["order_count"].item() == 2
        assert cust_001["return_count"].item() == 1
        assert cust_001["total_spent"].item() > 0

    @pytest.mark.integration
    def test_product_performance_enrichment(
        self,
        silver_products: pl.DataFrame,
        silver_order_items: pl.DataFrame,
        silver_return_items: pl.DataFrame,
        silver_cart_items: pl.DataFrame,
    ) -> None:
        """Verify product performance enrichment calculates margins correctly."""
        enriched_products = compute_product_performance(
            products=silver_products.lazy(),
            order_items=silver_order_items.lazy(),
            return_items=silver_return_items.lazy(),
            cart_items=silver_cart_items.lazy(),
        ).collect()

        # Should have product performance rows
        assert enriched_products.height > 0

        # Required columns present
        required_cols = [
            "product_id",
            "units_sold",
            "units_returned",
            "gross_revenue",
            "net_revenue",
            "return_rate",
        ]
        for col in required_cols:
            assert col in enriched_products.columns, f"Missing column: {col}"

        # Units returned should never exceed units sold
        for row in enriched_products.iter_rows(named=True):
            assert (
                row["units_returned"] <= row["units_sold"]
            ), f"Returns exceed sales for product {row['product_id']}"

        # Return rate should be between 0 and 1
        return_rates = enriched_products.filter(pl.col("return_rate").is_not_null())[
            "return_rate"
        ]
        for rate in return_rates:
            assert 0 <= rate <= 1, f"Invalid return rate: {rate}"


class TestEnrichedValidation:
    """Test validation of Enriched outputs."""

    @pytest.mark.integration
    def test_enriched_null_rate_computation(
        self,
        silver_customers: pl.DataFrame,
        silver_orders: pl.DataFrame,
        silver_returns: pl.DataFrame,
    ) -> None:
        """Verify null rate computation on enriched data."""
        enriched_clv = compute_customer_lifetime_value(
            customers=silver_customers.lazy(),
            orders=silver_orders.lazy(),
            returns=silver_returns.lazy(),
            reference_date=date(2020, 6, 1),
        ).collect()

        null_rates = compute_null_rates(enriched_clv, ["customer_id", "net_clv"])

        # Primary key should have 0% null rate
        assert null_rates.get("customer_id", 1.0) == 0.0

    @pytest.mark.integration
    def test_enriched_sanity_checks_pass(
        self,
        silver_products: pl.DataFrame,
        silver_order_items: pl.DataFrame,
        silver_return_items: pl.DataFrame,
        silver_cart_items: pl.DataFrame,
    ) -> None:
        """Verify sanity checks pass on valid enriched data."""
        enriched_products = compute_product_performance(
            products=silver_products.lazy(),
            order_items=silver_order_items.lazy(),
            return_items=silver_return_items.lazy(),
            cart_items=silver_cart_items.lazy(),
        ).collect()

        config = ValidationConfig(
            sanity_checks={
                "non_negative": ["units_sold", "units_returned", "gross_revenue"],
                "rate_0_1": ["return_rate"],
            }
        )

        issues = evaluate_sanity_checks(enriched_products, config)

        # Should have no sanity issues with valid data
        assert len(issues) == 0, f"Unexpected sanity issues: {issues}"


class TestEndToEndPipeline:
    """Full end-to-end pipeline integration test."""

    @pytest.mark.integration
    def test_full_pipeline_data_integrity(
        self,
        bronze_customers: pl.DataFrame,
        bronze_orders: pl.DataFrame,
        bronze_returns: pl.DataFrame,
        silver_customers: pl.DataFrame,
        silver_orders: pl.DataFrame,
        silver_returns: pl.DataFrame,
    ) -> None:
        """Verify data flows correctly through all pipeline stages."""
        # Stage 1: Bronze → Silver row preservation
        assert silver_customers.height == bronze_customers.height
        assert silver_orders.height == bronze_orders.height
        assert silver_returns.height == bronze_returns.height

        # Stage 2: Silver → Enriched transformation
        enriched_clv = compute_customer_lifetime_value(
            customers=silver_customers.lazy(),
            orders=silver_orders.lazy(),
            returns=silver_returns.lazy(),
            reference_date=date(2020, 6, 1),
        ).collect()

        # Stage 3: Validate enriched output
        # All customers should appear in enriched output
        assert enriched_clv.height == silver_customers.height

        # Aggregations should be correct
        total_orders_in_silver = silver_orders.height
        total_orders_in_enriched = enriched_clv["order_count"].sum()
        assert total_orders_in_enriched == total_orders_in_silver

        total_returns_in_silver = silver_returns.height
        total_returns_in_enriched = enriched_clv["return_count"].sum()
        assert total_returns_in_enriched == total_returns_in_silver

    @pytest.mark.integration
    def test_pipeline_handles_customers_without_orders(
        self,
        silver_customers: pl.DataFrame,
        silver_orders: pl.DataFrame,
        silver_returns: pl.DataFrame,
    ) -> None:
        """Verify pipeline handles edge case of customers with no orders."""
        # Add a customer with no orders
        new_customer = pl.DataFrame(
            {
                "customer_id": ["CUST-005"],
                "email": ["eve@example.com"],
                "first_name": ["Eve"],
                "last_name": ["Wilson"],
                "signup_date": [date(2020, 5, 20)],
                "loyalty_tier": ["Bronze"],
                "signup_channel": ["web"],
                "batch_id": ["batch-002"],
                "ingestion_ts": [datetime(2020, 5, 1, 12, 0)],
                "clv_bucket": ["low_value"],
            }
        )
        customers_with_inactive = pl.concat([silver_customers, new_customer])

        enriched_clv = compute_customer_lifetime_value(
            customers=customers_with_inactive.lazy(),
            orders=silver_orders.lazy(),
            returns=silver_returns.lazy(),
            reference_date=date(2020, 6, 1),
        ).collect()

        # New customer should be in output with zero metrics
        cust_005 = enriched_clv.filter(pl.col("customer_id") == "CUST-005")
        assert cust_005.height == 1
        assert cust_005["order_count"].item() == 0
        assert cust_005["total_spent"].item() == 0.0
        assert cust_005["customer_segment"].item() == "one-timer"

    @pytest.mark.integration
    def test_pipeline_output_parquet_compatible(
        self,
        tmp_path: Path,
        silver_customers: pl.DataFrame,
        silver_orders: pl.DataFrame,
        silver_returns: pl.DataFrame,
    ) -> None:
        """Verify enriched output can be written and read as Parquet."""
        enriched_clv = compute_customer_lifetime_value(
            customers=silver_customers.lazy(),
            orders=silver_orders.lazy(),
            returns=silver_returns.lazy(),
            reference_date=date(2020, 6, 1),
        ).collect()

        # Write to Parquet
        output_path = tmp_path / "int_customer_lifetime_value.parquet"
        enriched_clv.write_parquet(output_path)

        # Read back and verify
        read_back = pl.read_parquet(output_path)
        assert read_back.height == enriched_clv.height
        assert set(read_back.columns) == set(enriched_clv.columns)

        # Verify data integrity after round-trip
        original_ids = set(enriched_clv["customer_id"].to_list())
        read_ids = set(read_back["customer_id"].to_list())
        assert original_ids == read_ids
