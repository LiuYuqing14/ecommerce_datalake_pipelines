"""Customer-focused Enriched Silver runners."""

from __future__ import annotations

from datetime import date

import polars as pl

from src.settings import PipelineConfig
from src.transforms.churn_detection import compute_customer_retention_signals
from src.transforms.customer_lifetime_value import compute_customer_lifetime_value

from .shared import enriched_runner


@enriched_runner(
    output_table="int_customer_retention_signals",
    input_tables=["customers", "orders"],
)
def run_customer_retention(
    tables: dict[str, pl.LazyFrame], settings: PipelineConfig, ingest_dt: str
) -> pl.LazyFrame:
    """Customer retention signals transform."""
    return compute_customer_retention_signals(
        customers=tables["customers"],
        orders=tables["orders"],
        lookback_days=settings.churn_danger_window_days,
        reference_date=date.fromisoformat(ingest_dt),
    )


@enriched_runner(
    output_table="int_customer_lifetime_value",
    input_tables=["customers", "orders", "returns"],
)
def run_customer_lifetime_value(
    tables: dict[str, pl.LazyFrame], settings: PipelineConfig, ingest_dt: str
) -> pl.LazyFrame:
    """Customer lifetime value transform."""
    return compute_customer_lifetime_value(
        customers=tables["customers"],
        orders=tables["orders"],
        returns=tables["returns"],
        reference_date=date.fromisoformat(ingest_dt),
    )
