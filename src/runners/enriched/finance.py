"""Finance-focused Enriched Silver runners."""

from __future__ import annotations

import polars as pl

from src.settings import PipelineConfig
from src.transforms.regional_financials import compute_regional_financials
from src.transforms.shipping_economics import compute_shipping_economics

from .shared import enriched_runner


@enriched_runner(
    output_table="int_regional_financials",
    input_tables=["orders", "customers"],
)
def run_regional_financials(
    tables: dict[str, pl.LazyFrame], settings: PipelineConfig, ingest_dt: str
) -> pl.LazyFrame:
    """Regional financials transform."""
    return compute_regional_financials(
        orders=tables["orders"],
        customers=tables["customers"],
    )


@enriched_runner(
    output_table="int_shipping_economics",
    input_tables=["orders"],
)
def run_shipping_economics(
    tables: dict[str, pl.LazyFrame], settings: PipelineConfig, ingest_dt: str
) -> pl.LazyFrame:
    """Shipping economics transform."""
    return compute_shipping_economics(
        orders=tables["orders"],
    )
