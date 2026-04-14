"""Operations-focused Enriched Silver runners."""

from __future__ import annotations

import polars as pl

from src.settings import PipelineConfig
from src.transforms.daily_business_metrics import compute_daily_business_metrics

from .shared import enriched_runner


@enriched_runner(
    output_table="int_daily_business_metrics",
    input_tables=["orders", "returns", "shopping_carts"],
)
def run_daily_business_metrics(
    tables: dict[str, pl.LazyFrame], settings: PipelineConfig, ingest_dt: str
) -> pl.LazyFrame:
    """Daily business metrics transform."""
    return compute_daily_business_metrics(
        orders=tables["orders"],
        returns=tables["returns"],
        carts=tables["shopping_carts"],
        ratio_epsilon=settings.enriched_ratio_epsilon,
        rate_precision=settings.rate_precision,
        rate_cap_min=settings.rate_cap_min,
        rate_cap_max=settings.rate_cap_max,
        rate_cap_enabled=settings.rate_cap_enabled,
    )
