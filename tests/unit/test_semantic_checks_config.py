import polars as pl
import pytest

from src.settings import load_settings


def test_semantic_checks_parse() -> None:
    settings = load_settings("config/config.yml", strict=True)
    checks = settings.pipeline.validation.semantic_checks
    ratio_epsilon = settings.pipeline.enriched_ratio_epsilon

    for table, table_checks in checks.items():
        for check in table_checks:
            expr = check["expr"].format(
                ratio_epsilon=ratio_epsilon,
                cart_abandoned_min_value=settings.pipeline.cart_abandoned_min_value,
                rate_cap_min=settings.pipeline.rate_cap_min,
                rate_cap_max=settings.pipeline.rate_cap_max,
                return_units_max_ratio=settings.pipeline.return_units_max_ratio,
            )
            try:
                pl.sql_expr(expr)
            except Exception as exc:
                pytest.fail(f"{table}.{check['name']} invalid SQL: {exc}")
