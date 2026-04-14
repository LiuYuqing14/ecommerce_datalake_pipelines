from __future__ import annotations

from pathlib import Path

import polars as pl
from polars.exceptions import ComputeError, SQLInterfaceError, SQLSyntaxError

from src.settings import PipelineConfig, ValidationConfig
from src.validation.common import join_path, read_parquet_safe
from src.validation.enriched.data import (
    compute_row_delta,
    count_parquet_rows,
    get_schema_snapshot,
    resolve_partition,
)
from src.validation.enriched.models import EnrichedTableMetrics


def compute_null_rates(df: pl.DataFrame, columns: list[str]) -> dict[str, float]:
    null_rates: dict[str, float] = {}
    for column in columns:
        if column not in df.columns:
            continue
        total = df.height
        if total == 0:
            null_rates[column] = 0.0
            continue
        nulls = df.select(pl.col(column).is_null().sum()).item()
        null_rates[column] = round(nulls / total, 6)
    return null_rates


def evaluate_sanity_checks(df: pl.DataFrame, config: ValidationConfig) -> list[str]:
    issues: list[str] = []
    sanity = config.sanity_checks
    for column in sanity.get("non_negative", []):
        if column not in df.columns:
            continue
        negatives = df.select((pl.col(column) < 0).sum()).item()
        if negatives:
            issues.append(f"{column}: {negatives} negative")

    for column in sanity.get("rate_0_1", []):
        if column not in df.columns:
            continue
        out_of_range = df.select(
            ((pl.col(column) < 0) | (pl.col(column) > 1)).sum()
        ).item()
        if out_of_range:
            issues.append(f"{column}: {out_of_range} outside_0_1")
    return issues


def evaluate_semantic_checks(
    df: pl.DataFrame,
    table: str,
    config: ValidationConfig,
    settings: PipelineConfig,
    ratio_epsilon: float,
) -> list[str]:
    checks = config.semantic_checks.get(table, [])
    issues: list[str] = []
    for check in checks:
        name = check["name"]
        expr = check["expr"].format(
            ratio_epsilon=ratio_epsilon,
            cart_abandoned_min_value=settings.cart_abandoned_min_value,
            rate_cap_min=settings.rate_cap_min,
            rate_cap_max=settings.rate_cap_max,
            return_units_max_ratio=settings.return_units_max_ratio,
        )
        try:
            failures = df.filter(pl.sql_expr(expr)).height
        except (SQLSyntaxError, SQLInterfaceError) as exc:
            issues.append(f"{name}: invalid_sql_expression ({type(exc).__name__})")
            continue
        except ComputeError as exc:
            issues.append(f"{name}: failed_to_evaluate ({type(exc).__name__})")
            continue
        if failures:
            issues.append(f"{name}: {failures} rows")
    return issues


def validate_table(
    table: str,
    enriched_path: Path | str,
    ingest_dt: str | None,
    min_rows: int | None,
    partition_key: str,
    pipeline_env: str,
    settings: PipelineConfig,
    lookback_days: int,
) -> EnrichedTableMetrics:
    table_path = join_path(enriched_path, table)
    notes: list[str] = []
    status = "PASS"
    config = settings.validation
    ratio_epsilon = settings.enriched_ratio_epsilon

    schema_snapshot: dict[str, str] = {}
    null_rates: dict[str, float] = {}
    sanity_issues: list[str] = []
    semantic_issues: list[str] = []
    prior_rows: int | None = None
    row_delta_pct: float | None = None

    resolved_partition, partition_path = resolve_partition(
        table_path, partition_key, ingest_dt, lookback_days
    )
    if partition_path is None:
        notes.append("missing_partition")
        status = "FAIL" if pipeline_env == "prod" else "WARN"
        return EnrichedTableMetrics(
            table=table,
            partition_key=partition_key,
            ingest_dt=resolved_partition,
            row_count=0,
            min_rows=min_rows,
            prior_row_count=prior_rows,
            row_delta_pct=row_delta_pct,
            schema_snapshot=schema_snapshot,
            null_rates=null_rates,
            sanity_issues=sanity_issues,
            semantic_issues=semantic_issues,
            status=status,
            notes=notes,
        )

    row_count = count_parquet_rows(partition_path)
    prior_rows, row_delta_pct = compute_row_delta(
        table_path, partition_key, resolved_partition, row_count
    )

    schema_snapshot = get_schema_snapshot(partition_path)
    key_fields = config.key_fields.get(table, [])
    if schema_snapshot:
        available = set(schema_snapshot.keys())
        semantic_columns: list[str] = []
        for check in config.semantic_checks.get(table, []):
            tokens = (
                check["expr"]
                .replace("(", " ")
                .replace(")", " ")
                .replace("/", " ")
                .replace("*", " ")
                .replace("+", " ")
                .replace("-", " ")
                .split()
            )
            for token in tokens:
                if token.isidentifier():
                    semantic_columns.append(token)

        check_columns = (
            key_fields
            + config.sanity_checks.get("non_negative", [])
            + config.sanity_checks.get("rate_0_1", [])
            + semantic_columns
        )
        selected = []
        for col in check_columns:
            if col in available and col not in selected:
                selected.append(col)
        if selected:
            df = read_parquet_safe(partition_path, columns=selected)
            if df is not None:
                if table == "int_daily_business_metrics":
                    orders_count_f = pl.col("orders_count").cast(pl.Float64)
                    carts_created_f = pl.col("carts_created").cast(pl.Float64)
                    returns_count_f = pl.col("returns_count").cast(pl.Float64)

                    cart_rate_expr = orders_count_f / carts_created_f
                    return_rate_expr = returns_count_f / orders_count_f
                    if settings.rate_cap_enabled:
                        cart_rate_expr = cart_rate_expr.clip(
                            settings.rate_cap_min, settings.rate_cap_max
                        )
                        return_rate_expr = return_rate_expr.clip(
                            settings.rate_cap_min, settings.rate_cap_max
                        )

                    df = df.with_columns(
                        expected_cart_conversion_rate=pl.when(
                            pl.col("carts_created") > 0
                        )
                        .then(cart_rate_expr.round(settings.rate_precision))
                        .otherwise(0.0),
                        expected_return_rate=pl.when(pl.col("orders_count") > 0)
                        .then(return_rate_expr.round(settings.rate_precision))
                        .otherwise(0.0),
                    )
                null_rates = compute_null_rates(df, key_fields)
                sanity_issues = evaluate_sanity_checks(df, config)
                semantic_issues = evaluate_semantic_checks(
                    df, table, config, settings, ratio_epsilon
                )

    if row_count == 0:
        notes.append("empty_partition")
        status = "FAIL" if pipeline_env == "prod" else "WARN"

    if min_rows is not None and row_count < min_rows:
        notes.append(f"below_min_rows:{row_count}<{min_rows}")
        status = "FAIL" if pipeline_env == "prod" else "WARN"

    if sanity_issues:
        notes.append("sanity_checks_failed")
        status = "FAIL" if pipeline_env == "prod" else "WARN"
    if semantic_issues:
        notes.append("semantic_checks_failed")
        status = "FAIL" if pipeline_env == "prod" else "WARN"

    return EnrichedTableMetrics(
        table=table,
        partition_key=partition_key,
        ingest_dt=resolved_partition,
        row_count=row_count,
        min_rows=min_rows,
        prior_row_count=prior_rows,
        row_delta_pct=row_delta_pct,
        schema_snapshot=schema_snapshot,
        null_rates=null_rates,
        sanity_issues=sanity_issues,
        semantic_issues=semantic_issues,
        status=status,
        notes=notes,
    )
