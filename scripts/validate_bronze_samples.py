#!/usr/bin/env python3
"""Validate bronze samples against Pydantic schemas."""

from __future__ import annotations

import argparse
import os
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Type

import polars as pl
from pydantic import BaseModel, ValidationError

from src.settings import load_settings
from src.validation.schemas import CustomerRecord, OrderRecord, ProductRecord


@dataclass(frozen=True)
class ValidationResult:
    table: str
    checked: int
    failed: int
    sample_errors: list[str]


SCHEMA_MAP: dict[str, Type[BaseModel]] = {
    "customers": CustomerRecord,
    "orders": OrderRecord,
    "product_catalog": ProductRecord,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate bronze samples with Pydantic."
    )
    parser.add_argument(
        "--config",
        default="config/config.yml",
        help="Path to pipeline config YAML.",
    )
    parser.add_argument(
        "--bronze-root",
        default=None,
        help="Local bronze root path (overrides config).",
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated table list (default: schema-supported tables).",
    )
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=1000,
        help="Max rows to validate per table.",
    )
    parser.add_argument(
        "--max-fail-rate",
        type=float,
        default=0.0,
        help="Allowed failure rate per table before exiting non-zero.",
    )
    parser.add_argument(
        "--max-error-samples",
        type=int,
        default=5,
        help="Max error samples to print per table.",
    )
    return parser.parse_args()


def resolve_bronze_root(config_path: str, override: str | None) -> Path:
    if override:
        return Path(override)
    settings = load_settings(config_path)
    if settings.pipeline.bronze_bucket != "local":
        raise ValueError(
            "Non-local bronze bucket detected; pass --bronze-root for local validation."
        )
    return Path(settings.pipeline.bronze_prefix)


def iter_parquet_files(table_dir: Path) -> Iterable[Path]:
    yield from sorted(table_dir.glob("ingest_dt=*/part-*.parquet"))
    yield from sorted(table_dir.glob("ingest_dt=*/*.parquet"))
    if not any(table_dir.glob("ingest_dt=*/*.parquet")):
        yield from sorted(table_dir.glob("*.parquet"))


def sample_table(table_dir: Path, max_rows: int) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    remaining = max_rows
    for file_path in iter_parquet_files(table_dir):
        if remaining <= 0:
            break
        try:
            frames.append(pl.read_parquet(file_path, n_rows=remaining))
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Failed to read {file_path}: {exc}") from exc
        remaining = max_rows - sum(frame.height for frame in frames)
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal")


def validate_rows(
    table: str,
    schema: Type[BaseModel],
    df: pl.DataFrame,
    max_error_samples: int,
) -> ValidationResult:
    checked = 0
    failed = 0
    sample_errors: list[str] = []
    for row in df.iter_rows(named=True):
        checked += 1
        try:
            schema.model_validate(row)
        except ValidationError as exc:
            failed += 1
            if len(sample_errors) < max_error_samples:
                sample_errors.append(f"{table} row {checked}: {exc.errors()}")
    return ValidationResult(
        table=table,
        checked=checked,
        failed=failed,
        sample_errors=sample_errors,
    )


def main() -> int:
    args = parse_args()
    try:
        bronze_root = resolve_bronze_root(args.config, args.bronze_root)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if not bronze_root.exists():
        print(f"ERROR: bronze root not found: {bronze_root}", file=sys.stderr)
        return 2

    tables = (
        {name.strip() for name in args.tables.split(",") if name.strip()}
        if args.tables
        else set(SCHEMA_MAP.keys())
    )

    results: list[ValidationResult] = []
    exit_code = 0

    for table in sorted(tables):
        schema = SCHEMA_MAP.get(table)
        if not schema:
            print(f"SKIP: no schema defined for {table}")
            continue
        table_dir = bronze_root / table
        if not table_dir.exists():
            print(f"ERROR: missing table directory {table_dir}", file=sys.stderr)
            exit_code = 2
            continue

        df = sample_table(table_dir, args.sample_rows)
        if df.is_empty():
            print(f"ERROR: no parquet samples found for {table}", file=sys.stderr)
            exit_code = 2
            continue

        result = validate_rows(table, schema, df, args.max_error_samples)
        results.append(result)

        fail_rate = result.failed / result.checked if result.checked else 0.0
        status = "PASS" if fail_rate <= args.max_fail_rate else "FAIL"
        print(
            f"{status}: {table} checked={result.checked} failed={result.failed} "
            f"fail_rate={fail_rate:.4f}"
        )
        if status == "FAIL":
            exit_code = 1
            for sample in result.sample_errors:
                print(f"  sample_error: {sample}")

    return exit_code


if __name__ == "__main__":
    if not os.getenv("ECOM_CLI_SUPPRESS_DEPRECATION"):
        warnings.warn(
            (
                "Deprecated: use `ecomlake bronze validate` instead of "
                "scripts/validate_bronze_samples.py"
            ),
            UserWarning,
            stacklevel=2,
        )
    raise SystemExit(main())
