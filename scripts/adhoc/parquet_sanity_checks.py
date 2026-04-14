#!/usr/bin/env python3
"""Lightweight sanity checks for parquet shards."""

from __future__ import annotations

import argparse
import glob
from pathlib import Path
from typing import Iterable

import polars as pl


def _expand_paths(patterns: Iterable[str]) -> list[str]:
    paths: list[str] = []
    for pattern in patterns:
        matches = glob.glob(pattern)
        if matches:
            paths.extend(matches)
        else:
            paths.append(pattern)
    return sorted({p for p in paths})


def _schema_signature(schema: dict[str, pl.DataType]) -> tuple[tuple[str, str], ...]:
    return tuple((name, str(dtype)) for name, dtype in schema.items())


def main() -> int:
    parser = argparse.ArgumentParser(description="Parquet sanity checks.")
    parser.add_argument(
        "--paths",
        nargs="+",
        required=True,
        help="Parquet files or glob patterns.",
    )
    parser.add_argument(
        "--key",
        help="Column name to check uniqueness (e.g., customer_id).",
    )
    parser.add_argument(
        "--date-col",
        action="append",
        dest="date_cols",
        default=[],
        help="Date column to report min/max (repeatable).",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Limit rows read for checks (0 = no limit).",
    )

    args = parser.parse_args()
    paths = _expand_paths(args.paths)
    if not paths:
        print("No parquet files matched.")
        return 1

    missing = [p for p in paths if not Path(p).exists()]
    if missing:
        print("Missing files:")
        for path in missing:
            print(f"  - {path}")
        return 1

    total_rows = pl.scan_parquet(paths).select(pl.len().alias("rows")).collect().item()
    print(f"Files: {len(paths)}")
    print(f"Total rows: {total_rows}")

    base_schema = pl.read_parquet(paths[0], n_rows=1).schema
    base_sig = _schema_signature(base_schema)
    schema_mismatches: list[str] = []
    for path in paths[1:]:
        schema = pl.read_parquet(path, n_rows=1).schema
        if _schema_signature(schema) != base_sig:
            schema_mismatches.append(path)
    if schema_mismatches:
        print("Schema mismatches detected:")
        for path in schema_mismatches:
            print(f"  - {path}")
    else:
        print("Schema consistent across files.")

    df = pl.scan_parquet(paths)
    if args.max_rows and args.max_rows > 0:
        df = df.fetch(args.max_rows)
        print(f"Rows sampled: {df.height}")
    else:
        df = df.collect(streaming=True)

    if args.key:
        if args.key not in df.columns:
            print(f"Key column not found: {args.key}")
        else:
            unique_count = df.select(pl.col(args.key).n_unique()).item()
            duplicate_count = df.height - unique_count
            print(f"Unique {args.key}: {unique_count}")
            print(f"Duplicates: {duplicate_count}")

    for col in args.date_cols:
        if col not in df.columns:
            print(f"Date column not found: {col}")
            continue
        stats = df.select(
            pl.col(col).min().alias("min"),
            pl.col(col).max().alias("max"),
        ).to_dicts()[0]
        print(f"{col} min: {stats['min']} max: {stats['max']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
