#!/usr/bin/env python3
"""Scan parquet files under a run date and summarize by table."""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
from typing import Iterable


def _require_fsspec():
    try:
        import fsspec  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "fsspec is required to scan gs:// paths. Install gcsfs or fsspec."
        ) from exc
    return fsspec


def _list_local_files(base_path: str) -> list[str]:
    base = Path(base_path)
    if not base.exists():
        return []
    return [str(p) for p in base.rglob("*.parquet")]


def _list_gcs_files(base_path: str) -> list[str]:
    fsspec = _require_fsspec()
    fs = fsspec.filesystem("gcs")
    return fs.glob(f"{base_path.rstrip('/')}/**/*.parquet")


def _group_by_table(paths: Iterable[str], run_date: str) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = defaultdict(list)
    for path in paths:
        parts = path.split("/")
        idx = None
        for i, part in enumerate(parts):
            if part.endswith(f"={run_date}"):
                idx = i
                break
        if idx is None:
            continue
        table = parts[idx - 1] if idx > 0 else "unknown"
        grouped[table].append(path)
    return grouped


def _row_count(path: str) -> int:
    try:
        import pyarrow.parquet as pq  # type: ignore
    except Exception as exc:
        raise RuntimeError("pyarrow is required for row counting") from exc
    return pq.ParquetFile(path).metadata.num_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan parquet files by run date.")
    parser.add_argument(
        "--base-path",
        required=True,
        help="Base path (local or gs://...) containing parquet partitions.",
    )
    parser.add_argument(
        "--run-date",
        required=True,
        help="Partition date to scan (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        help="Optional table names to include (space-separated).",
    )
    parser.add_argument(
        "--key",
        default="customer_id",
        help="Column name to check for duplicates.",
    )
    parser.add_argument(
        "--check-duplicates",
        action="store_true",
        help="Compute duplicate counts for --key per table.",
    )
    parser.add_argument(
        "--row-counts",
        action="store_true",
        help="Sum row counts per table using parquet metadata.",
    )
    parser.add_argument(
        "--print-schemas",
        action="store_true",
        help="Print schema for the first file per table.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Limit rows read per table for duplicate checks (0 = no limit).",
    )
    args = parser.parse_args()

    if args.base_path.startswith("gs://"):
        paths = _list_gcs_files(args.base_path)
    else:
        paths = _list_local_files(args.base_path)

    grouped = _group_by_table(paths, args.run_date)
    if not grouped:
        print("No parquet files found for run date.")
        return 1

    if args.tables:
        allowed = set(args.tables)
        grouped = {k: v for k, v in grouped.items() if k in allowed}
        if not grouped:
            print("No parquet files matched the requested tables.")
            return 1

    print(f"Run date: {args.run_date}")
    print(f"Tables: {len(grouped)}")

    for table in sorted(grouped.keys()):
        files = grouped[table]
        print(f"\n{table}")
        print(f"  files: {len(files)}")
        if args.row_counts:
            total_rows = 0
            for path in files:
                total_rows += _row_count(path)
            print(f"  rows: {total_rows}")
        if args.print_schemas:
            import polars as pl

            schema = pl.read_parquet(files[0], n_rows=1).schema
            print(f"  schema: {schema}")
        if args.check_duplicates:
            import polars as pl

            df = pl.scan_parquet(files)
            if args.max_rows and args.max_rows > 0:
                df = df.fetch(args.max_rows)
            else:
                df = df.collect(streaming=True)
            if args.key not in df.columns:
                print(f"  key missing: {args.key}")
            else:
                unique_count = df.select(pl.col(args.key).n_unique()).item()
                duplicate_count = df.height - unique_count
                print(f"  unique_{args.key}: {unique_count}")
                print(f"  duplicate_{args.key}: {duplicate_count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
