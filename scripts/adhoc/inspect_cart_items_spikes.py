#!/usr/bin/env python3
"""Inspect cart_items partition row counts for spike validation."""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl


def parse_months(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect cart_items partition spikes.")
    parser.add_argument(
        "--root",
        default="samples/bronze/cart_items",
        help="cart_items sample root directory",
    )
    parser.add_argument(
        "--months",
        default="",
        help="Comma-separated months to include (YYYY-MM). Empty means all.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=1,
        help=(
            "Max parquet files per partition to count "
            "(match describe_parquet_samples.py)."
        ),
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Show top N partitions by row count.",
    )
    parser.add_argument(
        "--focus",
        default="2020-03-26,2020-03-27",
        help="Comma-separated ingest_dt values to highlight.",
    )
    args = parser.parse_args()

    root = Path(args.root)
    months = parse_months(args.months)
    focus_dates = parse_months(args.focus)

    counts: list[tuple[str, int, int]] = []
    for part_dir in sorted(root.glob("ingest_dt=*")):
        part = part_dir.name.split("=", 1)[-1]
        if months and part[:7] not in months:
            continue
        files = sorted(part_dir.glob("*.parquet"))
        if not files:
            continue
        file_sample = files[: args.max_files]
        row_count = 0
        for file_path in file_sample:
            row_count += pl.read_parquet(file_path).height
        counts.append((part, row_count, len(files)))

    if not counts:
        print("No partitions found for cart_items.")
        return

    rows = [c[1] for c in counts]
    avg = sum(rows) / len(rows)
    median = sorted(rows)[len(rows) // 2]
    print(f"partitions={len(rows)} avg_rows={avg:.2f} median_rows={median}")

    for part, row_count, file_count in sorted(counts):
        if part in focus_dates:
            pct = (row_count / avg - 1) * 100
            print(
                f"focus {part}: rows={row_count} files={file_count} "
                f"(+{pct:.0f}% vs avg)"
            )

    for part, row_count, file_count in sorted(counts, key=lambda x: x[1], reverse=True)[
        : args.top
    ]:
        pct = (row_count / avg - 1) * 100
        print(f"top {part}: rows={row_count} files={file_count} (+{pct:.0f}% vs avg)")


if __name__ == "__main__":
    main()
