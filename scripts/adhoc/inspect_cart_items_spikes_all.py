#!/usr/bin/env python3
"""Deep dive on cart_items spike days across related tables."""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

TABLES = ("cart_items", "shopping_carts", "orders")


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def find_partitions(root: Path, months: set[str]) -> dict[str, dict[str, list[Path]]]:
    partitions: dict[str, dict[str, list[Path]]] = {}
    for table in TABLES:
        table_root = root / table
        if not table_root.exists():
            continue
        table_parts: dict[str, list[Path]] = {}
        for part_dir in sorted(table_root.glob("ingest_dt=*")):
            part = part_dir.name.split("=", 1)[-1]
            if months and part[:7] not in months:
                continue
            files = sorted(part_dir.glob("*.parquet"))
            if not files:
                continue
            table_parts[part] = files
        if table_parts:
            partitions[table] = table_parts
    return partitions


def count_rows(files: list[Path], max_files: int) -> int:
    total = 0
    for file_path in files[:max_files]:
        total += pl.read_parquet(file_path).height
    return total


def partition_counts(
    partitions: dict[str, dict[str, list[Path]]],
    max_files: int,
) -> dict[str, dict[str, tuple[int, int]]]:
    counts: dict[str, dict[str, tuple[int, int]]] = {}
    for table, table_parts in partitions.items():
        table_counts: dict[str, tuple[int, int]] = {}
        for part, files in table_parts.items():
            table_counts[part] = (count_rows(files, max_files), len(files))
        counts[table] = table_counts
    return counts


def pick_baselines(cart_counts: dict[str, tuple[int, int]]) -> list[str]:
    rows = sorted(cart_counts.items(), key=lambda item: item[1][0])
    if not rows:
        return []
    median_idx = len(rows) // 2
    return [rows[median_idx][0], rows[0][0]]


def read_partition_df(
    root: Path,
    table: str,
    ingest_dt: str,
    max_files: int,
    columns: list[str] | None = None,
) -> pl.DataFrame:
    table_root = root / table / f"ingest_dt={ingest_dt}"
    files = sorted(table_root.glob("*.parquet"))
    frames = []
    for file_path in files[:max_files]:
        frames.append(pl.read_parquet(file_path, columns=columns))
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal")


def describe_item_per_cart(df: pl.DataFrame) -> dict[str, float]:
    if df.is_empty() or "cart_id" not in df.columns:
        return {}
    counts = df.group_by("cart_id").len()
    stats = counts.select(
        pl.len().alias("carts"),
        pl.col("len").sum().alias("items"),
        pl.col("len").mean().alias("mean"),
        pl.col("len").median().alias("median"),
        pl.col("len").quantile(0.90, interpolation="nearest").alias("p90"),
        pl.col("len").quantile(0.95, interpolation="nearest").alias("p95"),
        pl.col("len").quantile(0.99, interpolation="nearest").alias("p99"),
        pl.col("len").max().alias("max"),
    )
    return {k: float(v) for k, v in stats.row(0, named=True).items()}


def duplicate_stats(df: pl.DataFrame) -> dict[str, float]:
    if df.is_empty():
        return {}
    stats: dict[str, float] = {}
    if "cart_item_id" in df.columns:
        total = df.height
        unique = df["cart_item_id"].n_unique()
        stats["cart_item_id_total"] = float(total)
        stats["cart_item_id_unique"] = float(unique)
        stats["cart_item_id_dupes"] = float(total - unique)
    if {"cart_id", "product_id", "added_at"}.issubset(df.columns):
        total = df.height
        unique = df.select(["cart_id", "product_id", "added_at"]).n_unique()
        stats["line_key_unique"] = float(unique)
        stats["line_key_dupes"] = float(total - unique)
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect cart_items spike days.")
    parser.add_argument(
        "--root",
        default="samples/bronze",
        help="Sample root directory (contains cart_items, orders, shopping_carts).",
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
        help="Max parquet files per partition to count.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Show top N partitions by cart_items row count.",
    )
    parser.add_argument(
        "--analyze-top",
        type=int,
        default=5,
        help="Analyze top N cart_items partitions in detail.",
    )
    parser.add_argument(
        "--focus",
        default="",
        help="Comma-separated ingest_dt values to force include in analysis.",
    )
    args = parser.parse_args()

    months = set(parse_csv(args.months))
    focus_dates = set(parse_csv(args.focus))

    root = Path(args.root)
    partitions = find_partitions(root, months)
    counts = partition_counts(partitions, args.max_files)

    cart_counts = counts.get("cart_items", {})
    if not cart_counts:
        print("No cart_items partitions found.")
        return

    cart_rows = sorted(cart_counts.items(), key=lambda item: item[1][0])
    avg = sum(r[1][0] for r in cart_rows) / len(cart_rows)
    median = cart_rows[len(cart_rows) // 2][1][0]
    print(f"partitions={len(cart_rows)} avg_rows={avg:.2f} median_rows={median}")

    top_partitions = sorted(
        cart_counts.items(), key=lambda item: item[1][0], reverse=True
    )[: args.top]
    for part, (row_count, file_count) in top_partitions:
        pct = (row_count / avg - 1) * 100
        print(f"top {part}: rows={row_count} files={file_count} (+{pct:.0f}% vs avg)")

    analysis_dates = [p for p, _ in top_partitions[: args.analyze_top]]
    analysis_dates.extend(sorted(focus_dates))
    analysis_dates.extend(pick_baselines(cart_counts))
    analysis_dates = sorted(set(analysis_dates))

    print("\nCross-table counts:")
    for date in analysis_dates:
        parts = []
        for table in TABLES:
            table_count = counts.get(table, {}).get(date)
            if table_count:
                rows, files = table_count
                parts.append(f"{table}={rows} (files={files})")
        print(f"{date}: " + ", ".join(parts))

    print("\ncart_items distribution by cart_id:")
    for date in analysis_dates:
        df = read_partition_df(
            root,
            "cart_items",
            date,
            args.max_files,
            columns=["cart_item_id", "cart_id", "product_id", "added_at"],
        )
        if df.is_empty():
            continue
        stats = describe_item_per_cart(df)
        if not stats:
            continue
        print(
            f"{date}: carts={int(stats['carts'])} items={int(stats['items'])} "
            f"mean={stats['mean']:.2f} median={stats['median']:.0f} "
            f"p90={stats['p90']:.0f} p95={stats['p95']:.0f} "
            f"p99={stats['p99']:.0f} max={stats['max']:.0f}"
        )

    print("\ncart_items duplicate checks:")
    for date in analysis_dates:
        df = read_partition_df(
            root,
            "cart_items",
            date,
            args.max_files,
            columns=["cart_item_id", "cart_id", "product_id", "added_at"],
        )
        if df.is_empty():
            continue
        stats = duplicate_stats(df)
        if not stats:
            continue
        msg = [f"{date}:"]
        if "cart_item_id_dupes" in stats:
            msg.append(
                f"cart_item_id dupes={int(stats['cart_item_id_dupes'])} "
                f"of {int(stats['cart_item_id_total'])}"
            )
        if "line_key_dupes" in stats:
            msg.append(f"line_key dupes={int(stats['line_key_dupes'])}")
        print(" ".join(msg))


if __name__ == "__main__":
    main()
