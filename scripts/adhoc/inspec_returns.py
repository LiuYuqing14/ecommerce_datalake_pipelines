#!/usr/bin/env python3
from pathlib import Path

import polars as pl

root = Path("samples/bronze")
months = {"2020-03", "2023-01", "2025-10"}


def first_parquet(table):
    for part_dir in sorted((root / table).glob("ingest_dt=*")):
        files = sorted(part_dir.glob("*.parquet"))
        if files:
            return files[0]
    return None


def load_table(table, cols):
    rows = []
    for part_dir in sorted((root / table).glob("ingest_dt=*")):
        part = part_dir.name.split("=", 1)[-1]
        if part[:7] not in months:
            continue
        files = sorted(part_dir.glob("*.parquet"))
        if not files:
            continue
        df = pl.read_parquet(files[0], columns=cols)
        df = df.with_columns(pl.lit(part).alias("ingest_dt"))
        rows.append(df)
    return pl.concat(rows) if rows else pl.DataFrame()


def main() -> None:
    for table, id_cols in [
        ("returns", ["return_id", "order_id", "customer_id"]),
        ("return_items", ["return_item_id", "return_id", "order_id", "product_id"]),
    ]:
        first = first_parquet(table)
        if not first:
            print(f"{table}: no data")
            continue
        available = pl.read_parquet(first, n_rows=1).columns
        cols = [c for c in id_cols if c in available]
        df = load_table(table, cols)
        if df.is_empty():
            print(f"{table}: no data")
            continue
        print(f"== {table} ==")
        print("rows:", df.height)
        for col in [c for c in id_cols if c in df.columns]:
            print(col, "distinct:", df[col].n_unique())
            repeats = (
                df.group_by(col)
                .agg(pl.n_unique("ingest_dt").alias("partitions"))
                .filter(pl.col("partitions") > 1)
                .sort("partitions", descending=True)
                .head(5)
            )
            print("top repeated IDs across partitions:")
            print(repeats)
        print("")


if __name__ == "__main__":
    main()
