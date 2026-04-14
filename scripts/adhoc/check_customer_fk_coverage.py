#!/usr/bin/env python3
"""Check customer FK coverage for fact tables vs dims snapshot."""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl


def _glob_parquet(path: Path) -> list[str]:
    return [str(p) for p in path.rglob("*.parquet")]


def _load_customer_ids(paths: list[str]) -> pl.DataFrame:
    if not paths:
        return pl.DataFrame({"customer_id": []})
    frames: list[pl.DataFrame] = []
    for path in paths:
        try:
            frame = pl.read_parquet(path, columns=["customer_id"])
        except Exception:
            continue
        if "customer_id" not in frame.columns:
            continue
        frames.append(frame.select(pl.col("customer_id").cast(pl.Utf8)).drop_nulls())
    if not frames:
        return pl.DataFrame({"customer_id": []})
    return pl.concat(frames).unique()


def _load_base_customers(paths: list[str]) -> pl.DataFrame:
    if not paths:
        return pl.DataFrame({"customer_id": [], "signup_date": []})
    frames: list[pl.DataFrame] = []
    for path in paths:
        try:
            frame = pl.read_parquet(path, columns=["customer_id", "signup_date"])
        except Exception:
            continue
        if "customer_id" not in frame.columns:
            continue
        frames.append(
            frame.select(
                pl.col("customer_id").cast(pl.Utf8),
                pl.col("signup_date").cast(pl.Date, strict=False),
            ).drop_nulls(subset=["customer_id"])
        )
    if not frames:
        return pl.DataFrame({"customer_id": [], "signup_date": []})
    return pl.concat(frames)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check customer FK coverage for facts vs dims snapshot."
    )
    parser.add_argument("--run-date", required=True, help="Run date (YYYY-MM-DD)")
    parser.add_argument(
        "--silver-base-path",
        default="/opt/airflow/data/silver/base",
        help="Base silver path (local filesystem).",
    )
    parser.add_argument(
        "--dims-path",
        default="/opt/airflow/data/silver/dims",
        help="Dims snapshot base path (local filesystem).",
    )
    parser.add_argument(
        "--fact-tables",
        default="orders,shopping_carts,returns",
        help="Comma-separated fact tables to check.",
    )
    args = parser.parse_args()

    run_date = args.run_date
    base_path = Path(args.silver_base_path)
    dims_path = Path(args.dims_path)
    fact_tables = [t.strip() for t in args.fact_tables.split(",") if t.strip()]

    fact_files: list[str] = []
    for table in fact_tables:
        part_dir = base_path / table / f"ingestion_dt={run_date}"
        fact_files.extend(_glob_parquet(part_dir))

    dims_files = _glob_parquet(dims_path / "customers" / f"snapshot_dt={run_date}")
    base_customer_files = _glob_parquet(base_path / "customers")

    print(f"Run date: {run_date}")
    print(f"Fact tables: {', '.join(fact_tables)}")
    print(f"Fact files: {len(fact_files)}")
    print(f"Dims files: {len(dims_files)}")
    print(f"Base customer files: {len(base_customer_files)}")

    if not fact_files:
        print("No fact files found for the run date.")
        return 1
    if not dims_files:
        print("No dims snapshot files found for the run date.")
        return 1
    if not base_customer_files:
        print("No base customers found.")
        return 1

    facts = _load_customer_ids(fact_files)
    dims = _load_customer_ids(dims_files)
    base_customers = _load_base_customers(base_customer_files)

    if facts.is_empty():
        print("No customer_id values found in fact files.")
        return 1

    missing_from_dims = facts.join(dims, on="customer_id", how="anti")
    missing_from_base = facts.join(base_customers, on="customer_id", how="anti")
    joined = facts.join(base_customers, on="customer_id", how="left")
    late_signup = joined.filter(
        pl.col("signup_date").is_not_null()
        & (pl.col("signup_date") > pl.lit(run_date).cast(pl.Date))
    )

    def pct(part: int, total: int) -> str:
        return f"{(part / total * 100):.2f}%" if total else "0.00%"

    total_facts = facts.height
    print("")
    print("Coverage summary:")
    print(f"- Fact customer_ids: {total_facts}")
    print(
        f"- Missing from dims snapshot: {missing_from_dims.height} "
        f"({pct(missing_from_dims.height, total_facts)})"
    )
    print(
        f"- Missing from base customers: {missing_from_base.height} "
        f"({pct(missing_from_base.height, total_facts)})"
    )
    print(
        f"- Signup date AFTER run_date: {late_signup.height} "
        f"({pct(late_signup.height, total_facts)})"
    )

    guest_lower = facts.filter(pl.col("customer_id").str.starts_with("guest-")).height
    guest_upper = facts.filter(pl.col("customer_id").str.starts_with("GUEST-")).height
    print("")
    print("Guest ID checks:")
    print(f"- guest- prefix: {guest_lower}")
    print(f"- GUEST- prefix: {guest_upper}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
