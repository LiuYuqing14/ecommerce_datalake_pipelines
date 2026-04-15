from __future__ import annotations

import argparse
from datetime import datetime

import pandas as pd

from ml.feature_prep import latest_partition, load_parquet_table, to_numeric_safe, write_partitioned_parquet


def build_customer_segments(partition_value: str | None = None) -> pd.DataFrame:
    part = partition_value or latest_partition("int_customer_lifetime_value")
    if not part:
        raise ValueError("No partition found for int_customer_lifetime_value")

    df = load_parquet_table("int_customer_lifetime_value", part)
    if df.empty:
        raise ValueError(f"int_customer_lifetime_value is empty for partition {part}")

    df = to_numeric_safe(df, ["net_clv", "total_spent", "order_count", "avg_order_value"])

    if "avg_order_value" not in df.columns:
        spent = pd.to_numeric(df.get("total_spent", 0), errors="coerce").fillna(0)
        orders = pd.to_numeric(df.get("order_count", 0), errors="coerce").replace(0, pd.NA)
        df["avg_order_value"] = (spent / orders).fillna(0)

    def segment(row: pd.Series) -> str:
        clv = row.get("net_clv", 0) or 0
        orders = row.get("order_count", 0) or 0
        aov = row.get("avg_order_value", 0) or 0

        if clv >= 1000 and orders >= 3:
            return "champions"
        if clv >= 500:
            return "high_value"
        if orders >= 3 and aov < 80:
            return "loyal_low_aov"
        if orders <= 1:
            return "new_or_one_time"
        return "core"

    df["customer_segment"] = df.apply(segment, axis=1)
    df["segment_version"] = "rules_v1"
    df["segmented_at"] = datetime.utcnow().isoformat()

    return df


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--partition", default=None, help="Partition like 2020-01-05")
    args = parser.parse_args()

    part = args.partition or latest_partition("int_customer_lifetime_value")
    if not part:
        raise ValueError("No partition found")

    segmented = build_customer_segments(part)
    out_file = write_partitioned_parquet(
        segmented,
        table_name="int_customer_segments",
        partition_value=part,
        partition_key="date",
    )
    print(f"Wrote customer segments to {out_file}")


if __name__ == "__main__":
    main()