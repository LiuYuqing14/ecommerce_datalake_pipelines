from __future__ import annotations

import argparse
from datetime import datetime

import pandas as pd

from ml.feature_prep import (
    latest_partition,
    load_parquet_table,
    min_max_scale,
    to_numeric_safe,
    write_partitioned_parquet,
)


def build_inventory_risk(partition_value: str | None = None) -> pd.DataFrame:
    part = partition_value or latest_partition("int_product_performance")
    if not part:
        raise ValueError("No partition found for int_product_performance")

    product_df = load_parquet_table("int_product_performance", part)
    if product_df.empty:
        raise ValueError(f"int_product_performance is empty for partition {part}")

    velocity_df = load_parquet_table("int_sales_velocity", part)

    df = product_df.copy()

    if not velocity_df.empty and "product_id" in velocity_df.columns and "product_id" in df.columns:
        keep_cols = [c for c in ["product_id", "velocity_avg"] if c in velocity_df.columns]
        df = df.merge(
            velocity_df[keep_cols].drop_duplicates(subset=["product_id"]),
            on="product_id",
            how="left",
        )

    numeric_cols = [
        "inventory_quantity",
        "units_sold",
        "units_returned",
        "net_revenue",
        "gross_profit",
        "margin_pct",
        "return_rate",
        "velocity_avg",
        "unit_price",
        "unit_cost",
    ]
    df = to_numeric_safe(df, numeric_cols)

    if "sales_volume" not in df.columns:
        df["sales_volume"] = df["units_sold"] if "units_sold" in df.columns else 0

    if "return_volume" not in df.columns:
        df["return_volume"] = df["units_returned"] if "units_returned" in df.columns else 0

    if "utilization_ratio" not in df.columns:
        inv = pd.to_numeric(df.get("inventory_quantity", 0), errors="coerce").fillna(0)
        sold = pd.to_numeric(df.get("sales_volume", 0), errors="coerce").fillna(0)
        denom = (inv + sold).replace(0, pd.NA)
        df["utilization_ratio"] = (sold / denom).fillna(0)

    if "return_signal" not in df.columns:
        if "return_rate" in df.columns:
            df["return_signal"] = df["return_rate"].fillna(0)
        else:
            sold = pd.to_numeric(df.get("sales_volume", 0), errors="coerce").replace(0, pd.NA)
            ret = pd.to_numeric(df.get("return_volume", 0), errors="coerce").fillna(0)
            df["return_signal"] = (ret / sold).fillna(0)

    if "locked_capital" not in df.columns:
        inv = pd.to_numeric(df.get("inventory_quantity", 0), errors="coerce").fillna(0)
        if "unit_cost" in df.columns:
            unit_cost = pd.to_numeric(df["unit_cost"], errors="coerce").fillna(0)
        elif "unit_price" in df.columns:
            unit_cost = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0) * 0.6
        else:
            unit_cost = pd.Series([0.0] * len(df), index=df.index)
        df["locked_capital"] = inv * unit_cost

    overstock_risk = min_max_scale(df.get("inventory_quantity", pd.Series(0, index=df.index)))
    low_velocity_risk = 1 - min_max_scale(df.get("velocity_avg", pd.Series(0, index=df.index)))
    high_return_risk = min_max_scale(df.get("return_signal", pd.Series(0, index=df.index)))
    low_margin_risk = 1 - min_max_scale(df.get("margin_pct", pd.Series(0, index=df.index)))
    low_utilization_risk = 1 - min_max_scale(df.get("utilization_ratio", pd.Series(0, index=df.index)))
    capital_risk = min_max_scale(df.get("locked_capital", pd.Series(0, index=df.index)))

    df["attention_score"] = (
        0.25 * overstock_risk
        + 0.20 * low_velocity_risk
        + 0.20 * high_return_risk
        + 0.15 * low_margin_risk
        + 0.10 * low_utilization_risk
        + 0.10 * capital_risk
    ).round(4)

    def risk_tier(score: float) -> str:
        if score >= 0.75:
            return "high"
        if score >= 0.45:
            return "medium"
        return "low"

    df["risk_tier"] = df["attention_score"].apply(risk_tier)
    df["model_version"] = "heuristic_v1"
    df["scored_at"] = datetime.utcnow().isoformat()

    preferred_cols = [
        "product_id",
        "product_name",
        "category",
        "inventory_quantity",
        "sales_volume",
        "return_volume",
        "velocity_avg",
        "utilization_ratio",
        "return_signal",
        "locked_capital",
        "attention_score",
        "risk_tier",
        "net_revenue",
        "gross_profit",
        "margin_pct",
        "model_version",
        "scored_at",
    ]
    output_cols = [c for c in preferred_cols if c in df.columns] + [
        c for c in df.columns if c not in preferred_cols
    ]

    return df[output_cols]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--partition", default=None, help="Partition like 2020-01-05")
    args = parser.parse_args()

    part = args.partition or latest_partition("int_product_performance")
    if not part:
        raise ValueError("No partition found to score")

    scored = build_inventory_risk(part)
    out_file = write_partitioned_parquet(
        scored,
        table_name="int_inventory_risk",
        partition_value=part,
        partition_key="date",
    )
    print(f"Wrote inventory risk ranking to {out_file}")


if __name__ == "__main__":
    main()