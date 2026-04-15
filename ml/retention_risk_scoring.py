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


def build_retention_scores(partition_value: str | None = None) -> pd.DataFrame:
    part = partition_value or latest_partition("int_customer_lifetime_value")
    if not part:
        raise ValueError("No partition found for int_customer_lifetime_value")

    clv = load_parquet_table("int_customer_lifetime_value", part)
    if clv.empty:
        raise ValueError(f"int_customer_lifetime_value is empty for partition {part}")

    df = clv.copy()

    candidate_numeric = [
        "total_spent",
        "total_refunded",
        "net_clv",
        "order_count",
        "avg_order_value",
        "days_since_last_order",
        "refund_rate",
    ]
    df = to_numeric_safe(df, candidate_numeric)

    # fallback columns
    if "refund_rate" not in df.columns:
        refunded = df["total_refunded"] if "total_refunded" in df.columns else 0
        spent = df["total_spent"] if "total_spent" in df.columns else 0
        denom = pd.to_numeric(spent, errors="coerce").replace(0, pd.NA)
        df["refund_rate"] = (pd.to_numeric(refunded, errors="coerce") / denom).fillna(0)

    if "avg_order_value" not in df.columns:
        spent = pd.to_numeric(df.get("total_spent", 0), errors="coerce").fillna(0)
        orders = pd.to_numeric(df.get("order_count", 0), errors="coerce").replace(0, pd.NA)
        df["avg_order_value"] = (spent / orders).fillna(0)

    # Risk components: high refunds, low spend, low order count, low/negative CLV, long recency
    low_spend_risk = 1 - min_max_scale(df.get("total_spent", pd.Series(0, index=df.index)))
    low_order_risk = 1 - min_max_scale(df.get("order_count", pd.Series(0, index=df.index)))
    clv_risk = 1 - min_max_scale(df.get("net_clv", pd.Series(0, index=df.index)))
    refund_risk = min_max_scale(df.get("refund_rate", pd.Series(0, index=df.index)))

    if "days_since_last_order" in df.columns:
        recency_risk = min_max_scale(df["days_since_last_order"])
    else:
        recency_risk = pd.Series([0.25] * len(df), index=df.index)

    df["retention_risk_score"] = (
        0.30 * clv_risk
        + 0.20 * low_order_risk
        + 0.20 * low_spend_risk
        + 0.15 * refund_risk
        + 0.15 * recency_risk
    ).round(4)

    def label(score: float) -> str:
        if score >= 0.75:
            return "high"
        if score >= 0.45:
            return "medium"
        return "low"

    df["retention_risk_tier"] = df["retention_risk_score"].apply(label)

    # action-oriented segmentation
    def action(row: pd.Series) -> str:
        if row["retention_risk_tier"] == "high":
            return "win_back"
        if row["retention_risk_tier"] == "medium":
            return "nurture"
        return "retain"

    df["recommended_action"] = df.apply(action, axis=1)
    df["model_version"] = "heuristic_v1"
    df["scored_at"] = datetime.utcnow().isoformat()

    preferred_cols = [
        "customer_id",
        "email",
        "first_name",
        "last_name",
        "signup_date",
        "order_count",
        "total_spent",
        "total_refunded",
        "net_clv",
        "avg_order_value",
        "refund_rate",
        "days_since_last_order",
        "retention_risk_score",
        "retention_risk_tier",
        "recommended_action",
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

    part = args.partition or latest_partition("int_customer_lifetime_value")
    if not part:
        raise ValueError("No partition found to score")

    scored = build_retention_scores(part)
    out_file = write_partitioned_parquet(
        scored,
        table_name="int_customer_retention_signals",
        partition_value=part,
        partition_key="date",
    )
    print(f"Wrote retention scores to {out_file}")


if __name__ == "__main__":
    main()