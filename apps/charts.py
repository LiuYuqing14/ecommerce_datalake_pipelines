from __future__ import annotations

import pandas as pd
import plotly.express as px


def kpi_trend_bar(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
):
    if df.empty or x not in df.columns or y not in df.columns:
        return None

    plot_df = df.copy()
    fig = px.bar(
        plot_df,
        x=x,
        y=y,
        title=title,
    )
    fig.update_layout(
        xaxis_title="",
        yaxis_title="",
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def revenue_comparison_bar(df: pd.DataFrame):
    """
    Compare gross vs net revenue for the selected partition.
    """
    required = ["gross_revenue", "net_revenue"]
    if df.empty or any(col not in df.columns for col in required):
        return None

    plot_df = pd.DataFrame(
        {
            "metric": ["Gross Revenue", "Net Revenue"],
            "value": [
                df["gross_revenue"].sum(),
                df["net_revenue"].sum(),
            ],
        }
    )

    fig = px.bar(
        plot_df,
        x="metric",
        y="value",
        title="Gross vs Net Revenue",
    )
    fig.update_layout(
        xaxis_title="",
        yaxis_title="",
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def operational_metrics_bar(df: pd.DataFrame):
    """
    Compare carts created, returns count, refund total.
    """
    required = ["carts_created", "returns_count", "refund_total"]
    if df.empty or any(col not in df.columns for col in required):
        return None

    plot_df = pd.DataFrame(
        {
            "metric": ["Carts Created", "Returns Count", "Refund Total"],
            "value": [
                df["carts_created"].sum(),
                df["returns_count"].sum(),
                df["refund_total"].sum(),
            ],
        }
    )

    fig = px.bar(
        plot_df,
        x="metric",
        y="value",
        title="Operational Snapshot",
    )
    fig.update_layout(
        xaxis_title="",
        yaxis_title="",
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def clv_distribution_bar(
    df: pd.DataFrame,
    positive_only: bool = False,
):
    required = ["net_clv"]
    if df.empty or any(col not in df.columns for col in required):
        return None

    plot_df = df.copy()
    plot_df["net_clv"] = pd.to_numeric(
        plot_df["net_clv"],
        errors="coerce",
    ).fillna(0)

    if positive_only:
        plot_df = plot_df[plot_df["net_clv"] > 0]

    if plot_df.empty:
        return None

    bins = [-1, 0, 100, 500, 1000, 5000, float("inf")]
    labels = ["0", "1-100", "101-500", "501-1000", "1001-5000", "5000+"]
    plot_df["clv_bucket"] = pd.cut(
        plot_df["net_clv"],
        bins=bins,
        labels=labels,
    )

    agg = (
        plot_df.groupby("clv_bucket", observed=False)
        .size()
        .reset_index(name="customer_count")
    )

    title = (
        "Positive Customer CLV Distribution"
        if positive_only
        else "Customer CLV Distribution"
    )

    fig = px.bar(
        agg,
        x="clv_bucket",
        y="customer_count",
        title=title,
    )
    fig.update_layout(
        xaxis_title="CLV Bucket",
        yaxis_title="Customers",
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def customer_value_scatter(df: pd.DataFrame):
    """
    Scatter: total_spent vs order_count, colored by net_clv when available.
    """
    required = ["total_spent", "order_count", "net_clv"]
    if df.empty or any(col not in df.columns for col in required):
        return None

    plot_df = df.copy()
    for col in required:
        plot_df[col] = pd.to_numeric(plot_df[col], errors="coerce")

    hover_cols = (
        [c for c in ["customer_id"] if c in plot_df.columns]
    )

    fig = px.scatter(
        plot_df,
        x="order_count",
        y="total_spent",
        color="net_clv",
        hover_data=hover_cols,
        title="Customer Value: Spend vs Order Count",
    )
    fig.update_layout(
        xaxis_title="Order Count",
        yaxis_title="Total Spent",
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def top_customers_bar(df: pd.DataFrame, top_n: int = 10):
    required = ["customer_id", "net_clv"]
    if df.empty or any(col not in df.columns for col in required):
        return None

    plot_df = df.copy()
    plot_df["net_clv"] = pd.to_numeric(plot_df["net_clv"], errors="coerce")
    plot_df = plot_df.sort_values("net_clv", ascending=False).head(top_n)

    fig = px.bar(
        plot_df,
        x="net_clv",
        y="customer_id",
        orientation="h",
        title=f"Top {top_n} Customers by Net CLV",
    )
    fig.update_layout(
        xaxis_title="Net CLV",
        yaxis_title="Customer ID",
        margin=dict(l=20, r=20, t=50, b=20),
        yaxis={"categoryorder": "total ascending"},
    )
    return fig


def order_count_distribution_bar(df: pd.DataFrame):
    required = ["order_count"]
    if df.empty or any(col not in df.columns for col in required):
        return None

    plot_df = df.copy()
    plot_df["order_count"] = pd.to_numeric(
        plot_df["order_count"],
        errors="coerce",
    ).fillna(0)

    agg = (
        plot_df.groupby("order_count")
        .size()
        .reset_index(name="customer_count")
        .sort_values("order_count")
    )

    fig = px.bar(
        agg,
        x="order_count",
        y="customer_count",
        title="Order Count Distribution",
    )
    fig.update_layout(
        xaxis_title="Order Count",
        yaxis_title="Customers",
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def signup_trend_bar(df: pd.DataFrame):
    if df.empty or "signup_date" not in df.columns:
        return None

    plot_df = df.copy()
    plot_df["signup_date"] = pd.to_datetime(
        plot_df["signup_date"],
        errors="coerce",
    )
    plot_df = plot_df.dropna(subset=["signup_date"])

    if plot_df.empty:
        return None

    plot_df["signup_day"] = plot_df["signup_date"].dt.date
    agg = plot_df.groupby("signup_day").size().reset_index(name="customers")

    fig = px.bar(
        agg,
        x="signup_day",
        y="customers",
        title="Customer Signup Trend",
    )
    fig.update_layout(
        xaxis_title="Signup Date",
        yaxis_title="Customers",
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def top_products_bar(df: pd.DataFrame, top_n: int = 10):
    if (
        df.empty
        or "product_id" not in df.columns
        or "net_revenue" not in df.columns
    ):
        return None

    plot_df = df.copy()
    plot_df["net_revenue"] = pd.to_numeric(
        plot_df["net_revenue"],
        errors="coerce",
    )
    plot_df = plot_df.sort_values(
        "net_revenue",
        ascending=False,
    ).head(top_n)

    label_col = (
        "product_name"
        if "product_name" in plot_df.columns
        else "product_id"
    )

    hover_data = (
        ["product_id", "category"]
        if "category" in plot_df.columns
        else ["product_id"]
    )

    fig = px.bar(
        plot_df,
        x="net_revenue",
        y=label_col,
        orientation="h",
        title=f"Top {top_n} Products by Net Revenue",
        hover_data=hover_data,
    )
    fig.update_layout(
        xaxis_title="Net Revenue",
        yaxis_title="Product",
        margin=dict(l=20, r=20, t=50, b=20),
        yaxis={"categoryorder": "total ascending"},
    )
    return fig


def category_revenue_bar(df: pd.DataFrame):
    if (
        df.empty
        or "category" not in df.columns
        or "net_revenue" not in df.columns
    ):
        return None

    plot_df = df.copy()
    plot_df["net_revenue"] = pd.to_numeric(
        plot_df["net_revenue"],
        errors="coerce",
    )

    agg = (
        plot_df.groupby("category", as_index=False)["net_revenue"]
        .sum()
        .sort_values("net_revenue", ascending=False)
        .head(10)
    )

    fig = px.bar(
        agg,
        x="category",
        y="net_revenue",
        title="Top Categories by Net Revenue",
    )
    fig.update_layout(
        xaxis_title="Category",
        yaxis_title="Net Revenue",
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def return_rate_by_category(df: pd.DataFrame):
    if (
        df.empty
        or "category" not in df.columns
        or "return_rate" not in df.columns
    ):
        return None

    plot_df = df.copy()
    plot_df["return_rate"] = pd.to_numeric(
        plot_df["return_rate"],
        errors="coerce",
    )

    agg = (
        plot_df.groupby("category", as_index=False)["return_rate"]
        .mean()
        .sort_values("return_rate", ascending=False)
        .head(10)
    )

    fig = px.bar(
        agg,
        x="category",
        y="return_rate",
        title="Top Categories by Return Rate",
    )
    fig.update_layout(
        xaxis_title="Category",
        yaxis_title="Avg Return Rate",
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def sales_velocity_distribution(df: pd.DataFrame):
    if df.empty or "velocity_avg" not in df.columns:
        return None

    plot_df = df.copy()
    plot_df["velocity_avg"] = pd.to_numeric(
        plot_df["velocity_avg"],
        errors="coerce",
    )

    fig = px.histogram(
        plot_df,
        x="velocity_avg",
        nbins=20,
        title="Sales Velocity Distribution",
    )
    fig.update_layout(
        xaxis_title="Velocity Avg",
        yaxis_title="Count",
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def inventory_risk_distribution(df: pd.DataFrame):
    if df.empty or "risk_tier" not in df.columns:
        return None

    agg = (
        df.groupby("risk_tier", as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )

    fig = px.bar(
        agg,
        x="risk_tier",
        y="count",
        title="Inventory Risk Tier Distribution",
    )
    fig.update_layout(
        xaxis_title="Risk Tier",
        yaxis_title="Products",
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig


def top_attention_products(df: pd.DataFrame, top_n: int = 10):
    if (
        df.empty
        or "product_id" not in df.columns
        or "attention_score" not in df.columns
    ):
        return None

    plot_df = df.copy()
    plot_df["attention_score"] = pd.to_numeric(
        plot_df["attention_score"],
        errors="coerce",
    )
    plot_df = plot_df.sort_values(
        "attention_score",
        ascending=False,
    ).head(top_n)

    label_col = (
        "product_name"
        if "product_name" in plot_df.columns
        else "product_id"
    )

    hover_data = (
        ["product_id", "category"]
        if "category" in plot_df.columns
        else ["product_id"]
    )

    fig = px.bar(
        plot_df,
        x="attention_score",
        y=label_col,
        orientation="h",
        title=f"Top {top_n} Products Requiring Attention",
        hover_data=hover_data,
    )
    fig.update_layout(
        xaxis_title="Attention Score",
        yaxis_title="Product",
        margin=dict(l=20, r=20, t=50, b=20),
        yaxis={"categoryorder": "total ascending"},
    )
    return fig


def revenue_vs_inventory_scatter(df: pd.DataFrame):
    required = ["net_revenue", "inventory_quantity"]
    if df.empty or any(col not in df.columns for col in required):
        return None

    plot_df = df.copy()
    plot_df["net_revenue"] = pd.to_numeric(
        plot_df["net_revenue"],
        errors="coerce",
    )
    plot_df["inventory_quantity"] = pd.to_numeric(
        plot_df["inventory_quantity"],
        errors="coerce",
    )

    # 只显示正收入商品，图会干净很多
    plot_df = plot_df[plot_df["net_revenue"] > 0]

    if plot_df.empty:
        return None

    hover_cols = ["product_id"]
    if "product_name" in plot_df.columns:
        hover_cols.append("product_name")
    if "category" in plot_df.columns:
        hover_cols.append("category")

    color_col = "return_rate" if "return_rate" in plot_df.columns else None

    fig = px.scatter(
        plot_df,
        x="inventory_quantity",
        y="net_revenue",
        color=color_col,
        hover_data=hover_cols,
        title="Net Revenue vs Inventory Quantity",
    )
    fig.update_layout(
        xaxis_title="Inventory Quantity",
        yaxis_title="Net Revenue",
        margin=dict(l=20, r=20, t=50, b=20),
    )
    return fig