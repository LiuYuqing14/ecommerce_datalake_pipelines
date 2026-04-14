from __future__ import annotations

import pandas as pd
import streamlit as st

from charts import (
    revenue_comparison_bar,
    operational_metrics_bar,
    clv_distribution_bar,
    customer_value_scatter,
    top_customers_bar,
    order_count_distribution_bar,
    top_products_bar,
    category_revenue_bar,
    sales_velocity_distribution,
    return_rate_by_category,
    inventory_risk_distribution,
    top_attention_products,
    revenue_vs_inventory_scatter,
)

from data_loader import (
    latest_partition,
    load_customer_lifetime_value,
    load_customer_retention_signals,
    load_daily_business_metrics,
    load_inventory_risk,
    load_product_performance,
    load_regional_financials,
    load_sales_velocity,
    load_shipping_economics,
    preview_table_info,
)


st.set_page_config(
    page_title="Retail Analytics App",
    page_icon="📊",
    layout="wide",
)

st.title("Retail Analytics App")
st.caption("Built on top of the enriched layer from the ecom_datalake_pipelines repo.")


def safe_metric(df: pd.DataFrame, column: str, agg: str = "sum", fmt: str = "number") -> str:
    if df.empty or column not in df.columns:
        return "N/A"

    if agg == "count":
        value = df[column].notna().sum()
    else:
        series = pd.to_numeric(df[column], errors="coerce")

        if agg == "sum":
            value = series.sum()
        elif agg == "mean":
            value = series.mean()
        else:
            return "N/A"

    if pd.isna(value):
        return "N/A"

    if fmt == "pct":
        return f"{value:.2%}"
    if isinstance(value, (int, float)) and abs(value) >= 1000:
        return f"{value:,.0f}"
    return f"{value:,.2f}" if isinstance(value, float) else str(value)

def count_threshold(df: pd.DataFrame, column: str, threshold: float) -> str:
    if df.empty or column not in df.columns:
        return "N/A"

    series = pd.to_numeric(df[column], errors="coerce")
    value = (series > threshold).sum()
    return f"{value:,}"

def count_positive(df: pd.DataFrame, column: str) -> str:
    if df.empty or column not in df.columns:
        return "N/A"
    series = pd.to_numeric(df[column], errors="coerce")
    value = (series > 0).sum()
    return f"{value:,}"


def count_zero(df: pd.DataFrame, column: str) -> str:
    if df.empty or column not in df.columns:
        return "N/A"
    series = pd.to_numeric(df[column], errors="coerce")
    value = (series == 0).sum()
    return f"{value:,}"


def median_metric(df: pd.DataFrame, column: str) -> str:
    if df.empty or column not in df.columns:
        return "N/A"
    series = pd.to_numeric(df[column], errors="coerce")
    value = series.median()
    if pd.isna(value):
        return "N/A"
    return f"{value:,.2f}"

def safe_metric_if_col(df: pd.DataFrame, column: str | None, agg: str = "sum", fmt: str = "number") -> str:
    if not column:
        return "N/A"
    return safe_metric(df, column, agg, fmt)

def count_equals(df: pd.DataFrame, column: str, target: str) -> str:
    if df.empty or column not in df.columns:
        return "N/A"
    value = (df[column].astype(str) == target).sum()
    return f"{value:,}"


def first_available_sum(df: pd.DataFrame, candidates: list[str]) -> str:
    for col in candidates:
        if col in df.columns:
            return safe_metric(df, col, "sum")
    return "N/A"


def first_available_mean(df: pd.DataFrame, candidates: list[str]) -> str:
    for col in candidates:
        if col in df.columns:
            return safe_metric(df, col, "mean")
    return "N/A"

def count_risk_threshold(df: pd.DataFrame, candidates: list[str], threshold: float) -> str:
    if df.empty:
        return "N/A"

    value_col = next((c for c in candidates if c in df.columns), None)
    if value_col is None:
        return "N/A"

    series = pd.to_numeric(df[value_col], errors="coerce")
    value = (series >= threshold).sum()
    return f"{value:,}"

def count_equals_case_insensitive(df: pd.DataFrame, column: str, target: str) -> str:
    if df.empty or column not in df.columns:
        return "N/A"
    value = (df[column].astype(str).str.lower() == target.lower()).sum()
    return f"{value:,}"


def first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None

# ----------------------------
# Sidebar
# ----------------------------
st.sidebar.header("Controls")

default_partition = latest_partition("int_daily_business_metrics")
partition_value = st.sidebar.text_input(
    "Partition (YYYY-MM-DD)",
    value=default_partition or "2020-01-05",
    help="Used for partitioned enriched tables.",
)

page = st.sidebar.radio(
    "Page",
    options=[
        "Overview",
        "Customer Intelligence",
        "Product & Inventory",
        "Operations & Shipping",
        "Regional Finance",
        "Data Debug",
    ],
)


# ----------------------------
# Data loading
# ----------------------------
daily_df = load_daily_business_metrics(partition_value)
customer_ltv_df = load_customer_lifetime_value(partition_value)
retention_df = load_customer_retention_signals(partition_value)
product_df = load_product_performance(partition_value)
velocity_df = load_sales_velocity(partition_value)
shipping_df = load_shipping_economics(partition_value)
regional_df = load_regional_financials(partition_value)
inventory_df = load_inventory_risk(partition_value)


# ----------------------------
# Page: Overview
# ----------------------------
if page == "Overview":
    st.subheader("Overview")

    # ----------------------------
    # KPI cards
    # ----------------------------
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total Revenue", safe_metric(daily_df, "gross_revenue", "sum"))
    with c2:
        st.metric("Net Revenue", safe_metric(daily_df, "net_revenue", "sum"))
    with c3:
        st.metric("Orders", safe_metric(daily_df, "orders_count", "sum"))
    with c4:
        st.metric("Avg Order Value", safe_metric(daily_df, "avg_order_value", "mean"))

    c5, c6, c7, c8 = st.columns(4)
    with c5:
        st.metric("Carts Created", safe_metric(daily_df, "carts_created", "sum"))
    with c6:
        st.metric(
            "Cart Conversion Rate",
            safe_metric(daily_df, "cart_conversion_rate", "mean", fmt="pct"),
        )
    with c7:
        st.metric("Returns Count", safe_metric(daily_df, "returns_count", "sum"))
    with c8:
        st.metric(
            "Return Rate",
            safe_metric(daily_df, "return_rate", "mean", fmt="pct"),
        )

    # ----------------------------
    # Charts
    # ----------------------------
    st.markdown("### Performance Snapshot")

    left, right = st.columns(2)

    with left:
        revenue_fig = revenue_comparison_bar(daily_df)
        if revenue_fig is not None:
            st.plotly_chart(revenue_fig, use_container_width=True)
        else:
            st.info("Revenue chart unavailable.")

    with right:
        ops_fig = operational_metrics_bar(daily_df)
        if ops_fig is not None:
            st.plotly_chart(ops_fig, use_container_width=True)
        else:
            st.info("Operational chart unavailable.")

    # ----------------------------
    # Daily metrics table
    # ----------------------------
    st.markdown("### Daily Business Metrics")
    if daily_df.empty:
        st.warning("No data found for int_daily_business_metrics.")
    else:
        preferred_cols = [
            "date",
            "orders_count",
            "gross_revenue",
            "net_revenue",
            "avg_order_value",
            "carts_created",
            "cart_conversion_rate",
            "returns_count",
            "return_rate",
            "refund_total",
        ]
        display_cols = [c for c in preferred_cols if c in daily_df.columns]
        st.dataframe(daily_df[display_cols], use_container_width=True)

    # ----------------------------
    # Shipping snapshot
    # ----------------------------
    st.markdown("### Shipping Snapshot")
    if shipping_df.empty:
        st.info("No shipping economics data available.")
    else:
        preview_cols = [
            c
            for c in [
                "order_id",
                "shipping_speed",
                "shipping_cost",
                "actual_shipping_cost",
                "net_total",
            ]
            if c in shipping_df.columns
        ]
        st.dataframe(shipping_df[preview_cols].head(20), use_container_width=True)

# ----------------------------
# Page: Customer Intelligence
# ----------------------------
elif page == "Customer Intelligence":
    st.subheader("Customer Intelligence")

    # ----------------------------
    # KPI cards
    # ----------------------------
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Customers", safe_metric(customer_ltv_df, "customer_id", "count"))
    with c2:
        st.metric("Median CLV", median_metric(customer_ltv_df, "net_clv"))
    with c3:
        st.metric("Avg Orders / Customer", safe_metric(customer_ltv_df, "order_count", "mean"))
    with c4:
        st.metric("Retention Signals Rows", safe_metric(retention_df, "customer_id", "count"))

    c5, c6, c7, c8 = st.columns(4)
    with c5:
        st.metric("Total Customer Spend", safe_metric(customer_ltv_df, "total_spent", "sum"))
    with c6:
        st.metric("Total Refunded", safe_metric(customer_ltv_df, "total_refunded", "sum"))
    with c7:
        st.metric("Positive CLV Customers", count_positive(customer_ltv_df, "net_clv"))
    with c8:
        st.metric("Zero-CLV Customers", count_zero(customer_ltv_df, "net_clv"))

    # ----------------------------
    # Charts row 1
    # ----------------------------
    st.markdown("### Customer Value Snapshot")

    left, right = st.columns(2)

    with left:
        fig1 = clv_distribution_bar(customer_ltv_df, positive_only=True)
        if fig1 is not None:
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("CLV distribution chart unavailable.")

    with right:
        fig2 = customer_value_scatter(customer_ltv_df)
        if fig2 is not None:
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Customer value scatter unavailable.")

    # ----------------------------
    # Charts row 2
    # ----------------------------
    left2, right2 = st.columns(2)

    with left2:
        fig3 = top_customers_bar(customer_ltv_df, top_n=10)
        if fig3 is not None:
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("Top customers chart unavailable.")

    with right2:
        fig4 = order_count_distribution_bar(customer_ltv_df)
        if fig4 is not None:
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("Order count distribution unavailable.")

    # ----------------------------
    # Detail tables
    # ----------------------------
    left_table, right_table = st.columns(2)

    with left_table:
        st.markdown("### Customer Lifetime Value")
        if customer_ltv_df.empty:
            st.warning("No customer lifetime value data found.")
        else:
            preferred_cols = [
                "customer_id",
                "total_spent",
                "total_refunded",
                "net_clv",
                "order_count",
            ]
            display_cols = [c for c in preferred_cols if c in customer_ltv_df.columns]
            st.dataframe(
                customer_ltv_df[display_cols].sort_values(
                    by="net_clv",
                    ascending=False,
                ).head(50),
                use_container_width=True,
            )

    with right_table:
        st.markdown("### Retention Signals")
        if retention_df.empty:
            st.warning("No retention signals data found.")
        else:
            preferred_cols = [
                "customer_id",
                "email",
                "signup_date",
                "first_name",
                "last_name",
            ]
            display_cols = [c for c in preferred_cols if c in retention_df.columns]
            st.dataframe(
                retention_df[display_cols].head(50),
                use_container_width=True,
            )

# ----------------------------
# Page: Product & Inventory
# ----------------------------
elif page == "Product & Inventory":
    st.subheader("Product & Inventory")

    revenue_col = first_existing_column(product_df, ["net_revenue"])
    velocity_col = first_existing_column(velocity_df, ["velocity_avg"])
    risk_col = first_existing_column(inventory_df, ["attention_score"])

    # ----------------------------
    # KPI cards
    # ----------------------------
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Products", safe_metric(product_df, "product_id", "count"))
    with c2:
        st.metric("Total Net Revenue", safe_metric_if_col(product_df, revenue_col, "sum"))
    with c3:
        st.metric("Avg Velocity", safe_metric_if_col(velocity_df, velocity_col, "mean"))
    with c4:
        st.metric("Inventory Risk Rows", safe_metric(inventory_df, "product_id", "count"))

    c5, c6, c7, c8 = st.columns(4)
    with c5:
        st.metric("Avg Margin %", safe_metric(product_df, "margin_pct", "mean", fmt="pct"))
    with c6:
        st.metric("Avg Return Rate", safe_metric(product_df, "return_rate", "mean", fmt="pct"))
    with c7:
        st.metric("High Risk Tier Products", count_equals_case_insensitive(inventory_df, "risk_tier", "high"))
    with c8:
        st.metric("Avg Attention Score", safe_metric_if_col(inventory_df, risk_col, "mean"))

    c9, c10, c11, c12 = st.columns(4)
    with c9:
        st.metric("Total Units Sold", safe_metric(product_df, "units_sold", "sum"))
    with c10:
        st.metric("Total Units Returned", safe_metric(product_df, "units_returned", "sum"))
    with c11:
        st.metric("Total Locked Capital", safe_metric(inventory_df, "locked_capital", "sum"))
    with c12:
        st.metric("Avg Cart→Order Rate", safe_metric(product_df, "cart_to_order_rate", "mean", fmt="pct"))

    # ----------------------------
    # Charts row 1
    # ----------------------------
    st.markdown("### Product Performance Snapshot")

    left, right = st.columns(2)

    with left:
        fig1 = top_products_bar(product_df, top_n=10)
        if fig1 is not None:
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("Top products chart unavailable.")

    with right:
        fig2 = category_revenue_bar(product_df)
        if fig2 is not None:
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Category revenue chart unavailable.")

    # ----------------------------
    # Charts row 2
    # ----------------------------
    left2, right2 = st.columns(2)

    with left2:
        fig3 = return_rate_by_category(product_df)
        if fig3 is not None:
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("Return-rate-by-category chart unavailable.")

    with right2:
        fig4 = sales_velocity_distribution(velocity_df)
        if fig4 is not None:
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("Sales velocity distribution unavailable.")

    # ----------------------------
    # Charts row 3
    # ----------------------------
    left3, right3 = st.columns(2)

    with left3:
        fig5 = inventory_risk_distribution(inventory_df)
        if fig5 is not None:
            st.plotly_chart(fig5, use_container_width=True)
        else:
            st.info("Inventory risk distribution unavailable.")

    with right3:
        fig6 = top_attention_products(inventory_df, top_n=10)
        if fig6 is not None:
            st.plotly_chart(fig6, use_container_width=True)
        else:
            st.info("Top attention products chart unavailable.")

    # ----------------------------
    # Charts row 4
    # ----------------------------
    st.markdown("### Revenue vs Inventory")
    fig7 = revenue_vs_inventory_scatter(product_df)
    if fig7 is not None:
        st.plotly_chart(fig7, use_container_width=True)
    else:
        st.info("Revenue vs inventory scatter unavailable.")

    # ----------------------------
    # Detail tables
    # ----------------------------
    left_table, right_table = st.columns(2)

    with left_table:
        st.markdown("### Product Performance")
        if product_df.empty:
            st.warning("No product performance data found.")
        else:
            preferred_cols = [
                "product_id",
                "product_name",
                "category",
                "units_sold",
                "units_returned",
                "gross_revenue",
                "net_revenue",
                "gross_profit",
                "margin_pct",
                "return_rate",
                "cart_to_order_rate",
                "inventory_quantity",
            ]
            display_cols = [c for c in preferred_cols if c in product_df.columns]
            st.dataframe(
                product_df[display_cols].sort_values(by="net_revenue", ascending=False).head(50),
                use_container_width=True,
            )

    with right_table:
        st.markdown("### Inventory Risk")
        if inventory_df.empty:
            st.warning("No inventory risk data found.")
        else:
            preferred_cols = [
                "product_id",
                "product_name",
                "category",
                "inventory_quantity",
                "sales_volume",
                "return_volume",
                "utilization_ratio",
                "return_signal",
                "locked_capital",
                "attention_score",
                "risk_tier",
            ]
            display_cols = [c for c in preferred_cols if c in inventory_df.columns]
            st.dataframe(
                inventory_df[display_cols].sort_values(by="attention_score", ascending=False).head(50),
                use_container_width=True,
            )


# ----------------------------
# Page: Operations & Shipping
# ----------------------------
elif page == "Operations & Shipping":
    st.subheader("Operations & Shipping")

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Shipping Rows", safe_metric(shipping_df, "order_id", "count"))
    with c2:
        st.metric("Avg Shipping Cost", safe_metric(shipping_df, "shipping_cost", "mean"))

    if shipping_df.empty:
        st.warning("No shipping economics data found.")
    else:
        st.dataframe(shipping_df.head(50), use_container_width=True)


# ----------------------------
# Page: Regional Finance
# ----------------------------
elif page == "Regional Finance":
    st.subheader("Regional Finance")

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Regional Rows", safe_metric(regional_df, "order_id", "count"))
    with c2:
        st.metric("Regional Net Revenue", safe_metric(regional_df, "net_total", "sum"))

    if regional_df.empty:
        st.warning("No regional financial data found.")
    else:
        st.dataframe(regional_df.head(50), use_container_width=True)


# ----------------------------
# Page: Debug
# ----------------------------
elif page == "Data Debug":
    st.subheader("Data Debug")
    st.markdown("### Enriched Table Availability")
    st.dataframe(preview_table_info(), use_container_width=True)

    st.markdown("### Current Partition")
    st.code(partition_value)