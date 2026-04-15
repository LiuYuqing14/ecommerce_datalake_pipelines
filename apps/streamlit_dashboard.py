from __future__ import annotations

import pandas as pd
import streamlit as st

from charts import (
    category_revenue_bar,
    clv_distribution_bar,
    customer_value_scatter,
    inventory_risk_distribution,
    operational_metrics_bar,
    order_count_distribution_bar,
    return_rate_by_category,
    revenue_comparison_bar,
    revenue_vs_inventory_scatter,
    sales_velocity_distribution,
    top_attention_products,
    top_customers_bar,
    top_products_bar,
)

from data_loader import (
    latest_partition,
    load_customer_lifetime_value,
    load_customer_retention_signals,
    load_customer_segments,
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


def inject_custom_styles():
    st.markdown("""
    <style>
    .block-container {
        max-width: 1380px;
        padding-top: 1.2rem;
        padding-bottom: 2rem;
    }

    [data-testid="stAppViewContainer"] {
        background: linear-gradient(180deg, #f8fafc 0%, #eef4f8 100%);
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
    }

    [data-testid="stSidebar"] * {
        color: #f8fafc !important;
    }

    .hero-card {
        background: linear-gradient(135deg, #0f172a 0%, #1d4ed8 60%, #38bdf8 100%);
        padding: 1.6rem 1.8rem;
        border-radius: 22px;
        color: white;
        box-shadow: 0 12px 30px rgba(15, 23, 42, 0.18);
        margin-bottom: 1.25rem;
    }

    .hero-title {
        font-size: 2rem;
        font-weight: 800;
        margin-bottom: 0.25rem;
        letter-spacing: -0.02em;
    }

    .hero-subtitle {
        font-size: 1rem;
        opacity: 0.95;
        line-height: 1.5;
    }

    .section-title {
        font-size: 1.15rem;
        font-weight: 700;
        color: #0f172a;
        margin-top: 0.4rem;
        margin-bottom: 0.3rem;
    }

    .section-subtitle {
        font-size: 0.92rem;
        color: #475569;
        margin-top: 0;
        margin-bottom: 0.9rem;
    }

    .panel-card {
        background: rgba(255,255,255,0.92);
        border: 1px solid rgba(148,163,184,0.18);
        border-radius: 20px;
        padding: 1rem 1rem 0.8rem 1rem;
        box-shadow: 0 6px 20px rgba(15, 23, 42, 0.06);
        margin-bottom: 1rem;
    }

    [data-testid="stMetric"] {
        background: rgba(255,255,255,0.94);
        border: 1px solid rgba(148,163,184,0.18);
        padding: 1rem 1rem;
        border-radius: 18px;
        box-shadow: 0 6px 18px rgba(15, 23, 42, 0.06);
    }

    [data-testid="stMetricLabel"] {
        color: #64748b;
        font-weight: 600;
    }

    [data-testid="stMetricValue"] {
        color: #0f172a;
        font-weight: 800;
    }

    [data-testid="stDataFrame"] {
        border-radius: 16px;
        overflow: hidden;
        border: 1px solid rgba(148,163,184,0.16);
        box-shadow: 0 4px 16px rgba(15, 23, 42, 0.05);
    }

    div[data-testid="stPlotlyChart"] {
        background: transparent;
        border-radius: 16px;
    }
    </style>
    """, unsafe_allow_html=True)


def render_hero(title: str, subtitle: str):
    st.markdown(
        f"""
        <div class="hero-card">
            <div class="hero-title">{title}</div>
            <div class="hero-subtitle">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_header(title: str, subtitle: str | None = None):
    st.markdown(f'<div class="section-title">{title}</div>', unsafe_allow_html=True)
    if subtitle:
        st.markdown(f'<div class="section-subtitle">{subtitle}</div>', unsafe_allow_html=True)


def open_panel():
    st.markdown('<div class="panel-card">', unsafe_allow_html=True)


def close_panel():
    st.markdown('</div>', unsafe_allow_html=True)


def safe_metric(df: pd.DataFrame, column: str, agg: str = "sum", fmt: str = "number"):
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


def safe_metric_if_col(df: pd.DataFrame, column: str | None,
                       agg: str = "sum", fmt: str = "number") -> str:
    if not column:
        return "N/A"
    return safe_metric(df, column, agg, fmt)


def count_equals_case_insensitive(df: pd.DataFrame,
                                  column: str, target: str) -> str:
    if df.empty or column not in df.columns:
        return "N/A"
    value = (df[column].astype(str).str.lower() == target.lower()).sum()
    return f"{value:,}"


def first_existing_column(df: pd.DataFrame,
                          candidates: list[str]) -> str | None:
    for col in candidates:
        if col in df.columns:
            return col
    return None


inject_custom_styles()

render_hero(
    "Retail Analytics App",
    "A retail analytics dashboard that turns enriched ecommerce data into clear, actionable business insights across revenue, customer retention, product performance, inventory risk, and operations."
)

# ----------------------------
# Sidebar
# ----------------------------
st.sidebar.markdown("## Dashboard Controls")
st.sidebar.caption("Adjust the active partition and switch between analytics views.")
st.sidebar.markdown("---")

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

st.sidebar.markdown("---")
st.sidebar.caption("Use Data Debug to verify partitions and enriched outputs.")

# ----------------------------
# Data loading
# ----------------------------
daily_df = load_daily_business_metrics(partition_value)
customer_ltv_df = load_customer_lifetime_value(partition_value)
retention_df = load_customer_retention_signals(partition_value)
customer_segments_df = load_customer_segments(partition_value)
product_df = load_product_performance(partition_value)
velocity_df = load_sales_velocity(partition_value)
shipping_df = load_shipping_economics(partition_value)
regional_df = load_regional_financials(partition_value)
inventory_df = load_inventory_risk(partition_value)

# ----------------------------
# Page: Overview
# ----------------------------
if page == "Overview":
    section_header(
        "Overview",
        "Track high-level business performance, revenue movement, order flow, and operational health."
    )

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

    section_header("Performance Snapshot", "Compare headline revenue and operational indicators.")
    left, right = st.columns(2)

    with left:
        open_panel()
        revenue_fig = revenue_comparison_bar(daily_df)
        if revenue_fig is not None:
            st.plotly_chart(revenue_fig, use_container_width=True)
        else:
            st.info("Revenue chart unavailable.")
        close_panel()

    with right:
        open_panel()
        ops_fig = operational_metrics_bar(daily_df)
        if ops_fig is not None:
            st.plotly_chart(ops_fig, use_container_width=True)
        else:
            st.info("Operational chart unavailable.")
        close_panel()

    section_header("Daily Business Metrics")
    open_panel()
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
    close_panel()

    section_header("Shipping Snapshot")
    open_panel()
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
    close_panel()

# ----------------------------
# Page: Customer Intelligence
# ----------------------------
elif page == "Customer Intelligence":
    section_header(
        "Customer Intelligence",
        "Understand customer value, retention risk, and segmentation signals for targeted action."
    )

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
        st.metric("Avg Retention Risk", safe_metric(retention_df, "retention_risk_score", "mean"))
    with c6:
        st.metric(
            "High Risk Customers",
            count_equals_case_insensitive(retention_df, "retention_risk_tier", "high")
        )
    with c7:
        st.metric(
            "Win-back Candidates",
            count_equals_case_insensitive(retention_df, "recommended_action", "win_back")
        )
    with c8:
        st.metric("Segmented Customers", safe_metric(customer_segments_df, "customer_id", "count"))

    section_header("Customer Value Snapshot", "Explore CLV distribution, spend behavior, and top-value customers.")
    left, right = st.columns(2)

    with left:
        open_panel()
        fig1 = clv_distribution_bar(customer_ltv_df, positive_only=True)
        if fig1 is not None:
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("CLV distribution chart unavailable.")
        close_panel()

    with right:
        open_panel()
        fig2 = customer_value_scatter(customer_ltv_df)
        if fig2 is not None:
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Customer value scatter unavailable.")
        close_panel()

    left2, right2 = st.columns(2)

    with left2:
        open_panel()
        fig3 = top_customers_bar(customer_ltv_df, top_n=10)
        if fig3 is not None:
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("Top customers chart unavailable.")
        close_panel()

    with right2:
        open_panel()
        fig4 = order_count_distribution_bar(customer_ltv_df)
        if fig4 is not None:
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("Order count distribution unavailable.")
        close_panel()

    left_table, right_table = st.columns(2)

    with left_table:
        section_header("Customer Lifetime Value")
        open_panel()
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
            sort_col = "net_clv" if "net_clv" in customer_ltv_df.columns else "customer_id"
            st.dataframe(
                customer_ltv_df[display_cols].sort_values(by=sort_col, ascending=False).head(50),
                use_container_width=True,
            )
        close_panel()

    with right_table:
        section_header("Retention Signals")
        open_panel()
        if retention_df.empty:
            st.warning("No retention signals data found.")
        else:
            preferred_cols = [
                "customer_id",
                "email",
                "first_name",
                "last_name",
                "net_clv",
                "order_count",
                "days_since_last_order",
                "refund_rate",
                "retention_risk_score",
                "retention_risk_tier",
                "recommended_action",
            ]
            display_cols = [c for c in preferred_cols if c in retention_df.columns]
            sort_col = "retention_risk_score" if "retention_risk_score" in retention_df.columns else "customer_id"
            st.dataframe(
                retention_df[display_cols].sort_values(by=sort_col, ascending=False).head(50),
                use_container_width=True,
            )
        close_panel()

    section_header("Customer Segments", "Review segment-level customer grouping for lifecycle strategy.")
    open_panel()
    if customer_segments_df.empty:
        st.info("No customer segmentation data found.")
    else:
        seg_col1, seg_col2 = st.columns([1, 2])

        with seg_col1:
            if "customer_segment" in customer_segments_df.columns:
                seg_summary = (
                    customer_segments_df["customer_segment"]
                    .astype(str)
                    .value_counts()
                    .rename_axis("segment")
                    .reset_index(name="customers")
                )
                st.dataframe(seg_summary, use_container_width=True)
            else:
                st.info("No segment summary available.")

        with seg_col2:
            preferred_cols = [
                "customer_id",
                "email",
                "first_name",
                "last_name",
                "net_clv",
                "order_count",
                "avg_order_value",
                "customer_segment",
                "segment_version",
            ]
            display_cols = [c for c in preferred_cols if c in customer_segments_df.columns]
            sort_col = "net_clv" if "net_clv" in customer_segments_df.columns else "customer_id"
            st.dataframe(
                customer_segments_df[display_cols].sort_values(by=sort_col, ascending=False).head(50),
                use_container_width=True,
            )
    close_panel()

# ----------------------------
# Page: Product & Inventory
# ----------------------------
elif page == "Product & Inventory":
    section_header(
        "Product & Inventory",
        "Monitor product performance, sales velocity, margin pressure, and inventory attention risk."
    )

    revenue_col = first_existing_column(product_df, ["net_revenue"])
    velocity_col = first_existing_column(velocity_df, ["velocity_avg"])
    risk_col = first_existing_column(inventory_df, ["attention_score"])

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
        st.metric(
            "High Risk Tier Products",
            count_equals_case_insensitive(inventory_df, "risk_tier", "high")
        )
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

    section_header("Product Performance Snapshot", "Track top products, category revenue, return patterns, and velocity.")
    left, right = st.columns(2)

    with left:
        open_panel()
        fig1 = top_products_bar(product_df, top_n=10)
        if fig1 is not None:
            st.plotly_chart(fig1, use_container_width=True)
        else:
            st.info("Top products chart unavailable.")
        close_panel()

    with right:
        open_panel()
        fig2 = category_revenue_bar(product_df)
        if fig2 is not None:
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Category revenue chart unavailable.")
        close_panel()

    left2, right2 = st.columns(2)

    with left2:
        open_panel()
        fig3 = return_rate_by_category(product_df)
        if fig3 is not None:
            st.plotly_chart(fig3, use_container_width=True)
        else:
            st.info("Return-rate-by-category chart unavailable.")
        close_panel()

    with right2:
        open_panel()
        fig4 = sales_velocity_distribution(velocity_df)
        if fig4 is not None:
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("Sales velocity distribution unavailable.")
        close_panel()

    left3, right3 = st.columns(2)

    with left3:
        section_header("Inventory Risk Distribution")
        open_panel()
        fig5 = inventory_risk_distribution(inventory_df)
        if fig5 is not None:
            st.plotly_chart(fig5, use_container_width=True)
        else:
            st.info("Inventory risk distribution unavailable.")
        close_panel()

    with right3:
        section_header("Top Attention Products")
        open_panel()
        fig6 = top_attention_products(inventory_df, top_n=10)
        if fig6 is not None:
            st.plotly_chart(fig6, use_container_width=True)
        else:
            st.info("Top attention products chart unavailable.")
        close_panel()

    section_header("Revenue vs Inventory")
    open_panel()
    fig7 = revenue_vs_inventory_scatter(product_df)
    if fig7 is not None:
        st.plotly_chart(fig7, use_container_width=True)
    else:
        st.info("Revenue vs inventory scatter unavailable.")
    close_panel()

    left_table, right_table = st.columns(2)

    with left_table:
        section_header("Product Performance")
        open_panel()
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
            sort_col = "net_revenue" if "net_revenue" in product_df.columns else "product_id"
            st.dataframe(
                product_df[display_cols].sort_values(by=sort_col, ascending=False).head(50),
                use_container_width=True,
            )
        close_panel()

    with right_table:
        section_header("Inventory Risk")
        open_panel()
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
            sort_col = "attention_score" if "attention_score" in inventory_df.columns else "product_id"
            st.dataframe(
                inventory_df[display_cols].sort_values(by=sort_col, ascending=False).head(50),
                use_container_width=True,
            )
        close_panel()

# ----------------------------
# Page: Operations & Shipping
# ----------------------------
elif page == "Operations & Shipping":
    section_header(
        "Operations & Shipping",
        "Review shipping activity, cost efficiency, and operational delivery details."
    )

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Shipping Rows", safe_metric(shipping_df, "order_id", "count"))
    with c2:
        st.metric("Avg Shipping Cost", safe_metric(shipping_df, "shipping_cost", "mean"))

    open_panel()
    if shipping_df.empty:
        st.warning("No shipping economics data found.")
    else:
        st.dataframe(shipping_df.head(50), use_container_width=True)
    close_panel()

# ----------------------------
# Page: Regional Finance
# ----------------------------
elif page == "Regional Finance":
    section_header(
        "Regional Finance",
        "Inspect regional-level financial performance and net revenue contribution."
    )

    c1, c2 = st.columns(2)
    with c1:
        st.metric("Regional Rows", safe_metric(regional_df, "order_id", "count"))
    with c2:
        st.metric("Regional Net Revenue", safe_metric(regional_df, "net_total", "sum"))

    open_panel()
    if regional_df.empty:
        st.warning("No regional financial data found.")
    else:
        st.dataframe(regional_df.head(50), use_container_width=True)
    close_panel()

# ----------------------------
# Page: Debug
# ----------------------------
elif page == "Data Debug":
    section_header(
        "Data Debug",
        "Validate enriched table availability, partitions, and active dashboard input state."
    )

    section_header("Enriched Table Availability")
    open_panel()
    st.dataframe(preview_table_info(), use_container_width=True)
    close_panel()

    section_header("Current Partition")
    open_panel()
    st.code(partition_value)
    close_panel()