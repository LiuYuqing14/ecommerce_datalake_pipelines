from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

# Repo root = one level above apps/
REPO_ROOT = Path(__file__).resolve().parents[1]
ENRICHED_ROOT = REPO_ROOT / "data" / "silver" / "enriched"


def _safe_read_parquet(path: Path) -> pd.DataFrame:
    """
    Read a parquet file or directory into a pandas DataFrame.
    Returns an empty DataFrame if the path does not exist.
    """
    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_parquet(path)
    except Exception as exc:
        raise RuntimeError(f"Failed to read parquet from {path}: {exc}") from exc


def _list_partition_dirs(table_dir: Path) -> list[Path]:
    """
    Return partition subdirectories like:
    order_dt=2020-01-05, ingest_dt=2020-01-05, date=2020-01-05, etc.
    """
    if not table_dir.exists() or not table_dir.is_dir():
        return []

    return sorted([p for p in table_dir.iterdir() if p.is_dir()])


def _extract_partition_value(partition_dir_name: str) -> str:
    """
    Convert 'order_dt=2020-01-05' -> '2020-01-05'
    """
    if "=" not in partition_dir_name:
        return partition_dir_name
    return partition_dir_name.split("=", 1)[1]


def available_partitions(table_name: str) -> list[str]:
    """
    List available partition values for a table under data/silver/enriched/<table_name>.
    """
    table_dir = ENRICHED_ROOT / table_name
    dirs = _list_partition_dirs(table_dir)
    return [_extract_partition_value(d.name) for d in dirs]


def resolve_partition_path(table_name: str,
                           partition_value: Optional[str] = None) -> Path:
    """
    Resolve a table path.
    If partition_value is provided, find the matching partition directory.
    Otherwise return the table root directory.
    """
    table_dir = ENRICHED_ROOT / table_name
    if partition_value is None:
        return table_dir

    for child in _list_partition_dirs(table_dir):
        if _extract_partition_value(child.name) == partition_value:
            return child

    # Fall back to root if specific partition is not found
    return table_dir


@st.cache_data(show_spinner=False)
def load_table(table_name: str,
               partition_value: Optional[str] = None) -> pd.DataFrame:
    """
    Generic loader for enriched tables.
    """
    path = resolve_partition_path(table_name, partition_value)
    return _safe_read_parquet(path)


@st.cache_data(show_spinner=False)
def load_daily_business_metrics(partition_value: Optional[str] = None) -> pd.DataFrame:
    return load_table("int_daily_business_metrics", partition_value)


@st.cache_data(show_spinner=False)
def load_customer_lifetime_value(partition_value: Optional[str] = None):
    return load_table("int_customer_lifetime_value", partition_value)


@st.cache_data(show_spinner=False)
def load_customer_retention_signals(partition_value: Optional[str] = None):
    return load_table("int_customer_retention_signals", partition_value)

@st.cache_data(show_spinner=False)
def load_customer_segments(partition_value: Optional[str] = None):
    return load_table("int_customer_segments", partition_value)

@st.cache_data(show_spinner=False)
def load_product_performance(partition_value: Optional[str] = None):
    return load_table("int_product_performance", partition_value)


@st.cache_data(show_spinner=False)
def load_sales_velocity(partition_value: Optional[str] = None):
    return load_table("int_sales_velocity", partition_value)


@st.cache_data(show_spinner=False)
def load_shipping_economics(partition_value: Optional[str] = None):
    return load_table("int_shipping_economics", partition_value)


@st.cache_data(show_spinner=False)
def load_regional_financials(partition_value: Optional[str] = None):
    return load_table("int_regional_financials", partition_value)


@st.cache_data(show_spinner=False)
def load_inventory_risk(partition_value: Optional[str] = None):
    return load_table("int_inventory_risk", partition_value)


def latest_partition(table_name: str) -> Optional[str]:
    """
    Get the latest available partition value as a string.
    Works well for YYYY-MM-DD partition values.
    """
    parts = available_partitions(table_name)
    if not parts:
        return None
    return sorted(parts)[-1]


def preview_table_info() -> pd.DataFrame:
    """
    Quick helper for debugging: show which enriched tables
    exist and their latest partition.
    """
    target_tables = [
        "int_daily_business_metrics",
        "int_customer_lifetime_value",
        "int_customer_retention_signals",
        "int_product_performance",
        "int_sales_velocity",
        "int_shipping_economics",
        "int_regional_financials",
        "int_inventory_risk",
        "int_attributed_purchases",
        "int_cart_attribution",
        "int_customer_segments"
    ]

    rows = []
    for table in target_tables:
        parts = available_partitions(table)
        rows.append(
            {
                "table_name": table,
                "exists": (ENRICHED_ROOT / table).exists(),
                "num_partitions": len(parts),
                "latest_partition": sorted(parts)[-1] if parts else None,
            }
        )

    return pd.DataFrame(rows)