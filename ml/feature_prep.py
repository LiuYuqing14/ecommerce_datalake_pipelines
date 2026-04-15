from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
ENRICHED_ROOT = REPO_ROOT / "data" / "silver" / "enriched"


def _extract_partition_value(partition_dir_name: str) -> str:
    if "=" not in partition_dir_name:
        return partition_dir_name
    return partition_dir_name.split("=", 1)[1]


def available_partitions(table_name: str) -> list[str]:
    table_dir = ENRICHED_ROOT / table_name
    if not table_dir.exists() or not table_dir.is_dir():
        return []

    return sorted(
        _extract_partition_value(p.name)
        for p in table_dir.iterdir()
        if p.is_dir()
    )


def latest_partition(table_name: str) -> Optional[str]:
    parts = available_partitions(table_name)
    return parts[-1] if parts else None


def resolve_partition_path(table_name: str, partition_value: Optional[str] = None) -> Path:
    table_dir = ENRICHED_ROOT / table_name
    if partition_value is None:
        return table_dir

    for child in table_dir.iterdir() if table_dir.exists() else []:
        if child.is_dir() and _extract_partition_value(child.name) == partition_value:
            return child

    return table_dir / f"date={partition_value}"


def load_parquet_table(table_name: str, partition_value: Optional[str] = None) -> pd.DataFrame:
    path = resolve_partition_path(table_name, partition_value)
    if not path.exists():
        return pd.DataFrame()

    try:
        return pd.read_parquet(path)
    except Exception as exc:
        raise RuntimeError(f"Failed reading {table_name} from {path}: {exc}") from exc


def write_partitioned_parquet(
    df: pd.DataFrame,
    table_name: str,
    partition_value: str,
    partition_key: str = "date",
) -> Path:
    out_dir = ENRICHED_ROOT / table_name / f"{partition_key}={partition_value}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / "part-00000.parquet"
    df.to_parquet(out_file, index=False)
    return out_file


def to_numeric_safe(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    out = df.copy()
    for col in cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def min_max_scale(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(0.0)
    s_min = s.min()
    s_max = s.max()

    if pd.isna(s_min) or pd.isna(s_max) or s_max == s_min:
        return pd.Series([0.0] * len(s), index=s.index)

    return (s - s_min) / (s_max - s_min)


def zscore_clip(series: pd.Series, clip: float = 3.0) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(0.0)
    std = s.std(ddof=0)
    if std == 0 or pd.isna(std):
        return pd.Series([0.0] * len(s), index=s.index)

    z = (s - s.mean()) / std
    return z.clip(-clip, clip)