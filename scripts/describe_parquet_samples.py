#!/usr/bin/env python3
"""Profile Parquet samples from local bronze extracts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, cast

import polars as pl


@dataclass(frozen=True)
class PartitionProfile:
    table: str
    partition: str
    partition_key: str
    files: list[Path]
    row_count: int
    schema: dict[str, str]
    column_stats: list[dict[str, object]]


META_START = "<!-- GENERATED META -->"
META_END = "<!-- END GENERATED META -->"


def utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
    )


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def strip_meta_block(text: str) -> str:
    if META_START not in text or META_END not in text:
        return text
    before, rest = text.split(META_START, 1)
    _, after = rest.split(META_END, 1)
    return before + after


def apply_markdown_meta(text: str, timestamp: str) -> tuple[str, str]:
    body = strip_meta_block(text).rstrip() + "\n"
    body_hash = content_hash(body)
    meta_block = "\n".join(
        [
            META_START,
            f"Last updated (UTC): {timestamp}",
            f"Content hash (SHA-256): {body_hash}",
            META_END,
        ]
    )
    return f"{body}{meta_block}\n", body_hash


def parse_date(value: str) -> date | None:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def matches_partition(
    partition_key: str,
    partition: str,
    ingest_dts: set[str],
    months: set[str],
    start_date: date | None,
    end_date: date | None,
) -> bool:
    # Non-date partitions (e.g., product categories) should always be included,
    # even when date filters are provided.
    if parse_date(partition) is None and partition_key not in {
        "ingest_dt",
        "signup_date",
    }:
        return True
    if ingest_dts and partition in ingest_dts:
        return True
    if months and partition[:7] in months:
        return True
    if start_date and end_date:
        parsed = parse_date(partition)
        return parsed is not None and start_date <= parsed <= end_date
    return not (ingest_dts or months or (start_date and end_date))


def find_parquet_files(
    root: Path,
    max_files: int,
    tables_filter: set[str],
    ingest_dts: set[str],
    months: set[str],
    start_date: date | None,
    end_date: date | None,
) -> dict[str, list[tuple[str, str, list[Path]]]]:
    tables: dict[str, list[tuple[str, str, list[Path]]]] = {}
    partition_globs = {
        "customers": ("signup_date", "signup_date=*"),
        "product_catalog": ("category", "category=*"),
    }
    for table_dir in root.iterdir():
        if not table_dir.is_dir():
            continue
        if tables_filter and table_dir.name not in tables_filter:
            continue
        partition_key, partition_glob = partition_globs.get(
            table_dir.name, ("ingest_dt", "ingest_dt=*")
        )
        partitions: list[tuple[str, str, list[Path]]] = []
        for partition_dir in sorted(table_dir.glob(partition_glob)):
            if not partition_dir.is_dir():
                continue
            partition_value = partition_dir.name.split("=", 1)[-1]
            if not matches_partition(
                partition_key,
                partition_value,
                ingest_dts,
                months,
                start_date,
                end_date,
            ):
                continue
            parquet_files = sorted(partition_dir.glob("*.parquet"))
            if not parquet_files:
                continue
            partitions.append(
                (partition_key, partition_value, parquet_files[:max_files])
            )
        if partitions:
            tables[table_dir.name] = partitions
    return tables


def col_stats(series: pl.Series, row_count: int) -> dict[str, object]:
    null_count = int(series.null_count())
    null_pct = round(null_count / row_count * 100, 2) if row_count else 0.0
    distinct = int(series.n_unique())
    min_val = None
    max_val = None
    percentiles = None
    top_values = None

    # Numeric/temporal columns: get min/max and percentiles
    if series.dtype.is_numeric() or series.dtype.is_temporal():
        min_val = series.min()
        max_val = series.max()
        # Get percentiles for numeric columns
        if series.dtype.is_numeric() and row_count > 0:
            try:
                p25 = series.quantile(0.25, interpolation="nearest")
                p50 = series.quantile(0.50, interpolation="nearest")
                p75 = series.quantile(0.75, interpolation="nearest")
                p95 = series.quantile(0.95, interpolation="nearest")
                percentiles = {
                    "p25": p25,
                    "p50": p50,
                    "p75": p75,
                    "p95": p95,
                }
            except Exception:
                percentiles = None

    # String columns: get top 5 values
    elif series.dtype == pl.Utf8 and row_count > 0:
        try:
            value_counts = series.value_counts(sort=True).head(5)
            top_values = [(row[0], int(row[1])) for row in value_counts.iter_rows()]
        except Exception:
            top_values = None

    return {
        "column": series.name,
        "dtype": str(series.dtype),
        "null_pct": null_pct,
        "distinct": distinct,
        "min": min_val,
        "max": max_val,
        "percentiles": percentiles,
        "top_values": top_values,
    }


def profile_partition(
    files: list[Path], max_rows: int
) -> tuple[int, dict[str, str], list[dict[str, object]]]:
    frames: list[pl.DataFrame] = []
    for file_path in files:
        if max_rows > 0:
            frames.append(pl.read_parquet(file_path, n_rows=max_rows))
        else:
            frames.append(pl.read_parquet(file_path))
    if not frames:
        return 0, {}, []
    df = pl.concat(frames, how="diagonal")
    schema = {name: str(dtype) for name, dtype in df.schema.items()}
    row_count = df.height
    stats = [col_stats(df[column], row_count) for column in df.columns]
    return row_count, schema, stats


def normalize_schema_for_drift(schema: dict[str, str]) -> str:
    """Normalize schema key by treating Null types as wildcards.

    This prevents false positives when a partition has all nulls for a column
    (causing Polars to infer Null type instead of the actual type).
    """
    # Build normalized schema where Null is replaced with a wildcard marker
    normalized_pairs = []
    for name, dtype in schema.items():
        # Treat Null as a special marker that matches any type
        normalized_type = "*" if dtype == "Null" else dtype
        normalized_pairs.append(f"{name}:{normalized_type}")
    return "|".join(normalized_pairs)


def schemas_are_compatible(schema_key1: str, schema_key2: str) -> bool:
    """Check if two schema keys are compatible (Null vs non-Null types)."""
    pairs1 = schema_key1.split("|")
    pairs2 = schema_key2.split("|")

    if len(pairs1) != len(pairs2):
        return False

    for pair1, pair2 in zip(pairs1, pairs2, strict=False):
        col1, type1 = pair1.split(":", 1)
        col2, type2 = pair2.split(":", 1)

        # Column names must match
        if col1 != col2:
            return False

        # Types must match OR one must be Null
        if type1 != type2 and type1 != "Null" and type2 != "Null":
            return False

    return True


def render_markdown(
    profiles: Iterable[PartitionProfile],
    scope: dict[str, str],
) -> str:
    lines = [
        "# Bronze Sample Profile Report",
        "",
        "Generated from local parquet samples in `samples/bronze/`.",
        "",
    ]
    if scope:
        lines.append("## Sample Scope")
        lines.append("")
        for key, value in scope.items():
            lines.append(f"- **{key}**: {value}")
        lines.append("")
    profiles_list = list(profiles)
    schema_keys: dict[tuple[str, str], str] = {}
    for profile in profiles_list:
        schema_key = "|".join(
            f"{name}:{dtype}" for name, dtype in profile.schema.items()
        )
        schema_keys[(profile.table, profile.partition)] = schema_key

    # Build top-level summary
    total_rows = sum(p.row_count for p in profiles_list)
    unique_tables = len(set(p.table for p in profiles_list))
    unique_partitions = len(set(p.partition for p in profiles_list))

    # Calculate per-table totals
    table_row_counts: dict[str, int] = {}
    table_partition_counts: dict[str, int] = {}
    for profile in profiles_list:
        table_row_counts[profile.table] = (
            table_row_counts.get(profile.table, 0) + profile.row_count
        )
        table_partition_counts[profile.table] = (
            table_partition_counts.get(profile.table, 0) + 1
        )

    lines.append("## Overview")
    lines.append("")
    lines.append(f"- **Tables sampled**: {unique_tables}")
    lines.append(f"- **Partitions sampled**: {unique_partitions}")
    lines.append(f"- **Total sample rows**: {total_rows:,}")
    lines.append("")
    lines.append("### Per-Table Summary")
    lines.append("")
    lines.append("| Table | Partitions | Sample Rows |")
    lines.append("| --- | --- | --- |")
    for table in sorted(table_row_counts.keys()):
        lines.append(
            f"| {table} | {table_partition_counts[table]} | "
            f"{table_row_counts[table]:,} |"
        )
    lines.append("")

    # Data Quality Flags
    lines.append("### Data Quality Flags")
    lines.append("")
    quality_flags: list[tuple[str, str, str, str]] = []

    # Check for high null percentages
    for profile in profiles_list:
        for stat in profile.column_stats:
            null_pct = cast(float, stat["null_pct"])
            if null_pct > 50:
                quality_flags.append(
                    (
                        profile.table,
                        profile.partition,
                        f"high_nulls|{profile.table}|{stat['column']}",
                        (
                            f"⚠️ **{profile.table}.{stat['column']}**: "
                            f"{null_pct}% nulls (>50%)"
                        ),
                    )
                )

    # Check for low cardinality on PRIMARY entity ID columns only
    # Exclude: metadata columns, lookup IDs (agent, region, tier, status, etc.)
    metadata_columns = {"batch_id", "ingestion_ts", "event_id", "source_file"}
    lookup_id_patterns = {
        "agent_id",
        "region_id",
        "tier_id",
        "status_id",
        "warehouse_id",
        "store_id",
    }

    for profile in profiles_list:
        for stat in profile.column_stats:
            col_name = str(stat["column"]).lower()
            # Skip metadata columns
            if stat["column"] in metadata_columns:
                continue
            # Skip known lookup IDs (low cardinality is expected)
            if stat["column"] in lookup_id_patterns:
                continue

            # Flag PRIMARY entity ID columns with suspiciously low cardinality
            # These are typically table-named IDs like customer_id, order_id, product_id
            is_primary_id = col_name.endswith("_id") and any(
                col_name.startswith(entity)
                for entity in [
                    "customer",
                    "order",
                    "product",
                    "cart",
                    "return",
                    "item",
                ]
            )

            distinct_val = cast(int, stat["distinct"])
            null_pct_val = cast(float, stat["null_pct"])
            if is_primary_id and distinct_val < 10 and null_pct_val < 100:
                quality_flags.append(
                    (
                        profile.table,
                        profile.partition,
                        f"low_cardinality_id|{profile.table}|{stat['column']}",
                        (
                            f"⚠️ **{profile.table}.{stat['column']}**: Only "
                            f"{distinct_val} distinct values "
                            "(expected high cardinality for primary entity ID)"
                        ),
                    )
                )

    # Check for suspicious cardinality mismatches
    for profile in profiles_list:
        stat_dict = {s["column"]: s for s in profile.column_stats}
        # Check product_id vs product_name mismatches
        if "product_id" in stat_dict and "product_name" in stat_dict:
            prod_id_distinct = cast(int, stat_dict["product_id"]["distinct"])
            prod_name_distinct = cast(int, stat_dict["product_name"]["distinct"])
            if prod_name_distinct > prod_id_distinct * 1.5:
                quality_flags.append(
                    (
                        profile.table,
                        profile.partition,
                        f"cardinality_mismatch|{profile.table}|product_id_vs_product_name",
                        (
                            f"⚠️ **{profile.table}**: More product names than "
                            "product IDs (possible duplicates/variations)"
                        ),
                    )
                )

    # Check for volume anomalies (partition row count spikes)
    table_partition_rows: dict[str, list[tuple[str, int]]] = {}
    for profile in profiles_list:
        table_partition_rows.setdefault(profile.table, []).append(
            (profile.partition, profile.row_count)
        )

    for table, partition_rows in table_partition_rows.items():
        if len(partition_rows) > 3:
            row_counts = [r for _, r in partition_rows]
            avg_rows = sum(row_counts) / len(row_counts)
            for partition, rows in partition_rows:
                if rows > avg_rows * 1.5:
                    quality_flags.append(
                        (
                            table,
                            partition,
                            f"volume_spike|{table}|{partition}",
                            (
                                f"📊 **{table}** partition `{partition}`: "
                                f"{rows:,} rows "
                                f"(+{int((rows/avg_rows - 1) * 100)}% above "
                                "average)"
                            ),
                        )
                    )

    if quality_flags:
        # Aggregate by issue key (not exact message text)
        aggregated: dict[str, dict[str, object]] = {}
        for table, partition, issue_key, message in quality_flags:
            if issue_key not in aggregated:
                aggregated[issue_key] = {
                    "message": message,
                    "count": 0,
                    "partitions": set(),
                }
            entry = aggregated[issue_key]
            entry["count"] = cast(int, entry["count"]) + 1
            partitions = entry["partitions"]
            if isinstance(partitions, set):
                partitions.add(f"{table}:{partition}")

        sorted_flags = sorted(
            aggregated.items(),
            key=lambda item: cast(int, item[1]["count"]),
            reverse=True,
        )
        for _issue_key, meta in sorted_flags[:10]:
            # meta is dict[str, object]
            partitions_set = cast(Set[str], meta["partitions"])
            partitions_sample = sorted(list(partitions_set))[:5]
            partition_sample_str = ", ".join(partitions_sample)
            msg = cast(str, meta["message"])
            cnt = cast(int, meta["count"])
            lines.append((f"- {msg} (count={cnt}, " f"samples={partition_sample_str})"))
    else:
        lines.append("- ✅ No major data quality issues detected")

    lines.append("")

    lines.append("## Schema Drift")
    lines.append("")
    drift_found = False
    table_schemas: dict[str, dict[str, set[str]]] = {}
    table_schema_counts: dict[str, dict[str, int]] = {}

    # First pass: group schemas by exact match
    for (table, partition), schema_key in schema_keys.items():
        table_schemas.setdefault(table, {}).setdefault(schema_key, set()).add(partition)
        table_schema_counts.setdefault(table, {}).setdefault(schema_key, 0)
        table_schema_counts[table][schema_key] += 1

    # Second pass: merge compatible schemas (differ only by Null vs actual types)
    for table in list(table_schemas.keys()):
        schema_map = table_schemas[table]
        if len(schema_map) <= 1:
            continue

        # Find groups of compatible schemas
        schema_list = list(schema_map.keys())
        merged_schemas: dict[str, set[str]] = {}
        merged_counts: dict[str, int] = {}
        processed = set()

        for i, schema_key1 in enumerate(schema_list):
            if schema_key1 in processed:
                continue

            # Start a new group with this schema as the canonical version
            # (prefer the one without Null types)
            canonical = schema_key1
            group_partitions = set(schema_map[schema_key1])
            group_count = table_schema_counts[table][schema_key1]

            # Find all compatible schemas
            for schema_key2 in schema_list[i + 1 :]:
                if schema_key2 in processed:
                    continue

                if schemas_are_compatible(schema_key1, schema_key2):
                    # Merge into this group
                    group_partitions.update(schema_map[schema_key2])
                    group_count += table_schema_counts[table][schema_key2]
                    processed.add(schema_key2)

                    # Update canonical to prefer non-Null version
                    if "Null" not in schema_key2 and "Null" in canonical:
                        canonical = schema_key2

            merged_schemas[canonical] = group_partitions
            merged_counts[canonical] = group_count
            processed.add(schema_key1)

        # Replace with merged version
        table_schemas[table] = merged_schemas
        table_schema_counts[table] = merged_counts

    # Report true drift (incompatible schemas only)
    for table, schema_map in sorted(table_schemas.items()):
        if len(schema_map) <= 1:
            continue

        # Only report if schemas are truly incompatible
        drift_found = True
        lines.append(f"### {table}")
        lines.append("")
        lines.append(
            "⚠️ **True schema drift detected** (incompatible schemas, not just "
            "null inference):"
        )
        lines.append("")
        for schema_key, partitions in sorted(
            schema_map.items(), key=lambda x: len(x[1]), reverse=True
        ):
            partition_list = sorted(partitions)
            if len(partition_list) <= 5:
                partition_display = ", ".join(partition_list)
            else:
                partition_display = (
                    f"{partition_list[0]} ... {partition_list[-1]} "
                    f"({len(partition_list)} partitions)"
                )
            lines.append(f"- Schema key: `{schema_key}`")
            lines.append(f"  - Partitions ({len(partition_list)}): {partition_display}")
        lines.append("")

    if not drift_found:
        lines.append("- ✅ No schema drift detected across sampled partitions.")
        lines.append("")
        lines.append(
            "_Note: Partitions with all-null values for a column may show `Null` "
            "type instead of the actual type. These are treated as compatible "
            "schemas, not drift._"
        )
        lines.append("")

    lines.append("## Partition Coverage")
    lines.append("")
    lines.append(
        "Shows which partitions were sampled per table "
        "(for temporal schema drift detection)."
    )
    lines.append("")

    # Group partitions by table
    table_partitions: dict[str, list[tuple[str, str]]] = {}
    for profile in profiles_list:
        table_partitions.setdefault(profile.table, []).append(
            (profile.partition_key, profile.partition)
        )

    for table, partitions in sorted(table_partitions.items()):
        sorted_partitions = sorted(set(partitions), key=lambda item: item[1])
        grouped: dict[str, list[str]] = {}
        non_date_partitions: list[str] = []
        for partition_key, partition_value in sorted_partitions:
            if parse_date(partition_value):
                year_month = partition_value[:7]  # YYYY-MM
                grouped.setdefault(year_month, []).append(partition_value)
            else:
                non_date_partitions.append(f"{partition_key}={partition_value}")

        lines.append(f"**{table}** ({len(sorted_partitions)} partitions):")
        for year_month in sorted(grouped.keys()):
            dates = grouped[year_month]
            if len(dates) <= 5:
                lines.append(f"- `{year_month}`: {', '.join(dates)}")
            else:
                lines.append(
                    f"- `{year_month}`: {dates[0]} ... {dates[-1]} "
                    f"({len(dates)} days)"
                )
        if non_date_partitions:
            lines.append(f"- `non-date`: {', '.join(non_date_partitions[:10])}")
        lines.append("")

    lines.append("## Canonical Schema Keys")
    lines.append("")
    lines.append("| Table | Canonical Schema Key | Sample Partitions |")
    lines.append("| --- | --- | --- |")
    for table, counts in sorted(table_schema_counts.items()):
        canonical_key = max(counts.items(), key=lambda item: item[1])[0]
        partitions = sorted(table_schemas.get(table, {}).get(canonical_key, set()))
        sample_partitions = ", ".join(partitions[:5])
        lines.append(f"| {table} | `{canonical_key}` | {sample_partitions} |")
    lines.append("")

    lines.append("## Column Statistics (Sample Partition)")
    lines.append("")
    lines.append("Showing detailed stats for one representative partition per table.")
    lines.append("")

    # Group profiles by table and pick first partition per table
    table_profiles: dict[str, PartitionProfile] = {}
    for profile in profiles_list:
        if profile.table not in table_profiles:
            table_profiles[profile.table] = profile

    for table, profile in sorted(table_profiles.items()):
        lines.append(f"### {table}")
        lines.append("")
        lines.append(
            f"**Sample**: `{profile.partition_key}={profile.partition}` "
            f"({profile.row_count:,} rows)"
        )
        lines.append("")
        lines.append("| Column | Type | Null % | Distinct | Stats |")
        lines.append("| --- | --- | --- | --- | --- |")

        for stat in profile.column_stats:
            # Build stats cell content
            stats_parts = []

            # Min/Max for numeric/temporal
            if stat["min"] is not None and stat["max"] is not None:
                stats_parts.append(f"Range: `{stat['min']}` to `{stat['max']}`")

            # Percentiles for numeric
            percentiles = cast(Optional[Dict[str, Any]], stat["percentiles"])
            if percentiles:
                p = percentiles
                stats_parts.append(
                    f"p25={p['p25']}, p50={p['p50']}, p75={p['p75']}, p95={p['p95']}"
                )

            # Top values for strings
            top_values = cast(Optional[List[Tuple[Any, int]]], stat["top_values"])
            if top_values:
                top_str = ", ".join([f"`{val}` ({cnt})" for val, cnt in top_values[:3]])
                stats_parts.append(f"Top: {top_str}")

            stats_cell = "<br>".join(stats_parts) if stats_parts else "—"

            lines.append(
                "| {column} | {dtype} | {null_pct}% | {distinct:,} | {stats} |".format(
                    column=stat["column"],
                    dtype=stat["dtype"],
                    null_pct=stat["null_pct"],
                    distinct=stat["distinct"],
                    stats=stats_cell,
                )
            )
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile parquet samples.")
    parser.add_argument(
        "--root", default="samples/bronze", help="Sample root directory"
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=1,
        help="Max parquet files per partition",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=100000,
        help="Max rows per file sample (0=all)",
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated table names to include (optional)",
    )
    parser.add_argument(
        "--ingest-dts",
        default="",
        help="Comma-separated ingest_dt values (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--months",
        default="",
        help="Comma-separated months (YYYY-MM)",
    )
    parser.add_argument(
        "--date-range",
        default="",
        help="Date range in YYYY-MM-DD..YYYY-MM-DD format",
    )
    parser.add_argument(
        "--output",
        default="docs/data/BRONZE_PROFILE_REPORT.md",
        help="Output path for Markdown report",
    )
    parser.add_argument(
        "--schema-json",
        default="",
        help="Optional path to write schema map JSON",
    )
    parser.add_argument(
        "--update-contract",
        default="",
        help="Optional path to DATA_CONTRACT.md to refresh bronze->base mapping",
    )
    parser.add_argument(
        "--data-dictionary",
        default="",
        help="Optional path to write a DATA_DICTIONARY.md from profiled schema",
    )
    args = parser.parse_args()

    root = Path(args.root)
    if not root.exists():
        raise SystemExit(f"Root not found: {root}")

    tables_filter = {name.strip() for name in args.tables.split(",") if name.strip()}
    ingest_dts = {dt.strip() for dt in args.ingest_dts.split(",") if dt.strip()}
    months = {month.strip() for month in args.months.split(",") if month.strip()}
    start_date = end_date = None
    if args.date_range:
        start_str, end_str = args.date_range.split("..")
        start_date = parse_date(start_str)
        end_date = parse_date(end_str)

    tables = find_parquet_files(
        root,
        args.max_files,
        tables_filter,
        ingest_dts,
        months,
        start_date,
        end_date,
    )

    profiles: list[PartitionProfile] = []
    for table_name, partitions in sorted(tables.items()):
        for partition_key, partition_value, files in sorted(
            partitions, key=lambda item: item[1]
        ):
            row_count, schema, stats = profile_partition(files, args.max_rows)
            profiles.append(
                PartitionProfile(
                    table=table_name,
                    partition=partition_value,
                    partition_key=partition_key,
                    files=files,
                    row_count=row_count,
                    schema=schema,
                    column_stats=stats,
                )
            )

    scope: dict[str, str] = {}
    if tables_filter:
        scope["Tables"] = ", ".join(sorted(tables_filter))
    if ingest_dts:
        scope["Ingest dates"] = ", ".join(sorted(ingest_dts))
    if months:
        scope["Months"] = ", ".join(sorted(months))
    if start_date and end_date:
        scope["Date range"] = f"{start_date}..{end_date}"
    elif args.date_range:
        scope["Date range"] = args.date_range

    timestamp = utc_timestamp()

    markdown_body = render_markdown(profiles, scope)
    markdown, report_hash = apply_markdown_meta(markdown_body, timestamp)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown)
    print(f"Wrote {args.output}")

    if args.schema_json:
        schema_map: dict[str, dict[str, dict[str, str]]] = {}
        for profile in profiles:
            partition_id = f"{profile.partition_key}={profile.partition}"
            schema_map.setdefault(profile.table, {})[partition_id] = profile.schema
        schema_body = json.dumps(schema_map, indent=2, sort_keys=True)
        schema_hash = content_hash(schema_body)
        schema_payload = {
            "_meta": {
                "last_updated_utc": timestamp,
                "content_hash_sha256": schema_hash,
            },
            "schemas": schema_map,
        }
        schema_path = Path(args.schema_json)
        schema_path.parent.mkdir(parents=True, exist_ok=True)
        schema_path.write_text(json.dumps(schema_payload, indent=2, sort_keys=True))
        print(f"Wrote {args.schema_json}")

    schema_by_table: dict[str, dict[str, str]] = {}
    for profile in profiles:
        schema_by_table[profile.table] = profile.schema

    bool_fields = {
        "is_guest",
        "email_verified",
        "marketing_opt_in",
        "is_expedited",
        "is_reactivated",
    }

    timestamp_fields = {
        "order_date",
        "return_date",
        "created_at",
        "updated_at",
        "added_at",
        "ingestion_ts",
    }

    date_fields = {
        "signup_date",
        "loyalty_enrollment_date",
    }

    bronze_to_base = {
        "String": "string",
        "Int64": "int64",
        "Float64": "float64",
        "Boolean": "bool",
    }

    if args.update_contract:
        contract_path = Path(args.update_contract)
        contract = contract_path.read_text()

        contract_lines: list[str] = []
        contract_lines.append("## Observed Bronze Types vs Base Silver Targets")
        contract_lines.append(
            "Bronze source fields are string-heavy (timestamps and dates are "
            "strings). Base Silver must"
        )
        contract_lines.append(
            "explicitly cast to target types below. Use the profile report for "
            "observed types and"
        )
        contract_lines.append("validate that casts are applied in dbt-duckdb models.")
        contract_lines.append("")

        for table in sorted(schema_by_table.keys()):
            contract_lines.append(f"### {table}")
            for col, bronze_type in schema_by_table[table].items():
                target = bronze_to_base.get(bronze_type, bronze_type.lower())
                if col in timestamp_fields:
                    target = "timestamp"
                elif col in date_fields:
                    target = "date"
                elif col in bool_fields:
                    target = "bool"
                contract_lines.append(
                    f"- {col}: Bronze `{bronze_type.lower()}` -> Base Silver "
                    f"`{target}`"
                )
            contract_lines.append("")

        new_block = "\n".join(contract_lines).rstrip()
        start_marker = "## Observed Bronze Types vs Base Silver Targets"
        end_marker = "## Base Silver Tables (Required Fields)"
        if start_marker in contract and end_marker in contract:
            before, rest = contract.split(start_marker, 1)
            _, after = rest.split(end_marker, 1)
            contract = before + new_block + "\n\n" + end_marker + after
            contract_with_meta, _ = apply_markdown_meta(contract, timestamp)
            contract_path.write_text(contract_with_meta)
            print(f"Updated {args.update_contract} with bronze->base mapping.")
        else:
            raise SystemExit("Markers not found in DATA_CONTRACT.md")

    if args.data_dictionary:
        dict_path = Path(args.data_dictionary)
        common_desc = {
            "order_id": "Unique order identifier.",
            "order_date": "Timestamp when the order was placed.",
            "order_channel": "Channel used to place the order (e.g., Web, Phone).",
            "customer_id": "Unique customer identifier.",
            "email": "Customer email address.",
            "product_id": "Unique product identifier.",
            "product_name": "Product display name.",
            "category": "Product category.",
            "quantity": "Quantity of items in the line.",
            "quantity_returned": "Quantity of items returned.",
            "unit_price": "Unit sale price.",
            "discount_amount": "Discount amount applied.",
            "cost_price": "Unit cost for the item.",
            "gross_total": "Gross order total before discounts/fees.",
            "net_total": "Net order total after discounts.",
            "total_discount_amount": "Total discount applied to the order.",
            "shipping_cost": "Shipping cost charged to customer.",
            "actual_shipping_cost": "Actual shipping cost incurred.",
            "payment_method": "Payment method used for the order.",
            "payment_processing_fee": "Fee charged by payment processor.",
            "shipping_speed": "Selected shipping speed.",
            "shipping_address": "Shipping address text.",
            "billing_address": "Billing address text.",
            "cart_id": "Unique cart identifier.",
            "cart_item_id": "Unique cart item identifier.",
            "cart_total": "Total value of items in the cart.",
            "status": "Record status or state.",
            "created_at": "Timestamp when the record was created.",
            "updated_at": "Timestamp when the record was last updated.",
            "added_at": "Timestamp when the item was added.",
            "return_id": "Unique return identifier.",
            "return_date": "Timestamp when the return was initiated.",
            "return_type": "Type of return (e.g., refund, exchange).",
            "return_channel": "Channel used to process return.",
            "refund_method": "Refund method used.",
            "refunded_amount": "Amount refunded to the customer.",
            "reason": "Return reason.",
            "customer_status": "Customer lifecycle status.",
            "signup_date": "Date when the customer signed up.",
            "signup_channel": "Acquisition channel for the customer.",
            "loyalty_tier": "Customer loyalty tier.",
            "initial_loyalty_tier": "Initial loyalty tier at signup.",
            "loyalty_enrollment_date": "Date customer enrolled in loyalty program.",
            "clv_bucket": "Customer lifetime value bucket.",
            "gender": "Customer gender.",
            "age": "Customer age.",
            "is_guest": "Whether the customer is a guest checkout.",
            "email_verified": "Whether the email is verified.",
            "marketing_opt_in": "Whether the customer opted into marketing.",
            "phone_number": "Customer phone number.",
            "agent_id": "Sales or support agent identifier.",
            "total_items": "Count of items in the order.",
            "inventory_quantity": "Current inventory quantity.",
            "batch_id": "Batch identifier for ingestion run.",
            "ingestion_ts": "Ingestion timestamp for the record.",
            "event_id": "Unique event identifier for lineage.",
            "source_file": "Source file path in storage.",
            "is_expedited": "Whether expedited shipping was selected.",
            "is_reactivated": "Whether the customer was reactivated.",
        }

        dict_lines: list[str] = [
            "# Data Dictionary",
            "",
            "Derived from the Bronze profile report and Data Contract.",
            "",
        ]

        # Parse required fields from contract (Base Silver section)
        required_fields: dict[str, set[str]] = {}
        if args.update_contract:
            contract_text = Path(args.update_contract).read_text()
            current = None
            in_base = False
            for line in contract_text.splitlines():
                if line.startswith("## Base Silver Tables (Required Fields)"):
                    in_base = True
                    continue
                if in_base and line.startswith("## "):
                    break
                if in_base and line.startswith("### "):
                    current = line.replace("### ", "").strip()
                    required_fields.setdefault(current, set())
                    continue
                if in_base and line.startswith("- ") and current:
                    field = line.replace("- ", "", 1).strip()
                    if "(" in field:
                        field = field.split("(", 1)[0].strip()
                    required_fields[current].add(field)

        for table in sorted(schema_by_table.keys()):
            dict_lines.append(f"## {table}")
            dict_lines.append("")
            dict_lines.append(
                "| Column | Bronze Type | Base Silver Type | Required | Description |"
            )
            dict_lines.append("| --- | --- | --- | --- | --- |")
            for col, bronze_type in schema_by_table[table].items():
                base_type = bronze_to_base.get(bronze_type, bronze_type.lower())
                if col in timestamp_fields:
                    base_type = "timestamp"
                elif col in date_fields:
                    base_type = "date"
                elif col in bool_fields:
                    base_type = "bool"
                required_flag = (
                    "Yes" if col in required_fields.get(table, set()) else "No"
                )
                desc = common_desc.get(col, "")
                dict_lines.append(
                    f"| {col} | {bronze_type.lower()} | {base_type} | "
                    f"{required_flag} | {desc} |"
                )
            dict_lines.append("")

        data_dictionary, _ = apply_markdown_meta("\n".join(dict_lines), timestamp)
        dict_path.parent.mkdir(parents=True, exist_ok=True)
        dict_path.write_text(data_dictionary)
        print(f"Wrote {args.data_dictionary}")

    changelog_path = Path("CHANGELOG.md")
    if changelog_path.exists():
        scope_parts = []
        if tables_filter:
            scope_parts.append(f"tables={','.join(sorted(tables_filter))}")
        if ingest_dts:
            scope_parts.append(f"ingest_dts={','.join(sorted(ingest_dts))}")
        if months:
            scope_parts.append(f"months={','.join(sorted(months))}")
        if start_date and end_date:
            scope_parts.append(f"date_range={start_date}..{end_date}")
        elif args.date_range:
            scope_parts.append(f"date_range={args.date_range}")
        scope_text = " ".join(scope_parts) if scope_parts else "scope=all"
        changelog_line = (
            f"- Profile report refreshed {timestamp} {scope_text} "
            f"hash={report_hash}"
        )
        changelog = changelog_path.read_text()
        marker = "## [Unreleased]"
        if marker in changelog:
            before, rest = changelog.split(marker, 1)
            rest_lines = rest.splitlines()
            if rest_lines and rest_lines[0].strip() == "":
                rest_lines = rest_lines[1:]
            rest_text = "\n".join(rest_lines)
            changelog = f"{before}{marker}\n{changelog_line}\n{rest_text.lstrip()}"
            changelog_path.write_text(changelog)
            print("Updated CHANGELOG.md with profile report refresh.")


if __name__ == "__main__":
    if not os.getenv("ECOM_CLI_SUPPRESS_DEPRECATION"):
        warnings.warn(
            (
                "Deprecated: use `ecomlake bronze profile` instead of "
                "scripts/describe_parquet_samples.py"
            ),
            UserWarning,
            stacklevel=2,
        )
    main()
