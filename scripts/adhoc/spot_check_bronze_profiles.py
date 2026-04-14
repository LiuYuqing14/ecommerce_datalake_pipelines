#!/usr/bin/env python3
"""Generate spot-check markdown for bronze profile investigations."""

from __future__ import annotations

import argparse
import hashlib
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

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
    meta_lines = [
        META_START,
        f"Last updated (UTC): {timestamp}",
        f"Content hash (SHA-256): {body_hash}",
        META_END,
    ]
    meta_block = "\n".join(meta_lines)
    return f"{body}{meta_block}\n", body_hash


def apply_markdown_meta_with_profile(
    text: str,
    timestamp: str,
    profile_hash: str | None,
) -> tuple[str, str]:
    body = strip_meta_block(text).rstrip() + "\n"
    body_hash = content_hash(body)
    meta_lines = [
        META_START,
        f"Last updated (UTC): {timestamp}",
        f"Content hash (SHA-256): {body_hash}",
    ]
    if profile_hash:
        meta_lines.append(f"Profile report hash (SHA-256): {profile_hash}")
    meta_lines.append(META_END)
    meta_block = "\n".join(meta_lines)
    return f"{body}{meta_block}\n", body_hash


def parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_months(value: str) -> set[str]:
    return set(parse_csv(value))


def month_days(month: str) -> list[str]:
    year_str, month_str = month.split("-", 1)
    year = int(year_str)
    mon = int(month_str)
    days = []
    d = date(year, mon, 1)
    while d.month == mon:
        days.append(d.isoformat())
        d = (
            d.replace(day=d.day + 1)
            if d.day < 28
            else d + (date(year, mon, 28) - date(year, mon, 27))
        )
        if d.day == 1 and d.month != mon:
            break
    return days


def table_partitions(root: Path, table: str) -> dict[str, list[Path]]:
    parts: dict[str, list[Path]] = {}
    table_root = root / table
    if not table_root.exists():
        return parts
    for part_dir in sorted(table_root.glob("ingest_dt=*")):
        part = part_dir.name.split("=", 1)[-1]
        files = sorted(part_dir.glob("*.parquet"))
        if not files:
            continue
        parts[part] = files
    return parts


def load_table(
    root: Path,
    table: str,
    months: set[str],
    columns: list[str],
    max_files: int,
) -> pl.DataFrame:
    rows = []
    for part, files in sorted(table_partitions(root, table).items()):
        if months and part[:7] not in months:
            continue
        frames = []
        for file_path in files[:max_files]:
            frames.append(pl.read_parquet(file_path, columns=columns))
        if not frames:
            continue
        df = pl.concat(frames, how="diagonal")
        df = df.with_columns(pl.lit(part).alias("ingest_dt"))
        rows.append(df)
    return pl.concat(rows, how="diagonal") if rows else pl.DataFrame()


def id_reuse_table(df: pl.DataFrame, col: str, top_n: int) -> list[tuple[str, int]]:
    if df.is_empty() or col not in df.columns:
        return []
    repeats = (
        df.group_by(col)
        .agg(pl.n_unique("ingest_dt").alias("partitions"))
        .filter(pl.col("partitions") > 1)
        .sort("partitions", descending=True)
        .head(top_n)
    )
    return [(row[0], int(row[1])) for row in repeats.iter_rows()]


def cart_item_distribution(df: pl.DataFrame) -> dict[str, float]:
    if df.is_empty() or "cart_id" not in df.columns:
        return {}
    counts = df.group_by("cart_id").len()
    stats = counts.select(
        pl.len().alias("carts"),
        pl.col("len").sum().alias("items"),
        pl.col("len").mean().alias("mean"),
        pl.col("len").median().alias("median"),
        pl.col("len").quantile(0.90, interpolation="nearest").alias("p90"),
        pl.col("len").quantile(0.95, interpolation="nearest").alias("p95"),
        pl.col("len").quantile(0.99, interpolation="nearest").alias("p99"),
        pl.col("len").max().alias("max"),
    )
    return {k: float(v) for k, v in stats.row(0, named=True).items()}


def cart_item_duplicates(df: pl.DataFrame) -> dict[str, int]:
    if df.is_empty():
        return {}
    stats: dict[str, int] = {}
    if "cart_item_id" in df.columns:
        total = df.height
        unique = df["cart_item_id"].n_unique()
        stats["cart_item_id_total"] = int(total)
        stats["cart_item_id_dupes"] = int(total - unique)
    if {"cart_id", "product_id", "added_at"}.issubset(df.columns):
        total = df.height
        unique = df.select(["cart_id", "product_id", "added_at"]).n_unique()
        stats["line_key_dupes"] = int(total - unique)
    return stats


def build_markdown(lines: list[str], timestamp: str) -> tuple[str, str]:
    body = "\n".join(lines)
    return apply_markdown_meta(body, timestamp)


def extract_meta_hash(text: str) -> str | None:
    if META_START not in text or META_END not in text:
        return None
    _, rest = text.split(META_START, 1)
    block, _ = rest.split(META_END, 1)
    for line in block.splitlines():
        if line.startswith("Content hash (SHA-256): "):
            return line.split("Content hash (SHA-256): ", 1)[1].strip() or None
    return None


def get_profile_report_hash(path: Path) -> str | None:
    if not path.exists():
        return None
    content = path.read_text()
    meta_hash = extract_meta_hash(content)
    if meta_hash:
        return meta_hash
    return content_hash(strip_meta_block(content).rstrip() + "\n")


def update_changelog(
    changelog_path: Path,
    timestamp: str,
    scope_text: str,
    spot_hash: str,
    profile_hash: str | None,
) -> None:
    if not changelog_path.exists():
        return
    profile_part = f" profile_hash={profile_hash}" if profile_hash else ""
    changelog_line = (
        f"- Spot checks refreshed {timestamp} {scope_text} "
        f"spot_hash={spot_hash}{profile_part}"
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Spot-check bronze profile anomalies.")
    parser.add_argument(
        "--root", default="samples/bronze", help="Sample root directory"
    )
    parser.add_argument("--months", default="", help="Comma-separated months (YYYY-MM)")
    parser.add_argument(
        "--max-files",
        type=int,
        default=1,
        help="Max parquet files per partition to read",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Top N cart_items partitions to list",
    )
    parser.add_argument(
        "--analyze-top",
        type=int,
        default=5,
        help="Top N cart_items partitions to analyze",
    )
    parser.add_argument(
        "--output",
        default="docs/data/BRONZE_PROFILE_SPOT_CHECKS.md",
        help="Output markdown path",
    )
    parser.add_argument(
        "--profile-report",
        default="docs/data/BRONZE_PROFILE_REPORT.md",
        help="Profile report path to link hash metadata",
    )
    args = parser.parse_args()

    root = Path(args.root)
    months = parse_months(args.months)
    timestamp = utc_timestamp()

    profile_report_path = Path(args.profile_report)
    profile_hash = get_profile_report_hash(profile_report_path)

    lines: list[str] = [
        "# Bronze Profile Spot Checks",
        "",
        "Generated from local parquet samples in `samples/bronze/`.",
        "",
    ]

    if months:
        lines.append("## Scope")
        lines.append("")
        lines.append(f"- Months: {', '.join(sorted(months))}")
        lines.append(f"- Max files per partition: {args.max_files}")
        lines.append("")

    lines.append("## Returns and Return Items: ID Reuse")
    lines.append("")
    lines.append("| Table | Column | Rows | Distinct | Top Repeats Across Partitions |")
    lines.append("| --- | --- | --- | --- | --- |")

    for table, id_cols in [
        ("returns", ["return_id", "order_id", "customer_id"]),
        ("return_items", ["return_item_id", "return_id", "order_id", "product_id"]),
    ]:
        parts = table_partitions(root, table)
        if not parts:
            for col in id_cols:
                lines.append(f"| {table} | {col} | 0 | 0 | — |")
            continue
        available = pl.read_parquet(next(iter(parts.values()))[0], n_rows=1).columns
        cols = [c for c in id_cols if c in available]
        df = load_table(root, table, months, cols, args.max_files)
        for col in id_cols:
            if col not in df.columns:
                lines.append(f"| {table} | {col} | 0 | 0 | — |")
                continue
            repeats = id_reuse_table(df, col, 5)
            repeat_text = (
                ", ".join(f"{val}({cnt})" for val, cnt in repeats) if repeats else "—"
            )
            lines.append(
                f"| {table} | {col} | {df.height:,} | "
                f"{df[col].n_unique():,} | {repeat_text} |"
            )

    lines.append("")
    lines.append("## Returns Partition Coverage")
    lines.append("")
    lines.append("| Table | Month | Expected Days | Observed Days | Missing Days |")
    lines.append("| --- | --- | --- | --- | --- |")

    for table in ["returns", "return_items"]:
        parts = table_partitions(root, table)
        if not parts:
            for month in sorted(months):
                lines.append(f"| {table} | {month} | 0 | 0 | — |")
            continue
        month_set = months or {p[:7] for p in parts.keys()}
        for month in sorted(month_set):
            expected = month_days(month)
            observed = sorted([p for p in parts.keys() if p.startswith(month)])
            missing = sorted(set(expected) - set(observed))
            missing_display = ", ".join(missing[:5]) + (
                "..." if len(missing) > 5 else ""
            )
            lines.append(
                f"| {table} | {month} | {len(expected)} | {len(observed)} | "
                f"{missing_display or '—'} |"
            )

    lines.append("")
    lines.append("## cart_items Spike Validation")
    lines.append("")

    cart_parts = table_partitions(root, "cart_items")
    if not cart_parts:
        lines.append("- cart_items not found.")
    else:
        counts = []
        for part, files in cart_parts.items():
            if months and part[:7] not in months:
                continue
            row_count = 0
            for file_path in files[: args.max_files]:
                row_count += pl.read_parquet(file_path).height
            counts.append((part, row_count, len(files)))
        counts_sorted = sorted(counts, key=lambda item: item[1], reverse=True)
        rows = [c[1] for c in counts_sorted]
        if rows:
            avg = sum(rows) / len(rows)
            median = sorted(rows)[len(rows) // 2]
            lines.append(
                f"- Partitions: {len(rows)}; avg rows: {avg:.2f}; "
                f"median rows: {median}"
            )
        lines.append("")
        lines.append("| Partition | Rows | Files | Pct vs Avg |")
        lines.append("| --- | --- | --- | --- |")
        for part, row_count, file_count in counts_sorted[: args.top]:
            pct = (row_count / avg - 1) * 100 if rows else 0.0
            lines.append(f"| {part} | {row_count:,} | {file_count} | {pct:.0f}% |")

        lines.append("")
        lines.append("### Cross-Table Alignment (Top Spikes)")
        lines.append("")
        lines.append("| Partition | cart_items | shopping_carts | orders |")
        lines.append("| --- | --- | --- | --- |")

        top_dates = [p for p, _, _ in counts_sorted[: args.analyze_top]]
        for date_str in top_dates:

            def count_table(table: str, ingest_dt: str = date_str) -> int:
                files = (root / table / f"ingest_dt={ingest_dt}").glob("*.parquet")
                files = sorted(files)
                total = 0
                for file_path in files[: args.max_files]:
                    total += pl.read_parquet(file_path).height
                return total

            cart_rows = count_table("cart_items")
            cart_count = count_table("shopping_carts")
            order_count = count_table("orders")
            lines.append(
                f"| {date_str} | {cart_rows:,} | {cart_count:,} | {order_count:,} |"
            )

        lines.append("")
        lines.append("### cart_items Distribution by cart_id (Top Spikes + Baselines)")
        lines.append("")
        lines.append(
            "| Partition | Carts | Items | Mean | Median | P90 | P95 | P99 | Max |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")

        baseline_dates = []
        if counts_sorted:
            baseline_dates.append(counts_sorted[len(counts_sorted) // 2][0])
            baseline_dates.append(counts_sorted[-1][0])

        analysis_dates = sorted(set(top_dates + baseline_dates))
        for date_str in analysis_dates:
            df = load_table(
                root,
                "cart_items",
                {date_str[:7]},
                ["cart_item_id", "cart_id", "product_id", "added_at"],
                args.max_files,
            ).filter(pl.col("ingest_dt") == date_str)
            stats = cart_item_distribution(df)
            if not stats:
                continue
            lines.append(
                (
                    "| {date} | {carts} | {items} | {mean:.2f} | "
                    "{median:.0f} | {p90:.0f} | {p95:.0f} | {p99:.0f} | "
                    "{max:.0f} |"
                ).format(
                    date=date_str,
                    carts=int(stats["carts"]),
                    items=int(stats["items"]),
                    mean=stats["mean"],
                    median=stats["median"],
                    p90=stats["p90"],
                    p95=stats["p95"],
                    p99=stats["p99"],
                    max=stats["max"],
                )
            )

        lines.append("")
        lines.append("### cart_items Duplicate Checks (Top Spikes + Baselines)")
        lines.append("")
        lines.append("| Partition | cart_item_id dupes | line_key dupes |")
        lines.append("| --- | --- | --- |")
        for date_str in analysis_dates:
            df = load_table(
                root,
                "cart_items",
                {date_str[:7]},
                ["cart_item_id", "cart_id", "product_id", "added_at"],
                args.max_files,
            ).filter(pl.col("ingest_dt") == date_str)
            stats = cart_item_duplicates(df)
            if not stats:
                continue
            lines.append(
                f"| {date_str} | {stats.get('cart_item_id_dupes', 0)} | "
                f"{stats.get('line_key_dupes', 0)} |"
            )

    markdown, spot_hash = apply_markdown_meta_with_profile(
        "\n".join(lines),
        timestamp,
        profile_hash,
    )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown)
    print(f"Wrote {args.output}")

    scope_parts = []
    if months:
        scope_parts.append(f"months={','.join(sorted(months))}")
    scope_text = " ".join(scope_parts) if scope_parts else "scope=all"
    update_changelog(
        Path("CHANGELOG.md"),
        timestamp,
        scope_text,
        spot_hash,
        profile_hash,
    )


if __name__ == "__main__":
    main()
