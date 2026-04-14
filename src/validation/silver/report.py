from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from polars.exceptions import ComputeError, SchemaError
from pyarrow.lib import ArrowInvalid, ArrowTypeError

from src.validation.silver.models import SilverQualityReport

logger = logging.getLogger(__name__)


def generate_markdown_report(
    report: SilverQualityReport, output_path: str | Path
) -> None:
    """Generate self-documenting Markdown report and write to local or GCS.

    Args:
        report: Silver quality report data
        output_path: Report path (local or gs://)
    """
    from src.observability.config import get_config

    if report.overall_status == "PASS":
        overall_emoji = "✅"
    elif report.overall_status == "WARN":
        overall_emoji = "⚠️"
    else:
        overall_emoji = "❌"

    lines = [
        "# Silver Layer Quality Report",
        "",
        f"**Last Updated:** {report.timestamp}",
        f"**Run ID:** `{report.run_id}`",
        f"**Overall Status:** {overall_emoji} {report.overall_status}",
        "",
        "## Summary",
        "",
        ("| Table | Bronze | Silver | Quarantine | Pass Rate | SLA | Status |").replace(
            " ", " "
        ),
        (
            "|-------|-------------|-------------|------------|-----------|-----|--------|"
        ).replace(" ", " "),
    ]

    for metrics in report.table_metrics:
        status_emoji = {
            "PASS": "✅",
            "WARN": "⚠️",
            "FAIL": "❌",
        }.get(metrics.status, "❓")

        lines.append(
            f"| {metrics.table} "
            f"| {metrics.bronze_rows:,} "
            f"| {metrics.silver_rows:,} "
            f"| {metrics.quarantine_rows:,} "
            f"| {metrics.pass_rate:.2%} "
            f"| {metrics.sla_threshold:.0%} "
            f"| {status_emoji} {metrics.status} |"
        )

    total_tables = len(report.table_metrics)
    pass_pct = (report.tables_passing / total_tables * 100) if total_tables > 0 else 0

    lines.extend(
        [
            "",
            (
                f"**Tables Passing SLA:** {report.tables_passing}/{total_tables} "
                f"({pass_pct:.1f}%)"
            ),
        ]
    )

    if report.tables_warning > 0:
        lines.append(f"**Tables with Warnings:** {report.tables_warning}")
    if report.tables_failing > 0:
        lines.append(f"**Tables Failing:** {report.tables_failing}")

    failing_tables = [m for m in report.table_metrics if m.status == "FAIL"]
    warning_tables = [m for m in report.table_metrics if m.status == "WARN"]

    if failing_tables or warning_tables:
        lines.extend(["", "---", "", "## Issues Detected", ""])
        for metrics in failing_tables + warning_tables:
            emoji = "❌" if metrics.status == "FAIL" else "⚠️"
            lines.extend(
                [
                    (
                        f"### {emoji} {metrics.table}: Pass Rate "
                        f"{'Below' if metrics.status == 'FAIL' else 'Near'} SLA"
                    ),
                    "",
                    (
                        f"- **Pass Rate:** {metrics.pass_rate:.2%} "
                        f"(SLA: {metrics.sla_threshold:.0%})"
                    ),
                    f"- **Quarantine Count:** {metrics.quarantine_rows:,} rows",
                    "- **Recommended Action:** Investigate top quarantine reasons",
                    "",
                ]
            )
            if metrics.quarantine_breakdown:
                lines.extend(
                    [
                        "**Top Quarantine Reasons:**",
                        "",
                        "| Reason | Count | Percentage |",
                        "|--------|-------|------------|",
                    ]
                )
                for reason in metrics.quarantine_breakdown:
                    lines.append(
                        (
                            f"| {reason['reason']} | {reason['count']:,} | "
                            f"{reason['percentage']}% |"
                        )
                    )
                lines.append("")

    lines.extend(["---", "", "## Quarantine Analysis", ""])
    total_quarantined = sum(m.quarantine_rows for m in report.table_metrics)
    total_quarantine_pct = (
        (total_quarantined / report.total_processed * 100)
        if report.total_processed > 0
        else 0
    )
    lines.extend(
        [
            "### Overall Quarantine Statistics",
            "",
            (
                f"**Total Quarantined:** {total_quarantined:,} rows across "
                f"{len(report.table_metrics)} tables "
                f"({total_quarantine_pct:.1f}% of total)"
            ),
            "",
            "### Quarantine Breakdown by Table",
            "",
        ]
    )

    for metrics in sorted(
        report.table_metrics, key=lambda m: m.quarantine_rows, reverse=True
    ):
        if metrics.quarantine_rows == 0:
            continue
        quar_rate = (
            (
                metrics.quarantine_rows
                / (metrics.silver_rows + metrics.quarantine_rows)
                * 100
            )
            if (metrics.silver_rows + metrics.quarantine_rows) > 0
            else 0
        )
        emoji = "⚠️" if quar_rate > 5.0 else ""
        lines.extend(
            [
                f"#### {metrics.table}",
                f"- **Quarantine Rate:** {quar_rate:.2f}% {emoji}",
            ]
        )
        if metrics.quarantine_breakdown:
            top_reason = metrics.quarantine_breakdown[0]
            lines.append(
                f"- **Top Reason:** {top_reason['reason']} "
                f"({top_reason['percentage']}%) "
            )
        else:
            lines.append(
                "- **Top Reason:** empty partition placeholder (no real failures)"
            )
        lines.append("")

    if report.fk_mismatch_summary:
        lines.extend(["---", "", "## FK Mismatch Summary", ""])
        lines.extend(
            [
                (
                    "| Child Table | Child Key | Parent Table | Parent Key | Missing |"
                ).replace(" ", " "),
                "| --- | --- | --- | --- | --- |".replace(" ", " "),
            ]
        )
        for row in report.fk_mismatch_summary:
            lines.append(
                (
                    f"| {row['child_table']} | {row['child_key']} | "
                    f"{row['parent_table']} | {row['parent_key']} | "
                    f"{row['missing_rows']:,} |"
                )
            )
        lines.append("")

    if report.contract_issues:
        lines.extend(["---", "", "## Contract Issues", ""])
        for issue in report.contract_issues:
            lines.append(f"- **{issue['check']}**: {issue['message']}")
        lines.append("")

    lines.extend(
        [
            "---",
            "",
            "## Metadata",
            "",
            "- **Generated by:** `src/validation/silver/report.py`",
            "- **Validation Framework:** 1.1.0",
            "- **Report Format Version:** 1.0",
            "",
            "<!-- AUTO-GENERATED - DO NOT EDIT MANUALLY -->",
        ]
    )

    report_content = "\n".join(lines)

    # Write to local or GCS based on environment
    obs_config = get_config()
    if not obs_config.use_cloud_reports():
        # Local: write to filesystem
        local_path = Path(output_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_text(report_content)
        logger.info(f"Wrote Markdown report to: {local_path}")
    else:
        # Dev/Prod: write to GCS
        import fsspec

        fs = fsspec.filesystem("gcs")
        with fs.open(str(output_path), "w") as f:
            f.write(report_content)
        logger.info(f"Wrote Markdown report to GCS: {output_path}")


def build_profile_report(
    tables: list[str], silver_path: Path, output_path: Path
) -> None:
    """Generate a lightweight Silver profile report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Silver Data Profile",
        "",
        f"**Generated:** "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
        "## Table Profiles",
        "",
    ]
    for table in tables:
        table_path = silver_path / table
        if not table_path.exists():
            lines.extend([f"### {table}", "", "- **Status:** No data path found", ""])
            continue

        parquet_files = list(table_path.glob("**/*.parquet"))
        if not parquet_files:
            lines.extend(
                [f"### {table}", "", "- **Status:** No Parquet files found", ""]
            )
            continue

        try:
            df = pl.read_parquet(
                parquet_files,
                memory_map=False,
                low_memory=True,
                use_pyarrow=True,
            )
            schema = df.schema
            col_names = list(schema.keys())
            row_count = df.height
            null_counts = df.null_count().to_dicts()[0]
        except (ArrowInvalid, ArrowTypeError, OSError, ValueError) as exc:
            logger.warning(
                "Failed to profile table: parquet read error",
                extra={
                    "table": table,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            )
            lines.extend(
                [f"### {table}", "", "- **Status:** Failed to read parquet files", ""]
            )
            continue
        except (SchemaError, ComputeError) as exc:
            logger.warning(
                "Failed to profile table: schema error",
                extra={
                    "table": table,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            )
            lines.extend(
                [f"### {table}", "", "- **Status:** Failed to profile table schema", ""]
            )
            continue

        lines.extend(
            [
                f"### {table}",
                "",
                f"- **Row Count:** {row_count:,}",
                f"- **Column Count:** {len(col_names)}",
                "",
                "**Schema (column → dtype):**",
                "",
                "| Column | Dtype | Nulls |".replace(" ", " "),
                "|--------|-------|-------|".replace(" ", " "),
            ]
        )
        for col in col_names:
            dtype = str(schema[col])
            nulls = null_counts.get(col, 0)
            lines.append(f"| {col} | {dtype} | {nulls:,} |")
        lines.append("")

    output_path.write_text("\n".join(lines))
    logger.info(f"Wrote Silver profile report to: {output_path}")
