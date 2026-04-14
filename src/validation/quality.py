"""Common validation checks (skeleton)."""

from __future__ import annotations

from typing import Any, Iterable, cast


def require_columns(columns: Iterable[str], required: Iterable[str]) -> list[str]:
    missing = [col for col in required if col not in set(columns)]
    return missing


def format_reject_rate(rejects: int, total: int) -> float:
    if total == 0:
        return 0.0
    return rejects / total


def enforce_fk(df, column: str, ref_df, ref_col: str):
    if ref_df is None or column not in df.columns or ref_col not in ref_df.columns:
        return df
    return df[df[column].isin(ref_df[ref_col])]


def enforce_non_null(df, columns: Iterable[str]):
    cols = [col for col in columns if col in df.columns]
    if not cols:
        return df
    return df.dropna(subset=cols)


def validate_table(df, config: dict[str, object]) -> list[str]:
    failures: list[str] = []
    required = cast(Iterable[str], config.get("required_columns", []))
    missing = require_columns(df.columns, required)
    if missing:
        failures.append(f"missing_required_columns: {missing}")
    primary_key = list(cast(Iterable[str], config.get("primary_key", [])))
    if primary_key:
        missing_pk = require_columns(df.columns, primary_key)
        if missing_pk:
            failures.append(f"missing_primary_key_columns: {missing_pk}")
        else:
            null_count = int(df[list(primary_key)].isna().any(axis=1).sum())
            if null_count > 0:
                failures.append(f"primary_key_nulls: {null_count}")
    return failures


def split_fk(
    df,
    column: str,
    ref_df,
    ref_col: str,
    allow_prefixes: Iterable[str] | None = None,
):
    if ref_df is None or column not in df.columns or ref_col not in ref_df.columns:
        return df, df.iloc[0:0]
    mask = df[column].isin(ref_df[ref_col])
    if allow_prefixes:
        allowed = tuple(allow_prefixes)
        mask = mask | df[column].astype("string").str.startswith(allowed)
    return df[mask], df[~mask]


def evaluate_expectations(df, expectations: Iterable[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    for exp in expectations:
        exp_type = exp.get("type")
        if exp_type == "not_null":
            columns = [col for col in exp.get("columns", []) if col in df.columns]
            if columns:
                nulls = int(df[columns].isna().any(axis=1).sum())
                if nulls > 0:
                    failures.append(
                        f"expect_not_null_failed: {columns} null_rows={nulls}"
                    )
        elif exp_type == "unique":
            columns = [col for col in exp.get("columns", []) if col in df.columns]
            if columns:
                dupes = int(df.duplicated(subset=columns).sum())
                if dupes > 0:
                    failures.append(f"expect_unique_failed: {columns} dup_rows={dupes}")
        elif exp_type == "between":
            column = exp.get("column")
            if column in df.columns:
                min_val = exp.get("min")
                max_val = exp.get("max")
                series = df[column]
                if min_val is not None:
                    below = int((series < min_val).sum())
                    if below > 0:
                        failures.append(
                            f"expect_between_failed: {column} below_min={below}"
                        )
                if max_val is not None:
                    above = int((series > max_val).sum())
                    if above > 0:
                        failures.append(
                            f"expect_between_failed: {column} above_max={above}"
                        )
        elif exp_type == "in_set":
            column = exp.get("column")
            allowed = set(exp.get("allowed", []))
            if column in df.columns and allowed:
                invalid = int((~df[column].isin(allowed)).sum())
                if invalid > 0:
                    failures.append(f"expect_in_set_failed: {column} invalid={invalid}")
        else:
            failures.append(f"unknown_expectation_type: {exp_type}")
    return failures
