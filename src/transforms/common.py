import polars as pl


def _resolve_date_column(
    frame: pl.DataFrame | pl.LazyFrame, candidates: list[str]
) -> str:
    """Resolve a date column from a list of candidates based on the frame schema."""
    columns = (
        frame.columns
        if isinstance(frame, pl.DataFrame)
        else frame.collect_schema().names()
    )
    for candidate in candidates:
        if candidate in columns:
            return candidate
    raise ValueError(f"Missing date column, tried: {candidates}")


def _date_or_null(columns: list[str], primary: str, fallback: str) -> pl.Expr:
    """Return primary/fallback cast to Date, or null."""
    if primary in columns:
        return pl.col(primary).cast(pl.Date)
    if fallback in columns:
        return pl.col(fallback).cast(pl.Date)
    return pl.lit(None)
