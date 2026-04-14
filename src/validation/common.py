"""Shared utilities for validation scripts."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Union

import polars as pl
import pyarrow.parquet as pq
from pyarrow.lib import ArrowInvalid, ArrowTypeError

from src.observability import get_logger
from src.settings import load_settings
from src.specs import load_spec_safe

logger = get_logger(__name__)


@dataclass
class ValidationStatus:
    """Standardized validation status."""

    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


def is_gcs_path(path: str) -> bool:
    """Check if a path string is a GCS URI."""
    return path.startswith("gs://")


def is_parquet_file(path: Path) -> bool:
    """Return True if file has Parquet magic bytes in header and footer."""
    try:
        with path.open("rb") as handle:
            header = handle.read(4)
            if header != b"PAR1":
                return False
            handle.seek(-4, 2)
            footer = handle.read(4)
            return footer == b"PAR1"
    except OSError:
        return False


def get_gcs_filesystem():
    try:
        import fsspec
    except ModuleNotFoundError as exc:
        raise RuntimeError("gcsfs is required for gs:// validation reads") from exc
    return fsspec.filesystem("gcs")


def join_path(base: Union[Path, str], *parts: str) -> Union[Path, str]:
    base_str = str(base)
    if is_gcs_path(base_str):
        return "/".join([base_str.rstrip("/"), *parts])
    return Path(base_str).joinpath(*parts)


def path_exists(path: Union[Path, str]) -> bool:
    path_str = str(path)
    if is_gcs_path(path_str):
        fs = get_gcs_filesystem()
        return fs.exists(path_str)
    return Path(path_str).exists()


def collect_parquet_files(
    path: Union[Path, str],
    partition_key: str | None = None,
    partitions: list[str] | None = None,
) -> list[Union[Path, str]]:
    """Collect valid Parquet files under a path, using manifest if available."""
    path_str = str(path)
    candidates: list[Union[Path, str]] = []

    if is_gcs_path(path_str):
        fs = get_gcs_filesystem()

        def _ensure_gs(p):
            p_str = str(p)
            return f"gs://{p_str}" if not p_str.startswith("gs://") else p_str

        # 1. Try to read from _MANIFEST.json first (Fastest)
        manifest_path = f"{path_str.rstrip('/')}/_MANIFEST.json"
        try:
            if fs.exists(manifest_path):
                with fs.open(manifest_path, "r") as f:
                    manifest = json.load(f)

                files = manifest.get("files", [])
                base_dir = path_str.rstrip("/")

                # Filter files based on requested partitions
                for file_entry in files:
                    file_rel_path = (
                        file_entry.get("path")
                        if isinstance(file_entry, dict)
                        else file_entry
                    )
                    if not file_rel_path:
                        continue

                    full_path = f"{base_dir}/{file_rel_path}"

                    if partition_key and partitions:
                        match = False
                        for val in partitions:
                            if f"/{partition_key}={val}/" in full_path:
                                match = True
                                break
                        if not match:
                            continue

                    candidates.append(_ensure_gs(full_path))

                if candidates:
                    logger.info(
                        f"Collected {len(candidates)} files from manifest. "
                        f"Sample: {candidates[0]} (type: {type(candidates[0])})"
                    )
                    return sorted(candidates)
        except Exception as exc:
            logger.warning(
                f"Failed to use manifest for file collection at {manifest_path}, "
                f"falling back to glob: {exc}"
            )

        # 2. Fallback to glob (Slow)
        if partition_key and partitions:
            for value in partitions:
                partition_path = f"{path_str.rstrip('/')}/{partition_key}={value}"
                if not fs.exists(partition_path):
                    continue
                candidates.extend(
                    [_ensure_gs(p) for p in fs.glob(f"{partition_path}/**/*.parquet")]
                )
        else:
            candidates = [
                _ensure_gs(p) for p in fs.glob(f"{path_str.rstrip('/')}/**/*.parquet")
            ]

        if candidates:
            logger.info(
                f"Collected {len(candidates)} files via glob. "
                f"Sample: {candidates[0]} (type: {type(candidates[0])})"
            )

        return sorted(candidates)

    path_obj = Path(path_str)
    if partition_key and partitions:
        for value in partitions:
            local_partition_path = path_obj / f"{partition_key}={value}"
            if not local_partition_path.exists():
                continue
            candidates.extend(local_partition_path.glob("**/*.parquet"))
    else:
        candidates = list(path_obj.glob("**/*.parquet"))
    if not candidates:
        return []

    valid_files: list[Path | str] = []
    invalid_files: list[Path | str] = []
    seen: set[Path] = set()
    for file_path in candidates:
        if not isinstance(file_path, Path):
            valid_files.append(file_path)
            continue
        if file_path in seen:
            continue
        seen.add(file_path)
        if is_parquet_file(file_path):
            valid_files.append(file_path)
        else:
            invalid_files.append(file_path)

    if invalid_files:
        sample = ", ".join(str(p) for p in invalid_files[:3])
        logger.warning(
            "Skipping invalid parquet files",
            path=str(path),
            invalid_count=len(invalid_files),
            sample=sample,
        )

    return valid_files


def count_parquet_rows(
    path: Union[Path, str],
    partition_key: str | None = None,
    partitions: list[str] | None = None,
) -> int:
    """Count total rows in all Parquet files in a directory.

    Optimized to use _MANIFEST.json if available to avoid recursive file scans.
    """
    if not path_exists(path):
        logger.warning(f"Path does not exist: {path}")
        return 0

    path_str = str(path)
    is_gcs = is_gcs_path(path_str)
    fs = get_gcs_filesystem() if is_gcs else None

    # 1. Determine which subdirectories (partitions) to scan
    target_dirs = []
    if partition_key:
        if partitions:
            # Targeted partitions
            target_dirs = [
                f"{path_str.rstrip('/')}/{partition_key}={v}"
                for val in partitions
                for v in (val.split(",") if "," in val else [val])
            ]
        else:
            # Full scan: list all partition directories
            partition_values = list_partitions(path, partition_key)
            target_dirs = [
                f"{path_str.rstrip('/')}/{partition_key}={v}" for v in partition_values
            ]
    else:
        # Unpartitioned table
        target_dirs = [path_str]

    total_rows = 0
    manifest_name = "_MANIFEST.json"

    for d in target_dirs:
        manifest_found = False
        m_path = f"{d.rstrip('/')}/{manifest_name}"

        # Try manifest first (Fast)
        try:
            if is_gcs and fs:
                if fs.exists(m_path):
                    with fs.open(m_path, "r") as f:
                        data = json.load(f)
                        total_rows += int(data.get("total_rows", 0))
                        manifest_found = True
            elif not is_gcs:
                m_file = Path(m_path)
                if m_file.exists():
                    data = json.loads(m_file.read_text())
                    total_rows += int(data.get("total_rows", 0))
                    manifest_found = True
        except Exception as exc:
            logger.warning(f"Failed to read manifest at {m_path}: {exc}")

        # Fallback to parquet scan if manifest missing or failed (Slow)
        if not manifest_found:
            files = collect_parquet_files(d)
            for file_path in files:
                try:
                    if is_gcs and fs:
                        with fs.open(file_path, "rb") as handle:
                            parquet_file = pq.ParquetFile(handle)
                            total_rows += parquet_file.metadata.num_rows
                    elif not is_gcs:
                        parquet_file = pq.ParquetFile(file_path)
                        total_rows += parquet_file.metadata.num_rows
                except Exception as exc:
                    logger.error(f"Failed to read rows from {file_path}: {exc}")

    return total_rows


def read_parquet_safe(
    path: Union[Path, str],
    columns: list[str] | None = None,
    n_rows: int | None = None,
) -> pl.DataFrame | None:
    """Read a parquet file or directory safely, returning None on failure."""

    def _is_decimal_dtype(dtype: pl.DataType) -> bool:
        return str(dtype).startswith("Decimal")

    parquet_files = collect_parquet_files(path)
    if not parquet_files:
        return None
    parquet_paths = [str(p) for p in parquet_files]
    try:
        # Prefer Polars native reader (use_pyarrow=False)
        # to avoid GCSFile type issues with PyArrow
        return pl.read_parquet(
            parquet_paths,
            columns=columns,
            n_rows=n_rows,
            memory_map=False,
            low_memory=True,
            use_pyarrow=False,
        )
    except (
        ArrowInvalid,
        ArrowTypeError,
        OSError,
        ValueError,
        TypeError,
        pl.exceptions.ComputeError,
    ) as exc:
        logger.warning(
            "Failed to read parquet; falling back to per-file read",
            path=str(path),
            error_type=type(exc).__name__,
            error=str(exc),
        )

    frames: list[pl.DataFrame] = []
    remaining = n_rows
    for parquet_file in parquet_files:
        try:
            if isinstance(parquet_file, Path):
                frame = pl.read_parquet(
                    parquet_file,
                    columns=columns,
                    n_rows=remaining,
                    memory_map=False,
                    low_memory=True,
                    use_pyarrow=False,
                )
            elif is_gcs_path(str(parquet_file)):
                fs = get_gcs_filesystem()
                with fs.open(parquet_file, "rb") as handle:
                    frame = pl.read_parquet(
                        handle,
                        columns=columns,
                        n_rows=remaining,
                        memory_map=False,
                        low_memory=True,
                        use_pyarrow=False,
                    )
            else:
                frame = pl.read_parquet(
                    str(parquet_file),
                    columns=columns,
                    n_rows=remaining,
                    memory_map=False,
                    low_memory=True,
                    use_pyarrow=False,
                )
        except (ArrowInvalid, ArrowTypeError, OSError, ValueError) as exc:
            logger.error(
                "Failed to read parquet file",
                path=str(parquet_file),
                error_type=type(exc).__name__,
                error=str(exc),
            )
            continue

        for name, dtype in frame.schema.items():
            if dtype == pl.Categorical:
                frame = frame.with_columns(pl.col(name).cast(pl.Utf8))
            elif _is_decimal_dtype(dtype):
                frame = frame.with_columns(pl.col(name).cast(pl.Float64))

        frames.append(frame)
        if remaining is not None:
            remaining = max(remaining - frame.height, 0)
            if remaining == 0:
                break

    if not frames:
        return None

    if len(frames) == 1:
        return frames[0]

    return pl.concat(frames, how="vertical", rechunk=True)


def list_partitions(path: Union[Path, str], partition_key: str) -> list[str]:
    """List available partition values for a given key."""
    path_str = str(path)
    if is_gcs_path(path_str):
        fs = get_gcs_filesystem()
        entries = fs.glob(f"{path_str.rstrip('/')}/{partition_key}=*")
        partitions = []
        for entry in entries:
            name = entry.rstrip("/").split("/")[-1]
            if name.startswith(f"{partition_key}="):
                partitions.append(name.split("=", 1)[-1])
        return sorted(set(partitions))

    partitions = []
    for part_dir in Path(path_str).glob(f"{partition_key}=*"):
        if part_dir.is_dir():
            partitions.append(part_dir.name.split("=", 1)[-1])
    return sorted(partitions)


def resolve_layer_paths(
    config_path: str = "config/config.yml",
    bronze_over: str | None = None,
    silver_over: str | None = None,
    enriched_over: str | None = None,
    spec_path: str | None = None,
) -> dict[str, Path | str]:
    """Unified path resolution for validation scripts."""
    settings = load_settings(config_path)
    pl = settings.pipeline
    spec = load_spec_safe(spec_path)

    def _resolve_raw(over, env_var, bucket, prefix, spec_default: str | None) -> str:
        if over:
            return over
        env_val = os.getenv(env_var)
        if env_val:
            return env_val
        if spec_default:
            return spec_default
        return settings.resolve_path(bucket, prefix)

    def _maybe_path(value: str) -> Path | str:
        return value if is_gcs_path(value) else Path(value)

    bronze_raw = _resolve_raw(
        bronze_over,
        "BRONZE_BASE_PATH",
        pl.bronze_bucket,
        pl.bronze_prefix,
        spec.bronze.base_path if spec else None,
    )
    silver_raw = _resolve_raw(
        silver_over,
        "SILVER_BASE_PATH",
        pl.silver_bucket,
        pl.silver_base_prefix,
        spec.silver_base.base_path if spec else None,
    )
    enriched_raw = _resolve_raw(
        enriched_over,
        "SILVER_ENRICHED_PATH",
        pl.silver_bucket,
        pl.silver_enriched_prefix,
        spec.silver_enriched.base_path if spec else None,
    )

    quarantine_raw = os.getenv("SILVER_QUARANTINE_PATH")
    if not quarantine_raw:
        if is_gcs_path(silver_raw):
            quarantine_raw = f"{silver_raw.rstrip('/')}/quarantine"
        else:
            quarantine_raw = str(Path(silver_raw) / "quarantine")

    return {
        "bronze": _maybe_path(bronze_raw),
        "silver": _maybe_path(silver_raw),
        "enriched": _maybe_path(enriched_raw),
        "quarantine": _maybe_path(quarantine_raw),
    }


def resolve_reports_enabled(spec_path: str | None = None) -> bool:
    """Resolve report toggle via env override or spec default."""
    env_val = os.getenv("REPORTS_ENABLED")
    if env_val is not None:
        return env_val.lower() in {"1", "true", "yes", "on"}
    spec = load_spec_safe(spec_path)
    if spec and spec.validation is not None:
        return spec.validation.reports_enabled
    return True


def get_overall_status(statuses: list[str]) -> str:
    """Determine overall status from a list of table statuses."""
    if any(s == ValidationStatus.FAIL for s in statuses):
        return ValidationStatus.FAIL
    if any(s == ValidationStatus.WARN for s in statuses):
        return ValidationStatus.WARN
    return ValidationStatus.PASS


def handle_exit(overall_status: str, enforce: bool, env: str) -> int:
    """Standardized exit logic for validation gating."""
    if overall_status == ValidationStatus.FAIL:
        if enforce or env == "prod":
            return 1
    return 0
