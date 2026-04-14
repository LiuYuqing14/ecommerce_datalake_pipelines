"""Audit logging helpers for silver runs."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class AuditRecord:
    run_id: str
    table: str
    ingest_dt: str
    partition_key: str
    partition_value: str
    schema_version: str
    input_rows: int
    output_rows: int
    reject_rows: int
    started_at: str
    ended_at: str
    duration_sec: float
    quality_failures: list[str]

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def write_audit_file(path: str | Path, record: AuditRecord) -> None:
    path_str = str(path)
    if path_str.startswith("gs://"):
        try:
            import fsspec
        except ImportError as exc:
            raise RuntimeError("fsspec is required for GCS audit writes") from exc
        with fsspec.open(path_str, "w") as handle:
            handle.write(record.to_json())
        return
    Path(path_str).parent.mkdir(parents=True, exist_ok=True)
    Path(path_str).write_text(record.to_json())


def build_audit_record(
    run_id: str,
    table: str,
    ingest_dt: str,
    partition_key: str,
    partition_value: str,
    schema_version: str,
    input_rows: int,
    output_rows: int,
    reject_rows: int,
    started_at: str,
    ended_at: str,
    quality_failures: list[str] | None = None,
) -> AuditRecord:
    quality = quality_failures or []
    start_dt = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(ended_at.replace("Z", "+00:00"))
    duration_sec = (end_dt - start_dt).total_seconds()
    return AuditRecord(
        run_id=run_id,
        table=table,
        ingest_dt=ingest_dt,
        partition_key=partition_key,
        partition_value=partition_value,
        schema_version=schema_version,
        input_rows=input_rows,
        output_rows=output_rows,
        reject_rows=reject_rows,
        started_at=started_at,
        ended_at=ended_at,
        duration_sec=duration_sec,
        quality_failures=quality,
    )


def summarize_counts(
    input_rows: int, output_rows: int, reject_rows: int
) -> dict[str, Any]:
    return {
        "input_rows": input_rows,
        "output_rows": output_rows,
        "reject_rows": reject_rows,
    }
