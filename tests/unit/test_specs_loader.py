from __future__ import annotations

from pathlib import Path

import pytest

from src.specs import SpecValidationError, load_spec


def _write_file(path: Path, content: str) -> None:
    path.write_text(content)


def _write_min_specs(base: Path) -> None:
    _write_file(
        base / "bronze.yml",
        """
bronze:
  base_path: "${BRONZE_BASE_PATH:-samples/bronze}"
  tables:
    - name: "orders"
      partition_key: "ingest_dt"
""",
    )
    _write_file(
        base / "silver_base.yml",
        """
silver_base:
  base_path: "${SILVER_BASE_PATH:-data/silver/base}"
  quarantine_path: "${SILVER_QUARANTINE_PATH:-data/silver/base/quarantine}"
  tables:
    - name: "orders"
      partition_key: "ingestion_dt"
      source: "bronze.orders"
      dbt_model: "stg_ecommerce__orders"
""",
    )
    _write_file(
        base / "enriched.yml",
        """
silver_enriched:
  base_path: "${SILVER_ENRICHED_PATH:-data/silver/enriched}"
  lookback_days: ${ENRICHED_LOOKBACK_DAYS:-0}
  tables:
    - name: "int_sales_velocity"
      partition_key: "order_dt"
      inputs: ["silver_base.orders", "silver_base.order_items"]
""",
    )
    _write_file(
        base / "dims.yml",
        """
dims:
  base_path: "${SILVER_BASE_PATH:-data/silver/base}"
  tables:
    - name: "customers"
      partition_key: "signup_dt"
      dbt_model: "stg_ecommerce__customers"
""",
    )
    _write_file(
        base / "validation.yml",
        """
validation:
  reports_enabled: true
  output_dir: "${REPORTS_BASE_PATH:-docs/validation_reports}"
  strict_mode: false
""",
    )


def test_loads_spec_dir_with_env_expansion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    _write_min_specs(spec_dir)

    monkeypatch.setenv("BRONZE_BASE_PATH", "/tmp/bronze")
    spec = load_spec(spec_dir)

    assert spec.bronze.base_path == "/tmp/bronze"
    assert spec.silver_base.tables[0].dbt_model == "stg_ecommerce__orders"
    assert spec.silver_enriched.tables[0].partition_key == "order_dt"


def test_missing_required_spec_file_raises(tmp_path: Path) -> None:
    spec_dir = tmp_path / "specs"
    spec_dir.mkdir()
    _write_min_specs(spec_dir)

    (spec_dir / "dims.yml").unlink()

    with pytest.raises(SpecValidationError):
        load_spec(spec_dir)
