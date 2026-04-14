#!/usr/bin/env python3
"""Run dims dbt models based on the spec."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import warnings
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.runners.dims_snapshot import snapshot_dims  # noqa: E402
from src.specs import load_spec_safe  # noqa: E402

DEFAULT_MODELS = [
    "stg_ecommerce__customers",
    "stg_ecommerce__customers_quarantine",
    "stg_ecommerce__product_catalog",
    "stg_ecommerce__product_catalog_quarantine",
]


def build_models() -> list[str]:
    spec = load_spec_safe()
    if not spec:
        return DEFAULT_MODELS
    models: list[str] = []
    for table in spec.dims.tables:
        models.append(table.dbt_model)
        models.append(f"{table.dbt_model}_quarantine")
    return models


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run dims models + snapshots.")
    parser.add_argument(
        "--run-date",
        default=date.today().isoformat(),
        help="Snapshot date (YYYY-MM-DD). Defaults to today.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    models = build_models()
    cmd = [sys.executable, "-m", "src.runners.base_silver", "--select", *models]
    subprocess.run(cmd, check=True)
    snapshot_dims(args.run_date)


if __name__ == "__main__":
    if not os.getenv("ECOM_CLI_SUPPRESS_DEPRECATION"):
        warnings.warn(
            (
                "Deprecated: use `ecomlake dim run` instead of "
                "scripts/run_dims_from_spec.py"
            ),
            UserWarning,
            stacklevel=2,
        )
    main()
