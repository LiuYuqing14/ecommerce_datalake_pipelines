"""Spec loader with layered YAML merge and env expansion."""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from .models import PipelineSpec

logger = logging.getLogger(__name__)


class SpecValidationError(Exception):
    """Raised when spec YAML fails schema validation."""

    pass


_SPEC_ENV_PATTERN = re.compile(r"\$\{([^}:]+)(:-([^}]+))?\}")


def _expand_env_vars(value: Any) -> Any:
    if isinstance(value, str):

        def replacer(match: re.Match[str]) -> str:
            var_name = match.group(1)
            default = match.group(3)
            env_value = os.environ.get(var_name)
            if env_value:
                return env_value
            if default is not None:
                return default
            logger.warning("Spec env var %s not set; leaving empty", var_name)
            return ""

        return _SPEC_ENV_PATTERN.sub(replacer, value)
    if isinstance(value, list):
        return [_expand_env_vars(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand_env_vars(val) for key, val in value.items()}
    return value


def _deep_merge(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in incoming.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        payload = yaml.safe_load(path.read_text()) or {}
    except OSError as exc:
        raise SpecValidationError(f"Failed to read spec file {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise SpecValidationError(f"Spec file {path} must be a YAML mapping")
    return payload


def load_spec(spec_path: str | Path | None = None) -> PipelineSpec:
    """Load and validate the pipeline spec.

    Args:
        spec_path: Path to a spec directory or a single YAML file.
            Defaults to $ECOM_SPEC_PATH or config/specs.

    Returns:
        Validated PipelineSpec instance.
    """
    if spec_path is None:
        spec_path = os.getenv("ECOM_SPEC_PATH", "config/specs")

    path = Path(spec_path)
    if path.is_dir():
        required = [
            "bronze.yml",
            "silver_base.yml",
            "enriched.yml",
            "dims.yml",
            "validation.yml",
        ]
        payload: dict[str, Any] = {}
        for name in required:
            spec_file = path / name
            if not spec_file.exists():
                raise SpecValidationError(f"Missing required spec file: {spec_file}")
            payload = _deep_merge(payload, _load_yaml(spec_file))
    else:
        payload = _load_yaml(path)

    expanded = _expand_env_vars(payload)

    try:
        return PipelineSpec.model_validate(expanded)
    except ValidationError as exc:
        raise SpecValidationError(f"Spec validation failed: {exc}") from exc


def load_spec_safe(spec_path: str | Path | None = None) -> PipelineSpec | None:
    """Load spec and return None on validation errors."""
    try:
        return load_spec(spec_path)
    except SpecValidationError as exc:
        logger.warning("Falling back to config defaults (spec load failed): %s", exc)
        return None
