"""Spec models for spec-driven orchestration."""

from __future__ import annotations

from pydantic import BaseModel, Field


class SpecSemanticCheck(BaseModel):
    """Schema for a single semantic check expression."""

    name: str = Field(..., min_length=1)
    expr: str = Field(..., min_length=1)


class QualitySpec(BaseModel):
    """Quality thresholds for a silver base table."""

    sla: float | None = None
    min_rows: int | None = None
    allow_empty: bool = False


class BronzeTableSpec(BaseModel):
    name: str = Field(..., min_length=1)
    partition_key: str = Field(..., min_length=1)
    schema_path: str | None = None


class SilverBaseTableSpec(BaseModel):
    name: str = Field(..., min_length=1)
    partition_key: str = Field(..., min_length=1)
    source: str = Field(..., min_length=1)
    dbt_model: str = Field(..., min_length=1)
    quality: QualitySpec | None = None


class EnrichedTableSpec(BaseModel):
    name: str = Field(..., min_length=1)
    partition_key: str = Field(..., min_length=1)
    inputs: list[str] = Field(default_factory=list)
    min_rows: int | None = None
    semantic_checks: list[SpecSemanticCheck] = Field(default_factory=list)
    sanity_checks: list[str] = Field(default_factory=list)


class DimsTableSpec(BaseModel):
    name: str = Field(..., min_length=1)
    partition_key: str = Field(..., min_length=1)
    dbt_model: str = Field(..., min_length=1)


class BronzeSpec(BaseModel):
    base_path: str = Field(..., min_length=1)
    tables: list[BronzeTableSpec] = Field(default_factory=list)


class SilverBaseSpec(BaseModel):
    base_path: str = Field(..., min_length=1)
    quarantine_path: str = Field(..., min_length=1)
    tables: list[SilverBaseTableSpec] = Field(default_factory=list)


class SilverEnrichedSpec(BaseModel):
    base_path: str = Field(..., min_length=1)
    lookback_days: int = 0
    tables: list[EnrichedTableSpec] = Field(default_factory=list)


class DimsSpec(BaseModel):
    base_path: str = Field(..., min_length=1)
    tables: list[DimsTableSpec] = Field(default_factory=list)


class ValidationSpec(BaseModel):
    reports_enabled: bool = True
    output_dir: str = Field(..., min_length=1)
    strict_mode: bool = False


class PipelineSpec(BaseModel):
    bronze: BronzeSpec
    silver_base: SilverBaseSpec
    silver_enriched: SilverEnrichedSpec
    dims: DimsSpec
    validation: ValidationSpec
