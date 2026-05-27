"""Structured metadata model for SCT test-case documentation and labeling.

Metadata is embedded directly in test-case YAML files as a ``test_metadata:`` section
and validated on config load via pydantic. It flows to Argus at test runtime via
``submit_sct_run()``.
"""

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator


def _load_taxonomy() -> dict[str, Any]:
    taxonomy_path = Path(__file__).resolve().parents[1] / "docs" / "pipeline-labels" / "taxonomy.yaml"
    with taxonomy_path.open() as fh:
        return yaml.safe_load(fh)


_TAXONOMY = _load_taxonomy()

_TEST_TYPES: set[str] = set(_TAXONOMY["test_type"]["values"])
_TIERS: set[str] = set(_TAXONOMY["tier"]["values"].keys())
_DURATION_CLASSES: set[str] = set(_TAXONOMY["duration_class"]["values"].keys())
_STRESS_TOOLS: set[str] = set(_TAXONOMY["stress_tools"]["values"])
_WORKLOADS: set[str] = set(_TAXONOMY["workload"]["values"])


def _get_valid_backends() -> set[str]:
    """Import available_backends from sct_config lazily to avoid circular import."""
    from sdcm.sct_config import available_backends  # noqa: PLC0415 - circular import avoidance

    return set(available_backends)


class TestMetadata(BaseModel):
    """Structured metadata for test-case documentation and labeling.

    Embedded directly in test-case YAML files and validated on config load.
    Valid values are loaded from docs/pipeline-labels/taxonomy.yaml.
    """

    description: str | None = Field(default=None)
    test_type: str | None = Field(default=None)
    tier: str | None = Field(default="ondemand")
    duration_class: str | None = Field(default=None)
    supported_backends: list[str] | None = Field(default=None)
    stress_tools: list[str] = Field(default_factory=list)
    workload: str | None = Field(default=None)
    features: list[str] = Field(default_factory=list)

    @field_validator("test_type", mode="before")
    @classmethod
    def validate_test_type(cls, v):
        if v is not None and v not in _TEST_TYPES:
            raise ValueError(f"Invalid test_type '{v}'. Valid: {sorted(_TEST_TYPES)}")
        return v

    @field_validator("tier", mode="before")
    @classmethod
    def validate_tier(cls, v):
        if v is not None and v not in _TIERS:
            raise ValueError(f"Invalid tier '{v}'. Valid: {sorted(_TIERS)}")
        return v

    @field_validator("duration_class", mode="before")
    @classmethod
    def validate_duration_class(cls, v):
        if v is not None and v not in _DURATION_CLASSES:
            raise ValueError(f"Invalid duration_class '{v}'. Valid: {sorted(_DURATION_CLASSES)}")
        return v

    @field_validator("workload", mode="before")
    @classmethod
    def validate_workload(cls, v):
        if v is not None and v not in _WORKLOADS:
            raise ValueError(f"Invalid workload '{v}'. Valid: {sorted(_WORKLOADS)}")
        return v

    @field_validator("supported_backends", mode="before")
    @classmethod
    def validate_backends(cls, v):
        if v is None:
            return v
        valid_backends = _get_valid_backends()
        for b in v:
            if b not in valid_backends:
                raise ValueError(f"Invalid backend '{b}'. Valid: {sorted(valid_backends)}")
        return v

    @field_validator("stress_tools", mode="before")
    @classmethod
    def validate_stress_tools(cls, v):
        if not v:
            return v
        for tool in v:
            if tool not in _STRESS_TOOLS:
                raise ValueError(f"Invalid stress_tool '{tool}'. Valid: {sorted(_STRESS_TOOLS)}")
        return v

    @field_validator("features", mode="before")
    @classmethod
    def validate_features(cls, v):
        if not v:
            return v
        valid = set(_TAXONOMY["features"]["values"])
        for feature in v:
            if feature not in valid:
                raise ValueError(f"Invalid feature '{feature}'. Valid: {sorted(valid)}")
        return v
