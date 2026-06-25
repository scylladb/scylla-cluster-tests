"""Linter for test-case YAML metadata (test_metadata: sections).

Validates completeness, taxonomy conformance, and cross-reference accuracy
against the actual test configuration.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from sdcm.test_metadata import TestMetadata, _get_valid_backends


@dataclass
class LintResult:
    file_path: str
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return len(self.errors) == 0

    @property
    def missing(self) -> bool:
        return any("TD-001" in e for e in self.errors)


def lint_test_metadata(config_path: Path, taxonomy_path: Path | None = None) -> LintResult:
    """Validate test_metadata in a test-case YAML against taxonomy and config content.

    Note: taxonomy_path is accepted for future extensibility but currently unused —
    validation is performed against the module-level taxonomy loaded at import time.
    """
    result = LintResult(file_path=str(config_path))

    try:
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError) as exc:
        result.errors.append(f"TD-000: Failed to parse YAML: {exc}")
        return result

    # TD-001: test_metadata section must exist
    if "test_metadata" not in config or config["test_metadata"] is None:
        result.errors.append("TD-001: Missing required 'test_metadata' section")
        return result

    raw_meta = config["test_metadata"]

    # Validate via pydantic
    try:
        meta = TestMetadata(**raw_meta)
    except (TypeError, ValueError) as exc:
        result.errors.append(f"TD-003: Invalid metadata values: {exc}")
        return result

    # TD-002: description must be non-empty and >20 chars
    if not meta.description:
        result.errors.append("TD-002: 'description' field is missing or empty")
    elif len(meta.description.strip()) < 20:
        result.errors.append(f"TD-002: 'description' is too short ({len(meta.description.strip())} chars, minimum 20)")

    # TD-004: test_type should match directory name
    if meta.test_type:
        parent_dir = config_path.parent.name
        if meta.test_type not in parent_dir and parent_dir not in meta.test_type:
            result.warnings.append(f"TD-004: test_type '{meta.test_type}' may not match directory '{parent_dir}'")

    # TD-010: tier appropriate for duration
    if meta.tier and meta.duration_class:
        if meta.tier == "sanity" and meta.duration_class in ("medium", "long"):
            result.warnings.append(
                f"TD-010: tier 'sanity' is unusual for duration_class '{meta.duration_class}' — "
                "sanity tests are typically short"
            )

    # TD-011: supported_backends should include pipeline backend if detectable
    pipeline_backend = config.get("cluster_backend")
    if pipeline_backend and meta.supported_backends is not None:
        if pipeline_backend not in meta.supported_backends:
            result.warnings.append(f"TD-011: cluster_backend '{pipeline_backend}' not listed in supported_backends")

    # Cross-reference checks
    cross = cross_reference_config(config_path)
    result.warnings.extend(cross.warnings)
    result.errors.extend(cross.errors)

    return result


def cross_reference_config(config_path: Path) -> LintResult:
    """Check that metadata labels match the actual test configuration.

    Returns a LintResult with warnings for mismatches between metadata and config.
    """
    result = LintResult(file_path=str(config_path))

    try:
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError):
        return result

    raw_meta = config.get("test_metadata")
    if not raw_meta:
        return result

    try:
        meta = TestMetadata(**raw_meta)
    except (TypeError, ValueError):
        return result

    # TD-006: stress_tools should match stress commands in config
    stress_fields = [
        "stress_cmd",
        "stress_cmd_w",
        "stress_cmd_r",
        "stress_cmd_m",
        "stress_before_upgrade",
        "stress_after_cluster_upgrade",
    ]
    stress_text = " ".join(str(config.get(f, "") or "") for f in stress_fields)
    tool_patterns = {
        "cassandra-stress": r"cassandra-stress",
        "scylla-bench": r"scylla-bench",
        "ycsb": r"ycsb",
        "latte": r"latte",
        "gemini": r"gemini",
        "ndbench": r"ndbench",
        "nosqlbench": r"nosqlbench",
    }
    for tool, pattern in tool_patterns.items():
        if re.search(pattern, stress_text) and tool not in meta.stress_tools:
            result.warnings.append(f"TD-006: stress tool '{tool}' detected in config but not listed in stress_tools")

    # TD-007: supported_backends compatible with pipeline backend
    pipeline_backend = config.get("cluster_backend")
    if pipeline_backend and meta.supported_backends is not None:
        if pipeline_backend not in meta.supported_backends and pipeline_backend in _get_valid_backends():
            result.warnings.append(f"TD-007: cluster_backend '{pipeline_backend}' not in supported_backends")

    # TD-008: duration_class consistent with test_duration
    test_duration = config.get("test_duration")
    if test_duration and meta.duration_class:
        try:
            minutes = int(test_duration)
            expected = "short" if minutes < 360 else ("medium" if minutes <= 1440 else "long")
            if meta.duration_class != expected:
                result.warnings.append(
                    f"TD-008: duration_class '{meta.duration_class}' inconsistent with "
                    f"test_duration={minutes}min (expected '{expected}')"
                )
        except (ValueError, TypeError):
            result.warnings.append("TD-008: test_duration is not a valid integer, cannot verify duration_class")

    # TD-009: features consistent with config
    n_db_nodes = config.get("n_db_nodes", "")
    if isinstance(n_db_nodes, str) and len(n_db_nodes.split()) > 1:
        if "multi-dc" not in meta.features:
            result.warnings.append(
                "TD-009: multi-DC config detected (n_db_nodes has multiple values) but 'multi-dc' not in features"
            )

    if config.get("server_encrypt") and "tls-ssl" not in meta.features:
        result.warnings.append("TD-009: server_encrypt=true detected but 'tls-ssl' not in features")

    return result
