# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2024 ScyllaDB

"""Instance constraint parsing for constraint-based instance sizing.

This module provides the Constraint dataclass and parse_constraints function
used to express hardware requirements (vCPUs, memory, disk, architecture)
in a structured form that can then be matched against cloud instance catalogs.
"""

import logging
import math
import re
from dataclasses import dataclass, replace

from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog, InstanceTypeInfo, RoleConfig

LOG = logging.getLogger(__name__)

# Canonical field names accepted in constraint dicts
_ALIAS_TO_FIELD = {
    "vcpu": "vcpus",
    "vcpus": "vcpus",
    "memory": "memory_gb",
    "memory_gb": "memory_gb",
    "disk": "local_disk_gb",
    "local_disk_gb": "local_disk_gb",
    "local_disk_count": "local_disk_count",
    "arch": "arch",
}

# Arch normalisation map (shorthand → canonical)
_ARCH_ALIASES = {
    "arm64": "arm64",
    "arm": "arm64",
    "x86_64": "x86_64",
    "x86": "x86_64",
    "amd64": "x86_64",
}

# Unit multipliers for storage/memory suffixes
_UNIT_MULTIPLIER = {
    "gb": 1.0,
    "tb": 1024.0,
}

# Regex helpers
_RANGE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*(gb|tb)?\s*-\s*(\d+(?:\.\d+)?)\s*(gb|tb)?$", re.IGNORECASE)
_CMP_RE = re.compile(r"^(>=|<=|>|<)\s*(\d+(?:\.\d+)?)\s*(gb|tb)?$", re.IGNORECASE)
_PLAIN_NUM_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*(gb|tb)?$", re.IGNORECASE)

# Prefixes that indicate a literal cloud instance type string (non-constraint)
_LITERAL_PREFIXES = (
    "Standard_",  # Azure
    "VM.",  # OCI
    "BM.",  # OCI bare-metal
    "DenseIO.",  # OCI DenseIO
    "Optimized3.",  # OCI
)

# GCE instance types follow the pattern: <family><generation><optional-variant>-<type>-<vcpus>
# Examples: n2-standard-8, c4-highcpu-8, n2d-highmem-16, t2a-standard-4, z3-highmem-176
_GCE_PATTERN = re.compile(r"^[a-z]\d[a-z]*-[a-z]+-\d+")


class NoMatchingInstanceError(Exception):
    """Raised when no cloud instance satisfies all provided constraints."""

    def __init__(self, constraints, candidates_checked: int = 0):
        constraint_summary = ", ".join(f"{c.field} {c.operator} {c.value}" for c in constraints)
        msg = (
            f"No instance found matching constraints: [{constraint_summary}]. "
            f"Checked {candidates_checked} candidate(s)."
        )
        super().__init__(msg)
        self.constraints = constraints
        self.candidates_checked = candidates_checked


@dataclass
class Constraint:
    """A single hardware constraint expression.

    Attributes:
        field: The hardware field being constrained.
            One of: "vcpus", "memory_gb", "local_disk_gb", "arch".
        operator: The comparison operator.
            One of: "eq", "gte", "gt", "lte", "lt", "range".
        value: The constraint value.
            - int for vcpus (eq)
            - tuple[int, int] for vcpus (range)
            - float for memory_gb / local_disk_gb (eq, gte, gt, lte, lt)
            - tuple[float, float] for memory_gb / local_disk_gb (range)
            - str for arch (eq)
    """

    field: str
    operator: str
    value: object  # int | float | str | tuple


def _parse_unit_value(raw_value: str, default_multiplier: float = 1.0) -> float:
    """Parse a numeric string with an optional GB/TB suffix.

    Args:
        raw_value: A numeric string, optionally suffixed with "gb" or "tb".
        default_multiplier: Multiplier to use when no suffix is present.

    Returns:
        The numeric value as a float, scaled by the appropriate unit multiplier.
    """
    raw_value = raw_value.strip().lower()
    for suffix, mult in _UNIT_MULTIPLIER.items():
        if raw_value.endswith(suffix):
            return float(raw_value[: -len(suffix)].strip()) * mult
    return float(raw_value) * default_multiplier


def _parse_memory_or_disk(field: str, raw) -> Constraint:
    """Parse a memory or disk constraint value into a Constraint object.

    Supports plain integers/strings (interpreted as a minimum), comparison
    operators (>, >=, <, <=), and range expressions (e.g. "16-64" or
    "16gb-64gb").

    Args:
        field: Canonical field name, e.g. "memory_gb" or "local_disk_gb".
        raw: Raw constraint value — an int, float, or str.

    Returns:
        A Constraint for the specified field.

    Raises:
        ValueError: If the raw value cannot be parsed.
    """
    # Normalise to string and strip whitespace
    if isinstance(raw, (int, float)):
        raw_str = str(raw)
    else:
        raw_str = str(raw).strip()

    # Range: "16-64" or "16gb-64gb"
    m = _RANGE_RE.match(raw_str)
    if m:
        lo_val = float(m.group(1)) * _UNIT_MULTIPLIER.get((m.group(2) or "gb").lower(), 1.0)
        hi_val = float(m.group(3)) * _UNIT_MULTIPLIER.get((m.group(4) or "gb").lower(), 1.0)
        if lo_val > hi_val:
            raise ValueError(f"Invalid {field} range {raw!r}: lo ({lo_val}) > hi ({hi_val})")
        return Constraint(field, "range", (lo_val, hi_val))

    # Comparison: >=32, >32, <=64, <64 (with optional gb/tb suffix)
    m = _CMP_RE.match(raw_str)
    if m:
        op_str = m.group(1)
        num = float(m.group(2)) * _UNIT_MULTIPLIER.get((m.group(3) or "gb").lower(), 1.0)
        op_map = {">=": "gte", ">": "gt", "<=": "lte", "<": "lt"}
        return Constraint(field, op_map[op_str], num)

    # Plain number (int or float, optionally with gb/tb suffix) → gte (minimum)
    m = _PLAIN_NUM_RE.match(raw_str)
    if m:
        num = float(m.group(1)) * _UNIT_MULTIPLIER.get((m.group(2) or "gb").lower(), 1.0)
        return Constraint(field, "gte", num)

    raise ValueError(f"Cannot parse {field!r} constraint value: {raw!r}")


def _parse_vcpu(raw) -> Constraint:
    """Parse a vCPU constraint value into a Constraint object.

    Accepts plain integers (exact match), range strings (e.g. "8-16"),
    and comparison operators (e.g. ">=32", ">8", "<=64", "<16").

    Args:
        raw: Raw vCPU value — an int or a str.

    Returns:
        A Constraint for the "vcpus" field.

    Raises:
        ValueError: If the raw value cannot be parsed.
    """
    if isinstance(raw, int):
        return Constraint("vcpus", "eq", raw)

    if isinstance(raw, str):
        raw = raw.strip()
        m = re.match(r"^(\d+)\s*-\s*(\d+)$", raw)
        if m:
            lo, hi = int(m.group(1)), int(m.group(2))
            if lo > hi:
                raise ValueError(f"Invalid vCPU range {raw!r}: lo ({lo}) > hi ({hi})")
            return Constraint("vcpus", "range", (lo, hi))
        m = re.match(r"^(>=|>|<=|<)\s*(\d+)$", raw)
        if m:
            op_map = {">=": "gte", ">": "gt", "<=": "lte", "<": "lt"}
            return Constraint("vcpus", op_map[m.group(1)], int(m.group(2)))
        if raw.isdigit():
            return Constraint("vcpus", "eq", int(raw))
        raise ValueError(f"Cannot parse vcpu constraint value: {raw!r}")

    raise ValueError(f"Cannot parse vcpu constraint value: {raw!r}")


def _parse_arch(raw) -> Constraint:
    """Parse an architecture constraint value into a Constraint object.

    Normalises shorthand aliases ("arm" → "arm64", "x86" → "x86_64").

    Args:
        raw: Raw architecture value string.

    Returns:
        A Constraint for the "arch" field.

    Raises:
        ValueError: If the architecture string is not recognised.
    """
    key = str(raw).strip().lower()
    if key not in _ARCH_ALIASES:
        raise ValueError(f"Unknown architecture {raw!r}. Known values: {sorted(_ARCH_ALIASES)}")
    return Constraint("arch", "eq", _ARCH_ALIASES[key])


def parse_constraints(raw: dict) -> list:
    """Parse a raw constraint dict into a list of Constraint objects.

    The *raw* dict uses short, human-friendly key names that may come from
    SCT configuration or environment variables (where all values arrive as
    strings).

    Supported keys (aliases → canonical field):
        - "vcpu" / "vcpus"          → "vcpus"   (MANDATORY)
        - "memory" / "memory_gb"    → "memory_gb"
        - "disk" / "local_disk_gb"  → "local_disk_gb"
        - "arch"                    → "arch"

    Unknown keys are ignored with a WARNING log message.

    Args:
        raw: A dict of constraint key → value pairs, e.g.::

            {"vcpu": 8, "memory": 32, "disk": ">=500gb", "arch": "arm64"}

    Returns:
        A list of Constraint objects in the order: vcpus, then any additional
        fields in the order they appear in *raw*.

    Raises:
        ValueError: If the "vcpu" / "vcpus" key is missing.
        ValueError: If any value cannot be parsed according to the rules above.
    """
    constraints: list[Constraint] = []

    # Validate mandatory vcpu key presence before any other processing
    has_vcpu = "vcpu" in raw or "vcpus" in raw
    if not has_vcpu:
        raise ValueError("vcpu is mandatory in constraint dict")

    seen_fields: set[str] = set()

    for key, value in raw.items():
        canonical = _ALIAS_TO_FIELD.get(key)
        if canonical is None:
            LOG.warning("Ignoring unknown constraint key: %r", key)
            continue

        if canonical in seen_fields:
            # Deduplicate: first occurrence wins (aliased + canonical both present)
            LOG.warning(
                "Duplicate constraint key %r (canonical %r) ignored; using first value.",
                key,
                canonical,
            )
            continue
        seen_fields.add(canonical)

        if canonical == "vcpus":
            constraints.append(_parse_vcpu(value))
        elif canonical in ("memory_gb", "local_disk_gb", "local_disk_count"):
            constraints.append(_parse_memory_or_disk(canonical, value))
        elif canonical == "arch":
            constraints.append(_parse_arch(value))

    return constraints


def is_literal_instance_type(value) -> bool:
    """Return True if *value* looks like a literal cloud instance type string.

    A literal instance type is a string such as "i8g.2xlarge" (AWS),
    "Standard_L8s_v4" (Azure), or "n2-highmem-8" (GCE) — as opposed to a
    constraint dict or a plain word like "small".

    Heuristics applied (in order):
    1. Non-strings (e.g. dicts, lists) → False.
    2. Contains a "." → True (AWS: "i8g.2xlarge", OCI: "VM.Standard3.Flex").
    3. Starts with a known cloud-specific prefix → True.
    4. Otherwise → False.

    Args:
        value: The value to inspect.

    Returns:
        True if *value* is a recognisable cloud instance type string.
    """
    if not isinstance(value, str):
        return False

    stripped = value.strip()

    # Dots are present in AWS instance types and many OCI shapes
    if "." in stripped:
        return True

    # Known prefixes for Azure, OCI
    if stripped.startswith(_LITERAL_PREFIXES):
        return True

    # GCE pattern: e.g. c4-highcpu-8, n2d-standard-16, z3-highmem-176
    if _GCE_PATTERN.match(stripped):
        return True

    return False


# ---------------------------------------------------------------------------
# Matching engine
# ---------------------------------------------------------------------------


def _matches_constraint(instance: InstanceTypeInfo, constraint: Constraint) -> bool:  # noqa: PLR0911
    """Return True if *instance* satisfies the given *constraint*.

    For Flex shapes (instances with min/max memory range), memory_gb
    constraints are checked against the configurable range rather than
    the single default value.

    Args:
        instance: The cloud instance to test.
        constraint: A single hardware constraint to evaluate.

    Returns:
        True if the instance attribute satisfies the constraint.
    """
    attr = getattr(instance, constraint.field)
    op = constraint.operator

    is_flex_memory = (
        constraint.field == "memory_gb" and instance.min_memory_gb is not None and instance.max_memory_gb is not None
    )

    if is_flex_memory:
        return _matches_flex_memory(instance.min_memory_gb, instance.max_memory_gb, op, constraint.value)

    if op == "eq":
        return attr == constraint.value
    if op == "gte":
        return attr >= constraint.value
    if op == "gt":
        return attr > constraint.value
    if op == "lte":
        return attr <= constraint.value
    if op == "lt":
        return attr < constraint.value
    if op == "range":
        lo, hi = constraint.value
        return lo <= attr <= hi

    return False


def _matches_flex_memory(min_mem: float, max_mem: float, op: str, value) -> bool:
    checks = {
        "eq": lambda: min_mem <= value <= max_mem,
        "gte": lambda: max_mem >= value,
        "gt": lambda: max_mem > value,
        "lte": lambda: min_mem <= value,
        "lt": lambda: min_mem < value,
        "range": lambda: max_mem >= value[0] and min_mem <= value[1],
    }
    return checks.get(op, lambda: False)()


def _resolve_flex_instance(
    instance: InstanceTypeInfo,
    constraints: list[Constraint],
    mem_per_vcpu: float = 4.0,
) -> InstanceTypeInfo:
    """Resolve actual memory for OCI Flex shapes and update the instance_type name.

    For Flex shapes (those with min_memory_gb/max_memory_gb set), the catalog
    stores a standard default memory. After selection we pick the actual memory
    based on the user's memory constraint, clamp it to the allowed range, round
    up to the nearest whole GB, and append ":MemoryGB" to the instance_type.

    Non-Flex instances are returned unchanged.
    """
    if instance.min_memory_gb is None or instance.max_memory_gb is None:
        return instance

    mem_constraint = next((c for c in constraints if c.field == "memory_gb"), None)
    min_mem = instance.min_memory_gb
    max_mem = instance.max_memory_gb

    if mem_constraint is None:
        actual_mem = max(min_mem, min(instance.vcpus * mem_per_vcpu, max_mem))
    elif mem_constraint.operator == "eq":
        actual_mem = max(min_mem, min(mem_constraint.value, max_mem))
    elif mem_constraint.operator in ("gte", "gt"):
        actual_mem = max(min_mem, min(mem_constraint.value, max_mem))
    elif mem_constraint.operator in ("lte", "lt"):
        default_mem = max(min_mem, min(instance.vcpus * mem_per_vcpu, max_mem))
        actual_mem = min(default_mem, mem_constraint.value)
    elif mem_constraint.operator == "range":
        lo, _hi = mem_constraint.value
        actual_mem = max(min_mem, min(lo, max_mem))
    else:
        actual_mem = min_mem

    actual_mem = float(math.ceil(actual_mem))

    instance_type = instance.instance_type
    # Append ":MemoryGB" only if not already present (base form is "Shape:OCPUs")
    parts = instance_type.split(":")
    if len(parts) == 2:
        instance_type = f"{instance_type}:{int(actual_mem)}"

    return replace(
        instance,
        memory_gb=actual_mem,
        instance_type=instance_type,
    )


def _apply_implicit_constraints(parsed: list[Constraint], role_cfg: RoleConfig) -> None:
    existing_fields = {c.field for c in parsed}
    for field, raw_value in role_cfg.implicit_constraints.items():
        if field in existing_fields:
            continue
        if field == "local_disk_count" and "local_disk_gb" in existing_fields:
            continue
        if isinstance(raw_value, (int, float)) and not isinstance(raw_value, str):
            parsed.append(Constraint(field, "eq", float(raw_value)))
        else:
            parsed.append(_parse_memory_or_disk(field, raw_value))


def _apply_implicit_constraints_fallback(parsed: list[Constraint], role: str) -> None:
    _FALLBACK_DB = {"db", "db_oracle", "zero_token"}
    _FALLBACK_COMPUTE = {"loader", "monitor"}
    if role in _FALLBACK_DB and not any(c.field in ("local_disk_gb", "local_disk_count") for c in parsed):
        parsed.append(Constraint("local_disk_count", "gt", 0))
    if role in _FALLBACK_COMPUTE and not any(c.field in ("local_disk_gb", "local_disk_count") for c in parsed):
        parsed.append(Constraint("local_disk_count", "eq", 0))


def _build_sort_key(
    sort_order: list[str] | None,
    family_rank: dict[str, int],
    num_preferred: int,
    region: str | None = None,
):
    _NO_PRICE = float("inf")
    order = sort_order or ["preferred_family_rank", "price_per_hour", "vcpus"]
    field_getters = {
        "preferred_family_rank": lambda inst: family_rank.get(inst.family, num_preferred),
        "price_per_hour": lambda inst: inst.get_price(region) if inst.get_price(region) is not None else _NO_PRICE,
        "vcpus": lambda inst: inst.vcpus,
        "memory_gb": lambda inst: inst.memory_gb,
        "local_disk_gb": lambda inst: inst.local_disk_gb,
    }
    active = [field_getters[f] for f in order if f in field_getters]
    return lambda inst: tuple(g(inst) for g in active)


def select_instance(
    catalog: InstanceCatalog,
    role: str,
    cloud: str,
    constraints: dict,
    arch: str | None = None,
    region: str | None = None,
) -> InstanceTypeInfo:
    """Select the best cloud instance matching the given constraints.

    Algorithm:
    1. Parse raw *constraints* dict into Constraint objects.
    2. Determine effective architecture (from *arch* param, constraint dict,
       or cloud catalog default) and add an implicit arch constraint if not
       already present.
    3. For db/db_oracle/zero_token roles, implicitly require local storage
       (``local_disk_count > 0``) unless the caller explicitly constrained
       ``local_disk_gb``.
    4. For loader/monitor roles, implicitly require no local storage
       (``local_disk_count == 0``) unless the caller explicitly constrained
       ``local_disk_gb`` or ``local_disk_count``.
    5. Filter all cloud instances by the full constraint set.
    6. Narrow to preferred-family candidates when available.
    7. Sort: preferred family order → price ascending → vcpus ascending.
    8. Return the first (best) match.

    Args:
        catalog: Instance catalog to search.
        role: Workload role, e.g. "db", "loader", "monitor".
        cloud: Cloud provider name, e.g. "aws", "gce".
        constraints: Raw constraint dict, e.g. ``{"vcpu": 8, "memory": ">60"}``.
        arch: Optional architecture override ("x86_64", "arm64", or aliases).
            When provided, overrides any "arch" key in *constraints* and the
            catalog default.

    Returns:
        The best-matching InstanceTypeInfo.

    Raises:
        NoMatchingInstanceError: If no instance satisfies all constraints.
    """
    parsed = parse_constraints(constraints)

    sizing = catalog.sizing_config
    role_cfg = sizing.roles.get(role) if sizing else None

    parsed_arch_constraint = next((c for c in parsed if c.field == "arch"), None)

    if arch is not None:
        effective_arch = _ARCH_ALIASES.get(arch.strip().lower(), arch)
    elif parsed_arch_constraint is not None:
        effective_arch = parsed_arch_constraint.value
    else:
        cloud_default_arch = catalog.cloud_defaults.get(cloud, {}).get("arch", "x86_64")
        if role_cfg and role_cfg.arch_source == "cloud_default":
            effective_arch = cloud_default_arch
        elif role_cfg and role_cfg.arch:
            effective_arch = role_cfg.arch
        elif not role_cfg and role in {"db", "db_oracle", "zero_token"}:
            effective_arch = cloud_default_arch
        else:
            effective_arch = "x86_64"

    if parsed_arch_constraint is None:
        parsed.append(Constraint("arch", "eq", effective_arch))
    elif arch is not None:
        parsed = [c for c in parsed if c.field != "arch"]
        parsed.append(Constraint("arch", "eq", effective_arch))

    if role_cfg:
        _apply_implicit_constraints(parsed, role_cfg)
    else:
        _apply_implicit_constraints_fallback(parsed, role)

    preferred_families: list[str] = catalog.preferred_families.get(role, {}).get(cloud, [])
    family_rank: dict[str, int] = {fam: idx for idx, fam in enumerate(preferred_families)}

    cloud_instances = catalog.get_instances(cloud)

    candidates = [inst for inst in cloud_instances if all(_matches_constraint(inst, c) for c in parsed)]

    if not candidates:
        raise NoMatchingInstanceError(parsed, len(cloud_instances))

    if preferred_families:
        preferred_candidates = [inst for inst in candidates if inst.family in family_rank]
        if preferred_candidates:
            candidates = preferred_candidates

    candidates.sort(
        key=_build_sort_key(
            sort_order=sizing.sort_order if sizing else None,
            family_rank=family_rank,
            num_preferred=len(preferred_families),
            region=region,
        )
    )
    best = candidates[0]
    mem_per_vcpu = sizing.flex_defaults.get("mem_per_vcpu", 4.0) if sizing else 4.0
    return _resolve_flex_instance(best, parsed, mem_per_vcpu=mem_per_vcpu)
