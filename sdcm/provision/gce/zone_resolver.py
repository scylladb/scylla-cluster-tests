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
# Copyright (c) 2026 ScyllaDB

import logging
import random
from typing import Callable

from sdcm.utils.gce_utils import GceZoneResolver

LOGGER = logging.getLogger(__name__)


def _node_count_positive(value) -> bool:
    """Indicates if an SCT node-count parameter resolves to >0 in any region."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value > 0
    if isinstance(value, list):
        return any(int(n) > 0 for n in value if str(n).strip().lstrip("-").isdigit())
    if isinstance(value, str):
        return any(int(n) > 0 for n in value.split() if n.strip().lstrip("-").isdigit())
    return False


def _has_loaders(params) -> bool:
    return _node_count_positive(params.get("n_loaders"))


def _has_monitor(params) -> bool:
    return _node_count_positive(params.get("n_monitor_nodes"))


def _has_oracle(params) -> bool:
    return params.get("db_type") in ("mixed_scylla", "mixed_cassandra") and _node_count_positive(
        params.get("n_test_oracle_db_nodes")
    )


def _has_zero_token(params) -> bool:
    return bool(params.get("use_zero_nodes")) and _node_count_positive(params.get("n_db_zero_token_nodes"))


def _has_vector_store(params) -> bool:
    return _node_count_positive(params.get("n_vector_store_nodes"))


def _always(params) -> bool:  # noqa: ARG001
    return True


# (machine-type param key, gate). Gate decides whether this type will actually
# be launched; types whose gate is False are excluded from zone-filter intersection.
_MACHINE_TYPE_PARAM_GATES: tuple[tuple[str, Callable[[object], bool]], ...] = (
    ("gce_instance_type_db", _always),
    ("gce_instance_type_loader", _has_loaders),
    ("gce_instance_type_monitor", _has_monitor),
    ("zero_token_instance_type_db", _has_zero_token),
    ("instance_type_db_oracle", _has_oracle),
    ("instance_type_db_target", _always),
    ("nemesis_grow_shrink_instance_type", _always),
    ("instance_type_vector_store", _has_vector_store),
)


class NoValidAvailabilityZoneError(Exception):
    """Raised when no zone supports all required machine types in the configured region(s)."""


class GceAZResolver:
    """Resolve `availability_zone` config to zones supporting all required machine types (GCE)."""

    def __init__(self, params):
        self._params = params

    def required_machine_types(self) -> list[str]:
        """Get the deduplicated list of machine types a test will launch."""
        selected = []
        for key, gate in _MACHINE_TYPE_PARAM_GATES:
            machine_type = self._params.get(key)
            if machine_type and gate(self._params) and machine_type not in selected:
                selected.append(machine_type)
        return selected

    def resolve(self) -> None:
        """Filter `availability_zone` to zones supporting all required types in every region.

        If `pre_filter_unavailable_availability_zones` is False, returns without
        modifying params. Multi-AZ configs ("b,c,d") have invalid zones replaced with
        valid alternatives in the same region.
        Raises `NoValidAvailabilityZoneError` when no zone letter is valid in every region.
        """
        if not self._params.get("pre_filter_unavailable_availability_zones"):
            LOGGER.info("Upfront zone filter disabled; skipping GceAZResolver.resolve()")
            return

        machine_types = self.required_machine_types()
        if not machine_types:
            LOGGER.info("No machine types declared; skipping zone filter")
            return

        region_names = self._region_names()
        if not region_names:
            LOGGER.info("No region configured; skipping zone filter")
            return

        configured_letters = self._configured_az_letters()
        if not configured_letters:
            valid_letters = self._discover_valid_zone_letters(region_names, machine_types)
            if not valid_letters:
                raise NoValidAvailabilityZoneError(self._build_no_zone_error(region_names, machine_types))
            chosen = random.choice(valid_letters)
            LOGGER.info(
                "GceAZResolver: no availability_zone configured; selected '%s' from valid zones %s",
                chosen,
                valid_letters,
            )
            self._params["availability_zone"] = chosen
            return

        supported_letters = self._common_supported_letters(region_names, configured_letters, machine_types)
        if not supported_letters:
            raise NoValidAvailabilityZoneError(self._build_no_zone_error(region_names, machine_types))

        resolved = [letter for letter in configured_letters if letter in supported_letters]
        for letter in supported_letters:
            if len(resolved) >= len(configured_letters):
                break
            if letter not in resolved:
                resolved.append(letter)

        new_value = ",".join(resolved)
        original_value = self._params.get("availability_zone")
        if new_value != original_value:
            LOGGER.warning(
                "GceAZResolver: availability_zone '%s' does not support all required "
                "machine types %s in regions %s; replacing with '%s'",
                original_value,
                machine_types,
                region_names,
                new_value,
            )
            self._params["availability_zone"] = new_value
        else:
            LOGGER.info(
                "GceAZResolver: availability_zone '%s' already valid for regions %s", original_value, region_names
            )

    def _common_supported_letters(
        self, region_names: list[str], configured_letters: list[str], machine_types: list[str]
    ) -> list[str]:
        """Intersect supported zone letters across all configured regions.

        Returns letters in this order: configured letters first (preserving user
        intent), then any additional supported letters sorted alphabetically.
        """
        common: set[str] | None = None
        for region in region_names:
            preferred_full = [f"{region}-{letter}" for letter in configured_letters]
            resolver = GceZoneResolver()
            supported_full = resolver.get_common_zones(
                region=region,
                machine_types=machine_types,
                preferred_zones=preferred_full,
            )
            # GCE zone format: "us-east1-b" -> extract letter after last "-"
            letters = {zone.split("-")[-1] for zone in supported_full}
            common = letters if common is None else common & letters

        if not common:
            return []

        configured_first = [letter for letter in configured_letters if letter in common]
        additional = sorted(common - set(configured_first))
        return configured_first + additional

    def _region_names(self) -> list[str]:
        if regions := getattr(self._params, "gce_datacenters", None):
            return list(regions)
        raw = self._params.get("gce_datacenter") or ""
        if isinstance(raw, list):
            return raw
        return raw.split()

    def _configured_az_letters(self) -> list[str]:
        raw = self._params.get("availability_zone") or ""
        return [letter.strip() for letter in raw.split(",") if letter.strip()]

    def _build_no_zone_error(self, region_names: list[str], machine_types: list[str]) -> str:
        lines = [f"No zone supports all required machine types across regions {region_names}."]
        for region in region_names:
            resolver = GceZoneResolver()
            per_type = resolver.get_per_type_zones(region, machine_types)
            for mt, zones in per_type.items():
                if zones:
                    letters = [z.split("-")[-1] for z in zones]
                    lines.append(f"  {mt} in {region}: available in zones {letters}")
                else:
                    lines.append(f"  {mt} in {region}: NOT AVAILABLE in any zone")
        return "\n".join(lines)

    def _discover_valid_zone_letters(self, region_names: list[str], machine_types: list[str]) -> list[str]:
        common: set[str] | None = None
        for region in region_names:
            resolver = GceZoneResolver()
            all_zones = resolver.get_zones_for_region(region)
            supported_full = resolver.get_common_zones(
                region=region,
                machine_types=machine_types,
                preferred_zones=all_zones,
            )
            letters = {zone.split("-")[-1] for zone in supported_full}
            common = letters if common is None else common & letters

        if not common:
            return []
        return sorted(common)
