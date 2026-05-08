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
from typing import Callable

from sdcm.utils.aws_region import AwsRegion

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


# (instance-type param key, gate). The gate decides whether this type will actually
# be launched by the test; types whose gate is False are excluded from the AZ-filter
# intersection so SCT defaults like `instance_type_vector_store: 't4g.medium'` do not
# constrain AZ choice for tests that have n_vector_store_nodes=0.
_INSTANCE_TYPE_PARAM_GATES: tuple[tuple[str, Callable[[object], bool]], ...] = (
    ("instance_type_db", _always),
    ("instance_type_loader", _has_loaders),
    ("instance_type_monitor", _has_monitor),
    ("zero_token_instance_type_db", _has_zero_token),
    ("instance_type_db_oracle", _has_oracle),
    ("instance_type_db_target", _always),
    ("nemesis_grow_shrink_instance_type", _always),
    ("instance_type_vector_store", _has_vector_store),
)


class NoValidAvailabilityZoneError(Exception):
    """Raised when no AZ supports all required instance types in the configured region(s)."""


class AZResolver:
    """Resolve `availability_zone` config to AZs supporting all required instance types."""

    def __init__(self, params):
        self._params = params

    def required_instance_types(self) -> list[str]:
        """Get the deduplicated list of instance types a test will launch."""
        selected = []
        for key, gate in _INSTANCE_TYPE_PARAM_GATES:
            instance_type = self._params.get(key)
            if instance_type and gate(self._params) and instance_type not in selected:
                selected.append(instance_type)
        return selected

    def resolve(self) -> None:
        """Filter `availability_zone` to AZs supporting all required types in every region.

        If `pre_filter_unavailable_availability_zones` is False, returns without
        modifying params. Multi-AZ configs ("a,b,c") have invalid AZs replaced with
        valid alternatives that are valid across all configured regions.
        Raises `NoValidAvailabilityZoneError` when no AZ letter is valid in every region.
        """
        if not self._params.get("pre_filter_unavailable_availability_zones"):
            LOGGER.info("Upfront AZ filter disabled; skipping AZResolver.resolve()")
            return

        instance_types = self.required_instance_types()
        if not instance_types:
            LOGGER.info("No instance types declared; skipping AZ filter")
            return

        region_names = self._region_names()
        if not region_names:
            LOGGER.info("No region configured; skipping AZ filter")
            return

        configured_letters = self._configured_az_letters()
        supported_letters = self._common_supported_letters(region_names, configured_letters, instance_types)
        if not supported_letters:
            raise NoValidAvailabilityZoneError(
                f"No availability zone supports all required instance types {instance_types} "
                f"across regions {region_names}; cannot proceed with provisioning."
            )

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
                "AZResolver: availability_zone '%s' does not support all required "
                "instance types %s in regions %s; replacing with '%s'",
                original_value,
                instance_types,
                region_names,
                new_value,
            )
            self._params["availability_zone"] = new_value
        else:
            LOGGER.info("AZResolver: availability_zone '%s' already valid for regions %s", original_value, region_names)

    @staticmethod
    def _common_supported_letters(
        region_names: list[str], configured_letters: list[str], instance_types: list[str]
    ) -> list[str]:
        """Intersect supported AZ letters across all configured regions.

        Returns letters in this order: configured letters first (preserving user
        intent), then any additional supported letters sorted alphabetically.
        """
        common: set[str] | None = None
        for region in region_names:
            preferred_full = [f"{region}{letter}" for letter in configured_letters]
            supported_full = AwsRegion(region_name=region).get_common_availability_zones(
                instance_types=instance_types,
                preferred_azs=preferred_full,
            )
            letters = {az[len(region) :] for az in supported_full}
            common = letters if common is None else common & letters

        if not common:
            return []

        configured_first = [l for l in configured_letters if l in common]
        additional = sorted(common - set(configured_first))
        return configured_first + additional

    def _region_names(self) -> list[str]:
        if regions := getattr(self._params, "region_names", None):
            return list(regions)
        raw = self._params.get("region_name") or ""
        return raw.split()

    def _configured_az_letters(self) -> list[str]:
        raw = self._params.get("availability_zone") or ""
        return [letter.strip() for letter in raw.split(",") if letter.strip()]
