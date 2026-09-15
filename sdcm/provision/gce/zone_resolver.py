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
from typing import Callable, Iterator

from sdcm.provision.common.fallback import is_spot_capacity_scoring_enabled
from sdcm.provision.gce.capacity_advisor import rank_regions, rank_zone_letters
from sdcm.provision.gce.constants import SUPPORTED_GCE_REGIONS
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


def _node_count_total(value) -> int:
    """Sum an SCT node-count parameter, which may be an int or a per-DC list/space-separated string."""
    if value is None or isinstance(value, bool):
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    items = value if isinstance(value, list) else str(value).split()
    total = 0
    for item in items:
        try:
            total += int(item)
        except TypeError, ValueError:
            continue
    return total


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
    ("gce_instance_type_db_oracle", _has_oracle),
    ("instance_type_db_target", _always),
    ("nemesis_grow_shrink_instance_type", _always),
    ("instance_type_vector_store", _has_vector_store),
)


def is_capacity_advice_enabled(params) -> bool:
    """Return True when zone ordering should consult GCE Capacity Advisor."""
    return is_spot_capacity_scoring_enabled(params, backend="gce")


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
        Raises `NoValidAvailabilityZoneError` when no zone letter is valid in every region, or
        when fewer zones are valid than the configuration asks for - the zone count sets
        `racks_count`, so returning fewer would silently change the cluster topology.
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
            ranked = self._rank_letters_by_capacity_advice(region_names, valid_letters)
            # With advice available the best zone is a strictly better pick than a shuffle; without it, keep
            # the random choice, which spreads unconfigured jobs across zones instead of piling them onto one.
            chosen = ranked[0] if ranked is not valid_letters else random.choice(valid_letters)
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

        # Capacity advice only reorders the pool the zone slots are filled from. By default the configured
        # letters keep priority, so an explicit `availability_zone` stays honoured and only backfilled slots
        # are advice-driven; `spot_score_overrides_configured_az` opts into letting obtainability win outright.
        ranked_letters = self._rank_letters_by_capacity_advice(region_names, supported_letters)
        if self._params.get("spot_score_overrides_configured_az") and is_capacity_advice_enabled(self._params):
            resolved = ranked_letters[: len(configured_letters)]
        else:
            resolved = [letter for letter in configured_letters if letter in supported_letters]
            for letter in ranked_letters:
                if len(resolved) >= len(configured_letters):
                    break
                if letter not in resolved:
                    resolved.append(letter)

        if len(resolved) < len(configured_letters):
            # `availability_zone: 'b,c,d'` asks for three racks, one per zone. Handing back two
            # would silently build a two-rack cluster - `racks_count` is derived from this very
            # parameter - so the test would run a topology it never asked for. Dropping a rack is
            # the job's decision, not the resolver's. `get_region_fallback_candidates` already
            # holds a region to the configured zone count; `resolve` is held to the same rule.
            raise NoValidAvailabilityZoneError(
                f"availability_zone '{','.join(configured_letters)}' requests "
                f"{len(configured_letters)} zone(s), but only {len(resolved)} "
                f"('{','.join(resolved)}') support all required machine types {machine_types} "
                f"in every region of {region_names}. Reduce availability_zone, drop a region, "
                f"or use machine types available in more zones."
            )

        new_value = ",".join(resolved)
        original_value = self._params.get("availability_zone")
        if new_value != original_value:
            # Two different reasons land here, and saying the wrong one sends whoever reads this log chasing a
            # machine-type problem that does not exist: the configured zone may be genuinely unsupported for
            # the required types, or perfectly valid and merely outranked on obtainability.
            dropped = [letter for letter in configured_letters if letter not in supported_letters]
            if dropped:
                LOGGER.warning(
                    "GceAZResolver: availability_zone '%s' does not support all required "
                    "machine types %s in regions %s; replacing with '%s'",
                    original_value,
                    machine_types,
                    region_names,
                    new_value,
                )
            else:
                LOGGER.info(
                    "GceAZResolver: availability_zone '%s' is valid but outranked on capacity advice in "
                    "regions %s; using '%s' (spot_score_overrides_configured_az is enabled)",
                    original_value,
                    region_names,
                    new_value,
                )
            self._params["availability_zone"] = new_value
        else:
            LOGGER.info(
                "GceAZResolver: availability_zone '%s' already valid for regions %s", original_value, region_names
            )

    def get_region_fallback_candidates(self) -> Iterator[tuple[str, list[str]]]:
        """Yield ordered ``(region, az_letters)`` candidates for cluster region fallback.

        A region is eligible only if it can supply the configured number of zones
        that support all required machine types. The current region is excluded (it
        is the starting point).

        Candidates are yielded lazily: each region's zone/machine-type availability is
        probed only when the consumer actually reaches it, so a fallback that succeeds on
        the first candidate never pays to scan the remaining regions.

        Unlike AWS there is NO peering check: the SCT GCE network is a single global
        VPC, so every region's subnet is reachable, and GCE images are global, so no
        per-region image re-resolution is needed.
        """
        region_names = self._region_names()
        if not region_names:
            return
        current_region = region_names[0]
        configured_letters = self._configured_az_letters()
        cardinality = len(configured_letters) or 1
        machine_types = self.required_machine_types()

        for region in self._ordered_fallback_regions(exclude={current_region}):
            letters = self._common_supported_letters([region], configured_letters, machine_types)
            if len(letters) < cardinality:
                LOGGER.info(
                    "Region fallback: skipping %s (only %d zone(s) support %s, need %d)",
                    region,
                    len(letters),
                    machine_types,
                    cardinality,
                )
                continue
            yield region, letters[:cardinality]

    def get_dc_fallback_candidates(self, dc_index: int) -> Iterator[tuple[str, list[str]]]:
        """Yield ordered ``(region, az_letters)`` candidates to relocate the DC at ``dc_index``.

        A region is eligible if no DC currently occupies it and it supports the *configured* zone
        letters for all required machine types. Like the single-region variant there is NO peering or
        image check (global VPC, global images).

        Two properties the multi-DC fallback loop depends on:

        * The in-use region set is re-read from live config before every candidate. The loop holds one
          generator per DC for its whole lifetime while *other* DCs keep relocating, so a set
          snapshotted on first use goes stale and can hand back a region a sibling DC has since moved
          into - landing two DCs in the same region.
        * Only regions supporting the configured letters qualify, and those same letters are yielded.
          GCE ``availability_zone`` is a single global setting that DC relocation deliberately leaves
          alone, so qualifying a region through some *alternative* letter would aim the retry at a
          zone that region does not have.

        Candidates are yielded lazily: each region is probed only when the consumer reaches it,
        so a DC that relocates on its first candidate never scans the remaining regions.
        """
        if dc_index >= len(self._region_names()):
            return
        configured_letters = self._configured_az_letters()
        cardinality = len(configured_letters) or 1
        machine_types = self.required_machine_types()

        # Ranked once for the whole generator, but with an empty exclude set: the in-use check below has to
        # stay per-candidate (sibling DCs relocate while this generator is alive), so it cannot be folded into
        # the ranking.
        for region in self._ordered_fallback_regions(exclude=set()):
            # Re-read per candidate: sibling DCs may have relocated since the previous yield.
            if region in set(self._region_names()):
                continue
            letters = self._common_supported_letters([region], configured_letters, machine_types)
            if configured_letters:
                if missing := [letter for letter in configured_letters if letter not in letters]:
                    LOGGER.info(
                        "Region fallback (DC %d): skipping %s (configured zone(s) %s do not support %s)",
                        dc_index,
                        region,
                        ",".join(missing),
                        machine_types,
                    )
                    continue
                yield region, list(configured_letters)
            elif len(letters) < cardinality:
                LOGGER.info(
                    "Region fallback (DC %d): skipping %s (only %d zone(s) support %s, need %d)",
                    dc_index,
                    region,
                    len(letters),
                    machine_types,
                    cardinality,
                )
            else:
                yield region, letters[:cardinality]

    def get_az_fallback_candidates(self) -> Iterator[list[str]]:
        """Yield alternative zone-letter sets within the currently configured region(s).

        Backs `fallback_to_next_availability_zone` on the modern provisioning path: one exhausted zone
        should retry the region's remaining zones before the cluster relocates to a different region.

        Restricted to single-AZ configs, mirroring the legacy cluster path (:mod:`sdcm.cluster_gce`),
        which only retries zones for single-node provisioning. With several configured letters there is
        no way to tell which one the capacity error refers to without re-deriving the whole placement,
        so nothing is yielded and region fallback takes over.
        """
        configured_letters = self._configured_az_letters()
        if len(configured_letters) != 1:
            LOGGER.info(
                "Zone fallback: skipping, availability_zone '%s' is not a single zone",
                ",".join(configured_letters) or "(unset)",
            )
            return
        region_names = self._region_names()
        if not region_names:
            return

        machine_types = self.required_machine_types()
        for letter in self._common_supported_letters(region_names, configured_letters, machine_types):
            if letter in configured_letters:
                continue
            yield [letter]

    def advised_machine_types(self) -> list[str]:
        """Machine types to ask Capacity Advisor about - DB types only.

        `instanceSelections` means "any of these will do" for one homogeneous request, not "I need all of
        these". Mixing in a small, plentiful loader or monitor type would let it answer for the whole request
        and flatten obtainability across zones, destroying the ranking. DB nodes are also where the capacity
        risk and the cost actually are.
        """
        selected = []
        for key in ("gce_instance_type_db", "zero_token_instance_type_db", "instance_type_db_target"):
            if (machine_type := self._params.get(key)) and machine_type not in selected:
                selected.append(machine_type)
        return selected

    def advised_size(self) -> int:
        """Instance count the advice request should be sized for - DB nodes only, matching the types above."""
        total = _node_count_total(self._params.get("n_db_nodes"))
        if _has_zero_token(self._params):
            total += _node_count_total(self._params.get("n_db_zero_token_nodes"))
        return max(total, 1)

    def _rank_letters_by_capacity_advice(self, region_names: list[str], letters: list[str]) -> list[str]:
        """Reorder `letters` best-first by Capacity Advisor obtainability.

        A no-op unless advice is enabled, and a no-op for multi-region configs: a zone letter there must be
        valid in every region, and a letter that scores well in one region may score poorly in another, so
        there is no single meaningful ranking. Returns the given list object unchanged when the advice is
        unavailable, which callers can compare by identity to tell "not ranked" from "ranked to the same
        order".
        """
        if len(letters) < 2 or not is_capacity_advice_enabled(self._params):
            return letters
        if len(region_names) != 1:
            LOGGER.debug("GCE capacity advice: skipping zone ranking for multi-region config %s", region_names)
            return letters

        machine_types = self.advised_machine_types()
        if not machine_types:
            return letters

        min_obtainability = float(self._params.get("gce_spot_obtainability_min") or 0.0)
        ranked = rank_zone_letters(
            machine_types=machine_types,
            size=self.advised_size(),
            region=region_names[0],
            zone_letters=letters,
            min_obtainability=min_obtainability,
        )
        if ranked is None:
            return letters
        # `gce_spot_obtainability_min` can drop zones, and the caller fills a fixed number of zone slots from
        # what comes back. On GCE that count is also `racks_count`, so a short list would silently build a
        # cluster with fewer racks than the test was written for - which `resolve()` already refuses to do for
        # the machine-type filter. Hold the advice filter to the same rule.
        required = min(len(self._configured_az_letters()), len(letters))
        if len(ranked) < required:
            raise NoValidAvailabilityZoneError(
                f"Only {len(ranked)} zone(s) in {region_names[0]} reached "
                f"gce_spot_obtainability_min={min_obtainability} for {machine_types}, but {required} are "
                f"needed (candidates were {letters}, qualifying {ranked}); lower the threshold, use machine "
                f"types available in more zones, or pick another region."
            )
        if ranked != letters:
            LOGGER.info(
                "GCE capacity advice reordered zone candidates in %s: %s -> %s", region_names[0], letters, ranked
            )
        return ranked

    def _ordered_fallback_regions(self, exclude: set[str]) -> list[str]:
        """Candidate regions for relocation, obtainability-ordered when advice is enabled.

        Ranking regions costs one advice call per zone of each candidate, so it is done once here for the
        whole list rather than per candidate as the consumer walks it lazily.
        """
        regions = [region for region in SUPPORTED_GCE_REGIONS if region not in exclude]
        if not is_capacity_advice_enabled(self._params) or len(regions) < 2:
            return regions
        machine_types = self.advised_machine_types()
        if not machine_types:
            return regions
        resolver = GceZoneResolver()
        zones_per_region = {region: resolver.get_zones_for_region(region) for region in regions}
        ranked = rank_regions(
            machine_types=machine_types,
            size=self.advised_size(),
            regions=regions,
            zones_per_region=zones_per_region,
        )
        if ranked and ranked != regions:
            LOGGER.info("GCE capacity advice reordered region fallback candidates: %s -> %s", regions, ranked)
        return ranked or regions

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
