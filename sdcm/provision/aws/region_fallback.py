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

"""Shared helpers for AWS region fallback.

Both the modern provisioning path (`sct_provision.aws.layout.SCTProvisionAWSLayout`) and the
legacy tester path (`tester.ClusterTester.get_cluster_aws`) relocate the full cluster to another
region when region capacity is exhausted. This module shares their common placement and cleanup logic.
"""

import os
from collections.abc import Callable

from sdcm.provision.aws.capacity_errors import RegionAMINotFoundError
from sdcm.provision.aws.capacity_reservation import SCTCapacityReservation
from sdcm.provision.aws.utils import cleanup_abandoned_region
from sdcm.utils.common import convert_name_to_ami_if_needed, find_equivalent_ami


def enforce_single_region_gate(params) -> None:
    """Require exactly one configured region for region fallback."""
    if len(params.region_names) != 1:
        raise ValueError(
            f"fallback_to_next_region requires a single configured region (got {params.region_names}); "
            "region fallback does not support multi-DC."
        )


def enforce_multi_dc_fallback_supported(params) -> None:
    """Reject multi-DC fallback when region-scoped reservations are enabled."""
    if params.get("use_capacity_reservation") or params.get("use_dedicated_host"):
        raise ValueError(
            "fallback_to_next_region with multiple regions does not support use_capacity_reservation "
            "or use_dedicated_host (both are region-scoped); disable them or use a single region."
        )


def switch_region(
    params, region: str, az_letters: list[str], source_region: str | None, invalidate_caches: Callable[[], None]
) -> None:
    """Switch to a target region and refresh region-bound state.

    AMIs are re-resolved before any region state changes - if resolution fails,
    the caller stays pinned to the source region with no partial state applied.
    """
    params.resolve_amis([region], source_region=source_region)

    # region_names is env-first, so env. var is to be updated as well
    os.environ["SCT_REGION_NAME"] = region
    params["region_name"] = region
    params["availability_zone"] = ",".join(az_letters)

    invalidate_caches()
    SCTCapacityReservation.reservations = {}


def remap_dc_ami(params, dc_index: int, source_region: str, target_region: str) -> None:
    """Remap the relocated DC's AMI in each AMI parameter from source to target region."""
    region_count = len(params.region_names)
    intent = getattr(params, "_ami_params_snapshot", None) or {}
    pending: dict[str, str] = {}
    for key in params.ami_id_params:
        value = params.get(key)
        if not value:
            continue

        amis = value.split()
        if len(amis) not in (1, region_count):
            raise ValueError(
                f"{key} has {len(amis)} AMI(s) but the config has {region_count} region(s); "
                "cannot positionally remap it for region fallback"
            )

        if dc_index >= len(amis):
            continue

        amis[dc_index] = _resolve_relocated_ami(
            key=key,
            original_intent=intent.get(key),
            current_ami=amis[dc_index],
            dc_index=dc_index,
            region_count=region_count,
            source_region=source_region,
            target_region=target_region,
        )
        pending[key] = " ".join(amis)

    for key, new_value in pending.items():
        params[key] = new_value


def _resolve_relocated_ami(
    *, key, original_intent, current_ami, dc_index, region_count, source_region, target_region
) -> str:
    """Resolve one AMI param for the relocated DC in ``target_region``."""
    error_suffix = f"(from {source_region}) in {target_region}; region ineligible for fallback"

    def _raise_not_found(value: str) -> None:
        raise RegionAMINotFoundError(f"No equivalent AMI for {key}={value} {error_suffix}")

    tokens = (original_intent or "").split()
    use_name = bool(tokens and not tokens[0].startswith("ami-"))

    if use_name:
        name = tokens[dc_index] if len(tokens) == region_count else tokens[0]
        try:
            resolved = convert_name_to_ami_if_needed(name, (target_region,)).split()
        except ValueError as exc:
            raise RegionAMINotFoundError(f"No equivalent AMI for {key}={name} {error_suffix}") from exc

        if not resolved or not resolved[0].startswith("ami-"):
            _raise_not_found(name)
        return resolved[0]

    matches = find_equivalent_ami(current_ami, source_region, target_regions=[target_region])
    if not matches:
        _raise_not_found(current_ami)
    return matches[0]["ami_id"]


def switch_dc_region(
    params, dc_index: int, region: str, source_region: str, invalidate_caches: Callable[[], None]
) -> None:
    """Relocate one DC to the target region and refresh region-derived state."""
    remap_dc_ami(params, dc_index, source_region=source_region, target_region=region)

    region_names = list(params.region_names)
    region_names[dc_index] = region
    joined = " ".join(region_names)
    os.environ["SCT_REGION_NAME"] = joined
    params["region_name"] = joined

    invalidate_caches()


def restore_region(params, region: str | None, availability_zone: str, env_region: str | None) -> None:
    """Restore original region and AZ values after a failed relocation."""
    if env_region is None:
        os.environ.pop("SCT_REGION_NAME", None)
    else:
        os.environ["SCT_REGION_NAME"] = env_region

    if region is not None:
        params["region_name"] = region
    params["availability_zone"] = availability_zone


def cleanup_region(test_id: str, region: str | None, partial_cleanup: Callable[[], None]) -> None:
    """Clean up abandoned resources in the old region."""
    partial_cleanup()
    if region:
        cleanup_abandoned_region(test_id, region)
