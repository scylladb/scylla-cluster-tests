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

from sdcm.provision.aws.capacity_reservation import SCTCapacityReservation
from sdcm.provision.aws.utils import cleanup_abandoned_region


def enforce_single_region_gate(params) -> None:
    """Require exactly one configured region for region fallback."""
    if len(params.region_names) != 1:
        raise ValueError(
            f"fallback_to_next_region requires a single configured region (got {params.region_names}); "
            "region fallback does not support multi-DC."
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
