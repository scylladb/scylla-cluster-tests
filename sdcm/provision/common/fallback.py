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

"""Backend-agnostic gates for capacity-error fallback behavior.

Both AWS and GCE provisioning consult these to decide whether to retry provisioning in the next
availability zone (AZ fallback) or relocate the whole cluster to another region (region fallback).
They live here - not under a single backend - so neither backend imports the other's package.
"""

FALLBACK_SUPPORTED_BACKENDS: tuple[str, ...] = ("aws", "gce")


def is_az_fallback_enabled(params) -> bool:
    """Return True when AZ fallback on capacity errors is enabled.

    Reads `fallback_to_next_availability_zone` (backend-agnostic), falling back to the deprecated
    `aws_fallback_to_next_availability_zone` alias.
    """
    if (value := params.get("fallback_to_next_availability_zone")) is not None:
        return bool(value)
    return bool(params.get("aws_fallback_to_next_availability_zone"))


def is_region_fallback_enabled(params) -> bool:
    """Return True when whole-cluster region fallback on capacity errors is enabled."""
    return bool(params.get("fallback_to_next_region")) and params.get("cluster_backend") in FALLBACK_SUPPORTED_BACKENDS
