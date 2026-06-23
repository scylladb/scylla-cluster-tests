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

"""Centralized GCE capacity and provisioning error classification."""

CAPACITY_ERROR_MARKERS: list[str] = [
    "ZONE_RESOURCE_POOL_EXHAUSTED",
    "RESOURCE_POOL_EXHAUSTED",
    "stockout",
]

QUOTA_ERROR_MARKERS: list[str] = [
    "QUOTA_EXCEEDED",
    "quotaExceeded",
]

TYPE_NOT_AVAILABLE_MARKERS: list[str] = [
    "RESOURCE_NOT_FOUND",
    "machineTypeNotFound",
]


def is_zone_capacity_error(exception: BaseException) -> bool:
    """Return True if `exception` is a transient GCE zone capacity exhaustion error."""
    error_str = str(exception)
    return any(marker in error_str for marker in CAPACITY_ERROR_MARKERS)


def is_quota_error(exception: BaseException) -> bool:
    """Return True if `exception` is a GCE regional quota exhaustion error."""
    error_str = str(exception)
    return any(marker in error_str for marker in QUOTA_ERROR_MARKERS)


def is_type_unavailable_error(exception: BaseException) -> bool:
    """Return True if the machine type does not exist in the region/zone at all."""
    error_str = str(exception)
    return any(marker in error_str for marker in TYPE_NOT_AVAILABLE_MARKERS)


def classify_provisioning_error(exception: BaseException) -> str:
    """Classify a GCE provisioning error for fallback routing.

    Returns one of: "capacity", "quota", "type_unavailable", "unknown".
    """
    if is_zone_capacity_error(exception):
        return "capacity"
    if is_quota_error(exception):
        return "quota"
    if is_type_unavailable_error(exception):
        return "type_unavailable"
    return "unknown"
