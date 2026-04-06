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
# Copyright (c) 2025 ScyllaDB
"""Utility for looking up GCP instance network bandwidth and calculating stream_io_throughput_mb_per_sec.

The GCP network parameters JSON file (gcp_net_params.json) stores per-instance-type
bandwidth data in the same spirit as the AWS aws_net_params.json used by scylla-machine-image.

Each entry is a list of 4 elements:
    [instance_type, description, default_bandwidth_gbps, tier1_bandwidth_gbps_or_null]

The ``stream_io_throughput_mb_per_sec`` value is derived by:
    1. Converting the documented bandwidth from Gbps to MB/s  (Gbps * 125)
    2. Taking 75 % of the result

This 75 % policy matches the approach used across all cloud images (AWS, Azure, OCI, GCP).

**Tier 1 networking:**
GCP Tier 1 networking is an optional per-VM configuration that must be explicitly
enabled at instance creation time.  When enabled, larger machine types gain higher
maximum egress bandwidth (up to 100–200 Gbps depending on the series).

At runtime the tier can be detected via the GCP metadata server or the Compute Engine
API ``instances/get`` response (look for ``networkPerformanceConfig.totalEgressBandwidthTier``
set to ``TIER_1``).  If detection is not available, the conservative *default*
(non-tier1) bandwidth is used.
"""

from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path
from typing import NamedTuple

LOGGER = logging.getLogger(__name__)

_GCP_NET_PARAMS_FILE = Path(__file__).parent / "../provision/common/gcp_net_params.json"

# 75 % of documented bandwidth – aligned across all cloud images
_BANDWIDTH_UTILIZATION_FACTOR = 0.75


class GcpInstanceNetworkInfo(NamedTuple):
    """Network bandwidth information for a single GCP instance type."""

    instance_type: str
    description: str
    default_bandwidth_gbps: float
    tier1_bandwidth_gbps: float | None


@lru_cache(maxsize=1)
def _load_gcp_net_params() -> dict[str, GcpInstanceNetworkInfo]:
    """Load and index the GCP network parameters JSON by instance type."""
    path = _GCP_NET_PARAMS_FILE.resolve()
    try:
        with open(path, encoding="utf-8") as fh:
            raw = json.load(fh)
    except (OSError, json.JSONDecodeError):
        LOGGER.warning("Failed to load GCP network parameters from %s", path)
        return {}

    result: dict[str, GcpInstanceNetworkInfo] = {}
    for entry in raw:
        if len(entry) < 3:
            LOGGER.warning("Skipping malformed GCP net params entry (too few elements): %s", entry)
            continue
        instance_type = entry[0]
        description = entry[1]
        default_bw = float(entry[2])
        tier1_bw = float(entry[3]) if len(entry) >= 4 and entry[3] is not None else None
        result[instance_type] = GcpInstanceNetworkInfo(
            instance_type=instance_type,
            description=description,
            default_bandwidth_gbps=default_bw,
            tier1_bandwidth_gbps=tier1_bw,
        )
    return result


def get_gcp_instance_network_info(instance_type: str) -> GcpInstanceNetworkInfo | None:
    """Return network bandwidth info for a GCP instance type, or ``None`` if unknown."""
    return _load_gcp_net_params().get(instance_type)


def _gbps_to_mb_per_sec(gbps: float) -> int:
    """Convert Gbps to MB/s (decimal: 1 Gbps = 125 MB/s)."""
    return int(gbps * 125)


def calculate_stream_io_throughput(instance_type: str, *, tier1: bool = False) -> int | None:
    """Calculate ``stream_io_throughput_mb_per_sec`` for a GCP instance type.

    Applies the 75 % bandwidth-utilisation rule used across all cloud images.

    Args:
        instance_type: GCP machine type name, e.g. ``"n2-highmem-32"``.
        tier1: Whether Tier 1 networking is enabled on the instance.

    Returns:
        The recommended ``stream_io_throughput_mb_per_sec`` value (integer),
        or ``None`` if the instance type is not found in the data file.
    """
    info = get_gcp_instance_network_info(instance_type)
    if info is None:
        return None

    if tier1 and info.tier1_bandwidth_gbps is not None:
        bandwidth_gbps = info.tier1_bandwidth_gbps
    else:
        bandwidth_gbps = info.default_bandwidth_gbps

    bandwidth_mb_per_sec = _gbps_to_mb_per_sec(bandwidth_gbps)
    return int(bandwidth_mb_per_sec * _BANDWIDTH_UTILIZATION_FACTOR)
