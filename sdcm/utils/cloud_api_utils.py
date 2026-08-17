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

import json
import logging
import re
from decimal import Decimal, ROUND_UP
from pathlib import Path


LOGGER = logging.getLogger(__name__)

CLOUD_KEEP_ALIVE_HOURS = 72  # 3 days
CLOUD_KEEP_BUFFER_MINUTES = 125
MAX_CLUSTER_NAME_LENGTH = 63  # Siren limit for cluster name length

MIN_SCYLLA_VERSION_FOR_VS = "2025.4.0"


def compute_cluster_exp_hours(test_duration_minutes: int, keep_alive: bool = False) -> int:
    """
    Calculate ScyllaDB Cloud cluster expiration time.

    For keep_alive=True returns CLOUD_KEEP_ALIVE_HOURS number of hours, which is
    considered "keep alive" mode for cloud clusters testing.
    """
    if keep_alive:
        return CLOUD_KEEP_ALIVE_HOURS
    return int(Decimal((test_duration_minutes + CLOUD_KEEP_BUFFER_MINUTES) / 60).quantize(Decimal("1"), ROUND_UP))


def build_cloud_cluster_name(username: str, test_name: str, short_test_id: str, keep_hours: int) -> str:
    """Build ScyllaDB Cloud cluster name in format: TESTNAME-USERNAME-SHORTID-keep-Xh"""
    sanitized_username = username.replace(".", "_")
    cluster_name = f"{sanitized_username}-{short_test_id}-keep-{keep_hours:03d}h"

    prefix_len = MAX_CLUSTER_NAME_LENGTH - len(cluster_name) - 1
    if prefix_len >= 5:
        test_name = test_name[:prefix_len]
        cluster_name = f"{test_name}-{cluster_name}"

    LOGGER.debug("Generated cloud cluster name: '%s'", cluster_name)
    return cluster_name


def apply_keep_tag_to_name(cluster_name: str, keep_hours: int) -> str:
    """Apply keep tag to cluster name (adds if missing, replaces if exists)."""
    keep_tag = f"keep-{keep_hours:03d}h"
    keep_pattern = re.compile(r"keep-\d{3}h")

    if keep_pattern.search(cluster_name):
        new_name = keep_pattern.sub(keep_tag, cluster_name)
    else:
        # ensure room for keep tag that we add
        base_name = cluster_name[: MAX_CLUSTER_NAME_LENGTH - len(keep_tag) - 1]
        new_name = f"{base_name}-{keep_tag}"

    LOGGER.debug("Updated cluster name keep tag: '%s' -> '%s'", cluster_name, new_name)
    return new_name


def get_cloud_rest_credentials_from_file(file_path: str) -> dict:
    """Retrieve Scylla Cloud REST credentials from a file"""
    path = Path(file_path).expanduser().resolve()
    if not path.exists():
        raise ValueError(f"Scylla Cloud REST credentials file not found: {file_path}")

    with path.open("r", encoding="utf-8") as creds_file:
        creds = json.load(creds_file)
    return creds


def parse_availability_zones(value: str | None) -> list[str]:
    """Parse a comma-separated availability zone list from config into a clean list"""
    return [zone.strip() for zone in (value or "").split(",") if zone.strip()]


# AWS compresses region names in AZ IDs, e.g.: 'us-east-1' -> 'use1-az1', 'ap-southeast-1' -> 'apse1-az1'
AWS_AZ_ID_DIRECTIONS = {
    "north": "n",
    "south": "s",
    "east": "e",
    "west": "w",
    "central": "c",
    "northeast": "ne",
    "northwest": "nw",
    "southeast": "se",
    "southwest": "sw",
}


def aws_region_to_az_id_prefix(region_name: str) -> str | None:
    """Return AZ-ID region prefix (for example: 'us-east-1' -> 'use1')."""
    try:
        geo, direction, index = region_name.split("-")
    except ValueError:
        return None

    short_direction = AWS_AZ_ID_DIRECTIONS.get(direction)
    if short_direction is None:
        return None
    return f"{geo}{short_direction}{index}"


def expand_availability_zones(zones: list[str], node_count: int) -> list[str]:
    """Expand the configured AZ list to one entry per node."""
    if not zones:
        return []

    zone_count = len(zones)
    if node_count % zone_count:
        raise ValueError(
            f"Cannot spread {node_count} nodes evenly across {zone_count} availability zones {zones}. "
            f"Provide one zone per node or a list that divides the node count evenly."
        )

    return zones * (node_count // zone_count)
