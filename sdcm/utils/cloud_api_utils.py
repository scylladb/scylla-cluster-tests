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
from decimal import Decimal, ROUND_UP
from pathlib import Path


LOGGER = logging.getLogger(__name__)

CLOUD_KEEP_ALIVE_HOURS = 360  # 15 days
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


def get_cloud_rest_credentials_from_file(file_path: str) -> dict:
    """Retrieve Scylla Cloud REST credentials from a file"""
    path = Path(file_path).expanduser().resolve()
    if not path.exists():
        raise ValueError(f"Scylla Cloud REST credentials file not found: {file_path}")

    with path.open("r", encoding="utf-8") as creds_file:
        creds = json.load(creds_file)
    return creds
