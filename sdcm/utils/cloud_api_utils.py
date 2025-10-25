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
from pathlib import Path


LOGGER = logging.getLogger(__name__)

MIN_SCYLLA_VERSION_FOR_VS = "2025.4.0"
# Node instance type limitations for Vector Search Beta on Scylla Cloud
# Source: https://cloud.docs.scylladb.com/stable/vector-search/vector-search-clusters.html
XCLOUD_VS_INSTANCE_TYPES = {
    'aws': {
        't4g.small': 175,
        't4g.medium': 176,
        'r7g.medium': 177,
    },
    'gce': {
        'e2-small': 178,
        'e2-medium': 179,
        'n4-highmem-2': 180,
    }
}


def get_cloud_rest_credentials_from_file(file_path: str) -> dict:
    """Retrieve Scylla Cloud REST credentials from a file"""
    path = Path(file_path).expanduser().resolve()
    if not path.exists():
        raise ValueError(f"Scylla Cloud REST credentials file not found: {file_path}")

    with path.open('r', encoding='utf-8') as creds_file:
        creds = json.load(creds_file)
    return creds
