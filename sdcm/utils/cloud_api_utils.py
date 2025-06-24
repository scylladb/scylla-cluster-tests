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


def get_cloud_rest_credentials_from_file(file_path: str) -> dict:
    """Retrieve Scylla Cloud REST credentials from a file"""
    path = Path(file_path).expanduser()
    if not path.exists():
        raise ValueError(f"Scylla Cloud REST credentials file not found: {file_path}")

    with path.open('r', encoding='utf-8') as creds_file:
        creds = json.load(creds_file)
    return creds
