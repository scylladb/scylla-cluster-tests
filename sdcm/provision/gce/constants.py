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

"""
Constants for GCE provisioning.
"""

# Label constraints (GCE labels are more restrictive than AWS tags)
MAX_LABEL_KEY_LENGTH = 63
MAX_LABEL_VALUE_LENGTH = 63

# Disk types
DISK_TYPE_PD_STANDARD = "pd-standard"
DISK_TYPE_LOCAL_SSD = "local-ssd"

# Disk interfaces
DISK_INTERFACE_NVME = "NVME"

# Default configurations
DEFAULT_BOOT_DISK_SIZE_GB = 50

# Network tags for firewall rules
NETWORK_TAG_SCT_NETWORK_ONLY = "sct-network-only"
NETWORK_TAG_SCT_ALLOW_PUBLIC = "sct-allow-public"

# Regions eligible for whole-cluster region fallback (mirror of AWS_SUPPORTED_REGIONS).
# The SCT GCE network is a single global VPC and images are global resources, so any
# region with SCT infra prepared is reachable without peering or image re-resolution.
SUPPORTED_GCE_REGIONS: list[str] = [
    "us-east1",
    "us-east4",
    "us-west1",
    "us-central1",
]
