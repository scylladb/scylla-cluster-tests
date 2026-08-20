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

# Timeout values (in seconds)
OPERATION_TIMEOUT = 600  # 10 minutes for general operations
SPOT_REQUEST_TIMEOUT = 300  # 5 minutes for spot instance requests
INSTANCE_READY_TIMEOUT = 300  # 5 minutes for instance to be ready

# Instance name constraints
MAX_INSTANCE_NAME_LENGTH = 63  # GCE maximum instance name length
INSTANCE_NAME_PATTERN = r"^[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?$"

# Label constraints (GCE labels are more restrictive than AWS tags)
MAX_LABEL_KEY_LENGTH = 63
MAX_LABEL_VALUE_LENGTH = 63
LABEL_KEY_PATTERN = r"^[a-z](?:[-a-z0-9_]{0,62})?$"
LABEL_VALUE_PATTERN = r"^(?:[-a-z0-9_]{0,63})?$"

# Disk types
DISK_TYPE_PD_STANDARD = "pd-standard"
DISK_TYPE_PD_SSD = "pd-ssd"
DISK_TYPE_PD_BALANCED = "pd-balanced"
DISK_TYPE_LOCAL_SSD = "local-ssd"

# Disk interfaces
DISK_INTERFACE_NVME = "NVME"
DISK_INTERFACE_SCSI = "SCSI"

# Default configurations
DEFAULT_BOOT_DISK_SIZE_GB = 50
DEFAULT_ROOT_DISK_TYPE = DISK_TYPE_PD_STANDARD

# Network tags for firewall rules
NETWORK_TAG_SCT_NETWORK_ONLY = "sct-network-only"
NETWORK_TAG_SCT_ALLOW_PUBLIC = "sct-allow-public"

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Regions eligible for whole-cluster region fallback (mirror of AWS_SUPPORTED_REGIONS).
# The SCT GCE network is a single global VPC and images are global resources, so any
# region with SCT infra prepared is reachable without peering or image re-resolution.
SUPPORTED_GCE_REGIONS: list[str] = [
    "us-east1",
    "us-east4",
    "us-west1",
    "us-central1",
]

# Multiple network interfaces.
# GCE supports one NIC per vCPU, with an effective range of 2 to 8 NICs.
# Each secondary NIC uses its own subnet in the same VPC, created by `hydra prepare-regions -c gce`,
# which reserves a CIDR for one secondary subnet per region - hence 2 supported interfaces.
GCE_MIN_NETWORK_INTERFACES = 2
GCE_MAX_NETWORK_INTERFACES = 8
GCE_SUPPORTED_NETWORK_INTERFACES = 2
SECONDARY_SUBNET_NAME_TMPL = "{network_name}-nic{index}"
