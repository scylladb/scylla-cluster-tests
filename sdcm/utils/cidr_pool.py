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

import ipaddress
import logging
import random
from typing import Optional

from sdcm.utils.aws_region import AwsRegion
from sdcm.utils.gce_region import GceRegion

LOGGER = logging.getLogger(__name__)


class CidrAllocationError(Exception):
    """Exception for CIDR allocation errors"""


class CidrPoolManager:
    """Manager for dynamic CIDR allocation for Scylla Cloud clusters"""

    SCYLLA_CLOUD_CIDR_BASE = "172.31.0.0/16"
    DEFAULT_SUBNET_SIZE = 24
    RESERVED_RANGES = ["172.31.0.0/24"]  # default CIDR block for new Scylla Cloud clusters; ignore it in SCT

    def __init__(self, cidr_base: str = None, subnet_size: int = None):
        """
        Initialize CIDR pool manager

        :param cidr_base: base CIDR range for allocating blocks (e.g., "172.31.0.0/16")
        :param subnet_size: size of allocated subnets (e.g., 24 for /24)
        """
        self.cidr_base = ipaddress.ip_network(cidr_base or self.SCYLLA_CLOUD_CIDR_BASE)
        self.subnet_size = subnet_size or self.DEFAULT_SUBNET_SIZE
        self.reserved_ranges = [ipaddress.ip_network(cidr) for cidr in self.RESERVED_RANGES]

    def get_available_cidr(self, cloud_provider: str, region: str) -> str:
        """Allocate an available CIDR block for a new cluster"""
        used_cidrs = set()
        provider_map = {
            'aws': (AwsRegion, 'get_vpc_peering_routes'),
            'gce': (GceRegion, 'get_peering_routes')
        }

        try:
            provider = provider_map.get(cloud_provider.lower())
            if provider:
                region_instance = provider[0](region)
                used_cidrs.update(getattr(region_instance, provider[1])())
        except Exception as e:  # noqa: BLE001
            LOGGER.warning("Failed to discover used CIDRs for %s:%s: %s", cloud_provider, region, e)

        available_cidr = self._generate_next_cidr(used_cidrs)
        if not available_cidr:
            raise CidrAllocationError(
                f"No available CIDR blocks in range {self.cidr_base} for Scylla Cloud cluster "
                f"in {cloud_provider}:{region}")

        LOGGER.info("Allocated CIDR %s for %s:%s", available_cidr, cloud_provider, region)

        return str(available_cidr)

    def _generate_next_cidr(self, used_cidrs: set[str]) -> Optional[ipaddress.IPv4Network]:
        """Generate a random available CIDR from the base range"""
        used_networks = {ipaddress.ip_network(cidr) for cidr in used_cidrs}
        used_networks.update(self.reserved_ranges)

        try:
            subnets = list(self.cidr_base.subnets(new_prefix=self.subnet_size))
        except ValueError as e:
            raise CidrAllocationError(f"Invalid subnet configuration: {e}")
        available_subnets = [subnet for subnet in subnets if subnet not in used_networks]

        return random.choice(available_subnets) if available_subnets else None
