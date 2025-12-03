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
# Copyright (c) 2022 ScyllaDB

import logging
from dataclasses import dataclass, field

from typing import Dict, List

from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.network.models import PublicIPAddress

from sdcm.provision.provisioner import InstanceDefinition
from sdcm.utils.azure_utils import AzureService

LOGGER = logging.getLogger(__name__)


@dataclass
class IpAddressProvider:
    _resource_group_name: str
    _region: str
    _az: str
    _azure_service: AzureService = AzureService()
    _cache: Dict[str, PublicIPAddress] = field(default_factory=dict)

    def __post_init__(self):
        """Discover existing ip addresses for resource group."""
        try:
            ips = self._azure_service.network.public_ip_addresses.list(self._resource_group_name)
            for ip in ips:
                ip = self._azure_service.network.public_ip_addresses.get(self._resource_group_name, ip.name)  # noqa: PLW2901
                self._cache[ip.name] = ip
        except ResourceNotFoundError:
            pass

    def get_or_create(
        self, instance_definitions: List[InstanceDefinition], version: str = "IPV4"
    ) -> List[PublicIPAddress]:
        addresses = []
        pollers = []
        for definition in instance_definitions:
            ip_name = self._get_ip_name(definition.name, version)
            if ip_name in self._cache:
                addresses.append(self._cache[ip_name])
                continue
            if not definition.use_public_ip:
                address = PublicIPAddress(ip_address=None)
                address.name = ip_name
                self._cache[ip_name] = address
                addresses.append(address)
                continue
            LOGGER.info("Creating public_ip %s in resource group %s...", ip_name, self._resource_group_name)
            poller = self._azure_service.network.public_ip_addresses.begin_create_or_update(
                resource_group_name=self._resource_group_name,
                public_ip_address_name=ip_name,
                parameters={
                    "location": self._region,
                    "zones": [self._az] if self._az else [],
                    "sku": {
                        "name": "Standard",
                    },
                    "public_ip_allocation_method": "Static",
                    "public_ip_address_version": version.upper(),
                },
            )
            pollers.append((ip_name, poller))
        for ip_name, poller in pollers:
            poller.wait()
            # need to get it separately as seems not always it gets created even if result() returns proper ip_address.
            address = self._azure_service.network.public_ip_addresses.get(self._resource_group_name, ip_name)
            LOGGER.info(
                "Provisioned public ip %s (%s) in the %s resource group",
                address.name,
                address.ip_address,
                self._resource_group_name,
            )
            self._cache[ip_name] = address
            addresses.append(address)
        return addresses

    def get(self, name: str = "default", version: str = "IPV4") -> PublicIPAddress:
        ip_name = self._get_ip_name(name, version)
        try:
            return self._cache[ip_name]
        except KeyError:
            # case when we reuse cluster (or use provision step) when there is no ip address created
            address = PublicIPAddress(ip_address=None)
            address.name = ip_name
            return address

    def delete(self, ip_address: PublicIPAddress):
        # just remove from cache as it should be deleted along with network interface
        try:
            del self._cache[ip_address.name]
        except KeyError:
            # case when no public IP address was created
            pass

    def clear_cache(self):
        self._cache = {}

    @staticmethod
    def _get_ip_name(name: str, version: str):
        return f"{name}-{version.lower()}"
