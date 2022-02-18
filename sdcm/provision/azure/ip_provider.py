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

from typing import Dict

from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.network.models import PublicIPAddress

from sdcm.utils.azure_utils import AzureService

LOGGER = logging.getLogger(__name__)


@dataclass
class IpAddressProvider:
    _resource_group_name: str
    _region: str
    _azure_service: AzureService = AzureService()
    _cache: Dict[str, PublicIPAddress] = field(default_factory=dict)

    def __post_init__(self):
        """Discover existing ip addresses for resource group."""
        try:
            ips = self._azure_service.network.public_ip_addresses.list(self._resource_group_name)
            for ip in ips:
                ip = self._azure_service.network.public_ip_addresses.get(self._resource_group_name, ip.name)
                self._cache[ip.name] = ip
        except ResourceNotFoundError:
            pass

    def get_or_create(self, name: str = "default", version: str = "IPV4") -> PublicIPAddress:
        ip_name = self._get_ip_name(name, version)
        if ip_name in self._cache:
            return self._cache[ip_name]
        LOGGER.info("Creating public_ip in resource group {rg}...".format(rg=self._resource_group_name))
        self._azure_service.network.public_ip_addresses.begin_create_or_update(
            resource_group_name=self._resource_group_name,
            public_ip_address_name=ip_name,
            parameters={
                "location": self._region,
                "sku": {
                    "name": "Standard",
                },
                "public_ip_allocation_method": "Static",
                "public_ip_address_version": version.upper(),
            },
        ).wait()
        # need to get it separately as seems not always it gets created even if result() returns proper ip_address.
        public_ip_address = self._azure_service.network.public_ip_addresses.get(self._resource_group_name, ip_name)
        LOGGER.info("Provisioned public ip {name} ({address}) in the {resource} resource group".format(
            name=public_ip_address.name, resource=self._resource_group_name, address=public_ip_address.ip_address))
        self._cache[ip_name] = public_ip_address
        return public_ip_address

    def get(self, name: str = "default", version: str = "IPV4"):
        ip_name = self._get_ip_name(name, version)
        return self._cache[ip_name]

    def delete(self, ip_address: PublicIPAddress):
        # just remove from cache as it should be deleted along with network interface
        del self._cache[ip_address.name]

    def clear_cache(self):
        self._cache = {}

    @staticmethod
    def _get_ip_name(name: str, version: str):
        return f"{name}-{version.lower()}"
