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
from azure.mgmt.network.models import NetworkInterface

from sdcm.utils.azure_utils import AzureService

LOGGER = logging.getLogger(__name__)


@dataclass
class NetworkInterfaceProvider:
    _resource_group_name: str
    _region: str
    _azure_service: AzureService = AzureService()
    _cache: Dict[str, NetworkInterface] = field(default_factory=dict)

    def __post_init__(self):
        """Discover existing network interfaces for resource group."""
        try:
            nics = self._azure_service.network.network_interfaces.list(self._resource_group_name)
            for nic in nics:
                nic = self._azure_service.network.network_interfaces.get(self._resource_group_name, nic.name)
                self._cache[nic.name] = nic
        except ResourceNotFoundError:
            pass

    def get(self, name: str) -> NetworkInterface:
        return self._cache[self.get_nic_name(name)]

    def get_or_create(self, subnet_id: str, ip_addresses_ids: List[str], names: List[str]) -> List[NetworkInterface]:
        """Creates or gets (if already exists) network interface"""
        nics = []
        pollers = []
        for name, address in zip(names, ip_addresses_ids):
            nic_name = self.get_nic_name(name)
            if nic_name in self._cache:
                nics.append(self._cache[nic_name])
                continue
            parameters = {
                "location": self._region,
                "ip_configurations": [{
                    "name": nic_name,
                    "subnet": {
                        "id": subnet_id,
                    },
                }],
                "enable_accelerated_networking": True,
            }
            parameters["ip_configurations"][0]["public_ip_address"] = {
                "id": address,
                "properties": {"deleteOption": "Delete"}
            }
            LOGGER.info("Creating nic in resource group %s...", self._resource_group_name)
            poller = self._azure_service.network.network_interfaces.begin_create_or_update(
                resource_group_name=self._resource_group_name,
                network_interface_name=nic_name,
                parameters=parameters,
            )
            pollers.append((nic_name, poller))
        for nic_name, poller in pollers:
            poller.wait()
            nic = self._azure_service.network.network_interfaces.get(self._resource_group_name, nic_name)
            LOGGER.info("Provisioned nic %s in the %s resource group", nic.name, self._resource_group_name)
            self._cache[nic_name] = nic
            nics.append(nic)
        return nics

    def delete(self, nic: NetworkInterface):
        # just remove from cache as it should be deleted along with network interface
        del self._cache[nic.name]

    def clear_cache(self):
        self._cache = {}

    @staticmethod
    def get_nic_name(name: str):
        return f"{name}-nic"
