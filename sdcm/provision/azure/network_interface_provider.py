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
import time
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
                nic = self._azure_service.network.network_interfaces.get(self._resource_group_name, nic.name)  # noqa: PLW2901
                self._cache[nic.name] = nic
        except ResourceNotFoundError:
            pass

    def get(self, name: str, index: int = 0) -> NetworkInterface:
        return self._cache[self.get_nic_name(name, index)]

    def get_all(self, name: str) -> List[NetworkInterface]:
        """Every NIC of a VM, ordered by device index.

        Discovered from the cached resource group listing rather than from the test configuration,
        so teardown works for a provisioner that was created without one (e.g. discover_regions()).
        """
        prefix = self.get_nic_name(name)
        nics = [
            (self.get_nic_index(nic_name, name), nic)
            for nic_name, nic in self._cache.items()
            if nic_name.startswith(prefix)
        ]
        return [nic for _, nic in sorted(nics, key=lambda item: item[0])]

    def get_or_create(self, plans: Dict[str, List[Dict]]) -> Dict[str, List[NetworkInterface]]:
        """Creates or gets (if already exists) the network interfaces of every given VM.

        'plans' maps a VM name to its interfaces in device-index order, each an entry of
        {"subnet_id": ..., "address_id": <public IP id, or None for a NIC without one>}.

        Azure can only attach a NIC to a deallocated VM, so every NIC a VM will ever have is
        created here, before the VM itself.
        """
        pollers = []
        for name, plan in plans.items():
            for index, entry in enumerate(plan):
                nic_name = self.get_nic_name(name, index)
                if nic_name in self._cache:
                    continue
                parameters = {
                    "location": self._region,
                    "ip_configurations": [
                        {
                            "name": nic_name,
                            "subnet": {
                                "id": entry["subnet_id"],
                            },
                            "primary": True,
                        }
                    ],
                    "enable_accelerated_networking": True,
                }
                if entry["address_id"] is not None:
                    parameters["ip_configurations"][0]["public_ip_address"] = {
                        "id": entry["address_id"],
                        "properties": {"deleteOption": "Delete"},
                    }
                LOGGER.info("Creating nic %s in resource group %s...", nic_name, self._resource_group_name)
                poller = self._azure_service.network.network_interfaces.begin_create_or_update(
                    resource_group_name=self._resource_group_name,
                    network_interface_name=nic_name,
                    parameters=parameters,
                )
                pollers.append((nic_name, poller))
        for nic_name, poller in pollers:
            nic = poller.result()
            LOGGER.info("Provisioned nic %s in the %s resource group", nic.name, self._resource_group_name)
            self._cache[nic_name] = nic
        if pollers:
            time.sleep(5)  # wait for nic to be fully propagated before returning (SCT-373)
        return {name: self.get_all(name) for name in plans}

    def delete(self, nic: NetworkInterface):
        # just remove from cache as it should be deleted along with network interface
        del self._cache[nic.name]

    def clear_cache(self):
        self._cache = {}

    @staticmethod
    def get_nic_name(name: str, index: int = 0):
        """Name of the NIC of a device index.

        The primary NIC keeps the historical '<vm>-nic' name, so existing single-NIC runs and the
        cleanup tooling matching on it are unaffected.
        """
        if index:
            return f"{name}-nic{index}"
        return f"{name}-nic"

    @staticmethod
    def get_nic_index(nic_name: str, vm_name: str) -> int:
        """Device index encoded in a NIC name, 0 for the primary one."""
        suffix = nic_name[len(f"{vm_name}-nic") :]
        return int(suffix) if suffix.isdigit() else 0
