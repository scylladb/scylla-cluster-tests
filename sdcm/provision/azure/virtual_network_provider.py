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
from azure.mgmt.network.models import VirtualNetwork

from sdcm.utils.azure_utils import AzureService

LOGGER = logging.getLogger(__name__)


@dataclass
class VirtualNetworkProvider:
    _resource_group_name: str
    _region: str
    _azure_service: AzureService = AzureService()
    _cache: Dict[str, VirtualNetwork] = field(default_factory=dict)

    def __post_init__(self):
        """Discover existing virtual networks for resource group."""
        try:
            vnets = self._azure_service.network.virtual_networks.list(self._resource_group_name)
            for vnet in vnets:
                vnet = self._azure_service.network.virtual_networks.get(self._resource_group_name, vnet.name)
                self._cache[vnet.name] = vnet
        except ResourceNotFoundError:
            pass

    def get_or_create(self, name: str = "default") -> VirtualNetwork:
        if name in self._cache:
            return self._cache[name]
        LOGGER.info("Creating vnet in resource group {rg}...".format(rg=self._resource_group_name))
        self._azure_service.network.virtual_networks.begin_create_or_update(
            resource_group_name=self._resource_group_name,
            virtual_network_name=name,
            parameters={
                "location": self._region,
                "address_space": {
                    "address_prefixes": ["10.0.0.0/16"],
                }
            }
        ).wait()
        vnet = self._azure_service.network.virtual_networks.get(self._resource_group_name, name)
        LOGGER.info("Provisioned vnet {name} in the {resource} resource group".format(
            name=vnet.name, resource=self._resource_group_name))
        self._cache[vnet.name] = vnet
        return vnet

    def clear_cache(self):
        self._cache = {}
