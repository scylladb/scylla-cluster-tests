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
from azure.mgmt.network.models import Subnet

from sdcm.utils.azure_utils import AzureService

LOGGER = logging.getLogger(__name__)


@dataclass
class SubnetProvider:
    _resource_group_name: str
    _azure_service: AzureService = AzureService()
    _cache: Dict[str, Subnet] = field(default_factory=dict)

    def __post_init__(self):
        """Discover existing subnets for resource group."""
        try:
            vnets = self._azure_service.network.virtual_networks.list(self._resource_group_name)
            for vnet in vnets:
                subnets = self._azure_service.network.subnets.list(self._resource_group_name, vnet.name)
                for subnet in subnets:
                    self._cache[subnet.name] = subnet
        except ResourceNotFoundError:
            pass

    def get_or_create(self, vnet_name: str, network_sec_group_id: str, subnet_name: str = "default") -> Subnet:
        cache_name = f"{vnet_name}-{subnet_name}"
        if cache_name in self._cache:
            return self._cache[cache_name]
        LOGGER.info("Creating subnet in resource group {rg}...".format(rg=self._resource_group_name))
        subnet = self._azure_service.network.subnets.begin_create_or_update(
            resource_group_name=self._resource_group_name,
            virtual_network_name=vnet_name,
            subnet_name=subnet_name,
            subnet_parameters={
                "address_prefix": "10.0.0.0/24",
                "network_security_group": {
                    "id": network_sec_group_id,
                },
            },
        ).result()
        LOGGER.info("Provisioned subnet {name} for the {vnet_name} vnet".format(name=subnet.name, vnet_name=vnet_name))
        self._cache[cache_name] = subnet
        return subnet

    def clear_cache(self):
        self._cache = {}
