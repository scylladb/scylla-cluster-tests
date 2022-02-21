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

from typing import Dict, Iterable, Union, List

from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.network.models import NetworkSecurityGroup

from sdcm.utils.azure_utils import AzureService

LOGGER = logging.getLogger(__name__)


def rules_to_payload(rules: Iterable) -> List[Dict[str, Union[str, int]]]:
    """convert iterable rules to format accepted by provisioner"""
    template = {
        "name": "",
        "protocol": "TCP",
        "source_port_range": "*",
        "destination_port_range": "",
        "source_address_prefix": "*",
        "destination_address_prefix": "*",
        "access": "Allow",
        "priority": 300,
        "direction": "Inbound",
    }

    open_ports_rules = []
    for idx, port in enumerate(rules):
        templ = template.copy()
        templ["name"] = port.name
        templ["destination_port_range"] = str(port.value)
        templ["priority"] = templ["priority"] + idx
        open_ports_rules.append(templ)
    return open_ports_rules


@dataclass
class NetworkSecurityGroupProvider:
    _resource_group_name: str
    _region: str
    _azure_service: AzureService = AzureService()
    _cache: Dict[str, NetworkSecurityGroup] = field(default_factory=dict)

    def __post_init__(self):
        """Discover existing security groups for resource group."""
        try:
            groups = self._azure_service.network.network_security_groups.list(self._resource_group_name)
            for group in groups:
                sec_group = self._azure_service.network.network_security_groups.get(self._resource_group_name,
                                                                                    group.name)
                self._cache[group.name] = sec_group
        except ResourceNotFoundError:
            pass

    def get_or_create(self, security_rules: Iterable, name="default") -> NetworkSecurityGroup:
        """Creates or gets (if already exists) security group"""
        if name in self._cache:
            return self._cache[name]
        open_ports_rules = rules_to_payload(security_rules)
        LOGGER.info(
            "Creating SCT network security group in resource group {rg}...".format(rg=self._resource_group_name))
        self._azure_service.network.network_security_groups.begin_create_or_update(
            resource_group_name=self._resource_group_name,
            network_security_group_name=name,
            parameters={
                "location": self._region,
                "security_rules": open_ports_rules,
            },
        ).wait()
        network_sec_group = self._azure_service.network.network_security_groups.get(self._resource_group_name, name)
        LOGGER.info("Provisioned security group {name} in the {resource} resource group".format(
            name=network_sec_group.name, resource=self._resource_group_name))
        self._cache[name] = network_sec_group
        return network_sec_group

    def clear_cache(self):
        self._cache = {}
