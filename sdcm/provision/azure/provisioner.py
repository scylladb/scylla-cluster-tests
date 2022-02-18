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
from dataclasses import dataclass, field, fields
from typing import Dict, List
from azure.mgmt.compute.models import VirtualMachine, VirtualMachinePriorityTypes

from sdcm.provision.azure.ip_provider import IpAddressProvider
from sdcm.provision.azure.network_interface_provider import NetworkInterfaceProvider
from sdcm.provision.azure.network_security_group_provider import NetworkSecurityGroupProvider
from sdcm.provision.azure.resource_group_provider import ResourceGroupProvider
from sdcm.provision.azure.subnet_provider import SubnetProvider
from sdcm.provision.azure.virtual_machine_provider import VirtualMachineProvider
from sdcm.provision.azure.virtual_network_provider import VirtualNetworkProvider
from sdcm.provision.provisioner import Provisioner, InstanceDefinition, VmInstance, PricingModel
from sdcm.provision.security import ScyllaOpenPorts
from sdcm.utils.azure_utils import AzureService

LOGGER = logging.getLogger(__name__)


@dataclass
class AzureProvisioner(Provisioner):  # pylint: disable=too-many-instance-attributes
    """Provides api for VM provisioning in Azure cloud, tuned for Scylla QA. """
    _test_id: str
    _region: str
    _azure_service: AzureService = AzureService()
    _rg_provider: ResourceGroupProvider = field(init=False)
    _network_sec_group_provider: NetworkSecurityGroupProvider = field(init=False)
    _vnet_provider: VirtualNetworkProvider = field(init=False)
    _subnet_provider: SubnetProvider = field(init=False)
    _ip_provider: IpAddressProvider = field(init=False)
    _nic_provider: NetworkInterfaceProvider = field(init=False)
    _vm_provider: VirtualMachineProvider = field(init=False)
    _cache: Dict[str, VmInstance] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """'Reattaches' to resource group for given test_id by discovery of existing resources and populating cache."""
        LOGGER.info("getting resources for {}...".format(self._resource_group_name))
        self._rg_provider = ResourceGroupProvider(self._resource_group_name, self._region)
        self._network_sec_group_provider = NetworkSecurityGroupProvider(self._resource_group_name, self._region)
        self._vnet_provider = VirtualNetworkProvider(self._resource_group_name, self._region)
        self._subnet_provider = SubnetProvider(self._resource_group_name)
        self._ip_provider = IpAddressProvider(self._resource_group_name, self._region)
        self._nic_provider = NetworkInterfaceProvider(self._resource_group_name, self._region)
        self._vm_provider = VirtualMachineProvider(self._resource_group_name, self._region)
        for v_m in self._vm_provider.list():
            self._cache[v_m.name] = self._vm_to_instance(v_m)

    def create_virtual_machine(self, definition: InstanceDefinition,
                               pricing_model: PricingModel = PricingModel.SPOT) -> VmInstance:
        """Create virtual machine in provided region, specified by InstanceDefinition"""
        if definition.name in self._cache:
            return self._cache[definition.name]
        self._rg_provider.get_or_create()
        sec_group_id = self._network_sec_group_provider.get_or_create(security_rules=ScyllaOpenPorts).id
        vnet_name = self._vnet_provider.get_or_create().name
        subnet_id = self._subnet_provider.get_or_create(vnet_name, sec_group_id).id
        ip_address_id = self._ip_provider.get_or_create(definition.name).id
        nic_id = self._nic_provider.get_or_create(subnet_id, ip_address_id, name=definition.name).id
        v_m = self._vm_provider.get_or_create(definition, nic_id, pricing_model)
        instance = self._vm_to_instance(v_m)
        self._cache[definition.name] = instance
        return instance

    def terminate_virtual_machine(self, name: str, wait: bool = True) -> None:
        """Terminates virtual machine, cleaning attached ip address and network interface."""
        instance = self._cache.get(name)
        if not instance:
            LOGGER.warning("Instance {name} does not exist. Shouldn't have called it".format(name=name))
            return
        self._vm_provider.delete(name, wait=wait)
        del self._cache[name]
        self._nic_provider.delete(self._nic_provider.get(name))
        self._ip_provider.delete(self._ip_provider.get(name))

    def list_virtual_machines(self) -> List[VmInstance]:
        """List virtual machines for given provisioner."""
        return list(self._cache.values())

    def cleanup(self, wait: bool = False) -> None:
        """Triggers delete of all resources."""
        tasks = []
        self._rg_provider.delete(wait)

        for _field in fields(self):  # clear cache
            if _field.name not in ("_test_id", "_region"):
                setattr(self, _field.name, {})
        if wait is True:
            LOGGER.info("Waiting for completion of all resources cleanup")
            for task in tasks:
                task.wait()

    @property
    def _resource_group_name(self):
        return f"SCT-{self._test_id}-{self._region}"

    def _vm_to_instance(self, v_m: VirtualMachine) -> VmInstance:
        pub_address = self._ip_provider.get(v_m.name).ip_address
        nic = self._nic_provider.get(v_m.name)
        priv_address = nic.ip_configurations[0].private_ip_address
        tags = v_m.tags.copy()
        try:
            admin = v_m.os_profile.admin_username
        except AttributeError:
            # specialized machines don't provide usernames
            # todo lukasz: find a way to get admin name from image (is it possible??)
            admin = ""
        image = str(v_m.storage_profile.image_reference)
        if v_m.priority is VirtualMachinePriorityTypes.REGULAR:
            pricing_model = PricingModel.ON_DEMAND
        else:
            pricing_model = PricingModel.SPOT

        return VmInstance(name=v_m.name, region=v_m.location, user_name=admin, public_ip_address=pub_address,
                          private_ip_address=priv_address, tags=tags, pricing_model=pricing_model,
                          image=image, provisioner=self)
