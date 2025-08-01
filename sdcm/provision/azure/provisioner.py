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
import string
from datetime import datetime, timezone
from typing import Dict, List

from azure.mgmt.compute.models import VirtualMachine, VirtualMachinePriorityTypes
from azure.mgmt.resource.resources.models import ResourceGroup
from invoke import Result

from sdcm.provision.azure.ip_provider import IpAddressProvider
from sdcm.provision.azure.network_interface_provider import NetworkInterfaceProvider
from sdcm.provision.azure.network_security_group_provider import NetworkSecurityGroupProvider
from sdcm.provision.azure.resource_group_provider import ResourceGroupProvider
from sdcm.provision.azure.subnet_provider import SubnetProvider
from sdcm.provision.azure.virtual_machine_provider import VirtualMachineProvider
from sdcm.provision.azure.virtual_network_provider import VirtualNetworkProvider
from sdcm.provision.provisioner import Provisioner, InstanceDefinition, VmInstance, PricingModel, OperationPreemptedError
from sdcm.provision.security import ScyllaOpenPorts
from sdcm.utils.azure_utils import AzureService

LOGGER = logging.getLogger(__name__)


class AzureProvisioner(Provisioner):  # pylint: disable=too-many-instance-attributes
    """Provides api for VM provisioning in Azure cloud, tuned for Scylla QA. """

    def __init__(self, test_id: str, region: str, availability_zone: str,
                 azure_service: AzureService = AzureService(), **_):
        availability_zone = self._convert_az_to_zone(availability_zone)
        super().__init__(test_id, region, availability_zone)
        self._azure_service: AzureService = azure_service
        self._cache: Dict[str, VmInstance] = {}
        LOGGER.debug("getting resources for %s...", self._resource_group_name)
        self._rg_provider = ResourceGroupProvider(
            self._resource_group_name, self._region, self._az, self._azure_service)
        self._network_sec_group_provider = NetworkSecurityGroupProvider(self._resource_group_name, self._region,
                                                                        self._azure_service)
        self._vnet_provider = VirtualNetworkProvider(
            self._resource_group_name, self._region, self._az, self._azure_service)
        self._subnet_provider = SubnetProvider(self._resource_group_name, self._azure_service)
        self._ip_provider = IpAddressProvider(self._resource_group_name, self._region, self._az, self._azure_service)
        self._nic_provider = NetworkInterfaceProvider(self._resource_group_name, self._region, self._azure_service)
        self._vm_provider = VirtualMachineProvider(
            self._resource_group_name, self._region, self._az, self._azure_service)
        for v_m in self._vm_provider.list():
            try:
                self._cache[v_m.name] = self._vm_to_instance(v_m)
            except KeyError as exc:
                LOGGER.warning("Failed to cache %s instance. Probably is pending deletion. Error: %s", v_m.name, exc)

    @staticmethod
    def _convert_az_to_zone(availability_zone: str) -> str:
        """Azure uses numbers for availiability zones, while in tests we use letters.
        In case user provides letter instead of zone number, convert it to number.

        E.g.:
        a -> 1, b -> 2
        """
        if availability_zone and availability_zone in (chars := string.ascii_lowercase):
            return str(chars.find(availability_zone) + 1)
        return availability_zone

    @staticmethod
    def _get_az_from_name(resource_group: ResourceGroup) -> str:
        """Gets availability zone from name or return empty string if no az was used."""
        # this method should be removed when all supported branches support az
        if resource_group.name[-2] == "-":
            return resource_group.name[-1]
        else:
            return ""

    @classmethod
    def discover_regions(cls, test_id: str = "", regions: list = None,
                         azure_service: AzureService = AzureService(), **kwargs) -> List["AzureProvisioner"]:
        # pylint: disable=arguments-differ,unused-argument
        """Discovers provisioners for in each region for given test id.

        If test_id is not provided, it discovers all related to SCT provisioners."""

        all_resource_groups: List[ResourceGroup] = [
            rg
            for rg in azure_service.resource.resource_groups.list()
            if rg.name.startswith("SCT-") and (rg.location in regions if regions else True)
        ]
        if test_id:
            provisioner_params = [(test_id, rg.location, cls._get_az_from_name(rg), azure_service)
                                  for rg in all_resource_groups if test_id in rg.name]
        else:
            # extract test_id from rg names where rg.name format is: SCT-<test_id>-<region>-<az>
            provisioner_params = [(test_id, rg.location, cls._get_az_from_name(rg), azure_service) for rg in all_resource_groups
                                  if (test_id := rg.name.split("SCT-")[-1][:36]) and len(test_id) == 36]
        return [cls(*params) for params in provisioner_params]

    def get_or_create_instance(self, definition: InstanceDefinition,
                               pricing_model: PricingModel = PricingModel.SPOT) -> VmInstance:
        """Create virtual machine in provided region, specified by InstanceDefinition.

        Set definition.user_data to empty string when using specialized image."""
        return self.get_or_create_instances(definitions=[definition], pricing_model=pricing_model)[0]

    def get_or_create_instances(self,
                                definitions: List[InstanceDefinition],
                                pricing_model: PricingModel = PricingModel.SPOT
                                ) -> List[VmInstance]:
        """Create a set of instances specified by a list of InstanceDefinition.
        If instances already exist, returns them."""
        provisioned_vm_instances = []
        definitions_to_provision = []
        for definition in definitions:
            if definition.name in self._cache:
                provisioned_vm_instances.append(self._cache[definition.name])
            else:
                definitions_to_provision.append(definition)
        if not definitions_to_provision:
            return provisioned_vm_instances

        self._rg_provider.get_or_create()
        sec_group_id = self._network_sec_group_provider.get_or_create(security_rules=ScyllaOpenPorts).id
        vnet_name = self._vnet_provider.get_or_create().name
        subnet_id = self._subnet_provider.get_or_create(vnet_name, sec_group_id).id
        ip_addresses = self._ip_provider.get_or_create(names=[d.name for d in definitions_to_provision], version="IPV4")
        nics = self._nic_provider.get_or_create(subnet_id, ip_addresses_ids=[address.id for address in ip_addresses],
                                                names=[definition.name for definition in definitions_to_provision])
        try:
            v_ms = self._vm_provider.get_or_create(definitions=definitions_to_provision, nics_ids=[
                                                   nic.id for nic in nics], pricing_model=pricing_model)
        except OperationPreemptedError:
            # upon preemption error, need to recreate providers to rediscover resources as cache might be invalid.
            self._ip_provider = IpAddressProvider(
                self._resource_group_name, self._region, self._az, self._azure_service)
            self._nic_provider = NetworkInterfaceProvider(self._resource_group_name, self._region, self._azure_service)
            self._vm_provider = VirtualMachineProvider(
                self._resource_group_name, self._region, self._az, self._azure_service)
            raise
        for definition, v_m in zip(definitions, v_ms):
            instance = self._vm_to_instance(v_m)
            self._cache[definition.name] = instance
            provisioned_vm_instances.append(instance)
        return provisioned_vm_instances

    def terminate_instance(self, name: str, wait: bool = True) -> None:
        """Terminates virtual machine, cleaning attached ip address and network interface."""
        instance = self._cache.get(name)
        if not instance:
            LOGGER.warning("Instance %s does not exist. Shouldn't have called it", name)
            return
        self._vm_provider.delete(name, wait=wait)
        del self._cache[name]
        self._nic_provider.delete(self._nic_provider.get(name))
        self._ip_provider.delete(self._ip_provider.get(name))

    def reboot_instance(self, name: str, wait: bool, hard: bool = False) -> None:
        self._vm_provider.reboot(name, wait, hard)

    def list_instances(self) -> List[VmInstance]:
        """List virtual machines for given provisioner."""
        return list(self._cache.values())

    def cleanup(self, wait: bool = False) -> None:
        """Triggers delete of all resources."""
        tasks = []
        self._rg_provider.delete(wait)
        self._network_sec_group_provider.clear_cache()
        self._vnet_provider.clear_cache()
        self._subnet_provider.clear_cache()
        self._ip_provider.clear_cache()
        self._nic_provider.clear_cache()
        self._vm_provider.clear_cache()
        self._cache = {}
        if wait is True:
            LOGGER.info("Waiting for completion of all resources cleanup")
            for task in tasks:
                task.wait()

    def add_instance_tags(self, name: str, tags: Dict[str, str]) -> None:
        """Adds tags to instance."""
        LOGGER.info("Adding tags '%s' to intance '%s'...", tags, name)
        instance = self._vm_to_instance(self._vm_provider.add_tags(name, tags))
        self._cache[name] = instance
        LOGGER.info("Added tags '%s' to intance '%s'", tags, name)

    def run_command(self, name: str, command: str) -> Result:
        """Runs command on instance."""
        return self._vm_provider.run_command(name, command)

    @property
    def _resource_group_name(self):
        name = f"SCT-{self._test_id}-{self._region}"
        if self._az:
            name += f"-{self._az}"
        return name

    def _vm_to_instance(self, v_m: VirtualMachine) -> VmInstance:
        pub_address = self._ip_provider.get(v_m.name).ip_address
        nic = self._nic_provider.get(v_m.name)
        priv_address = nic.ip_configurations[0].private_ip_address
        tags = v_m.tags.copy()
        ssh_user = tags.pop("ssh_user", "")
        ssh_key = tags.pop("ssh_key", "")
        creation_time = datetime.fromisoformat(tags.pop("creation_time")).replace(
            tzinfo=timezone.utc) if 'creation_time' in tags else None
        image = str(v_m.storage_profile.image_reference)
        pricing_model = self._get_pricing_model(v_m)
        instance_type = v_m.hardware_profile.vm_size

        return VmInstance(name=v_m.name, region=v_m.location, user_name=ssh_user, ssh_key_name=ssh_key, public_ip_address=pub_address,
                          private_ip_address=priv_address, tags=tags, pricing_model=pricing_model,
                          image=image, creation_time=creation_time, instance_type=instance_type, _provisioner=self)

    @staticmethod
    def _get_pricing_model(v_m: VirtualMachine) -> PricingModel:
        try:
            priority = VirtualMachinePriorityTypes(v_m.priority)
        except ValueError:
            priority = VirtualMachinePriorityTypes.REGULAR
        if priority is VirtualMachinePriorityTypes.REGULAR:
            pricing_model = PricingModel.ON_DEMAND
        else:
            pricing_model = PricingModel.SPOT
        return pricing_model
