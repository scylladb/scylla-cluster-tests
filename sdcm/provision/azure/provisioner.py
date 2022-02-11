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

import os
import logging
from dataclasses import dataclass, field, fields
from typing import Dict, Any, Optional, List
import binascii

from azure.mgmt.compute.models import VirtualMachine, VirtualMachinePriorityTypes
from azure.mgmt.network.models import (NetworkSecurityGroup, VirtualNetwork, Subnet, PublicIPAddress, NetworkInterface)
from azure.mgmt.resource.resources.models import ResourceGroup
from sdcm.provision.azure.network_security_group_rules import open_ports_rules
from sdcm.provision.provisioner import Provisioner, InstanceDefinition, VmInstance, PricingModel
from sdcm.utils.azure_utils import AzureService

LOGGER = logging.getLogger(__name__)


@dataclass
class AzureProvisioner(Provisioner):  # pylint: disable=too-many-instance-attributes
    """Provides api for VM provisioning in Azure cloud, tuned for Scylla QA. """
    test_id: str
    _azure_service: AzureService = AzureService()
    _resource_groups_cache: Dict[str, ResourceGroup] = field(default_factory=dict)
    _network_sec_groups_cache: Dict[str, NetworkSecurityGroup] = field(default_factory=dict)
    _vnet_cache: Dict[str, VirtualNetwork] = field(default_factory=dict)
    _subnet_cache: Dict[str, Subnet] = field(default_factory=dict)
    _ip_cache: Dict[str, PublicIPAddress] = field(default_factory=dict)
    _nic_cache: Dict[str, NetworkInterface] = field(default_factory=dict)
    _vm_cache: Dict[str, VirtualMachine] = field(default_factory=dict)
    _instances_cache: Dict[str, VmInstance] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """'Reattaches' to resource group for given test_id by discovery of existing resources and populating cache."""
        rg_names = [rg.name for rg in list(self._azure_service.resource.resource_groups.list()) if
                    rg.name.startswith(f"sct-{self.test_id}-")]
        for resource_group_name in rg_names:
            LOGGER.info("getting resources for {}...".format(resource_group_name))
            resource_list = list(self._azure_service.resource.resources.list_by_resource_group(resource_group_name))
            resource_group = self._azure_service.resource.resource_groups.get(resource_group_name)
            self._resource_groups_cache[resource_group.name] = resource_group
            for resource in resource_list:
                match resource.type:
                    case "Microsoft.Network/networkSecurityGroups":
                        sec_group = self._azure_service.network.network_security_groups.get(resource_group_name,
                                                                                            resource.name)
                        self._network_sec_groups_cache[sec_group.name] = sec_group
                    case "Microsoft.Network/virtualNetworks":
                        v_net = self._azure_service.network.virtual_networks.get(resource_group_name, resource.name)
                        self._vnet_cache[v_net.name] = v_net
                        subnets = list(self._azure_service.network.subnets.list(resource_group_name, v_net.name))
                        for subnet in subnets:
                            self._subnet_cache[subnet.name] = subnet
                    case "Microsoft.Network/networkInterfaces":
                        nic = self._azure_service.network.network_interfaces.get(resource_group_name, resource.name)
                        self._nic_cache[nic.name] = nic
                    case "Microsoft.Network/publicIPAddresses":
                        ip_address = self._azure_service.network.public_ip_addresses.get(resource_group_name,
                                                                                         resource.name)
                        self._ip_cache[ip_address.name] = ip_address
                    case "Microsoft.Compute/virtualMachines":
                        v_m = self._azure_service.compute.virtual_machines.get(resource_group_name, resource.name)
                        self._vm_cache[v_m.name] = v_m
            for v_m in self._vm_cache.values():
                self._instances_cache[v_m.name] = self._vm_to_instance(v_m)

    def create_virtual_machine(self, region: str, definition: InstanceDefinition,
                               pricing_model: PricingModel = PricingModel.SPOT) -> VmInstance:
        """Create virtual machine in provided region, specified by InstanceDefinition"""
        if definition.name in self._instances_cache:
            return self._instances_cache[definition.name]
        resource_group_name = self._resource_group(region).name
        nic_id = self._network_interface(region, definition.name).id
        LOGGER.info(
            "Creating '{name}' VM in resource group {rg}...".format(name=definition.name, rg=resource_group_name))
        LOGGER.info(
            "Instance params: {definition}".format(definition=definition))
        params = {
            "location": region,
            "tags": definition.tags,
            "hardware_profile": {
                "vm_size": definition.type,
            },
            "network_profile": {
                "network_interfaces": [{
                    "id": nic_id,
                }],
            },
            "os_profile": {
                "computer_name": definition.name,
                "admin_username": definition.user_name,
                "admin_password": binascii.hexlify(os.urandom(20)).decode(),
                "linux_configuration": {
                    "disable_password_authentication": True,
                    "ssh": {
                        "public_keys": [{
                            "path": f"/home/{definition.user_name}/.ssh/authorized_keys",
                            "key_data": definition.ssh_public_key,
                        }],
                    },
                },
            }
        }
        storage_profile = self._get_scylla_storage_profile(image_id=definition.image_id, name=definition.name)
        params.update(storage_profile)
        params.update(self._get_pricing_params(pricing_model))
        v_m = self._azure_service.compute.virtual_machines.begin_create_or_update(
            resource_group_name=resource_group_name,
            vm_name=definition.name,
            parameters=params).result()
        LOGGER.info("Provisioned VM {name} in the {resource} resource group".format(
            name=v_m.name, resource=resource_group_name))
        self._vm_cache[v_m.name] = v_m
        instance = self._vm_to_instance(v_m)
        self._instances_cache[instance.name] = instance
        return instance

    def list_virtual_machines(self, region: Optional[str] = None) -> List[VmInstance]:
        """List virtual machines for given region. Filter by region."""
        machines = list(self._instances_cache.values())
        if region is not None:
            machines = [machine for machine in machines if machine.region == region]
        return machines

    def cleanup(self, wait: bool = False) -> None:
        """Triggers delete of all resources."""
        tasks = []
        LOGGER.info("Initiating cleanup of all resources...")
        for resource_group_name in self._resource_groups_cache:
            tasks.append(self._azure_service.resource.resource_groups.begin_delete(resource_group_name))
        LOGGER.info("Initiated cleanup of all resources")
        for _field in fields(self):  # clear cache
            if _field.name != "test_id":
                setattr(self, _field.name, {})
        if wait is True:
            LOGGER.info("Waiting for completion of all resources cleanup")
            for task in tasks:
                task.wait()

    def _vm_to_instance(self, v_m: VirtualMachine) -> VmInstance:
        nic = self._network_interface(v_m.location, v_m.name)
        pub_address = self._public_ip_address(v_m.location, v_m.name).ip_address
        priv_address = nic.ip_configurations[0].private_ip_address
        tags = v_m.tags.copy()
        admin = v_m.os_profile.admin_username
        image = str(v_m.storage_profile.image_reference)
        if v_m.priority is VirtualMachinePriorityTypes.REGULAR:
            pricing_model = PricingModel.ON_DEMAND
        else:
            pricing_model = PricingModel.SPOT

        return VmInstance(name=v_m.name, region=v_m.location, user_name=admin, public_ip_address=pub_address,
                          private_ip_address=priv_address, tags=tags, pricing_model=pricing_model,
                          image=image)

    def _resource_group(self, region: str) -> ResourceGroup:
        group_name = f"sct-{self.test_id}-{region.lower()}"
        if group_name in self._resource_groups_cache:
            return self._resource_groups_cache[group_name]
        LOGGER.info("Creating SCT resource group in region {region}...".format(region=region))
        resource_group = self._azure_service.resource.resource_groups.create_or_update(
            resource_group_name=group_name,
            parameters={
                "location": region
            },
        )
        LOGGER.info("Provisioned resource group {name} in the {region} region".format(
            name=resource_group.name, region=resource_group.location))
        self._resource_groups_cache[group_name] = resource_group
        return resource_group

    def _network_security_group(self, region: str) -> NetworkSecurityGroup:
        group_name = "default"
        if group_name in self._network_sec_groups_cache:
            return self._network_sec_groups_cache[group_name]
        resource_group_name = self._resource_group(region).name
        LOGGER.info("Creating SCT network security group in resource group {rg}...".format(rg=resource_group_name))
        network_sec_group = self._azure_service.network.network_security_groups.begin_create_or_update(
            resource_group_name=resource_group_name,
            network_security_group_name=group_name,
            parameters={
                "location": region,
                "security_rules": open_ports_rules,
            },
        ).result()
        LOGGER.info("Provisioned security group {name} in the {resource} resource group".format(
            name=network_sec_group.name, resource=resource_group_name))
        self._network_sec_groups_cache[group_name] = network_sec_group
        return network_sec_group

    def _virtual_network(self, region: str) -> VirtualNetwork:
        vnet_name = "default"
        if vnet_name in self._vnet_cache:
            return self._vnet_cache[vnet_name]
        resource_group_name = self._resource_group(region).name
        LOGGER.info("Creating vnet in resource group {rg}...".format(rg=resource_group_name))
        vnet = self._azure_service.network.virtual_networks.begin_create_or_update(
            resource_group_name=resource_group_name,
            virtual_network_name=vnet_name,
            parameters={
                "location": region,
                "address_space": {
                    "address_prefixes": ["10.0.0.0/16"],
                }
            }
        ).result()
        LOGGER.info("Provisioned vnet {name} in the {resource} resource group".format(
            name=vnet.name, resource=resource_group_name))
        self._vnet_cache[vnet_name] = vnet
        return vnet

    def _subnet(self, region: str) -> Subnet:
        subnet_name = "default"
        if subnet_name in self._subnet_cache:
            return self._subnet_cache[subnet_name]
        resource_group_name = self._resource_group(region).name
        vnet_name = self._virtual_network(region).name
        network_sec_group_id = self._network_security_group(region).id
        LOGGER.info("Creating subnet in resource group {rg}...".format(rg=resource_group_name))
        subnet = self._azure_service.network.subnets.begin_create_or_update(
            resource_group_name=resource_group_name,
            virtual_network_name=vnet_name,
            subnet_name=subnet_name,
            subnet_parameters={
                "address_prefix": "10.0.0.0/24",
                "network_security_group": {
                    "id": network_sec_group_id,
                },
            },
        ).result()
        LOGGER.info("Provisioned subnet {name} in the {resource} resource group".format(
            name=subnet.name, resource=resource_group_name))
        self._subnet_cache[subnet_name] = subnet
        return subnet

    def _public_ip_address(self, region: str, name: str) -> PublicIPAddress:
        version = "IPV4"
        ip_name = f"{region.lower()}-{name}-{version.lower()}"
        if ip_name in self._ip_cache:
            return self._ip_cache[ip_name]
        resource_group_name = self._resource_group(region).name
        LOGGER.info("Creating public_ip in resource group {rg}...".format(rg=resource_group_name))
        public_ip_address = self._azure_service.network.public_ip_addresses.begin_create_or_update(
            resource_group_name=resource_group_name,
            public_ip_address_name=ip_name,
            parameters={
                "location": region,
                "sku": {
                    "name": "Standard",
                },
                "public_ip_allocation_method": "Static",
                "public_ip_address_version": version.upper(),
            },
        ).result()
        LOGGER.info("Provisioned public ip {name} ({address}) in the {resource} resource group".format(
            name=public_ip_address.name, resource=resource_group_name, address=public_ip_address.ip_address))
        self._ip_cache[ip_name] = public_ip_address
        return public_ip_address

    def _network_interface(self, region: str, name: str) -> NetworkInterface:
        nic_name = f"{region.lower()}-{name}-nic"
        if nic_name in self._nic_cache:
            return self._nic_cache[nic_name]
        subnet_id = self._subnet(region).id
        resource_group_name = self._resource_group(region).name
        parameters = {
            "location": region,
            "ip_configurations": [{
                "name": nic_name,
                "subnet": {
                    "id": subnet_id,
                },
            }],
            "enable_accelerated_networking": True,
        }
        ip_address = self._public_ip_address(region, name)
        parameters["ip_configurations"][0]["public_ip_address"] = {
            "id": ip_address.id
        }
        LOGGER.info("Creating nic in resource group {rg}...".format(rg=resource_group_name))
        nic = self._azure_service.network.network_interfaces.begin_create_or_update(
            resource_group_name=resource_group_name,
            network_interface_name=nic_name,
            parameters=parameters,
        ).result()
        LOGGER.info("Provisioned nic {name} in the {resource} resource group".format(
            name=nic.name, resource=resource_group_name))
        self._nic_cache[nic_name] = nic
        return nic

    @staticmethod
    def _get_scylla_storage_profile(image_id: str, name: str, disk_size: str = None) -> Dict[str, Any]:
        """Creates storage profile based on image_id. image_id may refer to scylla-crafted images
         (starting with '/subscription') or to 'Urn' of image (see output of e.g. `az vm image list --output table`)"""
        storage_profile = {"storage_profile": {
            "os_disk": {
                           "name": f"{name}-os-disk",
                           "os_type": "linux",
                           "caching": "ReadWrite",
                           "create_option": "FromImage",
                           "managed_disk": {
                               "storage_account_type": "Premium_LRS",  # SSD
                           },
                           } | ({} if disk_size is None else {"disk_size_gb": disk_size}),
        }}
        if image_id.startswith("/subscriptions/"):
            storage_profile.update({
                "storage_profile": {
                    "image_reference": {"id": image_id},
                }
            })
        else:
            image_reference_values = image_id.split(":")
            storage_profile.update({
                "storage_profile": {
                    "image_reference": {
                        "publisher": image_reference_values[0],
                        "offer": image_reference_values[1],
                        "sku": image_reference_values[2],
                        "version": image_reference_values[3],
                    },
                }
            })
        return storage_profile

    @staticmethod
    def _get_pricing_params(pricing_model: PricingModel):
        if pricing_model != PricingModel.ON_DEMAND:
            return {
                "priority": "Spot",  # possible values are "Regular", "Low", or "Spot"
                "eviction_policy": "Delete",  # can be "Deallocate" or "Delete", Deallocate leaves disks intact
                "billing_profile": {
                    "max_price": -1,  # -1 indicates the VM shouldn't be evicted for price reasons
                }
            }
        else:
            return {}
