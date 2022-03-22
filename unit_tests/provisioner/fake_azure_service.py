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

import json
import os
import shutil

from pathlib import Path
from typing import List, Any, Dict

from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.network.models import (NetworkSecurityGroup, Subnet, PublicIPAddress, NetworkInterface, VirtualNetwork)
from azure.mgmt.resource.resources.models import ResourceGroup
from azure.mgmt.compute.models import VirtualMachine

from sdcm.utils.azure_utils import AzureService    # pylint: disable=import-error


def snake_case_to_camel_case(string):
    temp = string.split('_')
    return temp[0] + ''.join(ele.title() for ele in temp[1:])


def dict_keys_to_camel_case(dct):
    if isinstance(dct, list):
        return [dict_keys_to_camel_case(i) if isinstance(i, (dict, list)) else i for i in dct]
    return {snake_case_to_camel_case(a): dict_keys_to_camel_case(b) if isinstance(b, (dict, list))
            else b for a, b in dct.items()}


class WaitableObject:  # pylint: disable=too-few-public-methods

    def wait(self):
        pass


class FakeResourceGroups:

    def __init__(self, path: Path):
        self.path = path

    def create_or_update(self, resource_group_name: str, parameters: Dict[str, Any]) -> ResourceGroup:
        res_group = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}",
            "name": resource_group_name,
            "type": "Microsoft.Resources/resourceGroups",
            "properties": {
                "provisioningState": "Succeeded"
            },
        }
        res_group.update(**parameters)
        (self.path / resource_group_name).mkdir(exist_ok=True)
        with open(self.path / resource_group_name / "resource_group.json", "w", encoding="utf-8") as file:
            json.dump(res_group, fp=file, indent=2)
        return ResourceGroup.deserialize(res_group)

    def get(self, name) -> ResourceGroup:
        try:
            with open(self.path / name / "resource_group.json", "r", encoding="utf-8") as file:
                return ResourceGroup.deserialize(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("Resource group not found") from None

    def begin_delete(self, name: str) -> WaitableObject:
        shutil.rmtree(self.path / name)
        return WaitableObject()


class FakeNetworkSecurityGroup:
    def __init__(self, path: Path) -> None:
        self.path = path

    def list(self, resource_group_name: str) -> List[NetworkSecurityGroup]:
        try:
            files = [file for file in os.listdir(self.path / resource_group_name) if file.startswith("nsg-")]
        except FileNotFoundError:
            raise ResourceNotFoundError("No resource group") from None
        elements = []
        for file in files:
            with open(self.path / resource_group_name / file, "r", encoding="utf-8") as file:
                elements.append(NetworkSecurityGroup.deserialize(json.load(file)))
        return elements

    def begin_create_or_update(self, resource_group_name: str, network_security_group_name: str,
                               parameters: Dict[str, Any]) -> WaitableObject:
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
                  f"/Microsoft.Network/networkSecurityGroups/default",
            "name": network_security_group_name,
            "type": "Microsoft.Network/networkSecurityGroups",
            "location": parameters["location"],
            "etag": "W/\"90369d69-7507-4e77-8b90-66e1f971f290\"",
            "properties": {
                "subnets": [
                ],
                "resourceGuid": "2100b376-ad42-449d-a9c2-62cdade9db83",
                "provisioningState": "Succeeded"
            }
        }
        rules = []
        for rule in parameters["security_rules"]:
            rules.append({
                "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}"
                      f"/providers/Microsoft.Network/networkSecurityGroups/default/securityRules/{rule['name']}",
                "name": rule["name"],
                "etag": "W/\"90369d69-7507-4e77-8b90-66e1f971f290\"",
                "type": "Microsoft.Network/networkSecurityGroups/securityRules",
                "properties": {
                    "protocol": rule["protocol"],
                    "sourcePortRange": rule["source_port_range"],
                    "destinationPortRange": rule["destination_port_range"],
                    "sourceAddressPrefix": rule["source_address_prefix"],
                    "sourceAddressPrefixes": [],
                    "destinationAddressPrefix": rule["destination_address_prefix"],
                    "destinationAddressPrefixes": [],
                    "sourcePortRanges": [],
                    "destinationPortRanges": [],
                    "access": rule["access"],
                    "priority": rule["priority"],
                    "direction": rule["direction"],
                    "provisioningState": "Succeeded"
                }
            })
        base.update({"securityRules": rules})
        with open(self.path / resource_group_name / f"nsg-{network_security_group_name}.json", "w",
                  encoding="utf-8") as file:
            json.dump(base, fp=file, indent=2)
        return WaitableObject()

    def get(self, resource_group_name: str, network_security_group_name: str) -> NetworkSecurityGroup:
        try:
            with open(self.path / resource_group_name / f"nsg-{network_security_group_name}.json", "r",
                      encoding="utf-8") as file:
                return NetworkSecurityGroup.deserialize(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("Network security group not found") from None


class FakeVirtualNetwork:
    def __init__(self, path: Path) -> None:
        self.path = path

    def list(self, resource_group_name: str) -> List[VirtualNetwork]:
        try:
            files = [file for file in os.listdir(self.path / resource_group_name) if file.startswith("vnet-")]
        except FileNotFoundError:
            raise ResourceNotFoundError("No resource group") from None
        elements = []
        for file in files:
            with open(self.path / resource_group_name / file, "r", encoding="utf-8") as file:
                elements.append(VirtualNetwork.deserialize(json.load(file)))
        return elements

    def begin_create_or_update(self, resource_group_name: str, virtual_network_name: str, parameters: Dict[str, Any]
                               ) -> WaitableObject:
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
                  f"/Microsoft.Network/virtualNetworks/{virtual_network_name}",
            "name": virtual_network_name,
            "type": "Microsoft.Network/virtualNetworks",
            "location": parameters["location"],
            "etag": "W/\"821c1ea3-6313-4798-859b-63ba15e882cc\"",
            "properties": {
                "addressSpace": {
                    "addressPrefixes": [
                        parameters['address_space']['address_prefixes']
                    ]
                },
                "subnets": [
                ],
                "virtualNetworkPeerings": [],
                "resourceGuid": "e9660f35-9f2b-4134-8b0e-309bd9b3792c",
                "provisioningState": "Succeeded",
                "enableDdosProtection": False
            }
        }
        with open(self.path / resource_group_name / f"vnet-{virtual_network_name}.json", "w", encoding="utf-8") as file:
            json.dump(base, fp=file, indent=2)
        return WaitableObject()

    def get(self, resource_group_name: str, virtual_network_name: str) -> NetworkSecurityGroup:
        try:
            with open(self.path / resource_group_name / f"vnet-{virtual_network_name}.json", "r", encoding="utf-8") as file:
                return VirtualNetwork.deserialize(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("Network security group not found") from None


class FakeSubnet:
    def __init__(self, path: Path) -> None:
        self.path = path

    def list(self, resource_group_name: str, virtual_network_name: str) -> List[Subnet]:
        try:
            files = [file for file in os.listdir(self.path / resource_group_name) if
                     file.startswith(f"subnet-{virtual_network_name}")]
        except FileNotFoundError:
            raise ResourceNotFoundError("No resource group") from None
        elements = []
        for file in files:
            with open(self.path / resource_group_name / file, "r", encoding="utf-8") as file:
                elements.append(Subnet.deserialize(json.load(file)))
        return elements

    def begin_create_or_update(self, resource_group_name: str, virtual_network_name: str, subnet_name: str,
                               subnet_parameters: Dict[str, Any]) -> WaitableObject:
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
                  f"/Microsoft.Network/virtualNetworks/{virtual_network_name}/subnets/{subnet_name}",
            "name": subnet_name,
            "etag": "W/\"821c1ea3-6313-4798-859b-63ba15e882cc\"",
            "type": "Microsoft.Network/virtualNetworks/subnets",
            "properties": {
                "addressPrefix": subnet_parameters["address_prefix"],
                "networkSecurityGroup": {
                    "id": subnet_parameters["network_security_group"]["id"]
                },
                "ipConfigurations": [
                ],
                "delegations": [],
                "provisioningState": "Succeeded",
                "privateEndpointNetworkPolicies": "Enabled",
                "privateLinkServiceNetworkPolicies": "Enabled"
            }
        }
        with open(self.path / resource_group_name / f"subnet-{virtual_network_name}-{subnet_name}.json", "w",
                  encoding="utf-8") as file:
            json.dump(base, fp=file, indent=2)
        return WaitableObject()

    def get(self, resource_group_name: str, virtual_network_name: str, subnet_name: str) -> NetworkSecurityGroup:
        try:
            with open(self.path / resource_group_name / f"subnet-{virtual_network_name}-{subnet_name}.json", "r",
                      encoding="utf-8") as file:
                return Subnet.deserialize(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("Subnet group not found") from None


class FakeIpAddress:
    def __init__(self, path: Path) -> None:
        self.path = path

    def list(self, resource_group_name: str) -> List[PublicIPAddress]:
        try:
            files = [file for file in os.listdir(self.path / resource_group_name) if file.startswith("ip-")]
        except FileNotFoundError:
            raise ResourceNotFoundError("No resource group") from None
        elements = []
        for file in files:
            with open(self.path / resource_group_name / file, "r", encoding="utf-8") as file:
                elements.append(PublicIPAddress.deserialize(json.load(file)))
        return elements

    def begin_create_or_update(self, resource_group_name: str, public_ip_address_name: str, parameters: Dict[str, Any]
                               ) -> WaitableObject:
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
                  f"/Microsoft.Network/publicIPAddresses/{public_ip_address_name}",
            "name": public_ip_address_name,
            "type": "Microsoft.Network/publicIPAddresses",
            "location": parameters["location"],
            "sku": {
                "name": parameters["sku"]["name"],
                "tier": "Regional"
            },
            "etag": "W/\"c6bd8f78-66eb-4fc6-90d0-98d790c5467e\"",
            "properties": {
                "publicIPAllocationMethod": parameters["public_ip_allocation_method"],
                "publicIPAddressVersion": parameters["public_ip_address_version"],
                "ipConfiguration": {
                },
                "ipTags": [],
                "ipAddress": "52.240.59.45",
                "idleTimeoutInMinutes": 4,
                "resourceGuid": "63d8cd05-8056-488f-a252-2c5e6b38360e",
                "provisioningState": "Succeeded"
            }
        }
        with open(self.path / resource_group_name / f"ip-{public_ip_address_name}.json", "w", encoding="utf-8") as file:
            json.dump(base, fp=file, indent=2)
        return WaitableObject()

    def get(self, resource_group_name: str, public_ip_address_name: str) -> PublicIPAddress:
        try:
            with open(self.path / resource_group_name / f"ip-{public_ip_address_name}.json", "r",
                      encoding="utf-8") as file:
                return PublicIPAddress.deserialize(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("Public IP address not found") from None

    def begin_delete(self, resource_group_name: str, public_ip_address_name: str) -> WaitableObject:
        os.remove(self.path / resource_group_name / f"ip-{public_ip_address_name}.json")
        return WaitableObject()


class FakeNetworkInterface:
    def __init__(self, path: Path) -> None:
        self.path = path

    def list(self, resource_group_name: str) -> List[NetworkInterface]:
        try:
            files = [file for file in os.listdir(self.path / resource_group_name) if file.startswith("nic-")]
        except FileNotFoundError:
            raise ResourceNotFoundError("No resource group") from None
        elements = []
        for file in files:
            with open(self.path / resource_group_name / file, "r", encoding="utf-8") as file:
                elements.append(NetworkInterface.deserialize(json.load(file)))
        return elements

    def begin_create_or_update(self, resource_group_name: str, network_interface_name: str, parameters: Dict[str, Any]
                               ) -> WaitableObject:
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
                  f"/Microsoft.Network/networkInterfaces/{network_interface_name}",
            "name": network_interface_name,
            "type": "Microsoft.Network/networkInterfaces",
            "location": parameters["location"],
            "etag": "W/\"a1a80a74-a244-4e0b-9883-41eb47fa633e\"",
            "properties": {
                "ipConfigurations": [
                    {
                        "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups"
                              f"/{resource_group_name}/providers/Microsoft.Network/networkInterfaces/"
                              f"{network_interface_name}/ipConfigurations/{parameters['ip_configurations'][0]['name']}",
                        "name": parameters['ip_configurations'][0]['name'],
                        "etag": "W/\"a1a80a74-a244-4e0b-9883-41eb47fa633e\"",
                        "type": "Microsoft.Network/networkInterfaces/ipConfigurations",
                        "properties": {
                            "privateIPAddress": "10.0.0.4",
                            "privateIPAllocationMethod": "Dynamic",
                            "privateIPAddressVersion": "IPv4",
                            "subnet": {
                                "id": parameters["ip_configurations"][0]["subnet"]["id"]
                            },
                            "primary": True,
                            "publicIPAddress": {
                                "id": parameters['ip_configurations'][0]['public_ip_address']["id"],
                                "name": parameters['ip_configurations'][0]['public_ip_address']["id"].split("/", -1)[
                                    -1],
                                "type": "Microsoft.Network/publicIPAddresses",
                                "sku": {
                                    "name": "Basic",
                                    "tier": "Regional"
                                },
                                "properties": {
                                    "publicIPAllocationMethod": "Dynamic",
                                    "publicIPAddressVersion": "IPv4",
                                    "ipConfiguration": {
                                        "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups"
                                              f"/{resource_group_name}/providers/Microsoft.Network/networkInterfaces"
                                              f"/{network_interface_name}/ipConfigurations"
                                              f"/{parameters['ip_configurations'][0]['name']}"
                                    },
                                    "ipTags": [],
                                    "idleTimeoutInMinutes": 4,
                                    "resourceGuid": "cbf93df3-72ac-4c54-8bc1-71c9ebfb1907",
                                    "provisioningState": "Succeeded",
                                    "deleteOption": "Delete"
                                }
                            },
                            "provisioningState": "Succeeded"
                        }
                    }
                ],
                "tapConfigurations": [],
                "dnsSettings": {
                    "dnsServers": [],
                    "appliedDnsServers": [],
                    "internalDomainNameSuffix": "guhwn0jlt20edcyogcn3tm1zfe.bx.internal.cloudapp.net"
                },
                "macAddress": "00-0D-3A-14-3F-92",
                "primary": True,
                "enableAcceleratedNetworking": parameters["enable_accelerated_networking"],
                "enableIPForwarding": False,
                "hostedWorkloads": [],
                "resourceGuid": "3232a287-da4c-40eb-b5c8-cadb487d6dce",
                "provisioningState": "Succeeded",
                "nicType": "Standard"
            }
        }
        with open(self.path / resource_group_name / f"nic-{network_interface_name}.json", "w", encoding="utf-8") as file:
            json.dump(base, fp=file, indent=2)
        return WaitableObject()

    def get(self, resource_group_name: str, network_interface_name: str) -> NetworkInterface:
        try:
            with open(self.path / resource_group_name / f"nic-{network_interface_name}.json", "r",
                      encoding="utf-8") as file:
                return NetworkInterface.deserialize(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("NIC not found") from None

    def begin_delete(self, resource_group_name: str, network_interface_name: str) -> WaitableObject:
        os.remove(self.path / resource_group_name / f"nic-{network_interface_name}.json")
        return WaitableObject()


class FakeNetwork:  # pylint: disable=too-few-public-methods

    def __init__(self, path) -> None:
        self.path: Path = path
        self.network_security_groups = FakeNetworkSecurityGroup(self.path)
        self.virtual_networks = FakeVirtualNetwork(self.path)
        self.subnets = FakeSubnet(self.path)
        self.public_ip_addresses = FakeIpAddress(self.path)
        self.network_interfaces = FakeNetworkInterface(self.path)


class FakeVirtualMachines:
    def __init__(self, path: Path) -> None:
        self.path = path

    def list(self, resource_group_name: str) -> List[VirtualMachine]:
        try:
            files = [file for file in os.listdir(self.path / resource_group_name) if file.startswith("vm-")]
        except FileNotFoundError:
            raise ResourceNotFoundError("No resource group") from None
        elements = []
        for fle in files:
            with open(self.path / resource_group_name / fle, "r", encoding="utf-8") as file:
                elements.append(VirtualMachine.deserialize(json.load(file)))
        return elements

    def begin_create_or_update(self, resource_group_name: str, vm_name: str, parameters: Dict[str, Any]
                               ) -> WaitableObject:
        parameters = dict_keys_to_camel_case(parameters)
        location = parameters.pop("location")
        tags = parameters.pop("tags")

        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}"
                  f"/providers/Microsoft.Compute/virtualMachines/{vm_name}",
            "name": vm_name,
            "type": "Microsoft.Compute/virtualMachines",
            "location": location,
            "properties": {
                "hardwareProfile": {
                    "vmSize": "Standard_D2_v5"
                },
                "storageProfile": {
                    "imageReference": {
                        "publisher": "OpenLogic",
                        "offer": "CentOS",
                        "sku": "7.5",
                        "version": "latest",
                        "exactVersion": "7.5.201808150"
                    },
                    "osDisk": {
                        "osType": "Linux",
                        "name": "lukasz-3_OsDisk_1_7fcdd979aec64c40b5b153bfe9daed35",
                        "caching": "ReadWrite",
                        "createOption": "FromImage",
                        "diskSizeGB": 30,
                        "managedDisk": {
                            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups"
                                  f"/{resource_group_name}/providers/Microsoft.Compute/disks"
                                  f"/{vm_name}_OsDisk_1_7fcdd979aec64c40b5b153bfe9daed35",
                            "storageAccountType": "Standard_LRS"
                        },
                        "deleteOption": "Detach"
                    },
                    "dataDisks": []
                },
                "osProfile": {
                    "computerName": "lukasz-3",
                    "adminUsername": "lukasz",
                    "linuxConfiguration": {
                        "disablePasswordAuthentication": True,
                        "ssh": {
                            "publicKeys": [
                                {
                                    "path": "/home/lukasz/.ssh/authorized_keys",
                                    "keyData": "ssh-rsa fake_key_data== scylla-qa-ec2\n"
                                }
                            ]
                        },
                        "provisionVMAgent": True,
                        "patchSettings": {
                            "patchMode": "ImageDefault",
                            "assessmentMode": "ImageDefault"
                        }
                    },
                    "secrets": [],
                    "allowExtensionOperations": True,
                    "requireGuestProvisionSignal": True
                },
                "networkProfile": {
                    "networkInterfaces": [
                        {
                            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups"
                                  f"/{resource_group_name}/providers/Microsoft.Network/networkInterfaces/lukasz-3-nic",
                            "properties": {
                                "deleteOption": "Delete"
                            }
                        }
                    ]
                },
                "priority": "Spot",
                "evictionPolicy": "Delete",
                "billingProfile": {
                    "maxPrice": -1.0
                },
                "provisioningState": "Succeeded",
                "vmId": "ce7b8d6c-4204-4262-9504-862189431e5d"
            }
        }
        base["properties"].update(**parameters)
        base["tags"] = tags
        with open(self.path / resource_group_name / f"vm-{vm_name}.json", "w", encoding="utf-8") as file:
            json.dump(base, fp=file, indent=2)
        return WaitableObject()

    def begin_update(self, resource_group_name: str, vm_name: str, parameters: Dict[str, Any]) -> WaitableObject:
        try:
            with open(self.path / resource_group_name / f"vm-{vm_name}.json", "r", encoding="utf-8") as file:
                v_m = json.loads(file.read())
        except FileNotFoundError:
            raise ResourceNotFoundError("Virtual Machine not found") from None

        tags = parameters.pop("tags") if "tags" in parameters else {}
        v_m["properties"].update(**dict_keys_to_camel_case(parameters))
        v_m["tags"].update(**tags)
        VirtualMachine.deserialize(v_m)
        with open(self.path / resource_group_name / f"vm-{vm_name}.json", "w", encoding="utf-8") as file:
            json.dump(v_m, fp=file, indent=2)
        return WaitableObject()

    def begin_delete(self, resource_group_name: str, vm_name: str) -> WaitableObject:
        network_svc = FakeNetwork(self.path)
        v_m = self.get(resource_group_name, vm_name)
        os.remove(self.path / resource_group_name / f"vm-{vm_name}.json")
        nic_name = v_m.network_profile.network_interfaces[0].id.split("/", -1)[-1]
        nic = network_svc.network_interfaces.get(resource_group_name, nic_name)
        network_svc.network_interfaces.begin_delete(resource_group_name, nic_name)
        network_svc.public_ip_addresses.begin_delete(
            resource_group_name, nic.ip_configurations[0].public_ip_address.name)
        return WaitableObject()

    def get(self, resource_group_name: str, vm_name: str) -> VirtualMachine:
        try:
            with open(self.path / resource_group_name / f"vm-{vm_name}.json", "r", encoding="utf-8") as file:
                return VirtualMachine.deserialize(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("Virtual Machine not found") from None

    def begin_restart(self, resource_group_name, vm_name  # pylint: disable=unused-argument, no-self-use
                      ) -> WaitableObject:
        return WaitableObject()


class Compute:  # pylint: disable=too-few-public-methods

    def __init__(self, path) -> None:
        self.path: Path = path
        self.virtual_machines = FakeVirtualMachines(self.path)


class FakeResourceManagementClient:  # pylint: disable=too-few-public-methods

    def __init__(self, path: Path) -> None:
        self.path = path

    @property
    def resource_groups(self) -> FakeResourceGroups:
        return FakeResourceGroups(self.path)


class FakeAzureService(AzureService):

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.mkdir(exist_ok=True)

    @property
    def resource(self) -> FakeResourceManagementClient:
        return FakeResourceManagementClient(self.path)

    @property
    def network(self) -> FakeNetwork:
        return FakeNetwork(self.path)

    @property
    def compute(self) -> Compute:
        return Compute(self.path)
