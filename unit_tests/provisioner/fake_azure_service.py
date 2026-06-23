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
import subprocess

from dataclasses import dataclass
from pathlib import Path
from typing import List, Any, Dict

from azure.core.exceptions import ResourceNotFoundError, AzureError, ODataV4Error, _HttpResponseCommonAPI
from azure.mgmt.network.models import NetworkSecurityGroup, Subnet, PublicIPAddress, NetworkInterface, VirtualNetwork
from azure.mgmt.resource.resources.models import ResourceGroup
from azure.mgmt.compute.models import VirtualMachine, Image, InstanceViewStatus, RunCommandResult


def snake_case_to_camel_case(string):
    temp = string.split("_")
    return temp[0] + "".join(ele.title() for ele in temp[1:])


def dict_keys_to_camel_case(dct):
    if isinstance(dct, list):
        return [dict_keys_to_camel_case(i) if isinstance(i, (dict, list)) else i for i in dct]
    return {
        snake_case_to_camel_case(a): dict_keys_to_camel_case(b) if isinstance(b, (dict, list)) else b
        for a, b in dct.items()
    }


class WaitableObject:
    def __init__(self, error: AzureError = None):
        self.error = error

    def wait(self, timeout=None):
        if self.error:
            raise self.error

    def done(self) -> bool:
        return True


class ResultableObject:
    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr

    def result(
        self,
    ):
        value = InstanceViewStatus(
            additional_properties={},
            code="ProvisioningState/succeeded",
            level="Info",
            display_status="Provisioning succeeded",
            message=f"Enable succeeded: \n[stdout]\n{self.stdout}\n[stderr]\n{self.stderr}",
            time=None,
        )
        return RunCommandResult(value=[value])


class FakeResourceGroups:
    def __init__(self, path: Path):
        self.path = path

    def create_or_update(self, resource_group_name: str, parameters: Dict[str, Any]) -> ResourceGroup:
        res_group = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}",
            "name": resource_group_name,
            "type": "Microsoft.Resources/resourceGroups",
            "properties": {"provisioningState": "Succeeded"},
        }
        res_group.update(**parameters)
        (self.path / resource_group_name).mkdir(exist_ok=True)
        with open(self.path / resource_group_name / "resource_group.json", "w", encoding="utf-8") as file_obj:
            json.dump(res_group, fp=file_obj, indent=2)
        return ResourceGroup.deserialize(res_group)

    def get(self, name) -> ResourceGroup:
        try:
            with open(self.path / name / "resource_group.json", "r", encoding="utf-8") as file:
                return ResourceGroup.deserialize(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("Resource group not found") from None

    def list(self) -> List[ResourceGroup]:
        rgs = []
        for name in os.listdir(self.path):
            try:
                with open(self.path / str(name) / "resource_group.json", "r", encoding="utf-8") as file:
                    rgs.append(ResourceGroup.deserialize(json.load(file)))
            except FileNotFoundError:
                raise ResourceNotFoundError("Resource group not found") from None
        return rgs

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
        for file_name in files:
            with open(self.path / resource_group_name / file_name, "r", encoding="utf-8") as file_obj:
                elements.append(NetworkSecurityGroup.deserialize(json.load(file_obj)))
        return elements

    def begin_create_or_update(
        self, resource_group_name: str, network_security_group_name: str, parameters: Dict[str, Any]
    ) -> WaitableObject:
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
            f"/Microsoft.Network/networkSecurityGroups/default",
            "name": network_security_group_name,
            "type": "Microsoft.Network/networkSecurityGroups",
            "location": parameters["location"],
            "etag": 'W/"90369d69-7507-4e77-8b90-66e1f971f290"',
            "properties": {
                "subnets": [],
                "resourceGuid": "2100b376-ad42-449d-a9c2-62cdade9db83",
                "provisioningState": "Succeeded",
            },
        }
        rules = []
        for rule in parameters["security_rules"]:
            rules.append(
                {
                    "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}"
                    f"/providers/Microsoft.Network/networkSecurityGroups/default/securityRules/{rule['name']}",
                    "name": rule["name"],
                    "etag": 'W/"90369d69-7507-4e77-8b90-66e1f971f290"',
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
                        "provisioningState": "Succeeded",
                    },
                }
            )
        base.update({"securityRules": rules})
        with open(
            self.path / resource_group_name / f"nsg-{network_security_group_name}.json", "w", encoding="utf-8"
        ) as file:
            json.dump(base, fp=file, indent=2)
        return WaitableObject()

    def get(self, resource_group_name: str, network_security_group_name: str) -> NetworkSecurityGroup:
        try:
            with open(
                self.path / resource_group_name / f"nsg-{network_security_group_name}.json", "r", encoding="utf-8"
            ) as file:
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
        for file_name in files:
            with open(self.path / resource_group_name / file_name, "r", encoding="utf-8") as file_obj:
                elements.append(VirtualNetwork.deserialize(json.load(file_obj)))
        return elements

    def begin_create_or_update(
        self, resource_group_name: str, virtual_network_name: str, parameters: Dict[str, Any]
    ) -> WaitableObject:
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
            f"/Microsoft.Network/virtualNetworks/{virtual_network_name}",
            "name": virtual_network_name,
            "type": "Microsoft.Network/virtualNetworks",
            "location": parameters["location"],
            "etag": 'W/"821c1ea3-6313-4798-859b-63ba15e882cc"',
            "properties": {
                "addressSpace": {"addressPrefixes": [parameters["address_space"]["address_prefixes"]]},
                "subnets": [],
                "virtualNetworkPeerings": [],
                "resourceGuid": "e9660f35-9f2b-4134-8b0e-309bd9b3792c",
                "provisioningState": "Succeeded",
                "enableDdosProtection": False,
            },
        }
        with open(self.path / resource_group_name / f"vnet-{virtual_network_name}.json", "w", encoding="utf-8") as file:
            json.dump(base, fp=file, indent=2)
        return WaitableObject()

    def get(self, resource_group_name: str, virtual_network_name: str) -> NetworkSecurityGroup:
        try:
            with open(
                self.path / resource_group_name / f"vnet-{virtual_network_name}.json", "r", encoding="utf-8"
            ) as file:
                return VirtualNetwork.deserialize(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("Network security group not found") from None


class FakeSubnet:
    def __init__(self, path: Path) -> None:
        self.path = path

    def list(self, resource_group_name: str, virtual_network_name: str) -> List[Subnet]:
        try:
            files = [
                file
                for file in os.listdir(self.path / resource_group_name)
                if file.startswith(f"subnet-{virtual_network_name}")
            ]
        except FileNotFoundError:
            raise ResourceNotFoundError("No resource group") from None
        elements = []
        for file_name in files:
            with open(self.path / resource_group_name / file_name, "r", encoding="utf-8") as file_obj:
                elements.append(Subnet.deserialize(json.load(file_obj)))
        return elements

    def begin_create_or_update(
        self, resource_group_name: str, virtual_network_name: str, subnet_name: str, subnet_parameters: Dict[str, Any]
    ) -> WaitableObject:
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
            f"/Microsoft.Network/virtualNetworks/{virtual_network_name}/subnets/{subnet_name}",
            "name": subnet_name,
            "etag": 'W/"821c1ea3-6313-4798-859b-63ba15e882cc"',
            "type": "Microsoft.Network/virtualNetworks/subnets",
            "properties": {
                "addressPrefix": subnet_parameters["address_prefix"],
                "networkSecurityGroup": {"id": subnet_parameters["network_security_group"]["id"]},
                "ipConfigurations": [],
                "delegations": [],
                "provisioningState": "Succeeded",
                "privateEndpointNetworkPolicies": "Enabled",
                "privateLinkServiceNetworkPolicies": "Enabled",
            },
        }
        with open(
            self.path / resource_group_name / f"subnet-{virtual_network_name}-{subnet_name}.json", "w", encoding="utf-8"
        ) as file:
            json.dump(base, fp=file, indent=2)
        return WaitableObject()

    def get(self, resource_group_name: str, virtual_network_name: str, subnet_name: str) -> NetworkSecurityGroup:
        try:
            with open(
                self.path / resource_group_name / f"subnet-{virtual_network_name}-{subnet_name}.json",
                "r",
                encoding="utf-8",
            ) as file:
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
        for file_name in files:
            with open(self.path / resource_group_name / file_name, "r", encoding="utf-8") as file_obj:
                elements.append(PublicIPAddress.deserialize(json.load(file_obj)))
        return elements

    def begin_create_or_update(
        self, resource_group_name: str, public_ip_address_name: str, parameters: Dict[str, Any]
    ) -> WaitableObject:
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
            f"/Microsoft.Network/publicIPAddresses/{public_ip_address_name}",
            "name": public_ip_address_name,
            "type": "Microsoft.Network/publicIPAddresses",
            "location": parameters["location"],
            "sku": {"name": parameters["sku"]["name"], "tier": "Regional"},
            "etag": 'W/"c6bd8f78-66eb-4fc6-90d0-98d790c5467e"',
            "properties": {
                "publicIPAllocationMethod": parameters["public_ip_allocation_method"],
                "publicIPAddressVersion": parameters["public_ip_address_version"],
                "ipConfiguration": {},
                "ipTags": [],
                "ipAddress": "52.240.59.45",
                "idleTimeoutInMinutes": 4,
                "resourceGuid": "63d8cd05-8056-488f-a252-2c5e6b38360e",
                "provisioningState": "Succeeded",
            },
        }
        with open(
            self.path / resource_group_name / f"ip-{public_ip_address_name}.json", "w", encoding="utf-8"
        ) as file_obj:
            json.dump(base, fp=file_obj, indent=2)
        return WaitableObject()

    def get(self, resource_group_name: str, public_ip_address_name: str) -> PublicIPAddress:
        try:
            with open(
                self.path / resource_group_name / f"ip-{public_ip_address_name}.json", "r", encoding="utf-8"
            ) as file:
                return PublicIPAddress.deserialize(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("Public IP address not found") from None

    def begin_delete(self, resource_group_name: str, public_ip_address_name: str) -> WaitableObject:
        try:
            os.remove(self.path / resource_group_name / f"ip-{public_ip_address_name}.json")
        except FileNotFoundError:
            pass
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
        for file_name in files:
            with open(self.path / resource_group_name / file_name, "r", encoding="utf-8") as file_obj:
                elements.append(NetworkInterface.deserialize(json.load(file_obj)))
        return elements

    def begin_create_or_update(
        self, resource_group_name: str, network_interface_name: str, parameters: Dict[str, Any]
    ) -> WaitableObject:
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
            f"/Microsoft.Network/networkInterfaces/{network_interface_name}",
            "name": network_interface_name,
            "type": "Microsoft.Network/networkInterfaces",
            "location": parameters["location"],
            "etag": 'W/"a1a80a74-a244-4e0b-9883-41eb47fa633e"',
            "properties": {
                "ipConfigurations": [
                    {
                        "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups"
                        f"/{resource_group_name}/providers/Microsoft.Network/networkInterfaces/"
                        f"{network_interface_name}/ipConfigurations/{parameters['ip_configurations'][0]['name']}",
                        "name": parameters["ip_configurations"][0]["name"],
                        "etag": 'W/"a1a80a74-a244-4e0b-9883-41eb47fa633e"',
                        "type": "Microsoft.Network/networkInterfaces/ipConfigurations",
                        "properties": {
                            "privateIPAddress": "10.0.0.4",
                            "privateIPAllocationMethod": "Dynamic",
                            "privateIPAddressVersion": "IPv4",
                            "subnet": {"id": parameters["ip_configurations"][0]["subnet"]["id"]},
                            "primary": True,
                            "publicIPAddress": {
                                "id": parameters["ip_configurations"][0]["public_ip_address"]["id"],
                                "name": parameters["ip_configurations"][0]["public_ip_address"]["id"].split("/", -1)[
                                    -1
                                ],
                                "type": "Microsoft.Network/publicIPAddresses",
                                "sku": {"name": "Basic", "tier": "Regional"},
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
                                    "deleteOption": "Delete",
                                },
                            },
                            "provisioningState": "Succeeded",
                        },
                    }
                ],
                "tapConfigurations": [],
                "dnsSettings": {
                    "dnsServers": [],
                    "appliedDnsServers": [],
                    "internalDomainNameSuffix": "guhwn0jlt20edcyogcn3tm1zfe.bx.internal.cloudapp.net",
                },
                "macAddress": "00-0D-3A-14-3F-92",
                "primary": True,
                "enableAcceleratedNetworking": parameters["enable_accelerated_networking"],
                "enableIPForwarding": False,
                "hostedWorkloads": [],
                "resourceGuid": "3232a287-da4c-40eb-b5c8-cadb487d6dce",
                "provisioningState": "Succeeded",
                "nicType": "Standard",
            },
        }
        with open(
            self.path / resource_group_name / f"nic-{network_interface_name}.json", "w", encoding="utf-8"
        ) as file:
            json.dump(base, fp=file, indent=2)
        return WaitableObject()

    def get(self, resource_group_name: str, network_interface_name: str) -> NetworkInterface:
        try:
            with open(
                self.path / resource_group_name / f"nic-{network_interface_name}.json", "r", encoding="utf-8"
            ) as file:
                return NetworkInterface.deserialize(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("NIC not found") from None

    def begin_delete(self, resource_group_name: str, network_interface_name: str) -> WaitableObject:
        try:
            os.remove(self.path / resource_group_name / f"nic-{network_interface_name}.json")
        except FileNotFoundError:
            pass
        return WaitableObject()


class FakeNetwork:
    def __init__(self, path) -> None:
        self.path: Path = path
        self.network_security_groups = FakeNetworkSecurityGroup(self.path)
        self.virtual_networks = FakeVirtualNetwork(self.path)
        self.subnets = FakeSubnet(self.path)
        self.public_ip_addresses = FakeIpAddress(self.path)
        self.network_interfaces = FakeNetworkInterface(self.path)


@dataclass
class VMCreateBehavior:
    """Describe how a fake VM creation should behave in tests.

    If ``stuck`` is True, the VM stays in the Azure stuck state instead of succeeding.
    If ``recover_after_polls`` is set, the VM recovers after that many instance-view polls.
    If it is ``None``, the VM stays stuck until it is deleted and recreated.
    If ``redeploy_fails`` is True, a redeploy of this VM raises an error.
    If ``disappear_after_polls`` is set, the VM is deleted after that many instance-view polls so
    subsequent get() calls return 404 - simulating an Azure spot eviction during provisioning.
    """

    stuck: bool = False
    recover_after_polls: int | None = None
    redeploy_fails: bool = False
    disappear_after_polls: int | None = None


class FakeVirtualMachines:
    _scripts: Dict[str, Dict[str, Any]] = {}

    def __init__(self, path: Path) -> None:
        self.path = path

    @classmethod
    def set_provision_script(
        cls, vm_name: str, behaviors: List[VMCreateBehavior] | None = None, default: VMCreateBehavior | None = None
    ) -> None:
        """Store the scripted create behavior for a VM name."""
        cls._scripts[vm_name] = {
            "queue": list(behaviors or []),
            "default": default,
        }

    @classmethod
    def clear_scripts(cls) -> None:
        """Remove all scripted VM behaviors."""
        cls._scripts = {}

    @classmethod
    def _next_behavior(cls, vm_name: str) -> VMCreateBehavior:
        """Return the next scripted behavior for a VM, or the default healthy one."""
        script = cls._scripts.get(vm_name)
        if not script:
            return VMCreateBehavior()
        if script["queue"]:
            return script["queue"].pop(0)
        return script["default"] or VMCreateBehavior()

    def _vm_path(self, resource_group_name: str, vm_name: str) -> Path:
        """Build the JSON file path for a fake VM."""
        return self.path / resource_group_name / f"vm-{vm_name}.json"

    def _read_raw(self, resource_group_name: str, vm_name: str) -> Dict[str, Any]:
        """Load raw fake VM data from disk."""
        try:
            with open(self._vm_path(resource_group_name, vm_name), "r", encoding="utf-8") as file:
                return json.load(file)
        except FileNotFoundError:
            raise ResourceNotFoundError("Virtual Machine not found") from None

    def _write_raw(self, resource_group_name: str, vm_name: str, data: Dict[str, Any]) -> None:
        """Write raw fake VM data to disk."""
        with open(self._vm_path(resource_group_name, vm_name), "w", encoding="utf-8") as file:
            json.dump(data, fp=file, indent=2)

    @staticmethod
    def _build_instance_view(provisioning_state: str) -> Dict[str, Any]:
        """Build the fake Azure instance view for the VM state."""
        if provisioning_state == "Succeeded":
            return {
                "vmAgent": {
                    "vmAgentVersion": "2.9.1.1",
                    "statuses": [
                        {
                            "code": "ProvisioningState/succeeded",
                            "level": "Info",
                            "displayStatus": "Ready",
                            "message": "Guest Agent is running",
                        }
                    ],
                },
                "statuses": [
                    {
                        "code": "ProvisioningState/succeeded",
                        "level": "Info",
                        "displayStatus": "Provisioning succeeded",
                    },
                    {
                        "code": "PowerState/running",
                        "level": "Info",
                        "displayStatus": "VM running",
                    },
                ],
            }
        return {
            "statuses": [
                {
                    "code": "ProvisioningState/creating",
                    "level": "Info",
                    "displayStatus": "Creating",
                },
                {
                    "code": "PowerState/starting",
                    "level": "Info",
                    "displayStatus": "VM starting",
                },
            ],
        }

    def _advance_simulation(self, resource_group_name: str, vm_name: str, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Advance stuck-VM simulation by one poll and mark it succeeded when ready."""
        sim = raw.get("_simulation")
        if not sim or not sim.get("stuck"):
            return raw

        sim["polls_seen"] += 1
        disappear_after = sim.get("disappear_after_polls")
        if disappear_after is not None and sim["polls_seen"] >= disappear_after:
            os.remove(self._vm_path(resource_group_name, vm_name))
            raise ResourceNotFoundError("Virtual Machine not found")
        recover_after = sim.get("recover_after_polls")
        if recover_after is not None and sim["polls_seen"] >= recover_after:
            sim["stuck"] = False
            raw["properties"]["provisioningState"] = "Succeeded"
        self._write_raw(resource_group_name, vm_name, raw)
        return raw

    def list(self, resource_group_name: str) -> List[VirtualMachine]:
        try:
            files = [file for file in os.listdir(self.path / resource_group_name) if file.startswith("vm-")]
        except FileNotFoundError:
            raise ResourceNotFoundError("No resource group") from None
        elements = []
        for fle in files:
            with open(self.path / resource_group_name / fle, "r", encoding="utf-8") as file:
                raw = json.load(file)
            raw.pop("_simulation", None)
            elements.append(VirtualMachine.deserialize(raw))
        return elements

    def begin_create_or_update(
        self, resource_group_name: str, vm_name: str, parameters: Dict[str, Any]
    ) -> WaitableObject:
        tags = parameters.pop("tags") if "tags" in parameters else {}
        parameters = dict_keys_to_camel_case(parameters)
        location = parameters.pop("location")
        priority = parameters.get("priority") or ""
        if (
            tags.get("JenkinsJobTag") == "FailSpotDB"
            and priority.lower() == "spot"
            and tags.get("NodeType") == "scylla-db"
        ):
            # for testing fallback on demand
            class Resp(_HttpResponseCommonAPI):
                @property
                def status_code(self):
                    return 409

                def text(self):
                    return '{"message": "preemption error msg", "error": {"message": "preemption error", "code": "OperationPreempted"}}'

            return WaitableObject(error=ODataV4Error(response=Resp()))

        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}"
            f"/providers/Microsoft.Compute/virtualMachines/{vm_name}",
            "name": vm_name,
            "type": "Microsoft.Compute/virtualMachines",
            "location": location,
            "properties": {
                "hardwareProfile": {"vmSize": "Standard_D2_v5"},
                "storageProfile": {
                    "imageReference": {
                        "publisher": "OpenLogic",
                        "offer": "CentOS",
                        "sku": "7.5",
                        "version": "latest",
                        "exactVersion": "7.5.201808150",
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
                            "storageAccountType": "Standard_LRS",
                        },
                        "deleteOption": "Detach",
                    },
                    "dataDisks": [],
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
                                    "keyData": "ssh-rsa fake_key_data== scylla_test_id_ed25519\n",
                                }
                            ]
                        },
                        "provisionVMAgent": True,
                        "patchSettings": {"patchMode": "ImageDefault", "assessmentMode": "ImageDefault"},
                    },
                    "secrets": [],
                    "allowExtensionOperations": True,
                    "requireGuestProvisionSignal": True,
                },
                "networkProfile": {
                    "networkInterfaces": [
                        {
                            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups"
                            f"/{resource_group_name}/providers/Microsoft.Network/networkInterfaces/lukasz-3-nic",
                            "properties": {"deleteOption": "Delete"},
                        }
                    ]
                },
                "provisioningState": "Succeeded",
                "vmId": "ce7b8d6c-4204-4262-9504-862189431e5d",
            },
        }
        base["properties"].update(**parameters)
        base["tags"] = tags
        behavior = self._next_behavior(vm_name)
        if behavior.stuck:
            base["properties"]["provisioningState"] = "Creating"
            base["_simulation"] = {
                "stuck": True,
                "recover_after_polls": behavior.recover_after_polls,
                "disappear_after_polls": behavior.disappear_after_polls,
                "polls_seen": 0,
            }
        with open(self.path / resource_group_name / f"vm-{vm_name}.json", "w", encoding="utf-8") as file:
            json.dump(base, fp=file, indent=2)
        return WaitableObject()

    def begin_update(self, resource_group_name: str, vm_name: str, parameters: Dict[str, Any]) -> WaitableObject:
        v_m = self._read_raw(resource_group_name, vm_name)
        tags = parameters.pop("tags") if "tags" in parameters else {}
        v_m["properties"].update(**dict_keys_to_camel_case(parameters))
        v_m.setdefault("tags", {}).update(**tags)
        sim = v_m.pop("_simulation", None)
        VirtualMachine.deserialize(v_m)
        if sim is not None:
            v_m["_simulation"] = sim
        self._write_raw(resource_group_name, vm_name, v_m)
        return WaitableObject()

    def begin_delete(self, resource_group_name: str, vm_name: str) -> WaitableObject:
        raw = self._read_raw(resource_group_name, vm_name)
        os.remove(self._vm_path(resource_group_name, vm_name))
        nic_ref = (raw["properties"].get("networkProfile") or {}).get("networkInterfaces") or [{}]
        delete_option = (nic_ref[0].get("properties") or {}).get("deleteOption", "Detach")
        if delete_option == "Delete" and nic_ref[0].get("id"):
            network_svc = FakeNetwork(self.path)
            nic_name = nic_ref[0]["id"].split("/")[-1]
            try:
                nic = network_svc.network_interfaces.get(resource_group_name, nic_name)
                network_svc.network_interfaces.begin_delete(resource_group_name, nic_name)
                public_ip = nic.ip_configurations[0].public_ip_address
                if public_ip is not None and getattr(public_ip, "name", None):
                    network_svc.public_ip_addresses.begin_delete(resource_group_name, public_ip.name)
            except ResourceNotFoundError:
                pass
        return WaitableObject()

    def get(self, resource_group_name: str, vm_name: str, expand: str = "") -> VirtualMachine:
        raw = self._read_raw(resource_group_name, vm_name)
        if expand and "instanceview" in expand.lower():
            raw = self._advance_simulation(resource_group_name, vm_name, raw)
            raw = {**raw, "properties": {**raw["properties"]}}
            raw["properties"]["instanceView"] = self._build_instance_view(raw["properties"]["provisioningState"])
        raw.pop("_simulation", None)
        return VirtualMachine.deserialize(raw)

    def instance_view(self, resource_group_name: str, vm_name: str):
        return self.get(resource_group_name, vm_name, expand="instanceView").instance_view

    def begin_restart(self, resource_group_name, vm_name) -> WaitableObject:
        return WaitableObject()

    def begin_redeploy(self, resource_group_name, vm_name) -> WaitableObject:
        behavior = self._next_behavior(vm_name)
        if behavior.redeploy_fails:
            return WaitableObject(
                error=AzureError("VMRedeploymentFailed: redeployment failed due to an internal error")
            )
        raw = self._read_raw(resource_group_name, vm_name)
        raw["properties"]["provisioningState"] = "Creating" if behavior.stuck else "Succeeded"
        if behavior.stuck:
            raw["_simulation"] = {
                "stuck": True,
                "recover_after_polls": behavior.recover_after_polls,
                "polls_seen": 0,
            }
        else:
            raw.pop("_simulation", None)
        self._write_raw(resource_group_name, vm_name, raw)
        return WaitableObject()

    def begin_run_command(self, resource_group_name, vm_name, parameters) -> ResultableObject:
        result = subprocess.run(parameters.script[0], shell=True, capture_output=True, text=True, check=False)
        return ResultableObject(result.stdout, result.stderr)


class FakeImages:
    def __init__(self, path: Path) -> None:
        self.path = path

    def list_by_resource_group(self, resource_group_name) -> List[Image]:
        with open(self.path / resource_group_name / "azure_images_list.json", encoding="utf-8") as file:
            return [Image.deserialize(image) for image in json.load(file)]


class FakeGalleryImageVersions:
    def __init__(self, path: Path) -> None:
        self.path = path

    def list_by_gallery_image(
        self, resource_group_name: str, gallery_name: str, gallery_image_name: str
    ) -> List[Image]:
        try:
            with open(
                self.path / resource_group_name / f"{gallery_name}_{gallery_image_name}_gallery_images_list.json",
                encoding="utf-8",
            ) as file:
                return [Image.deserialize(image) for image in json.load(file)]
        except FileNotFoundError:
            raise ResourceNotFoundError("No gallery images found") from None


class Compute:
    def __init__(self, path) -> None:
        self.path: Path = path
        self.virtual_machines = FakeVirtualMachines(self.path)
        self.images = FakeImages(self.path)
        self.gallery_image_versions = FakeGalleryImageVersions(self.path)


class FakeResourceManagementClient:
    def __init__(self, path: Path) -> None:
        self.path = path

    @property
    def resource_groups(self) -> FakeResourceGroups:
        return FakeResourceGroups(self.path)


class FakeAzureService:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.mkdir(exist_ok=True)
        self.azure_credentials = {"tenant_id": "fake-tenant-id"}
        self.subscription_id = "fake-subscription-id"

    def get_vault_key(self, vault_uri, key_name):
        return None

    def create_vault_key(self, vault_uri, key_name):
        return f"{vault_uri}{key_name}"

    @property
    def resource(self) -> FakeResourceManagementClient:
        return FakeResourceManagementClient(self.path)

    @property
    def network(self) -> FakeNetwork:
        return FakeNetwork(self.path)

    @property
    def compute(self) -> Compute:
        return Compute(self.path)

    @property
    def keyvault(self):
        return FakeKeyVaultClient()


class FakeKeyVaultClient:
    class vaults:
        @staticmethod
        def begin_create_or_update(*args, **kwargs):
            return FakeVaultOperation()


class FakeVaultOperation:
    def result(self):
        class MockVault:
            def __init__(self):
                class Properties:
                    vault_uri = "https://fake-vault.vault.azure.net/"

                self.properties = Properties()

        return MockVault()
