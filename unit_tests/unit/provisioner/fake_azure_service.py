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
"""In-memory fake of the Azure SDK clients, backed by JSON files under a directory."""

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from azure.core.exceptions import AzureError, ODataV4Error, ResourceNotFoundError, _HttpResponseCommonAPI
from azure.mgmt.compute.models import (
    Image,
    InstanceViewStatus,
    ResourceSku,
    ResourceSkuCapabilities,
    RunCommandResult,
    VirtualMachine,
)
from azure.mgmt.network.models import NetworkInterface, NetworkSecurityGroup, PublicIPAddress, Subnet, VirtualNetwork
from azure.mgmt.resource.resources.models import ResourceGroup


def rest_body(parameters: Any) -> Dict[str, Any]:
    """The JSON body the SDK sends: an SDK model is serialized, a plain dict is sent as is."""
    return parameters.as_dict() if hasattr(parameters, "as_dict") else parameters


def snake_case_to_camel_case(string):
    """Convert a snake_case key to the camelCase the Azure JSON uses."""
    temp = string.split("_")
    return temp[0] + "".join(ele.title() for ele in temp[1:])


def dict_keys_to_camel_case(dct):
    """Recursively convert dict keys from snake_case to camelCase, also inside lists."""
    if isinstance(dct, list):
        return [dict_keys_to_camel_case(i) if isinstance(i, (dict, list)) else i for i in dct]
    return {
        snake_case_to_camel_case(a): dict_keys_to_camel_case(b) if isinstance(b, (dict, list)) else b
        for a, b in dct.items()
    }


class WaitableObject:
    """Stand-in for an Azure long-running operation: result() returns the value or raises the error."""

    def __init__(self, error: AzureError = None, value: Any = None):
        """Store the error to raise and the value to return."""
        self.error = error
        self.value = value

    def wait(self, timeout=None):
        """Raise the stored error, if any; return at once otherwise."""
        if self.error:
            raise self.error

    def done(self) -> bool:
        """Report the operation as finished."""
        return True

    def result(self, timeout=None):
        """Return the stored value, raising the stored error first if set."""
        if self.error:
            raise self.error
        return self.value


class ResultableObject:
    """Stand-in for a run-command poller holding the captured stdout and stderr."""

    def __init__(self, stdout, stderr):
        """Store the captured stdout and stderr."""
        self.stdout = stdout
        self.stderr = stderr

    def result(
        self,
    ):
        """Return a RunCommandResult whose status message embeds stdout and stderr."""
        value = InstanceViewStatus(
            code="ProvisioningState/succeeded",
            level="Info",
            display_status="Provisioning succeeded",
            message=f"Enable succeeded: \n[stdout]\n{self.stdout}\n[stderr]\n{self.stderr}",
            time=None,
        )
        return RunCommandResult(value=[value])


class FakeResourceGroups:
    """Fake resource groups, one directory per group under the service path."""

    def __init__(self, path: Path):
        """Keep the storage path the fake writes its JSON files to."""
        self.path = path

    def create_or_update(self, resource_group_name: str, parameters: Dict[str, Any]) -> ResourceGroup:
        """Write a resource group JSON file with the request parameters merged in."""
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
        return ResourceGroup(res_group)

    def get(self, name) -> ResourceGroup:
        """Return the stored resource group, raising ResourceNotFoundError if absent."""
        try:
            with open(self.path / name / "resource_group.json", "r", encoding="utf-8") as file:
                return ResourceGroup(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("Resource group not found") from None

    def list(self) -> List[ResourceGroup]:
        """Return all resource groups stored under the service path."""
        rgs = []
        for name in os.listdir(self.path):
            try:
                with open(self.path / str(name) / "resource_group.json", "r", encoding="utf-8") as file:
                    rgs.append(ResourceGroup(json.load(file)))
            except FileNotFoundError:
                raise ResourceNotFoundError("Resource group not found") from None
        return rgs

    def begin_delete(self, name: str) -> WaitableObject:
        """Delete the resource group directory with all its resources."""
        shutil.rmtree(self.path / name)
        return WaitableObject()


class FakeNetworkSecurityGroup:
    """Fake network security groups stored as nsg-<name>.json files."""

    def __init__(self, path: Path) -> None:
        """Keep the storage path the fake writes its JSON files to."""
        self.path = path

    def list(self, resource_group_name: str) -> List[NetworkSecurityGroup]:
        """Return the NSGs stored in a resource group."""
        try:
            files = [file for file in os.listdir(self.path / resource_group_name) if file.startswith("nsg-")]
        except FileNotFoundError:
            raise ResourceNotFoundError("No resource group") from None
        elements = []
        for file_name in files:
            with open(self.path / resource_group_name / file_name, "r", encoding="utf-8") as file_obj:
                elements.append(NetworkSecurityGroup(json.load(file_obj)))
        return elements

    def begin_create_or_update(
        self, resource_group_name: str, network_security_group_name: str, parameters: Dict[str, Any]
    ) -> WaitableObject:
        """Write an NSG JSON file with the requested rules, as Azure returns them."""
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
            f"/Microsoft.Network/networkSecurityGroups/default",
            "name": network_security_group_name,
            "type": "Microsoft.Network/networkSecurityGroups",
            "location": rest_body(parameters)["location"],
            "etag": 'W/"90369d69-7507-4e77-8b90-66e1f971f290"',
            "properties": {
                "subnets": [],
                "resourceGuid": "2100b376-ad42-449d-a9c2-62cdade9db83",
                "provisioningState": "Succeeded",
            },
        }
        rules = []
        for rule in rest_body(parameters)["properties"]["securityRules"]:
            rules.append(
                {
                    "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}"
                    f"/providers/Microsoft.Network/networkSecurityGroups/default/securityRules/{rule['name']}",
                    "name": rule["name"],
                    "etag": 'W/"90369d69-7507-4e77-8b90-66e1f971f290"',
                    "type": "Microsoft.Network/networkSecurityGroups/securityRules",
                    "properties": {
                        "sourceAddressPrefixes": [],
                        "destinationAddressPrefixes": [],
                        "sourcePortRanges": [],
                        "destinationPortRanges": [],
                        **rule["properties"],
                        "provisioningState": "Succeeded",
                    },
                }
            )
        base["properties"]["securityRules"] = rules
        with open(
            self.path / resource_group_name / f"nsg-{network_security_group_name}.json", "w", encoding="utf-8"
        ) as file:
            json.dump(base, fp=file, indent=2)
        return WaitableObject()

    def get(self, resource_group_name: str, network_security_group_name: str) -> NetworkSecurityGroup:
        """Return the stored NSG, raising ResourceNotFoundError if absent."""
        try:
            with open(
                self.path / resource_group_name / f"nsg-{network_security_group_name}.json", "r", encoding="utf-8"
            ) as file:
                return NetworkSecurityGroup(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("Network security group not found") from None


class FakeVirtualNetwork:
    """Fake virtual networks stored as vnet-<name>.json files."""

    def __init__(self, path: Path) -> None:
        """Keep the storage path the fake writes its JSON files to."""
        self.path = path

    def list(self, resource_group_name: str) -> List[VirtualNetwork]:
        """Return the virtual networks stored in a resource group."""
        try:
            files = [file for file in os.listdir(self.path / resource_group_name) if file.startswith("vnet-")]
        except FileNotFoundError:
            raise ResourceNotFoundError("No resource group") from None
        elements = []
        for file_name in files:
            with open(self.path / resource_group_name / file_name, "r", encoding="utf-8") as file_obj:
                elements.append(VirtualNetwork(json.load(file_obj)))
        return elements

    def begin_create_or_update(
        self, resource_group_name: str, virtual_network_name: str, parameters: Dict[str, Any]
    ) -> WaitableObject:
        """Write a virtual network JSON file with the requested address space."""
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
            f"/Microsoft.Network/virtualNetworks/{virtual_network_name}",
            "name": virtual_network_name,
            "type": "Microsoft.Network/virtualNetworks",
            "location": rest_body(parameters)["location"],
            "etag": 'W/"821c1ea3-6313-4798-859b-63ba15e882cc"',
            "properties": {
                "addressSpace": rest_body(parameters)["properties"]["addressSpace"],
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
        """Return the stored virtual network, raising ResourceNotFoundError if absent."""
        try:
            with open(
                self.path / resource_group_name / f"vnet-{virtual_network_name}.json", "r", encoding="utf-8"
            ) as file:
                return VirtualNetwork(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("Network security group not found") from None


class FakeSubnet:
    """Fake subnets stored as subnet-<vnet>-<name>.json files."""

    def __init__(self, path: Path) -> None:
        """Keep the storage path the fake writes its JSON files to."""
        self.path = path

    def list(self, resource_group_name: str, virtual_network_name: str) -> List[Subnet]:
        """Return the subnets of a virtual network stored in a resource group."""
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
                elements.append(Subnet(json.load(file_obj)))
        return elements

    def begin_create_or_update(
        self, resource_group_name: str, virtual_network_name: str, subnet_name: str, subnet_parameters: Dict[str, Any]
    ) -> WaitableObject:
        """Write a subnet JSON file with the requested properties and Azure defaults."""
        requested = rest_body(subnet_parameters)["properties"]
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
            f"/Microsoft.Network/virtualNetworks/{virtual_network_name}/subnets/{subnet_name}",
            "name": subnet_name,
            "etag": 'W/"821c1ea3-6313-4798-859b-63ba15e882cc"',
            "type": "Microsoft.Network/virtualNetworks/subnets",
            "properties": {
                **requested,
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
        """Return the stored subnet, raising ResourceNotFoundError if absent."""
        try:
            with open(
                self.path / resource_group_name / f"subnet-{virtual_network_name}-{subnet_name}.json",
                "r",
                encoding="utf-8",
            ) as file:
                return Subnet(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("Subnet group not found") from None


class FakeIpAddress:
    """Fake public IP addresses stored as ip-<name>.json files."""

    def __init__(self, path: Path) -> None:
        """Keep the storage path the fake writes its JSON files to."""
        self.path = path

    def list(self, resource_group_name: str) -> List[PublicIPAddress]:
        """Return the public IP addresses stored in a resource group."""
        try:
            files = [file for file in os.listdir(self.path / resource_group_name) if file.startswith("ip-")]
        except FileNotFoundError:
            raise ResourceNotFoundError("No resource group") from None
        elements = []
        for file_name in files:
            with open(self.path / resource_group_name / file_name, "r", encoding="utf-8") as file_obj:
                elements.append(PublicIPAddress(json.load(file_obj)))
        return elements

    def begin_create_or_update(
        self, resource_group_name: str, public_ip_address_name: str, parameters: Dict[str, Any]
    ) -> WaitableObject:
        """Write a public IP JSON file with a fixed address and the requested allocation."""
        requested = rest_body(parameters)
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
            f"/Microsoft.Network/publicIPAddresses/{public_ip_address_name}",
            "name": public_ip_address_name,
            "type": "Microsoft.Network/publicIPAddresses",
            "location": requested["location"],
            "sku": {"name": requested["sku"]["name"], "tier": "Regional"},
            "etag": 'W/"c6bd8f78-66eb-4fc6-90d0-98d790c5467e"',
            "properties": {
                "publicIPAllocationMethod": requested["properties"]["publicIPAllocationMethod"],
                "publicIPAddressVersion": requested["properties"]["publicIPAddressVersion"],
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
        """Return the stored public IP, raising ResourceNotFoundError if absent."""
        try:
            with open(
                self.path / resource_group_name / f"ip-{public_ip_address_name}.json", "r", encoding="utf-8"
            ) as file:
                return PublicIPAddress(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("Public IP address not found") from None

    def begin_delete(self, resource_group_name: str, public_ip_address_name: str) -> WaitableObject:
        """Remove the public IP file; a missing file is not an error."""
        try:
            os.remove(self.path / resource_group_name / f"ip-{public_ip_address_name}.json")
        except FileNotFoundError:
            pass
        return WaitableObject()


class FakeNetworkInterface:
    """Fake network interfaces stored as nic-<name>.json files."""

    def __init__(self, path: Path) -> None:
        """Keep the storage path the fake writes its JSON files to."""
        self.path = path

    def list(self, resource_group_name: str) -> List[NetworkInterface]:
        """Return the NICs stored in a resource group."""
        try:
            files = [file for file in os.listdir(self.path / resource_group_name) if file.startswith("nic-")]
        except FileNotFoundError:
            raise ResourceNotFoundError("No resource group") from None
        elements = []
        for file_name in files:
            with open(self.path / resource_group_name / file_name, "r", encoding="utf-8") as file_obj:
                elements.append(NetworkInterface(json.load(file_obj)))
        return elements

    @staticmethod
    def _ip_configuration(
        resource_group_name: str, network_interface_name: str, index: int, config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build one ipConfiguration, mirroring what Azure returns for the given request."""
        base_id = (
            f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}"
            f"/providers/Microsoft.Network/networkInterfaces/{network_interface_name}"
        )
        requested = config.get("properties", {})
        version = requested.get("privateIPAddressVersion", "IPv4")
        # keep the address recognisable per subnet, so a test can tell the NICs of a node apart:
        # the 'default' subnet holds the primary NIC, 'nic<N>' the secondary one of index N
        subnet_name = requested["subnet"]["id"].rsplit("/", 1)[-1]
        subnet_index = int(subnet_name.removeprefix("nic")) if subnet_name.startswith("nic") else 0
        properties = {
            "privateIPAllocationMethod": "Dynamic",
            "privateIPAddressVersion": version,
            "subnet": {"id": requested["subnet"]["id"]},
            "primary": requested.get("primary", index == 0),
            "provisioningState": "Succeeded",
        }
        if version == "IPv6":
            properties["privateIPAddress"] = f"fd00:db8:5c7:{subnet_index}::4"
        else:
            properties["privateIPAddress"] = f"10.0.{subnet_index}.4"
        if public_ip := requested.get("publicIPAddress"):
            properties["publicIPAddress"] = {
                "id": public_ip["id"],
                "name": public_ip["id"].split("/", -1)[-1],
                "type": "Microsoft.Network/publicIPAddresses",
                "sku": {"name": "Standard", "tier": "Regional"},
                "properties": {
                    "publicIPAllocationMethod": "Static",
                    "publicIPAddressVersion": version,
                    "ipConfiguration": {"id": f"{base_id}/ipConfigurations/{config['name']}"},
                    "ipTags": [],
                    "idleTimeoutInMinutes": 4,
                    "resourceGuid": "cbf93df3-72ac-4c54-8bc1-71c9ebfb1907",
                    "provisioningState": "Succeeded",
                    "deleteOption": "Delete",
                },
            }
        return {
            "id": f"{base_id}/ipConfigurations/{config['name']}",
            "name": config["name"],
            "etag": 'W/"a1a80a74-a244-4e0b-9883-41eb47fa633e"',
            "type": "Microsoft.Network/networkInterfaces/ipConfigurations",
            "properties": properties,
        }

    def begin_create_or_update(
        self, resource_group_name: str, network_interface_name: str, parameters: Dict[str, Any]
    ) -> WaitableObject:
        """Write a NIC JSON file with one ipConfiguration per requested config."""
        requested = rest_body(parameters)
        base = {
            "id": f"/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/{resource_group_name}/providers"
            f"/Microsoft.Network/networkInterfaces/{network_interface_name}",
            "name": network_interface_name,
            "type": "Microsoft.Network/networkInterfaces",
            "location": requested["location"],
            "etag": 'W/"a1a80a74-a244-4e0b-9883-41eb47fa633e"',
            "properties": {
                "ipConfigurations": [
                    self._ip_configuration(resource_group_name, network_interface_name, index, config)
                    for index, config in enumerate(requested["properties"]["ipConfigurations"])
                ],
                "tapConfigurations": [],
                "dnsSettings": {
                    "dnsServers": [],
                    "appliedDnsServers": [],
                    "internalDomainNameSuffix": "guhwn0jlt20edcyogcn3tm1zfe.bx.internal.cloudapp.net",
                },
                "macAddress": "00-0D-3A-14-3F-92",
                "primary": True,
                "enableAcceleratedNetworking": requested["properties"]["enableAcceleratedNetworking"],
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
        return WaitableObject(value=NetworkInterface(base))

    def get(self, resource_group_name: str, network_interface_name: str) -> NetworkInterface:
        """Return the stored NIC, raising ResourceNotFoundError if absent."""
        try:
            with open(
                self.path / resource_group_name / f"nic-{network_interface_name}.json", "r", encoding="utf-8"
            ) as file:
                return NetworkInterface(json.load(file))
        except FileNotFoundError:
            raise ResourceNotFoundError("NIC not found") from None

    def begin_delete(self, resource_group_name: str, network_interface_name: str) -> WaitableObject:
        """Remove the NIC file; a missing file is not an error."""
        try:
            os.remove(self.path / resource_group_name / f"nic-{network_interface_name}.json")
        except FileNotFoundError:
            pass
        return WaitableObject()


class FakeNetwork:
    """Bundle the fake network sub-services that share one storage path."""

    def __init__(self, path) -> None:
        """Create the network sub-services over the storage path."""
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
    If ``disappear_after_polls`` is set, the VM is deleted after that many instance-view polls so
    subsequent get() calls return 404 - simulating an Azure spot eviction during provisioning.
    If ``transient_error_polls`` is set, the first N instance-view polls raise a transient (non-404)
    AzureError - simulating ARM throttling / 5xx blips that must not abort the batch.
    """

    stuck: bool = False
    recover_after_polls: int | None = None
    disappear_after_polls: int | None = None
    transient_error_polls: int | None = None


class FakeVirtualMachines:
    """Fake VMs stored as vm-<name>.json files, with scripted create behavior per VM name."""

    _scripts: Dict[str, Dict[str, Any]] = {}

    def __init__(self, path: Path) -> None:
        """Keep the storage path the fake writes its JSON files to."""
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
        transient_error_polls = sim.get("transient_error_polls")
        if transient_error_polls is not None and sim["polls_seen"] <= transient_error_polls:
            self._write_raw(resource_group_name, vm_name, raw)
            raise AzureError("transient throttling (429) while polling VM")
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
        """Return the VMs stored in a resource group, hiding the simulation state."""
        try:
            files = [file for file in os.listdir(self.path / resource_group_name) if file.startswith("vm-")]
        except FileNotFoundError:
            raise ResourceNotFoundError("No resource group") from None
        elements = []
        for fle in files:
            with open(self.path / resource_group_name / fle, "r", encoding="utf-8") as file:
                raw = json.load(file)
            raw.pop("_simulation", None)
            elements.append(VirtualMachine(**raw))
        return elements

    def begin_create_or_update(
        self, resource_group_name: str, vm_name: str, parameters: Dict[str, Any]
    ) -> WaitableObject:
        """Write a VM JSON file; a scripted FailSpotDB spot VM fails with a preemption error."""
        tags = parameters.pop("tags") if "tags" in parameters else {}
        parameters = dict_keys_to_camel_case(parameters)
        location = parameters.pop("location")
        properties = parameters.pop("properties", {})
        priority = properties.get("priority") or parameters.get("priority") or ""
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
        base["properties"].update(**properties)
        base["tags"] = tags
        behavior = self._next_behavior(vm_name)
        if behavior.stuck:
            base["properties"]["provisioningState"] = "Creating"
            base["_simulation"] = {
                "stuck": True,
                "recover_after_polls": behavior.recover_after_polls,
                "disappear_after_polls": behavior.disappear_after_polls,
                "transient_error_polls": behavior.transient_error_polls,
                "polls_seen": 0,
            }
        with open(self.path / resource_group_name / f"vm-{vm_name}.json", "w", encoding="utf-8") as file:
            json.dump(base, fp=file, indent=2)
        return WaitableObject()

    def begin_update(self, resource_group_name: str, vm_name: str, parameters: Dict[str, Any]) -> WaitableObject:
        """Merge the update parameters into the stored VM and write it back."""
        v_m = self._read_raw(resource_group_name, vm_name)
        tags = parameters.pop("tags") if "tags" in parameters else {}
        v_m["properties"].update(**dict_keys_to_camel_case(parameters))
        v_m.setdefault("tags", {}).update(**tags)
        sim = v_m.pop("_simulation", None)
        VirtualMachine(**v_m)
        if sim is not None:
            v_m["_simulation"] = sim
        self._write_raw(resource_group_name, vm_name, v_m)
        return WaitableObject()

    def begin_delete(self, resource_group_name: str, vm_name: str) -> WaitableObject:
        """Delete the VM file, and its NIC and public IP when the NIC is deleted with the VM."""
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
        """Return the stored VM, advancing the simulation when the instance view is expanded."""
        raw = self._read_raw(resource_group_name, vm_name)
        if expand and "instanceview" in expand.lower():
            raw = self._advance_simulation(resource_group_name, vm_name, raw)
            raw = {**raw, "properties": {**raw["properties"]}}
            raw["properties"]["instanceView"] = self._build_instance_view(raw["properties"]["provisioningState"])
        raw.pop("_simulation", None)
        return VirtualMachine(**raw)

    def instance_view(self, resource_group_name: str, vm_name: str):
        """Return the instance view of a VM."""
        return self.get(resource_group_name, vm_name, expand="instanceView").instance_view

    def begin_restart(self, resource_group_name, vm_name) -> WaitableObject:
        """Accept a restart request without changing the fake VM."""
        return WaitableObject()

    def begin_run_command(self, resource_group_name, vm_name, parameters) -> ResultableObject:
        """Run the first script of the request in a local shell and capture its output."""
        result = subprocess.run(parameters.script[0], shell=True, capture_output=True, text=True, check=False)
        return ResultableObject(result.stdout, result.stderr)


class FakeImages:
    """Fake images listed in azure_images_list.json of a resource group directory."""

    def __init__(self, path: Path) -> None:
        """Keep the storage path the fake reads its JSON files from."""
        self.path = path

    def list_by_resource_group(self, resource_group_name) -> List[Image]:
        """Return the images listed in the resource group's azure_images_list.json."""
        with open(self.path / resource_group_name / "azure_images_list.json", encoding="utf-8") as file:
            return [self._make_image(image) for image in json.load(file)]

    @staticmethod
    def _make_image(data: dict) -> Image:
        """Build an Image model from a JSON dict, keeping its name."""
        name = data.pop("name", None)
        img = Image(**data)
        img.name = name
        data["name"] = name
        return img


class FakeGalleryImageVersions:
    """Fake gallery image versions read from a per-gallery JSON listing."""

    def __init__(self, path: Path) -> None:
        """Keep the storage path the fake reads its JSON files from."""
        self.path = path

    def list_by_gallery_image(
        self, resource_group_name: str, gallery_name: str, gallery_image_name: str
    ) -> List[Image]:
        """Return the gallery image versions listed for a gallery image."""
        try:
            with open(
                self.path / resource_group_name / f"{gallery_name}_{gallery_image_name}_gallery_images_list.json",
                encoding="utf-8",
            ) as file:
                return [FakeImages._make_image(image) for image in json.load(file)]
        except FileNotFoundError:
            raise ResourceNotFoundError("No gallery images found") from None


class FakeResourceSkus:
    """Minimal 'resource_skus' listing: enough to answer how many NICs a VM size accepts.

    Values match what Azure reports for these sizes, so a test that asks for more NICs than a size
    can carry fails here the same way it would against the real API.
    """

    MAX_NETWORK_INTERFACES = {
        "Standard_D2_v4": 2,
        "Standard_F4s_v2": 2,
        "Standard_L8s_v3": 4,
        "Standard_L16s_v3": 8,
    }

    def __init__(self, path: Path) -> None:
        """Keep the storage path of the fake; the SKU listing does not read it."""
        self.path = path

    def list(self, filter: str = None) -> List[ResourceSku]:  # noqa: A002
        """Return the fake SKUs with their MaxNetworkInterfaces capability; the filter is ignored."""
        return [
            ResourceSku(
                name=name,
                resource_type="virtualMachines",
                capabilities=[ResourceSkuCapabilities(name="MaxNetworkInterfaces", value=str(value))],
            )
            for name, value in self.MAX_NETWORK_INTERFACES.items()
        ]


class Compute:
    """Bundle the fake compute sub-services that share one storage path."""

    def __init__(self, path) -> None:
        """Create the compute sub-services over the storage path."""
        self.path: Path = path
        self.virtual_machines = FakeVirtualMachines(self.path)
        self.images = FakeImages(self.path)
        self.gallery_image_versions = FakeGalleryImageVersions(self.path)
        self.resource_skus = FakeResourceSkus(self.path)


class FakeResourceManagementClient:
    """Fake resource management client rooted at the storage path."""

    def __init__(self, path: Path) -> None:
        """Keep the storage path the fake writes its JSON files to."""
        self.path = path

    @property
    def resource_groups(self) -> FakeResourceGroups:
        """Return the fake resource groups service."""
        return FakeResourceGroups(self.path)


class FakeAzureService:
    """Top-level fake Azure client backed by a directory of JSON files."""

    def __init__(self, path: Path) -> None:
        """Create the storage directory and set the fixed tenant and subscription values."""
        self.path = path
        self.path.mkdir(exist_ok=True)
        self.azure_credentials = {"tenant_id": "fake-tenant-id"}
        self.subscription_id = "fake-subscription-id"
        self.tenant_id = "fake-tenant-id"
        self.minicloud_endpoint = None

    def get_vault_key(self, vault_uri, key_name):
        """Return None: no vault key is stored in the fake."""
        return None

    def create_vault_key(self, vault_uri, key_name):
        """Return the key URL built from the vault URI and key name."""
        return f"{vault_uri}{key_name}"

    @property
    def resource(self) -> FakeResourceManagementClient:
        """Return the fake resource management client."""
        return FakeResourceManagementClient(self.path)

    @property
    def network(self) -> FakeNetwork:
        """Return the fake network client."""
        return FakeNetwork(self.path)

    @property
    def compute(self) -> Compute:
        """Return the fake compute client."""
        return Compute(self.path)

    @property
    def keyvault(self):
        """Return the fake key vault client."""
        return FakeKeyVaultClient()


class FakeKeyVaultClient:
    """Fake key vault client exposing vaults.begin_create_or_update."""

    class vaults:
        """Namespace of the fake vault operations."""

        @staticmethod
        def begin_create_or_update(*args, **kwargs):
            """Return a fake vault operation that yields a vault with a fixed URI."""
            return FakeVaultOperation()


class FakeVaultOperation:
    """Fake long-running vault operation."""

    def result(self):
        """Return a fake vault with a fixed vault URI."""

        class MockVault:
            def __init__(self):
                class Properties:
                    vault_uri = "https://fake-vault.vault.azure.net/"

                self.properties = Properties()

        return MockVault()
