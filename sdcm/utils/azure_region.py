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
# Copyright (c) 2021 ScyllaDB

from __future__ import annotations

import os
import enum
import logging
import binascii
from typing import TYPE_CHECKING
from functools import cached_property
from contextlib import suppress

from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.compute.models import TargetRegion

from sdcm.utils.azure_utils import AzureService

if TYPE_CHECKING:
    # pylint: disable=ungrouped-imports
    from typing import Optional

    from azure.mgmt.compute.models import Gallery, GalleryImageVersion, VirtualMachine
    from azure.mgmt.network.models import (
        NetworkInterface,
        NetworkSecurityGroup,
        PublicIPAddress,
        Subnet,
        VirtualNetwork,
    )


LOGGER = logging.getLogger(__name__)


# Azure offers two main image types, generalized and specialized:
#   1) a generalized image is an image that requires setup to be completed on first boot. For example, on first boot
#      you set the hostname, admin user and other VM-specific configurations;
#   2) a specialize image is an image that are completely configured and not require VM and special parameters,
#      the platform will just turn the VM on.
#
# When you use a specialized image to create an instance you can't alter OS profile (i.e., can't set hostname and
# admin credentials)
#
# See https://docs.microsoft.com/en-us/azure/virtual-machines/linux/imaging#generalized-and-specialized for more.
#
# For SCT Runner image we use the specialized image type because we configure users and don't want to wipe out
# SSH keys we copied to the image during preparation step.
class AzureOsState(enum.Enum):
    GENERALIZED = "Generalized"
    SPECIALIZED = "Specialized"


def region_name_to_location(region_name: str) -> str:
    return region_name.lower().replace(" ", "")  # e.g., "East US" -> "eastus"


# All Azure resources (VMs, networks, public IPs, etc.) should be created in some Resource Group [1].
# Resource group requires to be created in some Azure region but it doesn't limit the region for resources it contains.
# Despite of that we create one resource group per region and use it for all SCT tests running in this region.
#
# Each region contains Virtual Network [2], Subnet [3] and Network Security Group [4].
# Network Security Group is associated with the Subnet and doesn't need to be associated with each Network Interface.
#
# For images we use Shared Image Gallery [5].  We create only one in East US region and will use it for all regions.
# But for images (they called Image Versions in Azure) we require for each region we use to be in the target regions.
#
# [1] https://docs.microsoft.com/en-us/azure/azure-resource-manager/management/overview#resource-groups
# [2] https://docs.microsoft.com/en-us/azure/virtual-network/virtual-networks-overview
# [3] https://docs.microsoft.com/en-us/azure/virtual-network/virtual-network-manage-subnet
# [4] https://docs.microsoft.com/en-us/azure/virtual-network/network-security-groups-overview#network-security-groups
# [5] https://docs.microsoft.com/en-us/azure/virtual-machines/shared-image-galleries
class AzureRegion:  # pylint: disable=too-many-public-methods
    SCT_GALLERY_REGION = "eastus"
    SCT_RESOURCE_GROUP_NAME_PREFIX = "SCT_IMAGES"

    VIRTUAL_NETWORK_NAME_SUFFIX = "vnet"
    NETWORK_SECURITY_GROUP_NAME_SUFFIX = "nsg"
    PUBLIC_IP_ADDRESS_NAME_SUFFIX = "ip"
    NETWORK_INTERFACE_NAME_SUFFIX = "nic"
    IP_CONFIGURATION_NAME_SUFFIX = "ip-config"

    def __init__(self, region_name: str) -> None:
        self.region_name = region_name

    @cached_property
    def azure_service(self) -> AzureService:  # pylint: disable=no-self-use; pylint doesn't now about cached_property
        return AzureService()

    @cached_property
    def location(self) -> str:
        return region_name_to_location(self.region_name)

    @cached_property
    def sct_gallery_location(self) -> str:
        return region_name_to_location(self.SCT_GALLERY_REGION)

    @cached_property
    def sct_resource_group_name(self) -> str:
        return f"{self.SCT_RESOURCE_GROUP_NAME_PREFIX}-{self.location}"

    @cached_property
    def sct_gallery_resource_group_name(self) -> str:
        return f"{self.SCT_RESOURCE_GROUP_NAME_PREFIX}-{self.sct_gallery_location}"

    @cached_property
    def sct_gallery_name(self) -> str:
        return self.SCT_RESOURCE_GROUP_NAME_PREFIX

    @cached_property
    def sct_network_security_group_name(self) -> str:
        return f"{self.sct_resource_group_name}-{self.NETWORK_SECURITY_GROUP_NAME_SUFFIX}"

    @cached_property
    def sct_virtual_network_name(self) -> str:
        return f"{self.sct_resource_group_name}-{self.VIRTUAL_NETWORK_NAME_SUFFIX}"

    @cached_property
    def sct_subnet_name(self) -> str:  # pylint: disable=no-self-use; pylint doesn't now about cached_property
        return "default"

    def common_parameters(self, location: Optional[str] = None, tags: Optional[dict] = None) -> dict:
        return {
            "location": location or self.location,
            "tags": tags or {},
        }

    def create_sct_resource_group(self, tags: Optional[dict] = None) -> None:
        LOGGER.info("Going to create SCT resource group...")
        if self.azure_service.resource.resource_groups.check_existence(
            resource_group_name=self.sct_resource_group_name,
        ):
            LOGGER.info("Resource group `%s' already exists in region `%s'",
                        self.sct_resource_group_name, self.region_name)
            return
        self.azure_service.resource.resource_groups.create_or_update(
            resource_group_name=self.sct_resource_group_name,
            parameters=self.common_parameters(tags=tags),
        )

    @cached_property
    def sct_gallery(self) -> Gallery:
        return self.azure_service.compute.galleries.get(
            resource_group_name=self.sct_gallery_resource_group_name,
            gallery_name=self.sct_gallery_name,
        )

    def create_sct_gallery(self, tags: Optional[dict] = None) -> None:
        LOGGER.info("Going to create SCT shared image gallery...")
        with suppress(ResourceNotFoundError):
            LOGGER.info("Gallery `%s' already exists", self.sct_gallery.name)
            return
        self.azure_service.compute.galleries.begin_create_or_update(
            resource_group_name=self.sct_gallery_resource_group_name,
            gallery_name=self.sct_gallery_name,
            gallery=self.common_parameters(location=self.sct_gallery_location, tags=tags) | {
                "description": "Shared Image Gallery for SCT",
            },
        ).wait()

    @cached_property
    def sct_network_security_group(self) -> NetworkSecurityGroup:
        return self.azure_service.network.network_security_groups.get(
            resource_group_name=self.sct_resource_group_name,
            network_security_group_name=self.sct_network_security_group_name,
        )

    def create_sct_network_security_group(self, tags: Optional[dict] = None) -> None:
        LOGGER.info("Going to create SCT network security group...")
        with suppress(ResourceNotFoundError):
            LOGGER.info("Network security group `%s' already exists in resource group `%s'",
                        self.sct_network_security_group.name, self.sct_resource_group_name)
            return
        self.azure_service.network.network_security_groups.begin_create_or_update(
            resource_group_name=self.sct_resource_group_name,
            network_security_group_name=self.sct_network_security_group_name,
            parameters=self.common_parameters(tags=tags) | {
                "security_rules": [{
                    "name": "SSH",
                    "protocol": "TCP",
                    "source_port_range": "*",
                    "destination_port_range": "22",
                    "source_address_prefix": "*",
                    "destination_address_prefix": "*",
                    "access": "Allow",
                    "priority": 300,
                    "direction": "Inbound",
                }],
            },
        ).wait()

    @cached_property
    def sct_virtual_network(self) -> VirtualNetwork:
        return self.azure_service.network.virtual_networks.get(
            resource_group_name=self.sct_resource_group_name,
            virtual_network_name=self.sct_virtual_network_name,
        )

    def create_sct_virtual_network(self, tags: Optional[dict] = None) -> None:
        LOGGER.info("Going to create SCT virtual network...")
        with suppress(ResourceNotFoundError):
            LOGGER.info("Virtual network `%s' already exists in resource group `%s'",
                        self.sct_virtual_network.name, self.sct_resource_group_name)
            return
        self.azure_service.network.virtual_networks.begin_create_or_update(
            resource_group_name=self.sct_resource_group_name,
            virtual_network_name=self.sct_virtual_network_name,
            parameters=self.common_parameters(tags=tags) | {
                "address_space": {
                    "address_prefixes": ["10.0.0.0/16"],
                }
            }
        ).wait()

    @cached_property
    def sct_subnet(self) -> Subnet:
        return self.azure_service.network.subnets.get(
            resource_group_name=self.sct_resource_group_name,
            virtual_network_name=self.sct_virtual_network_name,
            subnet_name=self.sct_subnet_name,
        )

    def create_sct_subnet(self) -> None:
        LOGGER.info("Going to create SCT subnet...")
        with suppress(ResourceNotFoundError):
            LOGGER.info("Subnet `%s' already exists in network `%s'",
                        self.sct_subnet.name, self.sct_virtual_network_name)
            return
        self.azure_service.network.subnets.begin_create_or_update(
            resource_group_name=self.sct_resource_group_name,
            virtual_network_name=self.sct_virtual_network_name,
            subnet_name=self.sct_subnet_name,
            subnet_parameters={
                "address_prefix": "10.0.0.0/24",
                "network_security_group": {
                    "id": self.sct_network_security_group.id,
                },
            },
        ).wait()

    def create_public_ip_address(self,
                                 public_ip_address_name: str,
                                 tags: Optional[dict] = None) -> PublicIPAddress:
        return self.azure_service.network.public_ip_addresses.begin_create_or_update(
            resource_group_name=self.sct_resource_group_name,
            public_ip_address_name=public_ip_address_name,
            parameters=self.common_parameters(tags=tags) | {
                "sku": {
                    "name": "Standard",
                },
                "public_ip_allocation_method": "Static",
                "public_ip_address_version": "IPV4",
            },
        ).result()

    def create_network_interface(self,
                                 network_interface_name: str,
                                 tags: Optional[dict] = None,
                                 create_public_ip_address: bool = True) -> NetworkInterface:
        parameters = self.common_parameters(tags=tags) | {
            "ip_configurations": [{
                "name": f"{network_interface_name}-{self.IP_CONFIGURATION_NAME_SUFFIX}",
                "subnet": {
                    "id": self.sct_subnet.id,
                },
            }],
            "enable_accelerated_networking": True,
        }
        if create_public_ip_address:
            parameters["ip_configurations"][0]["public_ip_address"] = {
                "id": self.create_public_ip_address(
                    public_ip_address_name=f"{network_interface_name}-{self.PUBLIC_IP_ADDRESS_NAME_SUFFIX}",
                    tags=tags,
                ).id,
            }
        return self.azure_service.network.network_interfaces.begin_create_or_update(
            resource_group_name=self.sct_resource_group_name,
            network_interface_name=network_interface_name,
            parameters=parameters,
        ).result()

    def create_virtual_machine(self,  # pylint: disable=too-many-arguments
                               vm_name: str,
                               vm_size: str,
                               image: dict[str, str],
                               os_state: AzureOsState = AzureOsState.GENERALIZED,
                               computer_name: Optional[str] = None,
                               admin_username: Optional[str] = None,
                               admin_public_key: Optional[str] = None,
                               disk_size: Optional[int] = None,
                               tags: Optional[dict] = None,
                               create_public_ip_address: bool = True,
                               spot: bool = False) -> VirtualMachine:
        if os_state is AzureOsState.GENERALIZED:
            assert admin_username and admin_public_key, "Need to provide login and public key for generalized image"
        return self.azure_service.compute.virtual_machines.begin_create_or_update(
            resource_group_name=self.sct_resource_group_name,
            vm_name=vm_name,
            parameters=self.common_parameters(tags=tags) | {
                "hardware_profile": {
                    "vm_size": vm_size,
                },
                "storage_profile": {
                    "image_reference": image,
                    "os_disk": {
                        "name": vm_name,
                        "os_type": "linux",
                        "caching": "ReadWrite",
                        "create_option": "FromImage",
                        "managed_disk": {
                            "storage_account_type": "Premium_LRS",  # SSD
                        },
                    } | ({} if disk_size is None else {
                        "disk_size_gb": disk_size,
                    }),
                },
                "network_profile": {
                    "network_interfaces": [{
                        "id": self.create_network_interface(
                            network_interface_name=f"{vm_name}-{self.NETWORK_INTERFACE_NAME_SUFFIX}",
                            tags=tags,
                            create_public_ip_address=create_public_ip_address,
                        ).id,
                    }],
                },
            } | ({} if os_state is AzureOsState.SPECIALIZED else {
                "os_profile": {
                    "computer_name": computer_name or vm_name.replace(".", "-"),
                    "admin_username": admin_username,
                    "admin_password": binascii.hexlify(os.urandom(20)).decode(),
                    "linux_configuration": {
                        "disable_password_authentication": True,
                        "ssh": {
                            "public_keys": [{
                                "path": f"/home/{admin_username}/.ssh/authorized_keys",
                                "key_data": admin_public_key,
                            }],
                        },
                    },
                },
            }) | ({} if not spot else {
                "priority": "Spot",  # possible values are "Regular", "Low", or "Spot"
                "eviction_policy": "Deallocate",  # can be "Deallocate" or "Delete"
                "billing_profile": {
                    "max_price": -1,  # -1 indicates the VM shouldn't be evicted for price reasons
                },
            }),
        ).result()

    def deallocate_virtual_machine(self, vm_name: str) -> None:
        self.azure_service.compute.virtual_machines.begin_deallocate(
            resource_group_name=self.sct_resource_group_name,
            vm_name=vm_name,
        ).wait()

    def create_gallery_image(self,
                             gallery_image_name: str,
                             os_state: AzureOsState,
                             tags: Optional[dict] = None) -> None:
        with suppress(ResourceNotFoundError):
            gallery_image = self.azure_service.compute.gallery_images.get(
                resource_group_name=self.sct_gallery_resource_group_name,
                gallery_name=self.sct_gallery_name,
                gallery_image_name=gallery_image_name,
            )
            LOGGER.info("Gallery image `%s' already exists in the gallery `%s'",
                        gallery_image.name, self.sct_gallery_name)
            return
        self.azure_service.compute.gallery_images.begin_create_or_update(
            resource_group_name=self.sct_gallery_resource_group_name,
            gallery_name=self.sct_gallery_name,
            gallery_image_name=gallery_image_name,
            gallery_image=self.common_parameters(location=self.sct_gallery_location, tags=tags) | {
                "hyper_v_generation": "V2",
                "identifier": {
                    "publisher": "ScyllaDB",
                    "offer": f"qa-{gallery_image_name}",
                    "sku": gallery_image_name,
                },
                "os_state": os_state.value,
                "os_type": "Linux",
            },
        ).wait()

    def get_gallery_image_version(self,
                                  gallery_image_name: str,
                                  gallery_image_version_name: str) -> GalleryImageVersion:
        return self.azure_service.compute.gallery_image_versions.get(
            resource_group_name=self.sct_gallery_resource_group_name,
            gallery_name=self.sct_gallery_name,
            gallery_image_name=gallery_image_name,
            gallery_image_version_name=gallery_image_version_name,
        )

    def create_gallery_image_version(self,
                                     gallery_image_name: str,
                                     gallery_image_version_name: str,
                                     source_id: str,
                                     tags: Optional[dict] = None) -> None:
        self.azure_service.compute.gallery_image_versions.begin_create_or_update(
            resource_group_name=self.sct_gallery_resource_group_name,
            gallery_name=self.sct_gallery_name,
            gallery_image_name=gallery_image_name,
            gallery_image_version_name=gallery_image_version_name,
            gallery_image_version=self.common_parameters(location=self.sct_gallery_location, tags=tags) | {
                "storage_profile": {
                    "source": {
                        "id": source_id,
                    },
                },
            },
        ).wait()

    def append_target_region_to_image_version(self,
                                              gallery_image_name: str,
                                              gallery_image_version_name: str,
                                              region_name: str) -> None:
        gallery_image_version = self.get_gallery_image_version(
            gallery_image_name=gallery_image_name,
            gallery_image_version_name=gallery_image_version_name,
        )
        gallery_image_version.publishing_profile.target_regions.append(TargetRegion(name=region_name))
        self.azure_service.compute.gallery_image_versions.begin_update(
            resource_group_name=self.sct_gallery_resource_group_name,
            gallery_name=self.sct_gallery_name,
            gallery_image_name=gallery_image_name,
            gallery_image_version_name=gallery_image_version_name,
            gallery_image_version=gallery_image_version,
        ).wait()

    def configure(self, tags: Optional[dict] = None) -> None:
        LOGGER.info("Configuring `%s' region...", self.region_name)
        self.create_sct_resource_group(tags=tags)
        self.create_sct_network_security_group(tags=tags)
        self.create_sct_virtual_network(tags=tags)
        self.create_sct_subnet()
        LOGGER.info("Region `%s' configured successfully.", self.region_name)

        if self.location != self.sct_gallery_location:
            LOGGER.info("Going to configure `%s' region for Shared Image Gallery...", self.SCT_GALLERY_REGION)
            type(self)(region_name=self.SCT_GALLERY_REGION).configure(tags=tags)
        else:
            self.create_sct_gallery(tags=tags)
