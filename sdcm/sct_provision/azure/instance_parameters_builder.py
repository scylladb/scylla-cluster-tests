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

import abc
from functools import cached_property
from typing import Union, List, Optional

from azure.mgmt.compute.v2020_12_01.models import DiskCreateOption
from azure.mgmt.compute.v2021_07_01.models import HardwareProfile, StorageProfile, OSProfile, OSDisk, CachingTypes, \
    ManagedDiskParameters, StorageAccountTypes, VirtualMachineNetworkInterfaceConfiguration, \
    VirtualMachineNetworkInterfaceDnsSettingsConfiguration, VirtualMachineNetworkInterfaceIPConfiguration, \
    VirtualMachinePublicIPAddressConfiguration, IPVersions, PublicIPAllocationMethod, LinuxConfiguration, \
    SshConfiguration, SshPublicKey, OperatingSystemStateTypes
from azure.mgmt.network.v2021_02_01.models import NetworkProfile, DeleteOptions, SubResource, PublicIPAddressSku, \
    PublicIPAddressSkuName, PublicIPAddressSkuTier, VirtualNetwork, NetworkSecurityGroup
from pydantic import Field

from sdcm.cluster import UserRemoteCredentials
from sdcm.provision.azure.instance_parameters_builder import AzureInstanceParamsBuilderBase
from sdcm.provision.azure.utils import AzureMixing
from sdcm.provision.common.user_data import UserDataBuilderBase
from sdcm.sct_config import SCTConfiguration


class AzureInstanceParamsBuilder(AzureInstanceParamsBuilderBase, AzureMixing, metaclass=abc.ABCMeta):
    params: Union[SCTConfiguration, dict] = Field(as_dict=False)
    region_id: int = Field(as_dict=False)
    user_data_raw: Union[str, UserDataBuilderBase] = Field(as_dict=False)
    os_state: OperatingSystemStateTypes = OperatingSystemStateTypes.SPECIALIZED

    _INSTANCE_TYPE_PARAM_NAME: str = None
    _IMAGE_ID_PARAM_NAME: str = None
    _ROOT_DISK_SIZE_PARAM_NAME: str = None
    _RESOURCE_NAME_PREFIX: str = 'SCT'

    @property
    def hardware_profile(self) -> HardwareProfile:  # pylint: disable=invalid-name
        return HardwareProfile(vm_size=self._instance_type)

    @property
    def location(self) -> str:  # pylint: disable=invalid-name
        return self._region_name

    @property
    def storage_profile(self) -> StorageProfile:  # pylint: disable=invalid-name
        return StorageProfile(
            image_reference=self._image_id,
            disk_size_gb=self._root_device_size,
            os_disk=OSDisk(
                name='os-disk',
                os_type='linux',
                caching=CachingTypes.READ_WRITE,
                create_option=DiskCreateOption.FROM_IMAGE,
                managed_disk=ManagedDiskParameters(
                    storage_account_type=StorageAccountTypes.PREMIUM_LRS
                ),
            ),
            data_disks=None  # TODO: Get it properly from sct_config
        )

    @property
    def network_profile(self) -> NetworkProfile:  # pylint: disable=invalid-name
        interfaces = [
            self._network_interface(name='eth0', primary=True, ip_version=self._ipv6_version),
        ]
        if self.params.get('extra_network_interface'):
            interfaces.append(self._network_interface(name='eth1', primary=False, ip_version=self._ipv6_version))
        return NetworkProfile(
            network_interface_configurations=interfaces,
        )

    @property
    def os_profile(self) -> OSProfile:  # pylint: disable=invalid-name
        return OSProfile(
            # computer_name=computer_name or vm_name.replace(".", "-"),
            admin_username='scylla-test',
            # admin_password=binascii.hexlify(os.urandom(20)).decode(),
            custom_data=self.user_data_raw,
            linux_configuration=LinuxConfiguration(
                provision_vm_agent=False,
                ssh=SshConfiguration(
                    public_keys=[
                        SshPublicKey(
                            path='/home/scylla-test/.ssh/authorized_keys',
                            key_data='',
                        )
                    ]
                ),
                disable_password_authentication=True,
            )
        )

    # @property
    # def KeyName(self) -> str:  # pylint: disable=invalid-name
    #     return self._credentials[self.region_id].key_pair_name
    #
    # @property
    # def UserData(self) -> Optional[str]:  # pylint: disable=invalid-name
    #     if not self.user_data_raw:
    #         return None
    #     if isinstance(self.user_data_raw, UserDataBuilderBase):
    #         user_data = self.user_data_raw.to_string()
    #     else:
    #         user_data = self.user_data_raw
    #     return base64.b64encode(user_data.encode('utf-8')).decode("ascii")

    # @cached_property
    # def _root_device_name(self):
    #     return ec2_ami_get_root_device_name(image_id=self.ImageId, region=self._region_name)

    @property
    def _ipv6_version(self) -> IPVersions:
        return IPVersions.I_PV6 if self.params.get('ip_ssh_connections') == 'ipv6' else IPVersions.I_PV4

    @property
    def _image_id(self) -> Optional[str]:  # pylint: disable=invalid-name
        if not self._image_ids:
            return None
        return self._image_ids[self.region_id]

    @property
    def _instance_type(self) -> str:  # pylint: disable=invalid-name
        return self.params.get(self._INSTANCE_TYPE_PARAM_NAME)

    @property
    def _root_device_size(self):
        return self.params.get(self._ROOT_DISK_SIZE_PARAM_NAME)

    @cached_property
    def _image_ids(self) -> List[str]:
        print(self._IMAGE_ID_PARAM_NAME)
        return self.params.get(self._IMAGE_ID_PARAM_NAME).split()

    @cached_property
    def _availability_zones(self) -> List[str]:
        return self.params.get('availability_zone').split(',')

    @cached_property
    def _resource_group_name(self):
        return f"{self._RESOURCE_NAME_PREFIX}-{self._region_name}"

    @cached_property
    def _virtual_network_name(self) -> str:
        return f"{self._resource_group_name}-vnet"

    @cached_property
    def _network_security_group_name(self) -> str:
        return f"{self._resource_group_name}-nsg"

    @cached_property
    def _networks(self) -> list[VirtualNetwork]:
        return [
            self._network.virtual_networks.get(
                resource_group_name=self._resource_group_name,
                virtual_network_name=self._virtual_network_name,
            )
        ]

    @cached_property
    def _networks_security_groups(self) -> list[NetworkSecurityGroup]:
        return [
            self._network.network_security_groups.get(
                resource_group_name=self._resource_group_name,
                network_security_group_name=self._network_security_group_name,
            )
        ]

    @cached_property
    def _subnet_ids(self) -> List[str]:
        return [network.id for network in self._networks]

    @cached_property
    def _ec2_security_group_ids(self) -> List[str]:
        return [sec_group.id for sec_group in self._networks_security_groups]

    def _network_interface(self, name: str, primary: bool, ip_version: IPVersions):
        return VirtualMachineNetworkInterfaceConfiguration(
            name=name,
            primary=primary,
            delete_option=DeleteOptions.DELETE,
            enable_accelerated_networking=True,
            enable_ip_forwarding=True,
            network_security_group=self._ec2_security_group_ids[self.region_id],
            ip_configurations=[
                VirtualMachineNetworkInterfaceIPConfiguration(
                    name=f"{name}-network-cfg",
                    subnet=SubResource(id=self._subnet_ids[self.region_id]),
                    primary=primary,
                    public_ip_address_configuration=VirtualMachinePublicIPAddressConfiguration(
                        name=f"{name}-ip-cfg",
                        sku=PublicIPAddressSku(
                            name=PublicIPAddressSkuName.BASIC,
                            tier=PublicIPAddressSkuTier.GLOBAL_ENUM,
                        ),
                        idle_timeout_in_minutes=180,
                        delete_option=DeleteOptions.DELETE,
                        public_ip_address_version=ip_version,
                        public_ip_allocation_method=PublicIPAllocationMethod.DYNAMIC,
                    ),
                    private_ip_address_version=ip_version,
                )
            ]
        )

    @property
    def _credentials(self):
        user_credentials = self.params.get('user_credentials_path')
        return [UserRemoteCredentials(key_file=user_credentials) for _ in self.params.region_names]

    @property
    def _region_name(self) -> str:
        return self.params.region_names[self.region_id]


class ScyllaInstanceParamsBuilder(AzureInstanceParamsBuilder):
    _INSTANCE_TYPE_PARAM_NAME = 'instance_type_db'
    _IMAGE_ID_PARAM_NAME = 'azure_image_db'
    _ROOT_DISK_SIZE_PARAM_NAME = 'root_disk_size_db'

    # @property
    # def BlockDeviceMappings(self) -> List[AWSDiskMapping]:
    #     device_mappings = super().BlockDeviceMappings
    #     volume_type = self.params.get('data_volume_disk_type')
    #     disk_num = self.params.get('data_volume_disk_num')
    #     if disk_num == 0:
    #         return device_mappings
    #     additional_volumes_ebs_info = AWSDiskMappingEbsInfo(
    #         DeleteOnTermination=True,
    #         VolumeSize=self.params.get('data_volume_disk_size'),
    #         VolumeType=volume_type,
    #         Iops=self.params.get('data_volume_disk_iops') if volume_type in ['io1', 'io2', 'gp3'] else None,
    #     )
    #     for disk_char in "fghijklmnop"[:disk_num]:
    #         device_mappings.append(
    #             AWSDiskMapping(
    #                 DeviceName=f"/dev/xvd{disk_char}",
    #                 Ebs=additional_volumes_ebs_info,
    #             )
    #         )
    #     return device_mappings


class OracleScyllaInstanceParamsBuilder(ScyllaInstanceParamsBuilder):
    _INSTANCE_TYPE_PARAM_NAME = 'instance_type_db'
    _IMAGE_ID_PARAM_NAME = 'azure_image_db_oracle'
    _ROOT_DISK_SIZE_PARAM_NAME = 'root_disk_size_db'


class LoaderInstanceParamsBuilder(AzureInstanceParamsBuilder):
    _INSTANCE_TYPE_PARAM_NAME = 'instance_type_loader'
    _IMAGE_ID_PARAM_NAME = 'azure_image_loader'
    _ROOT_DISK_SIZE_PARAM_NAME = 'root_disk_size_loader'


class MonitorInstanceParamsBuilder(AzureInstanceParamsBuilder):
    _INSTANCE_TYPE_PARAM_NAME = 'instance_type_monitor'
    _IMAGE_ID_PARAM_NAME = 'azure_image_monitor'
    _ROOT_DISK_SIZE_PARAM_NAME = 'root_disk_size_monitor'
