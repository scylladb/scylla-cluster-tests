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
from typing import Union, List, Optional, Tuple

from pydantic import Field

from sdcm.cluster import UserRemoteCredentials
from sdcm.provision.aws.instance_parameters import AWSDiskMapping, AWSPlacementInfo, AWSDiskMappingEbsInfo
from sdcm.provision.aws.instance_parameters_builder import AWSInstanceParamsBuilderBase
from sdcm.provision.common.user_data import UserDataBuilderBase
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.aws_utils import ec2_ami_get_root_device_name, get_ec2_network_configuration


class AWSInstanceParamsBuilder(AWSInstanceParamsBuilderBase, metaclass=abc.ABCMeta):
    params: Union[SCTConfiguration, dict] = Field(as_dict=False)
    region_id: int = Field(as_dict=False)
    user_data_raw: Union[str, UserDataBuilderBase] = Field(as_dict=False)
    availability_zone: int = 0
    placement_group: str = None

    _INSTANCE_TYPE_PARAM_NAME: str = None
    _IMAGE_ID_PARAM_NAME: str = None
    _ROOT_DISK_SIZE_PARAM_NAME: str = None
    _INSTANCE_PROFILE_PARAM_NAME: str = None

    @property
    def BlockDeviceMappings(self) -> List[AWSDiskMapping]:  # pylint: disable=invalid-name
        if not self.ImageId:
            return []
        device_mappings = []
        if self._root_device_size:
            device_mappings.append(
                AWSDiskMapping(
                    DeviceName=self._root_device_name,
                    Ebs=AWSDiskMappingEbsInfo(
                        VolumeSize=self._root_device_size,
                        VolumeType="gp3",
                    ),
                )
            )
        return device_mappings

    @property
    def ImageId(self) -> Optional[str]:  # pylint: disable=invalid-name
        if not self._image_ids:
            return None
        return self._image_ids[self.region_id]

    @property
    def KeyName(self) -> str:  # pylint: disable=invalid-name
        return self._credentials[self.region_id].key_pair_name

    @property
    def NetworkInterfaces(self) -> List[dict]:  # pylint: disable=invalid-name
        output = [{"DeviceIndex": 0, **self._network_interface_params}]
        if self.params.get("extra_network_interface"):
            output.append({"DeviceIndex": 1, **self._network_interface_params})
        return output

    @property
    def IamInstanceProfile(self):  # pylint: disable=invalid-name
        if profile := self.params.get(self._INSTANCE_PROFILE_PARAM_NAME):
            return {"Name": profile}
        return None

    @property
    def InstanceType(self) -> str:  # pylint: disable=invalid-name
        return self.params.get(self._INSTANCE_TYPE_PARAM_NAME)

    @property
    def Placement(self) -> Optional[AWSPlacementInfo]:  # pylint: disable=invalid-name
        return AWSPlacementInfo(
            AvailabilityZone=self._region_name + self._availability_zones[self.availability_zone],
            GroupName=self.placement_group,
        )

    @property
    def UserData(self) -> Optional[str]:  # pylint: disable=invalid-name
        if not self.user_data_raw:
            return None
        if isinstance(self.user_data_raw, UserDataBuilderBase):
            return self.user_data_raw.to_string()
        return self.user_data_raw

    @cached_property
    def _root_device_name(self):
        return ec2_ami_get_root_device_name(image_id=self.ImageId, region_name=self._region_name)

    @property
    def _root_device_size(self):
        return self.params.get(self._ROOT_DISK_SIZE_PARAM_NAME)

    @cached_property
    def _image_ids(self) -> List[str]:
        return self.params.get(self._IMAGE_ID_PARAM_NAME).split()

    @cached_property
    def _availability_zones(self) -> List[str]:
        return self.params.get("availability_zone").split(",")

    @cached_property
    def _ec2_network_configuration(self) -> Tuple[List[str], List[List[str]]]:
        return get_ec2_network_configuration(
            regions=self.params.region_names, availability_zones=self._availability_zones, params=self.params
        )

    @cached_property
    def _ec2_subnet_ids(self) -> List[str]:
        return self._ec2_network_configuration[1]

    @cached_property
    def _ec2_security_group_ids(self) -> List[str]:
        return self._ec2_network_configuration[0]

    @property
    def _network_interface_params(self):
        return {
            "SubnetId": self._ec2_subnet_ids[self.region_id][self.availability_zone],  # pylint: disable=invalid-sequence-index
            "Groups": self._ec2_security_group_ids[self.region_id],  # pylint: disable=invalid-sequence-index
        }

    @property
    def _credentials(self):
        user_credentials = self.params.get("user_credentials_path")
        return [UserRemoteCredentials(key_file=user_credentials) for _ in self.params.region_names]

    @property
    def _region_name(self) -> str:
        return self.params.region_names[self.region_id]


class ScyllaInstanceParamsBuilder(AWSInstanceParamsBuilder):
    _INSTANCE_TYPE_PARAM_NAME = "instance_type_db"
    _IMAGE_ID_PARAM_NAME = "ami_id_db_scylla"
    _ROOT_DISK_SIZE_PARAM_NAME = "root_disk_size_db"
    _INSTANCE_PROFILE_PARAM_NAME = "aws_instance_profile_name_db"

    @property
    def BlockDeviceMappings(self) -> List[AWSDiskMapping]:
        device_mappings = super().BlockDeviceMappings
        volume_type = self.params.get("data_volume_disk_type")
        disk_num = self.params.get("data_volume_disk_num")
        if disk_num == 0:
            return device_mappings
        additional_volumes_ebs_info = AWSDiskMappingEbsInfo(
            DeleteOnTermination=True,
            VolumeSize=self.params.get("data_volume_disk_size"),
            VolumeType=volume_type,
            Iops=self.params.get("data_volume_disk_iops") if volume_type in ["io1", "io2", "gp3"] else None,
        )
        for disk_char in "fghijklmnop"[:disk_num]:
            device_mappings.append(
                AWSDiskMapping(
                    DeviceName=f"/dev/xvd{disk_char}",
                    Ebs=additional_volumes_ebs_info,
                )
            )
        return device_mappings


class OracleScyllaInstanceParamsBuilder(ScyllaInstanceParamsBuilder):
    _INSTANCE_TYPE_PARAM_NAME = "instance_type_db_oracle"
    _IMAGE_ID_PARAM_NAME = "ami_id_db_oracle"
    _ROOT_DISK_SIZE_PARAM_NAME = "root_disk_size_db"


# Since AWS Loaders is being built on scylla image we need to base it from ScyllaInstanceParams
class LoaderInstanceParamsBuilder(AWSInstanceParamsBuilder):
    _INSTANCE_TYPE_PARAM_NAME = "instance_type_loader"
    _IMAGE_ID_PARAM_NAME = "ami_id_loader"
    _ROOT_DISK_SIZE_PARAM_NAME = "root_disk_size_loader"
    _INSTANCE_PROFILE_PARAM_NAME = "aws_instance_profile_name_loader"


class MonitorInstanceParamsBuilder(AWSInstanceParamsBuilder):
    _INSTANCE_TYPE_PARAM_NAME = "instance_type_monitor"
    _IMAGE_ID_PARAM_NAME = "ami_id_monitor"
    _ROOT_DISK_SIZE_PARAM_NAME = "root_disk_size_monitor"
