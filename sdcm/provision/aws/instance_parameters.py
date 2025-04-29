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

import base64
from typing import List, Optional, Literal, Union

from pydantic import BaseModel

from sdcm.provision.common.provisioner import InstanceParamsBase


class AWSNetworkInterfaces(BaseModel):
    DeviceIndex: int
    SubnetId: str
    Groups: List[str]


class AWSInstanceProfile(BaseModel):
    Name: str = None
    Arn: str = None


class AWSDiskMappingEbsInfo(BaseModel):
    VolumeType: Literal['standard', 'io1', 'io2', 'gp2', 'sc1', 'st1', 'gp3']
    VolumeSize: int
    VirtualName: str = None
    DeleteOnTermination: bool = None
    Iops: int = None
    SnapshotId: str = None
    KmsKeyId: str = None
    Throughput: int = None
    OutpostArn: str = None
    Encrypted: bool = None


class AWSDiskMapping(BaseModel):
    DeviceName: str
    Ebs: AWSDiskMappingEbsInfo


class AWSPlacementInfo(BaseModel):
    AvailabilityZone: str
    GroupName: str = None
    Tenancy: Literal['default', 'dedicated', 'host'] = 'default'


class AWSInstanceParams(InstanceParamsBase):

    ImageId: str
    KeyName: str
    InstanceType: str
    UserData: str = None
    NetworkInterfaces: List[AWSNetworkInterfaces] = None
    IamInstanceProfile: Optional[AWSInstanceProfile] = None
    BlockDeviceMappings: List[AWSDiskMapping] = None
    Placement: AWSPlacementInfo = None
    SubnetId: str = None
    SecurityGroups: List[str] = None
    AddressingType: str = None
    EbsOptimized: bool = None

    def dict(
        self,
        *,
        include: Union['AbstractSetIntStr', 'MappingIntStrAny'] = None,  # noqa: F821
        exclude: Union['AbstractSetIntStr', 'MappingIntStrAny'] = None,  # noqa: F821
        by_alias: bool = False,
        skip_defaults: bool = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        encode_user_data: bool = False
    ) -> 'DictStrAny':  # noqa: F821
        dict_data = super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )
        if encode_user_data:
            if user_data := dict_data.get('UserData'):
                dict_data['UserData'] = base64.b64encode(user_data.encode('ascii')).decode("ascii")
        return dict_data
