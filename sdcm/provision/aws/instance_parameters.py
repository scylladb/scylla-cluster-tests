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
from typing import List, Optional, Literal

from pydantic import BaseModel, validator

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
    Throughput: str = None
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
    # pylint: disable=invalid-name
    ImageId: str
    KeyName: str
    InstanceType: str
    UserData: str
    NetworkInterfaces: List[AWSNetworkInterfaces] = None
    IamInstanceProfile: Optional[AWSInstanceProfile] = None
    BlockDeviceMappings: List[AWSDiskMapping] = None
    Placement: AWSPlacementInfo = None
    SubnetId: str = None
    SecurityGroups: List[str] = None
    AddressingType: str = None
    EbsOptimized: bool = None

    @validator('UserData')
    def validate_user_data_raw(cls, value: str):  # pylint: disable=no-self-argument,no-self-use
        try:
            assert base64.b64encode(base64.b64decode(value)).decode('ascii') == value
        except AssertionError:
            raise
        except Exception:  # pylint: disable=broad-except
            assert False, 'UserData is not base64 formatted'
        return value
