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
from dataclasses import dataclass

from sdcm.provision.user_data import UserDataObject
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_provision.common.types import NodeTypeType
from sdcm.test_config import TestConfig


@dataclass
class SctUserDataObject(UserDataObject):
    test_config: TestConfig
    sct_config: SCTConfiguration
    instance_name: str
    node_type: NodeTypeType
