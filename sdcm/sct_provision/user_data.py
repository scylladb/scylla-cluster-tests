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

from typing import List, Type

from sdcm.sct_config import SCTConfiguration
from sdcm.sct_provision.instances_request_definition_builder import NodeTypeType
from sdcm.sct_provision.user_data_objects import SctUserDataObject
from sdcm.sct_provision.user_data_objects.scylla import ScyllaUserDataObject
from sdcm.sct_provision.user_data_objects.sshd import SshdUserDataObject
from sdcm.sct_provision.user_data_objects.syslog_ng import SyslogNgUserDataObject
from sdcm.test_config import TestConfig


def get_user_data_objects(test_config: TestConfig, sct_config: SCTConfiguration, instance_name: str, node_type: NodeTypeType
                          ) -> List[SctUserDataObject]:
    user_data_object_classes: List[Type[SctUserDataObject]] = [
        SyslogNgUserDataObject,
        SshdUserDataObject,
        ScyllaUserDataObject,
    ]
    user_data_objects = [
        klass(test_config=test_config, sct_config=sct_config, instance_name=instance_name, node_type=node_type)
        for klass in user_data_object_classes
    ]
    applicable_user_data_objects = [obj for obj in user_data_objects if obj.is_applicable]
    return applicable_user_data_objects
