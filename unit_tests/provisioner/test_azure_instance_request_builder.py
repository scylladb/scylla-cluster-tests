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
import os
from collections import namedtuple
from pathlib import Path


from sdcm.keystore import KeyStore
from sdcm.provision.provisioner import InstanceDefinition
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_provision import instances_request_builder
from sdcm.test_config import TestConfig


def test_can_create_basic_scylla_instance_definition_from_sct_config():
    """Test for azure_instance_request_builder"""
    EnvConfig = namedtuple('EnvConfig',
                           ["SCT_CLUSTER_BACKEND", "SCT_TEST_ID", "SCT_CONFIG_FILES", "SCT_AZURE_REGION_NAME",
                            "SCT_N_DB_NODES", "SCT_USER_PREFIX",
                            "SCT_AZURE_IMAGE_DB", "SCT_N_LOADERS", "SCT_N_MONITORS_NODES"])
    env_config = EnvConfig(
        SCT_CLUSTER_BACKEND="azure",
        SCT_TEST_ID="example_test_id",
        SCT_CONFIG_FILES=f'["{Path(__file__).parent.absolute()}/azure_default_config.yaml"]',
        SCT_AZURE_REGION_NAME="['eastus', 'easteu']",
        SCT_N_DB_NODES="3 1",
        SCT_USER_PREFIX="unit-test",
        SCT_AZURE_IMAGE_DB="some_image_id",
        SCT_N_LOADERS="2 0",
        SCT_N_MONITORS_NODES="1"
    )

    os.environ.update(env_config._asdict())
    config = SCTConfiguration()
    tags = TestConfig.common_tags()
    ssh_key = KeyStore().get_gce_ssh_key_pair()
    prefix = config.get('user_prefix')
    test_config = TestConfig()
    instance_requests = instances_request_builder.build(sct_config=config, test_config=test_config)

    definition = InstanceDefinition(name=f"{prefix}-db-node-eastus-1", image_id=env_config.SCT_AZURE_IMAGE_DB,
                                    type="Standard_L8s_v2", user_name="scyllaadm", root_disk_size=30,
                                    tags=tags | {"NodeType": "scylla-db", "keep_action": "", 'NodeIndex': '1'},
                                    ssh_key=ssh_key)
    assert len(instance_requests) == 2
    actual_request = instance_requests[0]

    assert actual_request.test_id == env_config.SCT_TEST_ID
    assert actual_request.backend == "azure"
    assert actual_request.region == "eastus"
    actual_request.definitions[0].user_data = definition.user_data  # ignoring user_data in this validation
    # ssh_key is not shown, if actual looks the same as expected possibly ssh_key differ
    assert definition == actual_request.definitions[0]
