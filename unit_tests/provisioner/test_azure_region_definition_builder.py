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
from sdcm.sct_provision import region_definition_builder
from sdcm.test_config import TestConfig


def test_can_create_basic_scylla_instance_definition_from_sct_config():
    """Test for azure_region_definition_builder"""
    EnvConfig = namedtuple('EnvConfig',
                           ["SCT_CLUSTER_BACKEND", "SCT_TEST_ID", "SCT_CONFIG_FILES", "SCT_AZURE_REGION_NAME",
                            "SCT_N_DB_NODES", "SCT_USER_PREFIX",
                            "SCT_AZURE_IMAGE_DB", "SCT_N_LOADERS", "SCT_N_MONITORS_NODES"])

    test_id = "3923f974-bf0e-4c3c-9f52-3f6473b8a0b6"
    test_config = TestConfig()
    test_config.set_test_id_only(test_id)
    env_config = EnvConfig(
        SCT_CLUSTER_BACKEND="azure",
        SCT_TEST_ID=test_config.test_id(),
        SCT_CONFIG_FILES=f'["{Path(__file__).parent.absolute()}/azure_default_config.yaml"]',
        SCT_AZURE_REGION_NAME="['eastus', 'easteu']",
        SCT_N_DB_NODES="3 1",
        SCT_USER_PREFIX="unit",
        SCT_AZURE_IMAGE_DB="/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/scylla-images/providers/"
                           "Microsoft.Compute/images/scylla-5.2.0-dev-x86_64-2022-08-22T04-18-36Z",
        SCT_N_LOADERS="2 0",
        SCT_N_MONITORS_NODES="1"
    )

    os.environ.update(env_config._asdict())
    config = SCTConfiguration()
    tags = TestConfig.common_tags()
    # TODO: switch to  get_azure_ssh_key_pair()
    #  temporary using gce keypair, until replacing keys in jenkins, and all backend would be using same key (including runners)
    ssh_key = KeyStore().get_gce_ssh_key_pair()
    prefix = config.get('user_prefix')
    builder = region_definition_builder.get_builder(params=config, test_config=test_config)
    region_definitions = builder.build_all_region_definitions()

    instance_definition = InstanceDefinition(name=f"{prefix}-db-node-{test_config.test_id()[:8]}-eastus-1",
                                             image_id=env_config.SCT_AZURE_IMAGE_DB,
                                             type="Standard_L8s_v3", user_name="scyllaadm", root_disk_size=30,
                                             tags=tags | {"NodeType": "scylla-db", "keep_action": "terminate",
                                                          'NodeIndex': '1', "TestId": test_config.test_id()},
                                             ssh_key=ssh_key)
    assert len(region_definitions) == 2
    actual_region_definition = region_definitions[0]

    assert actual_region_definition.test_id == test_config.test_id()
    assert actual_region_definition.backend == "azure"
    assert actual_region_definition.region == "eastus"
    # ignoring user_data in this validation
    actual_region_definition.definitions[0].user_data = instance_definition.user_data
    # ssh_key is not shown, if actual looks the same as expected possibly ssh_key differ
    assert instance_definition == actual_region_definition.definitions[0]
