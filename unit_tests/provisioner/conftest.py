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
import uuid
from collections import namedtuple
from pathlib import Path

import pytest

from sdcm.utils.sct_cmd_helpers import get_test_config
from sdcm.sct_config import SCTConfiguration
from sdcm.test_config import TestConfig
from sdcm.utils.azure_utils import AzureService
from unit_tests.provisioner.fake_azure_service import FakeAzureService


@pytest.fixture(scope="session")
def azure_service(tmp_path_factory) -> AzureService:
    run_on_real_azure = False   # make it True to test with real Azure
    if run_on_real_azure:
        # When true this becomes e2e test - takes around 8 minutes (2m provisioning, 6 min cleanup with wait=True)
        return AzureService()
    resources_path = tmp_path_factory.mktemp("azure-provision")
    # print(resources_path)
    return FakeAzureService(resources_path)


@pytest.fixture
def fallback_on_demand():
    os.environ["SCT_INSTANCE_PROVISION"] = "spot"
    os.environ["SCT_INSTANCE_PROVISION_FALLBACK_ON_DEMAND"] = "true"
    os.environ["BUILD_TAG"] = "FailSpotDB"


@pytest.fixture
def params():
    EnvConfig = namedtuple('EnvConfig',
                           ["SCT_CLUSTER_BACKEND", "SCT_TEST_ID", "SCT_CONFIG_FILES", "SCT_AZURE_REGION_NAME",
                            "SCT_N_DB_NODES",
                            "SCT_AZURE_IMAGE_DB", "SCT_N_LOADERS", "SCT_N_MONITORS_NODES", "SCT_AVAILABILITY_ZONE",
                            "SCT_IP_SSH_CONNECTIONS"])
    env_config = EnvConfig(
        SCT_CLUSTER_BACKEND="azure",
        SCT_TEST_ID=f"{str(uuid.uuid4())}",
        SCT_CONFIG_FILES=f'["{Path(__file__).parent.absolute()}/azure_default_config.yaml"]',
        SCT_AZURE_REGION_NAME="['eastus', 'easteu']",
        SCT_N_DB_NODES="3 1",
        SCT_AZURE_IMAGE_DB="/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/"
                           "scylla-images/providers/Microsoft.Compute/images/scylla-2024.1.9-x86_64-2024-08-30T04-49-46",
        SCT_N_LOADERS="2 0",
        SCT_N_MONITORS_NODES="1",
        SCT_AVAILABILITY_ZONE="a",
        SCT_IP_SSH_CONNECTIONS="public",
    )
    os.environ.update(env_config._asdict())
    return SCTConfiguration()


@pytest.fixture
def test_config(params):
    config = get_test_config()
    config.set_test_id("12345678-87654321")
    TestConfig.SYSLOGNG_ADDRESS = ("localhost", 12345)
    return config
