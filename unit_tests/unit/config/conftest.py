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
# Copyright (c) 2020 ScyllaDB

import logging

import pytest

from sdcm import sct_config
from sdcm.utils.common import get_latest_scylla_release


def latest_scylla_release(product="scylla"):
    """Return the latest released version of product, without verifying its download URL."""
    return get_latest_scylla_release(product=product, verify=False)


@pytest.fixture(scope="module")
def monkeymodule():
    """Fixture that provides a monkeypatching with module scope."""
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(scope="module")
def conf(monkeymodule):
    """SCTConfiguration loaded for the AWS backend from the minimal test case."""
    monkeymodule.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeymodule.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeymodule.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    monkeymodule.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    yield sct_config.SCTConfiguration()


@pytest.fixture(autouse=True)
def fixture_env(monkeypatch):
    """Set the docker-backend SCT env and quiet the cloud SDK loggers for every config test."""
    logging.basicConfig(level=logging.ERROR)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger("anyconfig").setLevel(logging.ERROR)

    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", latest_scylla_release())
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")


@pytest.fixture(scope="module")
def help_yaml():
    """YAML dump of the SCT configuration help, one entry per option."""
    return sct_config.SCTConfiguration.dump_help_config_yaml()


@pytest.fixture(scope="module")
def help_markdown():
    """Markdown dump of the SCT configuration help, one section per option."""
    return sct_config.SCTConfiguration.dump_help_config_markdown()
