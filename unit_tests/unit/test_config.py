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
import unittest.mock
from collections import namedtuple

import pytest

from sdcm import sct_config
from sdcm.test_config import TestConfig
from sdcm.utils.common import get_latest_scylla_release


def _get_latest_scylla_release(product="scylla"):
    return get_latest_scylla_release(product=product, verify=False)


@pytest.fixture(scope="module")
def monkeymodule():
    """Fixture that provides a monkeypatching with module scope."""
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(name="conf", scope="module")
def fixture_conf(monkeymodule):
    monkeymodule.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeymodule.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeymodule.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    monkeymodule.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    yield sct_config.SCTConfiguration()


@pytest.fixture(autouse=True)
def fixture_env(monkeypatch):
    logging.basicConfig(level=logging.ERROR)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger("anyconfig").setLevel(logging.ERROR)

    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", _get_latest_scylla_release())
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")


def test_01_dump_config(conf):
    logging.debug(conf.dump_config())


def test_02_verify_config(conf):
    conf.verify_configuration()
    conf.check_required_files()


def test_03_dump_help_config_yaml(conf):
    logging.debug(conf.dump_help_config_yaml())


def test_03_dump_help_config_markdown(conf):
    logging.debug(conf.dump_help_config_markdown())


def test_05_docker(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2025.1.0")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert "docker_image" in conf.dump_config()
    assert conf.docker_image == "scylladb/scylla"


def test_06a_docker_latest_no_loader(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "latest")
    monkeypatch.setenv("SCT_N_LOADERS", "0")
    docker_tag_after_processing = "fake_specific_docker_tag"

    with unittest.mock.patch.object(
        sct_config, "get_specific_tag_of_docker_image", return_value=docker_tag_after_processing
    ):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
    assert conf.scylla_version != "latest"


def test_07_baremetal_exception(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "baremetal")
    conf = sct_config.SCTConfiguration()

    with pytest.raises(AssertionError):
        conf.verify_configuration()


def test_08_baremetal(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "baremetal")
    monkeypatch.setenv("SCT_DB_NODES_PRIVATE_IP", '["1.2.3.4", "1.2.3.5"]')
    monkeypatch.setenv("SCT_DB_NODES_PUBLIC_IP", '["1.2.3.4", "1.2.3.5"]')
    monkeypatch.setenv("SCT_USE_PREINSTALLED_SCYLLA", "true")
    monkeypatch.setenv("SCT_S3_BAREMETAL_CONFIG", "some_config")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert "db_nodes_private_ip" in conf.dump_config()
    assert conf.db_nodes_private_ip == ["1.2.3.4", "1.2.3.5"]


def test_09_unknown_configure(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/unknown_param_in_config.yaml")
    with pytest.raises(ValueError) as exc:
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
    assert exc.value.args[0] == "Unknown configuration key='WTF'"


def test_09_unknown_env(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_WHAT_IS_THAT", "just_made_this_up")
    monkeypatch.setenv("SCT_WHAT_IS_THAT_2", "what is this ?")
    conf = sct_config.SCTConfiguration()

    with pytest.raises(ValueError) as context:
        conf.verify_configuration()
    msg = str(context.value)
    assert "SCT_WHAT_IS_THAT_2=what is this ?" in msg
    assert "SCT_WHAT_IS_THAT=just_made_this_up" in msg


def test_12_scylla_version_repo_ubuntu(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_SCYLLA_LINUX_DISTRO", "ubuntu-xenial")
    monkeypatch.setenv("SCT_SCYLLA_LINUX_DISTRO_LOADER", "ubuntu-xenial")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "3.0.3")
    monkeypatch.setenv(
        "SCT_GCE_IMAGE_DB", "https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7"
    )
    expected_repo = "https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-3.0-xenial.list"
    with (
        unittest.mock.patch.object(sct_config, "get_branch_version", return_value="4.7.dev", clear=True),
        unittest.mock.patch.object(sct_config, "find_scylla_repo", return_value=expected_repo, clear=True),
    ):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
    assert "scylla_repo" in conf.dump_config()
    assert conf.scylla_repo == expected_repo


def test_12_scylla_version_repo_ubuntu_loader_centos(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_SCYLLA_LINUX_DISTRO", "ubuntu-xenial")
    monkeypatch.setenv("SCT_SCYLLA_LINUX_DISTRO_LOADER", "centos")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "3.0.3")
    monkeypatch.setenv(
        "SCT_GCE_IMAGE_DB", "https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7"
    )
    expected_repo = "https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-3.0-xenial.list"
    with (
        unittest.mock.patch.object(sct_config, "get_branch_version", return_value="4.7.dev", clear=True),
        unittest.mock.patch.object(sct_config, "find_scylla_repo", return_value=expected_repo, clear=True),
    ):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
    assert "scylla_repo" in conf.dump_config()
    assert conf.scylla_repo == expected_repo


def test_12_k8s_scylla_version_ubuntu_loader_centos(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "k8s-local-kind")
    monkeypatch.setenv("SCT_SCYLLA_LINUX_DISTRO", "ubuntu-xenial")
    monkeypatch.setenv("SCT_SCYLLA_LINUX_DISTRO_LOADER", "centos")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert "scylla_repo" in conf.dump_config()
    assert not conf.scylla_repo


def test_14_check_rackaware_config_false_loader(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_RACK_AWARE_LOADER", "false")
    monkeypatch.setenv("SCT_SIMULATED_RACKS", "0")
    monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1")
    monkeypatch.setenv("SCT_AVAILABILITY_ZONE", "a,b")
    monkeypatch.setenv("SCT_N_DB_NODES", "2")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv(
        "SCT_TEARDOWN_VALIDATORS",
        """rackaware:
             enabled: true
        """,
    )

    conf = sct_config.SCTConfiguration()
    with pytest.raises(ValueError, match="rack_aware_loader' must be set to True for rackaware validator."):
        conf.verify_configuration()


def test_14_check_rackaware_config_one_racks(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_RACK_AWARE_LOADER", "true")
    monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1")
    monkeypatch.setenv("SCT_AVAILABILITY_ZONE", "a")
    monkeypatch.setenv("SCT_SIMULATED_RACKS", "0")
    monkeypatch.setenv("SCT_N_DB_NODES", "2")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv(
        "SCT_TEARDOWN_VALIDATORS",
        """rackaware:
             enabled: true
        """,
    )

    conf = sct_config.SCTConfiguration()
    with pytest.raises(
        ValueError, match="Rack-aware validation can only be performed in multi-availability zone or multi-"
    ):
        conf.verify_configuration()


def test_14_check_rackaware_config_no_rack_without_loader(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_RACK_AWARE_LOADER", "true")
    monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1 eu-west-2")
    monkeypatch.setenv("SCT_AVAILABILITY_ZONE", "a,b")
    monkeypatch.setenv("SCT_SIMULATED_RACKS", "0")
    monkeypatch.setenv("SCT_N_DB_NODES", "2 2")
    monkeypatch.setenv("SCT_N_LOADERS", "2 2")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy ami-dummy2")
    monkeypatch.setenv("SCT_AMI_ID_LOADER", "ami-loader1 ami-loader2")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv(
        "SCT_TEARDOWN_VALIDATORS",
        """rackaware:
             enabled: true
        """,
    )

    conf = sct_config.SCTConfiguration()
    with pytest.raises(ValueError, match="Rack-aware validation requires zones without loaders."):
        conf.verify_configuration()


def test_14_check_rackaware_config_multi_az(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_RACK_AWARE_LOADER", "true")
    monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1")
    monkeypatch.setenv("SCT_AVAILABILITY_ZONE", "a,b")
    monkeypatch.setenv("SCT_N_DB_NODES", "2")
    monkeypatch.setenv("SCT_N_LOADERS", "1")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv(
        "SCT_TEARDOWN_VALIDATORS",
        """rackaware:
             enabled: true
        """,
    )

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_14_check_rackaware_config_multi_region(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_RACK_AWARE_LOADER", "true")
    monkeypatch.setenv("SCT_REGION_NAME", '["eu-west-1", "us-east-1"]')
    monkeypatch.setenv("SCT_N_DB_NODES", "2 2")
    monkeypatch.setenv("SCT_N_LOADERS", "1 0")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy ami-dummy2")
    monkeypatch.setenv("SCT_AMI_ID_LOADER", "ami-loader1 ami-loader2")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv(
        "SCT_TEARDOWN_VALIDATORS",
        """rackaware:
             enabled: true
        """,
    )

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_14_check_rackaware_config_multi_az_and_region(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_RACK_AWARE_LOADER", "true")
    monkeypatch.setenv("SCT_REGION_NAME", '["eu-west-1", "us-east-1"]')
    monkeypatch.setenv("SCT_AVAILABILITY_ZONE", "a,b")
    monkeypatch.setenv("SCT_N_DB_NODES", "2 2")
    monkeypatch.setenv("SCT_N_LOADERS", "1 1")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy ami-dummy2")
    monkeypatch.setenv("SCT_AMI_ID_LOADER", "ami-loader1 ami-loader2")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv(
        "SCT_TEARDOWN_VALIDATORS",
        """rackaware:
             enabled: true
        """,
    )

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_conf_check_required_files_negative(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/stress_cmd_with_bad_profile.yaml")
    monkeypatch.setenv(
        "SCT_STRESS_CMD",
        """cassandra-stress user profile=/tmp/1232123123123123123.yaml ops'(insert=100)'\
        no-warmup cl=ONE duration=10m -mode cql3 native -rate threads=3000 -errors ignore""",
    )
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    try:
        conf.check_required_files()
    except ValueError as exc:
        assert (
            str(exc) == "Stress command parameter 'stress_cmd' contains profile '/tmp/1232123123123123123.yaml' that"
            " does not exists under data_dir/"
        )


def test_15_new_scylla_repo(monkeypatch):
    centos_repo = (
        "https://s3.amazonaws.com/downloads.scylladb.com/enterprise/rpm/unstable/centos/"
        "9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7ef592edaf15e028faf3b7f695f39ebc1"
        "-525a0255f73d454f8f97f32b8bdd71c8dec35d3d-a6b2b2355c666b1893f702a587287da978aeec22/71/scylla"
        ".repo"
    )

    monkeypatch.delenv("SCT_SCYLLA_VERSION", raising=False)
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_SCYLLA_REPO", centos_repo)
    monkeypatch.setenv("SCT_NEW_SCYLLA_REPO", centos_repo)
    monkeypatch.setenv("SCT_USER_PREFIX", "testing")
    monkeypatch.setenv(
        "SCT_GCE_IMAGE_DB", "https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7"
    )

    with unittest.mock.patch.object(sct_config, "get_branch_version", return_value="2019.1.1", clear=True):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf._get_target_upgrade_version()
    assert conf.target_upgrade_version == "2019.1.1"


def test_15a_new_scylla_repo_by_scylla_version(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "master:latest")
    monkeypatch.setenv("SCT_NEW_VERSION", "master:latest")
    monkeypatch.setenv("SCT_USER_PREFIX", "testing")
    monkeypatch.setenv(
        "SCT_GCE_IMAGE_DB", "https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7"
    )

    resolved_repo_link = "https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/master/rpm\
        /centos/2021-06-09T13:12:44Z/scylla.repo"

    with (
        unittest.mock.patch.object(sct_config, "get_branch_version", return_value="666.development", clear=True),
        unittest.mock.patch.object(sct_config, "find_scylla_repo", return_value=resolved_repo_link, clear=True),
    ):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf._get_target_upgrade_version()

    assert conf.scylla_repo == resolved_repo_link
    target_upgrade_version = conf.target_upgrade_version
    assert target_upgrade_version == "666.development" or target_upgrade_version.endswith(".dev")


def test_15b_image_id_by_scylla_version(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "master:latest")
    monkeypatch.setenv("SCT_USER_PREFIX", "testing")
    monkeypatch.setenv("SCT_GCE_IMAGE_DB", "")
    monkeypatch.setenv("SCT_USE_PREINSTALLED_SCYLLA", "true")

    resolved_image_link = (
        "https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/"
        "scylla-4-7-dev-0-20220113-8bcd23fa0-1-build-359"
    )
    image = namedtuple("Image", "name self_link")(
        name="scylla-4-7-dev-0-20220113-8bcd23fa0-1-build-359", self_link=resolved_image_link
    )

    with unittest.mock.patch.object(sct_config, "get_branched_gce_images", return_value=[image], clear=True):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf._get_target_upgrade_version()

    assert conf.gce_image_db == resolved_image_link


def test_17_verify_scylla_bench_required_parameters_in_command(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv(
        "SCT_STRESS_CMD", "scylla-bench -workload=sequential -mode=write -replication-factor=3 -partition-count=100"
    )
    monkeypatch.setenv(
        "SCT_STRESS_READ_CMD", "scylla-bench -workload=uniform -mode=read -replication-factor=3 -partition-count=100"
    )

    conf = sct_config.SCTConfiguration()
    assert conf.stress_cmd == [
        "scylla-bench -workload=sequential -mode=write -replication-factor=3 -partition-count=100"
    ]
    assert conf.stress_read_cmd == [
        "scylla-bench -workload=uniform -mode=read -replication-factor=3 -partition-count=100"
    ]


def test_17_1_raise_error_if_scylla_bench_command_dont_have_workload(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_STRESS_CMD", "scylla-bench -mode=write -replication-factor=3 -partition-count=100")
    monkeypatch.setenv(
        "SCT_STRESS_READ_CMD", "scylla-bench -workload=uniform -mode=read -replication-factor=3 -partition-count=100"
    )

    err_msg = "Scylla-bench command scylla-bench -mode=write -replication-factor=3 -partition-count=100 doesn't have parameter -workload"

    with pytest.raises(ValueError) as context:
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    assert err_msg in str(context.value)


def test_17_2_raise_error_if_scylla_bench_command_dont_have_mode(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv(
        "SCT_STRESS_CMD", "scylla-bench -workload=sequential -mode=write -replication-factor=3 -partition-count=100"
    )
    monkeypatch.setenv(
        "SCT_STRESS_READ_CMD", "scylla-bench -workload=uniform -replication-factor=3 -partition-count=100"
    )

    err_msg = "Scylla-bench command scylla-bench -workload=uniform -replication-factor=3 -partition-count=100 doesn't have parameter -mode"

    with pytest.raises(ValueError) as context:
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    assert err_msg in str(context.value)


def test_19_aws_region_with_no_region_data(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_REGION_NAME", "not-exists-2")

    with pytest.raises(ValueError, match="not-exists-2 isn't supported"):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()


def test_21_nested_values(monkeypatch):
    monkeypatch.setenv(
        "SCT_CONFIG_FILES",
        ('["unit_tests/test_configs/minimal_test_case.yaml", "unit_tests/test_data/stress_image_extra_config.yaml"]'),
    )
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-1234")
    monkeypatch.setenv("SCT_STRESS_READ_CMD.0", "cassandra_stress")
    monkeypatch.setenv("SCT_STRESS_READ_CMD.1", "cassandra_stress")
    monkeypatch.setenv("SCT_STRESS_IMAGE", '{"ycsb": "scylladb/something_else"}')
    monkeypatch.setenv("SCT_STRESS_IMAGE.cassandra-stress", "scylla-bench")
    monkeypatch.setenv("SCT_STRESS_IMAGE.scylla-bench", "scylladb/something")

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    stress_image = conf.stress_image
    assert stress_image["ndbench"] == "scylladb/hydra-loaders:ndbench-jdk8-A"
    assert stress_image["nosqlbench"] == "scylladb/hydra-loaders:nosqlbench-A"

    assert conf.get("stress_image.scylla-bench") == "scylladb/something"
    assert conf.get("stress_image.non-exist") is None

    assert conf.stress_read_cmd == ["cassandra_stress", "cassandra_stress"]


def test_22_get_none(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-1234")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    val = conf.get(None)
    assert val is None


def test_23_1_include_nemesis_selector(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv(
        "SCT_CONFIG_FILES",
        '["unit_tests/test_configs/minimal_test_case.yaml", "unit_tests/test_configs/nemesis_selector_list.yaml"]',
    )

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    assert conf.nemesis_selector == ["config_changes and topology_changes"]


def test_26_run_fullscan_params_validtion_positive(monkeypatch):
    monkeypatch.setenv(
        "SCT_CONFIG_FILES",
        '["unit_tests/test_configs/minimal_test_case.yaml", "unit_tests/test_configs/positive_fullscan_param.yaml"]',
    )
    sct_config.SCTConfiguration()


def test_27_run_fullscan_params_validtion_negative(monkeypatch):
    monkeypatch.setenv(
        "SCT_CONFIG_FILES",
        '["unit_tests/test_configs/minimal_test_case.yaml", "unit_tests/test_configs/negative_fullscan_param.yaml"]',
    )
    try:
        sct_config.SCTConfiguration()
    except ValueError as exp:
        assert (
            str(exp) == "Config params validation errors:\n\tfield 'mode' must be one of "
            "'('random', 'table', 'partition', 'aggregate', 'table_and_aggregate')' "
            "but got 'agggregate'\n\t"
            "field 'ks_cf' must be an instance of <class 'str'>, but got '1'\n\t"
            "field 'validate_data' must be an instance of <class 'bool'>, but got 'no'\n\t"
            "field 'full_scan_aggregates_operation_limit' must be an instance of <class 'int'>, but got 'a'"
        )


def test_28_number_of_nodes_per_az_must_be_divisable_by_number_of_az(monkeypatch):
    monkeypatch.setenv("SCT_N_DB_NODES", "3 3 2")
    monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1 eu-west-2 us-east-1")
    monkeypatch.setenv("SCT_AVAILABILITY_ZONE", "a,b,c")
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy ami-dummy ami-dummy")
    monkeypatch.setenv("SCT_AMI_ID_LOADER", "ami-loader1 ami-loader2 ami-loader3")
    with pytest.raises(AssertionError) as context:
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    assert "should be divisible by number of availability zones" in str(context.value)


def test_35_append_str_options(monkeypatch):
    monkeypatch.setenv("SCT_APPEND_SCYLLA_ARGS", "++ --overprovisioned 5")

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert conf.append_scylla_args == (
        "--blocked-reactor-notify-ms 25 --abort-on-lsa-bad-alloc 1 "
        "--abort-on-seastar-bad-alloc --abort-on-internal-error 1 --abort-on-ebadf 1 "
        "--enable-sstable-key-validation 1 --overprovisioned 5"
    )


def test_35_append_list_options(monkeypatch):
    monkeypatch.setenv("SCT_SCYLLA_D_OVERRIDES_FILES", '["++", "extra_file/scylla.d/io.conf"]')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert conf.scylla_d_overrides_files == ["extra_file/scylla.d/io.conf"]


def test_35_append_unsupported_list_options(monkeypatch):
    monkeypatch.setenv("SCT_AZURE_REGION_NAME", '["++", "euwest"]')
    with pytest.raises(AssertionError) as context:
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
    assert "Option azure_region_name is not appendable" == str(context.value)


def test_35_append_unsupported_str_options(monkeypatch):
    monkeypatch.setenv("SCT_MANAGER_VERSION", "++ new version")
    with pytest.raises(AssertionError) as context:
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
    assert "Option manager_version is not appendable" == str(context.value)


def test_36_update_config_based_on_version():
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.artifact_scylla_version = "2025.1.0~dev"
    conf.is_enterprise = True
    conf.update_config_based_on_version()


def test_37_validates_single_thread_count_for_all_throttle_steps(monkeypatch):
    monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", '{"read": 620, "write": [630], "mixed": [500]}')
    monkeypatch.setenv(
        "SCT_PERF_GRADUAL_THROTTLE_STEPS",
        '{"read": ["unthrottled", "unthrottled"], "mixed": [100, "unthrottled"], "write": [200, "unthrottled"]}',
    )
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_37_raises_error_for_mismatched_thread_and_throttle_step_counts(monkeypatch):
    monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", '{"read": [620, 630]}')
    monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", '{"read": ["100", "200", "unthrottled"]}')
    with pytest.raises(ValueError):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()


def test_37_handles_empty_performance_throughput_parameters(monkeypatch):
    monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", "{}")
    monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", "{}")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_37_validates_multiple_thread_counts_per_throttle_step(monkeypatch):
    monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", '{"write": [400, 420, 440, 460, 480]}')
    monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", '{"write": ["100", "200", "300", "400", "unthrottled"]}')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_37_raises_error_for_invalid_thread_count_type(monkeypatch):
    monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", '{"mixed": ["invalid_type"]}')
    monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", '{"mixed": ["unthrottled"]}')
    with pytest.raises(ValueError):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()


@pytest.mark.parametrize(
    "backend, version, expected_repo",
    (
        ("k8s-eks", "2024.1.21", "scylladb/scylla-enterprise"),
        ("k8s-gke", "2024.1.21", "scylladb/scylla-enterprise"),
        ("k8s-eks", "2025.4.0", "scylladb/scylla"),
        ("k8s-gke", "2025.4.0", "scylladb/scylla"),
        ("docker", "2025.1.0", "scylladb/scylla"),
        ("docker", "2025.1.0-dev", "scylladb/scylla-nightly"),
    ),
)
def test_38_verify_scylla_version_lookup_k8s(monkeypatch, backend, version, expected_repo):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", backend)
    monkeypatch.setenv("SCT_SCYLLA_VERSION", version)
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert "docker_image" in conf.dump_config()
    assert conf.get("docker_image") == expected_repo


def test_39_billing_project_from_job_name(monkeypatch):
    """Test that billing_project is set correctly from JOB_NAME."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")

    # Test with scylla-5.2 prefix
    monkeypatch.setenv("JOB_NAME", "scylla-5.2/longevity/test")
    conf = sct_config.SCTConfiguration()
    assert conf.get("billing_project") == "5.2"

    # Test with scylladb-5.4 prefix
    monkeypatch.setenv("JOB_NAME", "scylladb-5.4/longevity/test")
    conf = sct_config.SCTConfiguration()
    assert conf.get("billing_project") == "5.4"


def test_39_billing_project_staging_not_set(monkeypatch):
    """Test that billing_project is NOT set to 'staging'."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")

    # Test with staging folder - should not set billing_project
    monkeypatch.setenv("JOB_NAME", "scylla-staging/longevity/test")
    conf = sct_config.SCTConfiguration()
    # billing_project should not be set to "staging"
    assert conf.get("billing_project") != "staging"

    # Test with scylladb-staging folder - should not set billing_project
    monkeypatch.setenv("JOB_NAME", "scylladb-staging/longevity/test")
    conf = sct_config.SCTConfiguration()
    # billing_project should not be set to "staging"
    assert conf.get("billing_project") != "staging"


def test_40_aws_emr_defaults_loaded(conf):
    """Test that EMR defaults from aws_emr_config.yaml are loaded for AWS backend."""
    expected_keys = [
        "emr_release_label",
        "emr_instance_type_master",
        "emr_instance_type_core",
        "emr_instance_count_core",
        "emr_applications",
    ]
    dumped = conf.dump_config()
    for key in expected_keys:
        assert key in dumped

    assert conf.get("emr_release_label") == ""
    assert conf.get("emr_instance_type_master") == "m5.xlarge"
    assert conf.get("emr_instance_type_core") == "m5.xlarge"
    assert conf.get("emr_instance_count_core") == 2
    assert conf.get("emr_applications") == ["Spark"]
    assert conf.get("emr_keep_alive") is True


class TestCommonTags:
    """Tests for TestConfig.common_tags() method."""

    def test_jenkins_job_tag_from_build_tag(self, monkeypatch):
        """Test that JenkinsJobTag is set from BUILD_TAG."""
        monkeypatch.setenv("BUILD_TAG", "jenkins-scylla-master-tier1-test-128")
        monkeypatch.setenv("JOB_NAME", "scylla-master/test")
        tags = TestConfig.common_tags()
        assert tags["JenkinsJobTag"] == "jenkins-scylla-master-tier1-test-128"

    @pytest.mark.parametrize(
        "build_tag, expected_jenkins_job",
        [
            ("jenkins-scylla-master-tier1-longevity-12h-test-128", "jenkins-scylla-master-tier1-longevity-12h-test"),
            ("jenkins-scylla-master-test-256", "jenkins-scylla-master-test"),
            ("jenkins-job-name-1", "jenkins-job-name"),
            # Edge case: trailing segment is not digits - remains unchanged
            ("jenkins-no-trailing-dash", "jenkins-no-trailing-dash"),
            ("jenkins", "jenkins"),
            ("-42", ""),
        ],
    )
    def test_jenkins_job_derived_from_build_tag(self, monkeypatch, build_tag, expected_jenkins_job):
        """Test that JenkinsJob is derived from BUILD_TAG by stripping trailing dash+digits."""
        monkeypatch.setenv("BUILD_TAG", build_tag)
        monkeypatch.setenv("JOB_NAME", "scylla-master/test")
        tags = TestConfig.common_tags()
        assert tags["JenkinsJob"] == expected_jenkins_job

    def test_jenkins_job_not_set_without_build_tag(self, monkeypatch):
        """Test that JenkinsJob is not set when BUILD_TAG is not present."""
        monkeypatch.delenv("BUILD_TAG", raising=False)
        monkeypatch.setenv("JOB_NAME", "scylla-master/test")
        tags = TestConfig.common_tags()
        assert "JenkinsJobTag" not in tags
        assert "JenkinsJob" not in tags


def test_vector_store_ami_name_resolved_to_ami_id(monkeypatch):
    """Verify that ami_id_vector_store goes through convert_name_to_ami_if_needed.

    Regression test for scylladb/scylla-cluster-tests#13104 which accidentally
    dropped ami_id_vector_store from the ami_id_params list during the pydantic
    config refactor, causing image name-to-AMI resolution to be skipped.
    """
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-db-scylla")
    monkeypatch.setenv("SCT_AMI_ID_VECTOR_STORE", "vector-store-1-5-0-arm64-2026-03-17t07-07-32z")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_VECTOR_STORE", "i4i.large")
    monkeypatch.setenv("SCT_AMI_VECTOR_STORE_USER", "ubuntu")
    monkeypatch.setenv("SCT_N_VECTOR_STORE_NODES", "1")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")

    resolved_names = {}

    def fake_convert(param, region_names):
        if not param.startswith("ami-"):
            resolved_names[param] = f"ami-{param}"
            return f"ami-{param}"
        return param

    with unittest.mock.patch("sdcm.sct_config.convert_name_to_ami_if_needed", side_effect=fake_convert):
        sct_config.SCTConfiguration()

    assert "vector-store-1-5-0-arm64-2026-03-17t07-07-32z" in resolved_names, (
        "convert_name_to_ami_if_needed was not called for ami_id_vector_store"
    )
