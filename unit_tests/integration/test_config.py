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

"""
Integration tests for SCTConfiguration that require network access to AWS, GCE, or Azure APIs.
"""

import logging

import pytest

from sdcm import sct_config
from sdcm.utils.aws_utils import get_ssm_ami
from sdcm.utils.common import get_latest_scylla_release

pytestmark = [
    pytest.mark.integration,
]


def _get_latest_scylla_release(product="scylla"):
    return get_latest_scylla_release(product=product, verify=False)


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


def test_04_check_env_parse(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_REGION_NAME", '["eu-west-1", "us-east-1"]')
    monkeypatch.setenv("SCT_N_DB_NODES", "2 2")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy ami-dummy2")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.dump_config()

    assert conf.ami_id_db_scylla == "ami-dummy ami-dummy2"


def test_10_longevity(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/complex_test_case_with_version.yaml")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA_DESC", "master")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert conf.user_prefix == "longevity-50gb-4d-not-jenkins-maste"


def test_10_mananger_regression(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy ami-dummy2")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/multi_region_dc_test_case.yaml")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_12_scylla_version_ami(monkeypatch):
    monkeypatch.delenv("SCT_AMI_ID_DB_SCYLLA", raising=False)
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", _get_latest_scylla_release())
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/multi_region_dc_test_case.yaml")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    amis = conf.ami_id_db_scylla.split()
    assert len(amis) == 2
    assert all(ami.startswith("ami-") for ami in amis)


def test_12_scylla_version_ami_case1(monkeypatch):
    monkeypatch.delenv("SCT_AMI_ID_DB_SCYLLA", raising=False)
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", _get_latest_scylla_release())
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/multi_region_dc_test_case.yaml")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_12_scylla_version_ami_case2(monkeypatch):
    monkeypatch.delenv("SCT_AMI_ID_DB_SCYLLA", raising=False)
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "99.0.3")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/multi_region_dc_test_case.yaml")

    with pytest.raises(ValueError, match="AMIs for scylla_version='99.0.3' not found in eu-west-1"):
        sct_config.SCTConfiguration()


def test_12_scylla_version_repo(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", _get_latest_scylla_release())
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_12_scylla_version_repo_case1(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", _get_latest_scylla_release())
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_12_scylla_version_repo_case2(monkeypatch):
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "99.0.3")

    with pytest.raises(ValueError, match=r"repo for scylla version 99.0.3 wasn't found"):
        sct_config.SCTConfiguration()


def test_13_scylla_version_ami_branch_latest(monkeypatch):
    latest_branch = ".".join(_get_latest_scylla_release().split(".")[0:2])
    monkeypatch.delenv("SCT_AMI_ID_DB_SCYLLA", raising=False)
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", f"branch-{latest_branch}:latest")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/multi_region_dc_test_case.yaml")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    amis = conf.ami_id_db_scylla.split()
    assert len(amis) == 2
    assert all(ami.startswith("ami-") for ami in amis)


def test_13_bool(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_USE_PREINSTALLED_SCYLLA", "False")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/multi_region_dc_test_case.yaml")
    conf = sct_config.SCTConfiguration()

    assert conf.use_preinstalled_scylla is False


def test_14_aws_siren_from_env(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_DB_TYPE", "cloud_scylla")
    monkeypatch.setenv("SCT_REGION_NAME", "us-east-1")
    monkeypatch.setenv("SCT_N_DB_NODES", "2")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")
    monkeypatch.setenv("SCT_AUTHENTICATOR_USER", "user")
    monkeypatch.setenv("SCT_AUTHENTICATOR_PASSWORD", "pass")
    monkeypatch.setenv("SCT_CLOUD_CLUSTER_ID", "193712947904378")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/multi_region_dc_test_case.yaml")
    monkeypatch.setenv("SCT_CLOUD_CREDENTIALS_PATH", "/tmp/credentials_path")

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert conf.cloud_cluster_id == 193712947904378
    assert conf.authenticator_user == "user"
    assert conf.authenticator_password == "pass"


def test_16_default_oracle_scylla_version_eu_west_1(monkeypatch):
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_DB_TYPE", "mixed_scylla")

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    assert "ami-" in conf.ami_id_db_oracle


def test_16_oracle_scylla_version_us_east_1(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_ORACLE_SCYLLA_VERSION", _get_latest_scylla_release())
    monkeypatch.setenv("SCT_REGION_NAME", "us-east-1")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_DB_TYPE", "mixed_scylla")

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    assert conf.ami_id_db_oracle.startswith("ami-")


def test_16_oracle_scylla_version_eu_west_1(monkeypatch):
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_ORACLE_SCYLLA_VERSION", _get_latest_scylla_release())
    monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_DB_TYPE", "mixed_scylla")

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    assert conf.ami_id_db_oracle.startswith("ami-")


def test_16_oracle_scylla_version_and_oracle_ami_together(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_REGION_NAME", "eu-west-1")
    monkeypatch.setenv("SCT_ORACLE_SCYLLA_VERSION", _get_latest_scylla_release())
    monkeypatch.setenv("SCT_AMI_ID_DB_ORACLE", "ami-dummy")
    monkeypatch.setenv("SCT_DB_TYPE", "mixed_scylla")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")

    with pytest.raises(ValueError) as context:
        sct_config.SCTConfiguration()

    assert "'oracle_scylla_version' and 'ami_id_db_oracle' can't used together" in str(context.value)


def test_18_error_if_no_version_repo_ami_selected(monkeypatch):
    monkeypatch.delenv("SCT_AMI_ID_DB_SCYLLA", raising=False)
    monkeypatch.delenv("SCT_SCYLLA_VERSION", raising=False)
    for backend in sct_config.available_backends:
        if "k8s" in backend:
            continue
        if "siren" in backend:
            continue
        if backend == "xcloud":
            monkeypatch.setenv("SCT_XCLOUD_PROVIDER", "aws")
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", backend)
        monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")

        conf = sct_config.SCTConfiguration()
        with pytest.raises(AssertionError, match="scylla version/repos wasn't configured correctly") as context:
            conf.verify_configuration()
        assert "scylla version/repos wasn't configured correctly" in str(context.value)
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_DB_TYPE", "cloud_scylla")

    # 1) check that SCT_CLOUD_CLUSTER_ID is used for the exception
    conf = sct_config.SCTConfiguration()

    with pytest.raises(AssertionError) as context:
        conf.verify_configuration()
    assert "scylla version/repos wasn't configured correctly" in str(context.value)
    assert "SCT_CLOUD_CLUSTER_ID" in str(context.value)

    monkeypatch.setenv("SCT_CLOUD_CLUSTER_ID", "1234")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_20_user_data_format_version_aws(monkeypatch):
    monkeypatch.delenv("SCT_AMI_ID_DB_SCYLLA", raising=False)
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "master:latest")
    monkeypatch.setenv("SCT_ORACLE_SCYLLA_VERSION", _get_latest_scylla_release())
    monkeypatch.setenv("SCT_DB_TYPE", "mixed_scylla")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.verify_configuration_urls_validity()
    assert conf.user_data_format_version == "3"
    assert conf.oracle_user_data_format_version == "3"


def test_20_user_data_format_version_aws_2(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    ami_id = get_ssm_ami(
        "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id", region_name="eu-west-1"
    )
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", ami_id)  # run image which isn't scylla
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.verify_configuration_urls_validity()
    assert conf.user_data_format_version == "3"
    assert getattr(conf, "oracle_user_data_format_version", None) is None


def test_20_user_data_format_version_gce_1(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "master:latest")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.verify_configuration_urls_validity()
    assert conf.user_data_format_version == "3"


def test_20_user_data_format_version_gce_2(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", _get_latest_scylla_release())
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.verify_configuration_urls_validity()
    assert conf.user_data_format_version == "3"


def test_20_user_data_format_version_gce_3(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv(
        "SCT_GCE_IMAGE_DB",
        ("https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/family/ubuntu-2404-lts-amd64"),
    )
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.verify_configuration_urls_validity()
    assert conf.user_data_format_version == "2"


def test_20_user_data_format_version_azure(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "azure")
    monkeypatch.setenv("SCT_AZURE_REGION_NAME", "eastus")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "master:latest")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.verify_configuration_urls_validity()
    # since azure image listing is still buggy, can be sure which version we'll get
    # I would expect master:latest to be version 3 now, but azure.utils.get_scylla_images
    # returns something from 5 months ago.


def test_conf_check_required_files(monkeypatch):  # pylint: disable=no-self-use
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    ami_id = get_ssm_ami(
        "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id", region_name="eu-west-1"
    )
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", ami_id)
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv(
        "SCT_STRESS_CMD",
        """cassandra-stress user profile=/tmp/cs_profile_background_reads_overload.yaml \
        ops'(insert=100)' no-warmup cl=ONE duration=10m -mode cql3 native -rate threads=3000 -errors ignore""",
    )
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.check_required_files()
