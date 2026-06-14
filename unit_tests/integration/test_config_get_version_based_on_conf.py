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
# Copyright (c) 2023 ScyllaDB

import logging
import unittest.mock

import pytest

from sdcm import sct_config
from sdcm.utils.common import get_latest_scylla_release

pytestmark = [
    pytest.mark.integration,
]


def _get_latest_branch() -> str:
    full_version = get_latest_scylla_release("scylla-enterprise")
    return ".".join(full_version.split(".")[:2])


@pytest.fixture(scope="session", autouse=True)
def setup():
    logging.basicConfig(level=logging.ERROR)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("boto3").setLevel(logging.CRITICAL)
    logging.getLogger("anyconfig").setLevel(logging.ERROR)

    yield


@pytest.fixture(scope="session")
def latest_release():
    return _get_latest_branch()


@pytest.fixture(scope="function", autouse=True)
def function_setup(monkeypatch):
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    # gce/azure instance_type_db default to empty and are now required; set them so
    # gce/azure backends pass the per-backend required-params check.
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_DB", "n2-highmem-2")
    monkeypatch.setenv("SCT_AZURE_INSTANCE_TYPE_DB", "Standard_L8s_v3")


@pytest.mark.parametrize(
    argnames="scylla_version, expected_docker_image, expected_version, expected_is_enterprise",
    argvalues=[
        pytest.param("LATEST", "scylladb/scylla", "LATEST", False, id="latest-release"),
        pytest.param("latest", "scylladb/scylla-nightly", None, False, id="latest"),
        pytest.param("master:latest", "scylladb/scylla-nightly", None, False, id="master:latest"),
    ],
)
def test_docker(
    scylla_version, expected_docker_image, expected_version, expected_is_enterprise, monkeypatch, latest_release
):
    if scylla_version == "LATEST":
        scylla_version = latest_release
    if expected_version == "LATEST":
        expected_version = latest_release

    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", scylla_version)

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert "docker_image" in conf.dump_config()
    assert conf.get("docker_image") == expected_docker_image
    _version, _is_enterprise = conf.get_version_based_on_conf()
    assert _is_enterprise == expected_is_enterprise
    if expected_version is not None:
        assert _version == expected_version


@pytest.mark.parametrize(argnames="distro", argvalues=("ubuntu-xenial", "centos", "debian-jessie"))
@pytest.mark.parametrize(
    argnames="scylla_version, expected_version, expected_is_enterprise",
    argvalues=[
        pytest.param("LATEST", "LATEST", True, id="latest-release"),
        pytest.param("master:latest", None, True, id="master"),
        pytest.param("branch-LATEST:latest", None, True, id="branch"),
    ],
)
def test_scylla_repo(scylla_version, expected_version, expected_is_enterprise, distro, monkeypatch, latest_release):
    scylla_version = scylla_version.replace("LATEST", latest_release)
    if expected_version == "LATEST":
        expected_version = latest_release

    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", scylla_version)
    monkeypatch.setenv(
        "SCT_GCE_IMAGE_DB",
        "https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-stream-9",
    )
    monkeypatch.setenv("SCT_USE_PREINSTALLED_SCYLLA", "false")
    monkeypatch.setenv("SCT_SCYLLA_LINUX_DISTRO", distro)

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    _version, _is_enterprise = conf.get_version_based_on_conf()

    assert _is_enterprise == expected_is_enterprise
    if expected_version is not None:
        assert expected_version in _version


@pytest.mark.parametrize(
    argnames="scylla_version, expected_version, expected_is_enterprise",
    argvalues=[
        pytest.param("LATEST", "LATEST", True, id="latest-release"),
        pytest.param("master:latest", None, True, id="master"),
        pytest.param("branch-LATEST:latest", None, True, id="branch"),
    ],
)
@pytest.mark.parametrize(argnames="backend", argvalues=("aws", "gce", "azure"))
def test_images(backend, scylla_version, expected_version, expected_is_enterprise, monkeypatch, latest_release):
    scylla_version = scylla_version.replace("LATEST", latest_release)
    if expected_version == "LATEST":
        expected_version = latest_release

    monkeypatch.setenv("SCT_CLUSTER_BACKEND", backend)
    monkeypatch.setenv("SCT_SCYLLA_VERSION", scylla_version)

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    _version, _is_enterprise = conf.get_version_based_on_conf()

    assert _is_enterprise == expected_is_enterprise
    if expected_version is not None:
        assert expected_version in _version


def test_baremetal(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "baremetal")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "6.1")
    monkeypatch.setenv("SCT_S3_BAREMETAL_CONFIG", "some_config")
    monkeypatch.setenv("SCT_DB_NODES_PRIVATE_IP", '["127.0.0.1"]')
    monkeypatch.setenv("SCT_DB_NODES_PUBLIC_IP", '["127.0.0.1"]')

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    _version, _is_enterprise = conf.get_version_based_on_conf()

    assert "6.1" in _version
    assert not _is_enterprise


def test_unified_package(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv(
        "SCT_UNIFIED_PACKAGE",
        (
            "https://downloads.scylladb.com/unstable/scylla/master/relocatable/2023-11-13T03:04:27Z/"
            "scylla-unified-5.5.0~dev-0.20231113.7b08886e8dd8.x86_64.tar.gz"
        ),
    )
    monkeypatch.setenv(
        "SCT_GCE_IMAGE_DB",
        "https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-stream-9",
    )

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    # unified_package should auto-set use_preinstalled_scylla to False
    assert conf.get("use_preinstalled_scylla") is False

    _version, _is_enterprise = conf.get_version_based_on_conf()

    assert "5.5.0" in _version
    assert not _is_enterprise


def test_unified_package_aws_sets_ubuntu_user(monkeypatch):
    """Test that unified_package on AWS forces ami_db_scylla_user='ubuntu'."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv(
        "SCT_UNIFIED_PACKAGE",
        (
            "https://downloads.scylladb.com/unstable/scylla/master/relocatable/2023-11-13T03:04:27Z/"
            "scylla-unified-5.5.0~dev-0.20231113.7b08886e8dd8.x86_64.tar.gz"
        ),
    )
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-test123")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    assert conf.get("use_preinstalled_scylla") is False
    assert conf.get("ami_db_scylla_user") == "ubuntu"


@pytest.mark.parametrize(
    "scylla_version, backend, expected_ami_ssm",
    [
        pytest.param(
            "relocatable:latest",
            "aws",
            "resolve:ssm:/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
            id="relocatable-latest-aws-x86_64",
        ),
        pytest.param(
            "relocatable:master:x86_64",
            "aws",
            "resolve:ssm:/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id",
            id="relocatable-master-x86_64-aws",
        ),
        pytest.param(
            "relocatable:master:aarch64",
            "aws",
            "resolve:ssm:/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id",
            id="relocatable-master-aarch64-aws",
        ),
        pytest.param("relocatable:latest", "gce", None, id="relocatable-latest-gce"),
    ],
)
def test_relocatable_version_resolves_unified_package(scylla_version, backend, expected_ami_ssm, monkeypatch):
    """Test that relocatable:<branch> scylla_version resolves to unified_package URL without crashing."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", backend)
    monkeypatch.setenv("SCT_SCYLLA_VERSION", scylla_version)
    if backend == "gce":
        monkeypatch.setenv(
            "SCT_GCE_IMAGE_DB",
            "https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-stream-9",
        )

    fake_url = (
        "https://downloads.scylladb.com/unstable/scylla/master/relocatable/latest/"
        "scylla-unified-6.3.0~dev-0.20260101.abcdef123456.x86_64.tar.gz"
    )
    with (
        unittest.mock.patch("sdcm.sct_config.latest_unified_package", return_value=fake_url),
        unittest.mock.patch(
            "sdcm.sct_config.convert_name_to_ami_if_needed",
            side_effect=lambda param, region_names: param,
        ),
    ):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    assert conf.get("unified_package") == fake_url
    assert conf.get("scylla_version") == ""
    assert conf.get("use_preinstalled_scylla") is False
    if expected_ami_ssm:
        # On AWS, ami_id_db_scylla should be auto-set to the Ubuntu 24.04 SSM resolve pattern
        assert conf.get("ami_id_db_scylla") == expected_ami_ssm


def test_unified_package_aws_auto_resolves_ami(monkeypatch):
    """Test that unified_package on AWS auto-resolves ami_id_db_scylla to Ubuntu 24.04 base AMI when not set."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv(
        "SCT_UNIFIED_PACKAGE",
        (
            "https://downloads.scylladb.com/unstable/scylla/master/relocatable/2023-11-13T03:04:27Z/"
            "scylla-unified-5.5.0~dev-0.20231113.7b08886e8dd8.x86_64.tar.gz"
        ),
    )

    with unittest.mock.patch(
        "sdcm.sct_config.convert_name_to_ami_if_needed",
        side_effect=lambda param, region_names: param,
    ):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    assert conf.get("use_preinstalled_scylla") is False
    assert conf.get("ami_db_scylla_user") == "ubuntu"
    # ami_id_db_scylla should be auto-set to the Ubuntu 24.04 SSM resolve pattern
    assert "resolve:ssm:" in conf.get("ami_id_db_scylla")
    assert "ubuntu" in conf.get("ami_id_db_scylla")


def test_unified_package_aws_verify_passes_without_ami(monkeypatch):
    """Test that verify_configuration passes with unified_package on AWS even if ami_id_db_scylla is not explicitly set."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv(
        "SCT_UNIFIED_PACKAGE",
        (
            "https://downloads.scylladb.com/unstable/scylla/master/relocatable/2023-11-13T03:04:27Z/"
            "scylla-unified-5.5.0~dev-0.20231113.7b08886e8dd8.x86_64.tar.gz"
        ),
    )

    # Mock convert_name_to_ami_if_needed to pass through the SSM pattern (simulates AMI resolution)
    with unittest.mock.patch(
        "sdcm.sct_config.convert_name_to_ami_if_needed",
        side_effect=lambda param, region_names: param,
    ):
        conf = sct_config.SCTConfiguration()
        # This should NOT raise AssertionError about ami_id_db_scylla,
        # because unified_package is set and _check_version_supplied should not
        # require backend-specific images when unified_package is provided
        conf.verify_configuration()

    assert conf.get("use_preinstalled_scylla") is False
    assert conf.get("unified_package") is not None


def test_aws_ami_missing_scylla_version_tag(monkeypatch):
    """Test that missing scylla_version tag in AWS AMI raises clear ValueError."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-notags")

    # Mock get_ami_tags to return empty dict (AMI exists but has no tags)
    with unittest.mock.patch("sdcm.sct_config.get_ami_tags", return_value={}):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        with pytest.raises(
            ValueError, match=r"AMI 'ami-notags' .* does not have 'scylla_version' or 'ScyllaVersion' tag"
        ):
            conf.get_version_based_on_conf()


def test_gce_image_missing_scylla_version_tag(monkeypatch):
    """Test that missing scylla_version tag in GCE image raises clear ValueError."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_GCE_IMAGE_DB", "projects/test/global/images/scylla-test")

    # Mock get_gce_image_tags to return empty dict
    with unittest.mock.patch("sdcm.sct_config.get_gce_image_tags", return_value={}):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        with pytest.raises(ValueError, match=r"GCE image .* does not have 'scylla_version' tag"):
            conf.get_version_based_on_conf()


def test_azure_image_missing_scylla_version_tag(monkeypatch):
    """Test that missing scylla_version tag in Azure image raises clear ValueError."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "azure")
    monkeypatch.setenv(
        "SCT_AZURE_IMAGE_DB",
        "/subscriptions/test/resourceGroups/test/providers/Microsoft.Compute/images/scylla-test",
    )

    # Mock get_image_tags to return empty dict
    with unittest.mock.patch("sdcm.sct_config.azure_utils.get_image_tags", return_value={}):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        with pytest.raises(ValueError, match=r"Azure image .* does not have 'scylla_version' tag"):
            conf.get_version_based_on_conf()
