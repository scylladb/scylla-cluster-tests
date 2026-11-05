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
from sdcm.utils.version_utils import latest_unified_package

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
    argnames="scylla_version, expected_docker_image, expected_version",
    argvalues=[
        pytest.param("LATEST", "scylladb/scylla", "LATEST", id="latest-release"),
        pytest.param("latest", "scylladb/scylla-nightly", None, id="latest"),
        pytest.param("master:latest", "scylladb/scylla-nightly", None, id="master:latest"),
    ],
)
def test_docker(scylla_version, expected_docker_image, expected_version, monkeypatch, latest_release):
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
    _version = conf.get_version_based_on_conf()
    if expected_version is not None:
        assert _version == expected_version


@pytest.mark.parametrize(argnames="distro", argvalues=("ubuntu-xenial", "centos", "debian-jessie"))
@pytest.mark.parametrize(
    argnames="scylla_version, expected_version",
    argvalues=[
        pytest.param("LATEST", "LATEST", id="latest-release"),
        pytest.param("master:latest", None, id="master"),
        pytest.param("branch-LATEST:latest", None, id="branch"),
    ],
)
def test_scylla_repo(scylla_version, expected_version, distro, monkeypatch, latest_release):
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
    _version = conf.get_version_based_on_conf()

    if expected_version is not None:
        assert expected_version in _version


@pytest.mark.parametrize(
    argnames="scylla_version, expected_version",
    argvalues=[
        pytest.param("LATEST", "LATEST", id="latest-release"),
        pytest.param("master:latest", None, id="master"),
        pytest.param("branch-LATEST:latest", None, id="branch"),
    ],
)
@pytest.mark.parametrize(argnames="backend", argvalues=("aws", "gce", "azure"))
def test_images(backend, scylla_version, expected_version, monkeypatch, latest_release):
    scylla_version = scylla_version.replace("LATEST", latest_release)
    if expected_version == "LATEST":
        expected_version = latest_release

    monkeypatch.setenv("SCT_CLUSTER_BACKEND", backend)
    monkeypatch.setenv("SCT_SCYLLA_VERSION", scylla_version)

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    _version = conf.get_version_based_on_conf()

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

    _version = conf.get_version_based_on_conf()

    assert "6.1" in _version


def test_unified_package(monkeypatch):
    # Unlike the sibling tests below, this one actually downloads and unpacks the
    # package to read SCYLLA-VERSION-FILE, so the URL has to point at a package
    # that still exists. Resolve the current master build instead of pinning one:
    # unstable relocatables are garbage collected within a few months.
    unified_package = latest_unified_package()

    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_UNIFIED_PACKAGE", unified_package)
    monkeypatch.setenv(
        "SCT_GCE_IMAGE_DB",
        "https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-stream-9",
    )

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    # unified_package should auto-set use_preinstalled_scylla to False
    assert conf.get("use_preinstalled_scylla") is False

    _version = conf.get_version_based_on_conf()

    # e.g. scylla-unified-2026.4.0~dev-0.20260804.0cba3cd2a4b0.x86_64.tar.gz -> 2026.4.0
    expected_version = unified_package.rsplit("/", 1)[-1].removeprefix("scylla-unified-").split("~")[0]
    assert expected_version in _version


UNIFIED_PACKAGE_X86 = (
    "https://downloads.scylladb.com/unstable/scylla/master/relocatable/2023-11-13T03:04:27Z/"
    "scylla-unified-5.5.0~dev-0.20231113.7b08886e8dd8.x86_64.tar.gz"
)
UBUNTU_SSM_AMI = "resolve:ssm:/aws/service/canonical/ubuntu/server/24.04/stable/current/{ssm_arch}/hvm/ebs-gp3/ami-id"


@pytest.mark.parametrize(
    "ami, user",
    [
        pytest.param(None, None, id="no-ami"),
        pytest.param("ami-test123", None, id="default-scyllaadm-user"),
    ],
)
def test_unified_package_aws_fails_fast_without_ami_and_user(ami, user, monkeypatch):
    """unified_package on AWS needs the config to bring a plain distro AMI and its user; SCT doesn't guess."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_UNIFIED_PACKAGE", UNIFIED_PACKAGE_X86)
    if ami:
        monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", ami)

    with pytest.raises(ValueError, match="unified_package on AWS needs a plain distro AMI and its SSH user"):
        sct_config.SCTConfiguration()


def test_unified_package_aws_keeps_configured_user(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_UNIFIED_PACKAGE", UNIFIED_PACKAGE_X86)
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-test123")
    monkeypatch.setenv("SCT_AMI_DB_SCYLLA_USER", "ec2-user")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    assert conf.get("use_preinstalled_scylla") is False
    assert conf.get("ami_db_scylla_user") == "ec2-user"


@pytest.mark.parametrize(
    "scylla_version, arch, ssm_arch",
    [
        pytest.param("relocatable:master:x86_64", "x86_64", "amd64", id="relocatable-x86_64"),
        pytest.param("relocatable:master:aarch64", "aarch64", "arm64", id="relocatable-aarch64"),
    ],
)
def test_relocatable_version_on_aws_with_ubuntu_ami(scylla_version, arch, ssm_arch, monkeypatch):
    """relocatable: resolves to a unified_package; the base AMI and user come from env, as the PGO trigger passes them."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", scylla_version)
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", UBUNTU_SSM_AMI.format(ssm_arch=ssm_arch))
    monkeypatch.setenv("SCT_AMI_DB_SCYLLA_USER", "ubuntu")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")

    fake_url = f"https://example.com/relocatable/latest/scylla-unified-6.3.0~dev-0.20260101.abcdef123456.{arch}.tar.gz"
    with (
        unittest.mock.patch("sdcm.sct_config.config.latest_unified_package", return_value=fake_url) as latest,
        unittest.mock.patch(
            "sdcm.sct_config.config.convert_name_to_ami_if_needed",
            side_effect=lambda param, region_names: param,
        ),
    ):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    latest.assert_called_once_with(arch=arch, branch="master")
    assert conf.get("unified_package") == fake_url
    assert conf.get("scylla_version") == ""
    assert conf.get("use_preinstalled_scylla") is False
    assert conf.get("ami_db_scylla_user") == "ubuntu"
    assert conf.get("ami_id_db_scylla") == UBUNTU_SSM_AMI.format(ssm_arch=ssm_arch)


def test_relocatable_version_resolves_unified_package_on_gce(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "relocatable:latest")
    monkeypatch.setenv(
        "SCT_GCE_IMAGE_DB",
        "https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-stream-9",
    )

    fake_url = "https://example.com/relocatable/latest/scylla-unified-6.3.0~dev-0.20260101.abcdef123456.x86_64.tar.gz"
    with unittest.mock.patch("sdcm.sct_config.config.latest_unified_package", return_value=fake_url):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    assert conf.get("unified_package") == fake_url
    assert conf.get("use_preinstalled_scylla") is False


def test_aws_ami_missing_scylla_version_tag(monkeypatch):
    """Test that missing scylla_version tag in AWS AMI raises clear ValueError."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-notags")

    # Mock get_ami_tags to return empty dict (AMI exists but has no tags)
    with unittest.mock.patch("sdcm.sct_config.config.get_ami_tags", return_value={}):
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
    with unittest.mock.patch("sdcm.sct_config.config.get_gce_image_tags", return_value={}):
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
    with unittest.mock.patch("sdcm.sct_config.config.azure_utils.get_image_tags", return_value={}):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        with pytest.raises(ValueError, match=r"Azure image .* does not have 'scylla_version' tag"):
            conf.get_version_based_on_conf()
