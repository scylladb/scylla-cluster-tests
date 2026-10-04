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

import unittest.mock

import pytest

from sdcm import sct_config


@pytest.fixture(autouse=True)
def function_setup(monkeypatch):
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    # gce/azure instance_type_db default to empty and are now required; set them so
    # gce/azure backends pass the per-backend required-params check.
    monkeypatch.setenv("SCT_GCE_INSTANCE_TYPE_DB", "n2-highmem-2")
    monkeypatch.setenv("SCT_AZURE_INSTANCE_TYPE_DB", "Standard_L8s_v3")


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
        unittest.mock.patch("sdcm.sct_config.config.latest_unified_package", return_value=fake_url),
        unittest.mock.patch(
            "sdcm.sct_config.config.convert_name_to_ami_if_needed",
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
