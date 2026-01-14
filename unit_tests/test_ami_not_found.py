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
# Copyright (c) 2025 ScyllaDB

import unittest.mock
import pytest
from botocore.exceptions import ClientError

from sdcm import sct_config
from sdcm.utils.common import get_ami_tags
from sdcm.utils.version_utils import is_enterprise

# Constants for AWS operation names
AWS_DESCRIBE_IMAGES_OPERATION = "DescribeImages"


@pytest.fixture
def ami_not_found_error():
    """Create a ClientError for AMI not found."""
    return ClientError(
        error_response={"Error": {"Code": "InvalidAMIID.NotFound", "Message": "The image id does not exist"}},
        operation_name=AWS_DESCRIBE_IMAGES_OPERATION,
    )


@pytest.fixture
def ami_permission_error():
    """Create a ClientError for permission denied."""
    return ClientError(
        error_response={"Error": {"Code": "UnauthorizedOperation", "Message": "You are not authorized"}},
        operation_name=AWS_DESCRIBE_IMAGES_OPERATION,
    )


class TestAMINotFound:
    """Test suite for handling non-existent AMI errors."""

    def test_is_enterprise_with_none(self):
        """Test that is_enterprise handles None input gracefully."""
        assert is_enterprise(None) is False

    def test_is_enterprise_with_valid_enterprise_version(self):
        """Test that is_enterprise correctly identifies enterprise versions."""
        assert is_enterprise("2024.1") is True
        assert is_enterprise("2023.1.1") is True
        assert is_enterprise("2022.2.0") is True

    def test_is_enterprise_with_valid_oss_version(self):
        """Test that is_enterprise correctly identifies non-enterprise versions."""
        assert is_enterprise("5.4.1") is False
        assert is_enterprise("6.0.0") is False
        assert is_enterprise("master:latest") is False

    def test_get_ami_tags_not_found_error(self, ami_not_found_error):
        """Test that get_ami_tags raises clear error when AMI doesn't exist."""
        mock_image = unittest.mock.MagicMock()
        mock_image.reload.side_effect = ami_not_found_error

        with (
            unittest.mock.patch("sdcm.utils.common.get_scylla_images_ec2_resource") as mock_scylla_resource,
            unittest.mock.patch("sdcm.utils.common.boto3") as mock_boto3,
        ):
            mock_scylla_resource.return_value.Image.return_value = mock_image
            mock_boto3.resource.return_value.Image.return_value = mock_image

            # Clear the cache first
            get_ami_tags.cache_clear()

            with pytest.raises(ValueError, match=r"AMI 'ami-12345' does not exist in region 'us-east-1'"):
                get_ami_tags("ami-12345", "us-east-1")

    def test_get_ami_tags_permission_error_reraises(self, ami_permission_error):
        """Test that get_ami_tags re-raises AWS errors like permission denied instead of returning empty dict."""
        mock_image = unittest.mock.MagicMock()
        mock_image.reload.side_effect = ami_permission_error

        with (
            unittest.mock.patch("sdcm.utils.common.get_scylla_images_ec2_resource") as mock_scylla_resource,
            unittest.mock.patch("sdcm.utils.common.boto3") as mock_boto3,
        ):
            mock_scylla_resource.return_value.Image.return_value = mock_image
            mock_boto3.resource.return_value.Image.return_value = mock_image

            # Clear the cache first
            get_ami_tags.cache_clear()

            # Should re-raise the ClientError, not return {} which would cause misleading "missing tag" error
            with pytest.raises(ClientError) as exc_info:
                get_ami_tags("ami-12345", "us-east-1")

            assert exc_info.value.response["Error"]["Code"] == "UnauthorizedOperation"

    def test_get_ami_tags_with_valid_ami(self):
        """Test that get_ami_tags works correctly with a valid AMI."""
        mock_image = unittest.mock.MagicMock()
        mock_image.meta.data = {"ImageId": "ami-valid"}
        mock_image.tags = [{"Key": "scylla_version", "Value": "5.4.1"}, {"Key": "Name", "Value": "Test AMI"}]
        mock_image.owner_id = "123456789"

        with unittest.mock.patch("sdcm.utils.common.get_scylla_images_ec2_resource") as mock_scylla_resource:
            mock_scylla_resource.return_value.Image.return_value = mock_image

            # Clear the cache first
            get_ami_tags.cache_clear()

            tags = get_ami_tags("ami-valid", "us-east-1")

            assert tags["scylla_version"] == "5.4.1"
            assert tags["Name"] == "Test AMI"
            assert tags["owner_id"] == "123456789"

    def test_get_ami_tags_with_no_tags(self):
        """Test that get_ami_tags returns empty dict when AMI has no tags."""
        mock_image = unittest.mock.MagicMock()
        mock_image.meta.data = {"ImageId": "ami-notags"}
        mock_image.tags = None

        with (
            unittest.mock.patch("sdcm.utils.common.get_scylla_images_ec2_resource") as mock_scylla_resource,
            unittest.mock.patch("sdcm.utils.common.boto3") as mock_boto3,
        ):
            mock_scylla_resource.return_value.Image.return_value = mock_image
            mock_boto3.resource.return_value.Image.return_value = mock_image

            # Clear the cache first
            get_ami_tags.cache_clear()

            tags = get_ami_tags("ami-notags", "us-east-1")

            assert tags == {}


class TestSCTConfigVersionValidation:
    """Test suite for version tag validation in sct_config.py get_version_based_on_conf."""

    def test_aws_ami_missing_scylla_version_tag(self, monkeypatch):
        """Test that missing scylla_version tag in AWS AMI raises clear ValueError."""
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-notags")
        monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")

        # Mock get_ami_tags to return empty dict (AMI exists but has no tags)
        with unittest.mock.patch("sdcm.sct_config.get_ami_tags", return_value={}):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

            with pytest.raises(
                ValueError, match=r"AMI 'ami-notags' .* does not have 'scylla_version' or 'ScyllaVersion' tag"
            ):
                conf.get_version_based_on_conf()

    def test_gce_image_missing_scylla_version_tag(self, monkeypatch):
        """Test that missing scylla_version tag in GCE image raises clear ValueError."""
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
        monkeypatch.setenv("SCT_GCE_IMAGE_DB", "projects/test/global/images/scylla-test")
        monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")

        # Mock get_gce_image_tags to return empty dict
        with unittest.mock.patch("sdcm.sct_config.get_gce_image_tags", return_value={}):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

            with pytest.raises(ValueError, match=r"GCE image .* does not have 'scylla_version' tag"):
                conf.get_version_based_on_conf()

    def test_azure_image_missing_scylla_version_tag(self, monkeypatch):
        """Test that missing scylla_version tag in Azure image raises clear ValueError."""
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "azure")
        monkeypatch.setenv(
            "SCT_AZURE_IMAGE_DB",
            "/subscriptions/test/resourceGroups/test/providers/Microsoft.Compute/images/scylla-test",
        )
        monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")

        # Mock get_image_tags to return empty dict
        with unittest.mock.patch("sdcm.provision.azure.utils.get_image_tags", return_value={}):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

            with pytest.raises(ValueError, match=r"Azure image .* does not have 'scylla_version' tag"):
                conf.get_version_based_on_conf()
