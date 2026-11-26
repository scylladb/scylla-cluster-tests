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
# Copyright (c) 2026 ScyllaDB

from unittest.mock import MagicMock, patch

import pytest

from sdcm.utils.trigger_matrix.errors import TriggerMatrixError
from sdcm.utils.trigger_matrix.images import (
    _arch_from_image_name,
    resolve_architecture_from_ami,
    resolve_image_architecture,
)
from sdcm.utils.trigger_matrix.resolution import resolve_to_full_version

FULL_TAG = "2025.4.1-0.20250601.abc123def456-1"


@pytest.mark.parametrize(
    "full_version",
    [
        pytest.param("2024.2.5-0.20250221.cb9e2a54ae6d-1", id="release_tag"),
        pytest.param("2026.2.0~dev-0.20260322.f51126483167", id="dev_nightly_tag"),
        pytest.param("2025.1.3-0.20260101.abcdef123456-1", id="full_tag_with_suffix"),
    ],
)
def test_full_version_tag_returned_as_is(full_version):
    """resolve_to_full_version returns a full build tag unchanged."""
    result = resolve_to_full_version(full_version)
    assert result == full_version


@pytest.mark.parametrize(
    "release_version",
    [
        pytest.param("2026.1.8", id="release_2026"),
        pytest.param("2025.4.1", id="release_2025"),
        pytest.param("5.2.1", id="oss_release"),
    ],
)
def test_release_version_returned_as_is(release_version):
    """resolve_to_full_version returns a plain release version unchanged."""
    result = resolve_to_full_version(release_version)
    assert result == release_version


@patch("sdcm.utils.trigger_matrix.images._resolve_version_via_branched_ami")
def test_branch_qualifier_version_passes_as_is(mock_resolve):
    """A branch:latest qualifier is resolved through the AMI branch lookup."""
    mock_resolve.return_value = FULL_TAG

    result = resolve_to_full_version("master:latest")

    assert result == FULL_TAG
    mock_resolve.assert_called_once_with("master:latest", "eu-west-1", "x86_64")


@patch("sdcm.utils.trigger_matrix.images._resolve_version_via_branched_ami")
def test_simple_version_looked_up_as_release_branch(mock_resolve):
    """A bare version is looked up as the branch-<version>:latest qualifier."""
    mock_resolve.return_value = FULL_TAG

    result = resolve_to_full_version("2025.4")

    assert result == FULL_TAG
    mock_resolve.assert_called_once_with("branch-2025.4:latest", "eu-west-1", "x86_64")


@patch("sdcm.utils.trigger_matrix.images._resolve_version_via_branched_ami")
def test_custom_region_forwarded(mock_resolve):
    """The region argument is forwarded to the AMI branch lookup."""
    mock_resolve.return_value = FULL_TAG

    result = resolve_to_full_version("2025.4", region="us-east-1")

    assert result == FULL_TAG
    mock_resolve.assert_called_once_with("branch-2025.4:latest", "us-east-1", "x86_64")


@patch("sdcm.utils.trigger_matrix.images._resolve_version_via_branched_gce_image")
def test_gce_backend_resolved_against_gce_images(mock_resolve):
    """backend='gce' resolves the version through the GCE image lookup."""
    mock_resolve.return_value = FULL_TAG

    result = resolve_to_full_version("master:latest", backend="gce")

    assert result == FULL_TAG
    mock_resolve.assert_called_once_with("master:latest", "x86_64")


@patch("sdcm.utils.trigger_matrix.images._resolve_version_via_branched_azure_image")
def test_azure_backend_defaults_to_eastus(mock_resolve):
    """backend='azure' resolves through the Azure lookup in eastus by default."""
    mock_resolve.return_value = FULL_TAG

    result = resolve_to_full_version("master:latest", backend="azure")

    assert result == FULL_TAG
    mock_resolve.assert_called_once_with("master:latest", "eastus", "x86_64")


@patch("sdcm.utils.trigger_matrix.images._resolve_version_via_branched_ami")
def test_arch_forwarded_to_lookup(mock_resolve):
    """The arch argument is forwarded to the AMI branch lookup."""
    mock_resolve.return_value = FULL_TAG

    result = resolve_to_full_version("master:latest", arch="aarch64")

    assert result == FULL_TAG
    mock_resolve.assert_called_once_with("master:latest", "eu-west-1", "aarch64")


@patch("sdcm.utils.trigger_matrix.images._resolve_version_via_branched_gce_image")
def test_error_mentions_the_backend_that_failed(mock_resolve):
    """An unresolved version raises TriggerMatrixError naming the failing backend."""
    mock_resolve.return_value = ""

    with pytest.raises(TriggerMatrixError, match="on backend 'gce'"):
        resolve_to_full_version("master:latest", backend="gce")


@patch("sdcm.utils.trigger_matrix.images._resolve_version_via_branched_ami")
def test_raises_when_version_cannot_be_resolved(mock_resolve):
    """An unresolved release version raises TriggerMatrixError."""
    mock_resolve.return_value = ""

    with pytest.raises(TriggerMatrixError, match="Cannot resolve '2025.4'"):
        resolve_to_full_version("2025.4")


@patch("sdcm.utils.trigger_matrix.images._resolve_version_via_branched_ami")
def test_branch_version_fallthrough_raises(mock_resolve):
    """An unresolved branch qualifier raises TriggerMatrixError naming the qualifier."""
    mock_resolve.return_value = ""

    with pytest.raises(TriggerMatrixError, match="Cannot resolve 'branch-2025.4:latest'"):
        resolve_to_full_version("branch-2025.4:latest")


@patch("sdcm.utils.aws_utils.get_scylla_images_ec2_resource")
def test_resolve_architecture_from_ami_scylla_account(mock_ec2_resource):
    """An arm64 AMI read through the Scylla account resolves to aarch64."""
    mock_image = MagicMock()
    mock_image.architecture = "arm64"
    mock_ec2 = MagicMock()
    mock_ec2.Image.return_value = mock_image
    mock_ec2_resource.return_value = mock_ec2

    result = resolve_architecture_from_ami("ami-12345678", region="us-east-1")

    assert result == "aarch64"
    mock_ec2_resource.assert_called_once_with(region_name="us-east-1")


@patch("boto3.resource")
@patch("sdcm.utils.aws_utils.get_scylla_images_ec2_resource")
def test_resolve_architecture_from_ami_fallback_to_default(mock_ec2_resource, mock_boto3_resource):
    """When the Scylla account lookup fails, plain boto3 resolves the AMI architecture."""
    mock_ec2_resource.side_effect = Exception("STS role assumption failed")

    mock_image = MagicMock()
    mock_image.architecture = "x86_64"
    mock_ec2 = MagicMock()
    mock_ec2.Image.return_value = mock_image
    mock_boto3_resource.return_value = mock_ec2

    result = resolve_architecture_from_ami("ami-12345678", region="eu-west-1")

    assert result == "x86_64"
    mock_boto3_resource.assert_called_once_with("ec2", region_name="eu-west-1")


@patch("boto3.resource")
@patch("sdcm.utils.aws_utils.get_scylla_images_ec2_resource")
def test_resolve_architecture_from_ami_both_fail(mock_ec2_resource, mock_boto3_resource):
    """When both EC2 lookups fail, resolve_architecture_from_ami returns an empty string."""
    mock_ec2_resource.side_effect = Exception("STS failed")
    mock_boto3_resource.side_effect = Exception("No credentials")

    result = resolve_architecture_from_ami("ami-12345678", region="us-east-1")

    assert result == ""


@pytest.mark.parametrize(
    "image_name,expected",
    [
        pytest.param("scylla-2026.4.0-dev-aarch64-2026-07-21T03-23-02", "aarch64", id="dev-aarch64"),
        pytest.param("scylla-enterprise-2025.1.3-arm64-2025-06-15", "aarch64", id="enterprise-arm64"),
        pytest.param("scylla-2026.3.0-x86_64-2026-05-01", "x86_64", id="x86_64"),
        pytest.param("scylla-2026.3.0-release-20260501", "", id="no-arch-hint"),
        pytest.param("projects/scylla-images/global/images/scylla-aarch64-2026-07", "aarch64", id="gce-full-path"),
    ],
)
def test_arch_from_image_name(image_name, expected):
    """_arch_from_image_name reads the architecture hint from an image name, or returns empty."""
    assert _arch_from_image_name(image_name) == expected


@patch("sdcm.utils.trigger_matrix.images.resolve_architecture_from_ami")
def test_resolve_image_architecture_ami(mock_resolve_ami):
    """resolve_image_architecture delegates an AMI id to resolve_architecture_from_ami."""
    mock_resolve_ami.return_value = "aarch64"

    result = resolve_image_architecture(scylla_ami_id="ami-123", region="us-east-1")

    assert result == "aarch64"
    mock_resolve_ami.assert_called_once_with("ami-123", region="us-east-1")


def test_resolve_image_architecture_gce_name():
    """A GCE image name with an arm64 hint resolves to aarch64."""
    result = resolve_image_architecture(gce_image_db="projects/scylla-images/global/images/scylla-aarch64-2026-07")

    assert result == "aarch64"


def test_resolve_image_architecture_azure_name():
    """An Azure image name with an arm64 hint resolves to aarch64."""
    result = resolve_image_architecture(azure_image_db="/subscriptions/.../scylla-enterprise-arm64-2026.1")

    assert result == "aarch64"


def test_resolve_image_architecture_no_arch_hint():
    """An image name without an architecture hint resolves to an empty string."""
    result = resolve_image_architecture(gce_image_db="projects/scylla-images/global/images/scylla-2026-07")

    assert result == ""
