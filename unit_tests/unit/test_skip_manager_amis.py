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

"""Scylla Manager AMIs must never be picked by the Scylla AMI lookups.

Regression test for the manager-3.12 sanity failure where `scylla_version=2025.4` resolved to
`scylla-manager-3.13.0-dev-aarch64-...` (tagged scylla_version=2025.4.5, environment=production)
because it was the newest matching image, and then failed the `user_data_format_version` check.
"""

from unittest.mock import Mock, patch

import pytest

from sdcm.utils.common import get_branched_ami, get_scylla_ami_versions, is_scylla_manager_ami


def _image(image_id: str, name: str, creation_date: str, tags: dict[str, str]) -> Mock:
    image = Mock()
    image.image_id = image_id
    image.name = name
    image.creation_date = creation_date
    image.tags = [{"Key": key, "Value": value} for key, value in {"Name": name, **tags}.items()]
    return image


SCYLLA_AMI = _image(
    "ami-scylla",
    "ScyllaDB 2025.4.5 aarch64 2026-08-20",
    "2026-08-20T10:00:00.000Z",
    {
        "scylla_version": "2025.4.5-0.20260820.abcdef123456-1",
        "environment": "production",
        "branch": "branch-2025.4",
        "build_mode": "release",
        "user_data_format_version": "3",
    },
)
MANAGER_AMI = _image(
    "ami-0632b8f285fee79fa",
    "scylla-manager-3.13.0-dev-aarch64-2026-08-31T15-42-52",
    "2026-08-31T15:42:52.000Z",
    {
        "scylla_manager_version": "3.13.0-dev",
        "scylla_version": "2025.4.5",
        "environment": "production",
        "branch": "master",
        "build_mode": "release",
    },
)


@pytest.mark.parametrize(
    ("image", "expected"),
    [
        (SCYLLA_AMI, False),
        (MANAGER_AMI, True),
        (_image("ami-1", "scylla-manager-3.12.1-x86_64", "2026-01-01T00:00:00.000Z", {}), True),
        (_image("ami-2", "no-tags", "2026-01-01T00:00:00.000Z", {}), False),
    ],
    ids=["scylla", "manager-by-tag", "manager-by-name", "plain"],
)
def test_is_scylla_manager_ami(image, expected):
    assert is_scylla_manager_ami(image) is expected


def test_is_scylla_manager_ami_handles_untagged_image():
    image = Mock()
    image.tags = None
    assert is_scylla_manager_ami(image) is False


@pytest.fixture
def ec2_with_both_images():
    """Both owner accounts return the newer Manager AMI and the older Scylla AMI."""
    with (
        patch("sdcm.utils.common.boto3") as mock_boto3,
        patch("sdcm.utils.common.get_scylla_images_ec2_resource") as mock_images_resource,
    ):
        mock_boto3.resource.return_value.images.filter.return_value = [MANAGER_AMI, SCYLLA_AMI]
        mock_images_resource.return_value.images.filter.return_value = []
        get_scylla_ami_versions.cache_clear()
        yield
        get_scylla_ami_versions.cache_clear()


def test_get_scylla_ami_versions_skips_manager_ami(ec2_with_both_images):
    amis = get_scylla_ami_versions(region_name="us-east-1", arch="arm64", version="2025.4")
    assert [ami.image_id for ami in amis] == ["ami-scylla"]


def test_get_branched_ami_skips_manager_ami(ec2_with_both_images):
    amis = get_branched_ami(scylla_version="master:latest", region_name="us-east-1", arch="arm64")
    assert [ami.image_id for ami in amis] == ["ami-scylla"]


def test_get_branched_ami_all_skips_manager_ami(ec2_with_both_images):
    amis = get_branched_ami(scylla_version="master:all", region_name="us-east-1", arch="arm64")
    assert [ami.image_id for ami in amis] == ["ami-scylla"]
