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
from botocore.exceptions import ClientError

from sdcm.utils.aws_region import AwsRegion


@pytest.fixture
def aws_region():
    """AwsRegion for us-east-1 with boto3 and the region list patched; the EC2 client mock is ``_mock_ec2``."""
    with (
        patch("sdcm.utils.aws_region.boto3.client", return_value=MagicMock()) as mock_client,
        patch("sdcm.utils.aws_region.boto3.resource"),
        patch("sdcm.utils.common.all_aws_regions", return_value=["us-east-1", "eu-west-1"]),
    ):
        region = AwsRegion(region_name="us-east-1")
        region._mock_ec2 = mock_client.return_value
        yield region


def set_offerings(region: AwsRegion, offerings: list[tuple[str, str]]) -> None:
    """Make describe_instance_type_offerings return the given (zone, instance type) pairs."""
    region._mock_ec2.describe_instance_type_offerings.return_value = {
        "InstanceTypeOfferings": [{"Location": loc, "InstanceType": itype} for loc, itype in offerings]
    }


def test_get_common_availability_zones_intersection(aws_region):
    """get_common_availability_zones returns only the AZs that offer every requested instance type."""
    set_offerings(
        aws_region,
        [
            ("us-east-1a", "i4i.large"),
            ("us-east-1b", "i4i.large"),
            ("us-east-1d", "i4i.large"),
            ("us-east-1a", "t3.small"),
            ("us-east-1d", "t3.small"),
        ],
    )
    result = aws_region.get_common_availability_zones(["i4i.large", "t3.small"])
    # only AZs supporting BOTH types
    assert set(result) == {"us-east-1a", "us-east-1d"}


@pytest.mark.parametrize(
    "preferred_azs,expected_prefix",
    [
        pytest.param(["us-east-1d"], ["us-east-1d"], id="one-preferred"),
        pytest.param(["us-east-1d", "us-east-1b"], ["us-east-1d", "us-east-1b"], id="two-preferred"),
    ],
)
def test_get_common_availability_zones_preferred_first(aws_region, preferred_azs, expected_prefix):
    """get_common_availability_zones lists the preferred AZs first, in the given order."""
    set_offerings(
        aws_region,
        [
            ("us-east-1a", "i4i.large"),
            ("us-east-1b", "i4i.large"),
            ("us-east-1c", "i4i.large"),
            ("us-east-1d", "i4i.large"),
        ],
    )
    result = aws_region.get_common_availability_zones(["i4i.large"], preferred_azs=preferred_azs)
    assert result[: len(expected_prefix)] == expected_prefix


def test_get_common_availability_zones_empty_when_no_overlap(aws_region):
    """get_common_availability_zones returns an empty list when no AZ offers every requested type."""
    set_offerings(
        aws_region,
        [
            ("us-east-1a", "i4i.large"),
            ("us-east-1b", "t3.small"),
        ],
    )
    assert aws_region.get_common_availability_zones(["i4i.large", "t3.small"]) == []


def test_get_common_availability_zones_preferred_unavailable_ignored(aws_region):
    """get_common_availability_zones drops a preferred AZ that does not offer the instance type."""
    set_offerings(
        aws_region,
        [
            ("us-east-1a", "i4i.large"),
            ("us-east-1b", "i4i.large"),
        ],
    )
    # preferred AZ does not support the type — should be silently dropped from prefix,
    # remaining AZs returned in some order
    result = aws_region.get_common_availability_zones(["i4i.large"], preferred_azs=["us-east-1c"])
    assert set(result) == {"us-east-1a", "us-east-1b"}
    assert "us-east-1c" not in result


def test_configure_backup_storage_keeps_an_existing_bucket(monkeypatch):
    """configure_backup_storage accepts BucketAlreadyOwnedByYou and still sets the lifecycle rule."""
    for var in ("AWS_ENDPOINT_URL", "GCE_ENDPOINT_URL", "SCT_MINICLOUD_ENDPOINT_URL"):
        monkeypatch.delenv(var, raising=False)
    with (
        patch("sdcm.utils.aws_region.boto3.client") as mock_client,
        patch("sdcm.utils.aws_region.boto3.resource"),
        patch("sdcm.utils.common.all_aws_regions", return_value=["us-east-1", "eu-west-1"]),
    ):
        s3 = mock_client.return_value
        s3.create_bucket.side_effect = ClientError({"Error": {"Code": "BucketAlreadyOwnedByYou"}}, "CreateBucket")
        AwsRegion(region_name="eu-west-1").configure_backup_storage()

    s3.create_bucket.assert_called_once_with(
        Bucket="manager-backup-tests-eu-west-1", CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
    )
    s3.put_bucket_lifecycle_configuration.assert_called_once_with(
        Bucket="manager-backup-tests-eu-west-1",
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID": "auto-deletion",
                    "Status": "Enabled",
                    "Filter": {},
                    "Expiration": {"Days": 7},
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 7},
                    "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7},
                }
            ]
        },
    )


def test_ensure_builder_security_group_keeps_an_existing_group_alive(aws_region):
    """ensure_builder_security_group tags an existing builder group with keep=alive."""
    group = MagicMock(ip_permissions=[])
    aws_region._mock_ec2.describe_security_groups.return_value = {"SecurityGroups": [{"GroupId": "sg-builder"}]}
    aws_region.resource.SecurityGroup.return_value = group

    aws_region.ensure_builder_security_group({"jenkins": ["192.0.2.10/32"]})
    assert {"Key": "keep", "Value": "alive"} in group.create_tags.call_args.kwargs["Tags"]


def test_configure_skips_builder_group_under_minicloud(aws_region, monkeypatch):
    """configure() under minicloud skips the builder security group and never reads the keystore."""
    aws_region._is_minicloud = True
    for name in (
        "update_sct_key_pair",
        "create_sct_vpc",
        "create_sct_internet_gateway",
        "configure_sct_route_table",
        "create_sct_subnets",
        "create_sct_security_group",
        "create_sct_ssh_security_group",
        "configure_backup_storage",
        "configure_ssm",
    ):
        monkeypatch.setattr(aws_region, name, MagicMock())
    loader = MagicMock(side_effect=AssertionError("keystore must not be read under minicloud"))
    monkeypatch.setattr("sdcm.utils.aws_region.get_builder_ssh_sources", loader)
    ensure = MagicMock()
    monkeypatch.setattr(aws_region, "ensure_builder_security_group", ensure)

    aws_region.configure()

    loader.assert_not_called()
    ensure.assert_not_called()
