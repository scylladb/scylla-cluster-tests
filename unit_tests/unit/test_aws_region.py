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

from sdcm.utils.aws_region import AwsRegion


@pytest.fixture(name="aws_region")
def aws_region_fixture():
    with (
        patch("sdcm.utils.aws_region.boto3.client", return_value=MagicMock()) as mock_client,
        patch("sdcm.utils.aws_region.boto3.resource"),
        patch("sdcm.utils.common.all_aws_regions", return_value=["us-east-1", "eu-west-1"]),
    ):
        region = AwsRegion(region_name="us-east-1")
        region._mock_ec2 = mock_client.return_value
        yield region


def _set_offerings(region: AwsRegion, offerings: list[tuple[str, str]]) -> None:
    region._mock_ec2.describe_instance_type_offerings.return_value = {
        "InstanceTypeOfferings": [{"Location": loc, "InstanceType": itype} for loc, itype in offerings]
    }


def test_get_common_availability_zones_intersection(aws_region):
    _set_offerings(
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
        (["us-east-1d"], ["us-east-1d"]),
        (["us-east-1d", "us-east-1b"], ["us-east-1d", "us-east-1b"]),
    ],
)
def test_get_common_availability_zones_preferred_first(aws_region, preferred_azs, expected_prefix):
    _set_offerings(
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
    _set_offerings(
        aws_region,
        [
            ("us-east-1a", "i4i.large"),
            ("us-east-1b", "t3.small"),
        ],
    )
    assert aws_region.get_common_availability_zones(["i4i.large", "t3.small"]) == []


def test_get_common_availability_zones_preferred_unavailable_ignored(aws_region):
    _set_offerings(
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
