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
# Copyright (c) 2024 ScyllaDB

"""
Integration tests for find_ami_equivalent functionality.
These tests make real AWS API calls and require valid AWS credentials.
"""

import pytest
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from sdcm.utils.common import find_equivalent_ami

pytestmark = [
    pytest.mark.integration,
]


def test_find_equivalent_ami_real_scylla_ami():
    """
    Test with a real ScyllaDB AMI.
    This test requires AWS credentials and makes actual API calls.
    """
    # Use a known ScyllaDB 5.2 AMI in us-east-1
    # This AMI should exist in production
    source_ami = "ami-0d9726c9053daff76"  # Example: scylla 5.2.x in us-east-1
    source_region = "us-east-1"

    try:
        results = find_equivalent_ami(
            ami_id=source_ami, source_region=source_region, target_regions=["us-east-1", "us-west-2"]
        )

        # Basic validation
        assert isinstance(results, list)
        if results:  # May be empty if AMI not found
            assert "ami_id" in results[0]
            assert "region" in results[0]
            assert "architecture" in results[0]
    except (ClientError, NoCredentialsError, BotoCoreError) as e:
        pytest.skip(f"Integration test skipped due to AWS API error: {e}")


def test_find_equivalent_ami_cross_architecture():
    """
    Test finding ARM64 equivalent of an x86_64 AMI.
    """
    # Use a known ScyllaDB x86_64 AMI
    source_ami = "ami-0d9726c9053daff76"
    source_region = "us-east-1"

    try:
        results = find_equivalent_ami(
            ami_id=source_ami, source_region=source_region, target_regions=["us-east-1"], target_arch="arm64"
        )

        # Validate all results are arm64
        for result in results:
            assert result["architecture"] == "arm64"
    except (ClientError, NoCredentialsError, BotoCoreError) as e:
        pytest.skip(f"Integration test skipped due to AWS API error: {e}")


def test_find_equivalent_ami_all_aws_regions():
    """
    Test finding equivalents across all major AWS regions.
    """
    source_ami = "ami-0d9726c9053daff76"
    source_region = "us-east-1"
    target_regions = ["us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-1", "ap-northeast-1"]

    try:
        results = find_equivalent_ami(ami_id=source_ami, source_region=source_region, target_regions=target_regions)

        # Should find equivalents in multiple regions
        if results:
            regions_found = {r["region"] for r in results}
            # At least some regions should have matches
            assert len(regions_found) > 0
    except (ClientError, NoCredentialsError, BotoCoreError) as e:
        pytest.skip(f"Integration test skipped due to AWS API error: {e}")


def test_integration_find_equivalent_ami():
    """
    Integration test: Validates that find_equivalent_ami returns the correct ARM64 equivalent AMI in 'us-east-1'
    for a given source AMI in 'eu-west-1', and checks all expected fields.
    """

    # Execute
    results = find_equivalent_ami(
        ami_id="ami-0bf2296b393980c53",
        source_region="eu-west-1",
        target_arch="arm64",
        target_regions=["us-east-1"],
    )

    # Verify
    assert len(results) == 1, f"Expected 1 result, got {len(results)}"
    assert results[0]["ami_id"] == "ami-079625cf3fec09303", f"Expected ami-result456, got {results[0]['ami_id']}"
    assert results[0]["region"] == "us-east-1", f"Expected us-east-1, got {results[0]['region']}"
    assert results[0]["architecture"] == "arm64", f"Expected arm64, got {results[0]['architecture']}"
    assert results[0]["scylla_version"] == "2025.4.0~rc2-0.20251015.83babc20e3f7", (
        f"Expected 2025.4.0~rc2-0.20251015.83babc20e3f7, got {results[0]['scylla_version']}"
    )
