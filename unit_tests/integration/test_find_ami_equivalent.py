#!/usr/bin/env python3

"""
Integration tests for find_ami_equivalent functionality.
These tests require AWS credentials and make actual API calls.
"""

import pytest
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError

from sdcm.utils.common import find_equivalent_ami, get_ec2_image_version_tag, get_scylla_ami_versions
from utils.get_supported_scylla_base_versions import fetch_official_supported_versions


@pytest.mark.integration
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


@pytest.mark.integration
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


@pytest.mark.integration
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


@pytest.mark.integration
def test_integration_find_equivalent_ami():
    """
    Integration test: Validates that find_equivalent_ami returns the ARM64 AMI in 'us-east-1' with the same
    Scylla version as the x86_64 source AMI in 'eu-west-1', and checks all expected fields.

    Uses the GA AMI of the oldest officially supported release, and resolves the AMI IDs by version tag.
    """
    release = min(fetch_official_supported_versions(), key=lambda version: tuple(map(int, version.split("."))))
    source_amis = [
        ami
        for ami in get_scylla_ami_versions(region_name="eu-west-1", arch="x86_64", version=f"{release}.0")
        if get_ec2_image_version_tag(ami).startswith(f"{release}.0-")
    ]
    assert len(source_amis) == 1, f"Expected 1 x86_64 AMI of {release}.0 in eu-west-1, got {len(source_amis)}"
    scylla_version = get_ec2_image_version_tag(source_amis[0])
    target_amis = get_scylla_ami_versions(region_name="us-east-1", arch="arm64", version=scylla_version)
    assert len(target_amis) == 1, f"Expected 1 arm64 AMI of {scylla_version} in us-east-1, got {len(target_amis)}"

    # Execute
    results = find_equivalent_ami(
        ami_id=source_amis[0].image_id,
        source_region="eu-west-1",
        target_arch="arm64",
        target_regions=["us-east-1"],
    )

    # Verify
    assert len(results) == 1, f"Expected 1 result, got {len(results)}"
    assert results[0]["ami_id"] == target_amis[0].image_id, (
        f"Expected {target_amis[0].image_id}, got {results[0]['ami_id']}"
    )
    assert results[0]["region"] == "us-east-1", f"Expected us-east-1, got {results[0]['region']}"
    assert results[0]["architecture"] == "arm64", f"Expected arm64, got {results[0]['architecture']}"
    assert results[0]["scylla_version"] == scylla_version, (
        f"Expected {scylla_version}, got {results[0]['scylla_version']}"
    )
