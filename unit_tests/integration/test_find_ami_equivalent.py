#!/usr/bin/env python3

"""
Integration tests for find_ami_equivalent functionality.
These tests require AWS credentials and make actual API calls.
"""

import pytest

from sdcm.utils.common import find_equivalent_ami


@pytest.mark.integration
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
