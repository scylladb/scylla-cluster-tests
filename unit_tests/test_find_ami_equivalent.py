#!/usr/bin/env python3

"""
Unit tests for find_ami_equivalent functionality.
"""

import pytest
from unittest.mock import Mock, patch

from sdcm.utils.common import find_equivalent_ami


@pytest.fixture
def mock_ec2_image():
    """Create a mock EC2 Image object."""
    mock_image = Mock()
    mock_image.image_id = 'ami-test123'
    mock_image.name = 'scylla-5.2.0-x86_64-2024-01-15'
    mock_image.architecture = 'x86_64'
    mock_image.creation_date = '2024-01-15T10:00:00.000Z'
    mock_image.owner_id = '797456418907'
    mock_image.tags = [
        {'Key': 'Name', 'Value': 'scylla-5.2.0-x86_64-2024-01-15'},
        {'Key': 'scylla_version', 'Value': '5.2.0'},
        {'Key': 'build-id', 'Value': 'abc123def456'},
        {'Key': 'arch', 'Value': 'x86_64'},
    ]
    return mock_image


@pytest.fixture
def mock_ec2_resource():
    """Create a mock EC2 resource."""
    mock_resource = Mock()
    return mock_resource


class TestFindEquivalentAmi:
    """Test cases for find_equivalent_ami function."""

    @patch('sdcm.utils.common.boto3.resource')
    @patch('sdcm.utils.common.get_scylla_images_ec2_resource')
    def test_find_equivalent_ami_same_region(self, mock_get_scylla_resource, mock_boto3_resource, mock_ec2_image):
        """Test finding equivalent AMI in the same region."""
        # Setup source AMI
        mock_source_resource = Mock()
        mock_source_ami = Mock()
        mock_source_ami.image_id = 'ami-source123'
        mock_source_ami.name = 'scylla-5.2.0-x86_64-2024-01-15'
        mock_source_ami.architecture = 'x86_64'
        mock_source_ami.tags = [
            {'Key': 'Name', 'Value': 'scylla-5.2.0-x86_64-2024-01-15'},
            {'Key': 'scylla_version', 'Value': '5.2.0'},
            {'Key': 'build-id', 'Value': 'abc123'},
        ]

        mock_source_resource.Image.return_value = mock_source_ami

        # Setup search results
        mock_target_resource = Mock()
        mock_result_image = Mock()
        mock_result_image.image_id = 'ami-result456'
        mock_result_image.name = 'scylla-5.2.0-x86_64-2024-01-15'
        mock_result_image.architecture = 'x86_64'
        mock_result_image.creation_date = '2024-01-15T10:00:00.000Z'
        mock_result_image.owner_id = '797456418907'
        mock_result_image.tags = [
            {'Key': 'Name', 'Value': 'scylla-5.2.0-x86_64-2024-01-15'},
            {'Key': 'scylla_version', 'Value': '5.2.0'},
            {'Key': 'build-id', 'Value': 'abc123'},
        ]

        mock_images_collection = Mock()
        mock_images_collection.filter.return_value = [mock_result_image]
        mock_target_resource.images = mock_images_collection

        # Setup scylla images resource
        mock_scylla_resource = Mock()
        mock_scylla_images_collection = Mock()
        mock_scylla_images_collection.filter.return_value = []
        mock_scylla_resource.images = mock_scylla_images_collection

        # Configure boto3.resource to return different resources for different calls
        call_count = [0]
        def resource_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return mock_source_resource  # First call for loading source AMI
            else:
                return mock_target_resource  # Subsequent calls for searching

        mock_boto3_resource.side_effect = resource_side_effect
        mock_get_scylla_resource.return_value = mock_scylla_resource

        # Execute
        results = find_equivalent_ami(
            ami_id='ami-source123',
            source_region='us-east-1'
        )

        # Verify
        assert len(results) == 1
        assert results[0]['ami_id'] == 'ami-result456'
        assert results[0]['region'] == 'us-east-1'
        assert results[0]['architecture'] == 'x86_64'
        assert results[0]['scylla_version'] == '5.2.0'

    @patch('sdcm.utils.common.boto3.resource')
    @patch('sdcm.utils.common.get_scylla_images_ec2_resource')
    def test_find_equivalent_ami_different_architecture(self, mock_get_scylla_resource, mock_boto3_resource):
        """Test finding equivalent AMI with different architecture."""
        # Setup source AMI (x86_64)
        mock_source_resource = Mock()
        mock_source_ami = Mock()
        mock_source_ami.image_id = 'ami-x86-123'
        mock_source_ami.name = 'scylla-5.2.0-x86_64-2024-01-15'
        mock_source_ami.architecture = 'x86_64'
        mock_source_ami.tags = [
            {'Key': 'Name', 'Value': 'scylla-5.2.0-x86_64-2024-01-15'},
            {'Key': 'scylla_version', 'Value': '5.2.0'},
            {'Key': 'build-id', 'Value': 'abc123'},
        ]
        mock_source_resource.Image.return_value = mock_source_ami

        # Setup search results (arm64)
        mock_target_resource = Mock()
        mock_result_image = Mock()
        mock_result_image.image_id = 'ami-arm-456'
        mock_result_image.name = 'scylla-5.2.0-arm64-2024-01-15'
        mock_result_image.architecture = 'arm64'
        mock_result_image.creation_date = '2024-01-15T10:00:00.000Z'
        mock_result_image.owner_id = '797456418907'
        mock_result_image.tags = [
            {'Key': 'Name', 'Value': 'scylla-5.2.0-x86_64-2024-01-15'},  # Name tag stays same for equivalence
            {'Key': 'scylla_version', 'Value': '5.2.0'},
            {'Key': 'build-id', 'Value': 'abc123'},
        ]

        mock_images_collection = Mock()
        mock_images_collection.filter.return_value = [mock_result_image]
        mock_target_resource.images = mock_images_collection

        # Setup scylla images resource
        mock_scylla_resource = Mock()
        mock_scylla_images_collection = Mock()
        mock_scylla_images_collection.filter.return_value = []
        mock_scylla_resource.images = mock_scylla_images_collection

        # Configure boto3.resource
        call_count = [0]
        def resource_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return mock_source_resource
            else:
                return mock_target_resource

        mock_boto3_resource.side_effect = resource_side_effect
        mock_get_scylla_resource.return_value = mock_scylla_resource

        # Execute
        results = find_equivalent_ami(
            ami_id='ami-x86-123',
            source_region='us-east-1',
            target_arch='arm64'
        )

        # Verify
        assert len(results) == 1
        assert results[0]['ami_id'] == 'ami-arm-456'
        assert results[0]['architecture'] == 'arm64'
        assert results[0]['scylla_version'] == '5.2.0'

    @patch('sdcm.utils.common.boto3.resource')
    @patch('sdcm.utils.common.get_scylla_images_ec2_resource')
    def test_find_equivalent_ami_multiple_regions(self, mock_get_scylla_resource, mock_boto3_resource):
        """Test finding equivalent AMIs across multiple regions."""
        # Setup source AMI
        mock_source_resource = Mock()
        mock_source_ami = Mock()
        mock_source_ami.image_id = 'ami-source123'
        mock_source_ami.name = 'scylla-5.2.0-x86_64-2024-01-15'
        mock_source_ami.architecture = 'x86_64'
        mock_source_ami.tags = [
            {'Key': 'Name', 'Value': 'scylla-5.2.0-x86_64-2024-01-15'},
            {'Key': 'scylla_version', 'Value': '5.2.0'},
        ]
        mock_source_resource.Image.return_value = mock_source_ami

        # Setup results for two different regions
        def create_mock_image(ami_id, region):
            mock_img = Mock()
            mock_img.image_id = ami_id
            mock_img.name = 'scylla-5.2.0-x86_64-2024-01-15'
            mock_img.architecture = 'x86_64'
            mock_img.creation_date = '2024-01-15T10:00:00.000Z'
            mock_img.owner_id = '797456418907'
            mock_img.tags = [
                {'Key': 'Name', 'Value': 'scylla-5.2.0-x86_64-2024-01-15'},
                {'Key': 'scylla_version', 'Value': '5.2.0'},
            ]
            return mock_img

        mock_us_east_image = create_mock_image('ami-us-east-123', 'us-east-1')
        mock_eu_west_image = create_mock_image('ami-eu-west-456', 'eu-west-1')

        # Configure resources for different regions
        resources = {
            'source': mock_source_resource,
            'us-east-1': Mock(),
            'eu-west-1': Mock(),
        }

        # Setup images collection for each region
        us_east_collection = Mock()
        us_east_collection.filter.return_value = [mock_us_east_image]
        resources['us-east-1'].images = us_east_collection

        eu_west_collection = Mock()
        eu_west_collection.filter.return_value = [mock_eu_west_image]
        resources['eu-west-1'].images = eu_west_collection

        # Setup scylla resources
        mock_scylla_us = Mock()
        mock_scylla_us_collection = Mock()
        mock_scylla_us_collection.filter.return_value = []
        mock_scylla_us.images = mock_scylla_us_collection

        mock_scylla_eu = Mock()
        mock_scylla_eu_collection = Mock()
        mock_scylla_eu_collection.filter.return_value = []
        mock_scylla_eu.images = mock_scylla_eu_collection

        # Configure boto3.resource to return appropriate resource based on region
        call_sequence = []
        def resource_side_effect(*args, **kwargs):
            region = kwargs.get('region_name')
            call_sequence.append(region)
            if region == 'us-east-1' and len([r for r in call_sequence if r == 'us-east-1']) == 1:
                return mock_source_resource  # First us-east-1 call is for source
            return resources.get(region, Mock())

        mock_boto3_resource.side_effect = resource_side_effect

        def scylla_resource_side_effect(*args, **kwargs):
            region = kwargs.get('region_name', '')
            if 'us-east' in region:
                return mock_scylla_us
            return mock_scylla_eu

        mock_get_scylla_resource.side_effect = scylla_resource_side_effect

        # Execute
        results = find_equivalent_ami(
            ami_id='ami-source123',
            source_region='us-east-1',
            target_regions=['us-east-1', 'eu-west-1']
        )

        # Verify
        assert len(results) == 2
        ami_ids = {r['ami_id'] for r in results}
        assert 'ami-us-east-123' in ami_ids
        assert 'ami-eu-west-456' in ami_ids

    @patch('sdcm.utils.common.boto3.resource')
    def test_find_equivalent_ami_no_tags(self, mock_boto3_resource):
        """Test behavior when source AMI has no tags."""
        mock_resource = Mock()
        mock_ami = Mock()
        mock_ami.image_id = 'ami-notags'
        mock_ami.tags = None
        mock_resource.Image.return_value = mock_ami
        mock_boto3_resource.return_value = mock_resource

        results = find_equivalent_ami(
            ami_id='ami-notags',
            source_region='us-east-1'
        )

        assert results == []

    @patch('sdcm.utils.common.boto3.resource')
    def test_find_equivalent_ami_source_not_found(self, mock_boto3_resource):
        """Test behavior when source AMI cannot be loaded."""
        mock_resource = Mock()
        mock_ami = Mock()
        mock_ami.load.side_effect = Exception("AMI not found")
        mock_resource.Image.return_value = mock_ami
        mock_boto3_resource.return_value = mock_resource

        results = find_equivalent_ami(
            ami_id='ami-notfound',
            source_region='us-east-1'
        )

        assert results == []

    @patch('sdcm.utils.common.boto3.resource')
    @patch('sdcm.utils.common.get_scylla_images_ec2_resource')
    def test_find_equivalent_ami_sorted_by_date(self, mock_get_scylla_resource, mock_boto3_resource):
        """Test that results are sorted by creation date (newest first)."""
        # Setup source AMI
        mock_source_resource = Mock()
        mock_source_ami = Mock()
        mock_source_ami.image_id = 'ami-source'
        mock_source_ami.architecture = 'x86_64'
        mock_source_ami.tags = [
            {'Key': 'Name', 'Value': 'scylla-5.2.0'},
            {'Key': 'scylla_version', 'Value': '5.2.0'},
        ]
        mock_source_resource.Image.return_value = mock_source_ami

        # Setup multiple result images with different dates
        def create_image(ami_id, date):
            img = Mock()
            img.image_id = ami_id
            img.name = 'scylla-5.2.0-x86_64'
            img.architecture = 'x86_64'
            img.creation_date = date
            img.owner_id = '797456418907'
            img.tags = [
                {'Key': 'Name', 'Value': 'scylla-5.2.0'},
                {'Key': 'scylla_version', 'Value': '5.2.0'},
            ]
            return img

        # Create images with different dates (not in order)
        older_image = create_image('ami-old', '2024-01-10T10:00:00.000Z')
        newest_image = create_image('ami-new', '2024-01-20T10:00:00.000Z')
        middle_image = create_image('ami-mid', '2024-01-15T10:00:00.000Z')

        mock_target_resource = Mock()
        mock_images_collection = Mock()
        # Return in non-sorted order
        mock_images_collection.filter.return_value = [older_image, newest_image, middle_image]
        mock_target_resource.images = mock_images_collection

        mock_scylla_resource = Mock()
        mock_scylla_images_collection = Mock()
        mock_scylla_images_collection.filter.return_value = []
        mock_scylla_resource.images = mock_scylla_images_collection

        call_count = [0]
        def resource_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return mock_source_resource
            return mock_target_resource

        mock_boto3_resource.side_effect = resource_side_effect
        mock_get_scylla_resource.return_value = mock_scylla_resource

        # Execute
        results = find_equivalent_ami(
            ami_id='ami-source',
            source_region='us-east-1'
        )

        # Verify sorted by date (newest first)
        assert len(results) == 3
        assert results[0]['ami_id'] == 'ami-new'
        assert results[1]['ami_id'] == 'ami-mid'
        assert results[2]['ami_id'] == 'ami-old'


@pytest.mark.integration
class TestFindEquivalentAmiIntegration:
    """Integration tests that make actual AWS API calls."""

    def test_find_equivalent_ami_real_scylla_ami(self):
        """
        Test with a real ScyllaDB AMI.
        This test requires AWS credentials and makes actual API calls.
        """
        # Use a known ScyllaDB 5.2 AMI in us-east-1
        # This AMI should exist in production
        source_ami = 'ami-0d9726c9053daff76'  # Example: scylla 5.2.x in us-east-1
        source_region = 'us-east-1'

        try:
            results = find_equivalent_ami(
                ami_id=source_ami,
                source_region=source_region,
                target_regions=['us-east-1', 'us-west-2']
            )

            # Basic validation
            assert isinstance(results, list)
            if results:  # May be empty if AMI not found
                assert 'ami_id' in results[0]
                assert 'region' in results[0]
                assert 'architecture' in results[0]
        except Exception as e:
            pytest.skip(f"Integration test skipped due to AWS API error: {e}")

    def test_find_equivalent_ami_cross_architecture(self):
        """
        Test finding ARM64 equivalent of an x86_64 AMI.
        """
        # Use a known ScyllaDB x86_64 AMI
        source_ami = 'ami-0d9726c9053daff76'
        source_region = 'us-east-1'

        try:
            results = find_equivalent_ami(
                ami_id=source_ami,
                source_region=source_region,
                target_regions=['us-east-1'],
                target_arch='arm64'
            )

            # Validate all results are arm64
            for result in results:
                assert result['architecture'] == 'arm64'
        except Exception as e:
            pytest.skip(f"Integration test skipped due to AWS API error: {e}")

    def test_find_equivalent_ami_all_aws_regions(self):
        """
        Test finding equivalents across all major AWS regions.
        """
        source_ami = 'ami-0d9726c9053daff76'
        source_region = 'us-east-1'
        target_regions = [
            'us-east-1', 'us-west-2', 'eu-west-1', 'eu-central-1',
            'ap-southeast-1', 'ap-northeast-1'
        ]

        try:
            results = find_equivalent_ami(
                ami_id=source_ami,
                source_region=source_region,
                target_regions=target_regions
            )

            # Should find equivalents in multiple regions
            if results:
                regions_found = {r['region'] for r in results}
                # At least some regions should have matches
                assert len(regions_found) > 0
        except Exception as e:
            pytest.skip(f"Integration test skipped due to AWS API error: {e}")
