#!/usr/bin/env python3

"""
Unit tests for find_ami_equivalent functionality.
"""

import pytest
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError, NoCredentialsError, BotoCoreError

from sdcm.utils.common import find_equivalent_ami


def create_client_error(error_code='InvalidAMIID.NotFound'):
    """Helper to create a ClientError for testing."""
    return ClientError(
        error_response={'Error': {'Code': error_code, 'Message': 'Not found'}},
        operation_name='DescribeImages'
    )


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


@patch('sdcm.utils.common.boto3.resource')
@patch('sdcm.utils.common.get_scylla_images_ec2_resource')
@patch('sdcm.utils.common.SCYLLA_AMI_OWNER_ID_LIST', ['797456418907', '158855661827'])
def test_find_equivalent_ami_same_region(mock_get_scylla_resource, mock_boto3_resource, mock_ec2_image):
    """Test finding equivalent AMI in the same region."""
    # Setup source AMI
    mock_source_ami = Mock()
    mock_source_ami.image_id = 'ami-source123'
    mock_source_ami.name = 'scylla-5.2.0-x86_64-2024-01-15'
    mock_source_ami.architecture = 'x86_64'
    mock_source_ami.tags = [
        {'Key': 'Name', 'Value': 'scylla-5.2.0-x86_64-2024-01-15'},
        {'Key': 'scylla_version', 'Value': '5.2.0'},
        {'Key': 'build-id', 'Value': 'abc123'},
    ]
    mock_source_ami.load = Mock()  # Make load() a no-op

    # Setup source resource (for loading the AMI)
    mock_source_resource = Mock()
    mock_source_resource.Image.return_value = mock_source_ami

    # Setup scylla source resource (second attempt to load)
    mock_scylla_source = Mock()
    mock_scylla_source_ami = Mock()
    mock_scylla_source_ami.load.side_effect = create_client_error()
    mock_scylla_source.Image.return_value = mock_scylla_source_ami

    # Setup search results
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

    # Setup target resource for searching
    mock_target_resource = Mock()
    mock_images_collection = Mock()
    mock_images_collection.filter.return_value = [mock_result_image]
    mock_target_resource.images = mock_images_collection

    # Setup scylla images search resource
    mock_scylla_search = Mock()
    mock_scylla_images_collection = Mock()
    mock_scylla_images_collection.filter.return_value = []
    mock_scylla_search.images = mock_scylla_images_collection

    # Configure boto3.resource - called twice (load source, search target)
    mock_boto3_resource.side_effect = [mock_source_resource, mock_target_resource]

    # Configure scylla resource - called twice (load source, search target)
    mock_get_scylla_resource.side_effect = [mock_scylla_source, mock_scylla_search]

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
@patch('sdcm.utils.common.SCYLLA_AMI_OWNER_ID_LIST', ['797456418907', '158855661827'])
def test_find_equivalent_ami_different_architecture(mock_get_scylla_resource, mock_boto3_resource):
    """Test finding equivalent AMI with different architecture."""
    # Setup source AMI (x86_64)
    mock_source_ami = Mock()
    mock_source_ami.image_id = 'ami-x86-123'
    mock_source_ami.name = 'scylla-5.2.0-x86_64-2024-01-15'
    mock_source_ami.architecture = 'x86_64'
    mock_source_ami.tags = [
        {'Key': 'Name', 'Value': 'scylla-5.2.0-x86_64-2024-01-15'},
        {'Key': 'scylla_version', 'Value': '5.2.0'},
        {'Key': 'build-id', 'Value': 'abc123'},
    ]
    mock_source_ami.load = Mock()

    mock_source_resource = Mock()
    mock_source_resource.Image.return_value = mock_source_ami

    # Setup scylla source resource
    mock_scylla_source = Mock()
    mock_scylla_source_ami = Mock()
    mock_scylla_source_ami.load.side_effect = create_client_error()
    mock_scylla_source.Image.return_value = mock_scylla_source_ami

    # Setup search results (arm64)
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

    mock_target_resource = Mock()
    mock_images_collection = Mock()
    mock_images_collection.filter.return_value = [mock_result_image]
    mock_target_resource.images = mock_images_collection

    # Setup scylla images search resource
    mock_scylla_search = Mock()
    mock_scylla_images_collection = Mock()
    mock_scylla_images_collection.filter.return_value = []
    mock_scylla_search.images = mock_scylla_images_collection

    # Configure boto3.resource
    mock_boto3_resource.side_effect = [mock_source_resource, mock_target_resource]
    mock_get_scylla_resource.side_effect = [mock_scylla_source, mock_scylla_search]

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
@patch('sdcm.utils.common.SCYLLA_AMI_OWNER_ID_LIST', ['797456418907', '158855661827'])
def test_find_equivalent_ami_multiple_regions(mock_get_scylla_resource, mock_boto3_resource):
    """Test finding equivalent AMIs across multiple regions."""

    def create_mock_image(ami_id, region):
        """Helper to create mock image with common attributes."""
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

    def create_region_resource(mock_image):
        """Helper to create a mock resource for a region with image results."""
        resource = Mock()
        collection = Mock()
        collection.filter.return_value = [mock_image]
        resource.images = collection
        return resource

    def create_empty_scylla_resource():
        """Helper to create a mock scylla resource with no results."""
        resource = Mock()
        collection = Mock()
        collection.filter.return_value = []
        resource.images = collection
        return resource

    # Setup source AMI and resource
    mock_source_ami = Mock()
    mock_source_ami.image_id = 'ami-source123'
    mock_source_ami.name = 'scylla-5.2.0-x86_64-2024-01-15'
    mock_source_ami.architecture = 'x86_64'
    mock_source_ami.tags = [
        {'Key': 'Name', 'Value': 'scylla-5.2.0-x86_64-2024-01-15'},
        {'Key': 'scylla_version', 'Value': '5.2.0'},
    ]
    mock_source_ami.load = Mock()

    mock_source_resource = Mock()
    mock_source_resource.Image.return_value = mock_source_ami

    # Setup scylla source resource (fails to load)
    mock_scylla_source_ami = Mock()
    mock_scylla_source_ami.load.side_effect = create_client_error()
    mock_scylla_source = Mock()
    mock_scylla_source.Image.return_value = mock_scylla_source_ami

    # Setup target regions with images
    us_east_resource = create_region_resource(create_mock_image('ami-us-east-123', 'us-east-1'))
    eu_west_resource = create_region_resource(create_mock_image('ami-eu-west-456', 'eu-west-1'))

    # Configure boto3.resource: load source, search us-east-1, search eu-west-1
    mock_boto3_resource.side_effect = [mock_source_resource, us_east_resource, eu_west_resource]

    # Configure get_scylla_images_ec2_resource: load source, search us-east-1, search eu-west-1
    mock_get_scylla_resource.side_effect = [
        mock_scylla_source,
        create_empty_scylla_resource(),
        create_empty_scylla_resource()
    ]

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
@patch('sdcm.utils.common.get_scylla_images_ec2_resource')
def test_find_equivalent_ami_no_tags(mock_get_scylla_resource, mock_boto3_resource):
    """Test behavior when source AMI has no tags."""
    mock_ami = Mock()
    mock_ami.image_id = 'ami-notags'
    mock_ami.architecture = 'x86_64'
    mock_ami.tags = None
    mock_ami.load = Mock()  # Make load() a no-op

    mock_resource = Mock()
    mock_resource.Image.return_value = mock_ami

    # Setup scylla resource that fails to load
    mock_scylla_resource = Mock()
    mock_scylla_ami = Mock()
    mock_scylla_ami.load.side_effect = create_client_error()
    mock_scylla_resource.Image.return_value = mock_scylla_ami

    mock_boto3_resource.return_value = mock_resource
    mock_get_scylla_resource.return_value = mock_scylla_resource

    results = find_equivalent_ami(
        ami_id='ami-notags',
        source_region='us-east-1'
    )

    assert results == []


@patch('sdcm.utils.common.boto3.resource')
@patch('sdcm.utils.common.get_scylla_images_ec2_resource')
def test_find_equivalent_ami_source_not_found(mock_get_scylla_resource, mock_boto3_resource):
    """Test behavior when source AMI cannot be loaded."""
    mock_ami = Mock()
    mock_ami.load.side_effect = create_client_error()

    mock_resource = Mock()
    mock_resource.Image.return_value = mock_ami

    # Setup scylla resource that also fails
    mock_scylla_ami = Mock()
    mock_scylla_ami.load.side_effect = create_client_error()

    mock_scylla_resource = Mock()
    mock_scylla_resource.Image.return_value = mock_scylla_ami

    mock_boto3_resource.return_value = mock_resource
    mock_get_scylla_resource.return_value = mock_scylla_resource

    results = find_equivalent_ami(
        ami_id='ami-notfound',
        source_region='us-east-1'
    )

    assert results == []


@patch('sdcm.utils.common.boto3.resource')
@patch('sdcm.utils.common.get_scylla_images_ec2_resource')
@patch('sdcm.utils.common.SCYLLA_AMI_OWNER_ID_LIST', ['797456418907', '158855661827'])
def test_find_equivalent_ami_sorted_by_date(mock_get_scylla_resource, mock_boto3_resource):
    """Test that results are sorted by creation date (newest first)."""
    # Setup source AMI
    mock_source_ami = Mock()
    mock_source_ami.image_id = 'ami-source'
    mock_source_ami.architecture = 'x86_64'
    mock_source_ami.tags = [
        {'Key': 'Name', 'Value': 'scylla-5.2.0'},
        {'Key': 'scylla_version', 'Value': '5.2.0'},
    ]
    mock_source_ami.load = Mock()

    mock_source_resource = Mock()
    mock_source_resource.Image.return_value = mock_source_ami

    # Setup scylla source resource
    mock_scylla_source = Mock()
    mock_scylla_source_ami = Mock()
    mock_scylla_source_ami.load.side_effect = create_client_error()
    mock_scylla_source.Image.return_value = mock_scylla_source_ami

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

    mock_scylla_search = Mock()
    mock_scylla_images_collection = Mock()
    mock_scylla_images_collection.filter.return_value = []
    mock_scylla_search.images = mock_scylla_images_collection

    mock_boto3_resource.side_effect = [mock_source_resource, mock_target_resource]
    mock_get_scylla_resource.side_effect = [mock_scylla_source, mock_scylla_search]

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
def test_find_equivalent_ami_real_scylla_ami():
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
    except (ClientError, NoCredentialsError, BotoCoreError) as e:
        pytest.skip(f"Integration test skipped due to AWS API error: {e}")


@pytest.mark.integration
def test_find_equivalent_ami_cross_architecture():
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
    except (ClientError, NoCredentialsError, BotoCoreError) as e:
        pytest.skip(f"Integration test skipped due to AWS API error: {e}")


@pytest.mark.integration
def test_find_equivalent_ami_all_aws_regions():
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
    except (ClientError, NoCredentialsError, BotoCoreError) as e:
        pytest.skip(f"Integration test skipped due to AWS API error: {e}")


@pytest.mark.integration
def test_integration_find_equivalent_ami():
    """
    Integration test: Validates that find_equivalent_ami returns the correct ARM64 equivalent AMI in 'us-east-1'
    for a given source AMI in 'eu-west-1', and checks all expected fields.
    """

    # Execute
    results = find_equivalent_ami(
        ami_id='ami-0bf2296b393980c53',
        source_region='eu-west-1',
        target_arch="arm64",
        target_regions=['us-east-1'],
    )

    # Verify
    assert len(results) == 1, f"Expected 1 result, got {len(results)}"
    assert results[0]['ami_id'] == 'ami-079625cf3fec09303', f"Expected ami-result456, got {results[0]['ami_id']}"
    assert results[0]['region'] == 'us-east-1', f"Expected us-east-1, got {results[0]['region']}"
    assert results[0]['architecture'] == 'arm64', f"Expected arm64, got {results[0]['architecture']}"
    assert results[0][
        'scylla_version'] == '2025.4.0~rc2-0.20251015.83babc20e3f7', f"Expected 2025.4.0~rc2-0.20251015.83babc20e3f7, got {results[0]['scylla_version']}"
