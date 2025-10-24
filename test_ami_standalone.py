#!/usr/bin/env python3
"""
Standalone test script for find_ami_equivalent function.
This can be run without the full SCT test infrastructure.
"""

import sys
import json
from unittest.mock import Mock, patch

# Add sdcm to path
sys.path.insert(0, '/home/runner/work/scylla-cluster-tests/scylla-cluster-tests')

from sdcm.utils.common import find_equivalent_ami


def test_find_equivalent_ami_basic():
    """Basic test for find_equivalent_ami function."""
    
    with patch('sdcm.utils.common.boto3.resource') as mock_boto3_resource, \
         patch('sdcm.utils.common.get_scylla_images_ec2_resource') as mock_get_scylla_resource:
        
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
        assert len(results) == 1, f"Expected 1 result, got {len(results)}"
        assert results[0]['ami_id'] == 'ami-result456', f"Expected ami-result456, got {results[0]['ami_id']}"
        assert results[0]['region'] == 'us-east-1', f"Expected us-east-1, got {results[0]['region']}"
        assert results[0]['architecture'] == 'x86_64', f"Expected x86_64, got {results[0]['architecture']}"
        assert results[0]['scylla_version'] == '5.2.0', f"Expected 5.2.0, got {results[0]['scylla_version']}"
        
        print("✓ test_find_equivalent_ami_basic PASSED")


def test_find_equivalent_ami_no_tags():
    """Test behavior when source AMI has no tags."""
    
    with patch('sdcm.utils.common.boto3.resource') as mock_boto3_resource:
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
        
        assert results == [], f"Expected empty list, got {results}"
        print("✓ test_find_equivalent_ami_no_tags PASSED")


def test_find_equivalent_ami_not_found():
    """Test behavior when source AMI cannot be loaded."""
    
    with patch('sdcm.utils.common.boto3.resource') as mock_boto3_resource:
        mock_resource = Mock()
        mock_ami = Mock()
        mock_ami.load.side_effect = Exception("AMI not found")
        mock_resource.Image.return_value = mock_ami
        mock_boto3_resource.return_value = mock_resource
        
        results = find_equivalent_ami(
            ami_id='ami-notfound',
            source_region='us-east-1'
        )
        
        assert results == [], f"Expected empty list, got {results}"
        print("✓ test_find_equivalent_ami_not_found PASSED")


if __name__ == '__main__':
    print("Running standalone tests for find_ami_equivalent...")
    print()
    
    try:
        test_find_equivalent_ami_basic()
        test_find_equivalent_ami_no_tags()
        test_find_equivalent_ami_not_found()
        
        print()
        print("=" * 60)
        print("All tests PASSED! ✓")
        print("=" * 60)
        sys.exit(0)
    except AssertionError as e:
        print()
        print("=" * 60)
        print(f"Test FAILED: {e}")
        print("=" * 60)
        sys.exit(1)
    except Exception as e:
        print()
        print("=" * 60)
        print(f"Test ERROR: {e}")
        import traceback
        traceback.print_exc()
        print("=" * 60)
        sys.exit(1)
