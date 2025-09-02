#!/usr/bin/env python3

"""
Unit tests for AWS cluster functionality.
"""

import pytest
from unittest.mock import Mock, patch
import botocore.exceptions

from sdcm.cluster_aws import AWSNode


@pytest.fixture
def mock_aws_node():
    """Create a mock AWSNode for testing."""
    # Mock the essential components for AWSNode
    mock_instance = Mock()
    mock_instance.wait_until_terminated.return_value = None

    mock_parent_cluster = Mock()
    mock_parent_cluster.region_names = ['us-east-1']

    # Create a minimal AWSNode mock with the required attributes
    aws_node = Mock(spec=AWSNode)
    aws_node._instance = mock_instance
    aws_node.parent_cluster = mock_parent_cluster
    aws_node.dc_idx = 0
    aws_node.eip_allocation_id = 'eipalloc-05bed6a4528b5369f'
    aws_node.log = Mock()

    # Bind the actual release_address method from AWSNode to our mock
    aws_node.release_address = AWSNode.release_address.__get__(
        aws_node, AWSNode
    )

    return aws_node


class TestAWSNodeReleaseAddress:
    """Test cases for AWS node release_address functionality."""

    @patch('boto3.client')
    def test_release_address_invalid_allocation_id_ignored(self, mock_boto3_client, mock_aws_node):
        """Test that InvalidAllocationID.NotFound error is properly caught and ignored."""
        # Setup
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client

        # Create the specific error that should be caught
        error_response = {
            'Error': {
                'Code': 'InvalidAllocationID.NotFound',
                'Message': "The allocation ID 'eipalloc-05bed6a4528b5369f' does not exist"
            }
        }
        client_error = botocore.exceptions.ClientError(error_response, 'ReleaseAddress')
        mock_client.release_address.side_effect = client_error

        # Execute - should not raise an exception
        mock_aws_node.release_address()

        # Verify
        mock_boto3_client.assert_called_once_with('ec2', region_name='us-east-1')
        mock_client.release_address.assert_called_once_with(
            AllocationId='eipalloc-05bed6a4528b5369f'
        )
        mock_aws_node._instance.wait_until_terminated.assert_called_once()

        # Verify that warning was logged
        mock_aws_node.log.warning.assert_called_once()
        warning_call_args = mock_aws_node.log.warning.call_args[0]
        # Check the message format
        assert "Ignoring InvalidAllocationID.NotFound error" in warning_call_args[0]
        # Check that the exception and allocation ID are passed as arguments
        assert warning_call_args[1] is client_error
        assert warning_call_args[2] == 'eipalloc-05bed6a4528b5369f'

    @patch('boto3.client')
    def test_release_address_other_error_propagated(self, mock_boto3_client, mock_aws_node):
        """Test that other ClientError exceptions are still propagated."""
        # Setup
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client

        # Create a different error that should not be caught
        error_response = {
            'Error': {
                'Code': 'UnauthorizedOperation',
                'Message': "You are not authorized to perform this operation"
            }
        }
        client_error = botocore.exceptions.ClientError(error_response, 'ReleaseAddress')
        mock_client.release_address.side_effect = client_error

        # Execute - should raise the exception
        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            mock_aws_node.release_address()

        # Verify
        assert exc_info.value.response['Error']['Code'] == 'UnauthorizedOperation'
        mock_boto3_client.assert_called_once_with('ec2', region_name='us-east-1')
        mock_client.release_address.assert_called_once_with(
            AllocationId='eipalloc-05bed6a4528b5369f'
        )
        mock_aws_node._instance.wait_until_terminated.assert_called_once()
        # Verify that warning was NOT logged for other errors
        mock_aws_node.log.warning.assert_not_called()
