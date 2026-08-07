#!/usr/bin/env python3

"""
Unit tests for AWS cluster functionality.
"""

import pytest
from unittest.mock import Mock, patch
import botocore.exceptions

from sdcm.cluster_aws import AWSNode
from sdcm.sct_events import Severity


def make_client_error(code: str, message: str = "") -> botocore.exceptions.ClientError:
    """Build a botocore ClientError for the ReleaseAddress operation."""
    error_response = {"Error": {"Code": code, "Message": message}}
    return botocore.exceptions.ClientError(error_response, "ReleaseAddress")


@pytest.fixture
def no_retry_backoff():
    """Make exponential_retry's backoff instant, so retry tests don't sleep for the whole budget."""
    with patch("tenacity.nap.time.sleep") as mock_sleep:
        yield mock_sleep


@pytest.fixture
def mock_aws_node():
    """Create a mock AWSNode for testing."""
    # Mock the essential components for AWSNode
    mock_instance = Mock()
    mock_instance.wait_until_terminated.return_value = None

    mock_parent_cluster = Mock()
    mock_parent_cluster.region_names = ["us-east-1"]

    # Create a minimal AWSNode mock with the required attributes
    aws_node = Mock(spec=AWSNode)
    aws_node._instance = mock_instance
    aws_node.parent_cluster = mock_parent_cluster
    aws_node.dc_idx = 0
    aws_node.eip_allocation_id = "eipalloc-05bed6a4528b5369f"
    aws_node.log = Mock()
    aws_node.name = "longevity-test-db-node-1"

    # Bind the actual release_address method from AWSNode to our mock
    aws_node.release_address = AWSNode.release_address.__get__(aws_node, AWSNode)

    return aws_node


class TestAWSNodeReleaseAddress:
    """Test cases for AWS node release_address functionality."""

    @patch("boto3.client")
    def test_release_address_invalid_allocation_id_ignored(self, mock_boto3_client, mock_aws_node):
        """Test that InvalidAllocationID.NotFound error is properly caught and ignored."""
        # Setup
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client

        # Create the specific error that should be caught
        error_response = {
            "Error": {
                "Code": "InvalidAllocationID.NotFound",
                "Message": "The allocation ID 'eipalloc-05bed6a4528b5369f' does not exist",
            }
        }
        client_error = botocore.exceptions.ClientError(error_response, "ReleaseAddress")
        mock_client.release_address.side_effect = client_error

        # Execute - should not raise an exception
        mock_aws_node.release_address()

        # Verify
        mock_boto3_client.assert_called_once_with("ec2", region_name="us-east-1")
        mock_client.release_address.assert_called_once_with(AllocationId="eipalloc-05bed6a4528b5369f")
        mock_aws_node._instance.wait_until_terminated.assert_called_once()

        # Verify that warning was logged
        mock_aws_node.log.warning.assert_called_once()
        warning_call_args = mock_aws_node.log.warning.call_args[0]
        # Check the message format
        assert "Ignoring InvalidAllocationID.NotFound error" in warning_call_args[0]
        # Check that the exception and allocation ID are passed as arguments
        assert warning_call_args[1] is client_error
        assert warning_call_args[2] == "eipalloc-05bed6a4528b5369f"

    @patch("boto3.client")
    def test_release_address_other_error_propagated(self, mock_boto3_client, mock_aws_node):
        """Test that other ClientError exceptions are still propagated."""
        # Setup
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client

        # Create a different error that should not be caught
        error_response = {
            "Error": {"Code": "UnauthorizedOperation", "Message": "You are not authorized to perform this operation"}
        }
        client_error = botocore.exceptions.ClientError(error_response, "ReleaseAddress")
        mock_client.release_address.side_effect = client_error

        # Execute - should raise the exception
        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            mock_aws_node.release_address()

        # Verify
        assert exc_info.value.response["Error"]["Code"] == "UnauthorizedOperation"
        mock_boto3_client.assert_called_once_with("ec2", region_name="us-east-1")
        mock_client.release_address.assert_called_once_with(AllocationId="eipalloc-05bed6a4528b5369f")
        mock_aws_node._instance.wait_until_terminated.assert_called_once()
        # Verify that warning was NOT logged for other errors
        mock_aws_node.log.warning.assert_not_called()


@patch("boto3.client")
def test_release_address_retries_transient_network_interface_error(mock_boto3_client, mock_aws_node, no_retry_backoff):
    """The EIP is released once AWS cleans up the stale association of the already deleted ENI."""
    mock_client = Mock()
    mock_boto3_client.return_value = mock_client
    transient_error = make_client_error(
        "InvalidNetworkInterfaceID.NotFound", "The networkInterface ID 'eni-00bc825c56fb32e96' does not exist"
    )
    mock_client.release_address.side_effect = [transient_error, transient_error, None]

    mock_aws_node.release_address()

    assert mock_client.release_address.call_count == 3
    mock_boto3_client.assert_called_once_with("ec2", region_name="us-east-1")


@patch("boto3.client")
def test_release_address_raises_original_error_when_retries_exhausted(
    mock_boto3_client, mock_aws_node, no_retry_backoff
):
    """When the transient error never clears, the AWS error is surfaced, not tenacity's RetryError wrapper."""
    mock_client = Mock()
    mock_boto3_client.return_value = mock_client
    mock_client.release_address.side_effect = make_client_error("InvalidIPAddress.InUse", "Address is in use")

    with pytest.raises(botocore.exceptions.ClientError) as exc_info:
        mock_aws_node.release_address()

    assert exc_info.value.response["Error"]["Code"] == "InvalidIPAddress.InUse"
    assert mock_client.release_address.call_count == 8
    assert sum(call.args[0] for call in no_retry_backoff.call_args_list) == pytest.approx(54)


@pytest.mark.parametrize(
    "release_error",
    [
        make_client_error("InvalidNetworkInterfaceID.NotFound", "does not exist"),
        botocore.exceptions.EndpointConnectionError(endpoint_url="https://ec2.us-east-1.amazonaws.com"),
    ],
    ids=["transient_error", "connection_error"],
)
def test_destroy_continues_when_release_address_fails(mock_aws_node, release_error):
    """A failed EIP release must not abort destroy() after instance termination."""
    mock_aws_node.destroy = AWSNode.destroy.__get__(mock_aws_node, AWSNode)
    mock_aws_node.release_address = Mock(side_effect=release_error)

    with (
        patch("sdcm.cluster.BaseNode.destroy") as mock_base_destroy,
        patch("sdcm.cluster_aws.TestFrameworkEvent") as mock_event,
    ):
        mock_aws_node.destroy()

    mock_base_destroy.assert_called_once()
    mock_event.assert_called_once()
    assert mock_event.call_args.kwargs["severity"] is Severity.WARNING
    assert "eipalloc-05bed6a4528b5369f" in mock_event.call_args.kwargs["message"]
    mock_event.return_value.publish_or_dump.assert_called_once()
