#!/usr/bin/env python3

"""
Unit tests for AWS cluster functionality.
"""

import pytest
from unittest.mock import Mock, patch
import botocore.exceptions

from sdcm.cluster import BaseCluster, NodeError
from sdcm.cluster_aws import AWSNode, VectorStoreAWSNode
from sdcm.provision.aws.capacity_errors import InstanceTerminatedAtLaunchError
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


def make_waiter_error(name: str = "InstanceRunning") -> botocore.exceptions.WaiterError:
    return botocore.exceptions.WaiterError(name=name, reason="Max attempts exceeded", last_response={})


def make_wait_node(state_name: str, reason_code: str | None = None, reason_message: str = "") -> AWSNode:
    """A real AWSNode whose EC2 instance reports the given state and StateReason."""
    node = AWSNode.__new__(AWSNode)
    node._instance = Mock(id="i-0123456789abcdef0")
    node._instance.state = {"Name": state_name}
    node._instance.state_reason = {"Code": reason_code, "Message": reason_message} if reason_code else None
    node.scylla_network_configuration = None
    node.running_nemesis = None
    node.name = "longevity-test-db-node-1"
    return node


def make_failing_waiter(error: Exception) -> Mock:
    waiter = Mock(side_effect=error)
    waiter.__name__ = "wait_until_running"
    return waiter


@pytest.mark.parametrize(
    ("state_name", "reason_code"),
    [
        pytest.param("terminated", "Server.InternalError", id="terminated_internal_error"),
        pytest.param("shutting-down", "Server.InsufficientInstanceCapacity", id="shutting_down_capacity"),
    ],
)
def test_instance_wait_safe_launch_failure_raises_without_retry(state_name, reason_code, no_retry_backoff):
    """A launch failure reported by AWS is final, so the waiter must not run again."""
    node = make_wait_node(state_name, reason_code, "Internal error on launch")
    waiter = make_failing_waiter(make_waiter_error())

    with pytest.raises(InstanceTerminatedAtLaunchError, match=reason_code):
        node._instance_wait_safe(waiter)

    waiter.assert_called_once()
    no_retry_backoff.assert_not_called()


def test_instance_wait_safe_terminated_with_client_reason_raises_node_error(no_retry_backoff):
    """An instance terminated by the client is not an AWS launch failure and must not trigger AZ fallback."""
    node = make_wait_node("terminated", "Client.UserInitiatedShutdown", "User initiated shutdown")
    waiter = make_failing_waiter(make_waiter_error())

    with pytest.raises(NodeError, match="Client.UserInitiatedShutdown") as exc_info:
        node._instance_wait_safe(waiter)

    assert not isinstance(exc_info.value, InstanceTerminatedAtLaunchError)
    waiter.assert_called_once()


def test_instance_wait_safe_waiter_error_on_pending_instance_retries(no_retry_backoff):
    """A waiter timeout on an instance that is still alive keeps the existing retry behavior."""
    node = make_wait_node("pending")
    waiter = Mock(side_effect=[make_waiter_error(), None])
    waiter.__name__ = "wait_until_running"

    node._instance_wait_safe(waiter)

    assert waiter.call_count == 2
    no_retry_backoff.assert_called_once()


def test_init_instance_terminated_at_launch_destroys_node_without_public_ip_wait():
    """init() must destroy the node and give up at once, instead of polling for a public IP forever."""
    node = make_wait_node("terminated", "Server.InternalError", "Internal error on launch")
    node._instance.wait_until_running.side_effect = make_waiter_error()
    node._instance.wait_until_running.__name__ = "wait_until_running"
    parent_cluster = Mock(dead_nodes_list=[], dead_nodes_ip_address_list=[], nodes=[node])
    parent_cluster.test_config.IP_SSH_CONNECTIONS = "private"
    parent_cluster.terminate_node = lambda terminated, scylla_shards=0: BaseCluster.terminate_node(
        parent_cluster, terminated, scylla_shards
    )
    node.parent_cluster = parent_cluster
    node.__dict__["test_config"] = Mock(IP_SSH_CONNECTIONS="private", INTRA_NODE_COMM_PUBLIC=False, REUSE_CLUSTER=False)

    with (
        patch("sdcm.cluster_aws.ec2_instance_wait_public_ip") as wait_public_ip,
        patch.object(AWSNode, "destroy") as destroy,
        patch.object(AWSNode, "scylla_shards", 0),
        patch("sdcm.cluster.ScyllaLogCollector"),
        patch("sdcm.cluster.DbNodeLogger"),
        pytest.raises(InstanceTerminatedAtLaunchError),
    ):
        node.init()

    wait_public_ip.assert_not_called()
    destroy.assert_called_once()
    assert len(parent_cluster.dead_nodes_list) == 1
    assert parent_cluster.dead_nodes_list[0].private_ip is None


VECTOR_STORE_PASSWORD_FILE = "/home/ubuntu/vector-store/.scylla_password"


@pytest.fixture
def vector_store_aws_node():
    """Vector Store AWS node with a remoter that records the commands it runs."""
    node = VectorStoreAWSNode.__new__(VectorStoreAWSNode)
    node.parent_cluster = Mock(scylla_cluster=None)
    node.parent_cluster.params = {
        "vector_store_port": 6080,
        "vector_store_threads": 0,
        "vector_store_scylla_username": "vs_user",
        "vector_store_scylla_password": "vs_password",
        "ami_vector_store_user": "vs-service",
    }
    node.remoter = Mock()
    return node


def test_configure_vector_store_service_password_file_readable_by_service_user(vector_store_aws_node):
    """The vector-store service runs as ami_vector_store_user and must read the owner-only password file.

    The password must not reach the SCT log.
    """
    vector_store_aws_node.configure_vector_store_service()

    password_calls = [
        call for call in vector_store_aws_node.remoter.run.call_args_list if "vs_password" in call.args[0]
    ]
    assert len(password_calls) == 1
    command = password_calls[0].args[0]
    assert f"sudo chown vs-service: {VECTOR_STORE_PASSWORD_FILE}" in command
    assert f"sudo chmod 600 {VECTOR_STORE_PASSWORD_FILE}" in command
    assert password_calls[0].kwargs["verbose"] is False
