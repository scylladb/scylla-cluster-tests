"""Unit tests for cluster_cloud module."""

from unittest.mock import MagicMock, patch
import pytest

from sdcm.cluster_cloud import xcloud_super_if_supported, ScyllaCloudCluster
from sdcm.utils.cloud_api_utils import build_cloud_cluster_name
from sdcm.exceptions import WaitForTimeoutError


class TestXCloudSuperIfSupportedDecorator:
    """Test suite for xcloud_super_if_supported decorator."""

    def test_decorator_calls_base_method_when_supported(self):
        """Test that decorator calls the base class method when xcloud_connect_supported is True."""

        class BaseNode:
            def test_method(self, value):
                return f"BaseNode.test_method({value})"

        class CloudNode(BaseNode):
            @xcloud_super_if_supported
            def test_method(self, value):
                pass  # This should never be called

        # Create instance with xcloud support enabled
        node = CloudNode()
        node.xcloud_connect_supported = True
        node.log = MagicMock()

        result = node.test_method("test")

        assert result == "BaseNode.test_method(test)"
        node.log.debug.assert_not_called()

    def test_decorator_handles_inheritance_chain(self):
        """Test that decorator correctly handles inheritance chain without infinite recursion."""

        class BaseNode:
            def wait_ssh_up(self, verbose=True):
                return "BaseNode.wait_ssh_up called"

        class CloudNode(BaseNode):
            @xcloud_super_if_supported
            def wait_ssh_up(self, verbose=True):
                pass  # This should never be called directly

        class CloudVSNode(CloudNode):
            pass  # Doesn't override wait_ssh_up

        # Create CloudVSNode instance with xcloud support enabled
        vs_node = CloudVSNode()
        vs_node.xcloud_connect_supported = True
        vs_node.log = MagicMock()

        # This should NOT cause infinite recursion
        result = vs_node.wait_ssh_up()

        assert result == "BaseNode.wait_ssh_up called"
        vs_node.log.debug.assert_not_called()

    def test_decorator_returns_none_when_not_supported(self):
        """Test that decorator returns None when xcloud_connect_supported is False."""

        class BaseNode:
            def test_method(self):
                return "BaseNode.test_method"

        class CloudNode(BaseNode):
            @xcloud_super_if_supported
            def test_method(self):
                pass

        # Create instance with xcloud support disabled
        node = CloudNode()
        node.xcloud_connect_supported = False
        node.log = MagicMock()

        result = node.test_method()

        assert result is None
        node.log.debug.assert_called_once()
        assert "Skip test_method" in str(node.log.debug.call_args)

    def test_decorator_handles_missing_attribute(self):
        """Test that decorator handles missing xcloud_connect_supported attribute."""

        class BaseNode:
            def test_method(self):
                return "BaseNode.test_method"

        class CloudNode(BaseNode):
            @xcloud_super_if_supported
            def test_method(self):
                pass

        # Create instance without xcloud_connect_supported attribute
        node = CloudNode()
        node.log = MagicMock()

        result = node.test_method()

        # Should treat missing attribute as False
        assert result is None
        node.log.debug.assert_called_once()

    def test_decorator_preserves_method_arguments(self):
        """Test that decorator correctly passes arguments to base method."""

        class BaseNode:
            def test_method(self, arg1, arg2, kwarg1=None, kwarg2=None):
                return f"BaseNode.test_method({arg1}, {arg2}, {kwarg1}, {kwarg2})"

        class CloudNode(BaseNode):
            @xcloud_super_if_supported
            def test_method(self, arg1, arg2, kwarg1=None, kwarg2=None):
                pass

        node = CloudNode()
        node.xcloud_connect_supported = True
        node.log = MagicMock()

        result = node.test_method("a", "b", kwarg1="c", kwarg2="d")

        assert result == "BaseNode.test_method(a, b, c, d)"

    def test_decorator_works_with_multiple_inheritance_levels(self):
        """Test decorator works correctly with multiple inheritance levels."""

        class BaseNode:
            def test_method(self):
                return "BaseNode"

        class MiddleNode(BaseNode):
            pass  # Doesn't override test_method

        class CloudNode(MiddleNode):
            @xcloud_super_if_supported
            def test_method(self):
                pass

        class CloudVSNode(CloudNode):
            pass

        # Test with CloudVSNode (3 levels deep)
        vs_node = CloudVSNode()
        vs_node.xcloud_connect_supported = True
        vs_node.log = MagicMock()

        result = vs_node.test_method()

        assert result == "BaseNode"


class TestCloudClusterNaming:
    def test_simple_username(self):
        name = build_cloud_cluster_name("john", "PR-test", "abc12345", 6)
        assert name == "PR-test-john-abc12345-keep-006h"
        assert len(name) <= 63

    def test_username_with_dot(self):
        name = build_cloud_cluster_name("john.doe", "PR-test", "abc12345", 60)
        assert name == "PR-test-john_doe-abc12345-keep-060h"
        assert len(name) <= 63

    def test_no_space_for_testname_prefix(self):
        long_username = "123456789-abcdefghi-123456789-abcdefghi-123"  # 43 chars
        name = build_cloud_cluster_name(long_username, "PR-provision-test", "abc12345", 360)
        assert name == f"{long_username}-abc12345-keep-360h"
        assert len(name) <= 63

    def test_testname_prefix_truncation(self):
        name = build_cloud_cluster_name("firstname.laastname", "very-long-test-name-to-be-truncated", "abc12345", 360)
        assert name.startswith("very-long-test-name-to-b-")
        assert len(name) <= 63


class TestScyllaCloudClusterDiagnostics:
    """Test suite for ScyllaCloudCluster diagnostics."""

    @patch("sdcm.cluster_cloud.wait.wait_for")
    def test_wait_for_cluster_ready_timeout_includes_diagnostics(self, mock_wait_for):
        mock_cluster = MagicMock(spec=ScyllaCloudCluster)
        mock_cluster._account_id, mock_cluster._cluster_id, mock_cluster.dc_id = 123, 456, 1
        mock_cluster.log = MagicMock()

        mock_api_client = MagicMock()
        mock_cluster._api_client = mock_api_client

        mock_api_client.get_cluster_details.return_value = {
            "status": "BOOTSTRAP_ERROR",
            "promProxyEnabled": False,
            "errorCode": "081005",
        }
        mock_api_client.get_cluster_nodes.return_value = [
            {"id": "12345", "status": "ACTIVE"},
            {"id": "12346", "status": "ACTIVE"},
            {"id": "12347", "status": "BOOTSTRAPPING"},
        ]
        mock_api_client.get_vector_search_nodes.return_value = {
            "availabilityZones": [{"nodes": [{"id": "12348", "status": "ACTIVE"}]}]
        }
        mock_cluster._get_cluster_diagnostic_info = lambda: ScyllaCloudCluster._get_cluster_diagnostic_info(
            mock_cluster
        )
        mock_wait_for.side_effect = WaitForTimeoutError("Wait for cluster ready: timeout - 600 seconds")

        with pytest.raises(WaitForTimeoutError) as exc_info:
            ScyllaCloudCluster._wait_for_cluster_ready(mock_cluster, timeout=600)
        error_message = str(exc_info.value)
        expected_messages = [
            "cluster failed to become ready within 600 seconds (status: BOOTSTRAP_ERROR)",
            "Nodes status:",
            "✓ DB node 12345: ACTIVE",
            "✓ DB node 12346: ACTIVE",
            "⚠ DB node 12347: BOOTSTRAPPING",
            "✓ VS node 12348: ACTIVE",
            "Siren error code: 081005 (see https://cloud.docs.scylladb.com/stable/api-docs/api-error-codes.html)",
            "Original error: Wait for cluster ready: timeout - 600 seconds",
        ]
        assert all(msg in error_message for msg in expected_messages)
