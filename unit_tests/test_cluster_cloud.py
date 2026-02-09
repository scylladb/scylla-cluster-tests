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
<<<<<<< HEAD
||||||| parent of 7f23fcc38 (fix(xcloud): wait for cloud service installations in correct order)


class TestScyllaCloudClusterUpdateName:
    """Test suite for ScyllaCloudCluster.update_cluster_name method."""

    @pytest.fixture
    def mock_api_client(self):
        """Create a mock API client."""
        return MagicMock(get_current_account_id=MagicMock(return_value=123))

    @pytest.fixture
    def mock_cluster(self, mock_api_client):
        """Create a mock ScyllaCloudCluster instance."""
        with (
            patch("sdcm.cluster_cloud.TestConfig"),
            patch.object(ScyllaCloudCluster, "init_log_directory"),
            patch("sdcm.cluster.ScyllaClusterBenchmarkManager"),
        ):
            params = SCTConfiguration()
            params.update(
                {
                    "xcloud_provider": "aws",
                    "xcloud_vpc_peering": {"enabled": False},
                    "n_vector_store_nodes": 0,
                    "region_name": "us-east-1",
                }
            )
            cluster = ScyllaCloudCluster(
                cloud_api_client=mock_api_client, user_prefix="test", n_nodes=0, params=params, add_nodes=False
            )
            cluster._cluster_id = 456
            cluster.log = MagicMock()
            return cluster

    def test_update_cluster_name_success(self, mock_cluster, mock_api_client):
        """Test successful cluster name update."""
        mock_cluster.name = "old-cluster-name"

        mock_cluster.update_cluster_name("new-cluster-name")
        mock_api_client.update_cluster_name.assert_called_once_with(
            account_id=123, cluster_id=456, new_name="new-cluster-name"
        )

        assert mock_cluster.name == "new-cluster-name"
        mock_cluster.log.info.assert_any_call(
            "Updating cluster name from '%s' to '%s'", "old-cluster-name", "new-cluster-name"
        )
        mock_cluster.log.info.assert_any_call("Cluster name updated successfully to '%s'", "new-cluster-name")

    def test_update_cluster_name_no_cluster_id(self, mock_cluster):
        mock_cluster._cluster_id = None
        with pytest.raises(ScyllaCloudError, match="Cannot update cluster name: cluster ID is not set"):
            mock_cluster.update_cluster_name("new-name")

    def test_update_cluster_name_exceeds_max_length(self, mock_cluster):
        with pytest.raises(ValueError, match="Cluster name exceeds maximum length of 63 characters"):
            mock_cluster.update_cluster_name("a" * 64)


class TestApplyKeepTag:
    @pytest.mark.parametrize(
        "input_name,hours,expected",
        [
            ("test-user-abc12345-keep-004h", 24, "test-user-abc12345-keep-024h"),
            ("test-user-abc12345-keep-004h", CLOUD_KEEP_ALIVE_HOURS, "test-user-abc12345-keep-072h"),
            ("test-user-abc12345", 24, "test-user-abc12345-keep-024h"),
            ("test-abc12345-keep-100h", 6, "test-abc12345-keep-006h"),
        ],
    )
    def test_apply_keep_tag_to_name(self, input_name, hours, expected):
        assert apply_keep_tag_to_name(input_name, hours) == expected


class TestScyllaCloudClusterUpdateKeepTag:
    @pytest.fixture
    def mock_cluster(self):
        mock_api_client = MagicMock(get_current_account_id=MagicMock(return_value=123))
        with (
            patch("sdcm.cluster_cloud.TestConfig"),
            patch.object(ScyllaCloudCluster, "init_log_directory"),
            patch("sdcm.cluster.ScyllaClusterBenchmarkManager"),
        ):
            params = SCTConfiguration()
            params.update(
                {
                    "xcloud_provider": "aws",
                    "xcloud_vpc_peering": {"enabled": False},
                    "n_vector_store_nodes": 0,
                    "region_name": "us-east-1",
                }
            )
            cluster = ScyllaCloudCluster(
                cloud_api_client=mock_api_client, user_prefix="test", n_nodes=0, params=params, add_nodes=False
            )
            cluster._cluster_id = 456
            cluster.name = "test-user-abc12345-keep-004h"
            cluster.log = MagicMock()
            return cluster

    def test_set_keep_alive_updates_cluster_name(self, mock_cluster):
        assert mock_cluster._set_keep_alive() is True
        assert mock_cluster.name == "test-user-abc12345-keep-072h"

    def test_set_keep_duration_updates_cluster_name(self, mock_cluster):
        mock_cluster._set_keep_duration(24)
        assert mock_cluster.name == "test-user-abc12345-keep-024h"

    def test_keep_methods_skip_api_call_if_name_unchanged(self, mock_cluster):
        mock_cluster.name = "test-user-abc12345-keep-024h"
        mock_cluster._set_keep_duration(24)
        assert mock_cluster.name == "test-user-abc12345-keep-024h"
=======


class TestScyllaCloudClusterUpdateName:
    """Test suite for ScyllaCloudCluster.update_cluster_name method."""

    @pytest.fixture
    def mock_api_client(self):
        """Create a mock API client."""
        return MagicMock(get_current_account_id=MagicMock(return_value=123))

    @pytest.fixture
    def mock_cluster(self, mock_api_client):
        """Create a mock ScyllaCloudCluster instance."""
        with (
            patch("sdcm.cluster_cloud.TestConfig"),
            patch.object(ScyllaCloudCluster, "init_log_directory"),
            patch("sdcm.cluster.ScyllaClusterBenchmarkManager"),
        ):
            params = SCTConfiguration()
            params.update(
                {
                    "xcloud_provider": "aws",
                    "xcloud_vpc_peering": {"enabled": False},
                    "n_vector_store_nodes": 0,
                    "region_name": "us-east-1",
                }
            )
            cluster = ScyllaCloudCluster(
                cloud_api_client=mock_api_client, user_prefix="test", n_nodes=0, params=params, add_nodes=False
            )
            cluster._cluster_id = 456
            cluster.log = MagicMock()
            return cluster

    def test_update_cluster_name_success(self, mock_cluster, mock_api_client):
        """Test successful cluster name update."""
        mock_cluster.name = "old-cluster-name"

        mock_cluster.update_cluster_name("new-cluster-name")
        mock_api_client.update_cluster_name.assert_called_once_with(
            account_id=123, cluster_id=456, new_name="new-cluster-name"
        )

        assert mock_cluster.name == "new-cluster-name"
        mock_cluster.log.info.assert_any_call(
            "Updating cluster name from '%s' to '%s'", "old-cluster-name", "new-cluster-name"
        )
        mock_cluster.log.info.assert_any_call("Cluster name updated successfully to '%s'", "new-cluster-name")

    def test_update_cluster_name_no_cluster_id(self, mock_cluster):
        mock_cluster._cluster_id = None
        with pytest.raises(ScyllaCloudError, match="Cannot update cluster name: cluster ID is not set"):
            mock_cluster.update_cluster_name("new-name")

    def test_update_cluster_name_exceeds_max_length(self, mock_cluster):
        with pytest.raises(ValueError, match="Cluster name exceeds maximum length of 63 characters"):
            mock_cluster.update_cluster_name("a" * 64)


class TestApplyKeepTag:
    @pytest.mark.parametrize(
        "input_name,hours,expected",
        [
            ("test-user-abc12345-keep-004h", 24, "test-user-abc12345-keep-024h"),
            ("test-user-abc12345-keep-004h", CLOUD_KEEP_ALIVE_HOURS, "test-user-abc12345-keep-072h"),
            ("test-user-abc12345", 24, "test-user-abc12345-keep-024h"),
            ("test-abc12345-keep-100h", 6, "test-abc12345-keep-006h"),
        ],
    )
    def test_apply_keep_tag_to_name(self, input_name, hours, expected):
        assert apply_keep_tag_to_name(input_name, hours) == expected


class TestScyllaCloudClusterUpdateKeepTag:
    @pytest.fixture
    def mock_cluster(self):
        mock_api_client = MagicMock(get_current_account_id=MagicMock(return_value=123))
        with (
            patch("sdcm.cluster_cloud.TestConfig"),
            patch.object(ScyllaCloudCluster, "init_log_directory"),
            patch("sdcm.cluster.ScyllaClusterBenchmarkManager"),
        ):
            params = SCTConfiguration()
            params.update(
                {
                    "xcloud_provider": "aws",
                    "xcloud_vpc_peering": {"enabled": False},
                    "n_vector_store_nodes": 0,
                    "region_name": "us-east-1",
                }
            )
            cluster = ScyllaCloudCluster(
                cloud_api_client=mock_api_client, user_prefix="test", n_nodes=0, params=params, add_nodes=False
            )
            cluster._cluster_id = 456
            cluster.name = "test-user-abc12345-keep-004h"
            cluster.log = MagicMock()
            return cluster

    def test_set_keep_alive_updates_cluster_name(self, mock_cluster):
        assert mock_cluster._set_keep_alive() is True
        assert mock_cluster.name == "test-user-abc12345-keep-072h"

    def test_set_keep_duration_updates_cluster_name(self, mock_cluster):
        mock_cluster._set_keep_duration(24)
        assert mock_cluster.name == "test-user-abc12345-keep-024h"

    def test_keep_methods_skip_api_call_if_name_unchanged(self, mock_cluster):
        mock_cluster.name = "test-user-abc12345-keep-024h"
        mock_cluster._set_keep_duration(24)
        assert mock_cluster.name == "test-user-abc12345-keep-024h"


class TestCloudServiceInstallationOrdering:
    """Test suite for _wait_for_cloud_service_installations."""

    @pytest.fixture
    def mock_cluster(self):
        mock = MagicMock()
        mock._account_id = 123
        mock._cluster_id = 456
        mock.log = MagicMock()
        mock._api_client = MagicMock()

        mock._wait_for_cloud_service_installations = lambda: ScyllaCloudCluster._wait_for_cloud_services(mock)
        mock._get_pending_service_requests = lambda *args, **kwargs: ScyllaCloudCluster._get_pending_service_requests(
            mock, *args, **kwargs
        )
        mock._wait_for_cloud_request_completed = MagicMock()
        return mock

    def test_manager_in_progress_vs_queued(self, mock_cluster):
        """When Manager is IN_PROGRESS and VS is QUEUED, wait for Manager first."""
        mock_cluster._api_client.get_cluster_requests.return_value = [
            {"id": 1, "requestType": "INSTALL_MANAGER", "status": "IN_PROGRESS"},
            {"id": 2, "requestType": "INSTALL_VECTOR_SEARCH", "status": "QUEUED"},
        ]

        mock_cluster._wait_for_cloud_service_installations()

        calls = mock_cluster._wait_for_cloud_request_completed.call_args_list
        assert len(calls) == 2
        assert calls[0] == ((), {"request_id": 1, "request_type": "INSTALL_MANAGER"})
        assert calls[1] == ((), {"request_id": 2, "request_type": "INSTALL_VECTOR_SEARCH"})

    def test_vs_in_progress_manager_queued(self, mock_cluster):
        """When VS is IN_PROGRESS and Manager is QUEUED, wait for VS first."""
        mock_cluster._api_client.get_cluster_requests.return_value = [
            {"id": 1, "requestType": "INSTALL_MANAGER", "status": "QUEUED"},
            {"id": 2, "requestType": "INSTALL_VECTOR_SEARCH", "status": "IN_PROGRESS"},
        ]

        mock_cluster._wait_for_cloud_service_installations()

        calls = mock_cluster._wait_for_cloud_request_completed.call_args_list
        assert len(calls) == 2
        assert calls[0] == ((), {"request_id": 2, "request_type": "INSTALL_VECTOR_SEARCH"})
        assert calls[1] == ((), {"request_id": 1, "request_type": "INSTALL_MANAGER"})

    def test_one_already_completed(self, mock_cluster):
        """When one service is already COMPLETED, only wait for the other."""
        mock_cluster._api_client.get_cluster_requests.return_value = [
            {"id": 1, "requestType": "INSTALL_MANAGER", "status": "COMPLETED"},
            {"id": 2, "requestType": "INSTALL_VECTOR_SEARCH", "status": "IN_PROGRESS"},
        ]

        mock_cluster._wait_for_cloud_service_installations()

        calls = mock_cluster._wait_for_cloud_request_completed.call_args_list
        assert len(calls) == 1
        assert calls[0] == ((), {"request_id": 2, "request_type": "INSTALL_VECTOR_SEARCH"})

    def test_both_already_completed(self, mock_cluster):
        """When both services are already COMPLETED, no waiting needed."""
        mock_cluster._api_client.get_cluster_requests.return_value = [
            {"id": 1, "requestType": "INSTALL_MANAGER", "status": "COMPLETED"},
            {"id": 2, "requestType": "INSTALL_VECTOR_SEARCH", "status": "COMPLETED"},
        ]

        mock_cluster._wait_for_cloud_service_installations()
        mock_cluster._wait_for_cloud_request_completed.assert_not_called()
>>>>>>> 7f23fcc38 (fix(xcloud): wait for cloud service installations in correct order)
