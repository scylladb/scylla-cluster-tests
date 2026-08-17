"""Unit tests for cluster_cloud module."""

import pytest
from unittest.mock import MagicMock, patch

from sdcm.cloud_api_client import ScyllaCloudAPIClient
from sdcm.cluster_cloud import (
    xcloud_super_if_supported,
    ScyllaCloudCluster,
    ScyllaCloudError,
    CloudNode,
    VectorStoreSetCloud,
)
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.cloud_api_utils import (
    build_cloud_cluster_name,
    apply_keep_tag_to_name,
    CLOUD_KEEP_ALIVE_HOURS,
)
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


def _make_cloud_node():
    """Create a CloudNode-like mock for configure_remote_logging tests."""
    node = MagicMock()
    node.name = "db-node-test-1"
    node.xcloud_connect_supported = True
    node.parent_cluster.params.get.return_value = "vector"
    node._vector_is_active.return_value = True
    return node


def test_configure_remote_logging_managed_path_does_not_self_install():
    node = _make_cloud_node()
    node._managed_vector_ready.return_value = True
    CloudNode.configure_remote_logging(node)

    node._apply_vector_target_config.assert_called_once()
    node._self_install_vector.assert_not_called()


def test_configure_remote_logging_falls_back_to_self_install():
    node = _make_cloud_node()
    node._managed_vector_ready.return_value = False
    CloudNode.configure_remote_logging(node)

    node._self_install_vector.assert_called_once()
    node._apply_vector_target_config.assert_called_once()

    ordered = [c[0] for c in node.mock_calls]
    assert ordered.index("_self_install_vector") < ordered.index("_apply_vector_target_config")


def make_cloud_node_payload(
    node_id=1, az_name="us-east-1a", az_id="use1-az1", public_ip="54.0.0.1", private_ip="10.0.0.1"
):
    """Payload shaped like NodeInfoEnriched from GET /account/{id}/cluster/{id}/nodes"""
    return {
        "id": node_id,
        "azName": az_name,
        "azId": az_id,
        "rackName": az_id,
        "publicIp": public_ip,
        "privateIp": private_ip,
        "status": "ACTIVE",
        "state": "NORMAL",
        "instance": {"externalId": "i4i.large"},
        "region": {"externalId": "us-east-1", "name": "US East (N. Virginia)"},
        "dcId": 1,
    }


@pytest.mark.parametrize(
    "drop_external_id,expected",
    [
        # the (region, rack) join keys off the external id, not the display name
        (False, "us-east-1"),
        (True, "US East (N. Virginia)"),
    ],
)
def test_cloud_node_region_prefers_external_id(drop_external_id, expected):
    payload = make_cloud_node_payload()
    if drop_external_id:
        del payload["region"]["externalId"]
    node = MagicMock(_cloud_instance_data=payload)
    assert CloudNode.vm_region.fget(node) == expected


def test_refresh_instance_state_requests_enriched_payload():
    node = MagicMock(_node_id=1, _public_ip="54.0.0.1", _private_ip="10.0.0.1")
    node._account_id, node._cluster_id = 1, 2
    node._api_client = MagicMock()
    node._api_client.get_cluster_nodes.return_value = [make_cloud_node_payload(node_id=1)]
    CloudNode._refresh_instance_state(node)
    node._api_client.get_cluster_nodes.assert_called_once_with(account_id=1, cluster_id=2, enriched=True)


def test_init_nodes_from_data_mixed_az_and_azless_nodes():
    """Racks are derived per node: AZ-bearing nodes by alphabetical AZ order, AZ-less ones from the caller's rack"""
    mock_cluster = MagicMock(spec=ScyllaCloudCluster)
    mock_cluster.log = MagicMock()
    mock_cluster._create_node = MagicMock(side_effect=lambda **kwargs: kwargs)

    nodes_data = [
        make_cloud_node_payload(node_id=1, az_name="us-east-1c"),
        make_cloud_node_payload(node_id=2, az_name="us-east-1a"),
        {"id": 3, "publicIp": "54.0.0.3", "privateIp": "10.0.0.3", "status": "ACTIVE"},
        make_cloud_node_payload(node_id=4, az_name="us-east-1b"),
    ]
    created = ScyllaCloudCluster._init_nodes_from_data(mock_cluster, nodes_data=nodes_data, rack=9)
    assert [node["rack"] for node in created] == [2, 0, 9, 1]


def test_vs_nodes_data_injects_az_id_from_group():
    vs_set = MagicMock(spec=VectorStoreSetCloud)
    vs_set._get_vs_info.return_value = {
        "availabilityZones": [
            {
                "azid": "use1-az1",
                "rackName": "use1-az1",
                "nodes": [
                    {"id": 1, "status": "ACTIVE"},
                    {"id": 2, "status": "PENDING_DELETE"},
                ],
            },
            {
                "azid": "use1-az2",
                "rackName": "use1-az2",
                "nodes": [{"id": 3, "status": "ACTIVE"}],
            },
        ]
    }

    nodes = VectorStoreSetCloud.vs_nodes_data.fget(vs_set)
    assert [(node["id"], node["azId"]) for node in nodes] == [(1, "use1-az1"), (3, "use1-az2")]


def _build_real_cloud_cluster():
    """Construct a real ScyllaCloudCluster with mocked externals (mirrors legacy fixture)"""
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
        api_client = MagicMock(get_current_account_id=MagicMock(return_value=123))
        return ScyllaCloudCluster(
            cloud_api_client=api_client, user_prefix="test", n_nodes=0, params=params, add_nodes=False
        )


def test_cloud_cluster_datacenter_is_list():
    cluster_obj = _build_real_cloud_cluster()
    assert cluster_obj.datacenter == ["us-east-1"]


def test_update_racks_count_from_nodes():
    cluster_obj = _build_real_cloud_cluster()
    cluster_obj.nodes = [MagicMock(rack=0), MagicMock(rack=1), MagicMock(rack=2)]
    cluster_obj._update_racks_count()
    assert cluster_obj.racks_count == 3


def _fake_conf(values, region_names=("us-east-1",)):
    conf = MagicMock(region_names=list(region_names))
    conf.get.side_effect = lambda key: values.get(key)
    return conf


def _rackaware_conf(**overrides):
    config = {
        "rack_aware_loader": True,
        "n_loaders": 1,
    }
    config.update(overrides)
    return _fake_conf(config)


@pytest.mark.parametrize("zones", ["", "use1-az1,use1-az1,use1-az1"])
def test_rackaware_verification_xcloud_requires_two_distinct_zones(zones):
    conf = _rackaware_conf(cluster_backend="xcloud", xcloud_availability_zones=zones)
    with pytest.raises(ValueError, match="xcloud_availability_zones"):
        SCTConfiguration._verify_rackaware_configuration(conf)


def test_rackaware_verification_xcloud_passes_with_multi_az():
    conf = _rackaware_conf(cluster_backend="xcloud", xcloud_availability_zones="use1-az1,use1-az2,use1-az3")
    SCTConfiguration._verify_rackaware_configuration(conf)


def test_rackaware_verification_non_xcloud_unchanged():
    conf = _rackaware_conf(cluster_backend="aws", availability_zone="a,b,c", simulated_racks=0)
    SCTConfiguration._verify_rackaware_configuration(conf)


@pytest.mark.parametrize(
    "values,error",
    [
        # node count is Cloud-managed under a scaling policy, so pinned zones cannot be honoured
        (
            {
                "xcloud_availability_zones": "use1-az1,use1-az2",
                "xcloud_scaling_config": {"Mode": "xcloud"},
                "n_db_nodes": [3],
            },
            "xcloud_scaling_config",
        ),
        # 4 nodes cannot spread evenly over 3 zones
        (
            {"xcloud_availability_zones": "use1-az1,use1-az2,use1-az3", "n_db_nodes": [4]},
            "Cannot spread 4 nodes evenly",
        ),
    ],
)
def test_xcloud_az_validation_rejects_bad_config(values, error):
    with pytest.raises(ValueError, match=error):
        SCTConfiguration._validate_xcloud_availability_zones(_fake_conf(values))


@pytest.mark.parametrize(
    "region,zones,raises",
    [
        ("eu-west-1", "use1-az1,use1-az2,use1-az4", True),
        ("eu-west-1", "euw1-az1,euw1-az2,euw1-az3", False),
        ("us-east-1", "use1-az1,use1-az2,use1-az4", False),
        ("ap-southeast-1", "apse1-az1,apse1-az2,apse1-az3", False),
        ("us-gov-west-1", "usgw1-az1,usgw1-az2,usgw1-az3", False),
    ],
)
def test_xcloud_az_validation_checks_region_prefix(region, zones, raises):
    conf = _fake_conf(
        {"xcloud_availability_zones": zones, "n_db_nodes": [3], "xcloud_provider": "aws"},
        region_names=(region,),
    )
    if raises:
        with pytest.raises(ValueError, match="does not belong to region"):
            SCTConfiguration._validate_xcloud_availability_zones(conf)
    else:
        SCTConfiguration._validate_xcloud_availability_zones(conf)


def _make_api_client():
    with patch.object(ScyllaCloudAPIClient, "_create_session", return_value=MagicMock()):
        return ScyllaCloudAPIClient(api_url="https://api.example.com", auth_token="token")


def _minimal_create_kwargs():
    return dict(
        account_id=1,
        cluster_name="c",
        scylla_version="2025.4.0",
        cidr_block=None,
        broadcast_type="PUBLIC",
        allowed_ips=[],
        cloud_provider_id=1,
        region_id=1,
        instance_id=1,
        replication_factor=3,
        number_of_nodes=3,
        account_credential_id=1,
        free_trial=False,
        user_api_interface="CQL",
        enable_dns_association=True,
        jump_start=False,
        encryption_at_rest=None,
        maintenance_windows=[],
        scaling={},
        prom_proxy=True,
        vector_search=None,
        tablets=None,
    )


def test_create_cluster_request_without_az_override_keeps_payload():
    client = _make_api_client()
    client.request = MagicMock(return_value={"requestId": 1})
    client.create_cluster_request(**_minimal_create_kwargs())
    assert client.request.call_args.kwargs == {
        "clusterName": "c",
        "scyllaVersion": "2025.4.0",
        "cidrBlock": None,
        "broadcastType": "PUBLIC",
        "allowedIPs": [],
        "cloudProviderId": 1,
        "regionId": 1,
        "instanceId": 1,
        "replicationFactor": 3,
        "numberOfNodes": 3,
        "accountCredentialId": 1,
        "freeTier": False,
        "userApiInterface": "CQL",
        "enableDnsAssociation": True,
        "jumpStart": False,
        "encryptionAtRest": None,
        "maintenanceWindows": [],
        "scaling": {},
        "promProxy": True,
        "vectorSearch": None,
        "tablets": None,
    }


def test_create_cluster_request_with_az_override_sets_placement():
    client = _make_api_client()
    client.request = MagicMock(return_value={"requestId": 1})
    client.create_cluster_request(
        **_minimal_create_kwargs(), availability_zone_ids=["use1-az1", "use1-az2", "use1-az3"]
    )
    body = client.request.call_args.kwargs
    assert body["availabilityZoneIdsOverride"] == ["use1-az1", "use1-az2", "use1-az3"]
    assert body["placement"] == "true"


def _mock_cluster_for_prepare_config(az_knob=""):
    mock_cluster = MagicMock(spec=ScyllaCloudCluster)
    mock_cluster.log = MagicMock()
    mock_cluster.xcloud_scaling_config = {}
    mock_cluster.vpc_peering_enabled = False
    mock_cluster._deploy_vs_nodes = False
    mock_cluster._allowed_ips = []
    mock_cluster._account_id = 1
    mock_cluster.provider_id = 1
    mock_cluster.region_id = 1
    mock_cluster.shortid = "abc12345"
    mock_cluster.node_type = "scylla-db"
    mock_cluster.test_config = MagicMock(TEST_DURATION=60)
    mock_cluster._api_client = MagicMock(client_ip="1.2.3.4", get_instance_id_by_name=MagicMock(return_value=42))
    params = {
        "xcloud_availability_zones": az_knob,
        "xcloud_replication_factor": 3,
        "xcloud_credential_id": 7,
        "scylla_version": "2025.4.0",
    }
    mock_cluster.params = MagicMock()
    mock_cluster.params.get = MagicMock(side_effect=lambda key: params.get(key))
    mock_cluster.params.cloud_provider_params = {"instance_type_db": "i4i.large", "region": "us-east-1"}
    return mock_cluster


@patch("sdcm.cluster_cloud.get_username", return_value="user")
@patch("sdcm.cluster_cloud.get_test_name", return_value="test")
def test_prepare_cluster_config_expands_knob_to_node_count(mock_test_name, mock_username):
    az_ids = ["use1-az1", "use1-az2", "use1-az3"]
    cluster = _mock_cluster_for_prepare_config(az_knob=",".join(az_ids))
    config = ScyllaCloudCluster._prepare_cluster_config(cluster, node_count=6, instance_type=None)
    assert config["availability_zone_ids"] == az_ids * 2
