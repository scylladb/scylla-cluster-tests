"""Unit tests for cluster_cloud module."""

import inspect
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster import BaseNode
from sdcm.cluster_cloud import (
    CloudNode,
    ScyllaCloudCluster,
    ScyllaCloudError,
    VectorStoreSetCloud,
    xcloud_super_if_supported,
)
from sdcm.exceptions import WaitForTimeoutError
from sdcm.sct_config import SCTConfiguration
from sdcm.xcloud.api_client import ScyllaCloudAPIClient
from sdcm.xcloud.cluster_config_utils import (
    CLOUD_KEEP_ALIVE_HOURS,
    apply_keep_tag_to_name,
    build_cloud_cluster_name,
)


@pytest.fixture
def mock_api_client():
    """MagicMock API client that reports account 123."""
    return MagicMock(get_current_account_id=MagicMock(return_value=123))


@pytest.fixture
def update_name_cluster(mock_api_client):
    """ScyllaCloudCluster with cluster ID 456 and a logger mock, talking to mock_api_client."""
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


@pytest.fixture
def keep_tag_cluster():
    """ScyllaCloudCluster named with a keep-004h tag, so keep-duration updates change the name."""
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


@pytest.fixture
def vs_wait_cluster():
    """MagicMock cluster that runs the real _find_cluster_request for vector search waits."""
    mock = MagicMock()
    mock._account_id = 123
    mock._cluster_id = 456
    mock.log = MagicMock()
    mock._api_client = MagicMock()
    mock._find_cluster_request = lambda *args, **kwargs: ScyllaCloudCluster._find_cluster_request(mock, *args, **kwargs)
    mock._wait_for_cloud_request_completed = MagicMock()
    return mock


def make_cloud_node():
    """Create a CloudNode-like mock for configure_remote_logging tests."""
    node = MagicMock()
    node.name = "db-node-test-1"
    node.xcloud_connect_supported = True
    node.parent_cluster.params.get.return_value = "vector"
    node._vector_is_active.return_value = True
    return node


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


def fake_conf(values, region_names=("us-east-1",)):
    """SCTConfiguration stand-in: conf.get() reads from values, region_names set."""
    conf = MagicMock(region_names=list(region_names))
    conf.get.side_effect = values.get
    return conf


def rackaware_conf(**overrides):
    """fake_conf with rack-aware loader defaults, overridden by keyword arguments."""
    config = {
        "rack_aware_loader": True,
        "n_loaders": 1,
    }
    config.update(overrides)
    return fake_conf(config)


def make_api_client():
    """ScyllaCloudAPIClient whose HTTP session is a MagicMock, so no request leaves the process."""
    with patch.object(ScyllaCloudAPIClient, "_create_session", return_value=MagicMock()):
        return ScyllaCloudAPIClient(api_url="https://api.example.com", auth_token="token")


def minimal_create_kwargs():
    """Keyword arguments of ScyllaCloudAPIClient.create_cluster_request with no optional settings."""
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


def build_real_cloud_cluster():
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


def mock_cluster_for_prepare_config(az_knob=""):
    """MagicMock ScyllaCloudCluster whose params cover _prepare_cluster_config; az_knob sets xcloud_availability_zones."""
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
    mock_cluster.params.get = MagicMock(side_effect=params.get)
    mock_cluster.params.cloud_provider_params = {"instance_type_db": "i4i.large", "region": "us-east-1"}
    return mock_cluster


def test_decorator_calls_base_method_when_supported():
    """xcloud_super_if_supported calls the base class method when xcloud_connect_supported is True."""

    class BaseNode:
        def test_method(self, value):
            """Base implementation the decorator must reach."""
            return f"BaseNode.test_method({value})"

    class CloudNode(BaseNode):
        @xcloud_super_if_supported
        def test_method(self, value):
            """Overridden method; its body must never run when supported."""
            # This should never be called

    # Create instance with xcloud support enabled
    node = CloudNode()
    node.xcloud_connect_supported = True
    node.log = MagicMock()

    result = node.test_method("test")

    assert result == "BaseNode.test_method(test)"
    node.log.debug.assert_not_called()


def test_decorator_handles_inheritance_chain():
    """xcloud_super_if_supported resolves the base method through an inheritance chain without recursing."""

    class BaseNode:
        def wait_ssh_up(self, verbose=True):
            """Base implementation the decorator must reach."""
            return "BaseNode.wait_ssh_up called"

    class CloudNode(BaseNode):
        @xcloud_super_if_supported
        def wait_ssh_up(self, verbose=True):
            """Overridden method; its body must never run when supported."""
            # This should never be called directly

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


def test_decorator_returns_none_when_not_supported():
    """xcloud_super_if_supported returns None and logs a skip when xcloud_connect_supported is False."""

    class BaseNode:
        def test_method(self):
            """Base implementation the decorator must not reach."""
            return "BaseNode.test_method"

    class CloudNode(BaseNode):
        @xcloud_super_if_supported
        def test_method(self):
            """Overridden method; the decorator replaces it when unsupported."""

    # Create instance with xcloud support disabled
    node = CloudNode()
    node.xcloud_connect_supported = False
    node.log = MagicMock()

    result = node.test_method()

    assert result is None
    node.log.debug.assert_called_once_with("Skip test_method on scylla-cloud, no ssh connectivity available")


def test_decorator_handles_missing_attribute():
    """xcloud_super_if_supported treats a missing xcloud_connect_supported attribute as False."""

    class BaseNode:
        def test_method(self):
            """Base implementation the decorator must not reach."""
            return "BaseNode.test_method"

    class CloudNode(BaseNode):
        @xcloud_super_if_supported
        def test_method(self):
            """Overridden method; the decorator replaces it when unsupported."""

    # Create instance without xcloud_connect_supported attribute
    node = CloudNode()
    node.log = MagicMock()

    result = node.test_method()

    # Should treat missing attribute as False
    assert result is None
    node.log.debug.assert_called_once_with("Skip test_method on scylla-cloud, no ssh connectivity available")


def test_decorator_preserves_method_arguments():
    """xcloud_super_if_supported passes positional and keyword arguments to the base method."""

    class BaseNode:
        def test_method(self, arg1, arg2, kwarg1=None, kwarg2=None):
            """Base implementation that echoes its arguments."""
            return f"BaseNode.test_method({arg1}, {arg2}, {kwarg1}, {kwarg2})"

    class CloudNode(BaseNode):
        @xcloud_super_if_supported
        def test_method(self, arg1, arg2, kwarg1=None, kwarg2=None):
            """Overridden method; its body must never run when supported."""

    node = CloudNode()
    node.xcloud_connect_supported = True
    node.log = MagicMock()

    result = node.test_method("a", "b", kwarg1="c", kwarg2="d")

    assert result == "BaseNode.test_method(a, b, c, d)"


def test_decorator_works_with_multiple_inheritance_levels():
    """xcloud_super_if_supported finds the base method three inheritance levels up."""

    class BaseNode:
        def test_method(self):
            """Base implementation the decorator must reach."""
            return "BaseNode"

    class MiddleNode(BaseNode):
        pass  # Doesn't override test_method

    class CloudNode(MiddleNode):
        @xcloud_super_if_supported
        def test_method(self):
            """Overridden method; its body must never run when supported."""

    class CloudVSNode(CloudNode):
        pass

    # Test with CloudVSNode (3 levels deep)
    vs_node = CloudVSNode()
    vs_node.xcloud_connect_supported = True
    vs_node.log = MagicMock()

    result = vs_node.test_method()

    assert result == "BaseNode"


def test_cloud_cluster_name_simple_username():
    """build_cloud_cluster_name keeps a plain username and appends the keep tag."""
    name = build_cloud_cluster_name("john", "PR-test", "abc12345", 6)
    assert name == "PR-test-john-abc12345-keep-006h"
    assert len(name) <= 63


def test_cloud_cluster_name_username_with_dot():
    """build_cloud_cluster_name replaces dots in the username with underscores."""
    name = build_cloud_cluster_name("john.doe", "PR-test", "abc12345", 60)
    assert name == "PR-test-john_doe-abc12345-keep-060h"
    assert len(name) <= 63


def test_cloud_cluster_name_no_space_for_testname_prefix():
    """build_cloud_cluster_name drops the test name when a long username leaves no room for it."""
    long_username = "123456789-abcdefghi-123456789-abcdefghi-123"  # 43 chars
    name = build_cloud_cluster_name(long_username, "PR-provision-test", "abc12345", 360)
    assert name == f"{long_username}-abc12345-keep-360h"
    assert len(name) <= 63


def test_cloud_cluster_name_testname_prefix_truncation():
    """build_cloud_cluster_name truncates a long test name so the whole name fits in 63 characters."""
    name = build_cloud_cluster_name("firstname.laastname", "very-long-test-name-to-be-truncated", "abc12345", 360)
    assert name.startswith("very-long-test-name-to-b-")
    assert len(name) <= 63


def test_wait_for_cluster_ready_timeout_includes_diagnostics():
    """A cluster-ready timeout reports cluster status, node states, the Siren error code and the original error."""
    with patch("sdcm.cluster_cloud.wait.wait_for") as mock_wait_for:
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


def test_update_cluster_name_success(update_name_cluster, mock_api_client):
    """update_cluster_name sends the new name to the API and logs the old and new names."""
    update_name_cluster.name = "old-cluster-name"

    update_name_cluster.update_cluster_name("new-cluster-name")
    mock_api_client.update_cluster_name.assert_called_once_with(
        account_id=123, cluster_id=456, new_name="new-cluster-name"
    )

    assert update_name_cluster.name == "new-cluster-name"
    update_name_cluster.log.info.assert_any_call(
        "Updating cluster name from '%s' to '%s'", "old-cluster-name", "new-cluster-name"
    )
    update_name_cluster.log.info.assert_any_call("Cluster name updated successfully to '%s'", "new-cluster-name")


def test_update_cluster_name_no_cluster_id(update_name_cluster):
    """update_cluster_name raises ScyllaCloudError when the cluster ID is not set."""
    update_name_cluster._cluster_id = None
    with pytest.raises(ScyllaCloudError, match="Cannot update cluster name: cluster ID is not set"):
        update_name_cluster.update_cluster_name("new-name")


def test_update_cluster_name_exceeds_max_length(update_name_cluster):
    """update_cluster_name rejects a name longer than 63 characters."""
    with pytest.raises(ValueError, match="Cluster name exceeds maximum length of 63 characters"):
        update_name_cluster.update_cluster_name("a" * 64)


@pytest.mark.parametrize(
    "input_name,hours,expected",
    [
        pytest.param("test-user-abc12345-keep-004h", 24, "test-user-abc12345-keep-024h", id="replace-keep-24h"),
        pytest.param(
            "test-user-abc12345-keep-004h",
            CLOUD_KEEP_ALIVE_HOURS,
            "test-user-abc12345-keep-072h",
            id="replace-keep-alive-hours",
        ),
        pytest.param("test-user-abc12345", 24, "test-user-abc12345-keep-024h", id="append-keep-to-untagged"),
        pytest.param("test-abc12345-keep-100h", 6, "test-abc12345-keep-006h", id="replace-keep-6h"),
    ],
)
def test_apply_keep_tag_to_name(input_name, hours, expected):
    """apply_keep_tag_to_name sets the keep-NNNh tag, replacing an existing one."""
    assert apply_keep_tag_to_name(input_name, hours) == expected


def test_set_keep_alive_updates_cluster_name(keep_tag_cluster):
    """_set_keep_alive returns True and renames the cluster to the keep-alive duration."""
    assert keep_tag_cluster._set_keep_alive() is True
    assert keep_tag_cluster.name == "test-user-abc12345-keep-072h"


def test_set_keep_duration_updates_cluster_name(keep_tag_cluster):
    """_set_keep_duration renames the cluster to the given keep duration."""
    keep_tag_cluster._set_keep_duration(24)
    assert keep_tag_cluster.name == "test-user-abc12345-keep-024h"


def test_keep_methods_skip_api_call_if_name_unchanged(keep_tag_cluster):
    """Keep-duration updates leave the name alone when it already has the requested tag."""
    keep_tag_cluster.name = "test-user-abc12345-keep-024h"
    keep_tag_cluster._set_keep_duration(24)
    assert keep_tag_cluster.name == "test-user-abc12345-keep-024h"


def test_waits_for_vs_request_in_progress(vs_wait_cluster):
    """_wait_for_vector_search_installed waits on an INSTALL_VECTOR_SEARCH request that is still in progress."""
    vs_wait_cluster._api_client.get_cluster_requests.return_value = [
        {"id": 2, "requestType": "INSTALL_VECTOR_SEARCH", "status": "IN_PROGRESS"},
    ]

    ScyllaCloudCluster._wait_for_vector_search_installed(vs_wait_cluster)

    vs_wait_cluster._wait_for_cloud_request_completed.assert_called_once_with(
        request_id=2, request_type="INSTALL_VECTOR_SEARCH"
    )


def test_skips_wait_when_vs_request_completed(vs_wait_cluster):
    """_wait_for_vector_search_installed does not wait when the INSTALL_VECTOR_SEARCH request is completed."""
    vs_wait_cluster._api_client.get_cluster_requests.return_value = [
        {"id": 2, "requestType": "INSTALL_VECTOR_SEARCH", "status": "COMPLETED"},
    ]

    ScyllaCloudCluster._wait_for_vector_search_installed(vs_wait_cluster)

    vs_wait_cluster._wait_for_cloud_request_completed.assert_not_called()


@patch("sdcm.cluster_cloud.time.sleep")
def test_retries_until_vs_request_appears(mock_sleep, vs_wait_cluster):
    """_wait_for_vector_search_installed polls until the INSTALL_VECTOR_SEARCH request appears, then waits on it."""
    vs_wait_cluster._api_client.get_cluster_requests.side_effect = [
        [],
        [{"id": 2, "requestType": "INSTALL_VECTOR_SEARCH", "status": "QUEUED"}],
    ]

    ScyllaCloudCluster._wait_for_vector_search_installed(vs_wait_cluster)

    assert mock_sleep.call_count == 1
    vs_wait_cluster._wait_for_cloud_request_completed.assert_called_once_with(
        request_id=2, request_type="INSTALL_VECTOR_SEARCH"
    )


@patch("sdcm.cluster_cloud.time.sleep")
def test_raises_when_vs_request_never_appears(mock_sleep, vs_wait_cluster):
    """_wait_for_vector_search_installed raises ScyllaCloudError when the INSTALL_VECTOR_SEARCH request never appears."""
    vs_wait_cluster._api_client.get_cluster_requests.return_value = []

    with pytest.raises(ScyllaCloudError, match="INSTALL_VECTOR_SEARCH request did not appear"):
        ScyllaCloudCluster._wait_for_vector_search_installed(vs_wait_cluster)

    vs_wait_cluster._wait_for_cloud_request_completed.assert_not_called()


def test_configure_remote_logging_managed_path_does_not_self_install():
    """configure_remote_logging applies the SCT target config without self-installing when vector is managed."""
    node = make_cloud_node()
    node._managed_vector_ready.return_value = True
    CloudNode.configure_remote_logging(node)

    node._apply_vector_target_config.assert_called_once_with()
    node._self_install_vector.assert_not_called()


def test_configure_remote_logging_falls_back_to_self_install():
    """configure_remote_logging self-installs vector before applying the target config when it is not managed."""
    node = make_cloud_node()
    node._managed_vector_ready.return_value = False
    CloudNode.configure_remote_logging(node)

    node._self_install_vector.assert_called_once_with()
    node._apply_vector_target_config.assert_called_once_with()

    ordered = [c[0] for c in node.mock_calls]
    assert ordered.index("_self_install_vector") < ordered.index("_apply_vector_target_config")


@pytest.mark.parametrize(
    "drop_external_id,expected",
    [
        # the (region, rack) join keys off the external id, not the display name
        pytest.param(False, "us-east-1", id="external-id"),
        pytest.param(True, "US East (N. Virginia)", id="display-name-fallback"),
    ],
)
def test_cloud_node_region_prefers_external_id(drop_external_id, expected):
    """vm_region returns the region external ID, falling back to the display name when it is missing."""
    payload = make_cloud_node_payload()
    if drop_external_id:
        del payload["region"]["externalId"]
    node = MagicMock(_cloud_instance_data=payload)
    assert CloudNode.vm_region.fget(node) == expected


def test_refresh_instance_state_requests_enriched_payload():
    """_refresh_instance_state asks the API for enriched node data of the cluster."""
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
    """vs_nodes_data copies each availability zone's azid onto its active nodes and skips PENDING_DELETE ones."""
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


def test_cloud_cluster_datacenter_is_list():
    """Datacenter is a list holding the region of the cluster."""
    cluster_obj = build_real_cloud_cluster()
    assert cluster_obj.datacenter == ["us-east-1"]


def test_cloud_cluster_id_before_cluster_created_raises():
    """Reading cluster_id before the cluster is created must fail at once, not send cluster_id=None to the API."""
    cluster_obj = build_real_cloud_cluster()

    with pytest.raises(ScyllaCloudError, match="Cluster ID is not set"):
        _ = cluster_obj.cluster_id


def test_cloud_cluster_exposes_api_client_and_ids_once_created():
    """Helpers outside the cluster rely on its public API client, account ID and cluster ID."""
    cluster_obj = build_real_cloud_cluster()
    cluster_obj._cluster_id = 456

    assert cluster_obj.cluster_id == 456
    assert cluster_obj.account_id == 123
    assert cluster_obj.api_client.get_current_account_id() == 123


def test_update_racks_count_from_nodes():
    """_update_racks_count counts the distinct racks of the cluster nodes."""
    cluster_obj = build_real_cloud_cluster()
    cluster_obj.nodes = [MagicMock(rack=0), MagicMock(rack=1), MagicMock(rack=2)]
    cluster_obj._update_racks_count()
    assert cluster_obj.racks_count == 3


@pytest.mark.parametrize(
    "zones",
    [
        pytest.param("", id="no-zones"),
        pytest.param("use1-az1,use1-az1,use1-az1", id="one-zone-repeated"),
    ],
)
def test_rackaware_verification_xcloud_requires_two_distinct_zones(zones):
    """Rack-aware xcloud configs need at least two distinct availability zones."""
    conf = rackaware_conf(cluster_backend="xcloud", xcloud_availability_zones=zones)
    with pytest.raises(ValueError, match="xcloud_availability_zones"):
        SCTConfiguration._verify_rackaware_configuration(conf)


def test_rackaware_verification_xcloud_passes_with_multi_az():
    """Rack-aware xcloud configs with several distinct zones pass verification."""
    conf = rackaware_conf(cluster_backend="xcloud", xcloud_availability_zones="use1-az1,use1-az2,use1-az3")
    SCTConfiguration._verify_rackaware_configuration(conf)


def test_rackaware_verification_non_xcloud_unchanged():
    """Rack-aware verification leaves non-xcloud backends alone."""
    conf = rackaware_conf(cluster_backend="aws", availability_zone="a,b,c", simulated_racks=0)
    SCTConfiguration._verify_rackaware_configuration(conf)


@pytest.mark.parametrize(
    "values,error",
    [
        # node count is Cloud-managed under a scaling policy, so pinned zones cannot be honoured
        pytest.param(
            {
                "xcloud_availability_zones": "use1-az1,use1-az2",
                "xcloud_scaling_config": {"Mode": "xcloud"},
                "n_db_nodes": [3],
            },
            "xcloud_scaling_config",
            id="scaling-policy-pins-zones",
        ),
        # 4 nodes cannot spread evenly over 3 zones
        pytest.param(
            {"xcloud_availability_zones": "use1-az1,use1-az2,use1-az3", "n_db_nodes": [4]},
            "Cannot spread 4 nodes evenly",
            id="uneven-spread",
        ),
    ],
)
def test_xcloud_az_validation_rejects_bad_config(values, error):
    """_validate_xcloud_availability_zones rejects zone settings that conflict with scaling or node count."""
    with pytest.raises(ValueError, match=error):
        SCTConfiguration._validate_xcloud_availability_zones(fake_conf(values), MagicMock(), 1, 1)


@pytest.mark.parametrize(
    "zones,raises",
    [
        pytest.param("use1-az1,use1-az2,use1-az4", False, id="all-zones-available"),  # all three host the instance type
        pytest.param("use1-az1,use1-az2,use1-az3", True, id="zone-cannot-host-instance-type"),  # exists, cannot host it
        pytest.param("euw1-az1,euw1-az2,euw1-az3", True, id="zones-of-other-region"),
    ],
)
def test_xcloud_az_validation_checks_zones_are_available(zones, raises):
    """_validate_xcloud_availability_zones requires every configured zone to be usable in the region."""
    conf = fake_conf(
        {
            "xcloud_availability_zones": zones,
            "n_db_nodes": [3],
            "xcloud_provider": "aws",
            "instance_type_db": "i4i.large",
        }
    )
    api_client = MagicMock()
    api_client.get_availability_zones.return_value = [
        {"id": "use1-az1", "name": "us-east-1c"},
        {"id": "use1-az2", "name": "us-east-1d"},
        {"id": "use1-az4", "name": "us-east-1a"},
    ]

    if raises:
        with pytest.raises(ValueError, match="cannot be used in region"):
            SCTConfiguration._validate_xcloud_availability_zones(conf, api_client, 1, 1)
    else:
        SCTConfiguration._validate_xcloud_availability_zones(conf, api_client, 1, 1)


def test_get_availability_zones_resolves_cloud_account_and_sorts():
    """get_availability_zones resolves the cloud account ID and returns the zones sorted by ID."""
    client = make_api_client()
    active_accounts = [
        {"id": 201, "cloudProviderId": 2, "state": "ACTIVE"},
        {"id": 2, "cloudProviderId": 1, "state": "ACTIVE"},
    ]
    unsorted_zones = [{"id": "use1-az4", "name": "us-east-1a"}, {"id": "use1-az1", "name": "us-east-1c"}]

    with (
        patch.object(client, "get_current_account_id", return_value=7),
        patch.object(client, "get_active_accounts", return_value=active_accounts),
        patch.object(client, "request", return_value=unsorted_zones) as request_mock,
    ):
        zones = client.get_availability_zones(cloud_provider_id=1, region_id=3, instance_type_id=62)

    assert [zone["id"] for zone in zones] == ["use1-az1", "use1-az4"]
    request_mock.assert_called_once_with(
        "GET", "/account/7/cloud-account/2/region/3/zones", params={"instanceTypeId": 62}
    )


def test_create_cluster_request_without_az_override_keeps_payload():
    """create_cluster_request sends the base payload unchanged when no zone override is given."""
    client = make_api_client()
    client.request = MagicMock(return_value={"requestId": 1})
    client.create_cluster_request(**minimal_create_kwargs())
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
    """create_cluster_request sends the zone override IDs and sets placement when availability zones are given."""
    client = make_api_client()
    client.request = MagicMock(return_value={"requestId": 1})
    client.create_cluster_request(**minimal_create_kwargs(), availability_zone_ids=["use1-az1", "use1-az2", "use1-az3"])
    body = client.request.call_args.kwargs
    assert body["availabilityZoneIdsOverride"] == ["use1-az1", "use1-az2", "use1-az3"]
    assert body["placement"] == "true"


@patch("sdcm.cluster_cloud.get_username", return_value="user")
@patch("sdcm.cluster_cloud.get_test_name", return_value="test")
def test_prepare_cluster_config_expands_knob_to_node_count(mock_test_name, mock_username):
    """_prepare_cluster_config repeats the configured zone list to cover every node."""
    az_ids = ["use1-az1", "use1-az2", "use1-az3"]
    cluster = mock_cluster_for_prepare_config(az_knob=",".join(az_ids))
    config = ScyllaCloudCluster._prepare_cluster_config(cluster, node_count=6, instance_type=None)
    assert config["availability_zone_ids"] == az_ids * 2


@pytest.mark.parametrize(
    "instance_types,expected_ids",
    [
        pytest.param(["i8g.large"], [1], id="one-instance-type"),
        pytest.param(["i8g.large", "i8g.xlarge"], [1, 2], id="two-instance-types"),
        pytest.param([], [], id="no-instance-types"),
    ],
)
def test_resolve_scaling_config_replaces_instance_types_with_ids(instance_types, expected_ids):
    """_resolve_scaling_config swaps instance type names for their IDs without mutating the original config."""
    cluster = mock_cluster_for_prepare_config()
    ids = {"i8g.large": 1, "i8g.xlarge": 2}
    cluster._api_client.get_instance_id_by_name = MagicMock(side_effect=lambda **kw: ids[kw["instance_type_name"]])
    cluster.xcloud_scaling_config = {"Mode": "xcloud", "InstanceTypes": instance_types}

    scaling_config = ScyllaCloudCluster._resolve_scaling_config(cluster)

    assert scaling_config == {"Mode": "xcloud", "InstanceTypeIDs": expected_ids}
    assert cluster.xcloud_scaling_config == {"Mode": "xcloud", "InstanceTypes": instance_types}, (
        "original config must not be mutated"
    )


def test_resolve_scaling_config_without_instance_types():
    """_resolve_scaling_config returns the config unchanged when it names no instance types."""
    cluster = mock_cluster_for_prepare_config()
    cluster.xcloud_scaling_config = {"Mode": "xcloud", "InstanceFamilies": ["i8g"]}

    assert ScyllaCloudCluster._resolve_scaling_config(cluster) == cluster.xcloud_scaling_config
    cluster._api_client.get_instance_id_by_name.assert_not_called()


@patch("sdcm.cluster_cloud.get_username", return_value="user")
@patch("sdcm.cluster_cloud.get_test_name", return_value="test")
def test_prepare_cluster_config_sends_resolved_scaling_config(mock_test_name, mock_username):
    """_prepare_cluster_config sends the resolved scaling config and creates no nodes up front."""
    cluster = mock_cluster_for_prepare_config()
    cluster.xcloud_scaling_config = {"Mode": "xcloud", "InstanceTypes": ["i8g.large"]}
    cluster._resolve_scaling_config = MagicMock(return_value={"Mode": "xcloud", "InstanceTypeIDs": [1]})

    config = ScyllaCloudCluster._prepare_cluster_config(cluster, node_count=3, instance_type=None)

    assert config["scaling"] == {"Mode": "xcloud", "InstanceTypeIDs": [1]}
    assert config["number_of_nodes"] == 0


def test_cloud_node_accepts_every_wait_db_up_argument_of_its_base():
    """`CloudNode.wait_db_up()` forwards to `BaseNode.wait_db_up()` through a decorator.

    A caller passing an argument the override does not name gets a TypeError before the base
    implementation is ever reached, so the two signatures have to stay in step.
    """
    base = inspect.signature(BaseNode.wait_db_up).parameters
    override = inspect.signature(CloudNode.wait_db_up).parameters

    assert set(base) <= set(override), f"missing in the CloudNode override: {set(base) - set(override)}"
    for name, parameter in base.items():
        assert override[name].default == parameter.default, f"'{name}' default differs from the base one"
