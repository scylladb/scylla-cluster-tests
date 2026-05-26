"""Unit tests for OCI cluster node behavior."""

from unittest.mock import Mock, patch

import pytest

from sdcm.cluster_oci import OciCluster, OciNode


MOCK_CREDENTIALS = Mock(key_file="/tmp/test_key")
MOCK_PARENT_CLUSTER = Mock(params={"simulated_regions": 0})


def _mock_cloud_instance(private_ip_address="10.0.4.4"):
    return Mock(
        private_ip_address=private_ip_address,
        private_dns_name=None,
        public_ip_address=None,
        instance_type="VM.Standard.E4.Flex",
        user_name="opc",
        region="us-ashburn-1",
    )


def _base_node_init(
    self, name, parent_cluster, ssh_login_info=None, base_logdir=None, node_prefix=None, dc_idx=0, rack=0
):
    self.name = name
    self.test_config = Mock(IP_SSH_CONNECTIONS="private", INTRA_NODE_COMM_PUBLIC=False)
    self._private_ip_address_cached = None
    self._public_ip_address_cached = None
    self._ipv6_ip_address_cached = None
    self.log = Mock()


@patch("sdcm.cluster_oci.resolve_ip_to_dns", return_value="node.internal")
@patch("sdcm.cluster.BaseNode.__init__", new=_base_node_init)
def test_private_dns_name_uses_metadata_hostname_when_available(mock_resolver) -> None:
    """Test that FQDN from OCI metadata is used directly without reverse DNS lookup."""
    oci_node = OciNode(_mock_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(return_value="node.private.subnet.vcn.oraclevcn.com")

    assert oci_node.private_dns_name == "node.private.subnet.vcn.oraclevcn.com"

    mock_resolver.assert_not_called()
    oci_node.log.warning.assert_not_called()


@patch("sdcm.cluster_oci.resolve_ip_to_dns", return_value="node.internal")
@patch("sdcm.cluster.BaseNode.__init__", new=_base_node_init)
def test_private_dns_name_falls_back_to_reverse_dns_when_metadata_query_fails(mock_resolver) -> None:
    """Test that reverse DNS lookup is used when OCI metadata query raises an exception."""
    oci_node = OciNode(_mock_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(side_effect=RuntimeError("metadata unavailable"))

    assert oci_node.private_dns_name == "node.internal"

    mock_resolver.assert_called_once_with("10.0.4.4")
    oci_node.log.warning.assert_called_once()


@patch("sdcm.cluster_oci.resolve_ip_to_dns", return_value="db-node-short.subnet.vcn.oraclevcn.com")
@patch("sdcm.cluster.BaseNode.__init__", new=_base_node_init)
def test_private_dns_name_prefers_reverse_dns_fqdn_when_metadata_returns_short_label(mock_resolver) -> None:
    """Test that reverse DNS FQDN is preferred over a short hostname label from metadata."""
    oci_node = OciNode(_mock_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(return_value="db-node-short")

    assert oci_node.private_dns_name == "db-node-short.subnet.vcn.oraclevcn.com"

    mock_resolver.assert_called_once_with("10.0.4.4")


@patch(
    "sdcm.cluster_oci.resolve_ip_to_dns",
    side_effect=ValueError("Unable to resolve IP: [Errno 1] Unknown host"),
)
@patch("sdcm.cluster.BaseNode.__init__", new=_base_node_init)
def test_private_dns_name_falls_back_to_private_ip_when_reverse_dns_missing(mock_resolver) -> None:
    """Test that private IP is returned when both metadata and reverse DNS fail."""
    oci_node = OciNode(_mock_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(side_effect=RuntimeError("metadata unavailable"))

    assert oci_node.private_dns_name == "10.0.4.4"

    mock_resolver.assert_called_once_with("10.0.4.4")
    assert oci_node.log.warning.call_count == 2


@patch("sdcm.cluster.BaseNode.__init__", new=_base_node_init)
def test_private_dns_name_falls_back_to_node_name_when_private_ip_missing() -> None:
    """Test that node name is returned as last resort when private IP is also unavailable."""
    oci_node = OciNode(_mock_cloud_instance(None), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(side_effect=RuntimeError("metadata unavailable"))

    dns_name = oci_node.private_dns_name

    oci_node.query_oci_metadata.assert_called_once_with("hostname")
    assert oci_node.log.warning.call_count == 2
    oci_node.log.warning.assert_any_call(
        "Failed to query OCI metadata hostname for node %s (%s). Falling back.",
        oci_node.name,
        oci_node.query_oci_metadata.side_effect,
    )
    oci_node.log.warning.assert_any_call(
        "Node %s has no private IP while resolving private DNS name. Falling back to node name.",
        oci_node.name,
    )
    assert dns_name == oci_node.name


@patch("sdcm.cluster_oci.resolve_ip_to_dns", return_value="db-node-short.subnet.vcn.oraclevcn.com")
@patch("sdcm.cluster_oci.create_certificate")
@patch("sdcm.cluster.BaseNode.__init__", new=_base_node_init)
def test_create_node_certificate_includes_short_and_fqdn_dns_names(mock_create_cert, mock_resolver) -> None:
    """Test that node certificate includes both short hostname and FQDN as DNS names."""
    oci_node = OciNode(_mock_cloud_instance("10.0.3.13"), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(return_value="db-node-short")

    oci_node.create_node_certificate("cert.pem", "key.pem")

    mock_create_cert.assert_called_once()
    dns_names = mock_create_cert.call_args.kwargs["dns_names"]
    assert "db-node-short" in dns_names
    assert "db-node-short.subnet.vcn.oraclevcn.com" in dns_names


# --- OciCluster.add_nodes rack assignment tests ---


@pytest.fixture()
def oci_cluster_for_rack_tests():
    """Real OciCluster instance with patched init and mocked external dependencies."""
    with patch.object(OciCluster, "__init__", lambda self: None):
        cluster = OciCluster()
    cluster._node_index = 0
    cluster.racks_count = 3
    cluster.nodes = []
    cluster.log = Mock()
    cluster.params = Mock()
    cluster.params.get = Mock(return_value=0)  # simulated_regions=0
    cluster.instance_provision = "on_demand"
    # needed by @mark_new_nodes_as_running_nemesis decorator
    cluster.test_config = Mock()
    cluster.test_config.tester_obj = Mock(return_value=Mock(spec=[]))
    # mock external boundaries (provisioning and node creation)
    cluster._create_instances = Mock(
        side_effect=lambda count, *args, **kwargs: [_mock_cloud_instance() for _ in range(count)]
    )
    cluster._create_node = Mock(
        side_effect=lambda instance, node_index, dc_idx, rack: Mock(
            name=f"node-{node_index}",
            rack=rack,
            enable_auto_bootstrap=False,
        )
    )
    return cluster


def test_add_nodes_rack_none_assigns_zero_based_indices(oci_cluster_for_rack_tests):
    """Rack indices must be 0-based to match AZ selection in _get_availability_domain."""
    oci_cluster_for_rack_tests.add_nodes(count=6, rack=None)

    actual_racks = [call.kwargs["rack"] for call in oci_cluster_for_rack_tests._create_node.call_args_list]
    assert actual_racks == [0, 1, 2, 0, 1, 2]


def test_add_nodes_explicit_rack_uses_value_for_all_nodes(oci_cluster_for_rack_tests):
    """When rack is explicitly specified, all nodes get that rack value."""
    oci_cluster_for_rack_tests.add_nodes(count=3, rack=2)

    actual_racks = [call.kwargs["rack"] for call in oci_cluster_for_rack_tests._create_node.call_args_list]
    assert actual_racks == [2, 2, 2]


def test_add_nodes_propagates_explicit_rack_to_create_instances(oci_cluster_for_rack_tests):
    """Explicit rack must be forwarded to _create_instances for AZ placement."""
    oci_cluster_for_rack_tests.add_nodes(count=1, rack=1)

    oci_cluster_for_rack_tests._create_instances.assert_called_once_with(1, 0, instance_type=None, rack=1)


def test_add_nodes_propagates_none_rack_to_create_instances(oci_cluster_for_rack_tests):
    """rack=None must be forwarded so _create_instances falls back to NodeIndex-based AZ."""
    oci_cluster_for_rack_tests.add_nodes(count=1, rack=None)

    oci_cluster_for_rack_tests._create_instances.assert_called_once_with(1, 0, instance_type=None, rack=None)


@pytest.mark.parametrize(
    "node_index_start,count,expected_racks",
    [
        pytest.param(0, 3, [0, 1, 2], id="offset-0"),
        pytest.param(3, 3, [0, 1, 2], id="offset-3"),
        pytest.param(6, 3, [0, 1, 2], id="offset-6"),
    ],
)
def test_add_nodes_rack_none_consistent_across_node_index_offsets(
    oci_cluster_for_rack_tests, node_index_start, count, expected_racks
):
    """Rack round-robin must produce 0,1,2 regardless of _node_index offset."""
    oci_cluster_for_rack_tests._node_index = node_index_start

    oci_cluster_for_rack_tests.add_nodes(count=count, rack=None)

    actual_racks = [call.kwargs["rack"] for call in oci_cluster_for_rack_tests._create_node.call_args_list]
    assert actual_racks == expected_racks
