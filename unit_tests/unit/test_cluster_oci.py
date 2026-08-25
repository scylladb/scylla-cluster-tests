"""Unit tests for OCI cluster node behavior."""

import json
from unittest.mock import Mock, patch

import pytest

from sdcm.cluster_oci import CreateOciNodeError, OciCluster, OciNode
from sdcm.utils.oci_utils import SECONDARY_VNICS_SCRIPT_PATH

from unit_tests.lib.oci_test_helpers import (
    MOCK_CREDENTIALS,
    MOCK_PARENT_CLUSTER,
    base_node_init,
    make_cloud_instance,
)


@patch("sdcm.cluster_oci.resolve_ip_to_dns", return_value="node.internal")
@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_private_dns_name_uses_metadata_hostname_when_available(mock_resolver) -> None:
    """Test that FQDN from OCI metadata is used directly without reverse DNS lookup."""
    oci_node = OciNode(make_cloud_instance(private_ip="10.0.4.4"), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(return_value="node.private.subnet.vcn.oraclevcn.com")

    assert oci_node.private_dns_name == "node.private.subnet.vcn.oraclevcn.com"

    mock_resolver.assert_not_called()
    oci_node.log.warning.assert_not_called()


@patch("sdcm.cluster_oci.resolve_ip_to_dns", return_value="node.internal")
@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_private_dns_name_falls_back_to_reverse_dns_when_metadata_query_fails(mock_resolver) -> None:
    """Test that reverse DNS lookup is used when OCI metadata query raises an exception."""
    oci_node = OciNode(make_cloud_instance(private_ip="10.0.4.4"), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(side_effect=RuntimeError("metadata unavailable"))

    assert oci_node.private_dns_name == "node.internal"

    mock_resolver.assert_called_once_with("10.0.4.4")
    oci_node.log.warning.assert_called_once()


@patch("sdcm.cluster_oci.resolve_ip_to_dns", return_value="db-node-short.subnet.vcn.oraclevcn.com")
@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_private_dns_name_prefers_reverse_dns_fqdn_when_metadata_returns_short_label(mock_resolver) -> None:
    """Test that reverse DNS FQDN is preferred over a short hostname label from metadata."""
    oci_node = OciNode(make_cloud_instance(private_ip="10.0.4.4"), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(return_value="db-node-short")

    assert oci_node.private_dns_name == "db-node-short.subnet.vcn.oraclevcn.com"

    mock_resolver.assert_called_once_with("10.0.4.4")


@patch(
    "sdcm.cluster_oci.resolve_ip_to_dns",
    side_effect=ValueError("Unable to resolve IP: [Errno 1] Unknown host"),
)
@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_private_dns_name_falls_back_to_private_ip_when_reverse_dns_missing(mock_resolver) -> None:
    """Test that private IP is returned when both metadata and reverse DNS fail."""
    oci_node = OciNode(make_cloud_instance(private_ip="10.0.4.4"), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.query_oci_metadata = Mock(side_effect=RuntimeError("metadata unavailable"))

    assert oci_node.private_dns_name == "10.0.4.4"

    mock_resolver.assert_called_once_with("10.0.4.4")
    assert oci_node.log.warning.call_count == 2


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_private_dns_name_falls_back_to_node_name_when_private_ip_missing() -> None:
    """Test that node name is returned as last resort when private IP is also unavailable."""
    oci_node = OciNode(make_cloud_instance(private_ip=None), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
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
@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_create_node_certificate_includes_short_and_fqdn_dns_names(mock_create_cert, mock_resolver) -> None:
    """Test that node certificate includes both short hostname and FQDN as DNS names."""
    oci_node = OciNode(make_cloud_instance(private_ip="10.0.3.13"), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
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
        side_effect=lambda count, *args, **kwargs: [make_cloud_instance() for _ in range(count)]
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


# --- Network configuration and caching tests ---


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_set_network_configuration_safe_sets_config_when_valid():
    """_set_network_configuration_safe sets scylla_network_configuration on success."""
    oci_node = OciNode(make_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    mock_config = Mock()
    mock_config.test_communication = "10.0.0.1"
    oci_node._build_scylla_network_configuration = Mock(return_value=mock_config)

    oci_node._set_network_configuration_safe()

    assert oci_node.scylla_network_configuration is mock_config


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_set_network_configuration_safe_sets_none_on_failure():
    """_set_network_configuration_safe sets None when config can't resolve addresses."""
    oci_node = OciNode(make_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    mock_config = Mock()
    type(mock_config).test_communication = property(lambda self: (_ for _ in ()).throw(IndexError("no address")))
    oci_node._build_scylla_network_configuration = Mock(return_value=mock_config)

    oci_node._set_network_configuration_safe()

    assert oci_node.scylla_network_configuration is None


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_set_network_configuration_safe_sets_none_when_no_config():
    """_set_network_configuration_safe sets None when no scylla_network_config is defined."""
    oci_node = OciNode(make_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node._build_scylla_network_configuration = Mock(return_value=None)

    oci_node._set_network_configuration_safe()

    assert oci_node.scylla_network_configuration is None


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_discover_ipv6_from_os_parses_ip_json():
    """_discover_ipv6_from_os parses 'ip -6 -j addr' output into interface→address map."""
    oci_node = OciNode(make_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    ip_output = json.dumps(
        [
            {"ifname": "ens3", "addr_info": [{"family": "inet6", "local": "2001:db8::1"}]},
            {"ifname": "ens4", "addr_info": [{"family": "inet6", "local": "2001:db8::2"}]},
        ]
    )
    oci_node.remoter = Mock()
    oci_node.remoter.run.return_value = Mock(exit_status=0, stdout=ip_output)

    result = oci_node._discover_ipv6_from_os()

    assert result == {"ens3": ["2001:db8::1"], "ens4": ["2001:db8::2"]}


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_discover_ipv6_from_os_returns_empty_on_failure():
    """_discover_ipv6_from_os returns empty dict when command fails."""
    oci_node = OciNode(make_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.remoter = Mock()
    oci_node.remoter.run.return_value = Mock(exit_status=1, stdout="")

    assert oci_node._discover_ipv6_from_os() == {}


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_discover_ipv6_from_os_returns_empty_on_malformed_json():
    """_discover_ipv6_from_os returns empty dict on unparseable output."""
    oci_node = OciNode(make_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.remoter = Mock()
    oci_node.remoter.run.return_value = Mock(exit_status=0, stdout="not json")

    assert oci_node._discover_ipv6_from_os() == {}


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_network_interfaces_caches_result():
    """network_interfaces property returns cached value without rebuilding."""
    oci_node = OciNode(make_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    mock_interfaces = [Mock(), Mock()]
    oci_node._build_network_interfaces = Mock(return_value=mock_interfaces)

    first = oci_node.network_interfaces
    second = oci_node.network_interfaces

    assert first is second
    oci_node._build_network_interfaces.assert_called_once()


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_invalidate_network_interfaces_cache_forces_rebuild():
    """After invalidation, network_interfaces rebuilds from API."""
    oci_node = OciNode(make_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    first_interfaces = [Mock()]
    second_interfaces = [Mock()]
    oci_node._build_network_interfaces = Mock(side_effect=[first_interfaces, second_interfaces])

    first = oci_node.network_interfaces
    oci_node._invalidate_network_interfaces_cache()
    second = oci_node.network_interfaces

    assert first is first_interfaces
    assert second is second_interfaces
    assert oci_node._build_network_interfaces.call_count == 2


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_refresh_network_interfaces_info_invalidates_cache_before_super():
    """refresh_network_interfaces_info must invalidate cache so super() sees fresh data."""
    oci_node = OciNode(make_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node._cached_network_interfaces = [Mock()]
    oci_node._build_network_interfaces = Mock(return_value=[Mock()])
    oci_node.scylla_network_configuration = Mock()

    with patch("sdcm.cluster.BaseNode.refresh_network_interfaces_info") as mock_super:

        def check_cache_cleared():
            # At the point super() is called, the old cache must already be discarded
            assert oci_node._cached_network_interfaces is None

        mock_super.side_effect = check_cache_cleared
        oci_node.refresh_network_interfaces_info()

    mock_super.assert_called_once()


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_refresh_instance_state_without_network_config():
    """_refresh_instance_state returns instance IPs when no network config is set."""
    instance = make_cloud_instance(private_ip="10.0.1.1")
    instance.public_ip_address = "1.2.3.4"
    oci_node = OciNode(instance, MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.scylla_network_configuration = None

    public_ips, private_ips = oci_node._refresh_instance_state()

    assert public_ips == ["1.2.3.4"]
    assert private_ips == ["10.0.1.1"]


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_refresh_instance_state_with_network_config():
    """_refresh_instance_state extracts IPs from network config interfaces."""
    oci_node = OciNode(make_cloud_instance(), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    iface1 = Mock(ipv4_public_address="5.6.7.8", ipv4_private_addresses=["10.0.0.1"])
    iface2 = Mock(ipv4_public_address=None, ipv4_private_addresses=["10.0.0.2"])
    oci_node.scylla_network_configuration = Mock(network_interfaces=[iface1, iface2])
    oci_node._invalidate_network_interfaces_cache = Mock()
    oci_node._build_network_interfaces = Mock(return_value=[])

    with patch("sdcm.cluster.BaseNode.refresh_network_interfaces_info"):
        public_ips, private_ips = oci_node._refresh_instance_state()

    assert public_ips == ["5.6.7.8"]
    assert private_ips == ["10.0.0.1", "10.0.0.2"]


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_start_network_interface_reapplies_secondary_vnic_routing():
    """Bringing the secondary VNIC back up must re-run the 'sct-secondary-vnics' oneshot.

    'ip link set <iface> down' flushes the IPv6 addresses and the policy rules/routes of
    the VNIC's dedicated routing table, and the systemd unit alone only runs at boot.
    """
    parent_cluster = Mock(params={"simulated_regions": 0}, extra_network_interface=True)
    oci_node = OciNode(make_cloud_instance(), MOCK_CREDENTIALS, parent_cluster)
    oci_node.remoter = Mock()

    with patch("sdcm.cluster.BaseNode.start_network_interface") as mock_super:
        oci_node.start_network_interface()

    mock_super.assert_called_once_with(interface_name=None)
    oci_node.remoter.sudo.assert_called_once_with("systemctl restart sct-secondary-vnics.service")


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_start_network_interface_skips_routing_without_extra_interface():
    """Single-NIC clusters have no 'sct-secondary-vnics' unit installed, so skip the restart."""
    parent_cluster = Mock(params={"simulated_regions": 0}, extra_network_interface=False)
    oci_node = OciNode(make_cloud_instance(), MOCK_CREDENTIALS, parent_cluster)
    oci_node.remoter = Mock()

    with patch("sdcm.cluster.BaseNode.start_network_interface") as mock_super:
        oci_node.start_network_interface(interface_name="eth1")

    mock_super.assert_called_once_with(interface_name="eth1")
    oci_node.remoter.sudo.assert_not_called()


@patch("sdcm.cluster_oci.network_interfaces_count", return_value=2)
@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_configure_secondary_vnics_os_propagates_script_failure(mock_nic_count):
    """A failing routing setup must break the run right here, not silently later.

    Half-configured VNICs leave the node reachable over its primary interface, so swallowing
    the failure only surfaces much later as a confusing connectivity or streaming error.
    """
    oci_node = OciNode(make_cloud_instance(private_ip="10.0.1.10"), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.remoter = Mock()

    def fail_on_script_run(command, **kwargs):
        if command.startswith(SECONDARY_VNICS_SCRIPT_PATH):
            raise RuntimeError("script failed")
        return Mock()

    oci_node.remoter.sudo.side_effect = fail_on_script_run

    with pytest.raises(RuntimeError, match="script failed"):
        oci_node._configure_secondary_vnics_os()

    # the script must be run without 'ignore_status', so that the remoter raises on a non-zero exit
    run_call = [
        call for call in oci_node.remoter.sudo.call_args_list if call.args[0].startswith(SECONDARY_VNICS_SCRIPT_PATH)
    ]
    assert run_call, oci_node.remoter.sudo.call_args_list
    assert run_call[-1].kwargs == {}


@patch("sdcm.cluster_oci.network_interfaces_count", return_value=3)
@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_configure_secondary_vnics_os_passes_vnic_count_and_primary_ip(mock_nic_count):
    """Both the script invocation and the systemd unit must get the VNIC count and the primary IP."""
    oci_node = OciNode(make_cloud_instance(private_ip="10.0.1.10"), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    oci_node.remoter = Mock()

    oci_node._configure_secondary_vnics_os()

    commands = [call.args[0] for call in oci_node.remoter.sudo.call_args_list]
    assert f"{SECONDARY_VNICS_SCRIPT_PATH} 3 10.0.1.10" in commands
    unit_command = next(command for command in commands if "/etc/systemd/system/" in command)
    assert f"ExecStart={SECONDARY_VNICS_SCRIPT_PATH} 3 10.0.1.10" in unit_command


def _oci_node_with_dns_names(dns_names, use_dns_names=True):
    node = OciNode(make_cloud_instance(private_ip="10.1.5.22"), MOCK_CREDENTIALS, MOCK_PARENT_CLUSTER)
    node.__dict__["use_dns_names"] = use_dns_names
    node.scylla_network_configuration = Mock(
        network_interfaces=[Mock(dns_private_name=dns_name) for dns_name in dns_names]
    )
    node.check_dns_ready = Mock(return_value=True)
    return node


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_wait_for_private_dns_records_checks_every_interface():
    """Both the primary and the secondary VNIC records must be waited for.

    'broadcast_rpc_address' resolves to the secondary VNIC DNS name in a two-interface
    topology, and that record is published later than the primary one because SCT attaches
    the secondary VNIC only once the instance is already running.
    """
    node = _oci_node_with_dns_names(
        ["node-primary.private45ba196.sct2vcn.oraclevcn.com", "node-nic1.private805e3f9.sct2vcn.oraclevcn.com"]
    )

    node._wait_for_private_dns_records(timeout=5, interval=1)

    checked = sorted(call.kwargs["dns_host"] for call in node.check_dns_ready.call_args_list)
    assert checked == [
        "node-nic1.private805e3f9.sct2vcn.oraclevcn.com",
        "node-primary.private45ba196.sct2vcn.oraclevcn.com",
    ]


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_wait_for_private_dns_records_raises_on_unresolvable_record():
    """An unresolvable record must fail node creation, not leave Scylla to crash on startup."""
    unresolvable = "node-nic1.private805e3f9.sct2vcn.oraclevcn.com"
    node = _oci_node_with_dns_names([unresolvable])
    node.check_dns_ready = Mock(return_value=False)

    with pytest.raises(CreateOciNodeError, match=f"Private DNS record '{unresolvable}'"):
        node._wait_for_private_dns_records(timeout=5, interval=1)


@patch("sdcm.cluster.BaseNode.__init__", new=base_node_init)
def test_wait_for_private_dns_records_skipped_without_dns_names():
    """With 'use_dns_names' off, Scylla is configured with IPs, so there is nothing to wait for."""
    node = _oci_node_with_dns_names(["node-nic1.private805e3f9.sct2vcn.oraclevcn.com"], use_dns_names=False)

    node._wait_for_private_dns_records(timeout=5, interval=1)

    node.check_dns_ready.assert_not_called()
