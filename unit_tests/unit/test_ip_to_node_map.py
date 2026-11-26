from unittest.mock import MagicMock, PropertyMock

from sdcm.cluster import BaseCluster, BaseNode
from sdcm.cluster_aws import AWSNode
from sdcm.provision.network_configuration import NetworkInterface, ScyllaNetworkConfiguration
from unit_tests.lib.oci_test_helpers import make_node_mock, make_oci_node_mock


def make_node(private_ip=None, public_ip=None, ipv6_ip=None):
    """Build a MagicMock node whose IP properties and get_all_ip_addresses return the given IPs."""
    node = MagicMock()
    type(node).private_ip_address = PropertyMock(return_value=private_ip)
    type(node).public_ip_address = PropertyMock(return_value=public_ip)
    type(node).ipv6_ip_address = PropertyMock(return_value=ipv6_ip)
    node.get_all_ip_addresses.return_value = [ip for ip in (private_ip, public_ip, ipv6_ip) if ip]
    return node


def call_get_ip_to_node_map(nodes, subset=None):
    """Call BaseCluster.get_ip_to_node_map on a mock cluster holding the given nodes."""
    cluster = MagicMock(spec=BaseCluster)
    cluster.nodes = nodes
    return BaseCluster.get_ip_to_node_map(cluster, nodes=subset)


def make_interface(device_index, private_ip, public_ip=None, ipv6_ip=None, dns_name=None, use_dns_names=False):
    """Build a NetworkInterface with the given device index and addresses."""
    return NetworkInterface(
        ipv4_public_address=public_ip,
        ipv6_public_addresses=[ipv6_ip] if ipv6_ip else [],
        ipv4_private_addresses=[private_ip],
        ipv6_private_address=ipv6_ip or "",
        dns_private_name=dns_name,
        dns_public_name=None,
        device_index=device_index,
        device_name=f"eth{device_index}",
        mac_address=None,
        use_dns_names=use_dns_names,
    )


def make_aws_node(private_ip=None, public_ip=None, ipv6_ip=None, network_interfaces=()):
    """Build an AWSNode mock with the given IPs and network interfaces."""
    return make_node_mock(AWSNode, private_ip, public_ip, ipv6_ip, network_interfaces)


def test_returns_all_nodes_by_cached_ips():
    """get_ip_to_node_map maps every private, public and IPv6 address of each node to that node."""
    node1 = make_node(private_ip="10.0.0.1", public_ip="54.0.0.1", ipv6_ip="2001:db8::1")
    node2 = make_node(private_ip="10.0.0.2", public_ip="54.0.0.2", ipv6_ip="2001:db8::2")

    result = call_get_ip_to_node_map([node1, node2])

    assert len(result) == 6
    assert result["10.0.0.1"] is node1
    assert result["54.0.0.1"] is node1
    assert result["2001:db8::1"] is node1
    assert result["10.0.0.2"] is node2
    assert result["54.0.0.2"] is node2
    assert result["2001:db8::2"] is node2


def test_subset_returns_only_requested_nodes():
    """get_ip_to_node_map with subset maps only the IPs of the requested nodes."""
    node1 = make_node(private_ip="10.0.0.1", public_ip="54.0.0.1")
    node2 = make_node(private_ip="10.0.0.2", public_ip="54.0.0.2")
    node3 = make_node(private_ip="10.0.0.3", public_ip="54.0.0.3")

    result = call_get_ip_to_node_map([node1, node2, node3], subset=[node1, node3])

    assert result["10.0.0.1"] is node1
    assert result["54.0.0.3"] is node3
    assert "10.0.0.2" not in result
    assert "54.0.0.2" not in result


def test_none_ips_excluded():
    """get_ip_to_node_map skips IPs that are None and never maps None as a key."""
    node1 = make_node(private_ip="10.0.0.1", public_ip=None, ipv6_ip="2001:db8::1")
    node2 = make_node(private_ip=None, public_ip="54.0.0.2", ipv6_ip=None)

    result = call_get_ip_to_node_map([node1, node2])

    assert len(result) == 3
    assert result["10.0.0.1"] is node1
    assert result["2001:db8::1"] is node1
    assert result["54.0.0.2"] is node2
    assert None not in result


def test_get_all_ip_addresses_uses_properties_not_refresh():
    """BaseNode.get_all_ip_addresses reads the cached IP properties without refreshing the instance state."""
    node = MagicMock(spec=BaseNode)
    type(node).private_ip_address = PropertyMock(return_value="10.0.0.1")
    type(node).public_ip_address = PropertyMock(return_value="54.0.0.1")
    type(node).ipv6_ip_address = PropertyMock(return_value="2001:db8::1")

    result = BaseNode.get_all_ip_addresses(node)

    assert set(result) == {"10.0.0.1", "54.0.0.1", "2001:db8::1"}
    node._refresh_instance_state.assert_not_called()


def test_aws_node_get_all_ip_addresses_includes_secondary_nic():
    """AWS get_all_ip_addresses includes the address of a secondary NIC."""
    # Split-network config (mirrors scylla_addresses_on_different_interfaces.yaml):
    # listen/broadcast_address on nic 0, rpc_address/broadcast_rpc_address on nic 1.
    node = make_aws_node(
        private_ip="10.0.0.1",
        network_interfaces=[make_interface(0, "10.0.0.1"), make_interface(1, "10.0.1.1")],
    )

    result = BaseNode.get_all_ip_addresses(node)

    assert set(result) == {"10.0.0.1", "10.0.1.1"}


def test_get_ip_to_node_map_resolves_secondary_nic_address():
    """get_ip_to_node_map maps the secondary NIC address of an AWS node to that node."""
    node = make_aws_node(
        private_ip="10.0.0.1",
        network_interfaces=[make_interface(0, "10.0.0.1"), make_interface(1, "10.0.1.1")],
    )
    node.get_all_ip_addresses.return_value = BaseNode.get_all_ip_addresses(node)

    result = call_get_ip_to_node_map([node])

    assert result["10.0.0.1"] is node
    assert result["10.0.1.1"] is node


def test_aws_node_get_all_ip_addresses_no_duplicates_single_nic():
    """AWS get_all_ip_addresses adds no duplicate entries when the only NIC carries the primary IPs."""
    # Common case: the only NIC carries the IPs already returned for the primary interface,
    # so no extra/duplicate entries should be introduced.
    node = make_aws_node(
        private_ip="10.0.0.1",
        public_ip="54.0.0.1",
        ipv6_ip="2001:db8::1",
        network_interfaces=[make_interface(0, "10.0.0.1", public_ip="54.0.0.1", ipv6_ip="2001:db8::1")],
    )

    result = BaseNode.get_all_ip_addresses(node)

    assert sorted(result) == sorted(["10.0.0.1", "54.0.0.1", "2001:db8::1"])


def test_aws_node_get_all_ip_addresses_includes_secondary_nic_ipv6():
    """AWS get_all_ip_addresses includes the IPv6 address of a secondary NIC in compressed form."""
    node = make_aws_node(
        private_ip="10.0.0.1",
        network_interfaces=[
            make_interface(0, "10.0.0.1"),
            make_interface(1, "10.0.1.1", ipv6_ip="2001:0db8:0000:0000:0000:0000:0000:0002"),
        ],
    )

    result = BaseNode.get_all_ip_addresses(node)

    assert set(result) == {"10.0.0.1", "10.0.1.1", "2001:db8::2"}


def test_oci_node_get_all_ip_addresses_includes_secondary_nic():
    """OCI get_all_ip_addresses includes the address of a secondary NIC."""
    node = make_oci_node_mock(
        private_ip="10.1.3.5",
        network_interfaces=[make_interface(0, "10.1.3.5"), make_interface(1, "10.1.5.27")],
    )

    result = BaseNode.get_all_ip_addresses(node)

    assert set(result) == {"10.1.3.5", "10.1.5.27"}


def test_oci_node_get_all_ip_addresses_without_network_interfaces():
    """OCI get_all_ip_addresses returns only the private IP when no network interfaces are configured."""
    node = make_oci_node_mock(private_ip="10.1.3.5")

    result = BaseNode.get_all_ip_addresses(node)

    assert result == ["10.1.3.5"]


def test_oci_node_get_all_ip_addresses_with_dns_names_on_secondary_nic():
    """OCI node with DNS names on the secondary NIC still maps the NIC IP that nodetool reports."""
    # SCT-1360: with 'use_dns_names' the rpc/broadcast_rpc addresses of the configuration are DNS
    # names, while 'nodetool status' and 'nodetool gossipinfo' report the IP of the secondary NIC
    # where Scylla listens. That IP must still map to the node.
    scylla_network_config = [
        {"address": "listen_address", "ip_type": "ipv4", "public": False, "nic": 1},
        {"address": "rpc_address", "ip_type": "ipv4", "public": False, "nic": 1},
        {"address": "broadcast_rpc_address", "ip_type": "ipv4", "public": False, "nic": 1},
        {"address": "broadcast_address", "ip_type": "ipv4", "public": False, "nic": 1},
        {"address": "test_communication", "ip_type": "ipv4", "public": False, "nic": 0},
    ]
    node = make_oci_node_mock(private_ip="10.1.3.241")
    node.scylla_network_configuration = ScyllaNetworkConfiguration(
        network_interfaces=[
            make_interface(0, "10.1.3.241", dns_name="node1-nic0.private.sct2vcn.oraclevcn.com", use_dns_names=True),
            make_interface(1, "10.1.5.195", dns_name="node1-nic1.private.sct2vcn.oraclevcn.com", use_dns_names=True),
        ],
        scylla_network_config=scylla_network_config,
    )
    assert node.scylla_network_configuration.broadcast_rpc_address == "node1-nic1.private.sct2vcn.oraclevcn.com"
    node.get_all_ip_addresses.return_value = BaseNode.get_all_ip_addresses(node)

    result = call_get_ip_to_node_map([node])

    assert result["10.1.5.195"] is node
    assert result["10.1.3.241"] is node
