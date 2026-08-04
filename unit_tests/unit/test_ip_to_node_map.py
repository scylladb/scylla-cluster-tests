from unittest.mock import MagicMock, PropertyMock

from sdcm.cluster import BaseCluster, BaseNode
from sdcm.cluster_aws import AWSNode
from sdcm.provision.network_configuration import ScyllaNetworkConfiguration


def _make_node(private_ip=None, public_ip=None, ipv6_ip=None):
    node = MagicMock()
    type(node).private_ip_address = PropertyMock(return_value=private_ip)
    type(node).public_ip_address = PropertyMock(return_value=public_ip)
    type(node).ipv6_ip_address = PropertyMock(return_value=ipv6_ip)
    node.get_all_ip_addresses.return_value = [ip for ip in (private_ip, public_ip, ipv6_ip) if ip]
    return node


def _call_get_ip_to_node_map(nodes, subset=None):
    cluster = MagicMock(spec=BaseCluster)
    cluster.nodes = nodes
    return BaseCluster.get_ip_to_node_map(cluster, nodes=subset)


def test_returns_all_nodes_by_cached_ips():
    node1 = _make_node(private_ip="10.0.0.1", public_ip="54.0.0.1", ipv6_ip="2001:db8::1")
    node2 = _make_node(private_ip="10.0.0.2", public_ip="54.0.0.2", ipv6_ip="2001:db8::2")

    result = _call_get_ip_to_node_map([node1, node2])

    assert len(result) == 6
    assert result["10.0.0.1"] is node1
    assert result["54.0.0.1"] is node1
    assert result["2001:db8::1"] is node1
    assert result["10.0.0.2"] is node2
    assert result["54.0.0.2"] is node2
    assert result["2001:db8::2"] is node2


def test_subset_returns_only_requested_nodes():
    node1 = _make_node(private_ip="10.0.0.1", public_ip="54.0.0.1")
    node2 = _make_node(private_ip="10.0.0.2", public_ip="54.0.0.2")
    node3 = _make_node(private_ip="10.0.0.3", public_ip="54.0.0.3")

    result = _call_get_ip_to_node_map([node1, node2, node3], subset=[node1, node3])

    assert result["10.0.0.1"] is node1
    assert result["54.0.0.3"] is node3
    assert "10.0.0.2" not in result
    assert "54.0.0.2" not in result


def test_none_ips_excluded():
    node1 = _make_node(private_ip="10.0.0.1", public_ip=None, ipv6_ip="2001:db8::1")
    node2 = _make_node(private_ip=None, public_ip="54.0.0.2", ipv6_ip=None)

    result = _call_get_ip_to_node_map([node1, node2])

    assert len(result) == 3
    assert result["10.0.0.1"] is node1
    assert result["2001:db8::1"] is node1
    assert result["54.0.0.2"] is node2
    assert None not in result


def test_get_all_ip_addresses_uses_properties_not_refresh():
    node = MagicMock(spec=BaseNode)
    type(node).private_ip_address = PropertyMock(return_value="10.0.0.1")
    type(node).public_ip_address = PropertyMock(return_value="54.0.0.1")
    type(node).ipv6_ip_address = PropertyMock(return_value="2001:db8::1")

    result = BaseNode.get_all_ip_addresses(node)

    assert set(result) == {"10.0.0.1", "54.0.0.1", "2001:db8::1"}
    node._refresh_instance_state.assert_not_called()


def _make_aws_node(private_ip=None, public_ip=None, ipv6_ip=None, rpc_address=None, broadcast_rpc_address=None):
    node = MagicMock(spec=AWSNode)
    type(node).private_ip_address = PropertyMock(return_value=private_ip)
    type(node).public_ip_address = PropertyMock(return_value=public_ip)
    type(node).ipv6_ip_address = PropertyMock(return_value=ipv6_ip)
    node.scylla_network_configuration = MagicMock()
    node.scylla_network_configuration.rpc_address = rpc_address
    node.scylla_network_configuration.broadcast_rpc_address = broadcast_rpc_address
    return node


def test_aws_node_get_all_ip_addresses_includes_secondary_nic_rpc_address():
    # Split-network config (mirrors scylla_addresses_on_different_interfaces.yaml):
    # listen/broadcast_address on nic 0, rpc_address/broadcast_rpc_address on nic 1.
    node = _make_aws_node(
        private_ip="10.0.0.1",
        rpc_address="10.0.1.1",
        broadcast_rpc_address="10.0.1.1",
    )

    result = AWSNode.get_all_ip_addresses(node)

    assert set(result) == {"10.0.0.1", "10.0.1.1"}


def test_get_ip_to_node_map_resolves_secondary_nic_rpc_address():
    node = _make_aws_node(
        private_ip="10.0.0.1",
        rpc_address="10.0.1.1",
        broadcast_rpc_address="10.0.1.1",
    )
    node.get_all_ip_addresses.return_value = AWSNode.get_all_ip_addresses(node)

    result = _call_get_ip_to_node_map([node])

    assert result["10.0.0.1"] is node
    assert result["10.0.1.1"] is node


def test_aws_node_get_all_ip_addresses_no_duplicates_single_nic():
    # Common case: rpc_address/broadcast_rpc_address share the primary-NIC private IP already
    # returned by the base class, so no extra/duplicate entries should be introduced.
    node = _make_aws_node(
        private_ip="10.0.0.1",
        public_ip="54.0.0.1",
        rpc_address="10.0.0.1",
        broadcast_rpc_address="10.0.0.1",
    )

    result = AWSNode.get_all_ip_addresses(node)

    assert result.count("10.0.0.1") == 1
    assert set(result) == {"10.0.0.1", "54.0.0.1"}


def test_aws_node_get_all_ip_addresses_skips_listen_all():
    node = _make_aws_node(
        private_ip="10.0.0.1",
        rpc_address=ScyllaNetworkConfiguration.LISTEN_ALL,
        broadcast_rpc_address=ScyllaNetworkConfiguration.LISTEN_ALL,
    )

    result = AWSNode.get_all_ip_addresses(node)

    assert result == ["10.0.0.1"]


def test_aws_node_get_all_ip_addresses_skips_dns_name():
    node = _make_aws_node(
        private_ip="10.0.0.1",
        rpc_address="node1.internal.example.com",
        broadcast_rpc_address="node1.internal.example.com",
    )

    result = AWSNode.get_all_ip_addresses(node)

    assert result == ["10.0.0.1"]
