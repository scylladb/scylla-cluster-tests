from unittest.mock import MagicMock, PropertyMock

from sdcm.cluster import BaseCluster, BaseNode


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
