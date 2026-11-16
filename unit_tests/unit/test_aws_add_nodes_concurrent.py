# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""`AWSCluster.add_nodes` returns the nodes that the call created.

It used to return the last `count` nodes of the cluster. When two callers add nodes at the same time (a nemesis
and the cluster growth of the scale test), the tail of the node list holds nodes of both, so a caller got a node
of the other one and never set up one of its own.
"""

from unittest.mock import MagicMock

from sdcm.cluster_aws import AWSCluster


def _cluster():
    cluster = MagicMock()
    cluster.params = {"simulated_regions": False, "simulated_racks": False, "ip_ssh_connections": "private"}
    cluster.racks_count = 1
    cluster.nodes = []
    cluster._node_index = 0
    cluster._create_or_find_instances.side_effect = lambda count, **_: [MagicMock() for _ in range(count)]
    cluster._create_node.side_effect = lambda *_, **__: MagicMock(name="node")
    return cluster


def _add_nodes(cluster, count):
    return AWSCluster.add_nodes.__wrapped__(cluster, count=count, dc_idx=0, rack=0)


def test_add_nodes_returns_the_nodes_it_created():
    cluster = _cluster()
    _add_nodes(cluster, count=2)

    new_nodes = _add_nodes(cluster, count=3)

    assert new_nodes == cluster.nodes[2:]
    assert _add_nodes(cluster, count=0) == []


def test_add_nodes_does_not_return_the_nodes_of_a_concurrent_call():
    cluster = _cluster()
    nodes_of_the_other_call = []

    def another_call_adds_a_node():
        """Another caller adds a node after this call registered its nodes and before it returns them."""
        cluster.write_node_public_ip_file.side_effect = None
        nodes_of_the_other_call.extend(_add_nodes(cluster, count=1))

    cluster.write_node_public_ip_file.side_effect = another_call_adds_a_node

    new_nodes = _add_nodes(cluster, count=2)

    assert len(cluster.nodes) == 3
    assert new_nodes == cluster.nodes[:2]
    assert nodes_of_the_other_call == cluster.nodes[2:]
