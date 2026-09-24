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

"""Tests for simulated rack placement on the baremetal backend.

A physical host has no availability zone to derive a rack from, so without
`simulated_racks` every node lands in RACK0.  On ScyllaDB >= 2025.3, where
`rf_rack_valid_keyspaces` is enabled by default, a single rack rejects any
keyspace with RF > 1 as not RF-rack-valid.
"""

from unittest.mock import MagicMock

import pytest

from sdcm import cluster as sdcm_cluster
from sdcm.cluster_baremetal import (
    NodeIpsNotConfiguredError,
    PhysicalMachineCluster,
    PhysicalMachineNode,
)


def _cluster(node_count: int, racks_count: int) -> MagicMock:
    """A stand-in carrying only what add_nodes() touches."""
    cluster = MagicMock()
    cluster.node_prefix = "db-node"
    cluster.racks_count = racks_count
    cluster.nodes = []
    cluster._node_index = 0  # BaseCluster.__init__ sets this and add_nodes advances it
    cluster._node_public_ips = [f"1.2.3.{index}" for index in range(node_count)]
    cluster._node_private_ips = [f"10.0.0.{index}" for index in range(node_count)]
    return cluster


def _racks_assigned(cluster: MagicMock) -> list[int]:
    return [call.kwargs["rack"] for call in cluster._create_node.call_args_list]


@pytest.mark.parametrize(
    "node_count, racks_count, expected",
    [
        pytest.param(3, 3, [0, 1, 2], id="one-node-per-rack"),
        pytest.param(6, 3, [0, 1, 2, 0, 1, 2], id="round-robin-wraps"),
        pytest.param(4, 3, [0, 1, 2, 0], id="uneven-spread"),
        pytest.param(3, 1, [0, 0, 0], id="single-rack-stays-in-rack0"),
    ],
)
def test_nodes_spread_over_simulated_racks(node_count, racks_count, expected):
    """BaseCluster passes rack=None when simulated_racks is set: spread them ourselves."""
    cluster = _cluster(node_count, racks_count)

    PhysicalMachineCluster.add_nodes(cluster, count=node_count, rack=None)

    assert _racks_assigned(cluster) == expected


def test_explicit_rack_is_honoured():
    """Without simulated_racks the caller pins the rack, and it must not be recomputed."""
    cluster = _cluster(node_count=3, racks_count=3)

    PhysicalMachineCluster.add_nodes(cluster, count=3, rack=2)

    assert _racks_assigned(cluster) == [2, 2, 2]


def test_addresses_still_follow_the_node_index():
    """Rack spreading must not disturb which host each node is bound to."""
    cluster = _cluster(node_count=3, racks_count=3)

    PhysicalMachineCluster.add_nodes(cluster, count=3, rack=None)

    positional = [call.args[1:3] for call in cluster._create_node.call_args_list]
    assert positional == [("1.2.3.0", "10.0.0.0"), ("1.2.3.1", "10.0.0.1"), ("1.2.3.2", "10.0.0.2")]


def test_different_instance_types_are_rejected():
    """Pre-provisioned hosts cannot be re-typed; the guard predates rack support."""
    cluster = _cluster(node_count=1, racks_count=1)

    with pytest.raises(AssertionError):
        PhysicalMachineCluster.add_nodes(cluster, count=1, instance_type="i4i.large")


def test_add_nodes_returns_the_nodes_it_created():
    """BaseCluster.add_nodes is documented to return the list of nodes, as AWS and GCE do."""
    cluster = _cluster(node_count=3, racks_count=3)

    added = PhysicalMachineCluster.add_nodes(cluster, count=3, rack=None)

    assert added == cluster.nodes
    assert len(added) == 3


def test_a_second_call_continues_instead_of_restarting():
    """A grow/nemesis call must take the next host, not re-take the first one."""
    cluster = _cluster(node_count=4, racks_count=3)

    PhysicalMachineCluster.add_nodes(cluster, count=3, rack=None)
    PhysicalMachineCluster.add_nodes(cluster, count=1, rack=None)

    assert _racks_assigned(cluster) == [0, 1, 2, 0]
    names = [call.args[0] for call in cluster._create_node.call_args_list]
    assert names == ["db-node-0", "db-node-1", "db-node-2", "db-node-3"]
    addresses = [call.args[1] for call in cluster._create_node.call_args_list]
    assert addresses == ["1.2.3.0", "1.2.3.1", "1.2.3.2", "1.2.3.3"]


def test_running_out_of_configured_hosts_is_reported_clearly():
    """Physical hosts are a fixed list; asking past the end must say so, not IndexError."""
    cluster = _cluster(node_count=2, racks_count=2)

    with pytest.raises(NodeIpsNotConfiguredError, match="only 2 physical host"):
        PhysicalMachineCluster.add_nodes(cluster, count=3, rack=None)


def test_an_oversized_request_creates_nothing():
    """Validation happens up front: a request that cannot be satisfied in full must not leave
    half the nodes on the cluster, with _node_index advanced, while the caller gets an
    exception instead of the list it needs to initialize them with."""
    cluster = _cluster(node_count=2, racks_count=2)

    with pytest.raises(NodeIpsNotConfiguredError):
        PhysicalMachineCluster.add_nodes(cluster, count=3, rack=None)

    assert cluster._create_node.call_count == 0
    assert cluster.nodes == []
    assert cluster._node_index == 0


def test_a_grow_past_the_end_leaves_the_existing_nodes_alone():
    """The same, for a second call: the first three nodes stay, nothing partial is added."""
    cluster = _cluster(node_count=3, racks_count=3)
    PhysicalMachineCluster.add_nodes(cluster, count=3, rack=None)

    with pytest.raises(NodeIpsNotConfiguredError, match="1 node\\(s\\) requested from index 3"):
        PhysicalMachineCluster.add_nodes(cluster, count=1, rack=None)

    assert cluster._create_node.call_count == 3
    assert len(cluster.nodes) == 3
    assert cluster._node_index == 3


def test_disruption_name_is_accepted_and_marks_the_new_nodes():
    """Every other backend decorates add_nodes with @mark_new_nodes_as_running_nemesis;
    without it `_add_and_init_new_cluster_nodes()` -- which always passes disruption_name --
    raises TypeError before a node is added."""
    cluster = _cluster(node_count=2, racks_count=2)
    allocator = MagicMock()
    cluster.test_config.tester_obj.return_value.nemesis_allocator = allocator

    added = PhysicalMachineCluster.add_nodes(cluster, count=1, rack=None, disruption_name="GrowShrink")

    assert len(added) == 1
    allocator.set_running_nemesis.assert_called_once_with(added[0], "GrowShrink")


def test_after_config_reaches_the_node():
    """BaseScyllaCluster.node_setup runs BaseNode.after_config; dropping it here silently
    skips the caller's post-install hook."""
    cluster = _cluster(node_count=1, racks_count=1)
    callback = MagicMock()

    PhysicalMachineCluster.add_nodes(cluster, count=1, rack=0, after_config=callback)

    assert cluster._create_node.call_args.kwargs["after_config"] is callback


def test_capacity_counts_address_pairs_not_public_ips():
    """A host needs both addresses; a short private list must not slip past the guard."""
    cluster = _cluster(node_count=3, racks_count=1)
    cluster._node_private_ips = cluster._node_private_ips[:2]  # one host lacks a private address

    with pytest.raises(NodeIpsNotConfiguredError, match="only 2 physical host"):
        PhysicalMachineCluster.add_nodes(cluster, count=3, rack=None)


def test_the_rack_survives_the_hops_into_basenode(monkeypatch):
    """The tests above stop at a mocked `_create_node`, so they would still pass if the rack were
    dropped on the way to the node.  Exercise the two real hops --
    `_create_node()` -> `PhysicalMachineNode` -> `BaseNode` -- since that is the handoff the whole
    feature rests on: `SnitchConfig` reads `node.rack` to write cassandra-rackdc.properties.
    """
    recorded = {}

    def record_base_init(self, *args, **kwargs):
        recorded.update(kwargs)

    monkeypatch.setattr(sdcm_cluster.BaseNode, "__init__", record_base_init)
    monkeypatch.setattr(PhysicalMachineNode, "init", lambda self: None)

    parent = MagicMock()
    parent.credentials = [MagicMock(key_file="/dev/null")]
    callback = MagicMock()

    node = PhysicalMachineCluster._create_node(
        parent,
        "db-node-1",
        "1.2.3.1",
        "10.0.0.1",
        dc_idx=0,
        rack=2,
        node_index=1,
        after_config=callback,
    )

    assert recorded["rack"] == 2, "rack dropped between _create_node() and BaseNode"
    assert recorded["after_config"] is callback, "after_config dropped on the way to the node"
    assert node.node_index == 1
    assert node._public_ip == "1.2.3.1"
    assert node._private_ip == "10.0.0.1"
