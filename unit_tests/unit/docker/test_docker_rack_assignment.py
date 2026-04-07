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
# Copyright (c) 2025 ScyllaDB

"""Tests for DockerCluster rack assignment and node_container_run_args."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster_docker import NodeContainerMixin
from unit_tests.lib.fake_docker_cluster import DummyScyllaDockerCluster


@pytest.mark.parametrize(
    "racks_count, node_count, expected_racks",
    [
        pytest.param(3, 6, [0, 1, 2, 0, 1, 2], id="3-racks-6-nodes"),
        pytest.param(2, 5, [0, 1, 0, 1, 0], id="2-racks-5-nodes"),
        pytest.param(1, 3, [0, 0, 0], id="1-rack-3-nodes"),
    ],
)
def test_create_nodes_round_robin(tmp_path, racks_count, node_count, expected_racks):
    """Verify that _create_nodes assigns racks in round-robin order.

    Args:
        racks_count: Number of racks configured on the cluster.
        node_count: How many nodes to create.
        expected_racks: Expected rack index for each created node.
    """
    cluster = DummyScyllaDockerCluster(params={}, logdir=tmp_path, racks_count=racks_count)
    nodes = cluster._create_nodes(count=node_count)

    assert [n.rack for n in nodes] == expected_racks
    assert [n.node_index for n in nodes] == list(range(node_count))


def test_create_nodes_explicit_rack(tmp_path):
    """Verify that passing an explicit rack overrides round-robin assignment."""
    cluster = DummyScyllaDockerCluster(params={}, logdir=tmp_path, racks_count=3)
    nodes = cluster._create_nodes(count=3, rack=1)

    assert [n.rack for n in nodes] == [1, 1, 1]
    assert [n.node_index for n in nodes] == [0, 1, 2]


def test_get_nodes_re_derives_racks(tmp_path):
    """Verify that _get_nodes re-derives rack from node_index % racks_count."""
    cluster = DummyScyllaDockerCluster(params={}, logdir=tmp_path, racks_count=3, node_prefix="test-node")

    fake_containers = []
    for idx in range(5):
        container = MagicMock()
        container.labels = {"NodeIndex": str(idx)}
        container.name = f"test-node-{idx}"
        fake_containers.append(container)

    with patch("sdcm.cluster_docker.ContainerManager") as mock_cm:
        mock_cm.get_containers_by_prefix.return_value = fake_containers
        nodes = cluster._get_nodes()

    assert [n.rack for n in nodes] == [0, 1, 2, 0, 1]
    assert [n.node_index for n in nodes] == [0, 1, 2, 3, 4]


# ---------------------------------------------------------------------------
# node_container_run_args tests
# ---------------------------------------------------------------------------


def _make_mixin_node(rack=0, node_type="db", simulated_racks=0, seed_ip=None, n_db_nodes=None):
    """Return a minimal NodeContainerMixin instance with stubbed attributes."""
    node = object.__new__(NodeContainerMixin)
    node.rack = rack
    node.node_type = node_type
    node.name = f"test-db-node-{rack}"
    node.node_container_image_tag = "scylladb/scylla-nightly:test"
    node.parent_cluster = SimpleNamespace(
        params={
            "simulated_racks": simulated_racks,
            "n_db_nodes": n_db_nodes if n_db_nodes is not None else [3],
            "docker_network": "bridge",
            "append_scylla_args": "--smp 1",
        }
    )
    return node


def test_no_rack_args_when_simulated_racks_zero():
    """simulated_racks=0 → command is None (no seed, no rack args)."""
    node = _make_mixin_node(rack=0, simulated_racks=0)
    result = node.node_container_run_args(seed_ip=None)
    assert result["command"] is None


def test_no_rack_args_when_simulated_racks_one():
    """simulated_racks=1 → not > 1 → command is None."""
    node = _make_mixin_node(rack=0, simulated_racks=1)
    result = node.node_container_run_args(seed_ip=None)
    assert result["command"] is None


def test_rack_args_injected_when_simulated_racks_two():
    """simulated_racks=2, rack=0 → command contains --dc and --rack=RACK0."""
    node = _make_mixin_node(rack=0, simulated_racks=2)
    result = node.node_container_run_args(seed_ip=None)
    assert result["command"] is not None
    assert "--dc=datacenter1" in result["command"]
    assert "--rack=RACK0" in result["command"]


def test_rack_index_used_correctly():
    """rack=1 with simulated_racks=3 → command contains --rack=RACK1."""
    node = _make_mixin_node(rack=1, simulated_racks=3)
    result = node.node_container_run_args(seed_ip=None)
    assert "--rack=RACK1" in result["command"]
    assert "--rack=RACK0" not in result["command"]


def test_seed_and_rack_combined():
    """When seed_ip is set and simulated_racks=2, command has both --seeds and --rack."""
    node = _make_mixin_node(rack=0, simulated_racks=2)
    result = node.node_container_run_args(seed_ip="10.0.0.1")
    cmd = result["command"]
    assert '--seeds="10.0.0.1"' in cmd
    assert "--dc=datacenter1" in cmd
    assert "--rack=RACK0" in cmd


def test_seed_only_when_no_simulated_racks():
    """When seed_ip is set but simulated_racks=0, command contains only the seed arg."""
    node = _make_mixin_node(rack=0, simulated_racks=0)
    result = node.node_container_run_args(seed_ip="10.0.0.2")
    assert result["command"] == '--seeds="10.0.0.2"'
    assert "--rack" not in result["command"]


def test_non_db_node_gets_no_rack_args():
    """Non-db node types must never receive rack args regardless of simulated_racks."""
    node = _make_mixin_node(rack=0, node_type="loader", simulated_racks=3)
    result = node.node_container_run_args(seed_ip=None)
    assert result["command"] is None


def test_non_db_node_with_no_seed_and_no_racks():
    """Non-db node, no seed, no simulated_racks → command is None."""
    node = _make_mixin_node(rack=0, node_type="monitor", simulated_racks=0)
    result = node.node_container_run_args(seed_ip=None)
    assert result["command"] is None


@pytest.mark.parametrize(
    "n_db_nodes",
    [pytest.param([1], id="single-dc"), pytest.param([1, 0], id="multi-dc")],
)
def test_no_rack_args_for_single_node_cluster(n_db_nodes):
    """A one-node cluster gets no rack args, whatever simulated_racks says.

    It cannot span racks, and the snitch auto-resolution leaves `endpoint_snitch` alone, so
    the rack would never be read -- while the argument itself is fatal on a pre-2026.1 image.
    """
    node = _make_mixin_node(rack=0, simulated_racks=3, n_db_nodes=n_db_nodes)
    result = node.node_container_run_args(seed_ip=None)
    assert result["command"] is None


def test_rack_args_survive_growing_past_the_configured_racks():
    """A node added by a nemesis keeps getting rack args: `n_db_nodes` is the configured topology.

    `_create_nodes` hands out `node_index % racks_count`, so the fourth node of a 3-node,
    3-rack cluster lands back on RACK0.
    """
    node = _make_mixin_node(rack=0, simulated_racks=3, n_db_nodes=[3])
    result = node.node_container_run_args(seed_ip="10.0.0.1")
    assert "--rack=RACK0" in result["command"]
