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

from dataclasses import dataclass

import pytest


@dataclass
class FakeNode:
    """Minimal stand-in for a Docker cluster node."""

    node_index: int
    rack: int
    enable_auto_bootstrap: bool = False


@dataclass
class FakeContainer:
    """Minimal stand-in for a Docker container returned by ContainerManager."""

    name: str
    labels: dict


class FakeDockerCluster:
    """Lightweight replica of DockerCluster with only the rack-assignment logic.

    Avoids instantiating the real DockerCluster which pulls in TestConfig,
    init_log_directory, and other heavy dependencies.
    """

    def __init__(self, racks_count: int):
        self.racks_count = racks_count
        self.nodes: list[FakeNode] = []
        self.node_prefix = "fake-node"

    # ---- Methods copied verbatim from sdcm/cluster_docker.py ----

    def _get_new_node_indexes(self, count):
        return sorted(set(range(len(self.nodes) + count)) - set(node.node_index for node in self.nodes))

    def _create_node(self, node_index, container=None, rack=0):
        return FakeNode(node_index=node_index, rack=rack)

    def _create_nodes(self, count, rack=None, enable_auto_bootstrap=False):
        new_nodes = []
        for node_index in self._get_new_node_indexes(count):
            node_rack = node_index % self.racks_count if rack is None else rack
            node = self._create_node(node_index, rack=node_rack)
            node.enable_auto_bootstrap = enable_auto_bootstrap
            self.nodes.append(node)
            new_nodes.append(node)
        return new_nodes

    def _get_nodes(self):
        containers = ContainerManager_get_containers_by_prefix(self.node_prefix)
        for node_index, container in sorted(((int(c.labels["NodeIndex"]), c) for c in containers), key=lambda x: x[0]):
            node_rack = node_index % self.racks_count
            node = self._create_node(node_index, container, rack=node_rack)
            self.nodes.append(node)
        return self.nodes


# Placeholder replaced at test time via monkeypatch.
ContainerManager_get_containers_by_prefix = None


# ---------------------------------------------------------------------------
# Tests for _create_nodes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "racks_count, node_count, expected_racks",
    [
        pytest.param(3, 6, [0, 1, 2, 0, 1, 2], id="round_robin_3_racks"),
        pytest.param(2, 5, [0, 1, 0, 1, 0], id="round_robin_2_racks"),
        pytest.param(1, 3, [0, 0, 0], id="single_rack"),
    ],
)
def test_create_nodes_round_robin(racks_count, node_count, expected_racks):
    """Nodes created without an explicit rack should be assigned round-robin."""
    cluster = FakeDockerCluster(racks_count=racks_count)
    nodes = cluster._create_nodes(count=node_count)

    assert [n.rack for n in nodes] == expected_racks
    assert len(cluster.nodes) == node_count


def test_create_nodes_explicit_rack():
    """When an explicit rack is passed, every node must land on that rack."""
    cluster = FakeDockerCluster(racks_count=3)
    nodes = cluster._create_nodes(count=3, rack=1)

    assert [n.rack for n in nodes] == [1, 1, 1]


# ---------------------------------------------------------------------------
# Tests for _get_nodes
# ---------------------------------------------------------------------------


def test_get_nodes_re_derives_racks(monkeypatch):
    """_get_nodes should re-derive rack from node_index % racks_count."""
    containers = [FakeContainer(name=f"node-{i}", labels={"NodeIndex": str(i)}) for i in range(5)]

    monkeypatch.setattr(
        "unit_tests.test_docker_rack_assignment.ContainerManager_get_containers_by_prefix",
        lambda prefix: containers,
    )

    cluster = FakeDockerCluster(racks_count=3)
    nodes = cluster._get_nodes()

    assert [n.rack for n in nodes] == [0, 1, 2, 0, 1]
    assert [n.node_index for n in nodes] == [0, 1, 2, 3, 4]
