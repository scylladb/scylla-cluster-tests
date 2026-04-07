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

"""Tests for DockerCluster rack assignment in _create_nodes and _get_nodes."""

from unittest.mock import patch, MagicMock

import pytest

from unit_tests.lib.fake_docker_cluster import DummyScyllaDockerCluster


@pytest.mark.parametrize(
    "racks_count, node_count, expected_racks",
    [
        (3, 6, [0, 1, 2, 0, 1, 2]),
        (2, 5, [0, 1, 0, 1, 0]),
        (1, 3, [0, 0, 0]),
    ],
    ids=["3-racks-6-nodes", "2-racks-5-nodes", "1-rack-3-nodes"],
)
def test_create_nodes_round_robin(racks_count, node_count, expected_racks):
    """Verify that _create_nodes assigns racks in round-robin order.

    Args:
        racks_count: Number of racks configured on the cluster.
        node_count: How many nodes to create.
        expected_racks: Expected rack index for each created node.
    """
    cluster = DummyScyllaDockerCluster(params={}, racks_count=racks_count)
    nodes = cluster._create_nodes(count=node_count)

    assert [n.rack for n in nodes] == expected_racks
    assert [n.node_index for n in nodes] == list(range(node_count))


def test_create_nodes_explicit_rack():
    """Verify that passing an explicit rack overrides round-robin assignment."""
    cluster = DummyScyllaDockerCluster(params={}, racks_count=3)
    nodes = cluster._create_nodes(count=3, rack=1)

    assert [n.rack for n in nodes] == [1, 1, 1]
    assert [n.node_index for n in nodes] == [0, 1, 2]


def test_get_nodes_re_derives_racks():
    """Verify that _get_nodes re-derives rack from node_index % racks_count."""
    cluster = DummyScyllaDockerCluster(params={}, racks_count=3, node_prefix="test-node")

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
