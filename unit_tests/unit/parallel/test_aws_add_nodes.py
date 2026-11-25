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

"""`AWSCluster.add_nodes` initializes the new nodes in parallel."""

import threading
from unittest.mock import MagicMock

import pytest

from sdcm.cluster_aws import AWSCluster


@pytest.fixture
def aws_cluster(cluster_of, monkeypatch):
    """A stand-in AWS cluster with one rack, whose `_create_node` returns a stand-in node."""
    monkeypatch.setattr("sdcm.cluster_aws.ssh_connection_ip_type", lambda params: "ipv4")
    cluster = cluster_of(AWSCluster)
    cluster.params = {}
    cluster.racks_count = 1
    cluster._node_index = 0
    cluster.node_prefix = "test-node"
    cluster.logdir = "/tmp/sct-test"
    cluster._ec2_ami_username = "ubuntu"
    cluster.prepare_user_data.return_value = ""
    cluster._create_node.side_effect = create_node
    return cluster


def create_node(instance, ami_username, node_prefix, node_index, base_logdir, dc_idx, rack, after_config=None):
    """Stand-in for `AWSCluster._create_node`: a MagicMock node carrying its index and rack."""
    node = MagicMock()
    node.node_index = node_index
    node.rack = rack
    return node


def add_instances(cluster, count: int) -> list[MagicMock]:
    """Make `_create_or_find_instances` hand out `count` stand-in EC2 instances, in order."""
    instances = [MagicMock(id=f"i-{index}") for index in range(1, count + 1)]
    remaining = iter(instances)
    cluster._create_or_find_instances.side_effect = lambda count, **kwargs: [next(remaining) for _ in range(count)]
    return instances


def test_nodes_are_registered_in_instance_order(aws_cluster):
    """The new nodes are returned and registered in the order of their instances."""
    add_instances(aws_cluster, 4)

    added = AWSCluster.add_nodes(aws_cluster, count=4)

    assert aws_cluster.nodes == added
    assert [node.node_index for node in added] == [1, 2, 3, 4]
    assert aws_cluster._node_index == 4


def test_nodes_are_initialized_at_the_same_time(aws_cluster):
    """add_nodes fails unless all the nodes are in node.init() at the same time."""
    add_instances(aws_cluster, 3)
    barrier = threading.Barrier(3, timeout=5)

    def blocking_create_node(*args, **kwargs):
        barrier.wait()  # a serial loop breaks the barrier, raising BrokenBarrierError
        return create_node(*args, **kwargs)

    aws_cluster._create_node.side_effect = blocking_create_node

    assert len(AWSCluster.add_nodes(aws_cluster, count=3)) == 3


def test_racks_follow_the_instance_order_when_rack_is_none(aws_cluster):
    """Without a rack, the nodes are spread over the racks by their position."""
    aws_cluster.racks_count = 2
    add_instances(aws_cluster, 4)

    added = AWSCluster.add_nodes(aws_cluster, count=4, rack=None)

    assert [(node.node_index, node.rack) for node in added] == [(1, 0), (2, 1), (3, 0), (4, 1)]


def test_a_failed_node_is_left_out_and_its_instance_terminated(aws_cluster):
    """The nodes that initialized stay registered; the failed node's instance is terminated."""
    instances = add_instances(aws_cluster, 3)

    def failing_create_node(instance, *args, **kwargs):
        if instance is instances[1]:
            raise RuntimeError("init failed")
        return create_node(instance, *args, **kwargs)

    aws_cluster._create_node.side_effect = failing_create_node

    with pytest.raises(RuntimeError, match="init failed"):
        AWSCluster.add_nodes(aws_cluster, count=3)

    assert [node.node_index for node in aws_cluster.nodes] == [1, 3]
    instances[1].terminate.assert_called_once_with()
    instances[0].terminate.assert_not_called()
    instances[2].terminate.assert_not_called()
    aws_cluster.write_node_public_ip_file.assert_not_called()


def test_a_timed_out_node_has_its_instance_terminated(aws_cluster, monkeypatch):
    """A node still initializing at the timeout is left out, and its instance is terminated."""
    monkeypatch.setattr("sdcm.cluster_aws.NODE_INIT_TIMEOUT", 0.2)
    instances = add_instances(aws_cluster, 3)
    release = threading.Event()

    def hanging_create_node(instance, *args, **kwargs):
        if instance is instances[1]:
            release.wait(timeout=10)
        return create_node(instance, *args, **kwargs)

    aws_cluster._create_node.side_effect = hanging_create_node

    try:
        with pytest.raises(TimeoutError):
            AWSCluster.add_nodes(aws_cluster, count=3)
    finally:
        release.set()

    assert [node.node_index for node in aws_cluster.nodes] == [1, 3]
    instances[1].terminate.assert_called_once_with()


def test_zero_count_adds_nothing(aws_cluster):
    """add_nodes with count=0 creates no node."""
    assert AWSCluster.add_nodes(aws_cluster, count=0) == []
    aws_cluster._create_node.assert_not_called()
