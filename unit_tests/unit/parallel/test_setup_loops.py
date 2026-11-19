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

"""The cluster setup loops that run their nodes in parallel.

Each loop below is set up to call `hook(node)` once per node, at the point where that node's work
blocks on SSH, a container or a wait. It returns a callable that runs the loop.
"""

import threading
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from sdcm.cluster import BaseScyllaCluster
from sdcm.cluster_aws import CassandraAWSCluster, VectorStoreSetAWS
from sdcm.cluster_docker import VectorStoreSetDocker


def update_seed_provider(cluster_of, nodes, hook, monkeypatch):
    for node in nodes:

        @contextmanager
        def remote_scylla_yaml(node=node):
            hook(node)
            yield MagicMock()

        node.remote_scylla_yaml.side_effect = remote_scylla_yaml
    cluster = cluster_of(BaseScyllaCluster, nodes)
    return lambda: BaseScyllaCluster.update_seed_provider(cluster)


def cassandra_wait_for_init(cluster_of, nodes, hook, monkeypatch):
    monkeypatch.setattr("sdcm.cluster_aws.wait.wait_for", lambda func, node, **kwargs: hook(node))
    cluster = cluster_of(CassandraAWSCluster, nodes)
    # wait_for_init_wrap sets up and starts every node before it calls wait_for_init; this checks
    # only the wait itself.
    return lambda: CassandraAWSCluster.wait_for_init.__wrapped__(cluster)


def vector_store_aws_reconfigure(cluster_of, nodes, hook, monkeypatch):
    for node in nodes:
        node.configure_vector_store_service.side_effect = lambda node=node: hook(node)
    cluster = cluster_of(VectorStoreSetAWS, nodes)
    cluster.params = {}  # no source build: the nodes keep the vector-store of their AMI
    return lambda: VectorStoreSetAWS._reconfigure_vector_store_nodes(cluster)


def vector_store_docker_reconfigure(cluster_of, nodes, hook, monkeypatch):
    container_manager = MagicMock()
    container_manager.is_running.return_value = False
    container_manager.run_container.side_effect = lambda node, container_name: hook(node)
    monkeypatch.setattr("sdcm.cluster_docker.ContainerManager", container_manager)
    cluster = cluster_of(VectorStoreSetDocker, nodes)
    return lambda: VectorStoreSetDocker._reconfigure_vector_store_nodes(cluster)


SETUP_LOOPS = [
    pytest.param(update_seed_provider, id="update_seed_provider"),
    pytest.param(cassandra_wait_for_init, id="cassandra_wait_for_init"),
    pytest.param(vector_store_aws_reconfigure, id="vector_store_aws_reconfigure"),
    pytest.param(vector_store_docker_reconfigure, id="vector_store_docker_reconfigure"),
]


@pytest.mark.parametrize("setup_loop", SETUP_LOOPS)
def test_every_node_goes_through_the_loop_once(setup_loop, cluster_of, nodes, monkeypatch):
    """Each node goes through the loop exactly once."""
    processed = []

    setup_loop(cluster_of, nodes, lambda node: processed.append(node.name), monkeypatch)()

    assert sorted(processed) == ["node-0", "node-1", "node-2"]


@pytest.mark.parametrize("setup_loop", SETUP_LOOPS)
def test_nodes_go_through_the_loop_at_the_same_time(setup_loop, cluster_of, nodes, monkeypatch):
    """The loop fails unless all the nodes are in their per-node work at the same time."""
    barrier = threading.Barrier(len(nodes), timeout=5)

    # a loop that handles one node at a time breaks the barrier, raising BrokenBarrierError
    setup_loop(cluster_of, nodes, lambda node: barrier.wait(), monkeypatch)()


@pytest.mark.parametrize("setup_loop", SETUP_LOOPS)
def test_failure_on_one_node_is_raised_after_every_node_ran(setup_loop, cluster_of, nodes, monkeypatch):
    """A failure on one node fails the loop, and the other nodes still go through it."""
    processed = []

    def hook(node):
        processed.append(node.name)
        if node is nodes[1]:
            raise RuntimeError(f"{node.name} failed")

    with pytest.raises(RuntimeError, match="node-1 failed"):
        setup_loop(cluster_of, nodes, hook, monkeypatch)()

    assert sorted(processed) == ["node-0", "node-1", "node-2"]


def test_update_seed_provider_writes_each_node_s_proposed_seed_provider(cluster_of, nodes):
    """Each node's scylla.yaml gets the seed provider that was proposed for that node."""
    scylla_yamls = {node.name: MagicMock() for node in nodes}
    for node in nodes:

        @contextmanager
        def remote_scylla_yaml(scylla_yaml=scylla_yamls[node.name]):
            yield scylla_yaml

        node.remote_scylla_yaml.side_effect = remote_scylla_yaml

    BaseScyllaCluster.update_seed_provider(cluster_of(BaseScyllaCluster, nodes))

    for node in nodes:
        assert scylla_yamls[node.name].seed_provider == node.proposed_scylla_yaml.seed_provider
