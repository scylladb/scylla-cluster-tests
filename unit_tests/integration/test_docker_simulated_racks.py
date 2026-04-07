"""Integration tests for Docker simulated racks via ScyllaDockerCluster.node_setup.

External services: Docker (two Scylla containers)

Validates that ScyllaDockerCluster.node_setup correctly configures
cassandra-rackdc.properties and restarts nodes so that each node
reports a distinct rack (RACK0, RACK1) in system.local.

Also validates the REUSE_CLUSTER path: after initial rack setup, calling
node_setup with REUSE_CLUSTER=True must skip full setup while preserving
rack assignments.
"""

import logging

import pytest

from sdcm import wait
from sdcm.cluster import BaseNode
from unit_tests.lib.fake_docker_cluster import DummyScyllaDockerCluster, RackAwareDummyDockerNode

pytestmark = [pytest.mark.integration]


def _wait_for_cql(node, timeout=120):
    """Block until CQL port is accepting connections on *node*."""
    # Clear cached IP so the next lookup resolves the (potentially new)
    # address after a container restart.
    node._internal_ip_address = None

    def _cql_up():
        try:
            return node.is_port_used(port=BaseNode.CQL_PORT, service_name="scylla-server")
        except Exception:  # noqa: BLE001
            logging.error("CQL not up yet on %s", node.docker_id, exc_info=True)
            return False

    wait.wait_for(
        func=_cql_up,
        step=2,
        text=f"Waiting for CQL on {node.docker_id}",
        timeout=timeout,
        throw_exc=True,
    )


@pytest.fixture(scope="function")
def configure_racks(docker_scylla, docker_scylla_2, events):
    """Set up a two-node cluster with simulated racks via node_setup.

    Creates a minimal DummyScyllaDockerCluster, wraps each RemoteDocker
    in a DummyDockerNode, and calls the real ``node_setup`` which writes
    cassandra-rackdc.properties, wipes stale topology data, and
    restarts the containers.
    """
    cluster = DummyScyllaDockerCluster(
        params={"simulated_racks": 2, "docker_image": "scylladb/scylla:latest"},
    )

    raw_nodes = [docker_scylla, docker_scylla_2]
    nodes = [RackAwareDummyDockerNode(rd, rack=idx, node_index=idx) for idx, rd in enumerate(raw_nodes)]

    for node in nodes:
        cluster.node_setup(node)

    for node in nodes:
        _wait_for_cql(node)

    yield nodes[0], nodes[1]


@pytest.mark.xdist_group("docker-racks")
def test_rack_visibility(configure_racks):
    """Each node must report its assigned rack in system.local."""
    node1, node2 = configure_racks
    result = node1.run_cqlsh("SELECT rack FROM system.local")
    assert "RACK0" in result.stdout

    result = node2.run_cqlsh("SELECT rack FROM system.local")
    assert "RACK1" in result.stdout


@pytest.mark.xdist_group("docker-racks")
def test_keyspace_creation_with_rf2(configure_racks):
    """A NetworkTopologyStrategy keyspace with RF=2 must be creatable across racks."""
    node1, _ = configure_racks
    create_ks = (
        "CREATE KEYSPACE IF NOT EXISTS rack_test_ks "
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2}"
    )
    result = node1.run_cqlsh(create_ks)
    assert result.ok, f"Failed to create keyspace: {result.stderr}"


@pytest.mark.xdist_group("docker-racks")
def test_reuse_cluster_preserves_racks(configure_racks):
    """After initial rack setup, node_setup with REUSE_CLUSTER=True must
    skip full setup (no restart, no config_setup) while preserving the
    rack assignments that were written during the initial setup.
    """
    node1, node2 = configure_racks

    # Verify racks are correct before the reuse run
    result = node1.run_cqlsh("SELECT rack FROM system.local")
    assert "RACK0" in result.stdout

    result = node2.run_cqlsh("SELECT rack FROM system.local")
    assert "RACK1" in result.stdout

    # Simulate a REUSE_CLUSTER run: create a new cluster object with
    # reuse_cluster=True and call node_setup on the same nodes.
    reuse_cluster = DummyScyllaDockerCluster(
        params={"simulated_racks": 2, "docker_image": "scylladb/scylla:latest"},
        reuse_cluster=True,
    )

    reuse_cluster.node_setup(node1)
    reuse_cluster.node_setup(node2)

    # CQL should still be up (no restart happened)
    _wait_for_cql(node1, timeout=30)
    _wait_for_cql(node2, timeout=30)

    # Racks must still be the same after the reuse node_setup
    result = node1.run_cqlsh("SELECT rack FROM system.local")
    assert "RACK0" in result.stdout, f"Rack changed after reuse node_setup: {result.stdout}"

    result = node2.run_cqlsh("SELECT rack FROM system.local")
    assert "RACK1" in result.stdout, f"Rack changed after reuse node_setup: {result.stdout}"

    # The keyspace created by a previous test (or create it now) must still work
    create_ks = (
        "CREATE KEYSPACE IF NOT EXISTS reuse_rack_test_ks "
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2}"
    )
    result = node1.run_cqlsh(create_ks)
    assert result.ok, f"Failed to create keyspace after reuse: {result.stderr}"
