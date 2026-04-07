"""Integration tests for Docker simulated racks via entrypoint arguments.

External services: Docker (two Scylla containers)

Validates that passing --rack/--dc to the Docker entrypoint at container
creation time causes each node to report a distinct rack (RACK0, RACK1)
in system.local.  Uses Scylla >= 2026.1 images where the entrypoint
natively supports these arguments.

Also validates the REUSE_CLUSTER path: after initial rack setup, calling
node_setup with REUSE_CLUSTER=True must skip full setup while preserving
rack assignments.
"""

import pytest

from unit_tests.integration.conftest import configure_scylla_node
from unit_tests.lib.fake_docker_cluster import DummyDockerNode, DummyScyllaDockerCluster

pytestmark = [pytest.mark.integration]


# Scylla >= 2026.1 image that supports --rack/--dc entrypoint arguments.
_RACK_CAPABLE_IMAGE = "docker.io/scylladb/scylla-nightly:latest"


@pytest.fixture(scope="function")
def configure_racks(params, events):
    """Create a two-node cluster with racks configured via entrypoint args.

    Each container is started with --rack=RACKn --dc=datacenter1 passed to
    the Docker entrypoint, which writes cassandra-rackdc.properties and sets
    the snitch before the first Scylla boot.  No post-start reconfiguration
    or data wipe is needed.

    Uses native_entrypoint=True to bypass the SCT entry.sh which sets
    PasswordAuthenticator with ``authenticator_user``/``authenticator_password``
    scylla.yaml options that were removed in Scylla 2026.1.
    """
    # Clear auth params so run_cqlsh does not pass -u/-p credentials.
    # Use dict() to avoid Pydantic model_copy() since configure_scylla_node
    # only needs dict-like access.
    noauth_overrides = {"authenticator_user": "", "authenticator_password": ""}
    params_dict = dict(params)
    params_dict.update(noauth_overrides)

    scylla1 = configure_scylla_node(
        {"rack": 0, "image": _RACK_CAPABLE_IMAGE, "native_entrypoint": True},
        {**params_dict},
    )
    scylla2 = configure_scylla_node(
        {"rack": 1, "seeds": scylla1.ip_address, "image": _RACK_CAPABLE_IMAGE, "native_entrypoint": True},
        {**params_dict},
    )

    nodes = [
        DummyDockerNode(scylla1, rack=0, node_index=0),
        DummyDockerNode(scylla2, rack=1, node_index=1),
    ]

    yield nodes[0], nodes[1]

    scylla2.kill()
    scylla1.kill()


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

    # Racks must still be the same after the reuse node_setup (no restart happened)
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
