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

pytestmark = [
    pytest.mark.integration,
    pytest.mark.xdist_group("docker_heavy"),
]


# Rolling tag of the 2026.1 branch -- the first release whose Docker entrypoint
# accepts --rack/--dc.  The patch level is irrelevant to what this test asserts,
# so track the branch instead of pinning a single release.
_RACK_CAPABLE_IMAGE = "docker.io/scylladb/scylla:2026.1"


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

    containers = []
    try:
        containers.append(
            configure_scylla_node(
                {"rack": 0, "image": _RACK_CAPABLE_IMAGE, "native_entrypoint": True},
                {**params_dict},
            )
        )
        containers.append(
            configure_scylla_node(
                {"rack": 1, "seeds": containers[0].ip_address, "image": _RACK_CAPABLE_IMAGE, "native_entrypoint": True},
                {**params_dict},
            )
        )
        yield tuple(DummyDockerNode(container, rack=rack, node_index=rack) for rack, container in enumerate(containers))
    finally:
        # Kill whatever came up: collecting the containers as they start means the first one is
        # not leaked when the second fails, which would happen if teardown waited for `yield`.
        for container in reversed(containers):
            container.kill()


def test_rack_visibility(configure_racks):
    """Each node must report its assigned rack in system.local."""
    node1, node2 = configure_racks
    result = node1.run_cqlsh("SELECT rack FROM system.local")
    assert "RACK0" in result.stdout

    result = node2.run_cqlsh("SELECT rack FROM system.local")
    assert "RACK1" in result.stdout


def test_keyspace_creation_with_rf2(configure_racks):
    """A NetworkTopologyStrategy keyspace with RF=2 must be creatable across racks."""
    node1, _ = configure_racks
    create_ks = (
        "CREATE KEYSPACE IF NOT EXISTS rack_test_ks "
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2}"
    )
    result = node1.run_cqlsh(create_ks)
    assert result.ok, f"Failed to create keyspace: {result.stderr}"


def test_reuse_cluster_preserves_racks(tmp_path, configure_racks):
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
        params={"simulated_racks": 2, "docker_image": _RACK_CAPABLE_IMAGE},
        logdir=tmp_path,
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
