"""Integration test for Docker simulated_racks snitch configuration.

Verifies that the rack reconfiguration sequence -- writing
cassandra-rackdc.properties, switching endpoint_snitch to
GossipingPropertyFileSnitch, wiping persisted topology data, and
restarting Scylla -- causes the node to report the expected rack via CQL.

Why this test mirrors node_setup steps instead of calling node_setup() directly:
  ScyllaDockerCluster.node_setup() (cluster_docker.py) requires infrastructure
  that the test fixture doesn't provide:
    - is_scylla_installed() -- checks package manager, not available in test image
    - check_aio_max_nr() -- needs /proc/sys access on the host
    - config_setup() -- runs ``scylla --help`` / ``scylla --help-seastar`` to
      discover valid args, then calls fix_scylla_server_systemd_config (systemd
      unavailable)
    - restart_scylla() -- uses DockerNode.restart_scylla_server() which calls
      ContainerManager; the test fixture uses RemoteDocker, not DockerNode
  The rackdc.properties write, clean_scylla_data(), and snitch switch are the
  rack-specific steps that CAN run on RemoteDocker -- and those are exactly
  what this test exercises.

External services: Docker (Scylla container)
"""

import logging

import pytest

from sdcm import wait
from sdcm.cluster import BaseNode
from unit_tests.lib.fake_cluster import DummyScyllaCluster

LOGGER = logging.getLogger(__name__)

GPFS = "org.apache.cassandra.locator.GossipingPropertyFileSnitch"
RACKDC_PROPS_PATH = "/etc/scylla/cassandra-rackdc.properties"


def _reconfigure_rack(node, rack_index):
    """Apply rack config to a Docker Scylla node, mirroring node_setup's rack path.

    Performs the three rack-specific steps from ScyllaDockerCluster.node_setup
    (cluster_docker.py) that are compatible with the RemoteDocker test fixture:
      1. Write cassandra-rackdc.properties with RACK{rack_index}
      2. Switch endpoint_snitch to GossipingPropertyFileSnitch in scylla.yaml
      3. Wipe persisted topology data via BaseNode.clean_scylla_data()
    """
    rack_name = f"RACK{rack_index}"

    # 1. Write rack properties -- same printf command used in node_setup
    node.remoter.sudo(
        f"bash -c 'printf \"dc=datacenter1\\nrack={rack_name}\\nprefer_local=true\\n\" > {RACKDC_PROPS_PATH}'"
    )

    # 2. Switch endpoint_snitch -- node_setup does this via config_setup(),
    #    which calls remote_scylla_yaml() internally. We call it directly.
    with node.remote_scylla_yaml() as scylla_yml:
        scylla_yml.endpoint_snitch = GPFS

    # 3. Wipe persisted data -- delegates to BaseNode.clean_scylla_data()
    #    (cluster.py), the same method node_setup calls.
    node.clean_scylla_data()


def _restart_and_wait(node, timeout=180):
    """Restart container from the host side and wait for CQL to come back.

    Cannot use DockerNode.restart_scylla_server() because the test fixture
    provides RemoteDocker (not DockerNode). RemoteDocker.node.remoter runs
    commands on the host; RemoteDocker.remoter runs them inside the container.
    """
    node.node.remoter.run(f"docker restart {node.docker_id}", timeout=120)

    # Invalidate the cached internal IP in case docker restart assigned a new one.
    # RemoteDocker.internal_ip_address caches on first access (docker_remote.py).
    node._internal_ip_address = None

    wait.wait_for(
        func=lambda: node.is_port_used(port=BaseNode.CQL_PORT, service_name="scylla-server"),
        step=2,
        text=f"Waiting for Scylla to restart on {node.docker_id[:12]}",
        timeout=timeout,
        throw_exc=True,
    )


@pytest.mark.integration
def test_simulated_rack_via_snitch_reconfiguration(docker_scylla, params):
    """Verify a single Docker Scylla node reports the configured rack after snitch switch.

    External services: Docker (Scylla container)
    """
    rack_index = 1
    expected_rack = f"RACK{rack_index}"

    params["simulated_racks"] = 2
    cluster = DummyScyllaCluster([docker_scylla])
    cluster.params = params

    _reconfigure_rack(docker_scylla, rack_index)
    _restart_and_wait(docker_scylla)

    with cluster.cql_connection_patient(docker_scylla) as session:
        row = session.execute("SELECT rack FROM system.local").one()
        assert row.rack == expected_rack, f"Expected rack '{expected_rack}', got '{row.rack}'"


@pytest.mark.integration
def test_two_node_cluster_rack_visibility(docker_scylla, docker_scylla_2, params):
    """Verify two Docker nodes in different racks see each other's rack via CQL.

    Validates cross-node rack propagation through gossip: each node's
    system.peers must show the other node's rack name.

    External services: Docker (2 Scylla containers)
    """
    params["simulated_racks"] = 2
    cluster = DummyScyllaCluster([docker_scylla, docker_scylla_2])
    cluster.params = params

    nodes = [docker_scylla, docker_scylla_2]

    # Reconfigure both nodes before restarting either, to avoid gossip issues
    # where a restarted node tries to reach a still-stale peer.
    for rack_index, node in enumerate(nodes):
        _reconfigure_rack(node, rack_index)

    for node in nodes:
        _restart_and_wait(node)

    # Verify each node reports its own rack in system.local
    # and sees the other node's rack in system.peers.
    for rack_index, node in enumerate(nodes):
        other_rack_index = 1 - rack_index
        expected_local_rack = f"RACK{rack_index}"
        expected_peer_rack = f"RACK{other_rack_index}"

        with cluster.cql_connection_patient(node) as session:
            local_row = session.execute("SELECT rack FROM system.local").one()
            assert local_row.rack == expected_local_rack, (
                f"Node {rack_index}: expected local rack '{expected_local_rack}', got '{local_row.rack}'"
            )

            peer_row = session.execute("SELECT rack FROM system.peers").one()
            assert peer_row.rack == expected_peer_rack, (
                f"Node {rack_index}: expected peer rack '{expected_peer_rack}', got '{peer_row.rack}'"
            )
