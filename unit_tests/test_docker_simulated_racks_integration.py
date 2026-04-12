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

import logging
import subprocess

import pytest

from sdcm import wait
from sdcm.cluster import BaseNode

LOGGER = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.integration,
]

RACK_ASSIGNMENTS = {
    "docker_scylla": "RACK0",
    "docker_scylla_2": "RACK1",
}


def _restart_and_wait(node):
    """Restart a Docker container and wait for the CQL port to come back up.

    Args:
        node: A RemoteDocker node whose underlying container should be restarted.
    """
    LOGGER.info("Restarting container %s", node.docker_id)
    subprocess.run(["docker", "restart", node.docker_id], check=True, timeout=60)

    # Clear the cached internal IP -- it may change after a restart.
    node._internal_ip_address = None  # noqa: SLF001

    def cql_port_ready():
        try:
            return node.is_port_used(port=BaseNode.CQL_PORT, service_name="scylla-server")
        except Exception:  # noqa: BLE001
            return False

    wait.wait_for(func=cql_port_ready, step=2, text=f"Waiting for CQL on {node.docker_id}", timeout=120, throw_exc=True)


@pytest.fixture(scope="function")
def configure_racks(docker_scylla, docker_scylla_2):
    """Configure two Docker Scylla nodes with distinct rack assignments.

    Writes ``cassandra-rackdc.properties``, sets the endpoint snitch to
    ``GossipingPropertyFileSnitch`` in ``scylla.yaml``, wipes persisted
    system data, and restarts both containers so the new topology takes
    effect.

    Yields:
        tuple: ``(docker_scylla, docker_scylla_2)`` once both nodes are
        accepting CQL connections with the new rack configuration.
    """
    nodes = {"docker_scylla": docker_scylla, "docker_scylla_2": docker_scylla_2}

    for label, node in nodes.items():
        rack = RACK_ASSIGNMENTS[label]

        # 1. Write rack/dc properties.
        node.remoter.sudo(
            f'bash -c \'printf "dc=datacenter1\\nrack={rack}\\nprefer_local=true\\n"'
            " > /etc/scylla/cassandra-rackdc.properties'"
        )

        # 2. Switch snitch in scylla.yaml.
        node.remoter.sudo(
            "sed -i 's/endpoint_snitch:.*/endpoint_snitch: GossipingPropertyFileSnitch/' /etc/scylla/scylla.yaml"
        )

        # 3. Wipe persisted system data so the node picks up the new topology.
        node.remoter.sudo("rm -rf /var/lib/scylla/data/system/*")

    # 4. Restart both containers and wait for CQL.
    for node in nodes.values():
        _restart_and_wait(node)

    yield docker_scylla, docker_scylla_2


@pytest.mark.xdist_group("docker-racks")
def test_rack_visibility(configure_racks):
    """Verify each node reports its configured rack via ``system.local``."""
    node1, node2 = configure_racks

    res1 = node1.run_cqlsh("SELECT rack FROM system.local")
    assert res1.ok, f"CQL query on node1 failed: {res1.stdout}"
    assert "RACK0" in res1.stdout, f"Expected RACK0 in output, got: {res1.stdout}"

    res2 = node2.run_cqlsh("SELECT rack FROM system.local")
    assert res2.ok, f"CQL query on node2 failed: {res2.stdout}"
    assert "RACK1" in res2.stdout, f"Expected RACK1 in output, got: {res2.stdout}"


@pytest.mark.xdist_group("docker-racks")
def test_keyspace_creation_with_rf2(configure_racks):
    """Create a keyspace using ``NetworkTopologyStrategy`` with RF=2 across both racks."""
    node1, _ = configure_racks

    res = node1.run_cqlsh(
        "CREATE KEYSPACE IF NOT EXISTS test_racks "
        "WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 2}"
    )
    assert res.ok, f"Keyspace creation failed: {res.stdout}"
