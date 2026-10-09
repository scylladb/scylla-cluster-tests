import logging

import pytest

from sdcm.test_config import TestConfig
from unit_tests.lib.fake_cluster import DummyDbCluster


log = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.parametrize(
    "encrypted",
    [
        pytest.param(True, marks=pytest.mark.docker_scylla_args(ssl=True), id="encrypted"),
        pytest.param(False, marks=pytest.mark.docker_scylla_args(ssl=False), id="clear"),
    ],
)
def test_02_test_python_driver(docker_scylla, params, encrypted):
    params["client_encrypt"] = encrypted
    node = docker_scylla
    db_cluster = DummyDbCluster(nodes=[node], params=params)
    node.parent_cluster = db_cluster

    for func in [db_cluster.cql_connection_patient, db_cluster.cql_connection_patient_exclusive]:
        with func(node) as session:
            for host in session.cluster.metadata.all_hosts():
                log.debug(host)
            res = session.execute("SELECT * FROM system.local")
            output = res.all()
            log.debug(output)
            assert len(output) == 1


@pytest.mark.integration
@pytest.mark.parametrize("ip_ssh_connections", ["private", "public"])
def test_session_with_keyspace(docker_scylla, params, monkeypatch, ip_ssh_connections):
    """Open a session bound to a keyspace in pooled and control-connection fallback modes.

    External services: Docker (Scylla container)
    """
    monkeypatch.setattr(TestConfig, "IP_SSH_CONNECTIONS", ip_ssh_connections)
    node = docker_scylla
    db_cluster = DummyDbCluster(nodes=[node], params=params)
    node.parent_cluster = db_cluster

    with db_cluster.cql_connection(node, keyspace="system") as session:
        assert session.keyspace == "system"
        assert len(session.execute("SELECT key FROM local").all()) == 1


@pytest.mark.integration
@pytest.mark.parametrize("ip_ssh_connections", ["private", "public"])
def test_session_with_new_keyspace(docker_scylla, params, monkeypatch, ip_ssh_connections):
    """Create a keyspace in one session and use it from a second session bound to it.

    External services: Docker (Scylla container)
    """
    monkeypatch.setattr(TestConfig, "IP_SSH_CONNECTIONS", ip_ssh_connections)
    node = docker_scylla
    db_cluster = DummyDbCluster(nodes=[node], params=params)
    node.parent_cluster = db_cluster

    with db_cluster.cql_connection(node) as session:
        session.execute(
            "CREATE KEYSPACE IF NOT EXISTS new_ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
        )

    with db_cluster.cql_connection(node, keyspace="new_ks") as session:
        session.execute("CREATE TABLE IF NOT EXISTS t (k int PRIMARY KEY, v int)")
        session.execute("INSERT INTO t (k, v) VALUES (1, 1)")
        assert session.execute("SELECT v FROM t WHERE k = 1").one().v == 1
