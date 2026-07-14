from unittest.mock import MagicMock

import pytest

from sdcm.cluster_docker import CassandraDockerCluster


@pytest.fixture
def cassandra_docker_cluster_factory(tmp_path, monkeypatch):
    monkeypatch.setenv("_SCT_TEST_LOGDIR", str(tmp_path))

    def factory(cassandra_num_tokens=16):
        params = MagicMock()
        params.get.side_effect = lambda key, default=None: {
            "cassandra_num_tokens": cassandra_num_tokens,
            "docker_network": "test-network",
            "user_prefix": "test",
        }.get(key, default)
        return CassandraDockerCluster(n_nodes=0, params=params, user_prefix="test")

    return factory


@pytest.fixture
def cassandra_docker_cluster(cassandra_docker_cluster_factory):
    return cassandra_docker_cluster_factory()


def test_single_node_env_vars(cassandra_docker_cluster):
    node = MagicMock()
    env = cassandra_docker_cluster.cassandra_env_vars(node, seed_ip=None)

    assert env["CASSANDRA_CLUSTER_NAME"] == cassandra_docker_cluster.name
    assert env["CASSANDRA_SEEDS"] == ""
    assert env["CASSANDRA_ENDPOINT_SNITCH"] == "SimpleSnitch"
    assert env["CASSANDRA_NUM_TOKENS"] == "16"
    assert env["MAX_HEAP_SIZE"] == "512M"
    assert env["HEAP_NEWSIZE"] == "64M"


def test_second_node_gets_seed_ip(cassandra_docker_cluster):
    node = MagicMock()
    env = cassandra_docker_cluster.cassandra_env_vars(node, seed_ip="172.17.0.2")

    assert env["CASSANDRA_SEEDS"] == "172.17.0.2"


def test_custom_num_tokens(cassandra_docker_cluster_factory):
    cassandra_docker_cluster = cassandra_docker_cluster_factory(cassandra_num_tokens=256)
    node = MagicMock()
    env = cassandra_docker_cluster.cassandra_env_vars(node, seed_ip=None)

    assert env["CASSANDRA_NUM_TOKENS"] == "256"


def test_num_tokens_defaults_to_16_when_none(cassandra_docker_cluster_factory):
    cassandra_docker_cluster = cassandra_docker_cluster_factory(cassandra_num_tokens=None)
    node = MagicMock()
    env = cassandra_docker_cluster.cassandra_env_vars(node, seed_ip=None)

    assert env["CASSANDRA_NUM_TOKENS"] == "16"
