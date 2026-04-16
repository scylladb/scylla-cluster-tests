from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster_docker import CassandraDockerCluster


@pytest.fixture
def mock_params():
    params = MagicMock()
    params.get.side_effect = lambda key, default=None: {
        "cassandra_num_tokens": 16,
        "docker_network": "test-network",
        "user_prefix": "test",
    }.get(key, default)
    return params


@pytest.fixture
def cassandra_docker_cluster(mock_params):
    with patch.object(CassandraDockerCluster, "__init__", lambda self, **kw: None):
        cluster = CassandraDockerCluster()
        cluster.params = mock_params
        cluster.name = "test-cs-cluster"
        return cluster


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


def test_custom_num_tokens(mock_params):
    mock_params.get.side_effect = lambda key, default=None: {
        "cassandra_num_tokens": 256,
        "docker_network": "test-network",
        "user_prefix": "test",
    }.get(key, default)

    with patch.object(CassandraDockerCluster, "__init__", lambda self, **kw: None):
        cluster = CassandraDockerCluster()
        cluster.params = mock_params
        cluster.name = "test-cs-cluster"

    node = MagicMock()
    env = cluster.cassandra_env_vars(node, seed_ip=None)

    assert env["CASSANDRA_NUM_TOKENS"] == "256"


def test_num_tokens_defaults_to_16_when_none(mock_params):
    mock_params.get.side_effect = lambda key, default=None: {
        "cassandra_num_tokens": None,
        "docker_network": "test-network",
        "user_prefix": "test",
    }.get(key, default)

    with patch.object(CassandraDockerCluster, "__init__", lambda self, **kw: None):
        cluster = CassandraDockerCluster()
        cluster.params = mock_params
        cluster.name = "test-cs-cluster"

    node = MagicMock()
    env = cluster.cassandra_env_vars(node, seed_ip=None)

    assert env["CASSANDRA_NUM_TOKENS"] == "16"
