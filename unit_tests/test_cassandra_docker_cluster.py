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


class TestCassandraEnvVars:
    @patch.object(CassandraDockerCluster, "__init__", lambda self, **kw: None)
    def test_single_node_env_vars(self, mock_params):
        cluster = CassandraDockerCluster()
        cluster.params = mock_params
        cluster.name = "test-cs-cluster"

        node = MagicMock()
        env = cluster.cassandra_env_vars(node, seed_ip=None)

        assert env["CASSANDRA_CLUSTER_NAME"] == cluster.name
        assert env["CASSANDRA_SEEDS"] == ""
        assert env["CASSANDRA_ENDPOINT_SNITCH"] == "SimpleSnitch"
        assert env["CASSANDRA_NUM_TOKENS"] == "16"
        assert env["MAX_HEAP_SIZE"] == "512M"
        assert env["HEAP_NEWSIZE"] == "64M"

    @patch.object(CassandraDockerCluster, "__init__", lambda self, **kw: None)
    def test_second_node_gets_seed_ip(self, mock_params):
        cluster = CassandraDockerCluster()
        cluster.params = mock_params
        cluster.name = "test-cs-cluster"

        node = MagicMock()
        env = cluster.cassandra_env_vars(node, seed_ip="172.17.0.2")

        assert env["CASSANDRA_SEEDS"] == "172.17.0.2"

    @patch.object(CassandraDockerCluster, "__init__", lambda self, **kw: None)
    def test_custom_num_tokens(self, mock_params):
        mock_params.get.side_effect = lambda key, default=None: {
            "cassandra_num_tokens": 256,
            "docker_network": "test-network",
            "user_prefix": "test",
        }.get(key, default)

        cluster = CassandraDockerCluster()
        cluster.params = mock_params
        cluster.name = "test-cs-cluster"

        node = MagicMock()
        env = cluster.cassandra_env_vars(node, seed_ip=None)

        assert env["CASSANDRA_NUM_TOKENS"] == "256"

    @patch.object(CassandraDockerCluster, "__init__", lambda self, **kw: None)
    def test_num_tokens_defaults_to_16_when_none(self, mock_params):
        mock_params.get.side_effect = lambda key, default=None: {
            "cassandra_num_tokens": None,
            "docker_network": "test-network",
            "user_prefix": "test",
        }.get(key, default)

        cluster = CassandraDockerCluster()
        cluster.params = mock_params
        cluster.name = "test-cs-cluster"

        node = MagicMock()
        env = cluster.cassandra_env_vars(node, seed_ip=None)

        assert env["CASSANDRA_NUM_TOKENS"] == "16"
