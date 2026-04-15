from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster_docker import CassandraDockerCluster, CassandraYamlAttrProxy


@pytest.fixture
def mock_params():
    params = MagicMock()
    params.get.side_effect = lambda key, default=None: {
        "cassandra_num_tokens": 16,
        "docker_network": "test-network",
        "user_prefix": "test",
    }.get(key, default)
    return params


@pytest.fixture(autouse=True)
def _patch_cluster_init():
    with patch.object(CassandraDockerCluster, "__init__", lambda self, **kw: None):
        yield


@pytest.fixture
def cassandra_cluster(mock_params):
    cluster = CassandraDockerCluster()
    cluster.params = mock_params
    cluster.name = "test-cs-cluster"
    return cluster


def test_single_node_env_vars(cassandra_cluster):
    node = MagicMock()
    env = cassandra_cluster.cassandra_env_vars(node, seed_ip=None)

    assert env["CASSANDRA_CLUSTER_NAME"] == cassandra_cluster.name
    assert env["CASSANDRA_SEEDS"] == ""
    assert env["CASSANDRA_ENDPOINT_SNITCH"] == "SimpleSnitch"
    assert env["CASSANDRA_NUM_TOKENS"] == "16"
    assert env["MAX_HEAP_SIZE"] == "512M"
    assert env["HEAP_NEWSIZE"] == "64M"


def test_second_node_gets_seed_ip(cassandra_cluster):
    node = MagicMock()
    env = cassandra_cluster.cassandra_env_vars(node, seed_ip="172.17.0.2")

    assert env["CASSANDRA_SEEDS"] == "172.17.0.2"


def test_custom_num_tokens(mock_params):
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


def test_num_tokens_defaults_to_16_when_none(mock_params):
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


def test_yaml_attr_proxy_getattr():
    data = {"broadcast_rpc_address": "10.0.0.1", "cluster_name": "test"}
    proxy = CassandraYamlAttrProxy(data)

    assert proxy.broadcast_rpc_address == "10.0.0.1"
    assert proxy.cluster_name == "test"
    assert proxy.nonexistent_key is None


def test_yaml_attr_proxy_setattr():
    data = {"cluster_name": "test"}
    proxy = CassandraYamlAttrProxy(data)

    proxy.broadcast_rpc_address = "10.0.0.2"
    assert proxy.broadcast_rpc_address == "10.0.0.2"
    assert data["broadcast_rpc_address"] == "10.0.0.2"

    proxy.cluster_name = "updated"
    assert data["cluster_name"] == "updated"


def test_yaml_attr_proxy_model_dump():
    data = {"key1": "val1", "key2": "val2"}
    proxy = CassandraYamlAttrProxy(data)

    dumped = proxy.model_dump()
    assert dumped == {"key1": "val1", "key2": "val2"}
    assert dumped is not data
