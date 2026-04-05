from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster_cassandra import BaseCassandraCluster, compute_jvm_heap_mb


@pytest.mark.parametrize(
    "total_ram_mb,expected_max,expected_new",
    [
        pytest.param(100, 256, 64, id="100MB-floor-at-256"),
        pytest.param(512, 256, 64, id="512MB-half-ram-hits-floor"),
        pytest.param(1024, 512, 128, id="1GB-half-ram"),
        pytest.param(2048, 1024, 256, id="2GB-half-ram"),
        pytest.param(4096, 2048, 512, id="4GB-half-ram"),
        pytest.param(16384, 8192, 800, id="16GB-half-ram"),
        pytest.param(32768, 16384, 800, id="32GB-half-ram"),
        pytest.param(65536, 32768, 800, id="64GB-half-ram"),
        pytest.param(128 * 1024, 65536, 800, id="128GB-half-ram"),
    ],
)
def test_compute_jvm_heap_sizes(total_ram_mb, expected_max, expected_new):
    max_heap, heap_new = compute_jvm_heap_mb(total_ram_mb)
    assert max_heap == expected_max
    assert heap_new == expected_new


def test_compute_jvm_heap_min_2gb_on_large_system():
    """Heap should be at least 2 GB when system has >= 4 GB RAM."""
    max_heap, _ = compute_jvm_heap_mb(4096)
    assert max_heap >= 2048


# --- BaseCassandraCluster tests ---


@pytest.fixture
def cassandra_cluster():
    """Create a minimal BaseCassandraCluster instance for testing."""
    with patch.object(BaseCassandraCluster, "__init__", lambda self, **kw: None):
        cluster = BaseCassandraCluster()
        cluster.params = MagicMock()
        cluster.name = "test-cs-cluster"
        cluster.nodes = []
        cluster.nemesis_termination_event = MagicMock()
        cluster.nemesis = []
        cluster.nemesis_threads = []
        cluster.nemesis_count = 0
        cluster.test_config = MagicMock()
        cluster._node_cycle = None
        cluster.vector_store_cluster = None
        cluster.parallel_node_operations = False
        cluster.log = MagicMock()
        return cluster


def test_parallel_startup_is_false(cassandra_cluster):
    assert cassandra_cluster.parallel_startup is False


def test_get_scylla_args_returns_empty(cassandra_cluster):
    assert cassandra_cluster.get_scylla_args() == ""


def test_update_seed_provider_is_noop(cassandra_cluster):
    cassandra_cluster.update_seed_provider()


def test_validate_seeds_is_noop(cassandra_cluster):
    cassandra_cluster.validate_seeds_on_all_nodes()


@pytest.mark.parametrize(
    "version,expected_jdk",
    [
        pytest.param("4.0", 11, id="cassandra-4.0"),
        pytest.param("4.1", 11, id="cassandra-4.1"),
        pytest.param("5.0", 17, id="cassandra-5.0"),
        pytest.param("5.1", 17, id="cassandra-5.1"),
    ],
)
def test_jdk_version(version, expected_jdk):
    with patch.object(BaseCassandraCluster, "__init__", lambda self, **kw: None):
        cluster = BaseCassandraCluster()
        cluster.params = MagicMock()
        cluster.params.get.side_effect = lambda key, default=None: {
            "cassandra_version": version,
        }.get(key, default)
        assert cluster.jdk_version() == expected_jdk


def test_cassandra_version_from_params(cassandra_cluster):
    cassandra_cluster.params.get.side_effect = lambda key, default=None: {
        "cassandra_version": "5.0",
    }.get(key, default)
    assert cassandra_cluster.cassandra_version == "5.0"


def test_cassandra_version_falls_back_to_oracle_version(cassandra_cluster):
    cassandra_cluster.params.get.side_effect = lambda key, default=None: {
        "cassandra_version": None,
        "cassandra_oracle_version": "4.1",
    }.get(key, default)
    assert cassandra_cluster.cassandra_version == "4.1"


def test_data_nodes_returns_all_nodes(cassandra_cluster):
    node1, node2 = MagicMock(), MagicMock()
    cassandra_cluster.nodes = [node1, node2]
    assert cassandra_cluster.data_nodes == [node1, node2]


def test_zero_nodes_returns_empty(cassandra_cluster):
    cassandra_cluster.nodes = [MagicMock(), MagicMock()]
    assert cassandra_cluster.zero_nodes == []
