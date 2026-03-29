from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster_cassandra import BaseCassandraCluster, compute_jvm_heap_mb


class TestComputeJvmHeapMb:
    @pytest.mark.parametrize(
        "total_ram_mb,expected_max,expected_new",
        [
            pytest.param(512, 256, 64, id="512MB-min-heap"),
            pytest.param(1024, 512, 128, id="1GB-half-ram"),
            pytest.param(2048, 512, 128, id="2GB-quarter-ram"),
            pytest.param(4096, 1024, 256, id="4GB-quarter"),
            pytest.param(16384, 4096, 800, id="16GB-cap-new"),
            pytest.param(32768, 8192, 800, id="32GB-max-heap"),
            pytest.param(65536, 8192, 800, id="64GB-capped-at-8G"),
            pytest.param(128 * 1024, 8192, 800, id="128GB-still-capped"),
            pytest.param(100, 256, 64, id="100MB-floor-at-256"),
        ],
    )
    def test_heap_sizes(self, total_ram_mb, expected_max, expected_new):
        max_heap, heap_new = compute_jvm_heap_mb(total_ram_mb)
        assert max_heap == expected_max
        assert heap_new == expected_new

    def test_compressed_oops_cap(self):
        """Heap must never exceed 31 GB (compressed oops threshold)."""
        max_heap, _ = compute_jvm_heap_mb(200 * 1024)
        assert max_heap <= 31 * 1024


class TestBaseCassandraCluster:
    def _make_cluster(self, params=None):
        """Create a minimal BaseCassandraCluster instance for testing."""
        with patch.object(BaseCassandraCluster, "__init__", lambda self, **kw: None):
            cluster = BaseCassandraCluster()
            cluster.params = params or MagicMock()
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

    def test_parallel_startup_is_false(self):
        cluster = self._make_cluster()
        assert cluster.parallel_startup is False

    def test_get_scylla_args_returns_empty(self):
        cluster = self._make_cluster()
        assert cluster.get_scylla_args() == ""

    def test_update_seed_provider_is_noop(self):
        cluster = self._make_cluster()
        cluster.update_seed_provider()

    def test_validate_seeds_is_noop(self):
        cluster = self._make_cluster()
        cluster.validate_seeds_on_all_nodes()

    @pytest.mark.parametrize(
        "version,expected_jdk",
        [
            pytest.param("4.0", 11, id="cassandra-4.0"),
            pytest.param("4.1", 11, id="cassandra-4.1"),
            pytest.param("5.0", 17, id="cassandra-5.0"),
            pytest.param("5.1", 17, id="cassandra-5.1"),
        ],
    )
    def test_jdk_version(self, version, expected_jdk):
        params = MagicMock()
        params.get.side_effect = lambda key, default=None: {
            "cassandra_version": version,
        }.get(key, default)
        cluster = self._make_cluster(params)
        assert cluster.jdk_version() == expected_jdk

    def test_cassandra_version_from_params(self):
        params = MagicMock()
        params.get.side_effect = lambda key, default=None: {
            "cassandra_version": "5.0",
        }.get(key, default)
        cluster = self._make_cluster(params)
        assert cluster.cassandra_version == "5.0"

    def test_cassandra_version_falls_back_to_oracle_version(self):
        params = MagicMock()
        params.get.side_effect = lambda key, default=None: {
            "cassandra_version": None,
            "cassandra_oracle_version": "4.1",
        }.get(key, default)
        cluster = self._make_cluster(params)
        assert cluster.cassandra_version == "4.1"

    def test_data_nodes_returns_all_nodes(self):
        cluster = self._make_cluster()
        node1, node2 = MagicMock(), MagicMock()
        cluster.nodes = [node1, node2]
        assert cluster.data_nodes == [node1, node2]

    def test_zero_nodes_returns_empty(self):
        cluster = self._make_cluster()
        cluster.nodes = [MagicMock(), MagicMock()]
        assert cluster.zero_nodes == []
