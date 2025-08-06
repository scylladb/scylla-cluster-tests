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

import uuid
import time
import random
import pytest
import logging

pytestmark = [
    pytest.mark.usefixtures("events"),
    pytest.mark.integration,
]

LOGGER = logging.getLogger(__name__)


class TestVectorStoreIntegration:

    @pytest.mark.docker_scylla_args(vector_store=True)
    def test_vector_store_deployment(self, docker_scylla, docker_vector_store, params):
        """Test vector store deployment, connecting to ScyllaDB and that VS endpoints are available"""
        db_cluster, vs_cluster = docker_scylla.parent_cluster, docker_vector_store

        assert db_cluster.nodes[0].is_running()
        assert len(vs_cluster.nodes) == 1
        assert vs_cluster.nodes[0].is_running()

        vector_client = vs_cluster.nodes[0].get_vector_store_api_client()

        # status, info and indexes endpoints
        assert vector_client.get_status() in ('SERVING', 'BOOTSTRAPPING')
        assert isinstance(vector_client.get_info(), dict)
        assert isinstance(vector_client.get_indexes(), list)

    @pytest.mark.docker_scylla_args(vector_store=True)
    def test_vector_search(self, docker_scylla, docker_vector_store, params):
        """Test of vector search"""
        db_cluster, vs_cluster = docker_scylla.parent_cluster, docker_vector_store

        self._create_vector_table(db_cluster)
        test_vectors = self._insert_test_vectors(db_cluster)

        vector_client = vs_cluster.nodes[0].get_vector_store_api_client()
        self._wait_for_vector_indexing(vector_client)
        self._test_vector_search_operations(vector_client, test_vectors)

    @pytest.mark.docker_scylla_args(vector_store=True)
    def test_vector_search_error_handling(self, docker_scylla, docker_vector_store, params):
        """Test error handling when performing vector search operations"""
        db_cluster, vs_cluster = docker_scylla.parent_cluster, docker_vector_store
        vector_client = vs_cluster.nodes[0].get_vector_store_api_client()

        self._create_vector_table(db_cluster)
        self._insert_test_vectors(db_cluster, count=10)
        self._wait_for_vector_indexing(vector_client)

        # too few dimensions
        with pytest.raises(Exception):
            vector_client.ann_search(
                keyspace='vector_test', index='embeddings_vector_idx', embedding=[0.1, 0.2], limit=5)

        # search on non-existent index
        test_vector = [random.uniform(-1.0, 1.0) for _ in range(128)]
        with pytest.raises(Exception):
            vector_client.ann_search(
                keyspace='nonexistent', index='nonexistent_idx', embedding=test_vector, limit=5)

    @staticmethod
    def _create_vector_table(db_cluster: 'LocalScyllaClusterDummy') -> None:  # noqa: F821
        """Create vector table and index"""
        node = db_cluster.nodes[0]
        with db_cluster.cql_connection_patient(node) as session:
            session.execute("CREATE KEYSPACE IF NOT EXISTS vector_test "
                            "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
            session.execute("CREATE TABLE IF NOT EXISTS vector_test.embeddings "
                            "(id UUID PRIMARY KEY, "
                            "vector VECTOR<FLOAT, 5>, "
                            "metadata text) "
                            "WITH cdc = {'enabled': true}")
            session.execute("CREATE CUSTOM INDEX IF NOT EXISTS embeddings_vector_idx "
                            "ON vector_test.embeddings (vector) USING 'vector_index'")

    @staticmethod
    def _insert_test_vectors(db_cluster: 'LocalScyllaClusterDummy', count: int = 100, vector_dim: int = 5) -> list:  # noqa: F821
        """Insert test data with vector embeddings"""
        test_vectors = []
        with db_cluster.cql_connection_patient(db_cluster.nodes[0]) as session:
            insert_stmt = session.prepare(
                "INSERT INTO vector_test.embeddings (id, vector, metadata) VALUES (?, ?, ?)")

            for i in range(count):
                vector = [random.uniform(-1.0, 1.0) for _ in range(vector_dim)]
                # normalize vector
                magnitude = sum(x**2 for x in vector) ** 0.5
                if magnitude > 0:
                    vector = [x/magnitude for x in vector]

                vector_id = uuid.uuid4()
                metadata = f"test_item_{i}"

                session.execute(insert_stmt, (vector_id, vector, metadata))
                test_vectors.append({'id': vector_id, 'vector': vector, 'metadata': metadata})

        return test_vectors

    @staticmethod
    def _wait_for_vector_indexing(vector_client: 'VectorStoreClient', timeout: int = 300) -> None:  # noqa: F821
        """Wait for vector store to discover and index the new data"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            indexes = vector_client.get_indexes()
            vector_test_indexes = [idx for idx in indexes
                                   if idx.get('keyspace') == 'vector_test'
                                   and idx.get('index') == 'embeddings_vector_idx']
            if vector_test_indexes:
                try:
                    if vector_client.get_index_count('vector_test', 'embeddings_vector_idx') > 0:
                        return
                except Exception as e:  # noqa: BLE001
                    LOGGER.debug(f"Index not ready yet: {e}")
            time.sleep(5)

        raise RuntimeError(f"Vector indexing did not complete within {timeout} seconds")

    @staticmethod
    def _test_vector_search_operations(vector_client: 'VectorStoreClient', test_vectors: list) -> None:  # noqa: F821
        """Test vector search operations"""
        # basic ANN search
        query_vector = test_vectors[0]['vector']
        search_results = vector_client.ann_search(
            keyspace='vector_test', index='embeddings_vector_idx', embedding=query_vector, limit=10)
        primary_keys = search_results.get('primary_keys', {}).get('id', [])

        assert len(primary_keys) > 0, "No search results returned"
        assert 'distances' in search_results, "No distances in search results"
        closest_distance = search_results['distances'][0]
        assert closest_distance < 0.001, f"Self-search distance too high: {closest_distance}. Expected near 0."

        # search with limits
        for limit in [1, 5, 20]:
            results = vector_client.ann_search(
                keyspace='vector_test', index='embeddings_vector_idx', embedding=query_vector, limit=limit)
            primary_keys = results.get('primary_keys', {}).get('id', [])
            actual_count = len(primary_keys)
            expected_count = min(limit, len(test_vectors))
            assert actual_count == expected_count, f"Expected {expected_count} results, got {actual_count}"

        # index count
        count = vector_client.get_index_count('vector_test', 'embeddings_vector_idx')
        assert count == len(test_vectors), f"Expected {len(test_vectors)} vectors, got {count}"
