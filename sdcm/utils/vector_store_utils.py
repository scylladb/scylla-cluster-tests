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

from functools import cached_property

from sdcm.utils.vector_store_client import VectorStoreClient


class VectorStoreNodeMixin:
    """Mixin class providing common Vector Store node functionality."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._vector_store_client = None

    def wait_for_vector_store_ready(self, timeout: int = 300) -> bool:
        """Wait for Vector Store service to be ready"""
        try:
            return self.get_vector_store_api_client().wait_for_ready(timeout=timeout)
        except Exception as e:  # noqa: BLE001
            self.log.error("Failed to wait for Vector Store ready: %s", e)
            return False

    def get_vector_store_api_client(self) -> VectorStoreClient:
        """Get Vector Store API client"""
        if self._vector_store_client is None:
            self._vector_store_client = VectorStoreClient(self.vector_store_uri)
        return self._vector_store_client

    @cached_property
    def vector_store_uri(self) -> str:
        """Get Vector Store URI"""
        ip = self.public_ip_address if self.test_config.IP_SSH_CONNECTIONS == 'public' else self.private_ip_address
        return f"http://{ip}:{self.parent_cluster.params.get('vector_store_port')}"

    @property
    def scylla_uri(self) -> str:
        """Get Scylla URI"""
        scylla_uri = "127.0.0.1:9042"
        if self.parent_cluster.scylla_cluster:
            scylla_uri = (f"{self.parent_cluster.scylla_cluster.nodes[0].ip_address}:"
                          f"{self.parent_cluster.params.get('vector_store_scylla_port')}")
        return scylla_uri


class VectorStoreClusterMixin:
    """Mixin class providing common Vector Store cluster functionality."""

    def configure_with_scylla_cluster(self, scylla_cluster) -> None:
        """
        Configure Vector Store cluster to work with the given Scylla cluster.

        This should be called after both clusters are created.
        """
        if not scylla_cluster or not scylla_cluster.nodes:
            self.log.warning("No Scylla cluster nodes provided for Vector Store configuration")
            return

        self.scylla_cluster = scylla_cluster
        self._reconfigure_vector_store_nodes()
        self._configure_scylla_nodes_with_vector_store()

    def _configure_scylla_nodes_with_vector_store(self):
        """Configure Scylla nodes with Vector Store URIs"""
        if not (self.scylla_cluster and (vector_uris := self.get_vector_store_uris())):
            return

        vector_store_uri = vector_uris[0]
        self.log.debug("Configuring Scylla nodes with vector_store_uri: %s", vector_store_uri)

        for node in self.scylla_cluster.nodes:
            with node.remote_scylla_yaml() as scylla_yml:
                scylla_yml.vector_store_uri = vector_store_uri
            node.reload_config()

    def get_vector_store_uris(self) -> list[str]:
        """Get list of Vector Store URIs"""
        return [node.vector_store_uri for node in self.nodes]

    def wait_for_init(self, timeout: int = 300):
        """Wait for all Vector Store nodes to be ready"""
        self.log.info("Waiting for Vector Store nodes to be ready")
        for node in self.nodes:
            if not node.wait_for_vector_store_ready(timeout=timeout):
                raise RuntimeError(f"Vector Store node {node.name} failed to become ready within {timeout} seconds")
        self.log.info("All Vector Store nodes are ready")
