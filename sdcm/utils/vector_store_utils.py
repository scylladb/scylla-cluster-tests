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
from functools import cached_property

from sdcm.reporting.tooling_reporter import VectorStoreVersionReporter
from sdcm.utils.vector_store_client import VectorStoreClient

LOGGER = logging.getLogger(__name__)

# Fallbacks for a source build. Applied here rather than as schema defaults in
# 'defaults/test_default.yaml' on purpose: source mode is inferred from either
# 'vector_store_source_repo' or 'vector_store_source_ref' being set, so both have to stay
# empty by default for that check to work. Giving 'vector_store_source_repo' a non-empty
# schema default would make every run look like a source build.
DEFAULT_VECTOR_STORE_SOURCE_REPO = "https://github.com/scylladb/vector-store.git"
DEFAULT_VECTOR_STORE_SOURCE_REF = "master"


def is_vector_store_source_build(params) -> bool:
    """True if vector-store should be built from source instead of taken from a prebuilt AMI.

    Deliberately checks *both* params: a repo on its own is a complete request ("build this
    fork's default branch"), so keying off the ref alone would silently ignore it and
    provision from the AMI instead.
    """
    return bool(params.get("vector_store_source_repo") or params.get("vector_store_source_ref"))


def resolve_vector_store_source(params) -> tuple[str, str]:
    """Return the (repo, ref) to build, applying the defaults for whichever half was omitted."""
    return (
        params.get("vector_store_source_repo") or DEFAULT_VECTOR_STORE_SOURCE_REPO,
        params.get("vector_store_source_ref") or DEFAULT_VECTOR_STORE_SOURCE_REF,
    )


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

    def get_vector_store_source_build_info(self) -> tuple[str, str]:
        """Return the '(sha, ref)' vector-store on this node was built from, or ('', '').

        Only the aws backend can build from source (see is_vector_store_source_build); the other
        backends always run a prebuilt image, so they have nothing to report.
        """
        return "", ""

    @cached_property
    def vector_store_uri(self) -> str:
        """Get Vector Store URI"""
        ip = self.public_ip_address if self.test_config.IP_SSH_CONNECTIONS == "public" else self.private_ip_address
        return f"http://{ip}:{self.parent_cluster.params.get('vector_store_port')}"

    @property
    def scylla_uri(self) -> str:
        """Get Scylla URI"""
        scylla_uri = "127.0.0.1:9042"
        if self.parent_cluster.scylla_cluster:
            scylla_uri = (
                f"{self.parent_cluster.scylla_cluster.nodes[0].ip_address}:"
                f"{self.parent_cluster.params.get('vector_store_scylla_port')}"
            )
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

        vector_store_primary_uri = vector_uris[0]
        self.log.debug("Configuring Scylla nodes with vector_store_primary_uri: %s", vector_store_primary_uri)

        for node in self.scylla_cluster.nodes:
            with node.remote_scylla_yaml() as scylla_yml:
                scylla_yml.vector_store_primary_uri = vector_store_primary_uri
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
        self._report_vector_store_version()

    def _report_vector_store_version(self) -> None:
        """Submit Vector Store version to Argus."""
        if not self.nodes:
            return
        node = self.nodes[0]
        try:
            # Reading the build info runs a command on the node, so it belongs inside the guard:
            # reporting the version is best-effort and must never fail cluster init.
            source_repo, source_sha, source_ref = "", "", ""
            if is_vector_store_source_build(self.params):
                source_repo, _ = resolve_vector_store_source(self.params)
                source_sha, source_ref = node.get_vector_store_source_build_info()
            VectorStoreVersionReporter(
                node.get_vector_store_api_client(),
                self.test_config.argus_client(),
                source_repo=source_repo,
                source_sha=source_sha,
                source_ref=source_ref,
            ).report()
        except Exception:  # noqa: BLE001
            LOGGER.warning("Error submitting vector store version, VS package won't show in Argus.", exc_info=True)
