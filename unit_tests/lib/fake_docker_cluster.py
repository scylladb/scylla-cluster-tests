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

"""Lightweight ScyllaDockerCluster test double for unit and integration tests.

Provides a thin subclass that bypasses the heavy DockerCluster / BaseCluster
constructor chain while keeping real method implementations like ``node_setup``,
``_create_nodes``, and ``_get_nodes``.
"""

import logging
from types import SimpleNamespace

from sdcm.cluster_docker import ScyllaDockerCluster


class DummyScyllaDockerCluster(ScyllaDockerCluster):
    """Minimal ScyllaDockerCluster stub for testing.

    Bypasses the full ``DockerCluster`` / ``BaseCluster`` ``__init__`` chain
    and wires only the attributes that tested methods actually read.

    Supports:
    - ``node_setup`` (needs ``params``, ``logdir``, ``test_config``)
    - ``_create_nodes`` / ``_get_nodes`` (need ``racks_count``, ``nodes``,
      ``node_prefix``)
    """

    # noinspection PyMissingConstructor
    def __init__(
        self,
        params: dict,
        logdir: str = "/tmp",
        racks_count: int = 0,
        node_prefix: str = "dummy-node",
        reuse_cluster: bool = False,
    ):
        self.params = params
        self.logdir = logdir
        self.log = logging.getLogger(self.__class__.__name__)
        self.name = "dummy-scylla-docker-cluster"
        self.vector_store_cluster = None
        self.racks_count = racks_count
        self.nodes = []
        self.node_prefix = node_prefix
        # Set as instance attribute to shadow the cached_property descriptor,
        # allowing per-instance configuration of REUSE_CLUSTER for tests.
        self.test_config = SimpleNamespace(BACKTRACE_DECODING=False, REUSE_CLUSTER=reuse_cluster)

    @staticmethod
    def check_aio_max_nr(node, recommended_value=0):
        pass

    def _generate_db_node_certs(self, node):
        pass

    def get_scylla_args(self):
        return ""

    def _create_node(self, node_index, container=None, rack=0):
        return SimpleNamespace(node_index=node_index, rack=rack, enable_auto_bootstrap=False)


class DummyDockerNode:
    """Base wrapper around a ``RemoteDocker`` for use in integration tests.

    The ``docker_scylla`` fixture yields ``RemoteDocker`` instances, not
    ``DockerNode``.  This base class provides ``__init__`` and attribute
    delegation via ``__getattr__``.  Subclasses override specific methods
    that ``node_setup`` (or other cluster methods) call on the node.
    """

    def __init__(self, remote_docker, rack=0, node_index=0):
        self._remote_docker = remote_docker
        self.rack = rack
        self.node_index = node_index
        # Stub for the cached_property on BaseNode; the REUSE_CLUSTER path
        # in node_setup accesses node.raft to warm it up.
        self.raft = None

    def __getattr__(self, name):
        return getattr(self._remote_docker, name)


class RackAwareDummyDockerNode(DummyDockerNode):
    """``DummyDockerNode`` with methods needed by rack-aware ``node_setup``.

    Provides stub implementations of ``is_scylla_installed``,
    ``config_setup``, ``clean_scylla_data``, and ``restart_scylla``
    that operate on the underlying ``RemoteDocker`` container.
    """

    def is_scylla_installed(self, raise_if_not_installed=False):
        return True

    def config_setup(self, **kwargs):
        self._remote_docker.remoter.sudo(
            'sed -i "s/endpoint_snitch:.*/endpoint_snitch: GossipingPropertyFileSnitch/" /etc/scylla/scylla.yaml'
        )

    def clean_scylla_data(self):
        for cmd in [
            "rm -rf /var/lib/scylla/data/*",
            "find /var/lib/scylla/commitlog -type f -delete",
            "find /var/lib/scylla/hints -type f -delete",
            "find /var/lib/scylla/view_hints -type f -delete",
        ]:
            self._remote_docker.remoter.sudo(cmd, ignore_status=True)

    def restart_scylla(self, verify_up_before=True):
        rd = self._remote_docker
        rd.node.remoter.run(f"{rd.sudo_needed}docker restart {rd.docker_id}", timeout=60)
