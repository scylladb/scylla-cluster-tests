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
from pathlib import Path
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
        logdir: str | Path,
        racks_count: int = 0,
        node_prefix: str = "dummy-node",
        reuse_cluster: bool = False,
    ):
        self.params = params
        # ``logdir`` must be an isolated, per-test directory (e.g. pytest's
        # ``tmp_path``).  A shared world-writable location like ``/tmp`` lets
        # parallel tests collide and makes the inherited ``node_setup`` log
        # paths predictable/redirectable, so callers are required to pass one.
        self.logdir = str(logdir)
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

    def _create_node(self, node_index, container=None, after_config=None, rack=0):
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

    def __getattr__(self, name):
        return getattr(self._remote_docker, name)
