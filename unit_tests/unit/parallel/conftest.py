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
# Copyright (c) 2026 ScyllaDB

"""Fixtures for the tests of per-node work that runs in parallel."""

from functools import partial
from unittest.mock import MagicMock

import pytest

from sdcm.cluster import BaseCluster


@pytest.fixture
def nodes() -> list[MagicMock]:
    """Three stand-in nodes named node-0, node-1 and node-2."""
    named_nodes = []
    for index in range(3):
        node = MagicMock()
        node.name = f"node-{index}"  # MagicMock(name=...) names the mock's repr, not this attribute
        named_nodes.append(node)
    return named_nodes


@pytest.fixture
def cluster_of():
    """Build a stand-in for an instance of a cluster class, with the real `run_func_parallel`.

    The stand-in is a mock built from the class's spec, so the test calls the method it checks
    through the class, e.g. `BaseScyllaCluster.update_seed_provider(cluster)`.
    """

    def build(cluster_class: type, cluster_nodes=()) -> MagicMock:
        cluster = MagicMock(spec=cluster_class)
        cluster.log = MagicMock()
        cluster.nodes = list(cluster_nodes)
        cluster.run_func_parallel = partial(BaseCluster.run_func_parallel, cluster)
        return cluster

    return build
