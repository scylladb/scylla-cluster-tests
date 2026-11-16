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

"""`wait_for_init_wrap` timeout handling.

Every caller passes `timeout` in seconds; the wrapper used to compare it against minutes,
so a 40-minute timeout became 40 hours and `NodeSetupTimeout` never fired.
"""

import logging
import threading
import time
from unittest.mock import MagicMock

import pytest

from sdcm.cluster import NodeSetupTimeout, wait_for_init_wrap

NODE_SETUP_BLOCK_SEC = 20


class HangingSetupCluster:
    """Minimal cluster whose node setup hangs until released or `NODE_SETUP_BLOCK_SEC` passes."""

    parallel_startup = True
    log = logging.getLogger(__name__)

    def __init__(self):
        self.nodes = [MagicMock(name="node-1")]
        self.release = threading.Event()

    def node_setup(self, node, **kwargs):
        self.release.wait(NODE_SETUP_BLOCK_SEC)

    def node_startup(self, node, **kwargs):
        pass

    @wait_for_init_wrap
    def wait_for_init(self, node_list=None, verbose=False, timeout=None, check_node_health=True):
        pass


@pytest.fixture(name="cluster")
def cluster_fixture():
    """Cluster with a hung node setup, released on teardown so no thread outlives the test."""
    cluster = HangingSetupCluster()
    yield cluster
    cluster.release.set()


def test_wait_for_init_timeout_is_in_seconds(cluster, events_function_scope):
    """A hung node setup must raise NodeSetupTimeout once `timeout` seconds pass, not minutes."""
    start = time.perf_counter()
    with pytest.raises(NodeSetupTimeout, match=r"TIMEOUT \[2 s\]"):
        cluster.wait_for_init(node_list=cluster.nodes, timeout=2)

    assert time.perf_counter() - start < NODE_SETUP_BLOCK_SEC
