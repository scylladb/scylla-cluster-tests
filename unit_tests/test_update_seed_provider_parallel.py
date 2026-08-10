"""Unit tests for SCT-731: parallelize BaseScyllaCluster.update_seed_provider.

Verifies every node's scylla.yaml seed_provider is updated and that the per-node
updates run concurrently rather than serially.
"""

import threading
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster import BaseScyllaCluster, BaseCluster


def _make_nodes(count):
    # `MagicMock(name=...)` does NOT set an accessible `.name` attribute (it's a reserved
    # constructor kwarg used for the mock's repr); assign the attribute explicitly instead.
    nodes = []
    for i in range(count):
        node = MagicMock()
        node.name = f"node-{i}"
        nodes.append(node)
    return nodes


def _attach_run_func_parallel(cluster):
    # run_func_parallel is defined on the BaseCluster mixin; concrete cluster classes combine
    # BaseScyllaCluster with BaseCluster. In these isolated unit tests we instantiate a single
    # mixin, so bind the real implementation onto the instance.
    cluster.run_func_parallel = BaseCluster.run_func_parallel.__get__(cluster)


@pytest.fixture
def scylla_cluster():
    with patch.object(BaseScyllaCluster, "__init__", lambda self, **kw: None):
        cluster = BaseScyllaCluster()
        cluster.log = MagicMock()
        cluster.nodes = []
        _attach_run_func_parallel(cluster)
        return cluster


def test_update_seed_provider_processes_all_nodes(scylla_cluster):
    nodes = _make_nodes(4)
    for node in nodes:
        yaml_obj = MagicMock()

        @contextmanager
        def _cm(_yaml_obj=yaml_obj):
            yield _yaml_obj

        node.remote_scylla_yaml.side_effect = _cm
        node._yaml_obj = yaml_obj
    scylla_cluster.nodes = nodes

    scylla_cluster.update_seed_provider()

    for node in nodes:
        node.remote_scylla_yaml.assert_called_once()
        assert node._yaml_obj.seed_provider == node.proposed_scylla_yaml.seed_provider


def test_update_seed_provider_runs_in_parallel(scylla_cluster):
    barrier = threading.Barrier(3, timeout=10)
    nodes = _make_nodes(3)
    for node in nodes:

        @contextmanager
        def _cm(_node=node):
            barrier.wait()  # blocks unless all 3 run concurrently
            yield MagicMock()

        node.remote_scylla_yaml.side_effect = _cm
    scylla_cluster.nodes = nodes

    scylla_cluster.update_seed_provider()  # would raise BrokenBarrierError if serial
