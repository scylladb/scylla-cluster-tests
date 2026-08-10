"""Unit tests for SCT-721 (AWS only): parallelize node.init() in AWSCluster.add_nodes.

`_create_node` runs `node.init()`, which blocks per node on EC2 waiters, EIP allocation
and SSH/cloud-init. These tests verify the nodes are now created concurrently while
node ordering, `_node_index` and rack assignment stay deterministic, and that nodes
which initialized successfully are still registered for teardown when another fails.
"""

import threading
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster_aws import AWSCluster


def _make_cluster(racks_count=1):
    cluster = AWSCluster.__new__(AWSCluster)
    cluster.log = MagicMock()
    cluster.nodes = []
    cluster.racks_count = racks_count
    cluster._node_index = 0
    cluster.node_prefix = "test-node"
    cluster.logdir = "/tmp/sct-test"
    cluster._ec2_ami_username = "ubuntu"
    cluster.params = MagicMock()
    cluster.test_config = MagicMock()
    cluster.prepare_user_data = MagicMock(return_value="")
    cluster.write_node_public_ip_file = MagicMock()
    cluster.write_node_private_ip_file = MagicMock()
    return cluster


def _patch_instances(cluster, count):
    """Stub instance creation so add_nodes gets `count` fake EC2 instances."""
    instances = [MagicMock() for _ in range(count)]
    cluster._create_or_find_instances = MagicMock(side_effect=lambda count, **kwargs: instances[:count])
    return instances


@pytest.fixture(autouse=True)
def _no_ipv6():
    # keep add_nodes out of the ipv6 branch, which would need a real node.distro
    with patch("sdcm.cluster_aws.ssh_connection_ip_type", return_value="ipv4"):
        yield


def test_add_nodes_creates_all_nodes_in_order():
    cluster = _make_cluster()
    _patch_instances(cluster, 4)
    created = []

    def _create_node(instance, ami_username, node_prefix, node_index, base_logdir, dc_idx, rack, after_config=None):
        node = MagicMock()
        node.node_index = node_index
        node.rack = rack
        created.append(node)
        return node

    cluster._create_node = _create_node

    added = cluster.add_nodes(count=4)

    assert len(added) == 4
    assert cluster.nodes == added
    # node_index is assigned serially and ordering is preserved regardless of completion order
    assert [node.node_index for node in cluster.nodes] == [1, 2, 3, 4]
    assert cluster._node_index == 4


def test_add_nodes_initializes_nodes_in_parallel():
    cluster = _make_cluster()
    _patch_instances(cluster, 3)
    barrier = threading.Barrier(3, timeout=10)

    def _create_node(instance, ami_username, node_prefix, node_index, base_logdir, dc_idx, rack, after_config=None):
        barrier.wait()  # blocks unless all 3 nodes are initialized concurrently
        return MagicMock()

    cluster._create_node = _create_node

    # would raise BrokenBarrierError if node.init() still ran one node at a time
    cluster.add_nodes(count=3)


def test_add_nodes_spreads_racks_when_rack_is_none():
    cluster = _make_cluster(racks_count=2)
    _patch_instances(cluster, 4)
    racks = []
    lock = threading.Lock()

    def _create_node(instance, ami_username, node_prefix, node_index, base_logdir, dc_idx, rack, after_config=None):
        node = MagicMock()
        node.node_index = node_index
        with lock:
            racks.append((node_index, rack))
        return node

    cluster._create_node = _create_node

    cluster.add_nodes(count=4, rack=None)

    # rack assignment stays tied to the node's position, not to completion order
    assert sorted(racks) == [(1, 0), (2, 1), (3, 0), (4, 1)]


def test_add_nodes_keeps_successful_nodes_and_reraises():
    cluster = _make_cluster()
    _patch_instances(cluster, 3)

    def _create_node(instance, ami_username, node_prefix, node_index, base_logdir, dc_idx, rack, after_config=None):
        if node_index == 2:
            raise RuntimeError("boom")
        node = MagicMock()
        node.node_index = node_index
        return node

    cluster._create_node = _create_node

    with pytest.raises(RuntimeError, match="boom"):
        cluster.add_nodes(count=3)

    # the nodes that initialized successfully must stay registered so teardown terminates them
    assert [node.node_index for node in cluster.nodes] == [1, 3]
    # ip files are only written on a fully successful add_nodes, as before
    cluster.write_node_public_ip_file.assert_not_called()


def test_add_nodes_zero_count_is_noop():
    cluster = _make_cluster()
    cluster._create_node = MagicMock()

    assert cluster.add_nodes(count=0) == []
    cluster._create_node.assert_not_called()
