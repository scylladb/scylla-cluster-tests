import threading
from functools import partial
from unittest.mock import MagicMock

import pytest

from sdcm.utils.cluster_tools import (
    check_cluster_layout,
    clear_snapshot_nodes,
    flush_nodes,
    group_nodes_by_dc_idx,
    major_compaction_nodes,
)

# More nodes than ThreadPoolExecutor's default pool can ever hold (min(32, cpu_count + 4)).
NODES_ABOVE_DEFAULT_POOL = 40


def test_nodes_grouped_by_dc_idx_are_correctly_grouped():
    """Test that nodes are grouped by their dc_idx correctly."""
    nodes = [
        MagicMock(dc_idx=1),
        MagicMock(dc_idx=1),
        MagicMock(dc_idx=2),
    ]
    grouped_nodes = group_nodes_by_dc_idx(nodes)
    assert len(grouped_nodes) == 2
    assert len(grouped_nodes[1]) == 2
    assert len(grouped_nodes[2]) == 1


def test_check_cluster_layout():
    """Test that a cluster that matches the initial configuration."""
    db_cluster = MagicMock(
        nodes=[
            MagicMock(dc_idx=0, rack="rack1"),
            MagicMock(dc_idx=0, rack="rack1"),
            MagicMock(dc_idx=0, rack="rack2"),
            MagicMock(dc_idx=0, rack="rack2"),
        ]
    )
    db_cluster.params = {"capacity_errors_check_mode": "per-initial_config", "n_db_nodes": [4]}
    db_cluster.racks_count = 2
    assert check_cluster_layout(db_cluster) is True


def test_check_cluster_layout_on_two_dcs():
    """Test that a cluster with two datacenters is matching initial configuration."""
    db_cluster = MagicMock(
        nodes=[
            MagicMock(dc_idx=0, rack="rack1"),
            MagicMock(dc_idx=0, rack="rack2"),
            MagicMock(dc_idx=1, rack="rack1"),
            MagicMock(dc_idx=1, rack="rack2"),
        ]
    )
    db_cluster.params = {"capacity_errors_check_mode": "per-initial_config", "n_db_nodes": [2, 2]}
    db_cluster.racks_count = 2
    assert check_cluster_layout(db_cluster) is True


def test_check_cluster_layout_unbalanced_racks():
    """Test that a cluster that does match configuration returns True. even if racks are unbalanced."""

    db_cluster = MagicMock(
        nodes=[
            MagicMock(dc_idx=0, rack="rack1"),
            MagicMock(dc_idx=0, rack="rack1"),
            MagicMock(dc_idx=0, rack="rack2"),
        ]
    )
    db_cluster.params = {"capacity_errors_check_mode": "per-initial_config", "n_db_nodes": [3]}
    db_cluster.racks_count = 2
    assert check_cluster_layout(db_cluster) is True


def test_check_cluster_layout_unbalanced_on_two_dcs():
    """Test that a cluster that doesn't match configuration across two datacenters returns False."""
    db_cluster = MagicMock(
        nodes=[
            MagicMock(dc_idx=0, rack="rack1"),
            MagicMock(dc_idx=0, rack="rack1"),
            MagicMock(dc_idx=0, rack="rack2"),
            MagicMock(dc_idx=0, rack="rack2"),
            MagicMock(dc_idx=1, rack="rack1"),
            MagicMock(dc_idx=1, rack="rack1"),
            MagicMock(dc_idx=1, rack="rack2"),
        ]
    )
    db_cluster.params = {"capacity_errors_check_mode": "per-initial_config", "n_db_nodes": [4, 4]}
    db_cluster.racks_count = 2
    assert check_cluster_layout(db_cluster) is False


def test_cluster_with_no_nodes():
    """Test that a cluster with no nodes is considered correct layout."""
    db_cluster = MagicMock(nodes=[])
    db_cluster.params = {"capacity_errors_check_mode": "per-initial_config"}
    assert check_cluster_layout(db_cluster) is True


@pytest.mark.parametrize(
    "run_on_nodes",
    [
        pytest.param(partial(flush_nodes, keyspace="ks"), id="flush_nodes"),
        pytest.param(partial(major_compaction_nodes, keyspace="ks", table="t"), id="major_compaction_nodes"),
        pytest.param(clear_snapshot_nodes, id="clear_snapshot_nodes"),
    ],
)
def test_nodetool_runs_on_all_data_nodes_concurrently(run_on_nodes):
    """Every data node gets its own worker, so a cluster larger than the default thread pool is not serialized."""
    barrier = threading.Barrier(NODES_ABOVE_DEFAULT_POOL, timeout=30)
    nodes = [
        MagicMock(**{"run_nodetool.side_effect": lambda *_, **__: barrier.wait()})
        for _ in range(NODES_ABOVE_DEFAULT_POOL)
    ]
    cluster = MagicMock(data_nodes=nodes)

    run_on_nodes(cluster)

    assert not barrier.broken
    assert all(node.run_nodetool.called for node in nodes)
