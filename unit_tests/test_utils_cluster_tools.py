from sdcm.utils.cluster_tools import group_nodes_by_dc_idx, check_cluster_layout
from unittest.mock import MagicMock


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
    db_cluster = MagicMock(nodes=[
        MagicMock(dc_idx=0, rack="rack1"),
        MagicMock(dc_idx=0, rack="rack1"),
        MagicMock(dc_idx=0, rack="rack2"),
        MagicMock(dc_idx=0, rack="rack2"),
    ])
    db_cluster.params = {"capacity_errors_check_mode": "per-initial_config", "n_db_nodes": "4"}
    db_cluster.racks_count = 2
    assert check_cluster_layout(db_cluster) is True


def test_check_cluster_layout_on_two_dcs():
    """Test that a cluster with two datacenters is matching initial configuration."""
    db_cluster = MagicMock(nodes=[
        MagicMock(dc_idx=0, rack="rack1"),
        MagicMock(dc_idx=0, rack="rack2"),
        MagicMock(dc_idx=1, rack="rack1"),
        MagicMock(dc_idx=1, rack="rack2"),
    ])
    db_cluster.params = {"capacity_errors_check_mode": "per-initial_config", "n_db_nodes": "2 2"}
    db_cluster.racks_count = 2
    assert check_cluster_layout(db_cluster) is True


def test_check_cluster_layout_unbalanced_racks():
    """Test that a cluster that does match configuration returns True. even if racks are unbalanced."""

    db_cluster = MagicMock(nodes=[
        MagicMock(dc_idx=0, rack="rack1"),
        MagicMock(dc_idx=0, rack="rack1"),
        MagicMock(dc_idx=0, rack="rack2"),
    ])
    db_cluster.params = {"capacity_errors_check_mode": "per-initial_config", "n_db_nodes": "3"}
    db_cluster.racks_count = 2
    assert check_cluster_layout(db_cluster) is True


def test_check_cluster_layout_unbalanced_on_two_dcs():
    """Test that a cluster that doesn't match configuration across two datacenters returns False."""
    db_cluster = MagicMock(nodes=[
        MagicMock(dc_idx=0, rack="rack1"),
        MagicMock(dc_idx=0, rack="rack1"),
        MagicMock(dc_idx=0, rack="rack2"),
        MagicMock(dc_idx=0, rack="rack2"),
        MagicMock(dc_idx=1, rack="rack1"),
        MagicMock(dc_idx=1, rack="rack1"),
        MagicMock(dc_idx=1, rack="rack2"),
    ])
    db_cluster.params = {"capacity_errors_check_mode": "per-initial_config", "n_db_nodes": "4 4"}
    db_cluster.racks_count = 2
    assert check_cluster_layout(db_cluster) is False


def test_cluster_with_no_nodes():
    """Test that a cluster with no nodes is considered correct layout."""
    db_cluster = MagicMock(nodes=[])
    db_cluster.params = {"capacity_errors_check_mode": "per-initial_config"}
    assert check_cluster_layout(db_cluster) is True
