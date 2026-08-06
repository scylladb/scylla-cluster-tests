"""Tests for sdcm.nemesis.monkey.views_indexes module.

Covers the three MV/SI nemesis extracted in Phase 9: guard clauses that raise
UnsupportedNemesis, the happy-path flow (create → build → verify → drop) with
mocked cluster objects, and registry discovery / target-pool resolution.
"""

from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import pytest

import sdcm.nemesis
from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis.monkey.views_indexes import (
    AddRemoveMvNemesis,
    CreateIndexNemesis,
    KillMVBuildingCoordinator,
)
from sdcm.nemesis.utils import NEMESIS_TARGET_POOLS

_MODULE = "sdcm.nemesis.monkey.views_indexes"

pytestmark = pytest.mark.usefixtures("events")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def runner(base_runner):
    """``base_runner`` with attributes the MV/SI nemesis expect."""
    base_runner.cluster.nemesis_count = 1
    base_runner.cluster.get_non_system_ks_cf_list.return_value = ["ks1.tbl1"]
    base_runner.node_allocator = MagicMock()
    base_runner.current_disruption = "disruption-1"
    return base_runner


# ---------------------------------------------------------------------------
# CreateIndexNemesis
# ---------------------------------------------------------------------------


def test_create_index_skips_on_parallel_nemesis(runner):
    """Parallel nemesis run + open issue -> UnsupportedNemesis."""
    runner.cluster.nemesis_count = 2
    with patch(f"{_MODULE}.SkipPerIssues", return_value=True):
        monkey = CreateIndexNemesis(runner)
        with pytest.raises(UnsupportedNemesis, match="parallel nemesis run"):
            monkey.disrupt()


def test_create_index_raises_when_no_tables(runner):
    """No non-system table -> UnsupportedNemesis."""
    runner.cluster.get_non_system_ks_cf_list.return_value = []
    with patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=False):
        monkey = CreateIndexNemesis(runner)
        with pytest.raises(UnsupportedNemesis, match="No table found to create index"):
            monkey.disrupt()


def test_create_index_raises_when_no_column(runner):
    """No suitable column -> UnsupportedNemesis."""
    with (
        patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=False),
        patch(f"{_MODULE}.get_random_column_name", return_value=None),
    ):
        monkey = CreateIndexNemesis(runner)
        with pytest.raises(UnsupportedNemesis, match="No column found to create index"):
            monkey.disrupt()


def test_create_index_happy_path_creates_and_drops(runner):
    """Full flow builds the index, verifies the query, and always drops it."""
    with (
        patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=False),
        patch(f"{_MODULE}.get_random_column_name", return_value="col1"),
        patch(f"{_MODULE}.DbNodeLogger"),
        patch(f"{_MODULE}.adaptive_timeout") as mock_timeout,
        patch(f"{_MODULE}.create_index", return_value="idx1") as mock_create,
        patch(f"{_MODULE}.wait_for_index_to_be_built") as mock_wait,
        patch(f"{_MODULE}.verify_query_by_index_works") as mock_verify,
        patch(f"{_MODULE}.sleep_for_percent_of_duration"),
        patch(f"{_MODULE}.drop_index") as mock_drop,
    ):
        mock_timeout.return_value.__enter__.return_value = 100
        monkey = CreateIndexNemesis(runner)
        monkey.disrupt()

    mock_create.assert_called_once()
    mock_wait.assert_called_once()
    mock_verify.assert_called_once()
    mock_drop.assert_called_once()
    assert mock_drop.call_args[0][2] == "idx1"


def test_create_index_drops_index_even_when_verify_fails(runner):
    """If verification raises, the index is still dropped (finally block)."""
    with (
        patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=False),
        patch(f"{_MODULE}.get_random_column_name", return_value="col1"),
        patch(f"{_MODULE}.DbNodeLogger"),
        patch(f"{_MODULE}.adaptive_timeout") as mock_timeout,
        patch(f"{_MODULE}.create_index", return_value="idx1"),
        patch(f"{_MODULE}.wait_for_index_to_be_built", side_effect=RuntimeError("boom")),
        patch(f"{_MODULE}.drop_index") as mock_drop,
    ):
        mock_timeout.return_value.__enter__.return_value = 100
        monkey = CreateIndexNemesis(runner)
        with pytest.raises(RuntimeError, match="boom"):
            monkey.disrupt()

    mock_drop.assert_called_once()


# ---------------------------------------------------------------------------
# AddRemoveMvNemesis
# ---------------------------------------------------------------------------


def test_add_remove_mv_raises_when_no_free_nodes(runner):
    """All data nodes busy -> UnsupportedNemesis."""
    # base_runner mock nodes have a truthy ``running_nemesis`` by default.
    with patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=False):
        monkey = AddRemoveMvNemesis(runner)
        with pytest.raises(UnsupportedNemesis, match="Not enough free nodes"):
            monkey.disrupt()


def test_add_remove_mv_raises_when_no_tables(runner):
    """Free node available but no eligible table -> UnsupportedNemesis."""
    for node in runner.cluster.data_nodes:
        node.running_nemesis = None
    runner.cluster.get_non_system_ks_cf_list.return_value = []
    with (
        patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=False),
        patch(f"{_MODULE}.suppress_expected_unavailability_errors", return_value=nullcontext()),
    ):
        monkey = AddRemoveMvNemesis(runner)
        with pytest.raises(UnsupportedNemesis, match="Non-system keyspace and table"):
            monkey.disrupt()


def test_add_remove_mv_happy_path(runner):
    """Node is stopped, MV created & built, then dropped and scylla restarted."""
    for node in runner.cluster.data_nodes:
        node.running_nemesis = None
    with (
        patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=False),
        patch(f"{_MODULE}.suppress_expected_unavailability_errors", return_value=nullcontext()),
        patch(f"{_MODULE}.create_materialized_view_for_random_column") as mock_create,
        patch(f"{_MODULE}.adaptive_timeout") as mock_timeout,
        patch(f"{_MODULE}.wait_for_view_to_be_built") as mock_wait,
        patch(f"{_MODULE}.sleep_for_percent_of_duration"),
        patch(f"{_MODULE}.drop_materialized_view") as mock_drop,
    ):
        mock_timeout.return_value.__enter__.return_value = 100
        monkey = AddRemoveMvNemesis(runner)
        monkey.disrupt()

    runner.target_node.stop_scylla.assert_called_once()
    runner.target_node.start_scylla.assert_called_once()
    mock_create.assert_called_once()
    mock_wait.assert_called_once()
    mock_drop.assert_called_once()


def test_add_remove_mv_restarts_scylla_when_create_fails(runner):
    """If MV creation fails, scylla is restarted and the error re-raised."""
    for node in runner.cluster.data_nodes:
        node.running_nemesis = None
    with (
        patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=False),
        patch(f"{_MODULE}.suppress_expected_unavailability_errors", return_value=nullcontext()),
        patch(
            f"{_MODULE}.create_materialized_view_for_random_column",
            side_effect=RuntimeError("mv boom"),
        ),
    ):
        monkey = AddRemoveMvNemesis(runner)
        with pytest.raises(RuntimeError, match="mv boom"):
            monkey.disrupt()

    runner.target_node.stop_scylla.assert_called_once()
    runner.target_node.start_scylla.assert_called_once()


def test_add_remove_mv_restarts_scylla_when_connection_fails(runner):
    """If opening the CQL session itself fails, scylla is still restarted (not just
    when MV creation fails after a session was successfully opened)."""
    for node in runner.cluster.data_nodes:
        node.running_nemesis = None
    runner.cluster.cql_connection_patient.side_effect = RuntimeError("connection boom")
    with (
        patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=False),
        patch(f"{_MODULE}.suppress_expected_unavailability_errors", return_value=nullcontext()),
    ):
        monkey = AddRemoveMvNemesis(runner)
        with pytest.raises(RuntimeError, match="connection boom"):
            monkey.disrupt()

    runner.target_node.stop_scylla.assert_called_once()
    runner.target_node.start_scylla.assert_called_once()


# ---------------------------------------------------------------------------
# KillMVBuildingCoordinator
# ---------------------------------------------------------------------------


def test_kill_mv_coordinator_raises_without_consistent_topology(runner):
    """Consistent topology changes disabled -> UnsupportedNemesis."""
    runner.target_node.raft.is_consistent_topology_changes_enabled = False
    monkey = KillMVBuildingCoordinator(runner)
    with pytest.raises(UnsupportedNemesis, match="Consistent topology changes"):
        monkey.disrupt()


def test_kill_mv_coordinator_raises_without_tablets(runner):
    """Tablets feature disabled -> UnsupportedNemesis."""
    runner.target_node.raft.is_consistent_topology_changes_enabled = True
    with patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=False):
        monkey = KillMVBuildingCoordinator(runner)
        with pytest.raises(UnsupportedNemesis, match="works only with tablets"):
            monkey.disrupt()


@pytest.fixture()
def kill_runner(runner):
    """``runner`` wired for the full KillMVBuildingCoordinator orchestration.

    Feature guards pass, a single tablet keyspace/table is available, and the
    verification session returns a non-empty SELECT so the built-view assertion
    succeeds. ``cluster.nodes`` has two entries -> one coordinator restart.
    """
    runner.target_node.raft.is_consistent_topology_changes_enabled = True
    runner.cluster.nodes = [MagicMock(name="n1"), MagicMock(name="n2")]
    runner.cluster.get_non_system_ks_cf_with_tablets_list.return_value = ["ks1.tbl1"]
    runner._kill_scylla_daemon = MagicMock()
    runner.switch_target_node = MagicMock()
    # Verification SELECT must yield at least one row for the built-view assertion.
    session = MagicMock()
    session.execute.return_value = [MagicMock()]
    runner.cluster.cql_connection_patient.return_value.__enter__.return_value = session
    return runner


def test_kill_mv_coordinator_happy_path(kill_runner):
    """Full orchestration: coordinator switch/restart loop, successful build and
    query, and MV cleanup in the finally block."""
    coordinator = MagicMock(name="coordinator")
    with (
        patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=True),
        patch(f"{_MODULE}.is_views_with_tablets_enabled", return_value=True),
        patch(f"{_MODULE}.get_topology_coordinator_node", return_value=coordinator),
        patch(f"{_MODULE}.create_materialized_view_for_random_column") as mock_create,
        patch(f"{_MODULE}.wait_materialized_view_building_tasks_started"),
        patch(f"{_MODULE}.adaptive_timeout") as mock_timeout,
        patch(f"{_MODULE}.wait_for_view_to_be_built") as mock_wait,
        patch(f"{_MODULE}.drop_materialized_view") as mock_drop,
    ):
        mock_timeout.return_value.__enter__.return_value = 100
        monkey = KillMVBuildingCoordinator(kill_runner)
        monkey.disrupt()

    mock_create.assert_called_once()
    mock_wait.assert_called_once()
    mock_drop.assert_called_once()
    # runner delegation: coordinator is targeted and killed once (len(nodes) // 2 == 1).
    kill_runner.switch_target_node.assert_called_with(coordinator)
    kill_runner._kill_scylla_daemon.assert_called_once()


def test_kill_mv_coordinator_drops_view_when_build_fails(kill_runner):
    """If the view never finishes building, the MV is still dropped (finally)."""
    coordinator = MagicMock(name="coordinator")
    with (
        patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=True),
        patch(f"{_MODULE}.is_views_with_tablets_enabled", return_value=True),
        patch(f"{_MODULE}.get_topology_coordinator_node", return_value=coordinator),
        patch(f"{_MODULE}.create_materialized_view_for_random_column"),
        patch(f"{_MODULE}.wait_materialized_view_building_tasks_started"),
        patch(f"{_MODULE}.adaptive_timeout") as mock_timeout,
        patch(f"{_MODULE}.wait_for_view_to_be_built", side_effect=RuntimeError("not built")),
        patch(f"{_MODULE}.drop_materialized_view") as mock_drop,
    ):
        mock_timeout.return_value.__enter__.return_value = 100
        monkey = KillMVBuildingCoordinator(kill_runner)
        with pytest.raises(RuntimeError, match="not built"):
            monkey.disrupt()

    mock_drop.assert_called_once()


# ---------------------------------------------------------------------------
# Registry discovery / target pools
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "monkey_cls, expected_pool",
    [
        (CreateIndexNemesis, NEMESIS_TARGET_POOLS.data_nodes),
        (AddRemoveMvNemesis, NEMESIS_TARGET_POOLS.data_nodes),
        (KillMVBuildingCoordinator, NEMESIS_TARGET_POOLS.all_nodes),
    ],
)
def test_monkey_discovered_with_expected_target_pool(monkey_cls, expected_pool):
    """Auto-discovery registers each monkey and preserves its target pool."""
    assert monkey_cls.__name__ in sdcm.nemesis.__all__
    assert monkey_cls.target_pool == expected_pool
