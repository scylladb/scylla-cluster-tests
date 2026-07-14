"""Tests for sdcm.nemesis.monkey.add_remove_dc module."""

from threading import Event
from unittest.mock import MagicMock, call, patch

import pytest

from sdcm.exceptions import KillNemesis, UnsupportedNemesis
from sdcm.nemesis.monkey.add_remove_dc import AddRemoveDcNemesis
from sdcm.utils.replication_strategy_utils import (
    NetworkTopologyReplicationStrategy,
    ReplicationStrategy,
    SimpleReplicationStrategy,
)

_MODULE = "sdcm.nemesis.monkey.add_remove_dc"

pytestmark = pytest.mark.usefixtures("events")


def _clone_strategy(strategy):
    """Creates a deep copy of a replication strategy for comparison.

    Args:
        strategy: A ReplicationStrategy instance (NetworkTopologyReplicationStrategy or SimpleReplicationStrategy).

    Returns:
        A new instance of the same strategy type with identical replication factors.
    """
    if isinstance(strategy, NetworkTopologyReplicationStrategy):
        return NetworkTopologyReplicationStrategy(**strategy.replication_factors_per_dc)
    return SimpleReplicationStrategy(strategy.replication_factors[0])


class FakeReplicationStrategySetter:
    """Context manager that records keyspace strategy updates."""

    def __init__(self, name, events):
        """Initializes the fake replication strategy setter.

        Args:
            name: Identifier for this setter instance (used in rollback events).
            events: Shared list to record lifecycle events (e.g., rollback).
        """
        self.name = name
        self.events = events
        self.calls = []
        self.preserved = {}
        self.rollback_calls = []

    def __enter__(self):
        """Enters the context manager.

        Returns:
            Self for use in with-statement binding.
        """
        return self

    def __exit__(self, *exc):
        """Exits the context manager and triggers rollback.

        Args:
            *exc: Exception information (type, value, traceback).

        Returns:
            False to propagate any exception.
        """
        self.rollback_calls.append(self.preserved.copy())
        self.events.append(f"{self.name}:rollback")
        return False

    def __call__(self, **keyspaces):
        """Records a replication strategy update for the given keyspaces.

        Preserves the original strategy on first update to each keyspace for rollback.

        Args:
            **keyspaces: Mapping of keyspace names to new ReplicationStrategy instances.
        """
        self.calls.append(keyspaces)
        for keyspace, strategy in keyspaces.items():
            if keyspace not in self.preserved:
                self.preserved[keyspace] = _clone_strategy(ReplicationStrategy.get(None, keyspace))


@pytest.fixture()
def runner(base_runner):
    """Base runner extended with add/remove-dc specific cluster state."""
    status_by_dc = {"dc1": object()}

    base_runner.cluster.test_config = MagicMock(MULTI_REGION=False)
    base_runner.cluster.racks = ["rack1"]
    base_runner.cluster.nodes = list(base_runner.cluster.data_nodes)
    base_runner.cluster.is_features_enabled_on_node.return_value = True
    base_runner.tester.prepare_phase_active = Event()
    base_runner.decommission_nodes = MagicMock()
    base_runner.run_repair = MagicMock()
    base_runner.current_disruption = "AddRemoveDcNemesis"
    base_runner.monitoring_set = MagicMock()
    base_runner._nemesis_stress_failure_handler = MagicMock()

    base_runner.target_node.raft = MagicMock(is_consistent_topology_changes_enabled=True)

    base_runner.tester.db_cluster = MagicMock()
    base_runner.tester.db_cluster.get_nodetool_status.side_effect = lambda: dict(status_by_dc)

    base_runner.status_by_dc = status_by_dc
    return base_runner


def test_disrupt_raises_unsupported_for_multi_region(runner):
    """MULTI_REGION runs are rejected before any topology changes start."""
    runner.cluster.test_config.MULTI_REGION = True

    with pytest.raises(UnsupportedNemesis, match="multi-dc scenario"):
        AddRemoveDcNemesis(runner).disrupt()

    runner.tester.create_keyspace.assert_not_called()
    runner.cluster.add_nodes.assert_not_called()
    runner.run_repair.assert_not_called()
    runner.decommission_nodes.assert_not_called()
    assert not runner.executed


@pytest.mark.parametrize(
    "prepare_phase_active,error_type,expected_error",
    [
        pytest.param(True, UnsupportedNemesis, "prepare phase", id="active"),
        pytest.param(False, RuntimeError, "workflow started", id="inactive"),
    ],
)
def test_disrupt_prepare_phase_gate(runner, prepare_phase_active, error_type, expected_error):
    """disrupt() should skip only while the prepare phase event is set."""
    if prepare_phase_active:
        runner.tester.prepare_phase_active.set()

    monkey = AddRemoveDcNemesis(runner)
    monkey.create_new_dc_keyspace = MagicMock(side_effect=RuntimeError("workflow started"))

    with pytest.raises(error_type, match=expected_error):
        monkey.disrupt()

    if prepare_phase_active:
        monkey.create_new_dc_keyspace.assert_not_called()
    else:
        monkey.create_new_dc_keyspace.assert_called_once_with()

    runner.cluster.add_nodes.assert_not_called()
    runner.run_repair.assert_not_called()
    runner.decommission_nodes.assert_not_called()


def test_disrupt_updates_replication_and_cleans_new_dc(runner):
    """disrupt() should update replication for the temporary DC and remove it afterwards."""
    monkey = AddRemoveDcNemesis(runner)
    new_nodes = [MagicMock(name="node-new-1"), MagicMock(name="node-new-2")]
    events = []
    first_setter = FakeReplicationStrategySetter("system-keyspaces", events)
    second_setter = FakeReplicationStrategySetter("new-dc-rf", events)

    def add_nodes():
        monkey.new_nodes = new_nodes
        runner.status_by_dc["dc1_nemesis_dc"] = object()

    def decommission_nodes():
        events.append("decommission")
        runner.decommission_nodes(new_nodes)
        monkey.new_nodes = []
        runner.status_by_dc.pop("dc1_nemesis_dc", None)

    monkey.add_nodes_in_new_dc = MagicMock(side_effect=add_nodes)
    monkey.decommission_new_nodes = MagicMock(side_effect=decommission_nodes)
    monkey.rebuild_node = MagicMock()
    monkey.write_to_multi_dc_keyspace = MagicMock()
    monkey.verify_multi_dc_keyspace = MagicMock()

    def fake_get(node, keyspace):
        # After first_setter preserves a system keyspace, subsequent gets should return NTS
        if keyspace in first_setter.preserved:
            return NetworkTopologyReplicationStrategy(**{monkey.initial_dc_name: monkey.new_ks_rf})
        if keyspace in ("system_distributed", "system_traces"):
            return SimpleReplicationStrategy(monkey.new_ks_rf)
        return NetworkTopologyReplicationStrategy(**{monkey.initial_dc_name: monkey.new_ks_rf})

    with (
        patch(f"{_MODULE}.temporary_replication_strategy_setter", side_effect=[first_setter, second_setter]),
        patch(f"{_MODULE}.ReplicationStrategy.get", side_effect=fake_get),
        patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=False),
    ):
        monkey.disrupt()

    runner.tester.run_stress_thread.assert_called_once()
    stress_kwargs = runner.tester.run_stress_thread.call_args.kwargs
    expected_replication = f"replication(strategy=NetworkTopologyStrategy,{monkey.initial_dc_name}={monkey.new_ks_rf})"
    assert f"keyspace={monkey.new_ks_name}" in stress_kwargs["stress_cmd"]
    assert expected_replication in stress_kwargs["stress_cmd"]
    assert stress_kwargs["round_robin"] is True
    assert stress_kwargs["stop_test_on_failure"] is False
    runner.tester.verify_stress_thread.assert_called_once_with(
        runner.tester.run_stress_thread.return_value,
        error_handler=runner._nemesis_stress_failure_handler,
    )
    monkey.add_nodes_in_new_dc.assert_called_once_with()
    assert monkey.rebuild_node.call_args_list == [call(new_nodes[0]), call(new_nodes[1])]
    runner.run_repair.assert_called_once_with()
    monkey.write_to_multi_dc_keyspace.assert_called_once_with()
    monkey.decommission_new_nodes.assert_called_once_with()
    assert monkey.verify_multi_dc_keyspace.call_args_list == [
        call(consistency_level="ALL"),
        call(consistency_level="QUORUM"),
    ]

    assert list(first_setter.calls[0]) == ["system_distributed"]
    switched_strategy = first_setter.calls[0]["system_distributed"]
    assert isinstance(switched_strategy, NetworkTopologyReplicationStrategy)
    assert switched_strategy.replication_factors_per_dc == {monkey.initial_dc_name: monkey.new_ks_rf}
    preserved_strategy = first_setter.preserved["system_distributed"]
    assert isinstance(preserved_strategy, SimpleReplicationStrategy)
    assert preserved_strategy.replication_factors == [monkey.new_ks_rf]

    updated_keyspaces = [next(iter(entry)) for entry in second_setter.calls]
    assert updated_keyspaces == ["system_distributed", "system_traces", monkey.new_ks_name]
    for entry in second_setter.calls:
        strategy = next(iter(entry.values()))
        assert strategy.replication_factors_per_dc == {
            monkey.initial_dc_name: monkey.new_ks_rf,
            "dc1_nemesis_dc": monkey.num_nodes_in_new_dc,
        }

    for preserved_strategy in second_setter.preserved.values():
        assert preserved_strategy.replication_factors_per_dc == {
            monkey.initial_dc_name: monkey.new_ks_rf,
            "dc1_nemesis_dc": 0,
        }

    assert len(second_setter.rollback_calls) == 1
    assert list(second_setter.rollback_calls[0]) == updated_keyspaces
    assert events.index("new-dc-rf:rollback") < events.index("decommission")

    assert monkey.new_dc_name is None


def test_assert_new_dc_registered_retries_until_status_contains_new_dc(runner):
    """assert_new_dc_registered() should retry until nodetool status shows the temporary DC."""
    monkey = AddRemoveDcNemesis(runner)
    runner.tester.db_cluster.get_nodetool_status.side_effect = [
        {"dc1": object()},
        {"dc1": object(), "dc1_nemesis_dc": object()},
    ]
    runner.tester.db_cluster.get_nodetool_status.reset_mock()

    with patch("sdcm.utils.decorators.time.sleep") as sleep:
        monkey.assert_new_dc_registered()

    sleep.assert_called_once_with(30)
    assert runner.tester.db_cluster.get_nodetool_status.call_count == 2


def test_assert_new_dc_unregistered_retries_until_status_drops_new_dc(runner):
    """assert_new_dc_unregistered() should retry until nodetool status drops the temporary DC."""
    monkey = AddRemoveDcNemesis(runner)
    runner.tester.db_cluster.get_nodetool_status.side_effect = [
        {"dc1": object(), "dc1_nemesis_dc": object()},
        {"dc1": object()},
    ]
    runner.tester.db_cluster.get_nodetool_status.reset_mock()

    with patch("sdcm.utils.decorators.time.sleep") as sleep:
        monkey.assert_new_dc_unregistered()

    sleep.assert_called_once_with(30)
    assert runner.tester.db_cluster.get_nodetool_status.call_count == 2


@pytest.mark.parametrize(
    "method_name,status,message",
    [
        pytest.param(
            "assert_new_dc_registered",
            {"dc1": object()},
            "new datacenter was not registered",
            id="registered",
        ),
        pytest.param(
            "assert_new_dc_unregistered",
            {"dc1": object(), "dc1_nemesis_dc": object()},
            "new datacenter was not unregistered",
            id="unregistered",
        ),
    ],
)
def test_assert_new_dc_state_methods_retry_three_times_before_raising(runner, method_name, status, message):
    """DC state assertions should attempt the nodetool status check three times before failing."""
    monkey = AddRemoveDcNemesis(runner)
    runner.tester.db_cluster.get_nodetool_status.side_effect = None
    runner.tester.db_cluster.get_nodetool_status.return_value = status
    runner.tester.db_cluster.get_nodetool_status.reset_mock()

    with patch("sdcm.utils.decorators.time.sleep") as sleep:
        with pytest.raises(AssertionError, match=message):
            getattr(monkey, method_name)()

    assert runner.tester.db_cluster.get_nodetool_status.call_count == 3
    assert sleep.call_args_list == [call(30), call(30), call(30)]


def test_finalizer_drops_keyspace_and_decommissions_new_nodes(runner):
    """finalizer() should clean up the keyspace and temporary nodes on regular exits."""
    monkey = AddRemoveDcNemesis(runner)
    monkey.new_nodes = [MagicMock(name="node-new")]
    monkey.decommission_new_nodes = MagicMock()

    monkey.finalizer(Exception, None, None)

    assert runner.executed[-1] == f"DROP KEYSPACE IF EXISTS {monkey.new_ks_name}"
    monkey.decommission_new_nodes.assert_called_once_with()


def test_finalizer_skips_cleanup_on_kill_nemesis(runner):
    """finalizer() should not drop keyspace or decommission when KillNemesis is raised."""
    monkey = AddRemoveDcNemesis(runner)
    monkey.new_nodes = [MagicMock(name="node-new")]
    monkey.decommission_new_nodes = MagicMock()

    monkey.finalizer(KillNemesis, None, None)

    assert not runner.executed
    monkey.decommission_new_nodes.assert_not_called()


def test_finalizer_skips_decommission_when_no_new_nodes(runner):
    """finalizer() should not call decommission when new_nodes is empty."""
    monkey = AddRemoveDcNemesis(runner)
    monkey.new_nodes = []
    monkey.decommission_new_nodes = MagicMock()

    monkey.finalizer(Exception, None, None)

    assert runner.executed[-1] == f"DROP KEYSPACE IF EXISTS {monkey.new_ks_name}"
    monkey.decommission_new_nodes.assert_not_called()


@pytest.mark.parametrize(
    "feature_enabled,expected_count",
    [
        pytest.param(True, 2, id="multi-rf-change-enabled"),
        pytest.param(False, 1, id="multi-rf-change-disabled"),
    ],
)
def test_num_nodes_in_new_dc_depends_on_multi_rf_change_feature(runner, feature_enabled, expected_count):
    """num_nodes_in_new_dc should match the supported RF change size."""
    runner.cluster.is_features_enabled_on_node.return_value = feature_enabled
    monkey = AddRemoveDcNemesis(runner)

    assert monkey.num_nodes_in_new_dc == expected_count
    runner.cluster.is_features_enabled_on_node.assert_called_once_with(
        node=runner.target_node, feature_list=["KEYSPACE_MULTI_RF_CHANGE"]
    )


def test_system_keyspaces_includes_auth_when_topology_changes_disabled(runner):
    """system_keyspaces should include system_auth when consistent topology changes are off."""
    runner.target_node.raft.is_consistent_topology_changes_enabled = False
    monkey = AddRemoveDcNemesis(runner)

    assert "system_auth" in monkey.system_keyspaces


def test_system_keyspaces_excludes_auth_when_topology_changes_enabled(runner):
    """system_keyspaces should NOT include system_auth when consistent topology changes are on."""
    runner.target_node.raft.is_consistent_topology_changes_enabled = True
    monkey = AddRemoveDcNemesis(runner)

    assert "system_auth" not in monkey.system_keyspaces
