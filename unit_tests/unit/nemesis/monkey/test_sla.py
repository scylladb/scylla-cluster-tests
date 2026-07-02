"""Tests for sdcm.nemesis.monkey.sla module.

Tests focus on deterministic behavior: precondition validation, stress-command
filtering/building, error formatting, and correct delegation to SlaTests
methods.  All external I/O (Prometheus, CQL, SlaTests heavy logic) is mocked.
"""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.exceptions import NemesisSubTestFailure, UnsupportedNemesis
from sdcm.sct_events.system import TestStepEvent
from sdcm.nemesis.monkey.sla import (
    RemoveServiceLevelMonkey,
    SlaDecreaseSharesDuringLoad,
    SlaIncreaseSharesByAttachAnotherSlDuringLoad,
    SlaIncreaseSharesDuringLoad,
    SlaMaximumAllowedSlsWithMaxSharesDuringLoad,
    SlaMonkeyBase,
    SlaReplaceUsingDetachDuringLoad,
    SlaReplaceUsingDropDuringLoad,
)

_MODULE = "sdcm.nemesis.monkey.sla"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.usefixtures("events")


@pytest.fixture()
def runner(base_runner):
    """``base_runner`` extended with SLA-specific attributes.

    Adds ``monitoring_set`` (needed by all SlaTests-delegating monkeys) and
    configures cluster params so ``validate_sla_preconditions`` passes by default.
    """
    # monitoring_set needed by SlaTests-delegating monkeys
    monitor = MagicMock()
    monitor.nodes = [MagicMock()]
    monitor.nodes[0].external_address = "10.0.0.1"
    base_runner.monitoring_set = monitor

    # pass all three SLA preconditions by default
    base_runner.cluster.params.get.side_effect = lambda key, **kw: {
        "sla": True,
        "authenticator": "PasswordAuthenticator",
    }.get(key)
    base_runner.cluster.nodes = base_runner.cluster.data_nodes
    base_runner.cluster.nodes[0].is_enterprise = True

    # tester.params is None by default in TestRunner — replace with a MagicMock
    base_runner.tester.params = MagicMock()

    # stress params used by get_stress_params
    base_runner.tester.params.get.return_value = [
        "cassandra-stress write n=1000000 -schema 'replication(strategy=NetworkTopologyStrategy)'",
    ]
    base_runner.tester.get_c_s_column_definition.return_value = "-col n=5 size=FIXED(64)"
    base_runner.tester.get_data_set_size.return_value = 1000000

    return base_runner


@pytest.fixture()
def remove_sl_runner(runner):
    """Runner with a pre-configured role and service level."""
    role = MagicMock()
    role.attached_service_level_name = "sl_200"
    role.attached_service_level.shares = 200
    runner.tester.roles = [role]
    return runner


# ---------------------------------------------------------------------------
# validate_sla_preconditions
# ---------------------------------------------------------------------------


def test_preconditions_raises_when_sla_disabled(runner):
    """Raise UnsupportedNemesis when 'sla' param is falsy."""
    runner.cluster.params.get.side_effect = lambda key, **kw: {"sla": False, "authenticator": "Password"}.get(key)
    monkey = SlaIncreaseSharesDuringLoad(runner)

    with pytest.raises(UnsupportedNemesis, match="SLA nemesis can be run during SLA test only"):
        monkey.validate_sla_preconditions()


def test_preconditions_raises_when_not_enterprise(runner):
    """Raise UnsupportedNemesis when target node is not Scylla Enterprise."""
    runner.cluster.nodes[0].is_enterprise = False
    monkey = SlaIncreaseSharesDuringLoad(runner)

    with pytest.raises(UnsupportedNemesis, match="only supported by Scylla Enterprise"):
        monkey.validate_sla_preconditions()


def test_preconditions_raises_when_no_authenticator(runner):
    """Raise UnsupportedNemesis when authenticator param is falsy."""
    runner.cluster.params.get.side_effect = lambda key, **kw: {"sla": True, "authenticator": None}.get(key)
    monkey = SlaIncreaseSharesDuringLoad(runner)

    with pytest.raises(UnsupportedNemesis, match="can't work without authenticator"):
        monkey.validate_sla_preconditions()


def test_preconditions_passes_when_all_conditions_met(runner):
    """No exception raised when all three preconditions pass."""
    monkey = SlaIncreaseSharesDuringLoad(runner)
    monkey.validate_sla_preconditions()  # should not raise


# ---------------------------------------------------------------------------
# get_cassandra_stress_write_cmds
# ---------------------------------------------------------------------------


def test_get_write_cmds_returns_none_when_no_prepare_cmd(runner):
    """Return None when prepare_write_cmd is not configured."""
    runner.tester.params.get.return_value = None
    monkey = SlaIncreaseSharesDuringLoad(runner)

    assert monkey.get_cassandra_stress_write_cmds() is None


def test_get_write_cmds_wraps_single_string_into_list(runner):
    """A single string prepare_write_cmd is treated as a one-element list."""
    cmd = "cassandra-stress write n=1000 cl=QUORUM"
    runner.tester.params.get.return_value = cmd
    monkey = SlaIncreaseSharesDuringLoad(runner)

    result = monkey.get_cassandra_stress_write_cmds()

    assert result == [cmd]


def test_get_write_cmds_filters_out_profile_commands(runner):
    """Commands containing ' profile=' are excluded from the result."""
    runner.tester.params.get.return_value = [
        "cassandra-stress write n=1000 profile=/tmp/profile.yaml",  # excluded
        "cassandra-stress write n=2000 cl=QUORUM",  # included
    ]
    monkey = SlaIncreaseSharesDuringLoad(runner)

    result = monkey.get_cassandra_stress_write_cmds()

    assert result == ["cassandra-stress write n=2000 cl=QUORUM"]


def test_get_write_cmds_filters_out_commands_without_n(runner):
    """Commands without ' n=' (e.g. duration-based) are excluded."""
    runner.tester.params.get.return_value = [
        "cassandra-stress write duration=30m cl=QUORUM",  # excluded — no n=
        "cassandra-stress write n=5000 cl=QUORUM",  # included
    ]
    monkey = SlaIncreaseSharesDuringLoad(runner)

    result = monkey.get_cassandra_stress_write_cmds()

    assert result == ["cassandra-stress write n=5000 cl=QUORUM"]


def test_get_write_cmds_returns_none_when_all_filtered(runner):
    """Return None when all commands are filtered out."""
    runner.tester.params.get.return_value = [
        "cassandra-stress write duration=30m cl=QUORUM",
        "cassandra-stress write profile=/tmp/p.yaml n=1000",
    ]
    monkey = SlaIncreaseSharesDuringLoad(runner)

    assert monkey.get_cassandra_stress_write_cmds() is None


# ---------------------------------------------------------------------------
# get_cassandra_stress_definition
# ---------------------------------------------------------------------------


def test_get_stress_definition_returns_column_def_and_dataset_size(runner):
    """Returns (column_definition, dataset_size) from tester helpers."""
    monkey = SlaIncreaseSharesDuringLoad(runner)
    cmds = ["cassandra-stress write n=1000000 cl=QUORUM"]

    col_def, size = monkey.get_cassandra_stress_definition(cmds)

    assert col_def == "-col n=5 size=FIXED(64)"
    assert size == 1000000
    runner.tester.get_c_s_column_definition.assert_called_once_with(cs_cmd=cmds[0])
    runner.tester.get_data_set_size.assert_called_once_with(cmds[0])


def test_get_stress_definition_falls_back_to_default_dataset_size(runner):
    """When get_data_set_size returns 0/None, use default_data_set_size (5_000_000)."""
    runner.tester.get_data_set_size.return_value = 0
    monkey = SlaIncreaseSharesDuringLoad(runner)
    cmds = ["cassandra-stress write n=0 cl=QUORUM"]

    _, size = monkey.get_cassandra_stress_definition(cmds)

    assert size == 5_000_000


def test_get_stress_definition_custom_default(runner):
    """Custom default_data_set_size is used when get_data_set_size returns 0."""
    runner.tester.get_data_set_size.return_value = 0
    monkey = SlaIncreaseSharesDuringLoad(runner)
    cmds = ["cassandra-stress write n=0 cl=QUORUM"]

    _, size = monkey.get_cassandra_stress_definition(cmds, default_data_set_size=999)

    assert size == 999


# ---------------------------------------------------------------------------
# get_stress_params
# ---------------------------------------------------------------------------


def test_get_stress_params_raises_when_no_cmds(runner):
    """Raise UnsupportedNemesis when no usable stress commands are found."""
    runner.tester.params.get.return_value = None
    monkey = SlaIncreaseSharesDuringLoad(runner)

    with pytest.raises(UnsupportedNemesis, match="keyspace1.standard1"):
        monkey.get_stress_params()


def test_get_stress_params_returns_col_def_and_size(runner):
    """Return (column_definition, dataset_size) when everything is set up correctly."""
    monkey = SlaIncreaseSharesDuringLoad(runner)

    col_def, size = monkey.get_stress_params()

    assert col_def == "-col n=5 size=FIXED(64)"
    assert size == 1000000


# ---------------------------------------------------------------------------
# format_error_for_sla_test_and_raise
# ---------------------------------------------------------------------------


def test_format_error_does_nothing_when_no_errors():
    """No exception when all events are None."""
    SlaMonkeyBase.format_error_for_sla_test_and_raise([None, None])


def test_format_error_does_nothing_for_empty_list():
    """No exception for an empty events list."""
    SlaMonkeyBase.format_error_for_sla_test_and_raise([])


def test_format_error_raises_on_error_event():
    """Raise NemesisSubTestFailure with the error step and message."""
    event = TestStepEvent(step="Run stress", publish_event=False)
    event.add_error(["something went wrong"])

    with pytest.raises(NemesisSubTestFailure, match="Step: Run stress"):
        SlaMonkeyBase.format_error_for_sla_test_and_raise([None, event])


def test_format_error_includes_all_error_steps():
    """All failing steps appear in the exception message."""
    event_a = TestStepEvent(step="Step A", publish_event=False)
    event_a.add_error(["err A"])
    event_b = TestStepEvent(step="Step B", publish_event=False)
    event_b.add_error(["err B"])

    with pytest.raises(NemesisSubTestFailure) as exc_info:
        SlaMonkeyBase.format_error_for_sla_test_and_raise([event_a, event_b])

    msg = str(exc_info.value)
    assert "Step A" in msg
    assert "Step B" in msg


# ---------------------------------------------------------------------------
# RemoveServiceLevelMonkey
# ---------------------------------------------------------------------------


def test_remove_sl_raises_when_no_roles(runner):
    """Raise UnsupportedNemesis when tester has no pre-defined roles."""
    runner.tester.roles = []
    monkey = RemoveServiceLevelMonkey(runner)

    with pytest.raises(UnsupportedNemesis, match="Service Level and role are pre-defined"):
        monkey.disrupt()


def test_remove_sl_raises_when_roles_attr_missing(runner):
    """Raise UnsupportedNemesis when tester.roles attribute does not exist."""
    del runner.tester.roles
    monkey = RemoveServiceLevelMonkey(runner)

    with pytest.raises(UnsupportedNemesis, match="Service Level and role are pre-defined"):
        monkey.disrupt()


def test_remove_sl_drops_and_recreates_service_level(remove_sl_runner):
    """disrupt() drops the service level and re-creates it when drop succeeds."""
    with (
        patch(f"{_MODULE}.time.sleep"),
        patch(f"{_MODULE}.ServiceLevel") as mock_sl_cls,
    ):
        mock_new_sl = MagicMock()
        mock_sl_cls.return_value.create.return_value = mock_new_sl

        monkey = RemoveServiceLevelMonkey(remove_sl_runner)
        monkey.disrupt()

    role = remove_sl_runner.tester.roles[0]
    role.attached_service_level.drop.assert_called_once_with(if_exists=False)
    role.attach_service_level.assert_called_once_with(mock_new_sl)


def test_remove_sl_does_not_recreate_when_drop_fails(remove_sl_runner):
    """When drop() raises, the service level must NOT be re-created."""
    role = remove_sl_runner.tester.roles[0]
    role.attached_service_level.drop.side_effect = RuntimeError("drop failed")

    with (
        patch(f"{_MODULE}.time.sleep"),
        patch(f"{_MODULE}.ServiceLevel"),
        pytest.raises(RuntimeError, match="drop failed"),
    ):
        RemoveServiceLevelMonkey(remove_sl_runner).disrupt()

    role.attach_service_level.assert_not_called()


def test_remove_sl_sleeps_between_drop_and_recreate(remove_sl_runner):
    """Verify the 300s sleep between drop and re-create is called."""
    with (
        patch(f"{_MODULE}.time.sleep") as mock_sleep,
        patch(f"{_MODULE}.ServiceLevel"),
    ):
        RemoveServiceLevelMonkey(remove_sl_runner).disrupt()

    mock_sleep.assert_called_once_with(300)


# ---------------------------------------------------------------------------
# SlaTests-delegating monkeys  (SlaIncreaseSharesDuringLoad etc.)
# ---------------------------------------------------------------------------
#
# Pattern: mock PrometheusDBStats and SlaTests, call disrupt(), assert the
# correct SlaTests method is called with the expected arguments.
# ---------------------------------------------------------------------------

_SLA_TESTS_PATCH = f"{_MODULE}.SlaTests"
_PROMETHEUS_PATCH = f"{_MODULE}.PrometheusDBStats"

_EXPECTED_COL_DEF = "-col n=5 size=FIXED(64)"
_EXPECTED_DATASET = 1000000


@pytest.mark.parametrize(
    "monkey_cls, sla_method",
    [
        (SlaIncreaseSharesDuringLoad, "test_increase_shares_during_load"),
        (SlaDecreaseSharesDuringLoad, "test_decrease_shares_during_load"),
        (SlaReplaceUsingDetachDuringLoad, "test_replace_service_level_using_detach_during_load"),
        (SlaReplaceUsingDropDuringLoad, "test_replace_service_level_using_drop_during_load"),
        (SlaIncreaseSharesByAttachAnotherSlDuringLoad, "test_increase_shares_by_attach_another_sl_during_load"),
    ],
)
def test_sla_monkey_delegates_to_correct_sla_test_method(runner, monkey_cls, sla_method):
    """Each monkey calls exactly the expected SlaTests method."""
    with (
        patch(_PROMETHEUS_PATCH) as mock_prom_cls,
        patch(_SLA_TESTS_PATCH) as mock_sla_cls,
    ):
        mock_sla_instance = mock_sla_cls.return_value
        getattr(mock_sla_instance, sla_method).return_value = []

        monkey = monkey_cls(runner)
        monkey.disrupt()

    getattr(mock_sla_instance, sla_method).assert_called_once_with(
        tester=runner.tester,
        prometheus_stats=mock_prom_cls.return_value,
        num_of_partitions=_EXPECTED_DATASET,
        cassandra_stress_column_definition=_EXPECTED_COL_DEF,
    )


@pytest.mark.parametrize(
    "monkey_cls, sla_method",
    [
        (SlaIncreaseSharesDuringLoad, "test_increase_shares_during_load"),
        (SlaDecreaseSharesDuringLoad, "test_decrease_shares_during_load"),
    ],
)
def test_sla_monkey_raises_on_error_events(runner, monkey_cls, sla_method):
    """disrupt() raises NemesisSubTestFailure when SlaTests returns error events."""
    error_event = MagicMock()
    error_event.step = "Some step"
    error_event.__bool__ = lambda self: True
    error_event.__str__ = lambda self: "validation failed"

    with (
        patch(_PROMETHEUS_PATCH),
        patch(_SLA_TESTS_PATCH) as mock_sla_cls,
    ):
        getattr(mock_sla_cls.return_value, sla_method).return_value = [error_event]

        with pytest.raises(NemesisSubTestFailure):
            monkey_cls(runner).disrupt()


def test_sla_monkey_passes_prometheus_host(runner):
    """PrometheusDBStats is constructed with the monitoring node's external address."""
    with (
        patch(_PROMETHEUS_PATCH) as mock_prom_cls,
        patch(_SLA_TESTS_PATCH) as mock_sla_cls,
    ):
        mock_sla_cls.return_value.test_increase_shares_during_load.return_value = []

        SlaIncreaseSharesDuringLoad(runner).disrupt()

    mock_prom_cls.assert_called_once_with(host="10.0.0.1")


# ---------------------------------------------------------------------------
# SlaMaximumAllowedSlsWithMaxSharesDuringLoad
# ---------------------------------------------------------------------------


def test_maximum_sls_queries_existing_service_levels(runner):
    """Monkey queries existing SLs and passes the difference to SlaTests."""
    existing_sl_count = 3

    with (
        patch(_PROMETHEUS_PATCH) as mock_prom_cls,
        patch(_SLA_TESTS_PATCH) as mock_sla_cls,
        patch(f"{_MODULE}.ServiceLevel") as mock_sl_cls,
        patch(f"{_MODULE}.MAX_ALLOWED_SERVICE_LEVELS", 10),
    ):
        mock_sl_cls.return_value.list_all_service_levels.return_value = ["sl1"] * existing_sl_count
        mock_sla_cls.return_value.test_maximum_allowed_sls_with_max_shares_during_load.return_value = []

        SlaMaximumAllowedSlsWithMaxSharesDuringLoad(runner).disrupt()

    # MAX_ALLOWED_SERVICE_LEVELS(10) - (existing_sl_count(3) + 1) = 6
    mock_sla_cls.return_value.test_maximum_allowed_sls_with_max_shares_during_load.assert_called_once_with(
        tester=runner.tester,
        prometheus_stats=mock_prom_cls.return_value,
        num_of_partitions=_EXPECTED_DATASET,
        cassandra_stress_column_definition=_EXPECTED_COL_DEF,
        service_levels_amount=6,
    )


def test_maximum_sls_raises_when_no_capacity_remaining(runner):
    """Raise UnsupportedNemesis when all service-level slots are already occupied."""
    with (
        patch(_PROMETHEUS_PATCH),
        patch(f"{_MODULE}.ServiceLevel") as mock_sl_cls,
        patch(f"{_MODULE}.MAX_ALLOWED_SERVICE_LEVELS", 3),
    ):
        # list_all_service_levels returns 3 entries → created_service_levels = 4 → remaining = -1
        mock_sl_cls.return_value.list_all_service_levels.return_value = ["sl1", "sl2", "sl3"]

        with pytest.raises(UnsupportedNemesis, match="No remaining service-level slots"):
            SlaMaximumAllowedSlsWithMaxSharesDuringLoad(runner).disrupt()


def test_maximum_sls_raises_on_errors(runner):
    """disrupt() raises NemesisSubTestFailure when SlaTests returns error events."""
    error_event = MagicMock()
    error_event.step = "max SLs step"
    error_event.__bool__ = lambda self: True
    error_event.__str__ = lambda self: "max sls failed"

    with (
        patch(_PROMETHEUS_PATCH),
        patch(_SLA_TESTS_PATCH) as mock_sla_cls,
        patch(f"{_MODULE}.ServiceLevel") as mock_sl_cls,
        patch(f"{_MODULE}.MAX_ALLOWED_SERVICE_LEVELS", 10),
    ):
        mock_sl_cls.return_value.list_all_service_levels.return_value = []
        mock_sla_cls.return_value.test_maximum_allowed_sls_with_max_shares_during_load.return_value = [error_event]

        with pytest.raises(NemesisSubTestFailure):
            SlaMaximumAllowedSlsWithMaxSharesDuringLoad(runner).disrupt()
