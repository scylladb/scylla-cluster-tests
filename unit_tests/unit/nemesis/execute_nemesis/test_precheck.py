"""Tests for NemesisRunner.precheck_nemesis() and its integration with run().

precheck_nemesis() is evaluated once before the execution loop starts. It prunes
every nemesis whose precheck() returns a reason (or raises) from disruptions_list,
reports each exclusion via a DisruptionEvent and the Argus nemesis tab
(submit_nemesis + finalize_nemesis), and returns the list of (name, reason)
exclusions.  run() emits a single CRITICAL InfoEvent and stops when every selected
nemesis was pruned.

This is part of the execute-nemesis reporting pattern, hence its home here.
"""

import re
import threading
from unittest.mock import ANY, MagicMock

import pytest
from argus.common.enums import NemesisStatus

from unit_tests.unit.nemesis.execute_nemesis import (
    CustomTestNemesis,
    PrecheckErrorNemesis,
    PrecheckSkipNemesis,
    TestNemesisRunner,
)
from unit_tests.unit.nemesis.fake_cluster import FakeTester, PARAMS

# Fields that vary with time or identity — excluded from full-dict event assertions.
_TIMESTAMP_FIELDS = frozenset({"event_id", "event_timestamp", "begin_timestamp", "end_timestamp", "source_timestamp"})

# Expected DisruptionEvent END-phase snapshot for a clean skip reason.
SKIPPED_EVENT = {
    "base": "DisruptionEvent",
    "type": None,
    "subtype": None,
    "nemesis_name": "PrecheckSkipNemesis",
    "node": "None",
    "full_traceback": "",
    "log_level": 20,
    "errors": [],
    "publish_event": True,
    "is_skipped": True,
    "skip_reason": "static condition not met",
    "severity": "NORMAL",
    "period_type": "end",
    "subcontext": [],
}

# Expected DisruptionEvent END-phase snapshot for a precheck() exception.
# full_traceback is the real call stack — we store a canonical form here and
# reduce the actual value to the same form with a single re.sub in the test.
FAILED_EVENT = {
    "base": "DisruptionEvent",
    "type": None,
    "subtype": None,
    "nemesis_name": "PrecheckErrorNemesis",
    "node": "None",
    "full_traceback": "Traceback...RuntimeError: precheck blew up\n",
    "log_level": 20,
    "errors": ["precheck blew up"],
    "publish_event": True,
    "is_skipped": False,
    "skip_reason": "",
    "severity": "ERROR",
    "period_type": "end",
    "subcontext": [],
}


def stable(event: dict) -> dict:
    """Return the event dict with time-varying fields removed."""
    return {k: v for k, v in event.items() if k not in _TIMESTAMP_FIELDS}


def disruption_end_events(events_fixture) -> list[dict]:
    """All DisruptionEvent END-phase snapshots (stable fields only)."""
    return [
        stable(e)
        for e in events_fixture.published_events
        if e.get("base") == "DisruptionEvent" and e.get("period_type") == "end"
    ]


@pytest.fixture()
def runner(events_function_scope):
    """A TestNemesisRunner with a fresh FakeEventsDevice per test."""
    termination_event = threading.Event()
    tester = FakeTester(params=PARAMS)
    tester.db_cluster.check_cluster_health = MagicMock()
    tester.db_cluster.test_config = MagicMock()
    instance = TestNemesisRunner(tester, termination_event)
    instance.interval = 0
    return instance


@pytest.fixture()
def argus_mock(runner):
    """Wire a MagicMock Argus client into the runner's test_config."""
    mock = MagicMock()
    runner.cluster.test_config.argus_client = MagicMock(return_value=mock)
    return mock


# ---------------------------------------------------------------------------
# Pruning behaviour: disruptions_list is updated in place, excluded returned
# ---------------------------------------------------------------------------


def test_precheck_keeps_runnable_nemesis(runner):
    """A nemesis whose precheck() returns None stays in disruptions_list."""
    runner.disruptions_list = [CustomTestNemesis(runner)]

    excluded = runner.precheck_nemesis()

    assert excluded == []
    assert [n.__class__.__name__ for n in runner.disruptions_list] == ["CustomTestNemesis"]


def test_precheck_default_contract_keeps_disruptions_list_unchanged(runner):
    """Nemesis without precheck overrides keep the same disruption list."""
    runner.disruptions_list = [CustomTestNemesis(runner), CustomTestNemesis(runner)]
    before = runner._disruption_list_names

    excluded = runner.precheck_nemesis()

    assert excluded == []
    assert runner._disruption_list_names == before


def test_precheck_receives_representative_node_not_target_node(runner):
    """precheck() receives the representative cluster node before target selection."""
    nemesis = CustomTestNemesis(runner)
    nemesis.precheck = MagicMock(return_value=None)
    runner.target_node = MagicMock()
    runner.disruptions_list = [nemesis]

    excluded = runner.precheck_nemesis()

    assert excluded == []
    nemesis.precheck.assert_called_once_with(node=runner.cluster.nodes[0])
    assert nemesis.precheck.call_args.kwargs["node"] is not runner.target_node


@pytest.mark.parametrize(
    "nemesis_cls, reason",
    [
        pytest.param(PrecheckSkipNemesis, "static condition not met", id="skip"),
        pytest.param(PrecheckErrorNemesis, "precheck blew up", id="error"),
    ],
)
def test_precheck_excludes_nemesis(runner, nemesis_cls, reason):
    """A nemesis whose precheck() returns a reason or raises is pruned and returned."""
    runner.disruptions_list = [nemesis_cls(runner)]

    excluded = runner.precheck_nemesis()

    assert runner.disruptions_list == []
    assert excluded == [(nemesis_cls.__name__, reason)]


def test_precheck_partitions_mixed_list(runner):
    """Runnable nemesis stay, excluded ones (reason or exception) are pruned."""
    runner.disruptions_list = [
        CustomTestNemesis(runner),
        PrecheckSkipNemesis(runner),
        PrecheckErrorNemesis(runner),
    ]

    excluded = runner.precheck_nemesis()

    assert [n.__class__.__name__ for n in runner.disruptions_list] == ["CustomTestNemesis"]
    assert {name for name, _ in excluded} == {"PrecheckSkipNemesis", "PrecheckErrorNemesis"}


# ---------------------------------------------------------------------------
# Duplicate deduplication (nemesis_multiply_factor > 1)
# ---------------------------------------------------------------------------


def test_duplicate_instances_all_pruned_reported_once(runner, argus_mock, events_function_scope):
    """When disruptions_list contains N copies of the same excluded class
    (nemesis_multiply_factor > 1), the Argus report, excluded list, and
    DisruptionEvent are each emitted exactly once — not N times."""
    runner.disruptions_list = [PrecheckSkipNemesis(runner)] * 3

    excluded = runner.precheck_nemesis()

    assert runner.disruptions_list == []
    assert excluded == [("PrecheckSkipNemesis", "static condition not met")]
    argus_mock.submit_nemesis.assert_called_once()
    argus_mock.finalize_nemesis.assert_called_once()
    assert len(disruption_end_events(events_function_scope)) == 1


def test_duplicate_mixed_list_all_pruned_reported_once(runner, argus_mock):
    """Multiple copies of two different excluded classes produce two reports, not more."""
    runner.disruptions_list = [
        PrecheckSkipNemesis(runner),
        PrecheckSkipNemesis(runner),
        PrecheckErrorNemesis(runner),
        PrecheckErrorNemesis(runner),
    ]

    excluded = runner.precheck_nemesis()

    assert runner.disruptions_list == []
    assert len(excluded) == 2
    assert {name for name, _ in excluded} == {"PrecheckSkipNemesis", "PrecheckErrorNemesis"}
    assert argus_mock.submit_nemesis.call_count == 2
    assert argus_mock.finalize_nemesis.call_count == 2


def test_duplicate_instances_with_runnable_are_all_kept(runner):
    """Multiple copies of a runnable nemesis are ALL kept in disruptions_list."""
    runner.disruptions_list = [CustomTestNemesis(runner)] * 3

    excluded = runner.precheck_nemesis()

    assert excluded == []
    assert len(runner.disruptions_list) == 3


# ---------------------------------------------------------------------------
# DisruptionEvent reporting: skip -> SKIPPED/NORMAL, exception -> FAILED/ERROR
# ---------------------------------------------------------------------------


def test_skip_reason_emits_skipped_disruption_event(runner, events_function_scope):
    """A reason-returning precheck() emits one SKIPPED DisruptionEvent (END phase)
    with the full expected shape."""
    runner.disruptions_list = [PrecheckSkipNemesis(runner)]

    runner.precheck_nemesis()

    assert disruption_end_events(events_function_scope) == [SKIPPED_EVENT]


def test_exception_emits_failed_disruption_event(runner, events_function_scope):
    """A precheck() exception emits one FAILED DisruptionEvent (END phase).
    full_traceback is collapsed to 'Traceback...RuntimeError: <msg>' before
    comparison so the assertion is precise without encoding absolute paths."""
    runner.disruptions_list = [PrecheckErrorNemesis(runner)]

    runner.precheck_nemesis()

    end_events = disruption_end_events(events_function_scope)
    assert len(end_events) == 1
    event = {
        **end_events[0],
        "full_traceback": re.sub(
            r"Traceback.*?(RuntimeError:)", r"Traceback...\1", end_events[0]["full_traceback"], flags=re.DOTALL
        ),
    }
    assert event == FAILED_EVENT


# ---------------------------------------------------------------------------
# Argus nemesis tab (submit_nemesis + finalize_nemesis)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "nemesis_cls, description, status, message",
    [
        pytest.param(
            PrecheckSkipNemesis,
            "A nemesis whose precheck() returns a skip reason.",
            NemesisStatus.SKIPPED,
            "static condition not met",
            id="skip",
        ),
        pytest.param(
            PrecheckErrorNemesis,
            "A nemesis whose precheck() raises an exception.",
            NemesisStatus.FAILED,
            ANY,  # full traceback string — asserted in test_exception_emits_failed_disruption_event
            id="error",
        ),
    ],
)
def test_exclusion_reports_to_argus(runner, argus_mock, nemesis_cls, description, status, message):
    """Each pruned nemesis is submitted to and finalized in the Argus nemesis tab,
    using the representative precheck node and the matching status."""
    runner.disruptions_list = [nemesis_cls(runner)]

    runner.precheck_nemesis()

    argus_mock.submit_nemesis.assert_called_once_with(
        name=nemesis_cls.__name__,
        class_name="TestNemesisRunner",
        start_time=ANY,
        target_name="Node1",
        target_ip="127.0.0.1",
        target_shards=8,
        description=description,
    )
    argus_mock.finalize_nemesis.assert_called_once_with(
        name=nemesis_cls.__name__,
        start_time=ANY,
        status=status,
        message=message,
    )
    assert (
        argus_mock.submit_nemesis.call_args.kwargs["start_time"]
        == argus_mock.finalize_nemesis.call_args.kwargs["start_time"]
    )


def test_two_exclusions_report_both_to_argus(runner, argus_mock):
    """Two excluded nemesis produce two submit + finalize pairs with the right statuses."""
    runner.disruptions_list = [PrecheckSkipNemesis(runner), PrecheckErrorNemesis(runner)]

    runner.precheck_nemesis()

    assert argus_mock.submit_nemesis.call_count == 2
    finalize_by_name = {c.kwargs["name"]: c.kwargs["status"] for c in argus_mock.finalize_nemesis.call_args_list}
    assert finalize_by_name == {
        "PrecheckSkipNemesis": NemesisStatus.SKIPPED,
        "PrecheckErrorNemesis": NemesisStatus.FAILED,
    }


def test_no_exclusion_does_not_touch_argus(runner, argus_mock):
    """When no nemesis is excluded, Argus is not touched."""
    runner.disruptions_list = [CustomTestNemesis(runner)]

    runner.precheck_nemesis()

    argus_mock.submit_nemesis.assert_not_called()
    argus_mock.finalize_nemesis.assert_not_called()


def test_exception_argus_message_contains_traceback(runner, argus_mock):
    """finalize_nemesis for a broken precheck() passes the full traceback as the
    message, not just the bare exception string — matching the normal FAILED path."""
    runner.disruptions_list = [PrecheckErrorNemesis(runner)]

    runner.precheck_nemesis()

    message = argus_mock.finalize_nemesis.call_args.kwargs["message"]
    assert "precheck blew up" in message
    assert "Traceback" in message
    assert "precheck" in message


# ---------------------------------------------------------------------------
# run() integration: empty-rotation CRITICAL and clean stop
# ---------------------------------------------------------------------------


def test_run_all_pruned_emits_critical_and_stops(runner, events_function_scope):
    """When every selected nemesis is pruned, run() publishes one CRITICAL
    InfoEvent and stops without executing any nemesis."""
    runner.disruptions_list = [PrecheckSkipNemesis(runner)]

    runner.run(cycles_count=5)

    assert runner.disruptions_list == []
    critical_events = events_function_scope.get_events_by_category()["CRITICAL"]
    assert len(critical_events) == 1, f"Expected 1 CRITICAL InfoEvent, got {len(critical_events)}: {critical_events}"
    assert "PrecheckSkipNemesis" in critical_events[0]
    assert runner.duration_list == []


def test_run_partial_prune_runs_survivors_without_critical(runner, events_function_scope):
    """A partial prune keeps the runnable nemesis, runs it, and emits no CRITICAL event."""
    runner.disruptions_list = [PrecheckSkipNemesis(runner), CustomTestNemesis(runner)]

    runner.run(cycles_count=1)

    assert [n.__class__.__name__ for n in runner.disruptions_list] == ["CustomTestNemesis"]
    assert events_function_scope.get_events_by_category()["CRITICAL"] == []
    assert len(runner.duration_list) == 1
