"""Tests for consecutive skip tracking in execute_nemesis and the run() loop.

Includes a regression test for:
    AttributeError: 'NoneType' object has no attribute 'rsplit'
    at self.current_disruption.rsplit("-", 1)[0]
which occurred because execute_nemesis cleared self.current_disruption in its
finally block before run()'s except handler could read it via base_disruption_name.
"""

import threading
from unittest.mock import MagicMock

import pytest

from sdcm.exceptions import UnsupportedNemesis
from unit_tests.unit.nemesis.execute_nemesis import (
    CustomTestNemesis,
    SkippingTestNemesis,
    TestNemesisRunner,
    VersionNotFoundTestNemesis,
)
from unit_tests.unit.nemesis.fake_cluster import FakeTester, PARAMS


@pytest.fixture
def make_nemesis_runner(events_function_scope):
    """Factory fixture: returns a callable that builds a TestNemesisRunner from nemesis classes.

    Usage::

        runner = make_nemesis_runner(SkippingTestNemesis, CustomTestNemesis)
    """

    def _make(*nemesis_classes):
        termination_event = threading.Event()
        tester = FakeTester(params=PARAMS)
        tester.db_cluster.check_cluster_health = MagicMock()
        tester.db_cluster.test_config = MagicMock()
        runner = TestNemesisRunner(tester, termination_event)
        runner.disruptions_list = [cls(runner=runner) for cls in nemesis_classes]
        runner.interval = 0
        return runner

    return _make


def test_current_disruption_is_none_after_skip(skipping_nemesis, nemesis_runner):
    """Regression: current_disruption and target_node must be None after execute_nemesis
    even when UnsupportedNemesis is raised.

    Previously this caused AttributeError in run()'s except handler:
        self.current_disruption.rsplit("-", 1)[0]
        AttributeError: 'NoneType' object has no attribute 'rsplit'
    """
    with pytest.raises(UnsupportedNemesis):
        nemesis_runner.execute_nemesis(skipping_nemesis)

    assert nemesis_runner.current_disruption is None
    assert nemesis_runner.target_node is None


def test_consecutive_skips_incremented_on_skip(skipping_nemesis, nemesis_runner):
    """consecutive_skips must count up for each skipped execution."""
    for i in range(1, 4):
        with pytest.raises(UnsupportedNemesis):
            nemesis_runner.execute_nemesis(skipping_nemesis)
        assert nemesis_runner.consecutive_skips.get("SkippingTestNemesis") == i


def test_consecutive_skips_cleared_on_success(nemesis, skipping_nemesis, nemesis_runner):
    """consecutive_skips must be reset to empty after a successful execution."""
    with pytest.raises(UnsupportedNemesis):
        nemesis_runner.execute_nemesis(skipping_nemesis)
    assert len(nemesis_runner.consecutive_skips) == 1

    nemesis_runner.execute_nemesis(nemesis)
    assert len(nemesis_runner.consecutive_skips) == 0


def test_run_does_not_crash_on_skipped_nemesis(make_nemesis_runner):
    """Regression: run() must not crash with AttributeError when a nemesis skips.

    Reproduces the exact failure scenario from Jenkins build #40 where
    execute_nemesis cleared self.current_disruption before run()'s except
    handler tried to access self.base_disruption_name.
    """
    runner = make_nemesis_runner(SkippingTestNemesis)

    # Run with cycles_count=1 — must complete without AttributeError
    runner.run(cycles_count=1)

    assert runner.current_disruption is None
    assert runner.consecutive_skips.get("SkippingTestNemesis") == 1


def test_run_stops_and_publishes_critical_after_3_consecutive_skips(make_nemesis_runner, events_function_scope):
    """run() must stop the nemesis loop and publish a CRITICAL InfoEvent when a
    single-disruption list skips 3 or more times consecutively."""
    runner = make_nemesis_runner(SkippingTestNemesis)

    # Request 5 cycles — but the loop should break after 3 skips
    runner.run(cycles_count=5)

    assert len(runner.duration_list) == 3, (
        f"Expected loop to stop after 3 skips, but ran {len(runner.duration_list)} cycles"
    )
    critical_events = events_function_scope.get_events_by_category()["CRITICAL"]
    assert len(critical_events) == 1, f"Expected exactly 1 CRITICAL InfoEvent, got {len(critical_events)}"
    assert "SkippingTestNemesis" in critical_events[0]
    assert "3" in critical_events[0]


def test_run_continues_when_multiple_disruptions_and_one_skips(make_nemesis_runner, events_function_scope):
    """run() must NOT stop early when the disruptions list has more than one entry,
    even if one nemesis keeps skipping."""
    runner = make_nemesis_runner(SkippingTestNemesis, CustomTestNemesis)

    runner.run(cycles_count=6)

    assert len(runner.duration_list) == 6, f"Expected all 6 cycles to run, but only {len(runner.duration_list)} ran"
    critical_events = events_function_scope.get_events_by_category()["CRITICAL"]
    assert len(critical_events) == 0, "No CRITICAL InfoEvent should be published with multiple disruptions"


def test_run_resets_skips_after_success(make_nemesis_runner, events_function_scope):
    """A successful execution must clear consecutive_skips, preventing a false
    CRITICAL event even if many skips occur in total."""
    # Pattern: skip, skip, success, skip, skip, success — never reaches 3 consecutive
    runner = make_nemesis_runner(SkippingTestNemesis, SkippingTestNemesis, CustomTestNemesis)

    runner.run(cycles_count=6)

    assert len(runner.duration_list) == 6
    critical_events = events_function_scope.get_events_by_category()["CRITICAL"]
    assert len(critical_events) == 0, (
        "No CRITICAL event expected — successes reset the skip counter before it reaches 3"
    )


def test_run_stops_on_method_version_not_found(make_nemesis_runner, events_function_scope):
    """run() must also stop and publish a CRITICAL InfoEvent when the skip is caused
    by MethodVersionNotFound, not just UnsupportedNemesis."""
    runner = make_nemesis_runner(VersionNotFoundTestNemesis)

    runner.run(cycles_count=5)

    assert len(runner.duration_list) == 3, (
        f"Expected loop to stop after 3 skips, but ran {len(runner.duration_list)} cycles"
    )
    critical_events = events_function_scope.get_events_by_category()["CRITICAL"]
    assert len(critical_events) == 1
    assert "VersionNotFoundTestNemesis" in critical_events[0]
