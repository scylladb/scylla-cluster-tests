"""Tests for consecutive skip tracking in execute_nemesis and the run() loop.

Includes a regression test for:
    AttributeError: 'NoneType' object has no attribute 'rsplit'
    at self.current_disruption.rsplit("-", 1)[0]
which occurred because execute_nemesis cleared self.current_disruption in its
finally block before run()'s except handler could read it via base_disruption_name.
"""

import threading

import pytest
from unittest.mock import MagicMock

from sdcm.exceptions import UnsupportedNemesis
from unit_tests.nemesis.execute_nemesis import (
    SkippingTestNemesis,
    TestNemesisRunner,
)
from unit_tests.nemesis.fake_cluster import FakeTester, PARAMS


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


def test_run_does_not_crash_on_skipped_nemesis(events):
    """Regression: run() must not crash with AttributeError when a nemesis skips.

    Reproduces the exact failure scenario from Jenkins build #40 where
    execute_nemesis cleared self.current_disruption before run()'s except
    handler tried to access self.base_disruption_name.
    """
    termination_event = threading.Event()
    tester = FakeTester(params=PARAMS)
    tester.db_cluster.check_cluster_health = MagicMock()
    tester.db_cluster.test_config = MagicMock()
    runner = TestNemesisRunner(tester, termination_event, nemesis_selector="flag_a")
    runner.disruptions_list = [SkippingTestNemesis(runner=runner)]

    # Run with cycles_count=1 — must complete without AttributeError
    runner.run(cycles_count=1)

    assert runner.current_disruption is None
    assert runner.consecutive_skips.get("SkippingTestNemesis") == 1
