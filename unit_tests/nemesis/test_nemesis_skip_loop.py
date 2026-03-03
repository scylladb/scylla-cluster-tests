"""
Tests for nemesis skip loop prevention.

Verifies that when nemesis are repeatedly skipped, the runner stops the
thread and publishes a CRITICAL event:
- Same nemesis skipped 3 times in a row → CRITICAL + stop
- 50 nemesis skipped in a row (regardless of name) → CRITICAL + stop
"""

from threading import Event
from unittest.mock import MagicMock, patch

from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis import NemesisRunner
from sdcm.nemesis.registry import NemesisRegistry
from sdcm.sct_events import Severity
from unit_tests.nemesis import TestBaseClass
from unit_tests.nemesis.fake_cluster import FakeTester, PARAMS


class TestNemesisRunnerForSkip(NemesisRunner):
    """Test-specific NemesisRunner with controlled call_next_nemesis behavior."""

    __test__ = False

    def __init__(self, tester_obj, termination_event, *args, nemesis_selector=None, nemesis_seed=None, **kwargs):
        super().__init__(
            tester_obj, termination_event, *args, nemesis_selector=nemesis_selector, nemesis_seed=nemesis_seed, **kwargs
        )
        self.nemesis_registry = NemesisRegistry(base_class=TestBaseClass, flag_class=TestBaseClass)
        self._call_count = 0
        self._skip_sequence = []
        self._nemesis_names = []

    def set_skip_sequence(self, sequence, nemesis_names=None):
        """Set a repeating sequence of True/False for skip behavior.

        True means the nemesis will raise UnsupportedNemesis (skip),
        False means it will succeed.
        nemesis_names: list of nemesis names corresponding to each call.
        """
        self._skip_sequence = sequence
        self._nemesis_names = nemesis_names or ["TestNemesis"] * len(sequence)
        self._call_count = 0

    def call_next_nemesis(self):
        idx = self._call_count % len(self._skip_sequence)
        self._call_count += 1
        self._current_nemesis_name = self._nemesis_names[idx % len(self._nemesis_names)]
        if self._skip_sequence[idx]:
            raise UnsupportedNemesis("nemesis not supported in this configuration")


def make_runner(skip_sequence, nemesis_names=None, params=None):
    """Helper to create a NemesisRunner with controlled skip behavior."""
    all_params = dict(PARAMS)
    if params:
        all_params.update(params)
    tester = FakeTester(params=all_params)
    termination_event = Event()
    runner = TestNemesisRunnerForSkip(tester, termination_event)
    runner.disruptions_list = [MagicMock() for _ in range(len(skip_sequence))]
    runner.set_skip_sequence(skip_sequence, nemesis_names)
    return runner, termination_event


class TestNemesisSkipLoop:
    """Tests for the skip-loop prevention in NemesisRunner.run()."""

    def test_same_nemesis_skipped_3_times_sends_critical_and_stops(self):
        """When the same nemesis is skipped 3 times in a row, a CRITICAL event is published and the thread stops."""
        runner, termination_event = make_runner(
            skip_sequence=[True],
            nemesis_names=["AlwaysSkipMonkey"],
        )

        with patch("sdcm.nemesis.InfoEvent") as mock_info_event:
            mock_event_instance = MagicMock()
            mock_info_event.return_value = mock_event_instance
            runner.run()

            # Should have published a CRITICAL event
            assert mock_info_event.called, "InfoEvent should have been created"
            call_kwargs = mock_info_event.call_args.kwargs
            assert call_kwargs["severity"] == Severity.CRITICAL
            assert "AlwaysSkipMonkey" in call_kwargs["message"]
            assert "3 times" in call_kwargs["message"]

        # Thread should have stopped (loop exited via break)
        assert runner._call_count == 3

    def test_different_nemesis_skipped_resets_same_name_counter(self):
        """When different nemesis alternate being skipped, the same-nemesis counter resets."""
        # Alternate between two nemesis names, all skipped - should NOT trigger the 3-same-in-a-row limit
        # but should eventually trigger the 50 total limit
        runner, termination_event = make_runner(
            skip_sequence=[True, True],
            nemesis_names=["NemesisA", "NemesisB"],
        )

        with patch("sdcm.nemesis.InfoEvent") as mock_info_event:
            mock_event_instance = MagicMock()
            mock_info_event.return_value = mock_event_instance
            runner.run()

            # Should have published a CRITICAL event for 50 total skips
            assert mock_info_event.called, "InfoEvent should have been created"
            call_kwargs = mock_info_event.call_args.kwargs
            assert call_kwargs["severity"] == Severity.CRITICAL
            assert "50 nemesis" in call_kwargs["message"]

        # Should have run 50 times (total consecutive skips limit)
        assert runner._call_count == 50

    def test_50_consecutive_skips_sends_critical_and_stops(self):
        """When 50 nemesis are skipped in a row (mixed names), CRITICAL event is published."""
        # 10 different nemesis names cycling, all skip
        names = [f"Nemesis{i}" for i in range(10)]
        runner, termination_event = make_runner(
            skip_sequence=[True] * 10,
            nemesis_names=names,
        )

        with patch("sdcm.nemesis.InfoEvent") as mock_info_event:
            mock_event_instance = MagicMock()
            mock_info_event.return_value = mock_event_instance
            runner.run()

            assert mock_info_event.called
            call_kwargs = mock_info_event.call_args.kwargs
            assert call_kwargs["severity"] == Severity.CRITICAL
            assert "50 nemesis" in call_kwargs["message"]

        assert runner._call_count == 50

    def test_successful_nemesis_resets_all_skip_counters(self):
        """When a nemesis succeeds after skips, all skip counters reset."""
        # Skip twice, then succeed, repeat — should never trigger any limit
        runner, termination_event = make_runner(
            skip_sequence=[True, True, False],
            nemesis_names=["NemesisA", "NemesisA", "NemesisA"],
        )

        call_limit = [0]
        original_wait = termination_event.wait

        def stop_after_9_calls(timeout=None):
            call_limit[0] += 1
            if call_limit[0] >= 9:
                termination_event.set()
            return original_wait(timeout=0)

        termination_event.wait = stop_after_9_calls

        with patch("sdcm.nemesis.InfoEvent") as mock_info_event:
            runner.run()
            # No CRITICAL event should have been published
            mock_info_event.assert_not_called()

        # All 9 calls should have been made (3 cycles of skip, skip, success)
        assert runner._call_count == 9

    def test_no_skips_runs_normally(self):
        """When no nemesis are skipped, the runner operates normally."""
        runner, termination_event = make_runner(
            skip_sequence=[False],
            nemesis_names=["NormalNemesis"],
        )

        call_limit = [0]
        original_wait = termination_event.wait

        def stop_after_3_calls(timeout=None):
            call_limit[0] += 1
            if call_limit[0] >= 3:
                termination_event.set()
            return original_wait(timeout=0)

        termination_event.wait = stop_after_3_calls

        with patch("sdcm.nemesis.InfoEvent") as mock_info_event:
            runner.run()
            mock_info_event.assert_not_called()

        assert runner._call_count == 3

    def test_skip_interval_is_zero_before_threshold(self):
        """Before reaching any threshold, skipped nemesis use interval 0 to try the next one quickly."""
        runner, termination_event = make_runner(
            skip_sequence=[True, False],
            nemesis_names=["SkipNemesis", "OkNemesis"],
        )

        wait_intervals = []
        original_wait = termination_event.wait

        def track_wait(timeout=None):
            wait_intervals.append(timeout)
            if len(wait_intervals) >= 4:
                termination_event.set()
            return original_wait(timeout=0)

        termination_event.wait = track_wait

        with patch("sdcm.nemesis.InfoEvent"):
            runner.run()

        # Pattern: skip(0), ok(interval), skip(0), ok(interval)
        assert wait_intervals[0] == 0, "Skipped nemesis should use interval 0"
        assert wait_intervals[1] == runner.interval, "Successful nemesis should use normal interval"
