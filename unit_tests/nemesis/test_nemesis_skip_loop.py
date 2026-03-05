"""
Tests for nemesis skip loop prevention.

Verifies that when nemesis are repeatedly skipped, the runner stops the
thread and publishes a CRITICAL event:
- Single nemesis skipped 3 times → CRITICAL + stop
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

    def set_skip_sequence(self, sequence):
        """Set a repeating sequence of True/False for skip behavior.

        True means the nemesis will raise UnsupportedNemesis (skip),
        False means it will succeed.
        """
        self._skip_sequence = sequence
        self._call_count = 0

    def call_next_nemesis(self):
        idx = self._call_count % len(self._skip_sequence)
        self._call_count += 1
        self.current_disruption = f"TestDisruption-{self._call_count:04d}"
        if self._skip_sequence[idx]:
            raise UnsupportedNemesis("nemesis not supported in this configuration")


def make_runner(skip_sequence, num_disruptions=None, params=None):
    """Helper to create a NemesisRunner with controlled skip behavior."""
    all_params = dict(PARAMS)
    if params:
        all_params.update(params)
    tester = FakeTester(params=all_params)
    termination_event = Event()
    runner = TestNemesisRunnerForSkip(tester, termination_event)
    if num_disruptions is None:
        num_disruptions = len(skip_sequence)
    runner.disruptions_list = [MagicMock() for _ in range(num_disruptions)]
    runner.set_skip_sequence(skip_sequence)
    return runner, termination_event


class TestNemesisSkipLoop:
    """Tests for the skip-loop prevention in NemesisRunner.run()."""

    def test_single_nemesis_skipped_3_times_sends_critical_and_stops(self):
        """When only 1 nemesis is configured and skipped 3 times, a CRITICAL event is published and the thread stops."""
        runner, _ = make_runner(skip_sequence=[True], num_disruptions=1)

        with patch("sdcm.nemesis.InfoEvent") as mock_info_event:
            mock_event_instance = MagicMock()
            mock_info_event.return_value = mock_event_instance
            runner.run()

            assert mock_info_event.called, "InfoEvent should have been created"
            call_kwargs = mock_info_event.call_args.kwargs
            assert call_kwargs["severity"] == Severity.CRITICAL
            assert "3 times" in call_kwargs["message"]

        assert runner._call_count == 3

    def test_multiple_nemesis_skipped_3_times_does_not_trigger_single_check(self):
        """When multiple nemesis are configured, the 3-skip single-nemesis check does not apply."""
        runner, termination_event = make_runner(skip_sequence=[True], num_disruptions=3)

        call_limit = [0]
        original_wait = termination_event.wait

        def stop_after_calls(timeout=None):
            call_limit[0] += 1
            if call_limit[0] >= 5:
                termination_event.set()
            return original_wait(timeout=0)

        termination_event.wait = stop_after_calls

        with patch("sdcm.nemesis.InfoEvent") as mock_info_event:
            runner.run()
            mock_info_event.assert_not_called()

        assert runner._call_count == 5

    def test_50_consecutive_skips_sends_critical_and_stops(self):
        """When 50 nemesis are skipped in a row, CRITICAL event is published."""
        runner, _ = make_runner(skip_sequence=[True] * 10, num_disruptions=10)

        with patch("sdcm.nemesis.InfoEvent") as mock_info_event:
            mock_event_instance = MagicMock()
            mock_info_event.return_value = mock_event_instance
            runner.run()

            assert mock_info_event.called
            call_kwargs = mock_info_event.call_args.kwargs
            assert call_kwargs["severity"] == Severity.CRITICAL
            assert "50 nemesis" in call_kwargs["message"]

        assert runner._call_count == 50

    def test_successful_nemesis_resets_skip_counter(self):
        """When a nemesis succeeds after skips, the skip counter resets."""
        runner, termination_event = make_runner(
            skip_sequence=[True, True, False], num_disruptions=1
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
            mock_info_event.assert_not_called()

        assert runner._call_count == 9

    def test_no_skips_runs_normally(self):
        """When no nemesis are skipped, the runner operates normally."""
        runner, termination_event = make_runner(skip_sequence=[False], num_disruptions=1)

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
        runner, termination_event = make_runner(skip_sequence=[True, False], num_disruptions=2)

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
