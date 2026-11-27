"""Unit tests for kernel panic checker base class and detection logic."""

import os
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from sdcm.kernel_panic_checker import BaseKernelPanicChecker


class FakeKernelPanicChecker(BaseKernelPanicChecker):
    """Kernel panic checker that serves scripted console outputs, repeating the last one once exhausted."""

    provider_name = "fake"

    def __init__(self, console_outputs=None, **kwargs):
        super().__init__(**kwargs)
        self._console_outputs = list(console_outputs or [""])
        self._call_count = 0

    def _get_console_output(self) -> str:
        idx = min(self._call_count, len(self._console_outputs) - 1)
        self._call_count += 1
        return self._console_outputs[idx]

    def _get_instance_identifier(self) -> str:
        return "fake-instance-123"


NORMAL_BOOT_OUTPUT = """\
[    0.000000] Linux version 5.15.0 (gcc version 11.4.0)
[    0.000000] Command line: BOOT_IMAGE=/vmlinuz root=/dev/sda1
[    1.234567] systemd[1]: Started.
"""

PANIC_OUTPUT = """\
[    0.000000] Linux version 5.15.0 (gcc version 11.4.0)
[   42.123456] Kernel panic - not syncing: Fatal exception
[   42.123457] CPU: 0 PID: 1 Comm: systemd Not tainted
"""


@pytest.fixture
def logdir(tmp_path):
    """Provide a not-yet-created node log directory path under tmp_path."""
    return str(tmp_path / "node-logs")


def test_detects_kernel_panic():
    """A 'Kernel panic' line in the console output is detected as a panic."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker._check_for_panic(PANIC_OUTPUT) is True


def test_no_panic_in_normal_output():
    """Regular boot console output is reported as panic-free."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker._check_for_panic(NORMAL_BOOT_OUTPUT) is False


def test_no_panic_in_empty_output():
    """Empty console output is reported as panic-free."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker._check_for_panic("") is False


def test_case_insensitive_detection():
    """Panic detection matches 'kernel panic' regardless of letter case."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker._check_for_panic("KERNEL PANIC detected") is True
    assert checker._check_for_panic("Kernel Panic in module") is True


def test_extract_panic_lines():
    """Only the console lines that contain the panic message are extracted."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    lines = checker._extract_panic_lines(PANIC_OUTPUT)
    assert len(lines) == 1
    assert "Kernel panic - not syncing" in lines[0]


def test_extract_no_panic_lines():
    """Extracting panic lines from regular boot output yields an empty list."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    lines = checker._extract_panic_lines(NORMAL_BOOT_OUTPUT)
    assert lines == []


def test_saves_to_logdir(logdir):
    """Console output is written verbatim to console_output.log in the logdir."""
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=logdir)
    checker._save_console_output("some console output")

    log_path = os.path.join(logdir, "console_output.log")
    assert os.path.exists(log_path)
    with open(log_path, encoding="utf-8") as fobj:
        assert fobj.read() == "some console output"


def test_creates_logdir_if_missing(logdir):
    """Saving console output creates the logdir when it is missing."""
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=logdir)
    assert not os.path.exists(logdir)
    checker._save_console_output("output")
    assert os.path.exists(logdir)


def test_skips_save_if_no_logdir():
    """Saving console output without a logdir completes quietly."""
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=None)
    checker._save_console_output("output")


def test_skips_save_if_empty_output(logdir):
    """Empty console output leaves the logdir without a console_output.log file."""
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=logdir)
    checker._save_console_output("")
    assert not os.path.exists(os.path.join(logdir, "console_output.log"))


def test_overwrites_on_each_save(logdir):
    """Each save replaces console_output.log with the latest console output."""
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=logdir)
    checker._save_console_output("first")
    checker._save_console_output("second")

    log_path = os.path.join(logdir, "console_output.log")
    with open(log_path, encoding="utf-8") as fobj:
        assert fobj.read() == "second"


def test_ssh_no_host_returns_true():
    """SSH connectivity counts as healthy when the checker has no host to probe."""
    checker = FakeKernelPanicChecker(node_name="node-1", host=None)
    assert checker._check_ssh_connectivity() is True


@patch("sdcm.kernel_panic_checker.socket.create_connection")
def test_ssh_reachable_host(mock_conn):
    """SSH connectivity is reported when a TCP connection to the host succeeds."""
    mock_conn.return_value.__enter__ = MagicMock()
    mock_conn.return_value.__exit__ = MagicMock(return_value=False)
    checker = FakeKernelPanicChecker(node_name="node-1", host="10.0.0.1")
    assert checker._check_ssh_connectivity() is True


@patch("sdcm.kernel_panic_checker.socket.create_connection", side_effect=OSError("refused"))
def test_ssh_unreachable_host(_mock_conn):
    """SSH connectivity is reported lost when connecting to the host raises OSError."""
    checker = FakeKernelPanicChecker(node_name="node-1", host="10.0.0.1")
    assert checker._check_ssh_connectivity() is False


def test_ssh_lost_property():
    """ssh_lost turns true once a previously reachable host reaches the failure threshold."""
    checker = FakeKernelPanicChecker(node_name="node-1", host="10.0.0.1")
    assert checker.ssh_lost is False

    checker._ssh_was_reachable = True
    checker._ssh_consecutive_failures = 3
    assert checker.ssh_lost is True


def test_ssh_lost_requires_was_reachable():
    """ssh_lost stays false for a host that was never reachable, whatever the failure count."""
    checker = FakeKernelPanicChecker(node_name="node-1", host="10.0.0.1")
    checker._ssh_consecutive_failures = 10
    assert checker.ssh_lost is False


@patch("sdcm.kernel_panic_checker.KernelPanicEvent")
def test_detects_panic_and_stops(mock_event_cls, logdir):
    """The thread publishes a KernelPanicEvent and stops itself when the console shows a panic."""
    checker = FakeKernelPanicChecker(
        console_outputs=[NORMAL_BOOT_OUTPUT, PANIC_OUTPUT],
        node_name="node-1",
        logdir=logdir,
    )
    checker.CHECK_INTERVAL_SECONDS = 0.01

    checker.start()
    checker.join(timeout=5)

    assert checker._panic_detected.is_set()
    assert checker._stop_event.is_set()
    mock_event_cls.assert_called_once()
    assert "Kernel panic" in mock_event_cls.call_args[1]["message"]


def test_saves_output_every_iteration(logdir):
    """The running checker saves the console output to the logdir on its polling iterations."""
    checker = FakeKernelPanicChecker(
        console_outputs=["boot line 1", "boot line 1\nboot line 2"],
        node_name="node-1",
        logdir=logdir,
    )
    checker.CHECK_INTERVAL_SECONDS = 0.01

    def stop_after_delay():
        time.sleep(0.1)
        checker.stop()

    stopper = threading.Thread(target=stop_after_delay, daemon=True)
    stopper.start()
    checker.start()
    checker.join(timeout=5)

    log_path = os.path.join(logdir, "console_output.log")
    assert os.path.exists(log_path)


def test_stop_saves_final_output(logdir):
    """stop() saves the final console output to the logdir."""
    checker = FakeKernelPanicChecker(
        console_outputs=["final output"],
        node_name="node-1",
        logdir=logdir,
    )
    checker.stop()

    log_path = os.path.join(logdir, "console_output.log")
    assert os.path.exists(log_path)
    with open(log_path, encoding="utf-8") as fobj:
        assert fobj.read() == "final output"


def test_context_manager_starts_and_stops(logdir):
    """The context manager starts the checker thread on enter and stops it on exit."""
    checker = FakeKernelPanicChecker(
        console_outputs=["output"],
        node_name="node-1",
        logdir=logdir,
    )
    checker.CHECK_INTERVAL_SECONDS = 0.01

    with checker:
        assert checker.is_alive()

    checker.join(timeout=5)
    assert not checker.is_alive()


def test_is_daemon_thread():
    """The checker runs as a daemon thread so it lets the interpreter exit."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker.daemon is True


def test_initial_state():
    """A new checker keeps its node name, host and logdir and starts with all events cleared."""
    checker = FakeKernelPanicChecker(node_name="node-1", host="10.0.0.1", logdir="/tmp/test")
    assert checker.node_name == "node-1"
    assert checker.host == "10.0.0.1"
    assert checker.logdir == "/tmp/test"
    assert not checker._panic_detected.is_set()
    assert not checker._stop_event.is_set()
    assert not checker._suspended.is_set()
    assert checker._ssh_was_reachable is False
    assert checker._ssh_consecutive_failures == 0


@patch("sdcm.kernel_panic_checker.KernelPanicEvent")
def test_no_panic_while_suspended(mock_event_cls, logdir):
    """A suspended checker ignores panic output and publishes no KernelPanicEvent."""
    checker = FakeKernelPanicChecker(
        console_outputs=[PANIC_OUTPUT, PANIC_OUTPUT, PANIC_OUTPUT],
        node_name="node-1",
        logdir=logdir,
    )
    checker.CHECK_INTERVAL_SECONDS = 0.01
    checker.suspend()

    def stop_after_delay():
        time.sleep(0.1)
        checker.stop()

    helper = threading.Thread(target=stop_after_delay, daemon=True)
    helper.start()
    checker.start()
    checker.join(timeout=5)

    assert not checker._panic_detected.is_set()
    mock_event_cls.assert_not_called()


@patch("sdcm.kernel_panic_checker.KernelPanicEvent")
def test_panic_detected_after_resume(mock_event_cls, logdir):
    """After resume() the checker detects a panic that appears in later console output."""
    call_count = 0

    class RebootThenPanicChecker(FakeKernelPanicChecker):
        def _get_console_output(self) -> str:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return NORMAL_BOOT_OUTPUT
            return PANIC_OUTPUT

    checker = RebootThenPanicChecker(
        console_outputs=[],
        node_name="node-1",
        logdir=logdir,
    )
    checker.CHECK_INTERVAL_SECONDS = 0.01
    checker.suspend()

    def resume_after_delay():
        time.sleep(0.05)
        checker.resume()

    helper = threading.Thread(target=resume_after_delay, daemon=True)
    helper.start()
    checker.start()
    checker.join(timeout=5)

    assert checker._panic_detected.is_set()
    mock_event_cls.assert_called_once()


def test_suspend_resume_idempotent():
    """Repeated suspend() and resume() calls leave the suspended flag in the expected state."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    checker.suspend()
    checker.suspend()
    assert checker._suspended.is_set()

    checker.resume()
    assert not checker._suspended.is_set()
    checker.resume()
    assert not checker._suspended.is_set()


def test_suspended_context_manager():
    """suspended() sets the suspended flag inside the block and clears it on exit."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert not checker._suspended.is_set()

    with checker.suspended():
        assert checker._suspended.is_set()

    assert not checker._suspended.is_set()


class ScriptedSshProbe:
    """Stands in for `_check_ssh_connectivity`: returns `results` in turn, then `default`, and stops the checker
    after `limit` probes so a checker that never publishes does not run forever.
    """

    def __init__(self, checker, results, default=False, limit=100):
        self.checker = checker
        self.results = iter(results)
        self.default = default
        self.limit = limit
        self.calls = 0

    def __call__(self):
        """Return the next scripted probe result, stopping the checker once the probe limit is reached."""
        self.calls += 1
        if self.calls >= self.limit:
            self.checker._stop_event.set()
        return next(self.results, self.default)


def _run_with_ssh_probe(checker, probe):
    checker.CHECK_INTERVAL_SECONDS = 0.01
    with patch.object(checker, "_check_ssh_connectivity", side_effect=probe):
        checker.start()
        checker.join(timeout=5)
    assert not checker.is_alive()


@patch("sdcm.kernel_panic_checker.NodeUnreachableEvent")
def test_ssh_lost_publishes_critical_event_and_stops(mock_event_cls):
    """Losing SSH after it was reachable publishes NodeUnreachableEvent once the threshold is hit, when armed."""
    checker = FakeKernelPanicChecker(console_outputs=[NORMAL_BOOT_OUTPUT], node_name="loader-1", host="10.0.0.1")
    checker.fail_on_ssh_lost = True
    probe = ScriptedSshProbe(checker, [True])

    _run_with_ssh_probe(checker, probe)

    assert probe.calls == 1 + checker.SSH_LOST_CRITICAL_THRESHOLD
    mock_event_cls.assert_called_once()
    assert mock_event_cls.call_args.kwargs["node"] == "loader-1"
    assert "10.0.0.1" in mock_event_cls.call_args.kwargs["message"]


@patch("sdcm.kernel_panic_checker.NodeUnreachableEvent")
def test_ssh_lost_does_not_fail_test_by_default(mock_event_cls):
    """With fail_on_ssh_lost off by default, an SSH outage past the threshold publishes no event."""
    checker = FakeKernelPanicChecker(console_outputs=[NORMAL_BOOT_OUTPUT], node_name="db-1", host="10.0.0.1")
    probe = ScriptedSshProbe(checker, [True], limit=checker.SSH_LOST_CRITICAL_THRESHOLD * 3)

    _run_with_ssh_probe(checker, probe)

    assert checker._ssh_consecutive_failures > checker.SSH_LOST_CRITICAL_THRESHOLD
    mock_event_cls.assert_not_called()


@patch("sdcm.kernel_panic_checker.NodeUnreachableEvent")
def test_ssh_recovery_resets_failure_count(mock_event_cls):
    """A successful probe resets the failure count, so the event fires only after a full new outage."""
    checker = FakeKernelPanicChecker(console_outputs=[NORMAL_BOOT_OUTPUT], node_name="loader-1", host="10.0.0.1")
    checker.fail_on_ssh_lost = True
    almost_lost = [False] * (checker.SSH_LOST_CRITICAL_THRESHOLD - 1)
    probe = ScriptedSshProbe(checker, [True, *almost_lost, True])

    _run_with_ssh_probe(checker, probe)

    # without the reset the outages would add up and fire on the first probe after the recovery
    assert probe.calls == 1 + len(almost_lost) + 1 + checker.SSH_LOST_CRITICAL_THRESHOLD
    mock_event_cls.assert_called_once()


@patch("sdcm.kernel_panic_checker.NodeUnreachableEvent")
def test_ssh_lost_never_reachable_does_not_fail(mock_event_cls):
    """A host that was never reachable over SSH publishes no NodeUnreachableEvent."""
    checker = FakeKernelPanicChecker(console_outputs=[NORMAL_BOOT_OUTPUT], node_name="loader-1", host="10.0.0.1")
    checker.fail_on_ssh_lost = True
    probe = ScriptedSshProbe(checker, [], limit=checker.SSH_LOST_CRITICAL_THRESHOLD * 3)

    _run_with_ssh_probe(checker, probe)

    mock_event_cls.assert_not_called()


@patch("sdcm.kernel_panic_checker.NodeUnreachableEvent")
def test_ssh_lost_disarmed_while_running_does_not_fail(mock_event_cls):
    """Disarming fail_on_ssh_lost while the checker runs suppresses the NodeUnreachableEvent."""
    checker = FakeKernelPanicChecker(console_outputs=[NORMAL_BOOT_OUTPUT], node_name="loader-1", host="10.0.0.1")
    checker.fail_on_ssh_lost = True
    probe = ScriptedSshProbe(checker, [True], limit=checker.SSH_LOST_CRITICAL_THRESHOLD * 3)

    def disarm_on_first_failure():
        if probe.calls == 1:
            checker.fail_on_ssh_lost = False
        return probe()

    _run_with_ssh_probe(checker, disarm_on_first_failure)

    mock_event_cls.assert_not_called()


@patch("sdcm.kernel_panic_checker.KernelPanicEvent")
@patch("sdcm.kernel_panic_checker.NodeUnreachableEvent")
def test_kernel_panic_wins_over_ssh_lost(mock_unreachable_cls, mock_panic_cls):
    """A kernel panic seen during an SSH outage publishes KernelPanicEvent instead of NodeUnreachableEvent."""
    outputs = [NORMAL_BOOT_OUTPUT] * FakeKernelPanicChecker.SSH_LOST_CRITICAL_THRESHOLD + [PANIC_OUTPUT]
    checker = FakeKernelPanicChecker(console_outputs=outputs, node_name="loader-1", host="10.0.0.1")
    checker.fail_on_ssh_lost = True

    _run_with_ssh_probe(checker, ScriptedSshProbe(checker, [True]))

    mock_panic_cls.assert_called_once()
    mock_unreachable_cls.assert_not_called()
