"""Unit tests for kernel panic checker base class and detection logic."""

import os
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from sdcm.kernel_panic_checker import BaseKernelPanicChecker

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
    """Node log directory under tmp_path. It is not created."""
    return str(tmp_path / "node-logs")


class FakeKernelPanicChecker(BaseKernelPanicChecker):
    """Checker that returns console_outputs in turn, then repeats the last one."""

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
        """Count the probe, stop the checker at the limit, and return the next scripted result."""
        self.calls += 1
        if self.calls >= self.limit:
            self.checker._stop_event.set()
        return next(self.results, self.default)


def run_with_ssh_probe(checker, probe):
    """Run the checker with `probe` as its SSH check until it stops, and assert it has stopped."""
    checker.CHECK_INTERVAL_SECONDS = 0.01
    with patch.object(checker, "_check_ssh_connectivity", side_effect=probe):
        checker.start()
        checker.join(timeout=5)
    assert not checker.is_alive()


def expected_ssh_lost_message(checker, host):
    """The NodeUnreachableEvent message published once SSH has been lost for SSH_LOST_CRITICAL_THRESHOLD checks."""
    failures = checker.SSH_LOST_CRITICAL_THRESHOLD
    return (
        f"SSH port {checker.SSH_CHECK_PORT} on {host} is unreachable for {failures} "
        f"consecutive checks (at least {failures * checker.CHECK_INTERVAL_SECONDS}s), "
        "the node is considered dead"
    )


def test_detects_kernel_panic():
    """_check_for_panic is True when the output contains a kernel panic."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker._check_for_panic(PANIC_OUTPUT) is True


def test_no_panic_in_normal_output():
    """_check_for_panic is False for a normal boot log."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker._check_for_panic(NORMAL_BOOT_OUTPUT) is False


def test_no_panic_in_empty_output():
    """_check_for_panic is False for empty output."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker._check_for_panic("") is False


def test_case_insensitive_detection():
    """_check_for_panic matches "kernel panic" in any letter case."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker._check_for_panic("KERNEL PANIC detected") is True
    assert checker._check_for_panic("Kernel Panic in module") is True


def test_extract_panic_lines():
    """_extract_panic_lines returns the lines that mention a kernel panic."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    lines = checker._extract_panic_lines(PANIC_OUTPUT)
    assert len(lines) == 1
    assert "Kernel panic - not syncing" in lines[0]


def test_extract_no_panic_lines():
    """_extract_panic_lines returns an empty list for a normal boot log."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    lines = checker._extract_panic_lines(NORMAL_BOOT_OUTPUT)
    assert lines == []


def test_saves_to_logdir(logdir):
    """_save_console_output writes the output to console_output.log in logdir."""
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=logdir)
    checker._save_console_output("some console output")

    log_path = os.path.join(logdir, "console_output.log")
    assert os.path.exists(log_path)
    with open(log_path, encoding="utf-8") as fobj:
        assert fobj.read() == "some console output"


def test_creates_logdir_if_missing(logdir):
    """_save_console_output creates logdir when it does not exist."""
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=logdir)
    assert not os.path.exists(logdir)
    checker._save_console_output("output")
    assert os.path.exists(logdir)


def test_skips_save_if_no_logdir():
    """_save_console_output does nothing without a logdir."""
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=None)
    checker._save_console_output("output")


def test_skips_save_if_empty_output(logdir):
    """_save_console_output writes nothing for empty output."""
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=logdir)
    checker._save_console_output("")
    assert not os.path.exists(os.path.join(logdir, "console_output.log"))


def test_overwrites_on_each_save(logdir):
    """_save_console_output keeps only the latest output."""
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=logdir)
    checker._save_console_output("first")
    checker._save_console_output("second")

    log_path = os.path.join(logdir, "console_output.log")
    with open(log_path, encoding="utf-8") as fobj:
        assert fobj.read() == "second"


def test_ssh_no_host_returns_true():
    """_check_ssh_connectivity is True when no host is set."""
    checker = FakeKernelPanicChecker(node_name="node-1", host=None)
    assert checker._check_ssh_connectivity() is True


@patch("sdcm.kernel_panic_checker.socket.create_connection")
def test_ssh_reachable_host(mock_conn):
    """_check_ssh_connectivity is True when the TCP connect succeeds."""
    mock_conn.return_value.__enter__ = MagicMock()
    mock_conn.return_value.__exit__ = MagicMock(return_value=False)
    checker = FakeKernelPanicChecker(node_name="node-1", host="10.0.0.1")
    assert checker._check_ssh_connectivity() is True


@patch("sdcm.kernel_panic_checker.socket.create_connection", side_effect=OSError("refused"))
def test_ssh_unreachable_host(_mock_conn):
    """_check_ssh_connectivity is False when the TCP connect raises OSError."""
    checker = FakeKernelPanicChecker(node_name="node-1", host="10.0.0.1")
    assert checker._check_ssh_connectivity() is False


def test_ssh_lost_property():
    """ssh_lost is True only after SSH was reachable and failures accumulated."""
    checker = FakeKernelPanicChecker(node_name="node-1", host="10.0.0.1")
    assert checker.ssh_lost is False

    checker._ssh_was_reachable = True
    checker._ssh_consecutive_failures = 3
    assert checker.ssh_lost is True


def test_ssh_lost_requires_was_reachable():
    """ssh_lost stays False when SSH was never reachable."""
    checker = FakeKernelPanicChecker(node_name="node-1", host="10.0.0.1")
    checker._ssh_consecutive_failures = 10
    assert checker.ssh_lost is False


@patch("sdcm.kernel_panic_checker.KernelPanicEvent")
def test_detects_panic_and_stops(mock_event_cls, logdir):
    """Run publishes a KernelPanicEvent on panic and stops the checker."""
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
    mock_event_cls.assert_called_once_with(
        node="node-1",
        message="Kernel panic detected in console log for fake-instance-123: "
        "[   42.123456] Kernel panic - not syncing: Fatal exception",
    )


def test_saves_output_every_iteration(logdir):
    """Run saves the console output to console_output.log."""
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
    """Stop saves the last console output before the checker exits."""
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
    """The context manager starts the checker and stops it on exit."""
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
    """The checker thread is a daemon, so it never blocks interpreter exit."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker.daemon is True


def test_initial_state():
    """A new checker keeps its constructor arguments and starts with clear flags."""
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
    """Run publishes no panic event while the checker is suspended."""
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
    """A panic seen after resume publishes a KernelPanicEvent."""
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
    mock_event_cls.assert_called_once_with(
        node="node-1",
        message="Kernel panic detected in console log for fake-instance-123: "
        "[   42.123456] Kernel panic - not syncing: Fatal exception",
    )


def test_suspend_resume_idempotent():
    """Suspend and resume can each be called repeatedly."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    checker.suspend()
    checker.suspend()
    assert checker._suspended.is_set()

    checker.resume()
    assert not checker._suspended.is_set()
    checker.resume()
    assert not checker._suspended.is_set()


def test_suspended_context_manager():
    """suspended() sets the flag inside the block and clears it after."""
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert not checker._suspended.is_set()

    with checker.suspended():
        assert checker._suspended.is_set()

    assert not checker._suspended.is_set()


@patch("sdcm.kernel_panic_checker.NodeUnreachableEvent")
def test_ssh_lost_publishes_critical_event_and_stops(mock_event_cls):
    """Losing SSH on a fail_on_ssh_lost node publishes NodeUnreachableEvent and stops the checker."""
    checker = FakeKernelPanicChecker(console_outputs=[NORMAL_BOOT_OUTPUT], node_name="loader-1", host="10.0.0.1")
    checker.fail_on_ssh_lost = True
    probe = ScriptedSshProbe(checker, [True])

    run_with_ssh_probe(checker, probe)

    assert probe.calls == 1 + checker.SSH_LOST_CRITICAL_THRESHOLD
    mock_event_cls.assert_called_once_with(node="loader-1", message=expected_ssh_lost_message(checker, "10.0.0.1"))


@patch("sdcm.kernel_panic_checker.NodeUnreachableEvent")
def test_ssh_lost_does_not_fail_test_by_default(mock_event_cls):
    """Losing SSH without fail_on_ssh_lost counts the failures but publishes no event."""
    checker = FakeKernelPanicChecker(console_outputs=[NORMAL_BOOT_OUTPUT], node_name="db-1", host="10.0.0.1")
    probe = ScriptedSshProbe(checker, [True], limit=checker.SSH_LOST_CRITICAL_THRESHOLD * 3)

    run_with_ssh_probe(checker, probe)

    assert checker._ssh_consecutive_failures > checker.SSH_LOST_CRITICAL_THRESHOLD
    mock_event_cls.assert_not_called()


@patch("sdcm.kernel_panic_checker.NodeUnreachableEvent")
def test_ssh_recovery_resets_failure_count(mock_event_cls):
    """SSH recovery resets the failure count, so an earlier outage does not add to a later one."""
    checker = FakeKernelPanicChecker(console_outputs=[NORMAL_BOOT_OUTPUT], node_name="loader-1", host="10.0.0.1")
    checker.fail_on_ssh_lost = True
    almost_lost = [False] * (checker.SSH_LOST_CRITICAL_THRESHOLD - 1)
    probe = ScriptedSshProbe(checker, [True, *almost_lost, True])

    run_with_ssh_probe(checker, probe)

    # without the reset the outages would add up and fire on the first probe after the recovery
    assert probe.calls == 1 + len(almost_lost) + 1 + checker.SSH_LOST_CRITICAL_THRESHOLD
    mock_event_cls.assert_called_once_with(node="loader-1", message=expected_ssh_lost_message(checker, "10.0.0.1"))


@patch("sdcm.kernel_panic_checker.NodeUnreachableEvent")
def test_ssh_lost_never_reachable_does_not_fail(mock_event_cls):
    """SSH that was never reachable publishes no NodeUnreachableEvent."""
    checker = FakeKernelPanicChecker(console_outputs=[NORMAL_BOOT_OUTPUT], node_name="loader-1", host="10.0.0.1")
    checker.fail_on_ssh_lost = True
    probe = ScriptedSshProbe(checker, [], limit=checker.SSH_LOST_CRITICAL_THRESHOLD * 3)

    run_with_ssh_probe(checker, probe)

    mock_event_cls.assert_not_called()


@patch("sdcm.kernel_panic_checker.NodeUnreachableEvent")
def test_ssh_lost_disarmed_while_running_does_not_fail(mock_event_cls):
    """Disarming fail_on_ssh_lost during an outage suppresses NodeUnreachableEvent."""
    checker = FakeKernelPanicChecker(console_outputs=[NORMAL_BOOT_OUTPUT], node_name="loader-1", host="10.0.0.1")
    checker.fail_on_ssh_lost = True
    probe = ScriptedSshProbe(checker, [True], limit=checker.SSH_LOST_CRITICAL_THRESHOLD * 3)

    def disarm_on_first_failure():
        if probe.calls == 1:
            checker.fail_on_ssh_lost = False
        return probe()

    run_with_ssh_probe(checker, disarm_on_first_failure)

    mock_event_cls.assert_not_called()


@patch("sdcm.kernel_panic_checker.KernelPanicEvent")
@patch("sdcm.kernel_panic_checker.NodeUnreachableEvent")
def test_kernel_panic_wins_over_ssh_lost(mock_unreachable_cls, mock_panic_cls):
    """A kernel panic is published instead of NodeUnreachableEvent when both conditions trigger."""
    outputs = [NORMAL_BOOT_OUTPUT] * FakeKernelPanicChecker.SSH_LOST_CRITICAL_THRESHOLD + [PANIC_OUTPUT]
    checker = FakeKernelPanicChecker(console_outputs=outputs, node_name="loader-1", host="10.0.0.1")
    checker.fail_on_ssh_lost = True

    run_with_ssh_probe(checker, ScriptedSshProbe(checker, [True]))

    mock_panic_cls.assert_called_once_with(
        node="loader-1",
        message="Kernel panic detected in console log for fake-instance-123: "
        "[   42.123456] Kernel panic - not syncing: Fatal exception",
    )
    mock_unreachable_cls.assert_not_called()
