"""Unit tests for kernel panic checker base class and detection logic."""

import os
import threading
import time
from unittest.mock import patch, MagicMock

import pytest

from sdcm.kernel_panic_checker import BaseKernelPanicChecker


class FakeKernelPanicChecker(BaseKernelPanicChecker):
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
    return str(tmp_path / "node-logs")


def test_detects_kernel_panic():
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker._check_for_panic(PANIC_OUTPUT) is True


def test_no_panic_in_normal_output():
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker._check_for_panic(NORMAL_BOOT_OUTPUT) is False


def test_no_panic_in_empty_output():
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker._check_for_panic("") is False


def test_case_insensitive_detection():
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker._check_for_panic("KERNEL PANIC detected") is True
    assert checker._check_for_panic("Kernel Panic in module") is True


def test_extract_panic_lines():
    checker = FakeKernelPanicChecker(node_name="node-1")
    lines = checker._extract_panic_lines(PANIC_OUTPUT)
    assert len(lines) == 1
    assert "Kernel panic - not syncing" in lines[0]


def test_extract_no_panic_lines():
    checker = FakeKernelPanicChecker(node_name="node-1")
    lines = checker._extract_panic_lines(NORMAL_BOOT_OUTPUT)
    assert lines == []


def test_saves_to_logdir(logdir):
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=logdir)
    checker._save_console_output("some console output")

    log_path = os.path.join(logdir, "console_output.log")
    assert os.path.exists(log_path)
    with open(log_path, encoding="utf-8") as fobj:
        assert fobj.read() == "some console output"


def test_creates_logdir_if_missing(logdir):
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=logdir)
    assert not os.path.exists(logdir)
    checker._save_console_output("output")
    assert os.path.exists(logdir)


def test_skips_save_if_no_logdir():
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=None)
    checker._save_console_output("output")


def test_skips_save_if_empty_output(logdir):
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=logdir)
    checker._save_console_output("")
    assert not os.path.exists(os.path.join(logdir, "console_output.log"))


def test_overwrites_on_each_save(logdir):
    checker = FakeKernelPanicChecker(node_name="node-1", logdir=logdir)
    checker._save_console_output("first")
    checker._save_console_output("second")

    log_path = os.path.join(logdir, "console_output.log")
    with open(log_path, encoding="utf-8") as fobj:
        assert fobj.read() == "second"


def test_ssh_no_host_returns_true():
    checker = FakeKernelPanicChecker(node_name="node-1", host=None)
    assert checker._check_ssh_connectivity() is True


@patch("sdcm.kernel_panic_checker.socket.create_connection")
def test_ssh_reachable_host(mock_conn):
    mock_conn.return_value.__enter__ = MagicMock()
    mock_conn.return_value.__exit__ = MagicMock(return_value=False)
    checker = FakeKernelPanicChecker(node_name="node-1", host="10.0.0.1")
    assert checker._check_ssh_connectivity() is True


@patch("sdcm.kernel_panic_checker.socket.create_connection", side_effect=OSError("refused"))
def test_ssh_unreachable_host(_mock_conn):
    checker = FakeKernelPanicChecker(node_name="node-1", host="10.0.0.1")
    assert checker._check_ssh_connectivity() is False


def test_ssh_lost_property():
    checker = FakeKernelPanicChecker(node_name="node-1", host="10.0.0.1")
    assert checker.ssh_lost is False

    checker._ssh_was_reachable = True
    checker._ssh_consecutive_failures = 3
    assert checker.ssh_lost is True


def test_ssh_lost_requires_was_reachable():
    checker = FakeKernelPanicChecker(node_name="node-1", host="10.0.0.1")
    checker._ssh_consecutive_failures = 10
    assert checker.ssh_lost is False


@patch("sdcm.kernel_panic_checker.KernelPanicEvent")
def test_detects_panic_and_stops(mock_event_cls, logdir):
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
    checker = FakeKernelPanicChecker(node_name="node-1")
    assert checker.daemon is True


def test_initial_state():
    checker = FakeKernelPanicChecker(node_name="node-1", host="10.0.0.1", logdir="/tmp/test")
    assert checker.node_name == "node-1"
    assert checker.host == "10.0.0.1"
    assert checker.logdir == "/tmp/test"
    assert not checker._panic_detected.is_set()
    assert not checker._stop_event.is_set()
    assert checker._ssh_was_reachable is False
    assert checker._ssh_consecutive_failures == 0
