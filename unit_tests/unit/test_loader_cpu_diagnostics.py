# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""Unit tests for the loader CPU diagnostics sampler and its log streaming (SCT-601)."""

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from sdcm.loader_cpu_diagnostics_setup import (
    MAX_LOG_SIZE_MB,
    REMOTE_LOG_PATH,
    REMOTE_SCRIPT_PATH,
    SAMPLER_SCRIPT_NAME,
    SERVICE_NAME,
    THREAD_EVERY_N_SAMPLES,
    TOP_THREADS,
    LoaderCpuDiagnosticsSetup,
)
from sdcm.utils.remote_logger import LoaderCpuFileLogger, SSHGeneralSystemdLogger

SAMPLER_SCRIPT_PATH = Path(__file__).parent.parent.parent / "data_dir" / SAMPLER_SCRIPT_NAME


@pytest.fixture(name="node")
def fixture_node():
    """A loader node whose remoter records the commands the setup runs on it."""
    node = MagicMock()
    node.name = "loader-node-1"
    return node


def _sampler_setup_script(node) -> str:
    """The shell script the setup passed to remoter.sudo()."""
    assert node.remoter.sudo.call_args, "the setup ran no sudo command"
    return node.remoter.sudo.call_args.args[0]


# --- sampler service installation ---


def test_install_uploads_the_sampler_and_starts_the_service(node):
    LoaderCpuDiagnosticsSetup.install(node)

    node.install_package.assert_called_once_with("sysstat", ignore_status=True)
    src, dst = node.remoter.send_files.call_args.args
    assert Path(src).name == SAMPLER_SCRIPT_NAME
    assert Path(src).is_file(), f"the sampler is not shipped in data_dir: {src}"
    assert dst == f"/tmp/{SAMPLER_SCRIPT_NAME}"

    script = _sampler_setup_script(node)
    assert f"install -m 0755 /tmp/{SAMPLER_SCRIPT_NAME} {REMOTE_SCRIPT_PATH}" in script
    assert f"ExecStart={REMOTE_SCRIPT_PATH} " in script
    assert f"-o {REMOTE_LOG_PATH}" in script
    assert f"-T {TOP_THREADS} -m {MAX_LOG_SIZE_MB}" in script
    # restart and not start: a reused loader may already run an older sampler
    assert f"systemctl restart {SERVICE_NAME}.service" in script
    assert f"systemctl enable {SERVICE_NAME}.service" in script


def test_install_without_per_thread_disables_thread_sampling(node):
    LoaderCpuDiagnosticsSetup.install(node, per_thread=False)

    assert "-t 0 " in _sampler_setup_script(node)


def test_install_with_per_thread_samples_threads_every_nth_sample(node):
    LoaderCpuDiagnosticsSetup.install(node, per_thread=True)

    assert f"-t {THREAD_EVERY_N_SAMPLES} " in _sampler_setup_script(node)


def test_install_exec_start_is_a_single_line(node):
    """The unit file is written through a dedent()ed heredoc: a wrapped ExecStart would break it."""
    LoaderCpuDiagnosticsSetup.install(node, per_thread=True)

    exec_start = [line for line in _sampler_setup_script(node).splitlines() if "ExecStart=" in line]
    assert len(exec_start) == 1
    assert exec_start[0].rstrip().endswith(str(MAX_LOG_SIZE_MB))


def test_install_pins_the_sysstat_output_format(node):
    """A 12h locale shifts every mpstat/pidstat column by one field."""
    LoaderCpuDiagnosticsSetup.install(node)

    assert "Environment=S_TIME_FORMAT=ISO LC_ALL=C" in _sampler_setup_script(node)


def test_install_warns_when_the_service_is_not_sampling(node):
    """`systemctl restart` succeeds as soon as the process is forked, even if it dies right after."""
    node.remoter.run.return_value.ok = False

    LoaderCpuDiagnosticsSetup.install(node)

    assert any("is not active" in str(call) for call in node.log.warning.call_args_list)
    assert any("systemctl is-active" in str(call) for call in node.remoter.run.call_args_list)


def test_install_keeps_going_when_sysstat_is_unavailable(node):
    """sysstat is best effort - the sampler falls back to /proc, diagnostics must not fail a setup."""
    node.install_package.side_effect = Exception("no such package")
    node.remoter.run.return_value.ok = False

    LoaderCpuDiagnosticsSetup.install(node)

    assert f"systemctl restart {SERVICE_NAME}.service" in _sampler_setup_script(node)


# --- log streaming ---


def test_loader_cpu_logger_reads_the_path_the_sampler_writes():
    assert LoaderCpuFileLogger.REMOTE_LOG_PATH == REMOTE_LOG_PATH


def test_loader_cpu_logger_remote_files_do_not_collide_with_the_journal_logger():
    """Two SSH loggers on one node must not share the remote pid file.

    Keyed on the SCT pid alone, the second logger overwrote the pid file of the first one and
    stopping either killed the other one's remote command.
    """
    node = MagicMock()
    journal_logger = SSHGeneralSystemdLogger(node, "/tmp/system.log")
    cpu_logger = LoaderCpuFileLogger(node, "/tmp/loader-cpu.log")

    assert journal_logger._remote_file_id != cpu_logger._remote_file_id
    # the system log logger keeps its historical file names
    assert journal_logger._remote_file_id == str(os.getpid())
    assert cpu_logger._remote_file_id == f"{os.getpid()}_loader_cpu"


def test_loader_cpu_logger_resumes_at_the_local_offset(tmp_path):
    """The parent tails from byte 0, so every reconnect would duplicate the whole log."""
    target = tmp_path / "loader-cpu.log"
    target.write_bytes(b"x" * 4096)

    logger = LoaderCpuFileLogger(MagicMock(), str(target))

    assert logger._logger_cmd_template.endswith(f"-c +{4096 + 1}")
    # recomputed per call, the parent caches it
    target.write_bytes(b"x" * 8192)
    assert logger._logger_cmd_template.endswith(f"-c +{8192 + 1}")


def test_loader_cpu_logger_starts_from_the_beginning_without_a_local_file(tmp_path):
    logger = LoaderCpuFileLogger(MagicMock(), str(tmp_path / "missing.log"))

    assert logger._logger_cmd_template.endswith("-c +1")


def test_loader_cpu_logger_stop_kills_the_tail_by_command_line(tmp_path):
    """A process-group kill takes down the SSH channel it is issued over and wedges the remoter.

    That is what left four workers blocked after run 8597ebfd finished, and `concurrent.futures`
    joins its workers at interpreter exit, so the run hung for 6.5 hours.
    """
    node = MagicMock()
    logger = LoaderCpuFileLogger(node, str(tmp_path / "loader-cpu.log"))

    logger.stop()

    command = node.remoter.run.call_args.args[0]
    assert command == f"sudo pkill -f 'tail -f {REMOTE_LOG_PATH}'"
    assert "kill -9 -" not in command
    assert node.remoter.run.call_args.kwargs["timeout"] == LoaderCpuFileLogger.STOP_TIMEOUT


def test_loader_cpu_logger_stop_is_idempotent(tmp_path):
    """Teardown stops it twice; the second call must not touch a possibly wedged connection."""
    node = MagicMock()
    logger = LoaderCpuFileLogger(node, str(tmp_path / "loader-cpu.log"))

    logger.stop()
    logger.stop()

    assert node.remoter.run.call_count == 1


def test_loader_cpu_logger_stop_survives_an_unreachable_loader(tmp_path):
    """Teardown must not fail over a diagnostics logger."""
    node = MagicMock()
    node.remoter.run.side_effect = Exception("connection lost")
    logger = LoaderCpuFileLogger(node, str(tmp_path / "loader-cpu.log"))

    logger.stop()  # must not raise

    assert logger._termination_event.is_set()


# --- the sampler script itself ---


def test_sampler_script_has_valid_bash_syntax():
    assert subprocess.run(["bash", "-n", str(SAMPLER_SCRIPT_PATH)], capture_output=True, check=False).returncode == 0


def test_sampler_script_matches_processes_by_name_only():
    """`pgrep -f` would match the sampler itself: its own pattern is part of its command line."""
    code_lines = [line for line in SAMPLER_SCRIPT_PATH.read_text().splitlines() if not line.lstrip().startswith("#")]
    assert not [line for line in code_lines if "pgrep -f" in line]


def test_sampler_script_exits_on_termination_signals():
    """A TERM handler that does not exit leaves the loop running until systemd SIGKILLs it."""
    assert "trap 'exit 0' TERM INT" in SAMPLER_SCRIPT_PATH.read_text()


def test_sampler_script_pins_the_sysstat_time_format():
    """Belt and braces with the systemd unit: a 12h locale shifts every column by one field."""
    script = SAMPLER_SCRIPT_PATH.read_text()

    assert "export S_TIME_FORMAT=ISO" in script
    assert "export LC_ALL=C" in script


def test_sampler_script_does_not_hardcode_the_thread_sort_column():
    """%wait only exists since sysstat 11.5, so a fixed field index sorts by the wrong metric."""
    script = SAMPLER_SCRIPT_PATH.read_text()

    assert "thread_cpu_sort_key" in script
    assert "sort -k10" not in script
