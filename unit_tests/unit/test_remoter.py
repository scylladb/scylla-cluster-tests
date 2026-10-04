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
# Copyright (c) 2020 ScyllaDB

import os
from types import SimpleNamespace
from typing import Optional
from unittest.mock import patch

import pytest

# from parameterized import parameterized

from sdcm.remote import (
    RemoteLibSSH2CmdRunner,
    RetryableNetworkException,
    shell_script_cmd,
)
from sdcm.remote.base import CommandRunner, Result
from sdcm.remote.libssh2_client.exceptions import FailedToRunCommand, OpenChannelTimeout
from sdcm.remote.remote_file import remote_file
from sdcm.remote.remote_libssh_cmd_runner import MINICLOUD_CHANNEL_TIMEOUT_ALERT_THRESHOLD
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent


class TestSudoAndRunShellScript:
    @classmethod
    def setup_class(cls) -> None:
        class _Runner(CommandRunner):
            def run(self, cmd, *_, **__):
                self.command_to_run = cmd

            def _create_connection(self):
                pass

            def is_up(self, timeout: Optional[float] = None) -> bool:
                return True

        cls.remoter_cls = _Runner

    def test_sudo_non_root(self):
        remoter = self.remoter_cls("localhost", user="joe")
        remoter.sudo("true")
        assert remoter.command_to_run == "sudo true"

    def test_shell_script_cmd(self):
        assert shell_script_cmd("true") == 'bash -cxe "true"'


class TestRemoteFile:
    @classmethod
    def setup_class(cls) -> None:
        class _Runner:
            sf_data = sf_src = sf_dst = rf_src = rf_dst = None
            hostname = "localhost"
            command_to_run = ""
            rf_data = "new"

            def run(self, cmd, *_, **__):
                self.command_to_run = cmd
                if cmd == "mktemp":
                    return Result(stdout="temporary\n")
                elif 'stat -c "%U:%G"' in cmd:
                    return Result(stdout="bentsi:bentsi")
                elif 'stat -c "%a"' in cmd:
                    return Result(stdout="644")
                return Result(stdout="", stderr="")

            def send_files(self, src: str, dst: str, *_, **__) -> bool:
                with open(src, encoding="utf-8") as fobj:
                    self.sf_data = fobj.read()
                self.sf_src = src
                self.sf_dst = dst
                return True

            def receive_files(self, src: str, dst: str, *_, **__) -> bool:
                with open(dst, "w", encoding="utf-8") as fobj:
                    fobj.write(self.rf_data)
                self.rf_src = src
                self.rf_dst = dst
                return True

            def sudo(self, cmd, *_, **__):
                return self.run(cmd)

        cls.remoter_cls = _Runner

    def test_remote_file(self):
        remoter = self.remoter_cls()
        some_file = "/some/path/some.file"
        with remote_file(
            remoter=remoter, remote_path=some_file, preserve_ownership=False, preserve_permissions=False
        ) as fobj:
            fobj.write("test data")
        assert remoter.rf_src == some_file
        assert remoter.sf_dst == "temporary"
        assert remoter.rf_dst.startswith("/tmp/sct")
        assert remoter.rf_dst.endswith(os.path.basename(some_file))
        assert remoter.rf_dst == remoter.sf_src
        assert remoter.sf_data == "test data"
        assert not os.path.exists(remoter.sf_src)
        assert remoter.command_to_run == f"bash -cxe \"cat 'temporary' > '{some_file}'\nrm 'temporary'\n\""

    def test_remote_file_preserve_ownership(self):
        remoter = self.remoter_cls()
        some_file = "/some/path/some.file"
        with remote_file(
            remoter=remoter, remote_path=some_file, preserve_ownership=True, preserve_permissions=False, sudo=True
        ) as fobj:
            fobj.write("test data")
            assert remoter.command_to_run == f'stat -c "%U:%G" {some_file}'
        assert f"chown bentsi:bentsi {some_file}" == remoter.command_to_run

    def test_remote_file_preserve_permissions(self):
        remoter = self.remoter_cls()
        some_file = "/some/path/some.file"
        with remote_file(
            remoter=remoter, remote_path=some_file, preserve_ownership=False, preserve_permissions=True, sudo=True
        ) as fobj:
            fobj.write("test data")
            assert remoter.command_to_run == f'stat -c "%a" {some_file}'
        assert f"chmod 644 {some_file}" == remoter.command_to_run

    def test_remote_file_preserve_readonly(self):
        remoter = self.remoter_cls()
        some_file = "/some/path/some.file"
        with remote_file(
            remoter=remoter, remote_path=some_file, preserve_ownership=False, preserve_permissions=True, sudo=True
        ) as fobj:
            fobj.write(remoter.rf_data)
            assert remoter.command_to_run == f'stat -c "%a" {some_file}'

        assert remoter.rf_src == some_file
        assert remoter.rf_dst.startswith("/tmp/sct")
        assert remoter.rf_dst.endswith(os.path.basename(some_file))
        assert remoter.sf_data is None

    def test_remote_file_empty_mktemp_raises(self):
        """If 'mktemp' comes back with an empty result, remote_file() must raise clearly
        instead of proceeding with dst="" into a 300s retry loop that always fails."""

        class _EmptyMktempRunner(self.remoter_cls):
            def run(self, cmd, *_, **__):
                if cmd == "mktemp":
                    return Result(stdout="")
                return super().run(cmd, *_, **__)

        remoter = _EmptyMktempRunner()
        some_file = "/some/path/some.file"
        with pytest.raises(RuntimeError, match="mktemp.*empty"):
            with remote_file(
                remoter=remoter, remote_path=some_file, preserve_ownership=False, preserve_permissions=False
            ) as fobj:
                fobj.write("test data")


@pytest.fixture
def _clean_minicloud_timeout_counters():
    def reset():
        RemoteLibSSH2CmdRunner._minicloud_channel_timeouts.clear()
        RemoteLibSSH2CmdRunner._minicloud_channel_timeout_alerted = False

    reset()
    yield
    reset()


def _record_minicloud_timeouts(times, hostname="10.0.0.1", minicloud=True):
    runner = SimpleNamespace(hostname=hostname)
    with (
        patch("sdcm.utils.minicloud.endpoint.is_minicloud_active", return_value=minicloud),
        patch("sdcm.remote.remote_libssh_cmd_runner.TestFrameworkEvent") as event,
    ):
        for _ in range(times):
            RemoteLibSSH2CmdRunner._record_minicloud_channel_timeout(runner)
    return event


def test_minicloud_channel_timeout_alert_activates_from_test_config(_clean_minicloud_timeout_counters):
    """The remoter has no params of its own, so it must read them off TestConfig.

    A yaml-only setup turns minicloud on through the minicloud_endpoint_url param with nothing
    in the environment, so an env-only check would leave the alert silent for the whole run.
    """
    tester = SimpleNamespace(params={"minicloud_endpoint_url": "http://localhost:5000"})
    runner = SimpleNamespace(hostname="10.0.0.7")
    with (
        patch.dict(os.environ, {}, clear=True),  # no SCT_MINICLOUD_ENDPOINT_URL anywhere
        patch("sdcm.test_config.TestConfig") as test_config,
        patch.object(TestFrameworkEvent, "publish_or_dump", autospec=True) as publish,
    ):
        test_config.return_value.tester_obj.return_value = tester
        for _ in range(MINICLOUD_CHANNEL_TIMEOUT_ALERT_THRESHOLD):
            RemoteLibSSH2CmdRunner._record_minicloud_channel_timeout(runner)

    assert publish.call_count == 1


def test_minicloud_channel_timeout_alert_survives_a_missing_tester(_clean_minicloud_timeout_counters):
    """TestConfig has no tester before setUp runs - the hook must not explode there."""
    runner = SimpleNamespace(hostname="10.0.0.8")
    with (
        patch.dict(os.environ, {}, clear=True),
        patch("sdcm.test_config.TestConfig") as test_config,
        patch.object(TestFrameworkEvent, "publish_or_dump", autospec=True) as publish,
    ):
        test_config.return_value.tester_obj.return_value = None
        for _ in range(MINICLOUD_CHANNEL_TIMEOUT_ALERT_THRESHOLD):
            RemoteLibSSH2CmdRunner._record_minicloud_channel_timeout(runner)

    assert publish.call_count == 0


def test_minicloud_channel_timeout_alert_counts_every_guest_together(_clean_minicloud_timeout_counters):
    """Alerting is based on the cluster-wide timeout total across all guests."""
    hosts = [f"10.0.0.{index}" for index in range(1, 9)]
    with (
        patch("sdcm.utils.minicloud.endpoint.is_minicloud_active", return_value=True),
        patch.object(TestFrameworkEvent, "publish_or_dump", autospec=True) as publish,
    ):
        for _ in range(2):  # 16 timeouts, no single guest above 2
            for host in hosts:
                RemoteLibSSH2CmdRunner._record_minicloud_channel_timeout(SimpleNamespace(hostname=host))

    message = publish.call_args.args[0].message
    assert publish.call_count == 1
    assert "10 SSH channel timeouts" in message
    assert "10.0.0.1=2" in message


def test_minicloud_channel_timeout_alert_silent_outside_minicloud(_clean_minicloud_timeout_counters):
    event = _record_minicloud_timeouts(MINICLOUD_CHANNEL_TIMEOUT_ALERT_THRESHOLD * 2, minicloud=False)
    assert event.call_count == 0


def test_minicloud_channel_timeout_alert_fires_once_with_a_real_event(_clean_minicloud_timeout_counters):
    runner = SimpleNamespace(hostname="10.0.0.9")
    with (
        patch("sdcm.utils.minicloud.endpoint.is_minicloud_active", return_value=True),
        patch.object(TestFrameworkEvent, "publish_or_dump", autospec=True) as publish,
    ):
        for _ in range(MINICLOUD_CHANNEL_TIMEOUT_ALERT_THRESHOLD * 3):
            RemoteLibSSH2CmdRunner._record_minicloud_channel_timeout(runner)

    assert publish.call_count == 1
    event = publish.call_args.args[0]
    assert event.severity is Severity.WARNING
    assert "10.0.0.9" in event.message
    assert "minicloud_scylla_reserve_memory" in event.message


def test_minicloud_channel_timeout_alert_counts_a_wrapped_open_channel_timeout(_clean_minicloud_timeout_counters):
    """Ensure a wrapped OpenChannelTimeout is counted and retried."""
    runner = SimpleNamespace(
        hostname="10.0.0.5",
        exception_retryable=RemoteLibSSH2CmdRunner.exception_retryable,
        _is_error_retryable=lambda _: False,
    )
    runner._record_minicloud_channel_timeout = lambda: RemoteLibSSH2CmdRunner._record_minicloud_channel_timeout(runner)
    wrapped = FailedToRunCommand(Result(), OpenChannelTimeout("Failed to open channel in 15 seconds"))

    with patch("sdcm.utils.minicloud.endpoint.is_minicloud_active", return_value=True):
        with pytest.raises(RetryableNetworkException):
            RemoteLibSSH2CmdRunner._run_on_retryable_exception(runner, wrapped, new_session=True, suppress_errors=True)

    assert RemoteLibSSH2CmdRunner._minicloud_channel_timeouts == {"10.0.0.5": 1}
