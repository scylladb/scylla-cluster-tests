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
import getpass
import threading
from typing import Union, Optional
from logging import getLogger
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from invoke.exceptions import UnexpectedExit

# from parameterized import parameterized

from sdcm.remote import (
    RemoteLibSSH2CmdRunner,
    RemoteCmdRunner,
    LocalCmdRunner,
    RetryableNetworkException,
    SSHConnectTimeoutError,
    shell_script_cmd,
)
from sdcm.remote.kubernetes_cmd_runner import KubernetesCmdRunner
from sdcm.remote.base import CommandRunner, Result
from sdcm.remote.remote_file import remote_file
from sdcm.cluster_k8s import KubernetesCluster


class FakeKluster(KubernetesCluster):
    k8s_server_url = None

    def __init__(self, k8s_server_url):
        self.k8s_server_url = k8s_server_url

    def deploy(self):
        pass

    def create_kubectl_config(self):
        pass

    def create_token_update_thread(self):
        pass

    def deploy_node_pool(self, pool, wait_till_ready=True) -> None:
        pass

    def upgrade_kubernetes_platform(self, pod_objects, use_additional_scylla_nodepool):
        pass


def generate_all_commands_with_all_options():
    all_commands_with_all_options = []
    for ip in ["::1", "127.0.0.1"]:
        for cmd in [
            "echo 0",
            "echo 0; false",
            "adasdasdasd",
            "/bin/bash -c \"printf 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\\n%.0s' {1..100};\"",
        ]:
            for verbose_value in [False, True]:
                for ignore_status_value in [False, True]:
                    for new_session_value in [False, True]:
                        for retry_value in [1, 2]:
                            for timeout_value in [None, 5]:
                                for remoter_type in [RemoteCmdRunner, RemoteLibSSH2CmdRunner, KubernetesCmdRunner]:
                                    all_commands_with_all_options.append(
                                        (
                                            remoter_type,
                                            ip,
                                            cmd,
                                            verbose_value,
                                            ignore_status_value,
                                            new_session_value,
                                            retry_value,
                                            timeout_value,
                                        )
                                    )
    return all_commands_with_all_options


ALL_COMMANDS_WITH_ALL_OPTIONS = generate_all_commands_with_all_options()


class TestRemoteCmdRunners:
    """A class that use LocalCmdRunner to get example of result and runs tests against
      RemoteLibSSH2CmdRunner and RemoteCmdRunner, to make sure that they produce result that matches
      result of LocalCmdRunner

    To be ran manually, user under which test is ran have to have
      ~/.ssh/scylla_test_id_ed25519 key in it's ~/.ssh/authorized_keys
    """

    log = getLogger()
    key_file = "~/.ssh/scylla_test_id_ed25519"

    @staticmethod
    def _create_and_run_twice_in_same_thread(remoter_type, key_file, stmt, kwargs, paramiko_thread_results):
        if issubclass(remoter_type, (RemoteCmdRunner, RemoteLibSSH2CmdRunner)):
            remoter = remoter_type(hostname="127.0.0.1", user=getpass.getuser(), key_file=key_file)
        else:
            remoter = KubernetesCmdRunner(
                FakeKluster("http://127.0.0.1:8001"),
                pod_image="fake-pod-image",
                pod_name="sct-cluster-dc-1-kind-0",
                container="scylla",
                namespace="scylla",
            )
        try:
            result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # noqa: BLE001
            result = exc
        paramiko_thread_results.append(result)
        try:
            result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # noqa: BLE001
            result = exc
        paramiko_thread_results.append(result)
        remoter.stop()

    @staticmethod
    def _create_and_run_in_same_thread(remoter_type, host, key_file, stmt, kwargs, paramiko_thread_results):
        if issubclass(remoter_type, (RemoteCmdRunner, RemoteLibSSH2CmdRunner)):
            remoter = remoter_type(hostname=host, user=getpass.getuser(), key_file=key_file)
        else:
            remoter = KubernetesCmdRunner(
                FakeKluster("http://127.0.0.1:8001"),
                pod_image="fake-pod-image",
                pod_name="sct-cluster-dc-1-kind-0",
                container="scylla",
                namespace="scylla",
            )
        try:
            result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # noqa: BLE001
            result = exc
        paramiko_thread_results.append(result)
        remoter._reconnect()
        try:
            result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # noqa: BLE001
            result = exc
        paramiko_thread_results.append(result)
        remoter.stop()

    @staticmethod
    def _create_and_run_in_separate_thread(remoter, stmt, kwargs, paramiko_thread_results):
        try:
            result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # noqa: BLE001
            result = exc
        paramiko_thread_results.append(result)
        remoter._reconnect()
        try:
            result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # noqa: BLE001
            result = exc
        paramiko_thread_results.append(result)
        remoter.stop()

    @staticmethod
    def _compare_results(
        expected, result, stmt, kwargs, fields_to_compare=("stdout", "stderr", "command", "exited", "ok")
    ):
        expected_bucket = {"__class__": type(expected).__name__}
        result_bucket = {"__class__": type(result).__name__}

        if isinstance(expected, RetryableNetworkException):
            expected = expected.original
        elif isinstance(expected, SSHConnectTimeoutError):
            expected = object()
        if isinstance(expected, Exception):
            if hasattr(expected, "result"):
                expected = expected.result
        for attr_name in fields_to_compare:
            expected_bucket[attr_name] = getattr(expected, attr_name, None)
        if isinstance(result, RetryableNetworkException):
            result = result.original
        elif isinstance(result, SSHConnectTimeoutError):
            result = object()
        if isinstance(result, Exception):
            if hasattr(result, "result"):
                result = result.result
        for attr_name in fields_to_compare:
            attr_value = getattr(result, attr_name, None)
            if attr_name in ["stderr"] and attr_value is not None and attr_value[:5] == "bash:":
                # libssh2 by default runs bash, while paramiko is more specific, it runs /bin/bash
                #  as result, when paramiko gets bash error it gets output '/bin/bash: asdas is not defined'
                #  while libssh gets 'bash: asdas is not defined'
                attr_value = "/bin/bash:" + attr_value[5:]
            result_bucket[attr_name] = attr_value
        assert expected_bucket == result_bucket, (
            f"\nRunning command:\n{stmt}\n"
            f"With options: {kwargs!s}\n"
            f"Resulted in receiving {type(result).__name__}:\n"
            f"--------------- START ---------------\n"
            f"{result!s}\n"
            f"--------------- END ---------------\n"
            f"While expect to get {type(expected).__name__}:\n"
            f"--------------- START ---------------\n"
            f"{expected!s}\n"
            f"--------------- END ---------------\n"
        )

    @staticmethod
    def _run_parallel(thread_count, thread_body, args, kwargs):
        threads = []
        for _ in range(thread_count):
            threads.append(threading.Thread(target=thread_body, daemon=False, args=args, kwargs=kwargs))
        for future in threads:
            future.start()
        for future in threads:
            future.join()

    # @parameterized.expand(ALL_COMMANDS_WITH_ALL_OPTIONS)
    @pytest.mark.skip(reason="To be ran manually")
    def test_run_in_mainthread(
        self,
        remoter_type,
        host: str,
        stmt: str,
        verbose: bool,
        ignore_status: bool,
        new_session: bool,
        retry: int,
        timeout: Union[float, None],
    ):
        kwargs = {
            "verbose": verbose,
            "ignore_status": ignore_status,
            "new_session": new_session,
            "retry": retry,
            "timeout": timeout,
        }
        try:
            expected = LocalCmdRunner().run(stmt, **kwargs)
        except Exception as exc:  # noqa: BLE001
            expected = exc

        if issubclass(remoter_type, (RemoteCmdRunner, RemoteLibSSH2CmdRunner)):
            remoter = remoter_type(hostname=host, user=getpass.getuser(), key_file=self.key_file)
        else:
            remoter = KubernetesCmdRunner(
                FakeKluster("http://127.0.0.1:8001"),
                pod_image="fake-pod-image",
                pod_name="sct-cluster-dc-1-kind-0",
                container="scylla",
                namespace="scylla",
            )
        try:
            result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # noqa: BLE001
            result = exc
        remoter._reconnect()
        try:
            result2 = remoter.run(stmt, **kwargs)
        except Exception as exc:  # noqa: BLE001
            result2 = exc
        remoter.stop()

        self._compare_results(expected, result, stmt=stmt, kwargs=kwargs)
        self._compare_results(expected, result2, stmt=stmt, kwargs=kwargs)

    # @parameterized.expand(ALL_COMMANDS_WITH_ALL_OPTIONS)
    @pytest.mark.skip(reason="To be ran manually")
    def test_create_and_run_in_same_thread(
        self,
        remoter_type,
        host: str,
        stmt: str,
        verbose: bool,
        ignore_status: bool,
        new_session: bool,
        retry: int,
        timeout: Union[float, None],
    ):
        kwargs = {
            "verbose": verbose,
            "ignore_status": ignore_status,
            "new_session": new_session,
            "retry": retry,
            "timeout": timeout,
        }
        self.log.info(repr({stmt: stmt, **kwargs}))
        try:
            expected = LocalCmdRunner().run(stmt, **kwargs)
        except Exception as exc:  # noqa: BLE001
            expected = exc

        paramiko_thread_results = []
        self._run_parallel(
            3,
            thread_body=self._create_and_run_in_same_thread,
            args=(remoter_type, host, self.key_file, stmt, kwargs, paramiko_thread_results),
            kwargs={},
        )

        for paramiko_result in paramiko_thread_results:
            self._compare_results(expected, paramiko_result, stmt=stmt, kwargs=kwargs)

    # @parameterized.expand(ALL_COMMANDS_WITH_ALL_OPTIONS)
    @pytest.mark.skip(reason="To be ran manually")
    def test_create_and_run_in_separate_thread(
        self,
        remoter_type,
        host: str,
        stmt: str,
        verbose: bool,
        ignore_status: bool,
        new_session: bool,
        retry: int,
        timeout: Union[float, None],
    ):
        kwargs = {
            "verbose": verbose,
            "ignore_status": ignore_status,
            "new_session": new_session,
            "retry": retry,
            "timeout": timeout,
        }
        self.log.info(repr({stmt: stmt, **kwargs}))
        try:
            expected = LocalCmdRunner().run(stmt, **kwargs)
        except Exception as exc:  # noqa: BLE001
            expected = exc

        # Paramiko fails too often when it is invoked like that, that is why it is not in the test

        if issubclass(remoter_type, (RemoteCmdRunner, RemoteLibSSH2CmdRunner)):
            remoter = remoter_type(hostname=host, user=getpass.getuser(), key_file=self.key_file)
        else:
            remoter = KubernetesCmdRunner(
                FakeKluster("http://127.0.0.1:8001"),
                pod_image="fake-pod-image",
                pod_name="sct-cluster-dc-1-kind-0",
                container="scylla",
                namespace="scylla",
            )

        libssh2_thread_results = []

        self._run_parallel(
            3,
            thread_body=self._create_and_run_in_separate_thread,
            args=(remoter, stmt, kwargs, libssh2_thread_results),
            kwargs={},
        )

        for libssh2_result in libssh2_thread_results:
            self.log.error(str(libssh2_result))
            self._compare_results(expected, libssh2_result, stmt=stmt, kwargs=kwargs)

    # @parameterized.expand([
    #     (
    #         KubernetesCmdRunner,
    #         "/bin/bash -c \"printf 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    #         "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    #         "AAA\\n%.0s' {1..100};\""
    #     ),
    #     (
    #         RemoteCmdRunner,
    #         "/bin/bash -c \"printf 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    #         "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    #         "AA\\n%.0s' {1..100};\""
    #     ),
    #     (
    #             KubernetesCmdRunner,
    #             "/bin/bash -c \"printf 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    #             "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    #             "AA\\n%.0s' {1..100};\""
    #     )
    # ])
    @pytest.mark.skip(reason="To be ran manually")
    def test_load_1000_threads(self, remoter_type, stmt: str):
        kwargs = {"verbose": True, "ignore_status": False, "new_session": True, "retry": 2}
        self.log.info(repr({stmt: stmt, **kwargs}))
        try:
            expected = LocalCmdRunner().run(stmt, **kwargs)
        except Exception as exc:  # noqa: BLE001
            expected = exc

        libssh2_thread_results = []
        self._run_parallel(
            1000,
            thread_body=self._create_and_run_in_same_thread,
            args=(remoter_type, self.key_file, stmt, kwargs, libssh2_thread_results),
            kwargs={},
        )

        for libssh2_result in libssh2_thread_results:
            self.log.error(str(libssh2_result))
            self._compare_results(expected, libssh2_result, stmt=stmt, kwargs=kwargs)

    # @parameterized.expand([
    #     (RemoteLibSSH2CmdRunner, "export | grep SSH_CONNECTION ; false", True),
    #     (RemoteCmdRunner, "export | grep SSH_CONNECTION ; false", True),
    #     (KubernetesCmdRunner, "export | grep SSH_CONNECTION ; false", True),
    #
    #     (RemoteLibSSH2CmdRunner, "export | grep SSH_CONNECTION ; true", True),
    #     (RemoteCmdRunner, "export | grep SSH_CONNECTION ; true", True),
    #     (KubernetesCmdRunner, "export | grep SSH_CONNECTION ; true", True),
    #
    #     (RemoteLibSSH2CmdRunner, "export | grep SSH_CONNECTION ; false", False),
    #     (RemoteCmdRunner, "export | grep SSH_CONNECTION ; false", False),
    #     (KubernetesCmdRunner, "export | grep SSH_CONNECTION ; false", False),
    #
    #     (RemoteLibSSH2CmdRunner, "export | grep SSH_CONNECTION ; true", False),
    #     (RemoteCmdRunner, "export | grep SSH_CONNECTION ; true", False),
    #     (KubernetesCmdRunner, "export | grep SSH_CONNECTION ; true", False)
    # ])
    @pytest.mark.skip(reason="To be ran manually")
    def test_context_changing(self, remoter_type, stmt: str, change_context: bool):
        kwargs = {"verbose": True, "ignore_status": True, "timeout": 10, "change_context": change_context}
        self.log.info(repr({stmt: stmt, **kwargs}))
        paramiko_thread_results = []
        self._create_and_run_twice_in_same_thread(remoter_type, self.key_file, stmt, kwargs, paramiko_thread_results)
        if change_context and paramiko_thread_results[0].ok:
            assert paramiko_thread_results[0].stdout != paramiko_thread_results[1].stdout
        else:
            assert paramiko_thread_results[0].stdout == paramiko_thread_results[1].stdout


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

    def test_sudo_root(self):
        remoter = self.remoter_cls("localhost", user="root")
        remoter.run("true")
        assert remoter.command_to_run == "true"

    def test_sudo_non_root(self):
        remoter = self.remoter_cls("localhost", user="joe")
        remoter.sudo("true")
        assert remoter.command_to_run == "sudo true"

    def test_shell_script_cmd(self):
        assert shell_script_cmd("true") == 'bash -cxe "true"'


class _FakeRemoteFileRunner:
    sf_data = sf_src = sf_dst = rf_src = rf_dst = None
    hostname = "localhost"
    command_to_run = ""
    rf_data = "new"

    def __init__(self, exists: bool = True):
        self.exists = exists
        self.commands = []

    def run(self, cmd, *_, **__):
        self.command_to_run = cmd
        self.commands.append(cmd)
        if cmd == "mktemp":
            return Result(stdout="temporary\n")
        elif cmd.startswith("test -e"):
            return Result(stdout="exists\n" if self.exists else "")
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
        assert self.exists, "must not copy a missing file"
        with open(dst, "w", encoding="utf-8") as fobj:
            fobj.write(self.rf_data)
        self.rf_src = src
        self.rf_dst = dst
        return True

    def sudo(self, cmd, *_, **__):
        return self.run(cmd)


def test_remote_file_missing_target_is_created():
    """A file the image doesn't ship (e.g. io_properties.yaml on an image without an io preset) must be created, not fail on stat."""
    remoter = _FakeRemoteFileRunner(exists=False)
    some_file = "/etc/scylla.d/io_properties.yaml"
    with remote_file(remoter=remoter, remote_path=some_file, sudo=True) as fobj:
        assert fobj.getvalue() == ""
        fobj.write("disks: []")
    assert remoter.sf_data == "disks: []"
    assert not any(cmd.startswith(("stat", "chown", "chmod")) for cmd in remoter.commands)


def test_remote_file_probe_quotes_the_path():
    """A path with a space must be probed as one path, or an existing file reads as missing and is overwritten."""
    remoter = _FakeRemoteFileRunner()
    with remote_file(remoter=remoter, remote_path="/tmp/config copy.yaml") as fobj:
        assert fobj.getvalue() == "new"
    assert remoter.commands[0] == "test -e '/tmp/config copy.yaml' && echo exists || true"


def test_remote_file_probe_failure_raises():
    """A remoter failure on the probe must raise, not read as a missing file that is then overwritten."""
    remoter = _FakeRemoteFileRunner()
    failure = UnexpectedExit(Result(command="test -e", exited=255))
    with (
        patch.object(remoter, "run", side_effect=failure),
        pytest.raises(UnexpectedExit),
        remote_file(remoter=remoter, remote_path="/some/path/some.file"),
    ):
        pass


class TestRemoteFile:
    remoter_cls = _FakeRemoteFileRunner

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


@pytest.mark.parametrize(
    "stderr,expected",
    [
        pytest.param("Connection closed by 10.142.15.202 port 22\r\n", True, id="connection-closed"),
        pytest.param(
            "kex_exchange_identification: read: Connection reset by peer\r\nConnection reset by 10.142.15.202 port 22",
            True,
            id="kex-reset",
        ),
        pytest.param("ssh: connect to host 10.142.15.202 port 22: Connection refused\r\n", True, id="sshd-down"),
        pytest.param("scylla-test@10.0.0.1: Permission denied (publickey).", False, id="auth-failure"),
        pytest.param('rsync: link_stat "/tmp/x" failed: No such file or directory (2)', False, id="missing-file"),
    ],
)
def test_copy_error_retryable_on_connection_refusal(stderr, expected):
    """Sshd refusing a new connection is transient, so a file copy is retried; other errors are not."""
    assert RemoteLibSSH2CmdRunner._is_copy_error_retryable(stderr) is expected


@pytest.mark.parametrize("remoter_type", [RemoteCmdRunner, RemoteLibSSH2CmdRunner])
def test_receive_files_retries_when_sshd_refuses_the_connection(remoter_type, tmp_path):
    """A refused rsync falls back to scp, and a refused scp retries the copy, with either SSH transport.

    rsync and scp run through LocalCmdRunner, which raises invoke's exceptions, not the libssh2 ones.
    """
    refused = UnexpectedExit(Result(command="copy", exited=255, stderr="Connection closed by 10.0.0.1 port 22"))
    copied = Result(command="scp", exited=0)
    runner = remoter_type(hostname="10.0.0.1", user="scylla-test", key_file="/dev/null")

    with (
        patch("sdcm.remote.remote_base.LocalCmdRunner") as local_runner,
        patch("sdcm.utils.decorators.time.sleep") as sleep,
        patch.object(remoter_type, "use_rsync", return_value=True),
    ):
        # 1st attempt: rsync and scp refused; 2nd attempt: rsync refused, scp copies
        local_runner.return_value.run.side_effect = [refused, refused, refused, copied]
        runner.receive_files(src="/tmp/system.log", dst=str(tmp_path))

    assert local_runner.return_value.run.call_count == 4
    sleep.assert_called_once()


class _RefusingConnection:
    """A libssh2 connection whose host answers, but rejects the key."""

    def __init__(self):
        self.opened = 0

    def check_if_alive(self, timeout):
        raise ConnectionError("not connected")

    def close(self):
        pass

    def open(self, timeout):
        self.opened += 1
        raise PermissionError("userauth_publickey_fromfile")


def _libssh2_runner(connection):
    return SimpleNamespace(connection=connection, user="ubuntu", hostname="10.4.0.201", log=getLogger(__name__))


def test_libssh2_is_up_keeps_and_logs_why_ssh_is_down(caplog):
    """SCT-1439: 24 probes logged only "False", the AuthenticationError surfaced much later."""
    connection = _RefusingConnection()
    runner = _libssh2_runner(connection)

    with caplog.at_level("DEBUG"):
        assert RemoteLibSSH2CmdRunner.is_up(runner, timeout=0.01) is False

    assert connection.opened >= 1
    assert isinstance(runner.last_is_up_error, PermissionError)
    assert "SSH to ubuntu@10.4.0.201 is not up" in caplog.text
    assert "userauth_publickey_fromfile" in caplog.text


def test_libssh2_is_up_clears_the_last_error_once_ssh_is_up():
    connection = SimpleNamespace(check_if_alive=lambda timeout: True)
    runner = _libssh2_runner(connection)
    runner.last_is_up_error = PermissionError("old")

    assert RemoteLibSSH2CmdRunner.is_up(runner, timeout=0.01) is True
    assert runner.last_is_up_error is None
