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

"""Tests for the remote command runners, remote_file() and the minicloud SSH channel timeout alert."""

import getpass
import os
import threading
from logging import getLogger
from types import SimpleNamespace
from typing import Optional, Union
from unittest.mock import patch

import pytest
from invoke.exceptions import UnexpectedExit

from sdcm.cluster_k8s import KubernetesCluster

# from parameterized import parameterized
from sdcm.remote import (
    LocalCmdRunner,
    RemoteCmdRunner,
    RemoteLibSSH2CmdRunner,
    RetryableNetworkException,
    SSHConnectTimeoutError,
    shell_script_cmd,
)
from sdcm.remote.base import CommandRunner, Result
from sdcm.remote.kubernetes_cmd_runner import KubernetesCmdRunner
from sdcm.remote.libssh2_client.exceptions import FailedToRunCommand, OpenChannelTimeout
from sdcm.remote.remote_file import remote_file
from sdcm.remote.remote_libssh_cmd_runner import MINICLOUD_CHANNEL_TIMEOUT_ALERT_THRESHOLD
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent


class FakeKluster(KubernetesCluster):
    """KubernetesCluster stub that only carries the API server URL."""

    k8s_server_url = None

    def __init__(self, k8s_server_url):
        """Keep the API server URL that KubernetesCmdRunner connects to."""
        self.k8s_server_url = k8s_server_url

    def deploy(self):
        """Skip deployment: the fake cluster is not real."""

    def create_kubectl_config(self):
        """Skip kubectl config creation."""

    def create_token_update_thread(self):
        """Skip the token refresh thread."""

    def deploy_node_pool(self, pool, wait_till_ready=True) -> None:
        """Skip node pool creation."""

    def upgrade_kubernetes_platform(self, pod_objects, use_additional_scylla_nodepool):
        """Skip the platform upgrade."""


def generate_all_commands_with_all_options():
    """Cross product of commands and runner options, as parametrize rows for the command-runner comparison."""
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

# Manual runs only: LocalCmdRunner is the reference, the remote runners must return the same result.
# The user running the test needs KEY_FILE in ~/.ssh/authorized_keys.
KEY_FILE = "~/.ssh/scylla_test_id_ed25519"
LOG = getLogger()


def create_and_run_twice_in_same_thread(remoter_type, key_file, stmt, kwargs, paramiko_thread_results):
    """Run stmt twice on one new remoter in the calling thread, without reconnecting in between."""
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


def create_and_run_in_same_thread(remoter_type, host, key_file, stmt, kwargs, paramiko_thread_results):
    """Run stmt on a new remoter in the calling thread, reconnecting between the two runs."""
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


def create_and_run_in_separate_thread(remoter, stmt, kwargs, paramiko_thread_results):
    """Run stmt on a shared remoter from a worker thread, reconnecting between the two runs."""
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


def compare_results(expected, result, stmt, kwargs, fields_to_compare=("stdout", "stderr", "command", "exited", "ok")):
    """Assert that a remote result or exception matches the LocalCmdRunner outcome on the compared fields."""
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


def run_parallel(thread_count, thread_body, args, kwargs):
    """Start thread_count threads running thread_body with the same arguments and wait for all of them."""
    threads = []
    for _ in range(thread_count):
        threads.append(threading.Thread(target=thread_body, daemon=False, args=args, kwargs=kwargs))
    for future in threads:
        future.start()
    for future in threads:
        future.join()


class FakeCommandRunner(CommandRunner):
    """CommandRunner that records the last command instead of running it."""

    def run(self, cmd, *_, **__):
        """Record cmd as command_to_run."""
        self.command_to_run = cmd

    def _create_connection(self):
        """No connection is needed."""

    def is_up(self, timeout: Optional[float] = None) -> bool:
        """Always report the host as up."""
        return True


@pytest.fixture
def command_runner_cls():
    """FakeCommandRunner class that the sudo tests instantiate."""
    return FakeCommandRunner


class FakeRemoteFileRunner:
    """Remoter stub for remote_file(): answers its probes and records the files it copies."""

    sf_data = sf_src = sf_dst = rf_src = rf_dst = None
    hostname = "localhost"
    command_to_run = ""
    rf_data = "new"

    def __init__(self, exists: bool = True):
        """Set whether the remote file exists."""
        self.exists = exists
        self.commands = []

    def run(self, cmd, *_, **__):
        """Answer the mktemp, test -e and stat probes of remote_file(); other commands succeed silently."""
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
        """Read the local src into sf_data and record the destination."""
        with open(src, encoding="utf-8") as fobj:
            self.sf_data = fobj.read()
        self.sf_src = src
        self.sf_dst = dst
        return True

    def receive_files(self, src: str, dst: str, *_, **__) -> bool:
        """Write rf_data to the local dst, failing if the remote file is missing."""
        assert self.exists, "must not copy a missing file"
        with open(dst, "w", encoding="utf-8") as fobj:
            fobj.write(self.rf_data)
        self.rf_src = src
        self.rf_dst = dst
        return True

    def sudo(self, cmd, *_, **__):
        """Run cmd the same way as run()."""
        return self.run(cmd)


class EmptyMktempRunner(FakeRemoteFileRunner):
    """FakeRemoteFileRunner whose mktemp returns an empty path."""

    def run(self, cmd, *_, **__):
        """Return an empty mktemp result; other commands behave like FakeRemoteFileRunner."""
        if cmd == "mktemp":
            return Result(stdout="")
        return super().run(cmd, *_, **__)


@pytest.fixture
def clean_minicloud_timeout_counters():
    """Reset the minicloud SSH channel timeout counters before and after the test."""

    def reset():
        RemoteLibSSH2CmdRunner._minicloud_channel_timeouts.clear()
        RemoteLibSSH2CmdRunner._minicloud_channel_timeout_alerted = False

    reset()
    yield
    reset()


def record_minicloud_timeouts(times, hostname="10.0.0.1", minicloud=True):
    """Record times channel timeouts on one host and return the mocked TestFrameworkEvent class."""
    runner = SimpleNamespace(hostname=hostname)
    with (
        patch("sdcm.utils.minicloud.endpoint.is_minicloud_active", return_value=minicloud),
        patch("sdcm.remote.remote_libssh_cmd_runner.TestFrameworkEvent") as event,
    ):
        for _ in range(times):
            RemoteLibSSH2CmdRunner._record_minicloud_channel_timeout(runner)
    return event


class RefusingConnection:
    """A libssh2 connection whose host answers, but rejects the key."""

    def __init__(self):
        """Start with no open attempts."""
        self.opened = 0

    def check_if_alive(self, timeout):
        """Report the connection as not alive."""
        raise ConnectionError("not connected")

    def close(self):
        """No resources to release."""

    def open(self, timeout):
        """Count the attempt and reject it with a publickey failure."""
        self.opened += 1
        raise PermissionError("userauth_publickey_fromfile")


def libssh2_runner(connection):
    """SimpleNamespace standing in for a RemoteLibSSH2CmdRunner that uses the given connection."""
    return SimpleNamespace(connection=connection, user="ubuntu", hostname="10.4.0.201", log=getLogger(__name__))


# @parameterized.expand(ALL_COMMANDS_WITH_ALL_OPTIONS)
@pytest.mark.skip(reason="To be ran manually")
def test_run_in_mainthread(
    remoter_type,
    host: str,
    stmt: str,
    verbose: bool,
    ignore_status: bool,
    new_session: bool,
    retry: int,
    timeout: Union[float, None],
):
    """A remoter run from the main thread, twice with a reconnect, returns the LocalCmdRunner result."""
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
        remoter = remoter_type(hostname=host, user=getpass.getuser(), key_file=KEY_FILE)
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

    compare_results(expected, result, stmt=stmt, kwargs=kwargs)
    compare_results(expected, result2, stmt=stmt, kwargs=kwargs)


# @parameterized.expand(ALL_COMMANDS_WITH_ALL_OPTIONS)
@pytest.mark.skip(reason="To be ran manually")
def test_create_and_run_in_same_thread(
    remoter_type,
    host: str,
    stmt: str,
    verbose: bool,
    ignore_status: bool,
    new_session: bool,
    retry: int,
    timeout: Union[float, None],
):
    """Remoter results from three parallel threads, each reconnecting once, match LocalCmdRunner."""
    kwargs = {
        "verbose": verbose,
        "ignore_status": ignore_status,
        "new_session": new_session,
        "retry": retry,
        "timeout": timeout,
    }
    LOG.info(repr({stmt: stmt, **kwargs}))
    try:
        expected = LocalCmdRunner().run(stmt, **kwargs)
    except Exception as exc:  # noqa: BLE001
        expected = exc

    paramiko_thread_results = []
    run_parallel(
        3,
        thread_body=create_and_run_in_same_thread,
        args=(remoter_type, host, KEY_FILE, stmt, kwargs, paramiko_thread_results),
        kwargs={},
    )

    for paramiko_result in paramiko_thread_results:
        compare_results(expected, paramiko_result, stmt=stmt, kwargs=kwargs)


# @parameterized.expand(ALL_COMMANDS_WITH_ALL_OPTIONS)
@pytest.mark.skip(reason="To be ran manually")
def test_create_and_run_in_separate_thread(
    remoter_type,
    host: str,
    stmt: str,
    verbose: bool,
    ignore_status: bool,
    new_session: bool,
    retry: int,
    timeout: Union[float, None],
):
    """Remoter results from three parallel worker threads sharing one remoter match LocalCmdRunner."""
    kwargs = {
        "verbose": verbose,
        "ignore_status": ignore_status,
        "new_session": new_session,
        "retry": retry,
        "timeout": timeout,
    }
    LOG.info(repr({stmt: stmt, **kwargs}))
    try:
        expected = LocalCmdRunner().run(stmt, **kwargs)
    except Exception as exc:  # noqa: BLE001
        expected = exc

    # Paramiko fails too often when it is invoked like that, that is why it is not in the test

    if issubclass(remoter_type, (RemoteCmdRunner, RemoteLibSSH2CmdRunner)):
        remoter = remoter_type(hostname=host, user=getpass.getuser(), key_file=KEY_FILE)
    else:
        remoter = KubernetesCmdRunner(
            FakeKluster("http://127.0.0.1:8001"),
            pod_image="fake-pod-image",
            pod_name="sct-cluster-dc-1-kind-0",
            container="scylla",
            namespace="scylla",
        )

    libssh2_thread_results = []

    run_parallel(
        3,
        thread_body=create_and_run_in_separate_thread,
        args=(remoter, stmt, kwargs, libssh2_thread_results),
        kwargs={},
    )

    for libssh2_result in libssh2_thread_results:
        LOG.error(str(libssh2_result))
        compare_results(expected, libssh2_result, stmt=stmt, kwargs=kwargs)


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
def test_load_1000_threads(remoter_type, stmt: str):
    """1000 parallel threads running stmt on one remoter type all match LocalCmdRunner."""
    kwargs = {"verbose": True, "ignore_status": False, "new_session": True, "retry": 2}
    LOG.info(repr({stmt: stmt, **kwargs}))
    try:
        expected = LocalCmdRunner().run(stmt, **kwargs)
    except Exception as exc:  # noqa: BLE001
        expected = exc

    libssh2_thread_results = []
    run_parallel(
        1000,
        thread_body=create_and_run_in_same_thread,
        args=(remoter_type, KEY_FILE, stmt, kwargs, libssh2_thread_results),
        kwargs={},
    )

    for libssh2_result in libssh2_thread_results:
        LOG.error(str(libssh2_result))
        compare_results(expected, libssh2_result, stmt=stmt, kwargs=kwargs)


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
def test_context_changing(remoter_type, stmt: str, change_context: bool):
    """change_context decides whether a second run sees the shell context changed by the first."""
    kwargs = {"verbose": True, "ignore_status": True, "timeout": 10, "change_context": change_context}
    LOG.info(repr({stmt: stmt, **kwargs}))
    paramiko_thread_results = []
    create_and_run_twice_in_same_thread(remoter_type, KEY_FILE, stmt, kwargs, paramiko_thread_results)
    if change_context and paramiko_thread_results[0].ok:
        assert paramiko_thread_results[0].stdout != paramiko_thread_results[1].stdout
    else:
        assert paramiko_thread_results[0].stdout == paramiko_thread_results[1].stdout


def test_sudo_root(command_runner_cls):
    """run() of a root remoter runs the command as-is, without sudo."""
    remoter = command_runner_cls("localhost", user="root")
    remoter.run("true")
    assert remoter.command_to_run == "true"


def test_sudo_non_root(command_runner_cls):
    """sudo() of a non-root remoter prefixes the command with sudo."""
    remoter = command_runner_cls("localhost", user="joe")
    remoter.sudo("true")
    assert remoter.command_to_run == "sudo true"


def test_shell_script_cmd():
    """shell_script_cmd wraps the command in bash -cxe with the command quoted."""
    assert shell_script_cmd("true") == 'bash -cxe "true"'


def test_remote_file_missing_target_is_created():
    """A file the image doesn't ship (e.g. io_properties.yaml on an image without an io preset) must be created, not fail on stat."""
    remoter = FakeRemoteFileRunner(exists=False)
    some_file = "/etc/scylla.d/io_properties.yaml"
    with remote_file(remoter=remoter, remote_path=some_file, sudo=True) as fobj:
        assert fobj.getvalue() == ""
        fobj.write("disks: []")
    assert remoter.sf_data == "disks: []"
    assert not any(cmd.startswith(("stat", "chown", "chmod")) for cmd in remoter.commands)


def test_remote_file_probe_quotes_the_path():
    """A path with a space must be probed as one path, or an existing file reads as missing and is overwritten."""
    remoter = FakeRemoteFileRunner()
    with remote_file(remoter=remoter, remote_path="/tmp/config copy.yaml") as fobj:
        assert fobj.getvalue() == "new"
    assert remoter.commands[0] == "test -e '/tmp/config copy.yaml' && echo exists || true"


def test_remote_file_probe_failure_raises():
    """A remoter failure on the probe must raise, not read as a missing file that is then overwritten."""
    remoter = FakeRemoteFileRunner()
    failure = UnexpectedExit(Result(command="test -e", exited=255))
    with (
        patch.object(remoter, "run", side_effect=failure),
        pytest.raises(UnexpectedExit),
        remote_file(remoter=remoter, remote_path="/some/path/some.file"),
    ):
        pass


def test_remote_file():
    """remote_file() round-trips the file through a local temp copy and a remote temp file."""
    remoter = FakeRemoteFileRunner()
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


def test_remote_file_preserve_ownership():
    """preserve_ownership re-applies the remote owner with chown after the write."""
    remoter = FakeRemoteFileRunner()
    some_file = "/some/path/some.file"
    with remote_file(
        remoter=remoter, remote_path=some_file, preserve_ownership=True, preserve_permissions=False, sudo=True
    ) as fobj:
        fobj.write("test data")
        assert remoter.command_to_run == f'stat -c "%U:%G" {some_file}'
    assert f"chown bentsi:bentsi {some_file}" == remoter.command_to_run


def test_remote_file_preserve_permissions():
    """preserve_permissions re-applies the remote mode with chmod after the write."""
    remoter = FakeRemoteFileRunner()
    some_file = "/some/path/some.file"
    with remote_file(
        remoter=remoter, remote_path=some_file, preserve_ownership=False, preserve_permissions=True, sudo=True
    ) as fobj:
        fobj.write("test data")
        assert remoter.command_to_run == f'stat -c "%a" {some_file}'
    assert f"chmod 644 {some_file}" == remoter.command_to_run


def test_remote_file_preserve_readonly():
    """A read-only remote file is received and its mode restored, and nothing is sent back."""
    remoter = FakeRemoteFileRunner()
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


def test_remote_file_empty_mktemp_raises():
    """remote_file() raises a clear error when mktemp returns an empty path, instead of retrying with dst="" for 300s."""
    remoter = EmptyMktempRunner()
    some_file = "/some/path/some.file"
    with (
        pytest.raises(RuntimeError, match="mktemp.*empty"),
        remote_file(
            remoter=remoter, remote_path=some_file, preserve_ownership=False, preserve_permissions=False
        ) as fobj,
    ):
        fobj.write("test data")


def test_minicloud_channel_timeout_alert_activates_from_test_config(clean_minicloud_timeout_counters):
    """The remoter has no params of its own, so it must read them off TestConfig.

    A yaml-only setup turns minicloud on through the minicloud_endpoint_url param with nothing
    in the environment, so an env-only check would leave the alert silent for the whole run.
    """
    tester = SimpleNamespace(params={"minicloud_endpoint_url": "http://localhost:5000"})
    runner = SimpleNamespace(hostname="10.0.0.7")
    with (
        patch.dict(os.environ, {}, clear=True),  # no SCT_MINICLOUD_ENDPOINT_URL anywhere
        patch("sdcm.test_config.TestConfig") as test_config,
        patch.object(
            TestFrameworkEvent, "publish_or_dump", autospec=True, side_effect=TestFrameworkEvent.dont_publish
        ) as publish,
    ):
        test_config.return_value.tester_obj.return_value = tester
        for _ in range(MINICLOUD_CHANNEL_TIMEOUT_ALERT_THRESHOLD):
            RemoteLibSSH2CmdRunner._record_minicloud_channel_timeout(runner)

    assert publish.call_count == 1


def test_minicloud_channel_timeout_alert_survives_a_missing_tester(clean_minicloud_timeout_counters):
    """TestConfig has no tester before setUp runs - the hook must not explode there."""
    runner = SimpleNamespace(hostname="10.0.0.8")
    with (
        patch.dict(os.environ, {}, clear=True),
        patch("sdcm.test_config.TestConfig") as test_config,
        patch.object(
            TestFrameworkEvent, "publish_or_dump", autospec=True, side_effect=TestFrameworkEvent.dont_publish
        ) as publish,
    ):
        test_config.return_value.tester_obj.return_value = None
        for _ in range(MINICLOUD_CHANNEL_TIMEOUT_ALERT_THRESHOLD):
            RemoteLibSSH2CmdRunner._record_minicloud_channel_timeout(runner)

    assert publish.call_count == 0


def test_minicloud_channel_timeout_alert_counts_every_guest_together(clean_minicloud_timeout_counters):
    """Alerting is based on the cluster-wide timeout total across all guests."""
    hosts = [f"10.0.0.{index}" for index in range(1, 9)]
    with (
        patch("sdcm.utils.minicloud.endpoint.is_minicloud_active", return_value=True),
        patch.object(
            TestFrameworkEvent, "publish_or_dump", autospec=True, side_effect=TestFrameworkEvent.dont_publish
        ) as publish,
    ):
        for _ in range(2):  # 16 timeouts, no single guest above 2
            for host in hosts:
                RemoteLibSSH2CmdRunner._record_minicloud_channel_timeout(SimpleNamespace(hostname=host))

    message = publish.call_args.args[0].message
    assert publish.call_count == 1
    assert "10 SSH channel timeouts" in message
    assert "10.0.0.1=2" in message


def test_minicloud_channel_timeout_alert_silent_outside_minicloud(clean_minicloud_timeout_counters):
    """Channel timeouts outside a minicloud run never publish the alert, however many there are."""
    event = record_minicloud_timeouts(MINICLOUD_CHANNEL_TIMEOUT_ALERT_THRESHOLD * 2, minicloud=False)
    assert event.call_count == 0


def test_minicloud_channel_timeout_alert_fires_once_with_a_real_event(clean_minicloud_timeout_counters):
    """Crossing the threshold on minicloud publishes one WARNING event that names the host and the fix."""
    runner = SimpleNamespace(hostname="10.0.0.9")
    with (
        patch("sdcm.utils.minicloud.endpoint.is_minicloud_active", return_value=True),
        patch.object(
            TestFrameworkEvent, "publish_or_dump", autospec=True, side_effect=TestFrameworkEvent.dont_publish
        ) as publish,
    ):
        for _ in range(MINICLOUD_CHANNEL_TIMEOUT_ALERT_THRESHOLD * 3):
            RemoteLibSSH2CmdRunner._record_minicloud_channel_timeout(runner)

    assert publish.call_count == 1
    event = publish.call_args.args[0]
    assert event.severity is Severity.WARNING
    assert "10.0.0.9" in event.message
    assert "minicloud_scylla_reserve_memory" in event.message


def test_minicloud_channel_timeout_alert_counts_a_wrapped_open_channel_timeout(clean_minicloud_timeout_counters):
    """Ensure a wrapped OpenChannelTimeout is counted and retried."""
    runner = SimpleNamespace(
        hostname="10.0.0.5",
        exception_retryable=RemoteLibSSH2CmdRunner.exception_retryable,
        _is_error_retryable=lambda _: False,
    )
    runner._record_minicloud_channel_timeout = lambda: RemoteLibSSH2CmdRunner._record_minicloud_channel_timeout(runner)
    wrapped = FailedToRunCommand(Result(), OpenChannelTimeout("Failed to open channel in 15 seconds"))

    with (
        patch("sdcm.utils.minicloud.endpoint.is_minicloud_active", return_value=True),
        pytest.raises(RetryableNetworkException),
    ):
        RemoteLibSSH2CmdRunner._run_on_retryable_exception(runner, wrapped, new_session=True, suppress_errors=True)

    assert RemoteLibSSH2CmdRunner._minicloud_channel_timeouts == {"10.0.0.5": 1}


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


@pytest.mark.parametrize(
    "remoter_type",
    [
        pytest.param(RemoteCmdRunner, id="remote-cmd-runner"),
        pytest.param(RemoteLibSSH2CmdRunner, id="libssh2"),
    ],
)
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
    sleep.assert_called_once_with(10)


def test_libssh2_is_up_keeps_and_logs_why_ssh_is_down(caplog):
    """SCT-1439: 24 probes logged only "False", the AuthenticationError surfaced much later."""
    connection = RefusingConnection()
    runner = libssh2_runner(connection)

    with caplog.at_level("DEBUG"):
        assert RemoteLibSSH2CmdRunner.is_up(runner, timeout=0.01) is False

    assert connection.opened >= 1
    assert isinstance(runner.last_is_up_error, PermissionError)
    assert "SSH to ubuntu@10.4.0.201 is not up" in caplog.text
    assert "userauth_publickey_fromfile" in caplog.text


def test_libssh2_is_up_clears_the_last_error_once_ssh_is_up():
    """is_up() forgets the previous error as soon as the connection check succeeds."""
    connection = SimpleNamespace(check_if_alive=lambda timeout: True)
    runner = libssh2_runner(connection)
    runner.last_is_up_error = PermissionError("old")

    assert RemoteLibSSH2CmdRunner.is_up(runner, timeout=0.01) is True
    assert runner.last_is_up_error is None
