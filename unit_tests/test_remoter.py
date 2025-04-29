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
import unittest
import threading
from typing import Union, Optional
from logging import getLogger

# from parameterized import parameterized

from sdcm.remote import RemoteLibSSH2CmdRunner, RemoteCmdRunner, LocalCmdRunner, RetryableNetworkException, \
    SSHConnectTimeoutError, shell_script_cmd
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
    for ip in ['::1', '127.0.0.1']:
        for cmd in [
            'echo 0',
            'echo 0; false',
            'adasdasdasd',
            "/bin/bash -c \"printf 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\\n%.0s' {1..100};\""]:
            for verbose_value in [False, True]:
                for ignore_status_value in [False, True]:
                    for new_session_value in [False, True]:
                        for retry_value in [1, 2]:
                            for timeout_value in [None, 5]:
                                for remoter_type in [RemoteCmdRunner, RemoteLibSSH2CmdRunner, KubernetesCmdRunner]:
                                    all_commands_with_all_options.append(
                                        (remoter_type, ip, cmd, verbose_value, ignore_status_value, new_session_value,
                                         retry_value, timeout_value))
    return all_commands_with_all_options


ALL_COMMANDS_WITH_ALL_OPTIONS = generate_all_commands_with_all_options()


class TestRemoteCmdRunners(unittest.TestCase):
    """A class that use LocalCmdRunner to get example of result and runs tests against
      RemoteLibSSH2CmdRunner and RemoteCmdRunner, to make sure that they produce result that matches
      result of LocalCmdRunner

    To be ran manually, user under which test is ran have to have
      ~/.ssh/scylla_test_id_ed25519 key in it's ~/.ssh/authorized_keys
    """
    log = getLogger()
    key_file = '~/.ssh/scylla_test_id_ed25519'

    @staticmethod
    def _create_and_run_twice_in_same_thread(remoter_type, key_file, stmt, kwargs, paramiko_thread_results):
        if issubclass(remoter_type, (RemoteCmdRunner, RemoteLibSSH2CmdRunner)):
            remoter = remoter_type(hostname='127.0.0.1', user=getpass.getuser(), key_file=key_file)
        else:
            remoter = KubernetesCmdRunner(
                FakeKluster('http://127.0.0.1:8001'),
                pod_image="fake-pod-image",
                pod_name='sct-cluster-dc-1-kind-0', container="scylla", namespace="scylla")
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
                FakeKluster('http://127.0.0.1:8001'),
                pod_image="fake-pod-image",
                pod_name='sct-cluster-dc-1-kind-0', container="scylla", namespace="scylla")
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
            expected, result, stmt, kwargs, fields_to_compare=('stdout', 'stderr', 'command', 'exited', 'ok')):
        expected_bucket = {'__class__': type(expected).__name__}
        result_bucket = {'__class__': type(result).__name__}

        if isinstance(expected, RetryableNetworkException):
            expected = expected.original
        elif isinstance(expected, SSHConnectTimeoutError):
            expected = object()
        if isinstance(expected, Exception):
            if hasattr(expected, 'result'):
                expected = expected.result
        for attr_name in fields_to_compare:
            expected_bucket[attr_name] = getattr(expected, attr_name, None)
        if isinstance(result, RetryableNetworkException):
            result = result.original
        elif isinstance(result, SSHConnectTimeoutError):
            result = object()
        if isinstance(result, Exception):
            if hasattr(result, 'result'):
                result = result.result
        for attr_name in fields_to_compare:
            attr_value = getattr(result, attr_name, None)
            if attr_name in ['stderr'] and attr_value is not None and attr_value[:5] == 'bash:':
                # libssh2 by default runs bash, while paramiko is more specific, it runs /bin/bash
                #  as result, when paramiko gets bash error it gets output '/bin/bash: asdas is not defined'
                #  while libssh gets 'bash: asdas is not defined'
                attr_value = '/bin/bash:' + attr_value[5:]
            result_bucket[attr_name] = attr_value
        assert expected_bucket == result_bucket, \
            f'\nRunning command:\n{stmt}\n' \
            f'With options: {str(kwargs)}\n' \
            f'Resulted in receiving {type(result).__name__}:\n' \
            f'--------------- START ---------------\n' \
            f'{str(result)}\n' \
            f'--------------- END ---------------\n' \
            f'While expect to get {type(expected).__name__}:\n' \
            f'--------------- START ---------------\n' \
            f'{str(expected)}\n' \
            f'--------------- END ---------------\n'

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
    @unittest.skip('To be ran manually')
    def test_run_in_mainthread(
            self, remoter_type, host: str, stmt: str, verbose: bool, ignore_status: bool, new_session: bool, retry: int,
            timeout: Union[float, None]):
        kwargs = {
            'verbose': verbose,
            'ignore_status': ignore_status,
            'new_session': new_session,
            'retry': retry,
            'timeout': timeout}
        try:
            expected = LocalCmdRunner().run(stmt, **kwargs)
        except Exception as exc:  # noqa: BLE001
            expected = exc

        if issubclass(remoter_type, (RemoteCmdRunner, RemoteLibSSH2CmdRunner)):
            remoter = remoter_type(hostname=host, user=getpass.getuser(), key_file=self.key_file)
        else:
            remoter = KubernetesCmdRunner(
                FakeKluster('http://127.0.0.1:8001'),
                pod_image="fake-pod-image",
                pod_name='sct-cluster-dc-1-kind-0', container="scylla", namespace="scylla")
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
    @unittest.skip('To be ran manually')
    def test_create_and_run_in_same_thread(
            self, remoter_type, host: str, stmt: str, verbose: bool, ignore_status: bool, new_session: bool,
            retry: int, timeout: Union[float, None]):
        kwargs = {
            'verbose': verbose,
            'ignore_status': ignore_status,
            'new_session': new_session,
            'retry': retry,
            'timeout': timeout}
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
            kwargs={})

        for paramiko_result in paramiko_thread_results:
            self._compare_results(expected, paramiko_result, stmt=stmt, kwargs=kwargs)

    # @parameterized.expand(ALL_COMMANDS_WITH_ALL_OPTIONS)
    @unittest.skip('To be ran manually')
    def test_create_and_run_in_separate_thread(
            self, remoter_type, host: str, stmt: str, verbose: bool, ignore_status: bool,
            new_session: bool, retry: int, timeout: Union[float, None]):
        kwargs = {
            'verbose': verbose,
            'ignore_status': ignore_status,
            'new_session': new_session,
            'retry': retry,
            'timeout': timeout}
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
                FakeKluster('http://127.0.0.1:8001'),
                pod_image="fake-pod-image",
                pod_name='sct-cluster-dc-1-kind-0', container="scylla", namespace="scylla")

        libssh2_thread_results = []

        self._run_parallel(
            3,
            thread_body=self._create_and_run_in_separate_thread,
            args=(remoter, stmt, kwargs, libssh2_thread_results),
            kwargs={})

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
    @unittest.skip('To be ran manually')
    def test_load_1000_threads(self, remoter_type, stmt: str):
        kwargs = {
            'verbose': True,
            'ignore_status': False,
            'new_session': True,
            'retry': 2
        }
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
            kwargs={})

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
    @unittest.skip('To be ran manually')
    def test_context_changing(self, remoter_type, stmt: str, change_context: bool):
        kwargs = {
            'verbose': True,
            'ignore_status': True,
            'timeout': 10,
            'change_context': change_context
        }
        self.log.info(repr({stmt: stmt, **kwargs}))
        paramiko_thread_results = []
        self._create_and_run_twice_in_same_thread(remoter_type, self.key_file, stmt, kwargs, paramiko_thread_results)
        if change_context and paramiko_thread_results[0].ok:
            self.assertNotEqual(paramiko_thread_results[0].stdout, paramiko_thread_results[1].stdout)
        else:
            self.assertEqual(paramiko_thread_results[0].stdout, paramiko_thread_results[1].stdout)


class TestSudoAndRunShellScript(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
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
        self.assertEqual(remoter.command_to_run, "true")

    def test_sudo_non_root(self):
        remoter = self.remoter_cls("localhost", user="joe")
        remoter.sudo("true")
        self.assertEqual(remoter.command_to_run, "sudo true")

    def test_shell_script_cmd(self):
        self.assertEqual(shell_script_cmd("true"), 'bash -cxe "true"')


class TestRemoteFile(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        class _Runner:
            sf_data = sf_src = sf_dst = rf_src = rf_dst = None
            hostname = "localhost"
            command_to_run = ""
            rf_data = 'new'

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
        with remote_file(remoter=remoter, remote_path=some_file,
                         preserve_ownership=False, preserve_permissions=False) as fobj:
            fobj.write("test data")
        self.assertEqual(remoter.rf_src, some_file)
        self.assertEqual(remoter.sf_dst, "temporary")
        self.assertTrue(remoter.rf_dst.startswith("/tmp/sct"))
        self.assertTrue(remoter.rf_dst.endswith(os.path.basename(some_file)))
        self.assertEqual(remoter.rf_dst, remoter.sf_src)
        self.assertEqual(remoter.sf_data, "test data")
        self.assertFalse(os.path.exists(remoter.sf_src))
        self.assertEqual(remoter.command_to_run, f'bash -cxe "cat \'temporary\' > \'{some_file}\'\nrm \'temporary\'\n"')

    def test_remote_file_preserve_ownership(self):
        remoter = self.remoter_cls()
        some_file = "/some/path/some.file"
        with remote_file(remoter=remoter, remote_path=some_file,
                         preserve_ownership=True, preserve_permissions=False, sudo=True) as fobj:
            fobj.write("test data")
            assert remoter.command_to_run == f'stat -c "%U:%G" {some_file}'
        assert f"chown bentsi:bentsi {some_file}" == remoter.command_to_run

    def test_remote_file_preserve_permissions(self):
        remoter = self.remoter_cls()
        some_file = "/some/path/some.file"
        with remote_file(remoter=remoter, remote_path=some_file,
                         preserve_ownership=False, preserve_permissions=True, sudo=True) as fobj:
            fobj.write("test data")
            assert remoter.command_to_run == f'stat -c "%a" {some_file}'
        assert f"chmod 644 {some_file}" == remoter.command_to_run

    def test_remote_file_preserve_readonly(self):
        remoter = self.remoter_cls()
        some_file = "/some/path/some.file"
        with remote_file(remoter=remoter, remote_path=some_file,
                         preserve_ownership=False, preserve_permissions=True, sudo=True) as fobj:
            fobj.write(remoter.rf_data)
            assert remoter.command_to_run == f'stat -c "%a" {some_file}'

        self.assertEqual(remoter.rf_src, some_file)
        self.assertTrue(remoter.rf_dst.startswith("/tmp/sct"))
        self.assertTrue(remoter.rf_dst.endswith(os.path.basename(some_file)))
        self.assertEqual(remoter.sf_data, None)
