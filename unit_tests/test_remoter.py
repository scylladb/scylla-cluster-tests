from typing import Union
from logging import getLogger
import unittest
import getpass
import threading
from parameterized import parameterized
from sdcm.remote import RemoteLibSSH2CmdRunner, RemoteCmdRunner, LocalCmdRunner, RetryableNetworkException, \
    SSHConnectTimeoutError


ALL_COMMANDS_WITH_ALL_OPTIONS = []
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
                        ALL_COMMANDS_WITH_ALL_OPTIONS.append(
                            (cmd, verbose_value, ignore_status_value, new_session_value, retry_value, timeout_value))


class TestRemoteCmdRunners(unittest.TestCase):
    """A class that use LocalCmdRunner to get example of result and runs tests against
      RemoteLibSSH2CmdRunner and RemoteCmdRunner, to make sure that they produce result that matches
      result of LocalCmdRunner

    To be ran manually, user under which test is ran have to have
      ~/.ssh/scylla-qa-ec2 key in it's ~/.ssh/authorized_keys
    """
    log = getLogger()
    key_file = '~/.ssh/scylla-qa-ec2'

    @staticmethod
    def _create_and_run_twice_in_same_thread(remoter_type, key_file, stmt, kwargs, paramiko_thread_results):
        remoter = remoter_type(hostname='127.0.0.1', user=getpass.getuser(), key_file=key_file)
        try:
            result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # pylint: disable=broad-except
            result = exc
        paramiko_thread_results.append(result)
        try:
            result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # pylint: disable=broad-except
            result = exc
        paramiko_thread_results.append(result)
        remoter.stop()

    @staticmethod
    def _create_and_run_in_same_thread(remoter_type, key_file, stmt, kwargs, paramiko_thread_results):
        remoter = remoter_type(hostname='127.0.0.1', user=getpass.getuser(), key_file=key_file)
        try:
            result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # pylint: disable=broad-except
            result = exc
        paramiko_thread_results.append(result)
        remoter._reconnect()
        try:
            result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # pylint: disable=broad-except
            result = exc
        paramiko_thread_results.append(result)
        remoter.stop()

    @staticmethod
    def _create_and_run_in_separate_thread(remoter, stmt, kwargs, paramiko_thread_results):
        try:
            result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # pylint: disable=broad-except
            result = exc
        paramiko_thread_results.append(result)
        remoter._reconnect()
        try:
            result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # pylint: disable=broad-except
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
            expected = expected.result
        for attr_name in fields_to_compare:
            expected_bucket[attr_name] = getattr(expected, attr_name, None)
        if isinstance(result, RetryableNetworkException):
            result = result.original
        elif isinstance(result, SSHConnectTimeoutError):
            result = object()
        if isinstance(result, Exception):
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

    @parameterized.expand(ALL_COMMANDS_WITH_ALL_OPTIONS)
    @unittest.skip('To be ran manually')
    def test_run_in_mainthread(  # pylint: disable=too-many-arguments
            self, stmt: str, verbose: bool, ignore_status: bool, new_session: bool, retry: int,
            timeout: Union[float, None]):
        kwargs = {
            'verbose': verbose,
            'ignore_status': ignore_status,
            'new_session': new_session,
            'retry': retry,
            'timeout': timeout}
        try:
            expected = LocalCmdRunner().run(stmt, **kwargs)
        except Exception as exc:  # pylint: disable=broad-except
            expected = exc

        remoter = RemoteCmdRunner(
            hostname='127.0.0.1', user=getpass.getuser(), key_file=self.key_file)
        try:
            paramiko_result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # pylint: disable=broad-except
            paramiko_result = exc
        remoter._reconnect()
        try:
            paramiko_result2 = remoter.run(stmt, **kwargs)
        except Exception as exc:  # pylint: disable=broad-except
            paramiko_result2 = exc
        remoter.stop()

        remoter = RemoteLibSSH2CmdRunner(
            hostname='127.0.0.1', user=getpass.getuser(), key_file=self.key_file)
        try:
            lib2ssh_result = remoter.run(stmt, **kwargs)
        except Exception as exc:  # pylint: disable=broad-except
            lib2ssh_result = exc
        remoter._reconnect()
        try:
            lib2ssh_result2 = remoter.run(stmt, **kwargs)
        except Exception as exc:  # pylint: disable=broad-except
            lib2ssh_result2 = exc
        remoter.stop()
        self._compare_results(expected, paramiko_result, stmt=stmt, kwargs=kwargs)
        self._compare_results(expected, paramiko_result2, stmt=stmt, kwargs=kwargs)
        self._compare_results(expected, lib2ssh_result, stmt=stmt, kwargs=kwargs)
        self._compare_results(expected, lib2ssh_result2, stmt=stmt, kwargs=kwargs)

    @parameterized.expand(ALL_COMMANDS_WITH_ALL_OPTIONS)
    @unittest.skip('To be ran manually')
    def test_create_and_run_in_same_thread(  # pylint: disable=too-many-arguments,too-many-locals
            self, stmt: str, verbose: bool, ignore_status: bool, new_session: bool,
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
        except Exception as exc:  # pylint: disable=broad-except
            expected = exc

        paramiko_thread_results = []
        self._run_parallel(
            3,
            thread_body=self._create_and_run_in_same_thread,
            args=(RemoteCmdRunner, self.key_file, stmt, kwargs, paramiko_thread_results),
            kwargs={})

        libssh2_thread_results = []
        self._run_parallel(
            3,
            thread_body=self._create_and_run_in_same_thread,
            args=(RemoteLibSSH2CmdRunner, self.key_file, stmt, kwargs, libssh2_thread_results),
            kwargs={})

        for paramiko_result in paramiko_thread_results:
            self._compare_results(expected, paramiko_result, stmt=stmt, kwargs=kwargs)

        for libssh2_result in libssh2_thread_results:
            self._compare_results(expected, libssh2_result, stmt=stmt, kwargs=kwargs)

    @parameterized.expand(ALL_COMMANDS_WITH_ALL_OPTIONS)
    @unittest.skip('To be ran manually')
    def test_create_and_run_in_separate_thread(  # pylint: disable=too-many-arguments
            self, stmt: str, verbose: bool, ignore_status: bool,
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
        except Exception as exc:  # pylint: disable=broad-except
            expected = exc

        # Paramiko fails too often when it is invoked like that, that is why it is not in the test

        remoter = RemoteLibSSH2CmdRunner(hostname='127.0.0.1', user=getpass.getuser(), key_file=self.key_file)
        libssh2_thread_results = []

        self._run_parallel(
            3,
            thread_body=self._create_and_run_in_separate_thread,
            args=(remoter, stmt, kwargs, libssh2_thread_results),
            kwargs={})

        for libssh2_result in libssh2_thread_results:
            self.log.error(str(libssh2_result))
            self._compare_results(expected, libssh2_result, stmt=stmt, kwargs=kwargs)

    @parameterized.expand([
        (
            RemoteLibSSH2CmdRunner,
            "/bin/bash -c \"printf 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            "AAA\\n%.0s' {1..100};\""
        ),
        (
            RemoteCmdRunner,
            "/bin/bash -c \"printf 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            "AA\\n%.0s' {1..100};\""
        )
    ])
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
        except Exception as exc:  # pylint: disable=broad-except
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

    @parameterized.expand([
        (RemoteLibSSH2CmdRunner, "export | grep SSH_CONNECTION ; false", True),
        (RemoteCmdRunner, "export | grep SSH_CONNECTION ; false", True),
        (RemoteLibSSH2CmdRunner, "export | grep SSH_CONNECTION ; true", True),
        (RemoteCmdRunner, "export | grep SSH_CONNECTION ; true", True),
        (RemoteLibSSH2CmdRunner, "export | grep SSH_CONNECTION ; false", False),
        (RemoteCmdRunner, "export | grep SSH_CONNECTION ; false", False),
        (RemoteLibSSH2CmdRunner, "export | grep SSH_CONNECTION ; true", False),
        (RemoteCmdRunner, "export | grep SSH_CONNECTION ; true", False)
    ])
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
