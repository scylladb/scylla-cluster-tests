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

from abc import abstractmethod
from typing import Type, Tuple, List, Optional
from shlex import quote
import glob
import os
import shutil
import tempfile
import time
import threading

from invoke.watchers import StreamWatcher
from invoke.runners import Result

from sdcm.utils.decorators import retrying

from .base import RetryableNetworkException, CommandRunner
from .local_cmd_runner import LocalCmdRunner


class RemoteCmdRunnerBase(CommandRunner):  # pylint: disable=too-many-instance-attributes
    port: int = 22
    connect_timeout: int = 60
    key_file: str = ""
    extra_ssh_options: str = ""
    auth_sleep_time = 30
    proxy_host: str = None
    proxy_port: int = 22
    proxy_user: str = None
    proxy_password: str = None
    proxy_key: str = None
    _use_rsync = None
    known_hosts_file = None
    default_remoter_class: Type['RemoteCmdRunnerBase'] = None
    remoter_classes = {}
    exception_unexpected: Type[Exception] = None
    exception_failure: Type[Exception] = None
    exception_retryable: Tuple[Type[Exception]] = None
    connection_thread_map = threading.local()
    default_run_retry = 3

    def __init__(self, hostname: str, user: str = 'root',  # pylint: disable=too-many-arguments  # noqa: PLR0913
                 password: str = None, port: int = None, connect_timeout: int = None, key_file: str = None,
                 extra_ssh_options: str = None, auth_sleep_time: float = None,
                 proxy_host: str = None, proxy_port: int = 22, proxy_user: str = None, proxy_password: str = None,
                 proxy_key: str = None):
        if port is not None:
            self.port = port
        if connect_timeout is not None:
            self.connect_timeout = connect_timeout
        if key_file is not None:
            self.key_file = key_file
        if extra_ssh_options is not None:
            self.extra_ssh_options = extra_ssh_options
        if auth_sleep_time is not None:
            self.auth_sleep_time = auth_sleep_time
        # if proxy host attributes are set, the SSH connection to target node will be proxied through an
        # intermediate node, allowing to execute commands on a node which is not directly accessible from test runner
        if proxy_host is not None:
            self.proxy_host = proxy_host
        if proxy_port is not None:
            self.proxy_port = proxy_port
        if proxy_user is not None:
            self.proxy_user = proxy_user
        if proxy_password is not None:
            self.proxy_password = proxy_password
        if proxy_key is not None:
            self.proxy_key = proxy_key
        fd, self.known_hosts_file = tempfile.mkstemp()
        os.close(fd)
        self._context_generation = 0
        super().__init__(hostname=hostname, user=user, password=password)

    @property
    def connection(self):
        """
        Map connection to current thread.
        If there is no such thread, create it.
        """
        connection = getattr(self.connection_thread_map, str(id(self)), None)
        if connection is None:
            connection = self._create_connection()
            setattr(self.connection_thread_map, str(id(self)), connection)
            self._bind_generation_to_connection(connection)
        return connection

    @classmethod
    def get_retryable_exceptions(cls) -> Tuple[Type[Exception]]:
        return cls.exception_retryable

    def get_init_arguments(self) -> dict:
        """
        Return instance parameters required to rebuild instance
        """
        return {'hostname': self.hostname, 'user': self.user, 'password': self.password, 'port': self.port,
                'connect_timeout': self.connect_timeout, 'key_file': self.key_file,
                'extra_ssh_options': self.extra_ssh_options, 'auth_sleep_time': self.auth_sleep_time}

    def __init_subclass__(cls, ssh_transport: str = None, default: bool = False):
        if default:
            RemoteCmdRunnerBase.set_default_remoter_class(cls)
        if ssh_transport:
            cls.remoter_classes[ssh_transport] = cls

    @classmethod
    def create_remoter(cls, *args, **kwargs) -> 'RemoteCmdRunnerBase':  # pylint: disable=unused-argument
        """
        Use this function to create remote runner of the default type
        """
        if cls.default_remoter_class is None:
            raise RuntimeError("Can't create remoter, no default remoter class found")
        return cls.default_remoter_class(*args, **kwargs)  # pylint: disable=not-callable

    @classmethod
    def set_default_ssh_transport(cls, ssh_transport: str):
        remoter_class = cls.remoter_classes.get(ssh_transport, None)
        if remoter_class is None:
            raise ValueError(f'There is not remoter class with ssh_transport marker "{ssh_transport}"')
        RemoteCmdRunnerBase.default_remoter_class = remoter_class

    @staticmethod
    def set_default_remoter_class(remoter_class: Type['RemoteCmdRunnerBase']):
        RemoteCmdRunnerBase.default_remoter_class = remoter_class

    @abstractmethod
    def _create_connection(self):
        pass

    def _bind_generation_to_connection(self, connection: object):
        setattr(connection, '_context_generation', self._context_generation)

    def _is_connection_generation_ok(self, connection: object):
        return getattr(connection, '_context_generation', self._context_generation) == self._context_generation

    def stop(self):
        self._close_connection()

    def _close_connection(self):
        if self.connection:
            self.connection.close()

    def _open_connection(self):
        self.connection.open()

    def __del__(self):
        self.stop()

    @retrying(n=5, sleep_time=1, allowed_exceptions=(Exception,), message="Reconnecting")
    def _reconnect(self):
        """
            Close and reopen connection to the remote endpoint.
            It is done only for connection specific for given thread.
            Connections for other threads are not affected.
        """
        self.log.debug("Reconnecting to '%s'", self.hostname)
        self._close_connection()
        self._open_connection()
        self.log.debug("Connected!")

    def ssh_debug_cmd(self) -> str:
        if self.key_file:
            return "SSH access -> 'ssh -i %s %s@%s'" % (self.key_file,
                                                        self.user,
                                                        self.hostname)
        else:
            return "SSH access -> 'ssh %s@%s'" % (self.user,
                                                  self.hostname)

    @abstractmethod
    def is_up(self, timeout: float = 30):
        pass

    @retrying(n=3, sleep_time=5, allowed_exceptions=(RetryableNetworkException,))
    def receive_files(self, src: str, dst: str, delete_dst: bool = False,  # pylint: disable=too-many-arguments
                      preserve_perm: bool = True, preserve_symlinks: bool = False, timeout: float = 300):
        """
        Copy files from the remote host to a local path.

        If both machines support rsync, that command will be used.

        If not, an scp command will be assembled. Directories will be
        copied recursively. If a src component is a directory with a
        trailing slash, the content of the directory will be copied,
        otherwise, the directory itself and its content will be copied. This
        behavior is similar to that of the program 'rsync'.

        :param src: Either
            1) a single file or directory, as a string
            2) a list of one or more (possibly mixed) files or directories
        :param dst: A file or a directory (if src contains a
            directory or more than one element, you must supply a
            directory dst).
        :param delete_dst: If this is true, the command will also clear
            out any old files at dest that are not in the src
        :param preserve_perm: Tells get_file() to try to preserve the sources
            permissions on files and dirs.
        :param preserve_symlinks: Try to preserve symlinks instead of
            transforming them into files/dirs on copy.
        :param timeout: Timeout in seconds.

        :raises: invoke.exceptions.UnexpectedExit, invoke.exceptions.Failure if the remote copy command failed.
        """
        self.log.debug('<%s>: Receive files (src) %s -> (dst) %s', self.hostname, src, dst)
        # Start a master SSH connection if necessary.

        if isinstance(src, str):
            src = [src]
        dst = os.path.abspath(dst)

        # If rsync is disabled or fails, try scp.
        files_received = True
        try_scp = True
        if self.use_rsync():
            try:
                remote_source = self._encode_remote_paths(src)
                local_dest = quote(dst)
                rsync = self._make_rsync_cmd([remote_source], local_dest,
                                             delete_dst, preserve_symlinks, timeout)
                result = LocalCmdRunner().run(rsync, timeout=timeout)
                self.log.debug(result.exited)
                try_scp = False
            except (self.exception_failure, self.exception_unexpected) as ex:
                self.log.warning("<%s>: Trying scp, rsync failed: %s", self.hostname, ex)
                # Make sure master ssh available
                files_received = False

        if try_scp:
            # scp has no equivalent to --delete, just drop the entire dest dir
            if delete_dst and os.path.isdir(dst):
                shutil.rmtree(dst)
                os.makedirs(dst, exist_ok=True)

            remote_source = self._make_rsync_compatible_source(src, False)
            if remote_source:
                # _make_rsync_compatible_source() already did the escaping
                remote_source = self._encode_remote_paths(remote_source,
                                                          escape=False)
                local_dest = quote(dst)
                scp = self._make_scp_cmd([remote_source], local_dest)
                try:
                    result = LocalCmdRunner().run(scp, timeout=timeout)
                except self.exception_unexpected as ex:
                    if self._is_error_retryable(ex.result.stderr):
                        raise RetryableNetworkException(ex.result.stderr, original=ex) from ex
                    raise
                self.log.debug("<%s>: Command %s with status %s", self.hostname, result.command, result.exited)
                if result.exited:
                    files_received = False
                # Avoid "already printed" message without real error
                if result.stderr:
                    self.log.debug("<%s>: Stderr: %s", self.hostname, result.stderr)
                    files_received = False

        if not preserve_perm:
            # we have no way to tell scp to not try to preserve the
            # permissions so set them after copy instead.
            # for rsync we could use "--no-p --chmod=ugo=rwX" but those
            # options are only in very recent rsync versions
            self._set_umask_perms(dst)
        return files_received

    @retrying(n=3, sleep_time=5, allowed_exceptions=(RetryableNetworkException,))
    def send_files(self, src: str,  # pylint: disable=too-many-arguments,too-many-statements
                   dst: str, delete_dst: bool = False, preserve_symlinks: bool = False, verbose: bool = False) -> bool:
        """
        Copy files from a local path to the remote host.

        If both machines support rsync, that command will be used.

        If not, an scp command will be assembled. Directories will be
        copied recursively. If a src component is a directory with a
        trailing slash, the content of the directory will be copied,
        otherwise, the directory itself and its content will be copied. This
        behavior is similar to that of the program 'rsync'.

        :param src: Either
            1) a single file or directory, as a string
            2) a list of one or more (possibly mixed) files or directories
        :param dst: A file or a directory (if src contains a
            directory or more than one element, you must supply a
            directory dst).
        :param delete_dst: If this is true, the command will also clear
            out any old files at dest that are not in the src
        :param preserve_symlinks: Try to preserve symlinks instead of
            transforming them into files/dirs on copy.
        :param verbose: Log commands being used and their outputs.

        :raises: invoke.exceptions.UnexpectedExit, invoke.exceptions.Failure if the remote copy command failed
        """

        # pylint: disable=too-many-branches,too-many-locals
        self.log.debug('<%s>: Send files (src) %s -> (dst) %s', self.hostname, src, dst)
        # Start a master SSH connection if necessary.
        source_is_dir = False
        if isinstance(src, str):
            source_is_dir = os.path.isdir(src)
            src = [src]
        remote_dest = self._encode_remote_paths([dst])

        # If rsync is disabled or fails, try scp.
        try_scp = True
        files_sent = True
        if self.use_rsync():
            try:
                local_sources = [quote(os.path.expanduser(path)) for path in src]
                rsync = self._make_rsync_cmd(local_sources, remote_dest,
                                             delete_dst, preserve_symlinks)
                LocalCmdRunner().run(rsync)
                try_scp = False
            except (self.exception_failure, self.exception_unexpected) as details:
                self.log.warning("<%s>: Trying scp, rsync failed: %s", self.hostname, details)
                files_sent = False

        if try_scp:
            # scp has no equivalent to --delete, just drop the entire dest dir
            if delete_dst:
                dest_exists = False
                try:
                    result = self.run("test -x %s" % dst, verbose=False)
                    if result.ok:
                        dest_exists = True
                except (self.exception_failure, self.exception_unexpected):
                    pass

                dest_is_dir = False
                if dest_exists:
                    try:
                        result = self.run("test -d %s" % dst, verbose=False)
                        if result.ok:
                            dest_is_dir = True
                    except (self.exception_failure, self.exception_unexpected):
                        pass

                # If there is a list of more than one path, dst *has*
                # to be a dir. If there's a single path being transferred and
                # it is a dir, the dst also has to be a dir. Therefore
                # it has to be created on the remote machine in case it doesn't
                # exist, otherwise we will have an scp failure.
                if len(src) > 1 or source_is_dir:
                    dest_is_dir = True

                if dest_exists and dest_is_dir:
                    cmd = "rm -rf %s && mkdir %s" % (dst, dst)
                    self.run(cmd, verbose=verbose)

                elif not dest_exists and dest_is_dir:
                    cmd = "mkdir %s" % dst
                    self.run(cmd, verbose=verbose)

            local_sources = self._make_rsync_compatible_source(src, True)
            if local_sources:
                scp = self._make_scp_cmd(local_sources, remote_dest)
                try:
                    result = LocalCmdRunner().run(scp)
                except self.exception_unexpected as ex:
                    if self._is_error_retryable(ex.result.stderr):
                        raise RetryableNetworkException(ex.result.stderr, original=ex) from ex
                    raise
                self.log.debug('<%s>: Command %s with status %s', self.hostname, result.command, result.exited)
                if result.exited:
                    files_sent = False
        return files_sent

    def use_rsync(self) -> bool:
        if self._use_rsync is not None:
            return self._use_rsync

        # Check if rsync is available on the remote host. If it's not,
        # don't try to use it for any future file transfers.
        self._use_rsync = self._check_rsync()
        if not self._use_rsync:
            self.log.warning("<%s>: Command rsync not available -- disabled", self.hostname)
        return self._use_rsync

    def _check_rsync(self) -> bool:
        """
        Check if rsync is available on the remote host.
        """
        result = self.run("rsync --version", ignore_status=True)
        return result.ok

    def _encode_remote_paths(self, paths: List[str], escape=True) -> str:
        """
        Given a list of file paths, encodes it as a single remote path, in
        the style used by rsync and scp.
        """
        if escape:
            paths = [self._scp_remote_escape(path) for path in paths]
        return '%s@[%s]:"%s"' % (self.user, self.hostname, " ".join(paths))

    def _make_scp_cmd(self, src: str, dst: str, connect_timeout: int = 300, alive_interval: int = 300) -> str:
        """
        Given a list of source paths and a destination path, produces the
        appropriate scp command for encoding it. Remote paths must be
        pre-encoded.
        If remoter proxy_host attribute is set, the produced scp command will
        include ProxyCommand option.
        """
        key_option = ''
        if self.key_file:
            key_option = '-i %s' % os.path.expanduser(self.key_file)
        command = ("scp -r -o StrictHostKeyChecking=no -o BatchMode=yes "
                   "-o ConnectTimeout=%d -o ServerAliveInterval=%d "
                   "-o UserKnownHostsFile=%s -P %d %s %s %s %s")
        proxy_cmd = ''
        if self.proxy_host:
            proxy_cmd = self._make_proxy_cmd()
        return command % (connect_timeout, alive_interval,
                          self.known_hosts_file, self.port, key_option, proxy_cmd, " ".join(src), dst)

    def _make_rsync_compatible_globs(self, pth: str, is_local: bool) -> List[str]:
        """
        Given an rsync-style path (pth), returns a list of globbed paths.

        Those will hopefully provide equivalent behaviour for scp. Does not
        support the full range of rsync pattern matching behaviour, only that
        exposed in the get/send_file interface (trailing slashes).

        :param pth: rsync-style path.
        :param is_local: Whether the paths should be interpreted as local or
            remote paths.
        """

        # non-trailing slash paths should just work
        if not pth or pth[-1] != "/":
            return [pth]

        # make a function to test if a pattern matches any files
        if is_local:
            def glob_matches_files(path, pattern):
                return glob.glob(path + pattern)
        else:
            def glob_matches_files(path, pattern):
                match_cmd = "ls \"%s\"%s" % (quote(path), pattern)
                result = self.run(match_cmd, ignore_status=True)
                return result.exit_status == 0

        # take a set of globs that cover all files, and see which are needed
        patterns = ["*", ".[!.]*"]
        patterns = [p for p in patterns if glob_matches_files(pth, p)]

        # convert them into a set of paths suitable for the commandline
        if is_local:
            return ["\"%s\"%s" % (quote(pth), pattern)
                    for pattern in patterns]
        else:
            return [self._scp_remote_escape(pth) + pattern
                    for pattern in patterns]

    def _make_rsync_compatible_source(self, source: List[str], is_local: bool) -> List[str]:
        """
        Make an rsync compatible source string.

        Applies the same logic as _make_rsync_compatible_globs, but
        applies it to an entire list of sources, producing a new list of
        sources, properly quoted.
        """
        return sum((self._make_rsync_compatible_globs(path, is_local)
                    for path in source), [])

    @staticmethod
    def _set_umask_perms(dest: str):
        """
        Set permissions on all files and directories.

        Given a destination file/dir (recursively) set the permissions on
        all the files and directories to the max allowed by running umask.
        """

        # now this looks strange but I haven't found a way in Python to _just_
        # get the umask, apparently the only option is to try to set it
        umask = os.umask(0)
        os.umask(umask)

        max_privs = 0o777 & ~umask

        def set_file_privs(filename):
            file_stat = os.stat(filename)

            file_privs = max_privs
            # if the original file permissions do not have at least one
            # executable bit then do not set it anywhere
            if not file_stat.st_mode & 0o111:
                file_privs &= ~0o111

            os.chmod(filename, file_privs)

        # try a bottom-up walk so changes on directory permissions won't cut
        # our access to the files/directories inside it
        for root, dirs, files in os.walk(dest, topdown=False):
            # when setting the privileges we emulate the chmod "X" behaviour
            # that sets to execute only if it is a directory or any of the
            # owner/group/other already has execute right
            for dirname in dirs:
                os.chmod(os.path.join(root, dirname), max_privs)

            for filename in files:
                set_file_privs(os.path.join(root, filename))

        # now set privs for the dest itself
        if os.path.isdir(dest):
            os.chmod(dest, max_privs)
        else:
            set_file_privs(dest)

    def _make_rsync_cmd(  # pylint: disable=too-many-arguments
            self, src: list, dst: str, delete_dst: bool, preserve_symlinks: bool, timeout: int = 300) -> str:
        """
        Given a list of source paths and a destination path, produces the
        appropriate rsync command for copying them. Remote paths must be
        pre-encoded.
        If remoter proxy_host attribute is set, ssh remote shell for
        the produced rsync command will include ProxyCommand option.
        """
        proxy_cmd = ''
        if self.proxy_host:
            proxy_cmd = self._make_proxy_cmd()
        ssh_cmd = self._make_ssh_command(
            user=self.user, port=self.port, hosts_file=self.known_hosts_file, key_file=self.key_file,
            extra_ssh_options=self.extra_ssh_options.replace('-tt', '-t'),  proxy_cmd=proxy_cmd)

        if delete_dst:
            delete_flag = "--delete"
        else:
            delete_flag = ""
        if preserve_symlinks:
            symlink_flag = ""
        else:
            symlink_flag = "-L"
        command = "rsync %s %s --timeout=%s --rsh='%s' -az %s %s"
        return command % (symlink_flag, delete_flag, timeout, ssh_cmd,
                          " ".join(src), dst)

    def _make_proxy_cmd(self):
        """Creates an SSH ProxyCommand string suitable for use with SSH or SCP commands"""
        key = ''
        if self.proxy_key:
            key = os.path.expanduser(self.proxy_key)
        proxy_command = (
            f'-o ProxyCommand="ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i {key} -W %h:%p '
            f'{self.proxy_user}@{self.proxy_host}"')
        return proxy_command

    def _run_execute(self, cmd: str, timeout: Optional[float] = None,  # pylint: disable=too-many-arguments
                     ignore_status: bool = False, verbose: bool = True, new_session: bool = False,
                     watchers: Optional[List[StreamWatcher]] = None):
        if verbose:
            self.log.debug('<%s>: Running command "%s"...', self.hostname, cmd)
        start_time = time.perf_counter()
        command_kwargs = dict(
            command=cmd, warn=ignore_status,
            encoding='utf-8', hide=True,
            watchers=watchers, timeout=timeout,
            in_stream=False
        )
        if new_session:
            with self._create_connection() as connection:
                result = connection.run(**command_kwargs)
        else:
            connection = self.connection
            if not self._is_connection_generation_ok(connection):
                connection.close()
                connection.open()
                self._bind_generation_to_connection(connection)
            result = connection.run(**command_kwargs)
        result.duration = time.perf_counter() - start_time
        result.exit_status = result.exited
        return result

    def _run_pre_run(self, cmd: str, timeout: Optional[float] = None,  # pylint: disable=too-many-arguments
                     ignore_status: bool = False, verbose: bool = True, new_session: bool = False,
                     log_file: Optional[str] = None, retry: int = 1, watchers: Optional[List[StreamWatcher]] = None):
        pass

    @abstractmethod
    def _run_on_retryable_exception(self, exc: Exception, new_session: bool) -> bool:
        pass

    def _run_on_exception(self, exc: Exception, verbose: bool, ignore_status: bool) -> bool:
        if hasattr(exc, "result"):
            self._print_command_results(exc.result, verbose, ignore_status)  # pylint: disable=no-member
        return True

    def _get_retry_params(self, retry: int = 1) -> dict:
        if retry == 0:
            # Won't retry on any case
            allowed_exceptions = tuple()
            retry = 1
        elif retry == 1:
            # Only retry 3 times on network exception
            allowed_exceptions = (RetryableNetworkException,)
            retry = self.default_run_retry
        else:
            # Retry times that user wants on any exception
            allowed_exceptions = (Exception, )
        return {'n': retry, 'sleep_time': 5, 'allowed_exceptions': allowed_exceptions}

    # pylint: disable=too-many-arguments
    def run(self,
            cmd: str,
            timeout: float | None = None,
            ignore_status: bool = False,
            verbose: bool = True,
            new_session: bool = False,
            log_file: str | None = None,
            retry: int = 1,
            watchers: List[StreamWatcher] | None = None,
            change_context: bool = False
            ) -> Result:
        """
        Run command at the remote endpoint and return result
        :param cmd: Command to execute
        :param timeout: Seconds to complete the command
        :param ignore_status: If False exception will be raised if command return non-zero code
        :param verbose: If True start and end of the command will be logged
        :param new_session: If True a new session will be generated to run the command
        :param log_file: Path to the log file
        :param retry: number of run retries if command fails
        :param watchers: List of watchers
        :param change_context: If True, next run will trigger reconnect on all threads.
          Needed for cases when environment context is changed by the command,
          for example group has been added to the user.
        :return:
        """

        watchers = self._setup_watchers(verbose=verbose, log_file=log_file, additional_watchers=watchers)

        @retrying(**self._get_retry_params(retry))
        def _run():
            self._run_pre_run(cmd, timeout, ignore_status, verbose, new_session, log_file, retry, watchers)
            try:
                return self._run_execute(cmd, timeout, ignore_status, verbose, new_session, watchers)
            except self.exception_retryable as exc:
                if self._run_on_retryable_exception(exc, new_session):
                    raise
            except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                if self._run_on_exception(exc, verbose, ignore_status):
                    raise
            return None

        result = _run()
        self._print_command_results(result, verbose, ignore_status)
        if change_context and result.ok:
            # Will trigger reconnect on next run for any connection that belongs to the remoter
            self._context_generation += 1
        return result
