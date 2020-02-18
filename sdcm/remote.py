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
# Copyright (c) 2016 ScyllaDB

from __future__ import absolute_import
import glob
import logging
import os
import re
import shutil
import tempfile
import time
import getpass
import socket
import threading
from shlex import quote

from fabric import Connection, Config
from invoke.exceptions import UnexpectedExit, Failure
from invoke.watchers import StreamWatcher, Responder
from paramiko import SSHException, RSAKey
from paramiko.ssh_exception import NoValidConnectionsError, AuthenticationException

from sdcm.log import SDCMAdapter
from sdcm.utils.common import retrying

LOGGER = logging.getLogger(__name__)


# casue of this bug in astroid, and can't upgrade until python3
# https://github.com/PyCQA/pylint/issues/1965
# pylint: disable=too-many-function-args
class OutputCheckError(Exception):

    """
    Remote command output check failed.
    """


class SSHConnectTimeoutError(Exception):

    """
    Remote command output check failed.
    """


NETWORK_EXCEPTIONS = (NoValidConnectionsError, SSHException, SSHConnectTimeoutError, EOFError, AuthenticationException,
                      ConnectionResetError, ConnectionAbortedError, ConnectionError, ConnectionRefusedError)


class RetriableNetworkException(Exception):
    """
        SSH protocol exception that can be safely retried
    """


def is_error_retriable(err_str):
    """Check that exception can be safely retried"""
    exceptions = ("Authentication timeout", "Error reading SSH protocol banner", "Timeout opening channel",
                  "Unable to open channel", "Key-exchange timed out waiting for key negotiation",
                  "ssh_exchange_identification: Connection closed by remote host",
                  )
    for exception_str in exceptions:
        if exception_str in err_str:
            return True
    return False


def _scp_remote_escape(filename):
    """
    Escape special chars for SCP use.

    Bis-quoting has to be used with scp for remote files, "bis-quoting"
    as in quoting x 2. SCP does not support a newline in the filename.

    :param filename: the filename string to escape.

    :returns: The escaped filename string. The required englobing double
        quotes are NOT added and so should be added at some point by
        the caller.
    """
    escape_chars = r' !"$&' "'" r'()*,:;<=>?[\]^`{|}'

    new_name = []
    for char in filename:
        if char in escape_chars:
            new_name.append("\\%s" % (char,))
        else:
            new_name.append(char)

    return quote("".join(new_name))


def _make_ssh_command(user="root", port=22, opts='', hosts_file='/dev/null',  # pylint: disable=too-many-arguments
                      key_file=None, connect_timeout=300, alive_interval=300, extra_ssh_options=''):
    assert isinstance(connect_timeout, int)
    ssh_full_path = LocalCmdRunner().run('which ssh').stdout.strip()
    base_command = ssh_full_path
    base_command += " " + extra_ssh_options
    base_command += (" -a -x %s -o StrictHostKeyChecking=no "
                     "-o UserKnownHostsFile=%s -o BatchMode=yes "
                     "-o ConnectTimeout=%d -o ServerAliveInterval=%d "
                     "-l %s -p %d")
    if key_file is not None:
        base_command += ' -i %s' % os.path.expanduser(key_file)
    assert connect_timeout > 0  # can't disable the timeout
    return base_command % (opts, hosts_file, connect_timeout,
                           alive_interval, user, port)


class CommandRunner:
    def __init__(self, hostname, user='root', password=''):
        self.hostname = hostname
        self.user = user
        self.password = password
        self.log = SDCMAdapter(LOGGER, extra={'prefix': str(self)})
        self.connection = self._create_connection()

    def __str__(self):
        return '{} [{}@{}]'.format(self.__class__.__name__, self.user, self.hostname)

    def _setup_watchers(self, verbose, log_file, additional_watchers):
        watchers = additional_watchers if additional_watchers else []
        if verbose:
            watchers.append(OutputWatcher(self.log))
        if log_file:
            watchers.append(LogWriteWatcher(log_file))
        return watchers

    def run(self, cmd, timeout=None, ignore_status=False,  # pylint: disable=too-many-arguments
            verbose=True, new_session=False, log_file=None, retry=0, watchers=None):
        raise NotImplementedError("Should be implemented in subclasses")

    def _create_connection(self):
        raise NotImplementedError("_create_connection should be implemented")

    def _print_command_results(self, result, verbose, ignore_status):
        """When verbose=True and ignore_status=True that means nothing will be printed in any case"""
        if verbose and not result.failed:
            if result.stderr:
                self.log.info('STDERR: {}'.format(result.stderr))

            self.log.info('Command "{}" finished with status {}'.format(result.command, result.exited))
            return

        if verbose and result.failed and not ignore_status:
            self.log.error('Error executing command: "{}"; Exit status: {}'.format(result.command, result.exited))
            if result.stdout:
                self.log.debug('STDOUT: {}'.format(result.stdout[-240:]))
            if result.stderr:
                self.log.debug('STDERR: {}'.format(result.stderr))
            return


class LocalCmdRunner(CommandRunner):  # pylint: disable=too-few-public-methods

    def __init__(self, password=''):
        hostname = socket.gethostname()
        user = getpass.getuser()
        super(LocalCmdRunner, self).__init__(hostname, user=user, password=password)

    def _create_connection(self):
        return Connection(host=self.hostname, user=self.user)

    def run(self, cmd, timeout=300, ignore_status=False,  # pylint: disable=too-many-arguments
            verbose=True, new_session=False, log_file=None, retry=1, watchers=None):

        watchers = self._setup_watchers(verbose=verbose, log_file=log_file, additional_watchers=watchers)

        @retrying(n=retry)
        def _run():

            start_time = time.time()
            if verbose:
                self.log.debug('Running command "{}"...'.format(cmd))
            try:
                command_kwargs = dict(
                    command=cmd, warn=ignore_status,
                    encoding='utf-8',
                    hide=True,
                    watchers=watchers,
                    timeout=timeout,
                    env=os.environ, replace_env=True,
                    in_stream=False
                )
                if new_session:
                    with self._create_connection() as connection:
                        result = connection.local(**command_kwargs)
                else:
                    result = self.connection.local(**command_kwargs)
                setattr(result, 'duration', time.time() - start_time)
                setattr(result, 'exit_status', result.exited)
                return result

            except (Failure, UnexpectedExit) as details:
                if hasattr(details, "result"):
                    self._print_command_results(details.result, verbose, ignore_status)
                raise

        result = _run()
        self._print_command_results(result, verbose, ignore_status)
        return result


class RemoteCmdRunner(CommandRunner):  # pylint: disable=too-many-instance-attributes

    def __init__(self, hostname, user="root", port=22, connect_timeout=60, password="",  # pylint: disable=too-many-arguments
                 key_file=None, extra_ssh_options=""):

        self.key_file = key_file
        self.port = port
        self.extra_ssh_options = extra_ssh_options
        self.connect_timeout = connect_timeout
        self._use_rsync = None
        self.known_hosts_file = tempfile.mkstemp()[1]
        self._ssh_up_thread = None
        self._ssh_is_up = threading.Event()
        self._ssh_up_thread_termination = threading.Event()
        self.ssh_config = Config(overrides={
                                 'load_ssh_config': False,
                                 'UserKnownHostsFile': self.known_hosts_file,
                                 'ServerAliveInterval': 300,
                                 'StrictHostKeyChecking': 'no'})
        self.connect_config = {'pkey': RSAKey(filename=os.path.expanduser(self.key_file))}
        super(RemoteCmdRunner, self).__init__(hostname, user, password)
        self.start_ssh_up_thread()

    def stop(self):
        self._ssh_is_up.clear()
        self.stop_ssh_up_thread()
        self.connection.close()

    def __del__(self):
        self.stop()

    def _create_connection(self):
        return Connection(host=self.hostname,
                          user=self.user,
                          port=self.port,
                          config=self.ssh_config,
                          connect_timeout=self.connect_timeout,
                          connect_kwargs=self.connect_config)

    @retrying(n=5, sleep_time=1, allowed_exceptions=(Exception, ), message="Reconnecting")
    def reconnect(self):
        """
            Use with caution!!! This method forcefully disconnects the SSH session so the commands may stay
            running on the remote
        """
        self.log.debug("Reconnecting to '%s'", self.hostname)
        self.connection.close()
        self.connection.open()
        self.log.debug("Connected!")

    def ssh_debug_cmd(self):
        if self.key_file:
            return "SSH access -> 'ssh -i %s %s@%s'" % (self.key_file,
                                                        self.user,
                                                        self.hostname)
        else:
            return "SSH access -> 'ssh %s@%s'" % (self.user,
                                                  self.hostname)

    @retrying(n=3, sleep_time=5, allowed_exceptions=(RetriableNetworkException, ))
    def run(self, cmd, timeout=None, ignore_status=False,  # pylint: disable=too-many-arguments
            verbose=True, new_session=False, log_file=None, retry=1, watchers=None):

        watchers = self._setup_watchers(verbose=verbose, log_file=log_file, additional_watchers=watchers)

        @retrying(n=retry)
        def _run():
            if not self.is_up(timeout=self.connect_timeout):
                err_msg = "Unable to run '{}': failed connecting to '{}' during {}s"
                raise SSHConnectTimeoutError(err_msg.format(cmd, self.hostname, self.connect_timeout))
            try:
                if verbose:
                    self.log.debug('Running command "{}"...'.format(cmd))
                start_time = time.time()
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
                    result = self.connection.run(**command_kwargs)
                setattr(result, 'duration', time.time() - start_time)
                setattr(result, 'exit_status', result.exited)
                return result
            except NETWORK_EXCEPTIONS as ex:
                LOGGER.error(ex)
                self._ssh_is_up.clear()
                if is_error_retriable(str(ex)):
                    raise RetriableNetworkException(str(ex))
                raise
            except Exception as details:  # pylint: disable=broad-except
                if hasattr(details, "result"):
                    self._print_command_results(details.result, verbose, ignore_status)   # pylint: disable=no-member
                raise

        result = _run()
        self._print_command_results(result, verbose, ignore_status)
        return result

    def is_up(self, timeout=30):
        return self._ssh_is_up.wait(float(timeout))

    def _ssh_ping(self):
        try:
            # creating new connection each time in order not to interfere the main connection to decrease probability
            # of the EOF bug https://github.com/paramiko/paramiko/issues/1584
            with self._create_connection() as connection:
                result = connection.run("true", timeout=30, warn=False, encoding='utf-8', hide=True)
                return result.ok
        except Exception as details:  # pylint: disable=broad-except
            self.log.debug(details)
            return False

    def ssh_ping_thread(self):
        while not self._ssh_up_thread_termination.isSet():
            result = self._ssh_ping()
            if result:
                self._ssh_is_up.set()
            else:
                self._ssh_is_up.clear()
            self._ssh_up_thread_termination.wait(5)

    def start_ssh_up_thread(self):
        self._ssh_up_thread = threading.Thread(target=self.ssh_ping_thread)
        self._ssh_up_thread.daemon = True
        self._ssh_up_thread.start()

    def stop_ssh_up_thread(self):
        self._ssh_up_thread_termination.set()
        if self._ssh_up_thread:
            self._ssh_up_thread.join(5)
        self._ssh_up_thread = None

    @retrying(n=3, sleep_time=5, allowed_exceptions=(RetriableNetworkException, ))
    def receive_files(self, src, dst, delete_dst=False,  # pylint: disable=too-many-arguments
                      preserve_perm=True, preserve_symlinks=False):
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

        :raises: invoke.exceptions.UnexpectedExit, invoke.exceptions.Failure if the remote copy command failed.
        """
        self.log.debug('Receive files (src) %s -> (dst) %s', src, dst)
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
                                             delete_dst, preserve_symlinks)
                result = LocalCmdRunner().run(rsync)
                self.log.info(result.exited)
                try_scp = False
            except (Failure, UnexpectedExit) as ex:
                self.log.warning("Trying scp, rsync failed: %s", ex)
                # Make sure master ssh available
                files_received = False

        if try_scp:
            # scp has no equivalent to --delete, just drop the entire dest dir
            if delete_dst and os.path.isdir(dst):
                shutil.rmtree(dst)
                os.mkdir(dst)

            remote_source = self._make_rsync_compatible_source(src, False)
            if remote_source:
                # _make_rsync_compatible_source() already did the escaping
                remote_source = self._encode_remote_paths(remote_source,
                                                          escape=False)
                local_dest = quote(dst)
                scp = self._make_scp_cmd([remote_source], local_dest)
                try:
                    result = LocalCmdRunner().run(scp)
                except UnexpectedExit as ex:
                    if is_error_retriable(ex.result.stderr):
                        raise RetriableNetworkException(ex.result.stderr)
                    raise
                self.log.info("Command {} with status {}".format(result.command, result.exited))
                if result.exited:
                    files_received = False
                # Avoid "already printed" message without real error
                if result.stderr:
                    self.log.info("Stderr: {}".format(result.stderr))
                    files_received = False

        if not preserve_perm:
            # we have no way to tell scp to not try to preserve the
            # permissions so set them after copy instead.
            # for rsync we could use "--no-p --chmod=ugo=rwX" but those
            # options are only in very recent rsync versions
            self._set_umask_perms(dst)
        return files_received

    @retrying(n=3, sleep_time=5, allowed_exceptions=(RetriableNetworkException, ))
    def send_files(self, src, dst, delete_dst=False,  # pylint: disable=too-many-arguments,too-many-statements
                   preserve_symlinks=False, verbose=False):
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
        self.log.debug('Send files (src) %s -> (dst) %s', src, dst)
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
                local_sources = [quote(path) for path in src]
                rsync = self._make_rsync_cmd(local_sources, remote_dest,
                                             delete_dst, preserve_symlinks)
                LocalCmdRunner().run(rsync)
                try_scp = False
            except (Failure, UnexpectedExit) as details:
                self.log.warning("Trying scp, rsync failed: %s", details)
                files_sent = False

        if try_scp:
            # scp has no equivalent to --delete, just drop the entire dest dir
            if delete_dst:
                dest_exists = False
                try:
                    result = self.run("test -x %s" % dst, verbose=False)
                    if result.ok:
                        dest_exists = True
                except (Failure, UnexpectedExit):
                    pass

                dest_is_dir = False
                if dest_exists:
                    try:
                        result = self.run("test -d %s" % dst, verbose=False)
                        if result.ok:
                            dest_is_dir = True
                    except (Failure, UnexpectedExit):
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
                except UnexpectedExit as ex:
                    if is_error_retriable(ex.result.stderr):
                        raise RetriableNetworkException(ex.result.stderr)
                    raise
                self.log.info('Command {} with status {}'.format(result.command, result.exited))
                if result.exited:
                    files_sent = False
        return files_sent

    def use_rsync(self):
        if self._use_rsync is not None:
            return self._use_rsync

        # Check if rsync is available on the remote host. If it's not,
        # don't try to use it for any future file transfers.
        self._use_rsync = self._check_rsync()
        if not self._use_rsync:
            self.log.warning("Command rsync not available -- disabled")
        return self._use_rsync

    def _check_rsync(self):
        """
        Check if rsync is available on the remote host.
        """
        result = self.run("rsync --version", ignore_status=True)
        return result.ok

    def _encode_remote_paths(self, paths, escape=True):
        """
        Given a list of file paths, encodes it as a single remote path, in
        the style used by rsync and scp.
        """
        if escape:
            paths = [_scp_remote_escape(path) for path in paths]
        return '%s@[%s]:"%s"' % (self.user, self.hostname, " ".join(paths))

    def _make_scp_cmd(self, src, dst, connect_timeout=300, alive_interval=300):
        """
        Given a list of source paths and a destination path, produces the
        appropriate scp command for encoding it. Remote paths must be
        pre-encoded.
        """
        key_option = ''
        if self.key_file:
            key_option = '-i %s' % os.path.expanduser(self.key_file)
        command = ("scp -r -o StrictHostKeyChecking=no -o BatchMode=yes "
                   "-o ConnectTimeout=%d -o ServerAliveInterval=%d "
                   "-o UserKnownHostsFile=%s -P %d %s %s '%s'")
        return command % (connect_timeout, alive_interval,
                          self.known_hosts_file, self.port, key_option, " ".join(src), dst)

    def _make_rsync_compatible_globs(self, pth, is_local):
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
            return [_scp_remote_escape(pth) + pattern
                    for pattern in patterns]

    def _make_rsync_compatible_source(self, source, is_local):
        """
        Make an rsync compatible source string.

        Applies the same logic as _make_rsync_compatible_globs, but
        applies it to an entire list of sources, producing a new list of
        sources, properly quoted.
        """
        return sum((self._make_rsync_compatible_globs(path, is_local)
                    for path in source), [])

    @staticmethod
    def _set_umask_perms(dest):
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

    def _make_rsync_cmd(self, src, dst, delete_dst, preserve_symlinks):
        """
        Given a list of source paths and a destination path, produces the
        appropriate rsync command for copying them. Remote paths must be
        pre-encoded.
        """
        ssh_cmd = _make_ssh_command(user=self.user, port=self.port,
                                    hosts_file=self.known_hosts_file,
                                    key_file=self.key_file,
                                    extra_ssh_options=self.extra_ssh_options.replace('-tt', '-t'))

        if delete_dst:
            delete_flag = "--delete"
        else:
            delete_flag = ""
        if preserve_symlinks:
            symlink_flag = ""
        else:
            symlink_flag = "-L"
        command = "rsync %s %s --timeout=300 --rsh='%s' -az %s %s"
        return command % (symlink_flag, delete_flag, ssh_cmd,
                          " ".join(src), dst)


class OutputWatcher(StreamWatcher):  # pylint: disable=too-few-public-methods
    def __init__(self, log):
        super(OutputWatcher, self).__init__()
        self.len = 0
        self.log = log

    def submit(self, stream):
        stream_buffer = stream[self.len:]

        while '\n' in stream_buffer:
            out_buf, rest_buf = stream_buffer.split('\n', 1)
            self.log.info('{}'.format(out_buf))
            stream_buffer = rest_buf
        self.len = len(stream) - len(stream_buffer)
        return []


class LogWriteWatcher(StreamWatcher):  # pylint: disable=too-few-public-methods
    def __init__(self, log_file):
        super(LogWriteWatcher, self).__init__()
        self.len = 0
        self.log_file = log_file

    def submit(self, stream):
        stream_buffer = stream[self.len:]

        with open(self.log_file, "a+") as log_file:
            log_file.write(stream_buffer)

        self.len = len(stream)
        return []


class FailuresWatcher(Responder):

    def __init__(self, sentinel, callback=None):
        super(FailuresWatcher, self).__init__(None, None)
        self.sentinel = sentinel
        self.failure_index = 0
        self.callback = callback

    def first_matching_line(self, stream, index):
        new_ = stream[index:]
        for line in new_.splitlines():
            if re.findall(self.sentinel, line, re.S):
                return line
        return None

    def submit(self, stream):
        index = getattr(self, "failure_index")

        # Also check stream for our failure sentinel
        failed = self.pattern_matches(stream, self.sentinel, "failure_index")
        # Error out if we seem to have failed after a previous response.

        if failed:
            line = self.first_matching_line(stream, index)
            err = 'command failed found {!r} in \n{!r}'.format(self.sentinel, line)
            if callable(self.callback):
                self.callback(self.sentinel, line)
            raise OutputCheckError(err)

        return []
