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

from typing import List, Optional, Dict
from time import perf_counter, sleep
from os.path import normpath, expanduser, exists
from sys import float_info
from io import StringIO
from warnings import warn
from socket import socket, AF_INET, AF_INET6, SOCK_STREAM, gaierror, gethostbyname, error as sock_error
from threading import Thread, Lock, Event, BoundedSemaphore
from abc import abstractmethod, ABC
from queue import SimpleQueue as Queue
import ipaddress

from ssh2.channel import Channel
from ssh2.exceptions import AuthenticationError
from ssh2.error_codes import LIBSSH2_ERROR_EAGAIN

from .exceptions import AuthenticationException, UnknownHostException, ConnectError, PKeyFileError, UnexpectedExit, \
    CommandTimedOut, FailedToReadCommandOutput, ConnectTimeout, FailedToRunCommand, OpenChannelTimeout
from .result import Result
from .session import Session
from .timings import Timings, NullableTiming


__all__ = ['Session', 'Timings', 'Client', 'Channel', 'FailedToRunCommand']


LINESEP = b'\n'


class __DEFAULT__:
    """ Default value for function attribute when None is not an option """


class StreamWatcher(ABC):
    @abstractmethod
    def submit_line(self, line: str):
        pass


class SSHReaderThread(Thread):
    """
    Thread that reads data from ssh session socket and forwards it to Queue.
    It is needed because socket buffer gets overflowed if data is sent faster than watchers can process it, so
      we have to have Queue as a buffer with 'endless' memory, and fast reader that reads data from the socket
      and forward it to the Queue.
      As part of this process it splits data into lines, because watchers expect it is organized in this way.
    """

    def __init__(self, session: Session, channel: Channel, timeout: NullableTiming, timeout_read_data: NullableTiming):
        self.stdout = Queue()
        self.stderr = Queue()
        self.timeout_reached = False
        self._session = session
        self._channel = channel
        self._timeout = timeout
        self._timeout_read_data = timeout_read_data
        self.raised = None
        self._can_run = Event()
        self._can_run.set()
        super().__init__(daemon=True)

    def run(self):
        try:
            self._read_output(self._session, self._channel, self._timeout,
                              self._timeout_read_data, self.stdout, self.stderr)
        except Exception as exc:  # noqa: BLE001
            self.raised = exc

    def _read_output(
            self, session: Session, channel: Channel, timeout: NullableTiming, timeout_read_data: NullableTiming,
            stdout_stream: Queue, stderr_stream: Queue):
        """Reads data from ssh session, split it into lines and forward lines into stderr ad stdout pipes
        It is required for it to be fast, that is why there is code duplications and non-pythonic code
        """

        stdout_remainder = stderr_remainder = b''
        if timeout is None:
            end_time = float_info.max
        else:
            end_time = perf_counter() + timeout
        eof_result = stdout_size = stderr_size = 1
        while eof_result == LIBSSH2_ERROR_EAGAIN or stdout_size == LIBSSH2_ERROR_EAGAIN or \
                stdout_size > 0 or stderr_size == LIBSSH2_ERROR_EAGAIN or stderr_size > 0:
            if perf_counter() > end_time:
                self.timeout_reached = True
                break
            if not self._can_run.is_set():
                break
            with session.lock:
                if stdout_size == LIBSSH2_ERROR_EAGAIN and stderr_size == LIBSSH2_ERROR_EAGAIN:
                    session.simple_select(timeout=timeout_read_data)
                stdout_size, stdout_chunk = channel.read()
                stderr_size, stderr_chunk = channel.read_stderr()
                eof_result = channel.eof()

            if stdout_chunk and stdout_stream is not None:
                data_splitted = stdout_chunk.split(LINESEP)
                if len(data_splitted) == 1:
                    stdout_remainder = stdout_remainder + data_splitted.pop()
                else:
                    if stdout_remainder:
                        stdout_stream.put(stdout_remainder + data_splitted.pop(0))
                    stdout_remainder = data_splitted.pop()
                for chunk in data_splitted:
                    stdout_stream.put(chunk)

            if stderr_chunk and stderr_stream is not None:
                data_splitted = stderr_chunk.split(LINESEP)
                if len(data_splitted) == 1:
                    stderr_remainder = stderr_remainder + data_splitted.pop()
                else:
                    if stderr_remainder:
                        stderr_stream.put(stderr_remainder + data_splitted.pop(0))
                    stderr_remainder = data_splitted.pop()
                for chunk in data_splitted:
                    stderr_stream.put(chunk)
        if stdout_remainder:
            stdout_stream.put(stdout_remainder)
        if stderr_remainder:
            stderr_stream.put(stderr_remainder)

    def stop(self, timeout: float = None):
        self._can_run.clear()
        self.join(timeout)


class KeepAliveThread(Thread):
    def __init__(self, session: Session, keepalive_timeout: NullableTiming):
        self._keep_running = Event()
        self._keep_running.set()
        self._session = session
        self._keepalive_timeout = keepalive_timeout
        super().__init__(daemon=True)

    def run(self):
        while self._session and self._keep_running.is_set():
            try:
                time_to_wait = self._session.eagain(
                    self._session.keepalive_send, timeout=self._keepalive_timeout)
            except Exception:  # noqa: BLE001
                time_to_wait = self._keepalive_timeout
            sleep(time_to_wait)

    def stop(self):
        self._keep_running.clear()

    def stop_and_wait(self, timeout: float = None):
        self.stop()
        self.join(timeout)

    def __del__(self):
        self.stop_and_wait(1)
        self._keep_running = None


class FloodPreventingFacility(dict):
    """A lock registry.
    Calculate lock hash based on `hash` attribute and data source instance, that is provided to `get_lock`
    To calculate hash it splits `hash` attribute by comma(,),
      for each chunk it reads data source attribute and adds it's value to the hash.
    Get a lock from the store based on that hash, if there is no one - create it.
    The lock is `BoundedSemaphore`, a lock with acquire limit equals to `limit` attribute.
    """
    # TODO: Does not support multiprocessing, look at multiprocessing.Queue.__getstate__ as example
    limit: int = 5
    _hash_items: str = 'host'
    _hash_items_splitted: list = ['host']

    def __init__(self, limit: int = None, hash_items: str = None):
        if limit is not None:
            self.limit = limit
        if hash_items is not None:
            self.hash_items = hash_items
        self._read_lock = Lock()
        super().__init__()

    @property
    def hash_items(self):
        return self._hash_items

    @hash_items.setter
    def hash_items(self, hash_items: str):
        self._hash_items = hash_items
        self._hash_items_splitted = hash_items.split(',')

    def __reduce__(self):
        return self.__class__, (self.limit, self.hash_items)

    def _get_hash(self, session: object) -> str:
        """Calculate hash.
        To calculate hash it splits `hash` attribute by comma(,),
          for each chunk it reads data source attribute and adds it's value to the hash.
        """
        lock_hash = []
        for hash_item in self._hash_items_splitted:
            lock_hash.append(getattr(session, hash_item, 'NONE'))
        return '|'.join(lock_hash)

    def get_lock(self, session: object) -> BoundedSemaphore:
        """Get a lock from the store based on that hash, if there is no any - create it and return.
        """
        lock_hash = self._get_hash(session)
        with self._read_lock:
            lock = self.get(lock_hash, None)
            if lock is None:
                self[lock_hash] = lock = BoundedSemaphore(self.limit)
        return lock

    def acquire(self, session: object, blocking: bool = True):
        """Get a lock from the store based on the hash, if there is no any - create it and acquire.
        """
        self.get_lock(session).acquire(blocking)

    def release(self, session: object):
        """Get a lock from the store based on the hash and release it.
        """
        self[self._get_hash(session)].release()


DEFAULT_FLOOD_PREVENTING = FloodPreventingFacility(hash_items='host', limit=2)


class Client:
    """
    SSH2 Client, partially imitates invoke interface.
    It is not thread safe, so make sure that any given instance is not used by multiple threads.

    How to use:

    >>> from sdcm.remote.libssh2_client import Client
    >>> from sdcm.remote.libssh2_client.exceptions import UnexpectedExit
    >>> cl = Client(host='127.0.0.1', pkey='/home/dkropachev/.ssh/scylla_test_id_ed25519')
    >>> cl.connect()
    >>> try:
    ...     res = cl.run('asdasdasdasd')
    ...     assert False, "Should fail with UnexpectedExit"
    ... except UnexpectedExit:
    ...     pass
    >>>
    >>> res = cl.run('asdasdasdasd', warn=True)
    >>> assert res.exited == 127
    >>> assert res.stdout == ''
    >>> assert res.stderr == ''
    >>> res = cl.run('echo 123')
    >>> assert res.exited == 0
    >>> assert res.stdout.rstrip() == '123'
    >>> assert res.stderr == ''
    >>> res = cl.run('echo 123 >&2')
    >>> assert res.exited == 0
    >>> assert res.stdout == ''
    >>> assert res.stderr.rstrip() == '123'

    """

    # Flood preventing mechanism that prevents target system to be flooded by connection initializations
    #   and authentications
    session = None
    sock = None
    channel_lock = None
    keepalive_thread = None
    forward_requested = False
    host: str = None
    user: str = None
    password: str = None
    port: int = 22
    pkey: str = None
    allow_agent: bool = True
    forward_ssh_agent: bool = False
    proxy_host: str = None
    keepalive_seconds: int = 60
    timings: Timings = Timings()
    flood_preventing: FloodPreventingFacility = DEFAULT_FLOOD_PREVENTING

    def __init__(self, host: str, user: str, password: str = None,
                 port: int = None, pkey: str = None, allow_agent: bool = None, forward_ssh_agent: bool = None,
                 proxy_host: str = None, keepalive_seconds: int = None, timings: Timings = None,
                 flood_preventing: FloodPreventingFacility = None):
        self.host = host
        self.user = user
        if password is not None:
            self.password = password
        if port is not None:
            self.port = port
        if pkey is not None:
            self.pkey = pkey
        if allow_agent is not None:
            self.allow_agent = allow_agent
        if forward_ssh_agent is not None:
            self.forward_ssh_agent = forward_ssh_agent
        if proxy_host is not None:
            self.proxy_host = proxy_host
        if keepalive_seconds is not None:
            self.keepalive_seconds = keepalive_seconds
        if timings is not None:
            self.timings = timings
        if flood_preventing is not None:
            self.flood_preventing = flood_preventing
        self.channel_lock = Lock()
        self.session: Optional[Session] = None
        self.sock: Optional[socket] = None

    def __reduce__(self):
        return self.__class__, (
            self.host, self.user, self.password, self.port, self.pkey, self.allow_agent, self.forward_ssh_agent,
            self.proxy_host, self.keepalive_seconds, self.timings, self.flood_preventing)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.disconnect()

    def _init_ssh(self):
        self.session = Session()
        with self.session.lock:
            if self.timings.ssh_session_timeout:
                # libssh2 timeout is in ms
                self.session.set_timeout(self.timings.ssh_session_timeout * 1000)
            self.session.handshake(self.sock)
            self.session.set_blocking(False)

    def _init_auth(self):
        self._auth()

    def _init_tune(self):
        if self.keepalive_thread:
            self.keepalive_thread.stop()
        if self.timings.keepalive_timeout:
            # This part of logic is disabled
            # TODO: Make sure it works with no keep-alive packets and remove it if it does
            with self.session.lock:
                self.session.keepalive_config(False, self.timings.keepalive_timeout)
            self.keepalive_thread = KeepAliveThread(
                self.session, keepalive_timeout=self.timings.keepalive_sending_timeout)
            self.keepalive_thread.start()

    def _auth(self):
        if self.pkey is not None:
            self._pkey_auth()
            return
        if self.allow_agent:
            try:
                with self.session.lock:
                    self.session.agent_auth(self.user)
                return
            except Exception:  # noqa: BLE001
                pass
        self._password_auth()

    def _pkey_auth(self):
        self.session.eagain(
            self.session.userauth_publickey_fromfile, args=(
                self.user,
                self._validate_pkey_path(self.pkey, self.host),
                self.password if self.password is not None else ''))

    @staticmethod
    def _validate_pkey_path(pkey: str, host: str = None) -> Optional[str]:
        if pkey is None:
            return None
        pkey = normpath(expanduser(pkey))
        if not exists(pkey):
            msg = "File %s does not exist. " \
                  "Please use either absolute or relative to user directory " \
                  "paths like '~/.ssh/my_key' for pkey parameter"
            ex = PKeyFileError(msg, pkey)
            ex.host = host
            raise ex
        return pkey

    def _password_auth(self):
        try:
            self.session.eagain(
                self.session.userauth_password,
                args=(self.user, self.password), timeout=self.timings.auth_timeout)
        except Exception as error:  # noqa: BLE001
            raise AuthenticationException("Password authentication failed") from error

    @staticmethod
    def _get_socket_family(host):
        try:
            tmp = ipaddress.ip_address(host)
            if isinstance(tmp, ipaddress.IPv4Address):
                return AF_INET
            return AF_INET6
        except ValueError:
            return None

    def _init_socket(self, host: str, port: int):
        if self.sock:
            try:
                self.sock.close()
            except Exception:  # noqa: BLE001
                pass
        family = self._get_socket_family(host)
        if family is None:
            family = self._get_socket_family(gethostbyname(host))
            if family is None:
                raise ValueError(f"Can't resolve '{host}' to and ip")
        self.sock = socket(family, SOCK_STREAM)
        if self.timings.socket_timeout:
            self.sock.settimeout(self.timings.socket_timeout)
        try:
            self.sock.connect((host, port))
        except gaierror as ex:
            raise UnknownHostException("Unknown host %s - %s" % (host, str(ex.args[1]))) from ex
        except sock_error as ex:
            error_type = ex.args[1] if len(ex.args) > 1 else ex.args[0]
            raise ConnectError("Error connecting to host '%s:%s' - %s" % (host, port, str(error_type))) from ex

    @staticmethod
    def _process_output(watchers: List[StreamWatcher], encoding: str, stdout_stream: StringIO, stderr_stream: StringIO,
                        reader: SSHReaderThread, timeout: NullableTiming, timeout_read_data_chunk: NullableTiming):
        """Separate different approach for the case when watchers are present, since watchers are slow,
          we can loose data due to the socket buffer limit, if endpoint sending it faster than watchers can read it.
        To avoid that we run `SSHReaderThread` thread that picks data up from the socket, splits it into lines
        and puts it to stdout and stderr `Queues`.
        Meanwhile this function reads data from these `Queues` and store them in StringIO and throw it to the watchers
        """
        reader.start()
        if timeout:
            end_time = perf_counter() + timeout
        else:
            end_time = float_info.max
        while reader.is_alive() or reader.stdout.qsize() or reader.stderr.qsize():
            if perf_counter() > end_time:
                reader.stop()
                return False

            data_processed = False
            if stdout_stream is not None and reader.stdout.qsize():
                try:
                    stdout = reader.stdout.get(timeout=timeout_read_data_chunk)
                    data = stdout.decode(encoding, errors='ignore') + '\n'
                    stdout_stream.write(data)
                    for watcher in watchers:
                        watcher.submit_line(data)
                    data_processed = True
                except Exception:  # noqa: BLE001
                    pass
            if stderr_stream is not None and reader.stderr.qsize():
                try:
                    stderr = reader.stderr.get(timeout=timeout_read_data_chunk)
                    data = stderr.decode(encoding) + '\n'
                    stderr_stream.write(data)
                    for watcher in watchers:
                        watcher.submit_line(data)
                    data_processed = True
                except Exception:  # noqa: BLE001
                    pass
            if not data_processed:  # prevent unbounded loop in case no data on the streams
                sleep(timeout_read_data_chunk or 0.5)
        return True

    @staticmethod
    def _process_output_no_watchers(
            session: Session, channel: Channel, encoding: str, stdout_stream: StringIO,
            stderr_stream: StringIO, timeout: NullableTiming, timeout_read_data_chunk: NullableTiming) -> bool:
        eof_result = stdout_size = stderr_size = LIBSSH2_ERROR_EAGAIN
        if timeout:
            end_time = perf_counter() + timeout
        else:
            end_time = float_info.max
        while eof_result == LIBSSH2_ERROR_EAGAIN or stdout_size == LIBSSH2_ERROR_EAGAIN or stdout_size > 0 or \
                stderr_size == LIBSSH2_ERROR_EAGAIN or stderr_size > 0:
            if perf_counter() > end_time:
                return False
            with session.lock:
                if stdout_size == LIBSSH2_ERROR_EAGAIN and stderr_size == LIBSSH2_ERROR_EAGAIN:
                    session.simple_select(timeout=timeout_read_data_chunk)
                eof_result = channel.wait_eof()
                stdout_size, stdout_chunk = channel.read()
                stderr_size, stderr_chunk = channel.read_stderr()
            if stdout_chunk and stdout_stream is not None:
                stdout_stream.write(stdout_chunk.decode(encoding))
            if stderr_chunk and stderr_stream is not None:
                stderr_stream.write(stderr_chunk.decode(encoding))
        return True

    def check_if_alive(self, timeout: NullableTiming = __DEFAULT__):
        """Check and return if endpoint is capable of running commands
        """
        if timeout is __DEFAULT__:
            timeout = self.timings.check_if_alive_timeout
        return self.run('true', timeout=timeout).ok

    def connect(self, timeout: NullableTiming = __DEFAULT__):
        """Connect to the endpoint, initiate ssh and authenticate.
        """
        if timeout is __DEFAULT__:
            timeout = self.timings.connect_timeout
        if not timeout:
            try:
                with self.flood_preventing.get_lock(self):
                    self._connect()
            except Exception as exc:  # noqa: BLE001
                self.disconnect()
                raise ConnectError(str(exc)) from exc
            return
        end_time = perf_counter() + timeout
        delays_iter = iter(self.timings.connect_delays)
        delay = next(delays_iter)
        while True:
            with self.flood_preventing.get_lock(self):
                try:
                    self._connect()
                    break
                except AuthenticationError:
                    self.disconnect()
                    raise
                except Exception as exc:  # noqa: BLE001
                    self.disconnect()
                    if perf_counter() > end_time:
                        ex_msg = f'Failed to connect in {timeout} seconds, last error: ({type(exc).__name__}){str(exc)}'
                        raise ConnectTimeout(ex_msg) from exc
            delay = next(delays_iter, delay)
            sleep(delay)
        return

    def _connect(self):
        self._init_socket(self.host, self.port)
        self._init_ssh()
        self._init_auth()
        self._init_tune()

    def disconnect(self):
        """Disconnect session, close socket if needed."""
        if self.keepalive_thread:
            self.keepalive_thread.stop()
            self.keepalive_thread = None
        if self.session is not None:
            try:
                self.session.eagain(self.session.disconnect)
            except Exception:  # noqa: BLE001
                pass
            del self.session
            self.session = None
        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:  # noqa: BLE001
                pass
            self.sock = None

    def run(
            self, command: str, warn: bool = False, encoding: str = 'utf-8',
            hide=True, watchers=None, env=None, replace_env=False, in_stream=False, timeout=None) -> Result:
        """Run command, wait till it ends and return result in Result class.
        If `watchers` are defined it runs `SSHReaderThread` that reads data from the socket and forwards it to Queue.
        If `hide` is True it does not collect stdout and stderr.
        if `env` is set it loads variables from the dict to the session environment.
        Returns: instance of `Result`
        """
        if timeout is None:
            timeout = self.timings.read_command_output_timeout
        exception = None
        timeout_reached = False
        stdout = StringIO()
        stderr = StringIO()
        # TODO: Implement replace_env
        if env is None:
            shell = '/bin/bash'
        else:
            shell = env.get('SHELL', '/bin/bash')
        result = Result(
            command=command,
            encoding=encoding,
            env=env,
            hide=('stderr', 'stdout') if hide else (),
            pty=False,
            exited=None,
            shell=shell,
            stdout='',
            stderr=''
        )
        channel: Optional[Channel] = None
        try:
            if self.session is None:
                self.connect()
            channel = self.open_channel()
        except Exception as exc:  # noqa: BLE001
            return self._complete_run(
                channel, FailedToRunCommand(result, exc), timeout_reached, timeout, result, warn, stdout, stderr)
        try:
            self._apply_env(channel, env)
        except Exception as exc:  # noqa: BLE001
            return self._complete_run(
                channel, FailedToRunCommand(result, exc), timeout_reached, timeout, result, warn, stdout, stderr)
        if watchers:
            reader = SSHReaderThread(self.session, channel, timeout, self.timings.interactive_read_data_chunk_timeout)
            try:

                self.execute(command, channel=channel, use_pty=False)
                self._process_output(watchers, encoding, stdout, stderr, reader, timeout,
                                     self.timings.interactive_read_data_chunk_timeout)
            except Exception as exc:  # noqa: BLE001
                exception = FailedToReadCommandOutput(result, exc)
            if reader.is_alive():
                reader.stop()
            if exception is None and reader.raised:
                exception = reader.raised
            timeout_reached = reader.timeout_reached
        else:
            try:
                self.execute(command, channel=channel, use_pty=False)
                timeout_reached = not self._process_output_no_watchers(
                    self.session, channel, encoding, stdout, stderr, timeout,
                    self.timings.read_data_chunk_timeout)
            except Exception as exc:  # noqa: BLE001
                exception = FailedToReadCommandOutput(result, exc)
        return self._complete_run(channel, exception, timeout_reached, timeout, result, warn, stdout, stderr)

    @staticmethod
    def _apply_env(channel: Channel, env: Dict[str, str]):
        if env:
            for var, val in env.items():
                channel.setenv(str(var), str(val))

    def _complete_run(self, channel: Channel, exception: Exception,
                      timeout_reached: NullableTiming, timeout: NullableTiming, result: Result, warn,
                      stdout: StringIO, stderr: StringIO) -> Result:
        """Complete executing command and return result, no matter what had happened.
        """
        exit_status = None
        result.stdout = stdout.getvalue()
        result.stderr = stderr.getvalue()
        if channel is not None:
            try:
                with self.session.lock:
                    channel.close()
            except Exception as exc:  # noqa: BLE001
                print(f'Failed to close channel due to the following error: {exc}')
            try:
                self.session.eagain(channel.wait_closed, timeout=self.timings.channel_close_timeout)
            except Exception as exc:  # noqa: BLE001
                print(f'Failed to close channel due to the following error: {exc}')
            exit_status = channel.get_exit_status()
            self.session.drop_channel(channel)
            result.exited = exit_status
        if exception:
            raise exception
        if timeout_reached:
            raise CommandTimedOut(result, timeout)
        if not warn:
            if exit_status != 0:
                raise UnexpectedExit(result)
        return result

    def execute(self, cmd: str, use_pty: bool = False, channel: Channel = None) -> Channel:
        """Execute command on remote server
        """
        channel = self.open_channel() if channel is None else channel
        if use_pty:
            self.session.eagain(channel.pty)
        self.session.eagain(channel.execute, args=(cmd,))
        return channel

    def open_channel(self) -> Channel:
        """Open new channel in ssh2 session"""
        chan = LIBSSH2_ERROR_EAGAIN
        timeout = self.timings.open_channel_timeout
        if timeout:
            end_time = perf_counter() + timeout
        else:
            end_time = float_info.max
        delay_iter = iter(self.timings.open_channel_delays)
        delay = next(delay_iter)
        while chan == LIBSSH2_ERROR_EAGAIN:
            try:
                chan = self.session.eagain(self.session.open_session)
                if chan != LIBSSH2_ERROR_EAGAIN:
                    break
            except Exception:  # noqa: BLE001
                pass
            delay = next(delay_iter, delay)
            sleep(delay)
            current_time = perf_counter()
            if current_time > end_time:
                raise OpenChannelTimeout(f'Failed to open channel in {timeout} seconds')
        # Multiple forward requests result in ChannelRequestDenied
        # errors, flag is used to avoid this.
        if self.forward_ssh_agent and not self.forward_requested:
            if not hasattr(chan, 'request_auth_agent'):
                warn("Requested SSH Agent forwarding but libssh2 version used "
                     "does not support it - ignoring")
                return chan
            self.session.eagain(chan.request_auth_agent)
            self.forward_requested = True
        return chan

    def close_channel(self, channel: Channel):
        """Close ssh channel, opened by `open_channel`
        """
        self.session.eagain(channel.close, timeout=self.timings.close_channel_timeout)

    open = connect

    def close(self):
        """Close ssh connection, opened by `open` or `connect`
        """
        self.disconnect()
