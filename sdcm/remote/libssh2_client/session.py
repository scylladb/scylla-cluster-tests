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

from gc import collect as gc_collect
from select import select
from threading import Lock

from ssh2.session import Session as LibSSH2Session, LIBSSH2_SESSION_BLOCK_INBOUND, LIBSSH2_SESSION_BLOCK_OUTBOUND
from ssh2.exceptions import SocketRecvError
from ssh2.error_codes import LIBSSH2_ERROR_EAGAIN
from ssh2.channel import Channel

from .timings import NullableTiming


class Session(LibSSH2Session):
    """Custom SSH2 Session class with Lock in it, to make it thread safe where it is needed """

    # Libssh2 is currently not thread safe, we have work it around by having separete session for each thread
    #   but garbage collection part of the issue still not fixed
    # channels, drop_channel, __del__ and open_session are part of the workaround gc part of the issue.

    def __init__(self):
        # A lock that is used to make it thread safe
        self.lock = Lock()
        self.channels = []
        super().__init__()

    def simple_select(self, timeout: NullableTiming = None):
        """Perform single select on ssh2 session socket.
        It is standalone-function because it is an candidate to be compiled via Cython
        """
        try:
            _socket = self.sock
            directions = self.block_directions()
            if directions == 0:
                return
            readfds = (_socket,) \
                if (directions & LIBSSH2_SESSION_BLOCK_INBOUND) else ()
            writefds = (_socket,) \
                if (directions & LIBSSH2_SESSION_BLOCK_OUTBOUND) else ()
            select(readfds, writefds, (), timeout)
        except (ValueError, SocketRecvError):  # under high load it can throw these errors, on next try it will be ok
            pass

    def eagain(self, func, args=(), kwargs={},  # noqa: B006
               timeout: NullableTiming = None) -> int:
        """Running function followed by simple_select up until it return anything but `LIBSSH2_ERROR_EAGAIN`"""
        with self.lock:
            ret = func(*args, **kwargs)
            while ret == LIBSSH2_ERROR_EAGAIN:
                self.simple_select(timeout=timeout)
                ret = func(*args, **kwargs)
            return ret

    def open_session(self):
        channel = super().open_session()
        if isinstance(channel, Channel):
            self.channels.append(channel)
        return channel

    def drop_channel(self, channel: Channel):
        if channel in self.channels:
            self.channels.remove(channel)
        try:
            channel.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            channel.wait_closed()
        except Exception:  # noqa: BLE001
            pass
        del channel

    def __del__(self):
        if self.channels:
            while self.channels:
                self.drop_channel(self.channels.pop())
            gc_collect()
