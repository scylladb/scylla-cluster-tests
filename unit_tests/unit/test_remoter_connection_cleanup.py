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

"""
Tests that a remoter never runs commands over another remoter's connection (SCT-97).

`connection_thread_map` is a class-level `threading.local()` holding the SSH connections of all remoters.
It used to key them by `str(id(self))`. CPython reuses the id of a garbage-collected remoter, so a new
remoter could find the old remoter's connection, still open to another host, and run its commands there:
node-12's init ran `ip -o link` on node-10 and failed with a `KeyError` on its MAC address.
"""

import threading
from unittest.mock import MagicMock

import pytest

from sdcm.remote import remote_base
from sdcm.remote.remote_base import RemoteCmdRunnerBase


class MockRemoter(RemoteCmdRunnerBase):
    exception_unexpected = Exception
    exception_failure = Exception
    exception_retryable = (Exception,)

    def __init__(self, hostname, **kwargs):
        super().__init__(hostname=hostname, user="test", key_file="/tmp/test_key", **kwargs)
        self.created_connections = 0

    def _create_connection(self):
        self.created_connections += 1
        connection = MagicMock()
        connection.hostname = self.hostname
        return connection

    def is_up(self, timeout: float = 30):
        return True

    def _run_on_retryable_exception(self, exc, new_session, suppress_errors=False):
        return False


def _force_shared_id(monkeypatch):
    """Make `id()` return the same value for every remoter in `remote_base`.

    The fixed code doesn't call `id()`, so this changes nothing there. It makes the collision certain if
    the connection key ever goes back to `id(self)`, instead of waiting for CPython to reuse an address.
    """
    monkeypatch.setattr(remote_base, "id", lambda obj: 42, raising=False)


def _use_connection_in_thread(remoter):
    thread = threading.Thread(target=lambda: remoter.connection)
    thread.start()
    thread.join()


# --- connection key ---


def test_connection_key_recycled_id_gets_own_connection(monkeypatch):
    """A new remoter whose id() equals a dead remoter's must still connect to its own host.

    Forces every remoter to share one id(), so the test doesn't depend on CPython reusing a freed address.
    The old remoter is never stopped, as when its last reference is dropped in a thread that never used it.
    """
    _force_shared_id(monkeypatch)
    node10_remoter = MockRemoter(hostname="10.0.0.10")
    assert node10_remoter.connection.hostname == "10.0.0.10"

    node12_remoter = MockRemoter(hostname="10.0.0.12")

    assert node12_remoter.connection.hostname == "10.0.0.12"


# --- cleanup ---


def test_stop_removes_connection_from_thread_map():
    """stop() drops the closed connection from the current thread, so it isn't kept alive."""
    remoter = MockRemoter(hostname="10.0.0.1")
    _ = remoter.connection
    assert hasattr(RemoteCmdRunnerBase.connection_thread_map, remoter._connection_key)

    remoter.stop()

    assert not hasattr(RemoteCmdRunnerBase.connection_thread_map, remoter._connection_key)


def test_stop_in_other_thread_recycled_id_gets_own_connection(monkeypatch):
    """A connection left in a thread where stop() never ran can't reach a new remoter with the same id().

    stop() only clears the thread it runs in, so cleanup alone doesn't protect the worker thread.
    """
    _force_shared_id(monkeypatch)
    old_remoter = MockRemoter(hostname="10.0.0.10")
    connections = {}
    old_remoter_used = threading.Event()
    old_remoter_stopped = threading.Event()

    def worker():
        connections["old"] = old_remoter.connection
        old_remoter_used.set()
        old_remoter_stopped.wait(timeout=10)
        connections["new"] = MockRemoter(hostname="10.0.0.12").connection

    thread = threading.Thread(target=worker)
    thread.start()
    old_remoter_used.wait(timeout=10)
    old_remoter.stop()
    old_remoter_stopped.set()
    thread.join(timeout=10)

    assert connections["old"].hostname == "10.0.0.10"
    assert connections["new"].hostname == "10.0.0.12"


def test_stop_after_failed_init_does_not_raise(monkeypatch):
    """If __init__ fails partway, __del__ -> stop() still works, since the connection key is set first."""

    def fail_mkstemp():
        raise OSError("no space left on device")

    monkeypatch.setattr(remote_base.tempfile, "mkstemp", fail_mkstemp)
    remoter = MockRemoter.__new__(MockRemoter)
    with pytest.raises(OSError):
        remoter.__init__(hostname="10.0.0.4")

    remoter.stop()


def test_close_connection_does_not_create_connection():
    """_close_connection() on a remoter that never connected must not open a connection to close it."""
    remoter = MockRemoter(hostname="10.0.0.2")

    remoter._close_connection()

    assert remoter.created_connections == 0


def test_connection_per_thread_created_once():
    """Each thread gets its own connection, and reuses it on later calls."""
    remoter = MockRemoter(hostname="10.0.0.3")
    main_connection = remoter.connection
    assert remoter.connection is main_connection

    _use_connection_in_thread(remoter)

    assert remoter.created_connections == 2
