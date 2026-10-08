"""Shared fixtures for the pipeline linter unit tests."""

import logging
import socket

import pytest

# Loopback endpoints stay reachable: only calls leaving the machine mean a cloud API slipped
# past the linter's `_CLOUD_API_PATCHES`.
LOCAL_HOSTS = frozenset({"127.0.0.1", "::1", "localhost", ""})


class RemoteNetworkAccessError(RuntimeError):
    """A test reached the network instead of a stub."""


def is_local_address(address) -> bool:
    """AF_UNIX paths and loopback endpoints are not remote cloud calls."""
    if not isinstance(address, tuple) or not address:
        return True
    return address[0] in LOCAL_HOSTS


@pytest.fixture
def remote_connections(monkeypatch):
    """Refuse every outbound connection and record where it was headed.

    A test asserts the returned list is empty to prove the code under test never reached a cloud
    provider; refusing (rather than only recording) keeps a missing stub from making a real call.
    """
    attempts = []
    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex

    def refuse(address):
        attempts.append(address)
        raise RemoteNetworkAccessError(f"tried to connect to {address!r}")

    def connect(self, address, *args, **kwargs):
        if not is_local_address(address):
            refuse(address)
        return real_connect(self, address, *args, **kwargs)

    def connect_ex(self, address, *args, **kwargs):
        if not is_local_address(address):
            refuse(address)
        return real_connect_ex(self, address, *args, **kwargs)

    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(socket.socket, "connect_ex", connect_ex)
    return attempts


@pytest.fixture
def restore_root_logger():
    """`validate_pipeline` silences the root logger for good; keep that out of the other tests."""
    root = logging.getLogger()
    handlers, disabled = root.handlers, root.disabled
    yield
    root.handlers = handlers
    root.disabled = disabled
