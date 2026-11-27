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
import pytest

from sdcm.remote.libssh2_client import Result


class FakeRemoter:
    """Remoter stand-in that echoes the command back as stdout."""

    def run(self, cmd: str, timeout: int, retry: int) -> Result:
        """Return a successful Result whose stdout is the command itself."""
        return Result(stdout=cmd, stderr="", exited=0)


class FakeNode:
    """Node stand-in whose remoter echoes commands, so tests can assert the built curl command."""

    @property
    def remoter(self):
        """Return a FakeRemoter."""
        return FakeRemoter()


@pytest.fixture
def fake_node():
    """Provide a FakeNode for the REST client tests."""
    return FakeNode()
