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

"""Tests for the loaders cleanup done by 'BaseLoaderSet.kill_docker_loaders'."""

from types import MethodType
from unittest.mock import MagicMock

import pytest

from sdcm.cluster import BaseLoaderSet

TEST_ID = "7001f1a8-74b6-48a7-8a04-395da916410e"


@pytest.fixture(name="loader_set")
def fixture_loader_set():
    """Bind the real 'kill_docker_loaders' to a mock, so only its remoter calls are faked."""
    loader_set = MagicMock()
    loader_set.nodes = [MagicMock(), MagicMock()]
    loader_set.tags = {"TestId": TEST_ID}
    loader_set.kill_docker_loaders = MethodType(BaseLoaderSet.kill_docker_loaders, loader_set)
    return loader_set


def test_kill_docker_loaders_uses_new_session(loader_set):
    loader_set.kill_docker_loaders()

    for node in loader_set.nodes:
        node.remoter.run.assert_called_once()
        # NOTE: a cached connection to a vanished loader hangs on opening a channel (SCT-711)
        assert node.remoter.run.call_args.kwargs["new_session"] is True


def test_kill_docker_loaders_aborts_commands_on_unreachable_loader(loader_set):
    alive_loader, preempted_loader = loader_set.nodes
    preempted_loader.remoter.run.side_effect = ConnectionError("host is not reachable")

    loader_set.kill_docker_loaders()

    # NOTE: stress commands on a vanished loader never end by themselves (SCT-711)
    preempted_loader.remoter.abort_running_commands.assert_called_once_with()
    alive_loader.remoter.abort_running_commands.assert_not_called()
