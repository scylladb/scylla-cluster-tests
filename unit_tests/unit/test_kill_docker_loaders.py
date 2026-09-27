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

"""Tests for the stress containers cleanup done by 'BaseLoaderSet.kill_docker_loaders'."""

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
    for node in loader_set.nodes:
        node.remoter.run.return_value.stderr = ""
    loader_set.tags = {"TestId": TEST_ID}
    loader_set.kill_docker_loaders = MethodType(BaseLoaderSet.kill_docker_loaders, loader_set)
    return loader_set


def test_kill_docker_loaders_removes_stress_containers_only(loader_set):
    loader_set.kill_docker_loaders()

    # NOTE: 'shell_marker' is set on the stress containers only, so the db, monitoring and
    #       vector-store containers of the same test survive the cleanup, and 'xargs -r' makes an
    #       empty match a no-op. Compare the whole command rather than those parts: substring
    #       checks also pass on a dropped pipe or on a second, unfiltered sweep appended to it.
    expected_cmd = (
        f"docker ps -a -q --filter label=TestId={TEST_ID} --filter label=shell_marker | xargs -r docker rm -f"
    )
    for node in loader_set.nodes:
        node.remoter.run.assert_called_once()
        assert node.remoter.run.call_args.kwargs["cmd"] == expected_cmd
        # NOTE: a cached connection to a vanished loader hangs on opening a channel (SCT-711)
        assert node.remoter.run.call_args.kwargs["new_session"] is True
    # NOTE: docker access was not denied, so no diagnostics are collected
    loader_set.log.warning.assert_not_called()


def test_kill_docker_loaders_aborts_commands_on_unreachable_loader(loader_set):
    alive_loader, preempted_loader = loader_set.nodes
    preempted_loader.remoter.run.side_effect = ConnectionError("host is not reachable")

    loader_set.kill_docker_loaders()

    # NOTE: stress commands on a vanished loader never end by themselves (SCT-711)
    preempted_loader.remoter.abort_running_commands.assert_called_once_with()
    alive_loader.remoter.abort_running_commands.assert_not_called()


@pytest.mark.parametrize(
    "docker_denied",
    [
        pytest.param(
            "permission denied while trying to connect to the docker API at unix:///var/run/docker.sock",
            id="docker-29-and-newer",
        ),
        pytest.param(
            "permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock",
            id="docker-28-and-older",
        ),
    ],
)
def test_kill_docker_loaders_logs_diagnostics_when_docker_access_is_denied(loader_set, docker_denied):
    """A denied docker access is reported with the session and socket permissions, not as a kill."""
    denied, allowed = loader_set.nodes
    denied.remoter.run.return_value.stderr = docker_denied

    loader_set.kill_docker_loaders()

    denied.remoter.run.assert_called_with(
        "id -nG; getent group docker; ls -l /var/run/docker.sock; ps -o pid=,lstart=,args= -p $PPID",
        verbose=True,
        ignore_status=True,
        new_session=True,
    )
    assert denied.remoter.run.call_count == 2
    loader_set.log.warning.assert_called_once_with(
        "Docker access denied on %s, stress containers are not removed", denied.name
    )
    allowed.remoter.run.assert_called_once()
    loader_set.log.info.assert_called_once_with("Killed docker loader on node: %s", allowed.name)
