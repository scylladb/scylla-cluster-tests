from textwrap import dedent
from unittest import mock

import pytest
from packaging.version import Version

from sdcm.mgmt.common import ScyllaManagerError
from sdcm.mgmt.cli import ManagerCluster, ManagerTask, SCTool, SCToolDocker, create_sctool


def test_01_get_task_info_dict():
    manager_node_mock = mock.MagicMock()
    # This test stubs remoter.sudo, i.e. the host sctool runner; say so explicitly, or
    # create_sctool() sees a truthy MagicMock from is_docker() and picks SCToolDocker.
    manager_node_mock.is_docker.return_value = False
    remoter_result = mock.MagicMock()
    stdout = mock.PropertyMock(
        return_value=dedent("""Name:      healthcheck/cql
        Cron:     @every 15s
        Tz:       UTC

        Properties:
        - mode: cql

        ╭──────────────────────────────────────┬────────────────────────┬──────────┬────────╮
        │ ID                                   │ Start time             │ Duration │ Status │
        ├──────────────────────────────────────┼────────────────────────┼──────────┼────────┤
        │ 13814000-1dd2-11b2-a009-02c33d089f9b │ 07 Jan 23 23:08:59 UTC │ 0s       │ DONE   │
        ╰──────────────────────────────────────┴────────────────────────┴──────────┴────────╯""")
    )
    stderr = mock.PropertyMock(return_value=None)
    exited = mock.PropertyMock(return_value=0)
    type(remoter_result).stdout = stdout
    type(remoter_result).stderr = stderr
    type(remoter_result).exited = exited
    manager_node_mock.remoter.sudo.return_value = remoter_result
    task = ManagerTask(
        task_id="13814000-1dd2-11b2-a009-02c33d089f9b",
        cluster_id="8c20f334-cf37-4528-9219-862d75b84c99",
        manager_node=manager_node_mock,
    )

    assert task.get_task_info_dict() == {
        "Name": "healthcheck/cql",
        "Cron": "@every 15s",
        "Tz": "UTC",
        "Properties": "",
        "mode": "cql",
        "history": [
            ["", "ID", "Start time", "Duration", "Status"],
            ["", "13814000-1dd2-11b2-a009-02c33d089f9b", "07 Jan 23 23:08:59 UTC", "0s", "DONE"],
        ],
    }


@pytest.mark.parametrize(
    "version_string,expected",
    [
        # Plain PEP-440 version
        ("3.8.1", Version("3.8.1")),
        # Non-PEP-440 build-metadata suffix that real manager produces
        ("3.9.0-dev-0.20260306.76e78d56e-SNAPSHOT", Version("3.9.0")),
        # Another real-world-style string with extra segments after the dash
        ("3.8.0-0.20260213.3882815ee", Version("3.8.0")),
    ],
)
def test_parsed_client_version(version_string, expected):
    """parsed_client_version must not raise on non-PEP-440 manager version strings."""
    manager_node_mock = mock.MagicMock()
    sctool = SCTool(manager_node=manager_node_mock)
    with mock.patch.object(type(sctool), "client_version", new_callable=mock.PropertyMock, return_value=version_string):
        result = sctool.parsed_client_version
    assert result == expected


def _manager_node_mock(*, is_docker, container_name="sct-manager-server"):
    node = mock.MagicMock()
    node.is_docker.return_value = is_docker
    node.parent_cluster.manager_container_name = container_name if is_docker else None
    return node


@pytest.mark.parametrize(
    "is_docker, expected_runner",
    [(False, SCTool), (True, SCToolDocker)],
    ids=["host", "docker"],
)
def test_every_sctool_user_gets_the_backend_specific_runner(is_docker, expected_runner):
    """A ManagerCluster or a task must not fall back to the host runner on Docker.

    On Docker sctool only exists inside the manager container. Objects that built their own
    plain SCTool would run `sudo sctool` on the monitor node, where there is no such binary,
    so `cluster list` returns nothing and every backup/repair fails.
    """
    node = _manager_node_mock(is_docker=is_docker)

    # SCToolDocker subclasses SCTool, so isinstance() would pass either way -- compare exact types.
    assert type(create_sctool(manager_node=node)) is expected_runner
    assert type(ManagerCluster(manager_node=node, cluster_id="c1").sctool) is expected_runner
    assert type(ManagerTask(task_id="t1", cluster_id="c1", manager_node=node).sctool) is expected_runner


def test_docker_sctool_without_a_container_name_fails_loudly():
    node = _manager_node_mock(is_docker=True, container_name=None)

    with pytest.raises(ScyllaManagerError, match="manager_container_name"):
        create_sctool(manager_node=node)
