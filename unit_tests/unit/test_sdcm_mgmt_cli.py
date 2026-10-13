from textwrap import dedent
from unittest import mock

import pytest
from packaging.version import Version

from sdcm.mgmt.cli import (
    BackupStageNotReachedError,
    BackupTask,
    ManagerCluster,
    ManagerTask,
    SCTool,
    SCToolDocker,
    create_sctool,
)
from sdcm.mgmt.common import ScyllaManagerError


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


def _manager_node_mock(*, is_docker, is_kubernetes=False, container_name="sct-manager-server"):
    node = mock.MagicMock()
    node.is_docker.return_value = is_docker
    node.is_kubernetes.return_value = is_kubernetes
    node.parent_cluster.manager_container_name = container_name if is_docker else None
    return node


@pytest.mark.parametrize(
    "is_docker, is_kubernetes, expected_runner",
    [(False, False, SCTool), (True, False, SCToolDocker), (True, True, SCTool)],
    ids=["host", "docker", "k8s-pod"],
)
def test_every_sctool_user_gets_the_backend_specific_runner(is_docker, is_kubernetes, expected_runner):
    """A ManagerCluster or a task must not fall back to the host runner on Docker.

    On Docker sctool only exists inside the manager container. Objects that built their own
    plain SCTool would run `sudo sctool` on the monitor node, where there is no such binary,
    so `cluster list` returns nothing and every backup/repair fails.
    """
    node = _manager_node_mock(is_docker=is_docker, is_kubernetes=is_kubernetes)

    # SCToolDocker subclasses SCTool, so isinstance() would pass either way -- compare exact types.
    assert type(create_sctool(manager_node=node)) is expected_runner
    assert type(ManagerCluster(manager_node=node, cluster_id="c1").sctool) is expected_runner
    assert type(ManagerTask(task_id="t1", cluster_id="c1", manager_node=node).sctool) is expected_runner


def test_docker_sctool_without_a_container_name_fails_loudly():
    node = _manager_node_mock(is_docker=True, container_name=None)

    with pytest.raises(ScyllaManagerError, match="manager_container_name"):
        create_sctool(manager_node=node)


def backup_task_with_progress_statuses(statuses):
    """BackupTask whose consecutive `sctool progress` calls report the given `Status:` values."""
    task = BackupTask(
        task_id="backup/2a4125d6-5d5a-45b9-9d8d-dec038b3732d", cluster_id="c1", manager_node=mock.MagicMock()
    )
    task.sctool.run = mock.Mock(
        side_effect=[
            mock.Mock(stdout=f"Run:\t\t{i}\nStatus:\t\t{status}\nSnapshot Tag:\tsm_x\n")
            for i, status in enumerate(statuses)
        ]
    )
    return task


@pytest.mark.parametrize(
    "statuses,expected_calls",
    [
        (["RUNNING (taking snapshot)", "RUNNING (uploading data)"], 2),
        (["RUNNING (indexing files)", "RUNNING (uploading schema)", "RUNNING (moving manifests)"], 3),
        (["RUNNING (retention)"], 1),
    ],
)
def test_wait_for_started_upload_stage_returns_once_the_upload_started(statuses, expected_calls):
    task = backup_task_with_progress_statuses(statuses)
    with mock.patch("sdcm.utils.decorators.time.sleep"):
        assert task.wait_for_started_upload_stage(timeout=60) is True
    assert task.sctool.run.call_count == expected_calls


@pytest.mark.parametrize(
    "final_status",
    [
        "DONE",
        "ERROR (taking snapshot)",
        "ERROR (uploading data)",
        "STOPPED (uploading data)",
        "ABORTED (indexing files)",
        "WAITING (uploading data)",
    ],
)
def test_wait_for_stage_fails_fast_once_the_task_is_not_running(final_status):
    task = backup_task_with_progress_statuses(["RUNNING (taking snapshot)", final_status])
    with mock.patch("sdcm.utils.decorators.time.sleep"), pytest.raises(ScyllaManagerError, match="can't be reached"):
        task.wait_for_uploading_stage(timeout=60)
    assert task.sctool.run.call_count == 2


@pytest.mark.parametrize("status", ["NEW", "STARTING"])
def test_wait_for_stage_keeps_waiting_while_the_task_has_not_started(status):
    task = backup_task_with_progress_statuses([status, status, "RUNNING (uploading data)"])
    with mock.patch("sdcm.utils.decorators.time.sleep"):
        assert task.wait_for_uploading_stage(timeout=60) is True
    assert task.sctool.run.call_count == 3


@pytest.mark.parametrize("stage", ["taking snapshot", "indexing files", "uploading manifests", "uploading schema"])
def test_wait_for_started_upload_stage_keeps_waiting_before_the_upload(stage):
    task = backup_task_with_progress_statuses([f"RUNNING ({stage})"] * 3)
    with (
        mock.patch("sdcm.utils.decorators.time.sleep"),
        mock.patch("sdcm.utils.decorators.time.time", side_effect=[0, 5, 100]),
        pytest.raises(BackupStageNotReachedError),
    ):
        task.wait_for_started_upload_stage(timeout=10)
