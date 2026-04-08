from textwrap import dedent
from unittest import mock

import pytest
from packaging.version import Version

from sdcm.mgmt.cli import ManagerTask, SCTool


def test_01_get_task_info_dict():
    manager_node_mock = mock.MagicMock()
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
