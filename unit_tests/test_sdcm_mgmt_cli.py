from textwrap import dedent
from unittest import mock

from sdcm.mgmt.cli import ManagerTask


def test_01_get_task_info_dict():
    manager_node_mock = mock.MagicMock()
    remoter_result = mock.MagicMock()
    stdout = mock.PropertyMock(return_value=dedent("""Name:      healthcheck/cql
        Cron:     @every 15s
        Tz:       UTC

        Properties:
        - mode: cql

        ╭──────────────────────────────────────┬────────────────────────┬──────────┬────────╮
        │ ID                                   │ Start time             │ Duration │ Status │
        ├──────────────────────────────────────┼────────────────────────┼──────────┼────────┤
        │ 13814000-1dd2-11b2-a009-02c33d089f9b │ 07 Jan 23 23:08:59 UTC │ 0s       │ DONE   │
        ╰──────────────────────────────────────┴────────────────────────┴──────────┴────────╯"""))
    stderr = mock.PropertyMock(return_value=None)
    exited = mock.PropertyMock(return_value=0)
    type(remoter_result).stdout = stdout
    type(remoter_result).stderr = stderr
    type(remoter_result).exited = exited
    manager_node_mock.remoter.sudo.return_value = remoter_result
    task = ManagerTask(task_id='13814000-1dd2-11b2-a009-02c33d089f9b',
                       cluster_id='8c20f334-cf37-4528-9219-862d75b84c99',
                       manager_node=manager_node_mock)

    assert task.get_task_info_dict() == {
        'Name': 'healthcheck/cql',
        'Cron': '@every 15s',
        'Tz': 'UTC',
        'Properties': '',
        'mode': 'cql',
        'history': [
            ['', 'ID', 'Start time', 'Duration', 'Status'],
            ['', '13814000-1dd2-11b2-a009-02c33d089f9b', '07 Jan 23 23:08:59 UTC', '0s', 'DONE']
        ]
    }
