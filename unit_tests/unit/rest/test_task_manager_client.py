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

from sdcm.rest.task_manager_client import TaskManagerClient

LISTED_TASKS = {
    "repair": [
        {"task_id": "running-repair", "state": "running"},
        {"task_id": "finished-repair", "state": "done"},
    ],
    "tablets": [
        {"task_id": "auto-repair-request", "type": "auto_repair", "state": "running"},
        {"task_id": "user-repair-request", "type": "user_repair", "state": "created"},
        {"task_id": "tablet-migration", "type": "migration", "state": "running"},
    ],
}


@pytest.fixture
def task_manager_client(fake_node, monkeypatch):
    """Client whose node lists LISTED_TASKS in each task manager module."""
    monkeypatch.setattr(TaskManagerClient, "list_module_tasks", lambda self, module: LISTED_TASKS[module])
    return TaskManagerClient(fake_node)


def test_get_active_repair_tasks(task_manager_client):
    """A tablet repair request must count as active even when no repair runs on the node.

    Scylla refuses another repair of the tablet until the request completes, also between retries
    after a failed attempt (SCT-1385). Finished repairs and tablet migrations must not count.
    """
    assert [task["task_id"] for task in task_manager_client.get_active_repair_tasks()] == [
        "running-repair",
        "auto-repair-request",
        "user-repair-request",
    ]
