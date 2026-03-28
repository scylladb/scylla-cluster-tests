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
# Copyright (c) 2017 ScyllaDB

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from sdcm import wait

if TYPE_CHECKING:
    from sdcm.cluster import BaseNode

LOGGER = logging.getLogger(__name__)


def parse_scylla_task_list(task_list_output: str) -> list[dict]:
    """
    Parse the output of `nodetool tasks list <module>`.

    Example for module node_ops:

    task_id                              type         kind    scope   state   sequence_number keyspace table entity                               shard start_time           end_time
    3e1352f2-f851-11f0-baee-91a4c04b9eaf decommission cluster cluster running 0                              c6b9b533-2a7b-489c-975f-b06be7f34f21 0
    98dcba80-f850-11f0-8e7a-fb0a23d80219 bootstrap    cluster cluster done    0                                                                   0     2026-01-23T11:42:32Z 2026-01-23T11:42:33Z
    98a94e70-f850-11f0-aee7-d796348a2472 bootstrap    cluster cluster done    0                                                                   0     2026-01-23T11:42:33Z 2026-01-23T11:42:34Z
    98d2cf70-f850-11f0-a951-16cbcc0cf770 bootstrap    cluster cluster done    0                                                                   0     2026-01-23T11:42:31Z 2026-01-23T11:42:32Z
    98a94e70-f850-11f0-aa78-9ebdd96d0853 bootstrap    cluster cluster done    0                                                                   0     2026-01-23T11:42:34Z 2026-01-23T11:42:36Z
    984e3620-f850-11f0-8eb1-21d470e96ad2 bootstrap    cluster cluster done    0                                                                   0     2026-01-23T11:42:29Z 2026-01-23T11:42:31Z
    """
    result = []
    lines = task_list_output.strip().splitlines()
    if not lines:
        raise ValueError(f"Tasks list output is empty, {task_list_output=}")

    header_line, *task_lines = lines

    # Find column names and their starting positions
    columns = []
    for match in re.finditer(r"(\S+)", header_line):
        columns.append((match.group(1), match.start()))

    # Parse each task line
    for task_line in task_lines:
        if not task_line:
            continue

        row = {}
        for i, (col_name, start_pos) in enumerate(columns):
            # End position is either the start of next column or end of line
            if i + 1 < len(columns):
                end_pos = columns[i + 1][1]
            else:
                end_pos = len(task_line)

            # Extract value and strip whitespace
            value = task_line[start_pos:end_pos].strip()
            row[col_name] = value or None

        result.append(row)

    return result


def wait_for_tasks(node: "BaseNode", module: str, timeout: int = 300, filter: dict = None):
    """
    Wait for a Scylla task to complete on the given node.

    :param node: The Scylla node where nodetool commands are run.
    :param module: parameter for `nodetool tasks list <module>`.
    :param timeout: Maximum time to wait for the task.
    :param filter: Key-value pairs to match against task attributes.

    Example:
        tasks = wait_for_tasks(node, "node_ops", filter={"state": "done", "type": "bootstrap"})
    """

    def _get_tasks():
        result = node.run_nodetool(f"tasks list {module}")
        tasks = parse_scylla_task_list(result.stdout)
        LOGGER.debug(f"tasks list output:\n{result.stdout}")
        LOGGER.debug(f"Current tasks: {tasks}")
        # return all tasks that match the provided key-value properties
        return [t for t in tasks if all(t.get(k) == v for k, v in (filter or {}).items())]

    return wait.wait_for(_get_tasks, timeout=timeout)
