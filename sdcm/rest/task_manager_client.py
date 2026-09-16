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

import json

from sdcm.rest.remote_curl_client import RemoteCurlClient


class TaskManagerClient(RemoteCurlClient):
    def __init__(self, node: "BaseNode"):  # noqa: F821
        super().__init__(host="localhost:10000", endpoint="task_manager", node=node)

    def list_module_tasks(self, module: str) -> list[dict]:
        result = self.run_remoter_curl(method="GET", path=f"list_module_tasks/{module}", params={})
        return json.loads(result.stdout)

    def get_active_repair_tasks(self) -> list[dict]:
        """Return repair-module tasks in an active state on the node.

        This is the same listing Scylla Manager's "ensure no active repairs" guard checks
        before starting a repair, so an empty result means that guard will pass.
        """
        return [task for task in self.list_module_tasks("repair") if task.get("state") in ("created", "running")]
