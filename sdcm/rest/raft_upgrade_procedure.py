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
# Copyright (c) 2024 ScyllaDB
import json
from sdcm.cluster import BaseNode
from sdcm.rest.remote_curl_client import RemoteCurlClient
from sdcm.wait import wait_for


class RaftUpgradeProcedure(RemoteCurlClient):
    """Raft upgrade procedure to enable consistent topology changes.
    The procedure should be run only once after all nodes had been upgraded
    Doc:
    https://opensource.docs.scylladb.com/stable/upgrade/upgrade-opensource/upgrade-guide-from-5.4-to-6.0/enable-consistent-topology.html


    """

    def __init__(self, node: BaseNode):
        super().__init__(host="localhost:10000", endpoint="storage_service", node=node)

    def start_upgrade_procedure(self) -> str:
        path = "raft_topology/upgrade"
        return self.run_remoter_curl(method="POST", path=path, params=None, timeout=30).stdout.strip()

    def get_upgrade_procedure_status(self) -> str:
        """rest api return json string"""
        path = "raft_topology/upgrade"
        return json.loads(self.run_remoter_curl(method="GET", path=path, params=None, timeout=30).stdout.strip())

    def wait_upgrade_procedure_done(self):
        wait_for(
            lambda: self.get_upgrade_procedure_status().lower() == "done",
            step=5,
            text="Check raft upgrade procedure state",
            timeout=60,
        )
