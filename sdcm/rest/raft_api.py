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
from typing import TypeVar
from sdcm.rest.remote_curl_client import RemoteCurlClient

BaseNode = TypeVar("BaseNode")


class RaftApi(RemoteCurlClient):
    """Raft api commands"""

    def __init__(self, node: BaseNode):
        super().__init__(host="localhost:10000", endpoint="raft", node=node)

    def read_barrier(self, group_id: str) -> str:
        path = f"read_barrier?group_id={group_id}"
        return self.run_remoter_curl(method="POST", path=path, params={}, timeout=30).stdout.strip()

    def get_group0_leader_host_id(self) -> str:
        """Return the current group0 Raft leader host id (the live topology coordinator).

        No group_id param is passed, so the endpoint defaults to group0. The response is a
        JSON-quoted UUID string (e.g. '"<uuid>"'); when no leader is known during an
        election/stepdown, the response contains the nil UUID
        ('"00000000-0000-0000-0000-000000000000"'), not an empty string, because Scylla
        serializes current_leader() via server_id::to_sstring(). The raw stdout is returned
        unparsed - the caller must parse it and handle that transient sentinel.
        """
        path = "leader_host"
        return self.run_remoter_curl(method="GET", path=path, params={}, timeout=30).stdout.strip()
