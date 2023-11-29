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
# Copyright (c) 2022 ScyllaDB

from typing import Optional

from fabric.runners import Result

from sdcm.cluster import BaseNode
from sdcm.rest.remote_curl_client import RemoteCurlClient


class StorageServiceClient(RemoteCurlClient):
    def __init__(self, node: BaseNode):
        super().__init__(host="localhost:10000", endpoint="storage_service", node=node)

    def compact_ks_cf(self, keyspace: str, cf: Optional[str] = None) -> Result:
        params = {"cf": cf} if cf else {}
        path = f"keyspace_compaction/{keyspace}"

        return self.run_remoter_curl(method="POST", path=path, params=params, timeout=360)

    def cleanup_ks_cf(self, keyspace: str, cf: Optional[str] = None, timeout: int = 600) -> Result:
        params = {"cf": cf} if cf else {}
        path = f"keyspace_cleanup/{keyspace}"

        return self.run_remoter_curl(method="POST", path=path, params=params, timeout=timeout)

    def scrub_ks_cf(self, keyspace: str, cf: Optional[str] = None, scrub_mode: Optional[str] = None) -> Result:
        params = {"cf": cf} if cf else {}

        if scrub_mode:
            params.update({"scrub_mode": scrub_mode})

        path = f"keyspace_scrub/{keyspace}"

        return self.run_remoter_curl(method="GET", path=path, params=params)

    def upgrade_sstables(self, keyspace: str = "ks", cf: Optional[str] = None):
        params = {"cf": cf} if cf else {}
        path = f"keyspace_upgrade_sstables/{keyspace}"

        return self.run_remoter_curl(method="GET", path=path, params=params)
