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

import logging
from typing import Literal

from fabric.runners import Result

from sdcm.cluster import BaseNode
from sdcm.rest.remote_curl_client import RemoteCurlClient

LOGGER = logging.getLogger(__name__)


class CompactionManagerClient(RemoteCurlClient):
    def __init__(self, node: BaseNode):
        super().__init__(host="localhost:10000", endpoint="compaction_manager", node=node)

    def stop_compaction(self, compaction_type: Literal["reshape"]) -> Result:
        """Stops compaction using Scylla Rest API.
        """
        LOGGER.debug("Stopping reshape compaction via Scylla REST API")
        params = {"type": compaction_type.upper()}

        return self.run_remoter_curl(method="POST", path="stop_compaction", params=params, timeout=360)

    def stop_keyspace_compaction(self, keyspace: str, compaction_type: str, tables: list, timeout: int = 360):
        LOGGER.debug("Stopping keyspace compaction via REST API")
        params = {"type": compaction_type, "tables": tables}
        path = f"stop_keyspace_compaction/{keyspace}"

        return self.run_remoter_curl(method="POST", path=path, params=params, timeout=timeout)
