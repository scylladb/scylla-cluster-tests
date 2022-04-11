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

# These classes are in preliminary phase - will require more generalization when more features will be added
# possibly this file could be generated automatically

import logging

from sdcm.cluster import BaseNode
from sdcm.remote.libssh2_client import Result

LOGGER = logging.getLogger(__name__)


class ScyllaApiException(Exception):
    pass


class CompactionManagerApi:  # pylint: disable=too-few-public-methods

    def __init__(self, node: BaseNode) -> None:
        self.node = node

    def stop_reshape_compaction(self) -> Result:
        LOGGER.debug("Stopping reshape compaction via Scylla REST API")
        result = self.node.remoter.run(
            "curl -S -X POST http://localhost:10000/compaction_manager/stop_compaction?type=RESHAPE")
        if result.failed:
            raise ScyllaApiException(f"Failed to stop reshape compaction. Stdout: {result.stdout}"
                                     f"stderr: {result.stderr}")
        return result


class ScyllaApiClient:  # pylint: disable=too-few-public-methods
    """Scylla api client that uses curl on localhost to perform api operations
    """

    def __init__(self, node: BaseNode) -> None:
        self.node = node
        self.compaction_manager_api = CompactionManagerApi(node)

    @property
    def compaction_manager(self) -> CompactionManagerApi:
        return self.compaction_manager_api
