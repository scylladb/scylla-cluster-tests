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

from typing import Literal, TYPE_CHECKING

if TYPE_CHECKING:
    from sdcm.cluster import BaseNode
from sdcm.rest.rest_client import RestClient


class ScyllaApiException(Exception):
    pass


class RemoteCurlClient(RestClient):
    def __init__(self, host: str, endpoint: str, node: 'BaseNode'):
        super().__init__(host=host, endpoint=endpoint)
        self._node = node
        self._remoter = self._node.remoter

    def run_remoter_curl(self, method: Literal["GET", "POST"],
                         path: str,
                         params: dict[str, str] | None,
                         timeout: int = 120,
                         retry: int = 0):
        prepared_request = self._prepare_request(method=method, path=path, params=params)
        result = self._remoter.run(
            f'curl -v -X {prepared_request.method} "{prepared_request.url}"', timeout=timeout, retry=retry)
        if result.failed:
            raise ScyllaApiException(f"Scylla Rest Api request failed. Method: {prepared_request.method}, "
                                     f"url: {prepared_request.url}, "
                                     f"stdout: {result.stdout}, "
                                     f"stderr: {result.stderr}")
        return result
