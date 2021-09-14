from typing import Literal

from sdcm.cluster import BaseNode
from sdcm.rest.rest_client import RestClient


class RemoteCurlClient(RestClient):
    def __init__(self, host: str, endpoint: str, node: BaseNode):
        super().__init__(host=host, endpoint=endpoint)
        self._node = node
        self._remoter = self._node.remoter

    def run_remoter_curl(self, method: Literal["GET", "POST"], path: str, params: dict[str, str], timeout: int = 30):
        prepared_request = self._prepare_request(method=method, path=path, params=params)

        return self._remoter.run(f'curl -v -X {prepared_request.method} "{prepared_request.url}"', timeout=timeout)
