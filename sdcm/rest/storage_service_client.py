from typing import Optional

from fabric.runners import Result
from requests import PreparedRequest

from sdcm.cluster import BaseNode

from sdcm.rest.rest_client import RestClient


class StorageServiceClient:
    def __init__(self, node: BaseNode):
        self._node = node
        self._host = "localhost:10000"
        self._remoter = self._node.remoter
        self._endpoint = "storage_service"
        self.__client = RestClient(host=self._host, endpoint=self._endpoint)

    def compact_ks_cf(self, keyspace: str, cf: str) -> Result:
        params = {"cf": cf} if cf else {}
        full_path = f"keyspace_compaction/{keyspace}"
        prepared_request = self.__client.prepare_post_request(path=full_path, params=params)
        response = self._run_remoter_curl(prepared_request)

        return response

    def cleanup_ks_cf(self, keyspace: str, cf: str) -> Result:
        params = {"cf": cf} if cf else {}
        full_path = f"keyspace_cleanup/{keyspace}"
        prepared_request = self.__client.prepare_post_request(path=full_path, params=params)
        response = self._run_remoter_curl(prepared_request)

        return response

    def scrub_ks_cf(self, keyspace: str, cf: Optional[str], scrub_mode: Optional[str] = None) -> Result:
        params = {"cf": cf} if cf else {}

        if scrub_mode:
            params.update({"scrub_mode": scrub_mode})

        full_path = f"keyspace_scrub/{keyspace}"
        prepared_request = self.__client.prepare_get_request(path=full_path, params=params)
        response = self._run_remoter_curl(prepared_request)

        return response

    def upgrade_sstables(self, keyspace: str = "ks", cf: str = "cf"):
        params = {"cf": cf} if cf else {}
        full_path = f"keyspace_upgrade_sstables/{keyspace}"
        prepared_request = self.__client.prepare_get_request(path=full_path, params=params)
        response = self._run_remoter_curl(prepared_request)

        return response

    def _run_remoter_curl(self, prepared_request: PreparedRequest, timeout: int = None):
        return self._remoter.run(f'curl -v -X {prepared_request.method} "{prepared_request.url}"', timeout=timeout)
