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

        return self.run_remoter_curl(method="POST", path=path, params=params)

    def cleanup_ks_cf(self, keyspace: str, cf: Optional[str] = None) -> Result:
        params = {"cf": cf} if cf else {}
        path = f"keyspace_cleanup/{keyspace}"

        return self.run_remoter_curl(method="POST", path=path, params=params)

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
