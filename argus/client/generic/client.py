import logging
from argus.client.base import ArgusClient


LOGGER = logging.getLogger(__name__)

class ArgusGenericClient(ArgusClient):
    test_type = "generic"
    schema_version: None = "v1"
    def __init__(self, auth_token: str, base_url: str, api_version="v1") -> None:
        super().__init__(auth_token, base_url, api_version)

    def submit_generic_run(self, build_id: str, run_id: str, started_by: str, build_url: str, scylla_version: str | None = None):
        request_body = {
            "build_id": build_id,
            "run_id": run_id,
            "started_by": started_by,
            "build_url": build_url,
            "scylla_version": scylla_version
        }

        response = self.submit_run(run_type=self.test_type, run_body=request_body)
        self.check_response(response)


    def finalize_generic_run(self, run_id: str, status: str, scylla_version: str | None = None):
        response = self.finalize_run(run_type=self.test_type, run_id=run_id, body={
            "status": status,
            "scylla_version": scylla_version,
        })
        self.check_response(response)