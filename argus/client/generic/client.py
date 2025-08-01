import logging
from argus.client.base import ArgusClient


LOGGER = logging.getLogger(__name__)


class ArgusGenericClient(ArgusClient):
    test_type = "generic"
    schema_version: None = "v1"

    class Routes(ArgusClient.Routes):
        TRIGGER_JOBS = "/planning/plan/trigger"

    def __init__(self, auth_token: str, base_url: str, api_version="v1", extra_headers: dict | None = None) -> None:
        super().__init__(auth_token, base_url, api_version, extra_headers=extra_headers)

    def submit_generic_run(self, build_id: str, run_id: str, started_by: str, build_url: str, sub_type: str = None, scylla_version: str | None = None):
        request_body = {
            "build_id": build_id,
            "run_id": run_id,
            "started_by": started_by,
            "build_url": build_url,
            "sub_type": sub_type,
            "scylla_version": scylla_version
        }

        response = self.submit_run(run_type=self.test_type, run_body=request_body)
        self.check_response(response)

    def trigger_jobs(self, common_params: dict[str, str], params: list[dict[str, str]], version: str = None, release: str = None, plan_id: str = None):
        request_body = {
            "common_params": common_params,
            "params": params,
            "version": version,
            "release": release,
            "plan_id": plan_id,
        }
        response = self.post(
            endpoint=self.Routes.TRIGGER_JOBS,
            location_params={},
            body={
                **self.generic_body,
                **request_body,
            }
        )
        self.check_response(response)
        return response.json()

    def finalize_generic_run(self, run_id: str, status: str, scylla_version: str | None = None):
        response = self.finalize_run(run_type=self.test_type, run_id=run_id, body={
            "status": status,
            "scylla_version": scylla_version,
        })
        self.check_response(response)
