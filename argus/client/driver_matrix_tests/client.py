from uuid import UUID
from argus.common.enums import TestStatus
from argus.client.base import ArgusClient


class ArgusDriverMatrixClient(ArgusClient):
    test_type = "driver-matrix-tests"
    schema_version: None = "v2"

    class Routes(ArgusClient.Routes):
        SUBMIT_DRIVER_RESULT = "/driver_matrix/result/submit"
        SUBMIT_DRIVER_FAILURE = "/driver_matrix/result/fail"
        SUBMIT_ENV = "/driver_matrix/env/submit"

    def __init__(self, run_id: UUID, auth_token: str, base_url: str, api_version="v1") -> None:
        super().__init__(auth_token, base_url, api_version)
        self.run_id = run_id

    def submit_driver_matrix_run(self, job_name: str, job_url: str) -> None:
        response = super().submit_run(run_type=self.test_type, run_body={
            "run_id": str(self.run_id),
            "job_name": job_name,
            "job_url": job_url
        })

        self.check_response(response)

    def submit_driver_result(self, driver_name: str, driver_type: str, raw_junit_data: bytes):
        """
            Submit results of a single driver run
        """
        response = self.post(
            endpoint=self.Routes.SUBMIT_DRIVER_RESULT,
            location_params={},
            body={
                **self.generic_body,
                "run_id": str(self.run_id),
                "driver_name": driver_name,
                "driver_type": driver_type,
                "raw_xml": str(raw_junit_data, encoding="utf-8"),
            }
        )
        self.check_response(response)

    def submit_driver_failure(self, driver_name: str, driver_type: str, failure_reason: str):
        """
            Submit a failure to run of a specific driver test
        """
        response = self.post(
            endpoint=self.Routes.SUBMIT_DRIVER_FAILURE,
            location_params={},
            body={
                **self.generic_body,
                "run_id": str(self.run_id),
                "driver_name": driver_name,
                "driver_type": driver_type,
                "failure_reason": failure_reason,
            }
        )
        self.check_response(response)

    def submit_env(self, env_info: str):
        """
            Submit env-info file (Build-00.txt)
        """
        response = self.post(
            endpoint=self.Routes.SUBMIT_ENV,
            location_params={},
            body={
                **self.generic_body,
                "run_id": str(self.run_id),
                "raw_env": env_info,
            }
        )
        self.check_response(response)

    def set_matrix_status(self, status: TestStatus):
        response = super().set_status(run_type=self.test_type, run_id=self.run_id, new_status=status)
        self.check_response(response)
