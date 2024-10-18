import re
import logging
from dataclasses import asdict
from typing import Any, Type
from uuid import UUID

import requests

from argus.common.enums import TestStatus
from argus.client.generic_result import GenericResultTable
from argus.client.sct.types import LogLink

JSON = dict[str, Any] | list[Any] | int | str | float | bool | Type[None]
LOGGER = logging.getLogger(__name__)


class ArgusClientError(Exception):
    pass


class ArgusClient:
    schema_version: str | None = None

    class Routes():
        # pylint: disable=too-few-public-methods
        SUBMIT = "/testrun/$type/submit"
        GET = "/testrun/$type/$id/get"
        HEARTBEAT = "/testrun/$type/$id/heartbeat"
        GET_STATUS = "/testrun/$type/$id/get_status"
        SET_STATUS = "/testrun/$type/$id/set_status"
        SET_PRODUCT_VERSION = "/testrun/$type/$id/update_product_version"
        SUBMIT_LOGS = "/testrun/$type/$id/logs/submit"
        SUBMIT_RESULTS = "/testrun/$type/$id/submit_results"
        FETCH_RESULTS = "/testrun/$type/$id/fetch_results"
        FINALIZE = "/testrun/$type/$id/finalize"

    def __init__(self, auth_token: str, base_url: str, api_version="v1") -> None:
        self._auth_token = auth_token
        self._base_url = base_url
        self._api_ver = api_version

    @property
    def auth_token(self) -> str:
        return self._auth_token

    def verify_location_params(self, endpoint: str, location_params: dict[str, str]) -> bool:
        required_params: list[str] = re.findall(r"\$[\w_]+", endpoint)
        for param in required_params:
            if param.lstrip("$") not in location_params.keys():
                raise ArgusClientError(f"Missing required location argument for endpoint {endpoint}: {param}")

        return True

    @staticmethod
    def check_response(response: requests.Response, expected_code: int = 200):
        if response.status_code != expected_code:
            raise ArgusClientError(
                f"Unexpected HTTP Response encountered - expected: {expected_code}, got: {response.status_code}",
                expected_code,
                response.status_code,
                response.request,
            )

        response_data: JSON = response.json()
        LOGGER.debug("API Response: %s", response_data)
        if response_data.get("status") != "ok":
            exc_args = response_data["response"]["arguments"]
            raise ArgusClientError(
                f"API Error encountered using endpoint: {response.request.method} {response.request.path_url}",
                exc_args[0] if len(exc_args) > 0 else response_data.get("response", {}).get("exception", "#NoMessage"),
            )

    def get_url_for_endpoint(self, endpoint: str, location_params: dict[str, str] | None) -> str:
        if self.verify_location_params(endpoint, location_params):
            for param, value in location_params.items():
                endpoint = endpoint.replace(f"${param}", str(value))
        return f"{self._base_url}/api/{self._api_ver}/client{endpoint}"

    @property
    def generic_body(self) -> dict:
        return {
            "schema_version": self.schema_version
        }

    @property
    def request_headers(self):
        return {
            "Authorization": f"token {self.auth_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def get(self, endpoint: str, location_params: dict[str, str] = None, params: dict = None) -> requests.Response:
        response = requests.get(
            url=self.get_url_for_endpoint(
                endpoint=endpoint,
                location_params=location_params
            ),
            params=params,
            headers=self.request_headers
        )

        return response

    def post(
        self,
        endpoint: str,
        location_params: dict = None,
        params: dict = None,
        body: dict = None,
    ) -> requests.Response:
        response = requests.post(
            url=self.get_url_for_endpoint(
                endpoint=endpoint,
                location_params=location_params
            ),
            params=params,
            json=body,
            headers=self.request_headers,
        )

        return response

    def submit_run(self, run_type: str, run_body: dict) -> requests.Response:
        return self.post(endpoint=self.Routes.SUBMIT, location_params={"type": run_type}, body={
            **self.generic_body,
            **run_body
        })

    def get_run(self, run_type: str = None, run_id: UUID | str = None) -> requests.Response:

        if not run_type and hasattr(self, "test_type"):
            run_type = self.test_type

        if not run_id and hasattr(self, "run_id"):
            run_id = self.run_id

        if not (run_type and run_id):
            raise ValueError("run_type and run_id must be set in func params or object attributes")

        response = self.get(endpoint=self.Routes.GET, location_params={"type": run_type, "id": run_id })
        self.check_response(response)

        return response.json()["response"]

    def get_status(self, run_type: str = None, run_id: UUID = None) -> TestStatus:
        if not run_type and hasattr(self, "test_type"):
            run_type = self.test_type

        if not run_id and hasattr(self, "run_id"):
            run_id = self.run_id

        if not (run_type and run_id):
            raise ValueError("run_type and run_id must be set in func params or object attributes")

        response = self.get(
            endpoint=self.Routes.GET_STATUS,
            location_params={"type": run_type, "id": str(run_id)},
        )
        self.check_response(response)
        return TestStatus(response.json()["response"])

    def set_status(self, run_type: str, run_id: UUID, new_status: TestStatus) -> requests.Response:
        return self.post(
            endpoint=self.Routes.SET_STATUS,
            location_params={"type": run_type, "id": str(run_id)},
            body={
                **self.generic_body,
                "new_status": new_status.value
            }
        )

    def update_product_version(self, run_type: str, run_id: UUID, product_version: str) -> requests.Response:
        return self.post(
            endpoint=self.Routes.SET_PRODUCT_VERSION,
            location_params={"type": run_type, "id": str(run_id)},
            body={
                **self.generic_body,
                "product_version": product_version
            }
        )

    def submit_logs(self, run_type: str, run_id: UUID, logs: list[LogLink]) -> requests.Response:
        return self.post(
            endpoint=self.Routes.SUBMIT_LOGS,
            location_params={"type": run_type, "id": str(run_id)},
            body={
                **self.generic_body,
                "logs": [asdict(l) for l in logs]
            }
        )

    def finalize_run(self, run_type: str, run_id: UUID, body: dict = None) -> requests.Response:
        body = body if body else {}
        return self.post(
            endpoint=self.Routes.FINALIZE,
            location_params={"type": run_type, "id": str(run_id)},
            body={
                **self.generic_body,
                **body,
            }
        )

    def heartbeat(self, run_type: str, run_id: UUID) -> None:
        response = self.post(
            endpoint=self.Routes.HEARTBEAT,
            location_params={"type": run_type, "id": str(run_id)},
            body={
                **self.generic_body,
            }
        )
        self.check_response(response)

    def submit_results(self, result: GenericResultTable) -> None:
        response = self.post(
            endpoint=self.Routes.SUBMIT_RESULTS,
            location_params={"type": self.test_type, "id": str(self.run_id)},
            body={
                **self.generic_body,
                "run_id": str(self.run_id),
                ** result.as_dict(),
            }
        )
        self.check_response(response)

