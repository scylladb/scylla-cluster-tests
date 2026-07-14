import functools
import re
import logging
from dataclasses import asdict
from pathlib import Path
from typing import Any, Type
from uuid import UUID

import requests

from argus.common.enums import TestStatus
from argus.client.session import create_session
from argus.client.generic_result import GenericResultTable
from argus.client.replay_log import ReplayLog, ReplayLogOnlyResponse
from argus.client.sct.types import LogLink

JSON = dict[str, Any] | list[Any] | int | str | float | bool | Type[None]
LOGGER = logging.getLogger(__name__)


def _evaluate_response(response: requests.Response) -> tuple[bool, str | None]:
    """Classify an Argus HTTP response into ``(success, error)`` for the replay log.

    Only a response with ``response.ok`` true (status < 400) and a JSON body
    with ``status == "ok"`` counts as success. A non-2xx/3xx status, a non-JSON
    body (auth proxy, gateway error), or ``{"status": "error", ...}`` is a
    failure that a replay tool should re-send.
    """
    if not response.ok:
        return False, f"HTTP {response.status_code}"
    try:
        payload = response.json()
    except ValueError:
        return False, f"HTTP {response.status_code} non-JSON response"
    if payload.get("status") == "ok":
        return True, None
    return False, f"HTTP {response.status_code} status={payload.get('status')!r}"


class ArgusClientError(Exception):
    pass


class ArgusClient:
    schema_version: str | None = None

    class Routes():
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

    # Subclasses override ``test_type`` as a class attribute; ``run_id`` is
    # set on the instance by subclass constructors. Both are surfaced in the
    # replay-log filename.
    test_type: str | None = None

    def __init__(self, auth_token: str, base_url: str, log_dir: str | Path, api_version="v1",
                 extra_headers: dict | None = None, timeout: int = 60, max_retries: int = 3,
                 use_tunnel: bool | None = None, replay_log_only: bool = False,
                 run_id: UUID | str | None = None) -> None:
        self._auth_token = auth_token
        self._base_url = base_url
        self._api_ver = api_version
        self._timeout = timeout
        self._replay_log_only = replay_log_only
        # Set run_id on the instance so subclasses that read ``self.run_id``
        # later see the explicit value, not the class-attribute default.
        if run_id is not None:
            self.run_id = run_id
        # In replay-log-only mode no HTTP calls are made, so skip opening a
        # session (and any SSH tunnel that might come with it).
        if replay_log_only:
            self.session = None
        else:
            self.session = create_session(
                auth_token=auth_token,
                base_url=base_url,
                use_tunnel=use_tunnel,
                max_retries=max_retries,
            )
            if extra_headers:
                self.session.headers.update(extra_headers)

        self._replay_log = ReplayLog(
            log_dir=log_dir,
            run_id=str(run_id) if run_id is not None else None,
            test_type=self.test_type,
        )

    @property
    def replay_log_path(self) -> Path:
        return self._replay_log.path

    def close(self) -> None:
        if self.session is not None:
            self.session.close()
        self._replay_log.close()

    def __enter__(self) -> "ArgusClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

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
        # In replay-log-only mode no HTTP call is made; behave like a mock so
        # callers (e.g. SCT tests that previously used ``MagicMock``) do not
        # have to special-case GETs.
        if self._replay_log_only:
            LOGGER.debug("GET [replay-log-only] %s params: %s", endpoint, params)
            return ReplayLogOnlyResponse(endpoint=endpoint)
        url = self.get_url_for_endpoint(
            endpoint=endpoint,
            location_params=location_params
        )
        LOGGER.debug("GET Request: %s, params: %s", url, params)
        response = self.session.get(
            url=url,
            params=params,
            headers=self.request_headers,
            timeout=self._timeout
        )
        LOGGER.debug("GET Response: %s %s", response.status_code, response.url)

        return response

    def post(
        self,
        endpoint: str,
        location_params: dict = None,
        params: dict = None,
        body: dict = None,
    ) -> requests.Response:
        record = functools.partial(
            self._replay_log.write, "POST", endpoint, location_params, params, body
        )

        if self._replay_log_only:
            # Record the request so a future replay can re-send it, but skip
            # the HTTP call. Not sent means not successful.
            LOGGER.debug("POST [replay-log-only] %s body: %s", endpoint, body)
            record(success=False)
            return ReplayLogOnlyResponse(endpoint=endpoint)

        url = self.get_url_for_endpoint(
            endpoint=endpoint,
            location_params=location_params
        )
        LOGGER.debug("POST Request: %s, params: %s, body: %s", url, params, body)
        try:
            response = self.session.post(
                url=url,
                params=params,
                json=body,
                headers=self.request_headers,
                timeout=self._timeout
            )
        except Exception as exc:
            record(success=False, error=f"{type(exc).__name__}: {exc}")
            raise
        LOGGER.debug("POST Response: %s %s", response.status_code, response.url)
        success, error = _evaluate_response(response)
        record(success=success, error=error)
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

        response = self.get(endpoint=self.Routes.GET, location_params={"type": run_type, "id": run_id})
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
