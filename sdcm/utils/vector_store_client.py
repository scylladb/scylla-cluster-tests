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
# Copyright (c) 2025 ScyllaDB

import logging
import requests
import time

LOGGER = logging.getLogger(__name__)


class VectorStoreClient:
    """HTTP client for Vector Store API"""

    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make HTTP request with error handling"""
        url = f"{self.base_url}{endpoint}"
        kwargs.setdefault("timeout", self.timeout)

        LOGGER.debug("Making %s request to %s", method, url)
        response = self.session.request(method, url, **kwargs)

        LOGGER.debug("Response: %s %s", response.status_code, response.reason)
        response.raise_for_status()
        return response

    def get_status(self) -> str:
        """Get Vector Store operational status"""
        return self.request("GET", "/api/v1/status").json()

    def get_info(self) -> dict:
        """Get Vector Store service information"""
        return self.request("GET", "/api/v1/info").json()

    def get_indexes(self) -> list[dict]:
        """Get list of all vector indexes"""
        return self.request("GET", "/api/v1/indexes").json()

    def ann_search(self, keyspace: str, index: str, vector: list[float], limit: int = 10) -> dict:
        """Perform Approximate Nearest Neighbor search

        :param keyspace: ScyllaDB keyspace name
        :param index: vector index name
        :param vector: query vector (list of floats)
        :param limit: maximum number of results to return

        :returns dict: search results with 'primary_keys' and 'distances' fields
        """
        payload = {"vector": vector, "limit": limit}
        return self.request("POST", f"/api/v1/indexes/{keyspace}/{index}/ann", json=payload).json()

    def get_index_status(self, keyspace: str, index: str) -> dict:
        """Get status and vector count for a specific index"""
        return self.request("GET", f"/api/v1/indexes/{keyspace}/{index}/status").json()

    def get_index_status_or_none(self, keyspace: str, index: str) -> dict | None:
        """Get status and count for an index, or None if it has not been discovered yet.

        A newly created index is not immediately visible to Vector Store -- the endpoint 404s
        until it is -- so callers that need to poll through that discovery gap (e.g. measuring
        index build time) should use this instead of 'get_index_status'.
        """
        try:
            return self.get_index_status(keyspace, index)
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                return None
            raise

    def get_index_count(self, keyspace: str, index: str) -> int:
        """Get number of embeddings in a vector index"""
        return self.get_index_status(keyspace, index)["count"]

    def wait_for_ready(
        self,
        timeout: int = 300,
        check_interval: float = 5,
        required_statuses: tuple[str, ...] = ("SERVING", "BOOTSTRAPPING"),
    ) -> bool:
        """Wait for Vector Store to report one of *required_statuses*

        The default accepts BOOTSTRAPPING, which is what cluster readiness means here: the service
        answers and is catching up. A caller that needs it to be actually serving before it measures
        anything -- index build time, query latency -- should pass ('SERVING',) instead.
        """
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                status = self.get_status()
                if status in required_statuses:
                    LOGGER.info("Vector Store is ready (status: %s)", status)
                    return True
            except Exception:  # noqa: BLE001
                pass
            LOGGER.debug("Vector Store is not ready yet")
            time.sleep(check_interval)

        LOGGER.error("Vector Store did not become ready within %s seconds", timeout)
        return False

    def wait_for_index_absent(self, keyspace: str, index: str, timeout: float, check_interval: float = 1.0):
        """Wait until an index is no longer discoverable, or raise once *timeout* has passed

        Vector Store's view of an index is asynchronous in both directions: it takes time to
        discover a fresh index, and time to forget a dropped one. A caller that recreates an index
        needs this wait, or 'CREATE CUSTOM INDEX' can race a drop that ScyllaDB already considers
        done but Vector Store has not caught up with yet.

        A failed request counts as "still there" rather than as a disappearance: mistaking a network
        hiccup for a completed drop would let the next build start too early. Both names must
        already be case-folded the way ScyllaDB folds unquoted identifiers -- querying with the
        unfolded name 404s forever, which would read as "already dropped".
        """
        deadline = time.monotonic() + timeout
        while True:
            try:
                status = self.get_index_status_or_none(keyspace, index)
            except Exception as exc:  # noqa: BLE001
                LOGGER.debug("Index status check for '%s.%s' failed: %s", keyspace, index, exc)
                status = {"status": f"request failed: {exc}"}
            if status is None:
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Index '{keyspace}.{index}' was not dropped within {timeout}s "
                    f"(last status: {status.get('status')})"
                )
            time.sleep(check_interval)

    def close(self):
        self.session.close()
