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
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()

    def request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make HTTP request with error handling"""
        url = f"{self.base_url}{endpoint}"
        kwargs.setdefault('timeout', self.timeout)

        LOGGER.debug("Making %s request to %s", method, url)
        response = self.session.request(method, url, **kwargs)

        LOGGER.debug("Response: %s %s", response.status_code, response.reason)
        response.raise_for_status()
        return response

    def get_status(self) -> dict:
        """Get Vector Store operational status"""
        return self.request('GET', '/api/v1/status').json()

    def get_info(self) -> dict:
        """Get Vector Store service information"""
        return self.request('GET', '/api/v1/info').json()

    def get_indexes(self) -> list[dict]:
        """Get list of all vector indexes"""
        return self.request('GET', '/api/v1/indexes').json()

    def ann_search(self, keyspace: str, index: str, embedding: list[float], limit: int = 10) -> dict:
        """Perform Approximate Nearest Neighbor search

        :param keyspace: ScyllaDB keyspace name
        :param index: vector index name
        :param embedding: query vector (list of floats)
        :param limit: maximum number of results to return

        :returns dict: search results with 'primary_keys' and 'distances' fields
        """
        payload = {
            'embedding': embedding,
            'limit': limit}
        return self.request('POST', f'/api/v1/indexes/{keyspace}/{index}/ann', json=payload).json()

    def get_index_count(self, keyspace: str, index: str) -> int:
        """Get number of embeddings in a vector index"""
        return self.request('GET', f'/api/v1/indexes/{keyspace}/{index}/count').json()

    def wait_for_ready(self, timeout: int = 300, check_interval: int = 5) -> bool:
        """Wait for Vector Store to become ready (to have status SERVING or INDEXING_VECTORS)"""
        end_time = time.time() + timeout
        while time.time() < end_time:
            try:
                status = self.get_status()
                if status in ('SERVING', 'BOOTSTRAPPING'):
                    LOGGER.info("Vector Store is ready (status: %s)", status)
                    return True
            except Exception:  # noqa: BLE001
                pass
            LOGGER.debug("Vector Store is not ready yet")
            time.sleep(check_interval)

        LOGGER.error("Vector Store did not become ready within %s seconds", timeout)
        return False

    def close(self):
        self.session.close()
