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
# Copyright (c) 2026 ScyllaDB

"""Unit tests for the index/readiness polling of VectorStoreClient.

Used by the search performance tests to poll through the discovery gap between 'CREATE INDEX' and
the index becoming visible to vector-store (404 until then), and the same gap on the way out -- see
docs/fts-search-test.md.
"""

from unittest.mock import MagicMock, patch

import pytest
import requests

from sdcm.utils import vector_store_client as vector_store_client_module
from sdcm.utils.vector_store_client import VectorStoreClient


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(vector_store_client_module.time, "sleep", lambda _seconds: None)


def _http_error(status_code):
    response = MagicMock()
    response.status_code = status_code
    error = requests.exceptions.HTTPError(f"{status_code} error")
    error.response = response
    return error


def test_get_index_status_or_none_returns_status_on_success():
    client = VectorStoreClient(base_url="http://vs.example")
    with patch.object(client, "get_index_status", return_value={"status": "SERVING", "count": 5}) as mocked:
        assert client.get_index_status_or_none("ks", "idx") == {"status": "SERVING", "count": 5}
    mocked.assert_called_once_with("ks", "idx")


def test_get_index_status_or_none_returns_none_on_404():
    client = VectorStoreClient(base_url="http://vs.example")
    with patch.object(client, "get_index_status", side_effect=_http_error(404)):
        assert client.get_index_status_or_none("ks", "idx") is None


def test_get_index_status_or_none_reraises_non_404_http_errors():
    client = VectorStoreClient(base_url="http://vs.example")
    with patch.object(client, "get_index_status", side_effect=_http_error(500)):
        with pytest.raises(requests.exceptions.HTTPError):
            client.get_index_status_or_none("ks", "idx")


def test_get_index_status_or_none_reraises_when_response_is_missing():
    """An HTTPError with no attached response (e.g. a connection-level failure) must not be
    mistaken for 'not discovered yet'."""
    client = VectorStoreClient(base_url="http://vs.example")
    error = requests.exceptions.HTTPError("no response")
    error.response = None
    with patch.object(client, "get_index_status", side_effect=error):
        with pytest.raises(requests.exceptions.HTTPError):
            client.get_index_status_or_none("ks", "idx")


def test_wait_for_index_absent_returns_once_the_index_is_gone():
    client = VectorStoreClient(base_url="http://vs.example")
    statuses = [{"status": "SERVING", "count": 5}, None]
    with patch.object(client, "get_index_status_or_none", side_effect=statuses):
        client.wait_for_index_absent("ks", "idx", timeout=10)


def test_wait_for_index_absent_raises_past_the_deadline():
    client = VectorStoreClient(base_url="http://vs.example")
    with patch.object(client, "get_index_status_or_none", return_value={"status": "SERVING"}):
        with pytest.raises(RuntimeError, match="was not dropped"):
            client.wait_for_index_absent("ks", "idx", timeout=0)


def test_wait_for_index_absent_treats_a_failed_request_as_still_there():
    """A network hiccup must not be mistaken for the index having disappeared."""
    client = VectorStoreClient(base_url="http://vs.example")
    with patch.object(client, "get_index_status_or_none", side_effect=RuntimeError("connection reset")):
        with pytest.raises(RuntimeError, match="request failed: connection reset"):
            client.wait_for_index_absent("ks", "idx", timeout=0)


def test_wait_for_ready_accepts_bootstrapping_by_default():
    client = VectorStoreClient(base_url="http://vs.example")
    with patch.object(client, "get_status", return_value="BOOTSTRAPPING"):
        assert client.wait_for_ready(timeout=1, check_interval=0) is True


def test_wait_for_ready_can_require_serving_only():
    """What an index-timing caller needs: 'answers and is catching up' is not good enough."""
    client = VectorStoreClient(base_url="http://vs.example")
    with patch.object(client, "get_status", return_value="BOOTSTRAPPING"):
        assert client.wait_for_ready(timeout=0.01, check_interval=0, required_statuses=("SERVING",)) is False
    with patch.object(client, "get_status", return_value="SERVING"):
        assert client.wait_for_ready(timeout=1, check_interval=0, required_statuses=("SERVING",)) is True
