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
# Copyright (c) 2022 ScyllaDB

from unittest.mock import patch

import pytest

from sdcm.rest.rest_client import RestClient


@pytest.fixture()
def client():
    """RestClient for http://localhost/api."""
    return RestClient(host="localhost", endpoint="api")


def test_session_has_retry_adapter_http(client):
    """The session retries http requests up to 5 times."""
    adapter = client.session.get_adapter("http://localhost")
    assert adapter.max_retries.total == 5


def test_session_has_retry_adapter_https(client):
    """The session retries https requests up to 5 times."""
    adapter = client.session.get_adapter("https://localhost")
    assert adapter.max_retries.total == 5


def test_retry_targets_5xx_and_429(client):
    """Retries cover rate limiting (429) and the 502/503 gateway errors."""
    adapter = client.session.get_adapter("http://localhost")
    assert 429 in adapter.max_retries.status_forcelist
    assert 502 in adapter.max_retries.status_forcelist
    assert 503 in adapter.max_retries.status_forcelist


def test_get_uses_session(client):
    """get() sends the request through the session with the joined URL."""
    with patch.object(client.session, "get") as mock_get:
        mock_get.return_value.status_code = 200
        client.get("test")
        mock_get.assert_called_once_with(url="http://localhost/api/test", params=None)


def test_post_uses_session(client):
    """post() sends the request through the session with the joined URL."""
    with patch.object(client.session, "post") as mock_post:
        mock_post.return_value.status_code = 200
        client.post("test")
        mock_post.assert_called_once_with(url="http://localhost/api/test", params=None)


def test_prepare_request_still_works(client):
    """_prepare_request builds a GET request with the joined URL and query params."""
    prepared = client._prepare_request("GET", "test", params={"foo": "bar"})
    assert prepared.method == "GET"
    assert "test" in prepared.url
    assert "foo=bar" in prepared.url
