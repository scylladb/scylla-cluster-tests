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


import pytest

from sdcm.rest.rest_client import RestClient


@pytest.fixture()
def client():
    return RestClient(host="localhost", endpoint="api")


def test_session_has_retry_adapter_http(client):
    adapter = client.session.get_adapter("http://localhost")
    assert adapter.max_retries.total == 5


def test_retry_targets_5xx_and_429(client):
    adapter = client.session.get_adapter("http://localhost")
    assert 429 in adapter.max_retries.status_forcelist
    assert 502 in adapter.max_retries.status_forcelist
    assert 503 in adapter.max_retries.status_forcelist


def test_prepare_request_still_works(client):
    prepared = client._prepare_request("GET", "test", params={"foo": "bar"})
    assert prepared.method == "GET"
    assert "test" in prepared.url
    assert "foo=bar" in prepared.url
