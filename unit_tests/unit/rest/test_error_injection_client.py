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
import pytest

from sdcm.rest.error_injection import ErrorInjection


@pytest.fixture
def error_injection_client(fake_node):
    return ErrorInjection(fake_node)


def test_list_injected_errors(error_injection_client):
    result = error_injection_client.list_injected_errors()

    assert result.stdout == (
        "curl -v --retry 5 --retry-max-time 300 --connect-timeout 10"
        ' -X GET "http://localhost:10000/v2/error_injection/injection"'
    )


def test_inject_error(error_injection_client):
    result = error_injection_client.inject_error(error_name="test_error")

    assert result.stdout == (
        "curl -v --retry 5 --retry-max-time 300 --connect-timeout 10"
        ' -X POST "http://localhost:10000/v2/error_injection/injection/test_error?one_shot=false"'
    )


def test_inject_error_with_data_one_shot(error_injection_client):
    result = error_injection_client.inject_error(error_name="test_error", one_shot=True, data={"key": "value"})

    assert result.stdout == (
        "curl -v --retry 5 --retry-max-time 300 --connect-timeout 10"
        " -X POST -H 'Content-Type: application/json' -d '{\"key\": \"value\"}'"
        ' "http://localhost:10000/v2/error_injection/injection/test_error?one_shot=true"'
    )


def test_remove_errors(error_injection_client):
    result = error_injection_client.remove_errors()

    assert result.stdout == (
        "curl -v --retry 5 --retry-max-time 300 --connect-timeout 10"
        ' -X DELETE "http://localhost:10000/v2/error_injection/injection"'
    )


def test_send_message_to_error(error_injection_client):
    result = error_injection_client.send_message_to_error(error_name="test_error")

    assert result.stdout == (
        "curl -v --retry 5 --retry-max-time 300 --connect-timeout 10"
        ' -X POST "http://localhost:10000/v2/error_injection/injection/test_error/message"'
    )
