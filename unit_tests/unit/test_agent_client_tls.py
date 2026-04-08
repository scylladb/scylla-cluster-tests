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

from sdcm.utils.agent_client import AgentClient
from sdcm.remote.agent_cmd_runner import AgentCmdRunner


@pytest.mark.parametrize(
    "tls,expected_url",
    [
        pytest.param(True, "https://192.168.1.10:16000", id="tls_enabled"),
        pytest.param(False, "http://192.168.1.10:16000", id="tls_disabled"),
    ],
)
def test_base_url_scheme(tls, expected_url):
    client = AgentClient("192.168.1.10", "test-key", port=16000, tls=tls)
    assert client.base_url == expected_url


@pytest.mark.parametrize(
    "ca_cert,expected_verify",
    [
        pytest.param("/path/to/ca.pem", "/path/to/ca.pem", id="ca_cert_provided"),
        pytest.param(None, True, id="ca_cert_not_provided"),
    ],
)
def test_session_verify_with_tls(ca_cert, expected_verify):
    client = AgentClient("192.168.1.10", "test-key", port=16000, tls=True, ca_cert=ca_cert)
    assert client.session.verify == expected_verify


@pytest.mark.parametrize(
    "tls,expected_scheme",
    [
        pytest.param(True, "https://", id="tls_enabled"),
        pytest.param(False, "http://", id="tls_disabled"),
    ],
)
def test_runner_tls_propagation(tls, expected_scheme):
    runner = AgentCmdRunner(
        hostname="192.168.1.10",
        api_key="test-key",
        port=16000,
        tls=tls,
        ca_cert="/path/to/ca.pem" if tls else None,
    )
    client = runner._create_connection()
    assert client.tls is tls
    assert client.base_url.startswith(expected_scheme)
    assert expected_scheme in runner.ssh_debug_cmd()
    if tls:
        assert client.session.verify == "/path/to/ca.pem"
