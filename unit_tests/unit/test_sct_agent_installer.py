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

import pytest
import yaml

from sdcm.utils.sct_agent_installer import (
    get_agent_config_yaml,
    reconfigure_agent_script,
)

TLS_KWARGS = {
    "tls": True,
    "tls_cert_file": "/etc/sct-agent/certs/server.crt",
    "tls_key_file": "/etc/sct-agent/certs/server.key",
}


def test_config_yaml_with_tls_enabled():
    parsed = yaml.safe_load(get_agent_config_yaml(api_keys=["test-key"], **TLS_KWARGS))
    assert parsed["security"]["tls"] == {
        "enabled": True,
        "cert_file": "/etc/sct-agent/certs/server.crt",
        "key_file": "/etc/sct-agent/certs/server.key",
    }


def test_config_yaml_without_tls():
    parsed = yaml.safe_load(get_agent_config_yaml(api_keys=["test-key"]))
    assert "tls" not in parsed.get("security", {})


@pytest.mark.parametrize(
    "tls,expected_scheme,expected_curl",
    [
        pytest.param(True, "https://localhost", "curl -kf", id="tls_enabled"),
        pytest.param(False, "http://localhost", "curl -f", id="tls_disabled"),
    ],
)
def test_reconfigure_script_check_scheme(tls, expected_scheme, expected_curl):
    script = reconfigure_agent_script(
        api_keys=["test-key"],
        **(TLS_KWARGS if tls else {}),
    )
    assert expected_scheme in script
    assert expected_curl in script
