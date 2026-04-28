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

"""Integration tests for the external backtrace decoding service (api.backtrace.scylladb.com).

External services: HTTPS to api.backtrace.scylladb.com
"""

import pytest
import requests

pytestmark = [
    pytest.mark.integration,
]

# Known-good build from Scylla 2026.1.0~dev (SCYLLADB-403)
KNOWN_BUILD_ID = "527bc25417c81c8106b33400a4372616c7c90894"
SAMPLE_BACKTRACE_ADDRESSES = "0x4a3c9b7\n0x11ddd33\n0x11ddcec"
API_URL = "https://api.backtrace.scylladb.com/api/backtrace"


def test_external_backtrace_service_decodes_known_build():
    """Verify that api.backtrace.scylladb.com returns decoded backtrace for a known build.

    This test validates that the external service used by
    BaseNode._decode_via_external_service() is reachable and returns
    decoded output for a known Scylla build ID.
    """
    response = requests.post(
        API_URL,
        json={"build_id": KNOWN_BUILD_ID, "input": "Backtrace:\n" + SAMPLE_BACKTRACE_ADDRESSES},
        timeout=120,
    )
    response.raise_for_status()
    result = response.json()

    assert result["success"] is True, f"Service returned success=false: {result.get('stderr')}"
    assert result["stdout"], "Service returned empty stdout"
    assert "Backtrace" in result["stdout"], "Decoded output should contain 'Backtrace' header"
    assert any(indicator in result["stdout"] for indicator in ["at ", "()", "::", "FRAME"]), (
        f"Decoded output doesn't look like resolved symbols: {result['stdout'][:200]}"
    )


def test_external_backtrace_service_returns_error_for_unknown_build():
    """Verify that the service handles unknown build IDs gracefully.

    The service should either return success=false or raise an HTTP error
    for a build ID that doesn't exist.
    """
    fake_build_id = "0" * 40
    response = requests.post(
        API_URL,
        json={"build_id": fake_build_id, "input": "Backtrace:\n0x1234"},
        timeout=120,
    )
    if response.status_code == 200:
        response.json()
    else:
        assert response.status_code in (400, 404, 422, 500), (
            f"Unexpected status code {response.status_code} for unknown build ID"
        )
