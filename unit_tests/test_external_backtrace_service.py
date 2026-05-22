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

"""Integration tests for BaseNode._decode_via_external_service().

External services: HTTPS to backtrace.scylladb.com
"""

import pytest

from unit_tests.lib.dummy_remote import DummyRemote
from unit_tests.lib.fake_cluster import DummyNode

pytestmark = [
    pytest.mark.integration,
]

# Known-good build from Scylla 2026.1.0~dev (SCYLLADB-403)
KNOWN_BUILD_ID = "527bc25417c81c8106b33400a4372616c7c90894"
SAMPLE_BACKTRACE_ADDRESSES = "0x4a3c9b7\n0x11ddd33\n0x11ddcec"


@pytest.fixture(name="node")
def node_fixture(tmp_path):
    node = DummyNode(name="test_node", parent_cluster=None, base_logdir=tmp_path)
    node.remoter = DummyRemote()
    return node


def test_decode_via_external_service_returns_symbols(node):
    """Verify _decode_via_external_service returns decoded symbols for a known build."""
    result = node._decode_via_external_service(KNOWN_BUILD_ID, SAMPLE_BACKTRACE_ADDRESSES)

    assert result, "Decoded output should not be empty"
    assert "Backtrace" in result
    assert any(indicator in result for indicator in ["at ", "()", "::", "FRAME"]), (
        f"Decoded output doesn't look like resolved symbols: {result[:200]}"
    )


def test_decode_via_external_service_raises_for_unknown_build(node):
    """Verify _decode_via_external_service raises for an unknown build ID."""
    fake_build_id = "0" * 40
    with pytest.raises(Exception):
        node._decode_via_external_service(fake_build_id, "0x1234")
