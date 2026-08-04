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

from http import HTTPStatus

import pytest
import yaml

from sdcm.utils.session import create_retry_session
from unit_tests.lib.dummy_remote import DummyRemote
from unit_tests.lib.fake_cluster import DummyNode

pytestmark = [
    pytest.mark.integration,
]

LATEST_MASTER_BUILD_URL = (
    "https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/master/relocatable/latest/00-Build.txt"
)
# Addresses inside Scylla's text segment, so any build symbolizes them to something.
SAMPLE_BACKTRACE_ADDRESSES = "0x4a3c9b7\n0x11ddd33\n0x11ddcec"


@pytest.fixture(name="node")
def node_fixture(tmp_path):
    node = DummyNode(name="test_node", parent_cluster=None, base_logdir=tmp_path)
    node.remoter = DummyRemote()
    return node


@pytest.fixture(name="known_build_id", scope="module")
def known_build_id_fixture():
    """Resolve a build the service can still symbolize.

    A pinned build id rots: the service needs the build's unstripped package,
    and unstable relocatables are garbage collected within a few months. Take
    the current promoted master build instead - newer builds exist, but they
    are not indexed by the service until they are promoted.
    """
    session = create_retry_session()

    res = session.get(LATEST_MASTER_BUILD_URL, timeout=30)
    res.raise_for_status()
    build_id = yaml.safe_load(res.content)["scylla-x86_64-BuildID[sha1]"]

    known = session.get("https://backtrace.scylladb.com/api/search/build_id", params={"build_id": build_id}, timeout=30)
    # Only an unindexed build is a reason to skip. Anything else -- an outage, an
    # auth error -- must fail loudly rather than quietly pass the suite.
    if known.status_code == HTTPStatus.NOT_FOUND:
        pytest.skip(f"backtrace.scylladb.com has not indexed the latest master build ({build_id}) yet")
    known.raise_for_status()
    return build_id


def test_decode_via_external_service_returns_symbols(node, known_build_id):
    """Verify _decode_via_external_service returns decoded symbols for a known build."""
    result = node._decode_via_external_service(known_build_id, SAMPLE_BACKTRACE_ADDRESSES)

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
