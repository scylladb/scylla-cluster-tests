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
# Copyright (c) 2024 ScyllaDB

"""Tests for Argus integration when Argus is disabled.

When enable_argus is false, TestConfig._argus_client remains as the default
MagicMock instance. The guards in BaseNode's Argus methods should detect this
and return early without attempting API calls.
"""

from unittest.mock import MagicMock, patch

import pytest

from sdcm import sct_config
from sdcm.test_config import TestConfig
from unit_tests.lib.fake_cluster import DummyNode


class SimpleDummyCluster:
    """Minimal cluster stub providing only what BaseNode.__init__ needs."""

    def __init__(self):
        self.params = sct_config.SCTConfiguration()
        self.params["region_name"] = "test_region"
        self.node_type = "scylla-db"
        self.name = "dummy_db_cluster"
        self.cluster_backend = "aws"


@pytest.fixture
def argus_node(tmp_path):
    """Create a DummyNode with Argus in disabled state (MagicMock client)."""
    cluster = SimpleDummyCluster()
    node = DummyNode(
        name="test_node",
        parent_cluster=cluster,
        base_logdir=str(tmp_path),
        ssh_login_info=dict(key_file="~/.ssh/scylla-test"),
    )
    return node


def test_add_node_to_argus_when_disabled(argus_node):
    """_add_node_to_argus returns early when client is MagicMock."""
    client = TestConfig.argus_client()
    assert isinstance(client, MagicMock)

    argus_node._add_node_to_argus()

    client.create_resource.assert_not_called()


def test_update_shards_in_argus_when_disabled(argus_node):
    """update_shards_in_argus returns early when client is MagicMock."""
    client = TestConfig.argus_client()
    assert isinstance(client, MagicMock)

    with patch.object(type(argus_node), "scylla_shards", new_callable=lambda: property(lambda self: 4)):
        argus_node.update_shards_in_argus()

    client.update_shards_for_resource.assert_not_called()


def test_update_rack_info_in_argus_when_disabled(argus_node):
    """update_rack_info_in_argus returns early when client is MagicMock."""
    client = TestConfig.argus_client()
    assert isinstance(client, MagicMock)

    argus_node.update_rack_info_in_argus("dc1", "rack1")

    client.update_resource.assert_not_called()


def test_terminate_node_in_argus_when_disabled(argus_node):
    """_terminate_node_in_argus returns early when client is MagicMock."""
    client = TestConfig.argus_client()
    assert isinstance(client, MagicMock)

    argus_node._terminate_node_in_argus()

    client.terminate_resource.assert_not_called()
