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

"""Tests for Argus integration when Argus is disabled."""

import unittest
from unittest.mock import MagicMock, patch
import pytest

from sdcm import sct_config
from sdcm.cluster import BaseNode
from unit_tests.test_utils_common import DummyNode


class DummyDbCluster:
    """Minimal dummy cluster for testing."""

    def __init__(self, params=None):
        self.params = params or sct_config.SCTConfiguration()
        self.params["region_name"] = "test_region"
        self.node_type = "scylla-db"
        self.name = "dummy_db_cluster"
        self.cluster_backend = "aws"


class TestArgusDisabled(unittest.TestCase):
    """Test that Argus methods don't raise errors when Argus is disabled."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_config_patcher = patch("sdcm.test_config.TestConfig")
        self.mock_test_config_class = self.test_config_patcher.start()
        self.mock_test_config_instance = MagicMock()
        self.mock_test_config_class.return_value = self.mock_test_config_instance

        # Create a node with mocked test_config
        self.dummy_cluster = DummyDbCluster()
        self.node = DummyNode(
            name="test_node",
            parent_cluster=self.dummy_cluster,
            base_logdir="/tmp/test",
            ssh_login_info=dict(key_file="~/.ssh/scylla-test"),
        )

    def tearDown(self):
        """Clean up test fixtures."""
        self.test_config_patcher.stop()

    def test_add_node_to_argus_when_disabled(self):
        """Test that _add_node_to_argus doesn't make API calls when Argus is disabled."""
        # Set up MagicMock as the argus_client (simulating Argus being disabled)
        # This matches the default state in TestConfig when enable_argus: false
        disabled_argus_client = MagicMock()
        self.node.test_config.argus_client.return_value = disabled_argus_client

        # Call the method
        self.node._add_node_to_argus()

        # Verify that no API methods were called on the mock client
        # because isinstance(disabled_argus_client, MagicMock) is True
        disabled_argus_client.create_resource.assert_not_called()

    def test_update_shards_in_argus_when_disabled(self):
        """Test that update_shards_in_argus doesn't make API calls when Argus is disabled."""
        # Set up MagicMock as the argus_client
        disabled_argus_client = MagicMock()
        self.node.test_config.argus_client.return_value = disabled_argus_client
        self.node.scylla_shards = 4

        # Call the method
        self.node.update_shards_in_argus()

        # Verify that no API methods were called
        disabled_argus_client.update_shards_for_resource.assert_not_called()

    def test_update_rack_info_in_argus_when_disabled(self):
        """Test that update_rack_info_in_argus doesn't make API calls when Argus is disabled."""
        # Set up MagicMock as the argus_client
        disabled_argus_client = MagicMock()
        self.node.test_config.argus_client.return_value = disabled_argus_client

        # Call the method
        self.node.update_rack_info_in_argus("dc1", "rack1")

        # Verify that no API methods were called
        disabled_argus_client.update_resource.assert_not_called()

    def test_terminate_node_in_argus_when_disabled(self):
        """Test that _terminate_node_in_argus doesn't make API calls when Argus is disabled."""
        # Set up MagicMock as the argus_client
        disabled_argus_client = MagicMock()
        self.node.test_config.argus_client.return_value = disabled_argus_client

        # Call the method
        self.node._terminate_node_in_argus()

        # Verify that no API methods were called
        disabled_argus_client.terminate_resource.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
