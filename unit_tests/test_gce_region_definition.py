#!/usr/bin/env python3

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

"""Unit tests for GCE region definition builder."""

from unittest.mock import Mock, patch

from sdcm.sct_provision.gce.gce_region_definition_builder import GceDefinitionBuilder
from sdcm.sct_config import SCTConfiguration
from sdcm.test_config import TestConfig


class TestGceDefinitionBuilder:
    """Test suite for GCE definition builder."""

    def test_get_provisioner_config_returns_network_name(self):
        """Test that get_provisioner_config returns GCE network configuration."""
        # Arrange
        mock_params = Mock(spec=SCTConfiguration)
        mock_params.get.return_value = "test-network"
        mock_test_config = Mock(spec=TestConfig)

        builder = GceDefinitionBuilder(params=mock_params, test_config=mock_test_config)

        # Act
        config = builder.get_provisioner_config()

        # Assert
        assert config == {"network_name": "test-network"}
        mock_params.get.assert_called_with("gce_network")

    @patch("sdcm.sct_provision.gce.gce_region_definition_builder.get_gce_service_accounts")
    def test_get_service_accounts_calls_gce_utils(self, mock_get_service_accounts):
        """Test that get_service_accounts calls the GCE utility function."""
        # Arrange
        mock_params = Mock(spec=SCTConfiguration)
        mock_test_config = Mock(spec=TestConfig)
        expected_accounts = ["test-service-account"]
        mock_get_service_accounts.return_value = expected_accounts

        builder = GceDefinitionBuilder(params=mock_params, test_config=mock_test_config)

        # Act
        result = builder.get_service_accounts()

        # Assert
        assert result == expected_accounts
        mock_get_service_accounts.assert_called_once()

    def test_regions_property_handles_single_datacenter(self):
        """Test that regions property converts single datacenter to list."""
        # Arrange
        mock_params = Mock(spec=SCTConfiguration)
        mock_params.get.return_value = "us-east1-b"
        mock_test_config = Mock(spec=TestConfig)

        builder = GceDefinitionBuilder(params=mock_params, test_config=mock_test_config)

        # Act
        regions = builder.regions

        # Assert
        assert regions == ["us-east1-b"]
        mock_params.get.assert_called_with("gce_datacenter")

    def test_regions_property_handles_list_datacenter(self):
        """Test that regions property handles list of datacenters."""
        # Arrange
        mock_params = Mock(spec=SCTConfiguration)
        mock_params.get.return_value = ["us-east1-b", "us-west1-a"]
        mock_test_config = Mock(spec=TestConfig)

        builder = GceDefinitionBuilder(params=mock_params, test_config=mock_test_config)

        # Act
        regions = builder.regions

        # Assert
        assert regions == ["us-east1-b", "us-west1-a"]
        mock_params.get.assert_called_with("gce_datacenter")

    @patch("sdcm.sct_provision.gce.gce_region_definition_builder.gce_instance_name")
    def test_instance_name_uses_gce_naming_utility(self, mock_gce_instance_name):
        """Test that instance_name uses the GCE-specific naming utility."""
        # Arrange
        mock_params = Mock(spec=SCTConfiguration)
        mock_test_config = Mock(spec=TestConfig)
        mock_gce_instance_name.return_value = "test-db-node-12345678-0-1"

        builder = GceDefinitionBuilder(params=mock_params, test_config=mock_test_config)

        # Act
        name = builder.instance_name("test", "db", "12345678", "us-east1-b", 1, 0)

        # Assert
        assert name == "test-db-node-12345678-0-1"
        mock_gce_instance_name.assert_called_once_with(node_prefix="test-db-node-12345678", dc_idx=0, node_index=1)
