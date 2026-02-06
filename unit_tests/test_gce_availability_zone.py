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

"""Test that GCE correctly ignores availability_zone parameter and uses random_zone."""

import pytest
from unittest.mock import MagicMock, patch

from sdcm.sct_runner import GceSctRunner
from sdcm.sct_config import SCTConfiguration


class TestGceAvailabilityZone:
    """Test GCE availability_zone parameter handling."""

    @patch("sdcm.sct_runner.get_gce_compute_images_client")
    @patch("sdcm.sct_runner.get_gce_compute_instances_client")
    @patch("sdcm.sct_runner.random_zone")
    def test_gce_sct_runner_ignores_availability_zone_parameter(
        self, mock_random_zone, mock_instances_client, mock_images_client
    ):
        """Test that GceSctRunner ignores the availability_zone parameter and uses random_zone."""
        # Setup mocks
        mock_random_zone.return_value = "c"  # GCE zones are single letters
        mock_images_client.return_value = (MagicMock(), {"project_id": "test-project"})
        mock_instances_client.return_value = (MagicMock(), {"project_id": "test-project"})

        # Create a mock SCTConfiguration
        mock_params = MagicMock(spec=SCTConfiguration)
        mock_params.get.return_value = None

        # Test with availability_zone='a' (should be ignored)
        runner = GceSctRunner(
            region_name="us-east1",
            availability_zone="a",  # This should be ignored
            params=mock_params
        )

        # Verify that random_zone was called with the region
        mock_random_zone.assert_called_once_with("us-east1")

        # Verify that the runner uses the random zone, not the parameter
        assert runner.availability_zone == "c", "GceSctRunner should use random_zone, not the parameter"
        assert runner.zone == "us-east1-c", "Zone should be region-randomzone"

    @patch("sdcm.sct_runner.get_gce_compute_images_client")
    @patch("sdcm.sct_runner.get_gce_compute_instances_client")
    @patch("sdcm.sct_runner.random_zone")
    def test_gce_sct_runner_ignores_params_availability_zone(
        self, mock_random_zone, mock_instances_client, mock_images_client
    ):
        """Test that GceSctRunner ignores availability_zone from params."""
        # Setup mocks
        mock_random_zone.return_value = "d"  # GCE zones are single letters
        mock_images_client.return_value = (MagicMock(), {"project_id": "test-project"})
        mock_instances_client.return_value = (MagicMock(), {"project_id": "test-project"})

        # Create a mock SCTConfiguration with availability_zone set
        mock_params = MagicMock(spec=SCTConfiguration)
        mock_params.get.return_value = "b"  # This should also be ignored

        # Test with no availability_zone parameter
        runner = GceSctRunner(
            region_name="us-central1",
            availability_zone="",  # Empty string
            params=mock_params
        )

        # Verify that random_zone was called
        mock_random_zone.assert_called_once_with("us-central1")

        # Verify that the runner uses the random zone
        assert runner.availability_zone == "d", "GceSctRunner should use random_zone"
        assert runner.zone == "us-central1-d", "Zone should be region-randomzone"


class TestGceClusterAvailabilityZone:
    """Test GCE cluster availability_zone parameter handling."""

    @patch("sdcm.cluster_gce.random_zone")
    def test_gce_cluster_ignores_availability_zone_param(self, mock_random_zone):
        """Test that GCE cluster ignores availability_zone from params and uses random_zone."""
        from sdcm.cluster_gce import ScyllaGCECluster

        # Setup mock to return different zones for each region
        mock_random_zone.side_effect = ["a", "b", "c"]

        # Create mock params with availability_zone set
        mock_params = MagicMock(spec=SCTConfiguration)
        mock_params.get.side_effect = lambda key: {
            "availability_zone": "x",  # This should be ignored
            "cluster_backend": "gce",
        }.get(key)

        # Mock GCE service
        mock_gce_service = (MagicMock(), {"project_id": "test-project"})

        # Create cluster instance (we'll only test the __init__ logic)
        with patch.object(ScyllaGCECluster, "_node_startup"):
            cluster = ScyllaGCECluster(
                gce_image="test-image",
                gce_image_type="test-type",
                gce_image_size=10,
                gce_network="test-network",
                gce_service=mock_gce_service,
                credentials=MagicMock(),
                gce_region_names=["us-east1", "us-west1", "us-central1"],
                params=mock_params,
                cluster_uuid="test-uuid",
                n_nodes=0,  # Don't create nodes
                add_nodes=False,  # Don't add nodes
            )

        # Verify that random_zone was called for each region
        assert mock_random_zone.call_count == 3, "random_zone should be called for each region"

        # Verify that the zones were created with random_zone results, not the param
        expected_zones = ["us-east1-a", "us-west1-b", "us-central1-c"]
        assert cluster._gce_zone_names == expected_zones, (
            f"Zones should use random_zone results, not availability_zone param. "
            f"Expected: {expected_zones}, Got: {cluster._gce_zone_names}"
        )
