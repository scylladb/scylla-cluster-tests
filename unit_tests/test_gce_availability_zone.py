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

"""Test that GCE correctly validates availability_zone against SUPPORTED_REGIONS."""

import pytest
from unittest.mock import MagicMock, patch

from sdcm.sct_runner import GceSctRunner
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.gce_utils import is_valid_zone_for_region


class TestGceAvailabilityZoneValidation:
    """Test GCE availability_zone validation logic."""

    def test_is_valid_zone_for_region(self):
        """Test the is_valid_zone_for_region helper function."""
        # Valid zones for us-east1 are 'c' and 'd' (per SUPPORTED_REGIONS)
        assert is_valid_zone_for_region("us-east1", "c") is True
        assert is_valid_zone_for_region("us-east1", "d") is True
        
        # Invalid zones for us-east1
        assert is_valid_zone_for_region("us-east1", "a") is False
        assert is_valid_zone_for_region("us-east1", "b") is False
        assert is_valid_zone_for_region("us-east1", "z") is False
        
        # Valid zones for us-east4
        assert is_valid_zone_for_region("us-east4", "a") is True
        assert is_valid_zone_for_region("us-east4", "b") is True
        assert is_valid_zone_for_region("us-east4", "c") is True
        
        # Invalid zone for us-east4
        assert is_valid_zone_for_region("us-east4", "d") is False
        
        # Invalid region
        assert is_valid_zone_for_region("invalid-region", "a") is False

    @patch("sdcm.sct_runner.get_gce_compute_images_client")
    @patch("sdcm.sct_runner.get_gce_compute_instances_client")
    @patch("sdcm.sct_runner.random_zone")
    def test_gce_sct_runner_uses_valid_zone(
        self, mock_random_zone, mock_instances_client, mock_images_client
    ):
        """Test that GceSctRunner uses a valid provided zone."""
        # Setup mocks
        mock_random_zone.return_value = "c"
        mock_images_client.return_value = (MagicMock(), {"project_id": "test-project"})
        mock_instances_client.return_value = (MagicMock(), {"project_id": "test-project"})

        # Create a mock SCTConfiguration
        mock_params = MagicMock(spec=SCTConfiguration)
        mock_params.get.return_value = None

        # Test with valid zone 'c' for us-east1 (valid per SUPPORTED_REGIONS)
        runner = GceSctRunner(
            region_name="us-east1",
            availability_zone="c",  # This is valid for us-east1
            params=mock_params
        )

        # Verify that random_zone was NOT called because we provided a valid zone
        mock_random_zone.assert_not_called()

        # Verify that the runner uses the provided zone
        assert runner.availability_zone == "c", "GceSctRunner should use valid provided zone"
        assert runner.zone == "us-east1-c", "Zone should be region-validzone"

    @patch("sdcm.sct_runner.get_gce_compute_images_client")
    @patch("sdcm.sct_runner.get_gce_compute_instances_client")
    @patch("sdcm.sct_runner.random_zone")
    def test_gce_sct_runner_rejects_invalid_zone(
        self, mock_random_zone, mock_instances_client, mock_images_client
    ):
        """Test that GceSctRunner rejects invalid zone and uses random_zone."""
        # Setup mocks
        mock_random_zone.return_value = "d"  # Will be used as fallback
        mock_images_client.return_value = (MagicMock(), {"project_id": "test-project"})
        mock_instances_client.return_value = (MagicMock(), {"project_id": "test-project"})

        # Create a mock SCTConfiguration
        mock_params = MagicMock(spec=SCTConfiguration)
        mock_params.get.return_value = None

        # Test with invalid zone 'a' for us-east1 (not in SUPPORTED_REGIONS['us-east1'])
        runner = GceSctRunner(
            region_name="us-east1",
            availability_zone="a",  # Invalid for us-east1 (only c,d are valid)
            params=mock_params
        )

        # Verify that random_zone WAS called because the provided zone is invalid
        mock_random_zone.assert_called_once_with("us-east1")

        # Verify that the runner uses random zone, not the invalid provided zone
        assert runner.availability_zone == "d", "GceSctRunner should use random_zone for invalid zone"
        assert runner.zone == "us-east1-d", "Zone should be region-randomzone"

    @patch("sdcm.sct_runner.get_gce_compute_images_client")
    @patch("sdcm.sct_runner.get_gce_compute_instances_client")
    @patch("sdcm.sct_runner.random_zone")
    def test_gce_sct_runner_handles_comma_separated_zones(
        self, mock_random_zone, mock_instances_client, mock_images_client
    ):
        """Test that GceSctRunner handles comma-separated zones correctly."""
        # Setup mocks
        mock_random_zone.return_value = "c"
        mock_images_client.return_value = (MagicMock(), {"project_id": "test-project"})
        mock_instances_client.return_value = (MagicMock(), {"project_id": "test-project"})

        # Create a mock SCTConfiguration
        mock_params = MagicMock(spec=SCTConfiguration)
        mock_params.get.return_value = None

        # Test with comma-separated zones where first one is valid
        runner = GceSctRunner(
            region_name="us-east1",
            availability_zone="c,d",  # Should take 'c' which is valid
            params=mock_params
        )

        # Verify the first zone was used
        mock_random_zone.assert_not_called()
        assert runner.availability_zone == "c"

    @patch("sdcm.sct_runner.get_gce_compute_images_client")
    @patch("sdcm.sct_runner.get_gce_compute_instances_client")
    @patch("sdcm.sct_runner.random_zone")
    def test_gce_sct_runner_uses_random_when_no_zone_provided(
        self, mock_random_zone, mock_instances_client, mock_images_client
    ):
        """Test that GceSctRunner uses random_zone when no zone is provided."""
        # Setup mocks
        mock_random_zone.return_value = "c"
        mock_images_client.return_value = (MagicMock(), {"project_id": "test-project"})
        mock_instances_client.return_value = (MagicMock(), {"project_id": "test-project"})

        # Create a mock SCTConfiguration
        mock_params = MagicMock(spec=SCTConfiguration)
        mock_params.get.return_value = None

        # Test with empty zone
        runner = GceSctRunner(
            region_name="us-central1",
            availability_zone="",
            params=mock_params
        )

        # Verify that random_zone was called
        mock_random_zone.assert_called_once_with("us-central1")
        assert runner.availability_zone == "c"


class TestGceClusterAvailabilityZone:
    """Test GCE cluster availability_zone validation."""

    @patch("sdcm.cluster_gce.random_zone")
    def test_gce_cluster_uses_valid_zone_from_params(self, mock_random_zone):
        """Test that GCE cluster uses valid zone from params."""
        from sdcm.cluster_gce import ScyllaGCECluster

        # Setup mock - should not be called if zone is valid
        mock_random_zone.return_value = "x"

        # Create mock params with valid availability_zone
        mock_params = MagicMock(spec=SCTConfiguration)
        mock_params.get.side_effect = lambda key: {
            "availability_zone": "c",  # Valid for us-east1
            "cluster_backend": "gce",
        }.get(key)

        # Mock GCE service
        mock_gce_service = (MagicMock(), {"project_id": "test-project"})

        # Create cluster instance (we'll only test the __init__ logic)
        # Patch methods that are called during initialization
        with (
            patch("sdcm.cluster_gce.TestConfig"),
            patch.object(ScyllaGCECluster, "init_log_directory"),
            patch("sdcm.cluster.ScyllaClusterBenchmarkManager"),
        ):
            cluster = ScyllaGCECluster(
                gce_image="test-image",
                gce_image_type="test-type",
                gce_image_size=10,
                gce_network="test-network",
                gce_service=mock_gce_service,
                credentials=MagicMock(),
                gce_region_names=["us-east1"],
                params=mock_params,
                cluster_uuid="test-uuid",
                n_nodes=0,
                add_nodes=False,
            )

        # Verify that random_zone was NOT called because zone 'c' is valid for us-east1
        mock_random_zone.assert_not_called()

        # Verify that the valid zone was used
        expected_zones = ["us-east1-c"]
        assert cluster._gce_zone_names == expected_zones, (
            f"Cluster should use valid zone from params. "
            f"Expected: {expected_zones}, Got: {cluster._gce_zone_names}"
        )

    @patch("sdcm.cluster_gce.random_zone")
    def test_gce_cluster_uses_random_for_invalid_zone(self, mock_random_zone):
        """Test that GCE cluster uses random_zone for invalid zone from params."""
        from sdcm.cluster_gce import ScyllaGCECluster

        # Setup mock to return valid zones
        mock_random_zone.side_effect = ["d", "a", "b"]

        # Create mock params with invalid availability_zone for the regions
        mock_params = MagicMock(spec=SCTConfiguration)
        mock_params.get.side_effect = lambda key: {
            "availability_zone": "a",  # Invalid for us-east1 (only c,d valid)
            "cluster_backend": "gce",
        }.get(key)

        # Mock GCE service
        mock_gce_service = (MagicMock(), {"project_id": "test-project"})

        # Create cluster instance
        # Patch methods that are called during initialization
        with (
            patch("sdcm.cluster_gce.TestConfig"),
            patch.object(ScyllaGCECluster, "init_log_directory"),
            patch("sdcm.cluster.ScyllaClusterBenchmarkManager"),
        ):
            cluster = ScyllaGCECluster(
                gce_image="test-image",
                gce_image_type="test-type",
                gce_image_size=10,
                gce_network="test-network",
                gce_service=mock_gce_service,
                credentials=MagicMock(),
                gce_region_names=["us-east1", "us-east4", "us-west1"],
                params=mock_params,
                cluster_uuid="test-uuid",
                n_nodes=0,
                add_nodes=False,
            )

        # Verify that random_zone was called for us-east1 (where 'a' is invalid)
        # but NOT for us-east4 and us-west1 (where 'a' is valid)
        assert mock_random_zone.call_count == 1, "random_zone should be called only for us-east1"
        
        # Verify the zones: us-east1 gets random 'd', us-east4 and us-west1 use 'a'
        expected_zones = ["us-east1-d", "us-east4-a", "us-west1-a"]
        assert cluster._gce_zone_names == expected_zones, (
            f"Cluster should use random for invalid zone, valid for others. "
            f"Expected: {expected_zones}, Got: {cluster._gce_zone_names}"
        )
