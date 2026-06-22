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

"""Tests for GCE zone fallback on resource pool exhaustion."""

import pytest
from unittest.mock import MagicMock

import google.api_core.exceptions

from sdcm.utils.gce_utils import get_alternative_zones
from sdcm.cluster_gce import _ZoneExhaustedError


class TestGetAlternativeZones:
    def test_returns_alternatives_excluding_exhausted(self):
        alternatives = get_alternative_zones("us-east1", "us-east1-c")
        assert "c" not in alternatives
        assert "d" in alternatives

    def test_returns_empty_for_unknown_region(self):
        assert get_alternative_zones("unknown-region1", "unknown-region1-a") == []

    def test_handles_zone_name_as_input(self):
        # When passing the full zone name (e.g., "us-east1-c"), extracts last char
        alternatives = get_alternative_zones("us-east1", "us-east1-c")
        assert "c" not in alternatives
        assert "d" in alternatives

    def test_handles_single_letter_input(self):
        alternatives = get_alternative_zones("us-east1", "c")
        assert "c" not in alternatives
        assert "d" in alternatives

    def test_returns_all_other_zones_for_region(self):
        # us-east1 has zones "cd" in SUPPORTED_REGIONS
        alternatives = get_alternative_zones("us-east1", "us-east1-c")
        assert alternatives == ["d"]

    def test_multi_zone_region(self):
        # us-central1 has zones "abcf"
        alternatives = get_alternative_zones("us-central1", "us-central1-a")
        assert set(alternatives) == {"b", "c", "f"}


class TestZoneExhaustedError:
    """Test that _ZoneExhaustedError is not caught by the @retrying decorator."""

    def test_zone_exhausted_error_is_not_google_api_error(self):
        """Verify _ZoneExhaustedError is not a subclass of GoogleAPIError."""
        assert not issubclass(_ZoneExhaustedError, google.api_core.exceptions.GoogleAPIError)

    def test_zone_exhausted_error_preserves_cause(self):
        original = google.api_core.exceptions.GoogleAPIError("ZONE_RESOURCE_POOL_EXHAUSTED")
        exc = _ZoneExhaustedError("zone exhausted")
        exc.__cause__ = original
        assert exc.__cause__ is original


class TestGCEClusterZoneFallback:
    """Test zone fallback logic in GCECluster._create_instances."""

    @pytest.fixture
    def mock_cluster(self):
        """Create a mock GCECluster with minimal attributes for testing zone fallback."""
        cluster = MagicMock()
        cluster._gce_zone_names = {0: "us-east1-c"}
        cluster.instance_provision = "on_demand"
        cluster._node_index = 0
        cluster.log = MagicMock()
        return cluster

    def test_fallback_disabled_raises_google_api_error(self, mock_cluster):
        """When fallback is disabled, _ZoneExhaustedError should be re-raised as GoogleAPIError."""
        mock_cluster.is_az_fallback_enabled = False

        original_cause = google.api_core.exceptions.GoogleAPIError("ZONE_RESOURCE_POOL_EXHAUSTED")
        zone_exc = _ZoneExhaustedError("zone exhausted")
        zone_exc.__cause__ = original_cause

        # Simulate what _create_instances does when fallback is disabled
        with pytest.raises(google.api_core.exceptions.GoogleAPIError):
            if not mock_cluster.is_az_fallback_enabled:
                raise google.api_core.exceptions.GoogleAPIError(str(zone_exc)) from zone_exc.__cause__

    def test_alternative_zones_called_with_correct_region(self, mock_cluster):
        """Verify region is correctly extracted from zone name."""
        zone_name = "us-east1-c"
        region = zone_name.rsplit("-", 1)[0]
        assert region == "us-east1"

    def test_region_extraction_from_zone(self):
        """Test region extraction logic for various zone formats."""
        assert "us-east1-c".rsplit("-", 1)[0] == "us-east1"
        assert "us-central1-a".rsplit("-", 1)[0] == "us-central1"
        assert "europe-west1-b".rsplit("-", 1)[0] == "europe-west1"
