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

"""Unit tests for GCE image filter construction."""

import pytest

from sdcm.utils.common import build_gce_image_filter, gce_image_version_label


@pytest.mark.parametrize(
    "version,expected",
    [
        pytest.param("2026.4.0~dev-0.20260804.9a3aba9e452a", "2026-4-0-dev-0-20260804-9a3aba9e452a", id="dev"),
        pytest.param("2025.4.10-0.20260609.99f4121cd8e1-1", "2025-4-10-0-20260609-99f4121cd8e1", id="release"),
        pytest.param("2026.3.0.rc1.0.20260730.726f67a532e2", "2026-3-0-rc1-0-20260730-726f67a532e2", id="rc"),
        pytest.param("2025.4", "", id="open-ended-version"),
        pytest.param("master:latest", "", id="branch"),
        pytest.param("", "", id="empty"),
    ],
)
def test_gce_image_version_label(version, expected):
    assert gce_image_version_label(version) == expected


class TestGceImageFilterConstruction:
    """Test suite for GCE image filter construction."""

    @pytest.mark.parametrize(
        "version,expected_substring,should_not_contain",
        [
            # Simple version with 2 parts (e.g., enterprise version format)
            pytest.param(
                "2025.1",
                "(labels.scylla_version eq '2025-1.*(-\\d)?(\\d)?(\\d)?(-rc)?(\\d)?(\\d)?')",
                "')')(",
                id="enterprise-version-2025.1",
            ),
            # Simple version with 3 parts
            pytest.param("5.2.1", "(labels.scylla_version eq '5-2-1.*')", "')')(", id="oss-version-5.2.1"),
            # RC version
            pytest.param("2025.1-rc1", "(labels.scylla_version eq '2025-1-rc1.*')", "')')(", id="rc-version"),
        ],
    )
    def test_simple_version_filter_construction(self, version, expected_substring, should_not_contain):
        """Test that simple version filters are constructed correctly."""
        filters = build_gce_image_filter(version)

        # Should contain the expected substring
        assert expected_substring in filters, f"Expected '{expected_substring}' in filter: {filters}"

        # Should NOT contain the buggy pattern (extra closing parenthesis/quote)
        assert should_not_contain not in filters, f"Filter should not contain '{should_not_contain}': {filters}"

        # Should contain production environment filter
        assert "(labels.environment eq 'production')" in filters

    @pytest.mark.parametrize(
        "version,expected_label",
        [
            pytest.param(
                "2024.2.5-0.20250221.cb9e2a54ae6d-1", "2024-2-5-0-20250221-cb9e2a54ae6d", id="full-enterprise-tag"
            ),
            pytest.param("5.4.8-0.20250221.9cc3d32e35b4-1", "5-4-8-0-20250221-9cc3d32e35b4", id="full-oss-tag"),
            pytest.param(
                "2026.4.0~dev-0.20260804.9a3aba9e452a", "2026-4-0-dev-0-20260804-9a3aba9e452a", id="dev-nightly-tag"
            ),
            pytest.param("2026.3.0.rc1.0.20260730.726f67a532e2", "2026-3-0-rc1-0-20260730-726f67a532e2", id="rc-tag"),
        ],
    )
    def test_full_version_tag_filter_construction(self, version, expected_label):
        """Full version tags match one exact build, whatever environment it was promoted to.

        Nightly images are labeled `environment=daily` and fresh release candidates
        `environment=candidate`, so filtering on `production` would hide the very build that
        was asked for (SCT-665). The `-N` packaging revision AWS tags carry is not part of
        the GCE label either.
        """
        filters = build_gce_image_filter(version)

        assert "(labels.environment eq 'production')" not in filters
        assert f"(labels.scylla_version eq '{expected_label}')" in filters
        assert "(name ne debug-.*)" in filters

    def test_no_version_filter(self):
        """Test filter when no version is specified."""
        filters = build_gce_image_filter(None)

        # Should have family + production environment filters
        assert filters == "(family eq 'scylla(-enterprise)?')(labels.environment eq 'production')"

    def test_all_version_filter(self):
        """Test filter when version is 'all'."""
        filters = build_gce_image_filter("all")

        # Should have family + production environment filters
        assert filters == "(family eq 'scylla(-enterprise)?')(labels.environment eq 'production')"

    def test_filter_starts_with_family(self):
        """Test that all filters start with the family filter."""
        for version in ["2025.1", "5.2.1", None, "all", "2024.2.5-0.20250221.cb9e2a54ae6d-1"]:
            filters = build_gce_image_filter(version)
            assert filters.startswith("(family eq 'scylla(-enterprise)?')"), (
                f"Filter for version={version} should start with the family filter: {filters}"
            )

    def test_open_ended_versions_stay_on_released_images(self):
        """Only exact builds skip the production filter — "give me 2025.1" still means released."""
        for version in ["2025.1", "5.2.1", None, "all"]:
            assert "(labels.environment eq 'production')" in build_gce_image_filter(version)
