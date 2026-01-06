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

from sdcm.utils.version_utils import parse_scylla_version_tag


def build_gce_filter(version: str = None) -> str:
    """
    Build GCE image filter string.

    This mimics the filter construction logic in get_scylla_gce_images_versions.
    """
    filters = "(family eq 'scylla(-enterprise)?')"
    if version and version != "all":
        if parse_scylla_version_tag(version):
            normalized_version = version.replace(".", "-").replace("~", "-")
            filters += f"(labels.scylla_version eq '{normalized_version}')"
        else:
            filters += "(labels.environment eq 'production')"
            filters += f"(labels.scylla_version eq '{version.replace('.', '-').replace('~', '-')}.*"
            if "rc" not in version and len(version.split(".")) < 3:
                filters += "(-\\d)?(\\d)?(\\d)?(-rc)?(\\d)?(\\d)?')"
            else:
                filters += "')"
    return filters


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
        filters = build_gce_filter(version)

        # Should contain the expected substring
        assert expected_substring in filters, f"Expected '{expected_substring}' in filter: {filters}"

        # Should NOT contain the buggy pattern (extra closing parenthesis/quote)
        assert should_not_contain not in filters, f"Filter should not contain '{should_not_contain}': {filters}"

        # Should contain production environment filter
        assert "(labels.environment eq 'production')" in filters

    @pytest.mark.parametrize(
        "version",
        [
            pytest.param("2024.2.5-0.20250221.cb9e2a54ae6d-1", id="full-enterprise-tag"),
            pytest.param("5.4.8-0.20250221.9cc3d32e35b4-1", id="full-oss-tag"),
        ],
    )
    def test_full_version_tag_filter_construction(self, version):
        """Test that full version tags use exact matching."""
        filters = build_gce_filter(version)

        # Should NOT contain production environment filter (exact match)
        assert "(labels.environment eq 'production')" not in filters

        # Should contain normalized version label
        normalized = version.replace(".", "-").replace("~", "-")
        assert f"(labels.scylla_version eq '{normalized}')" in filters

        # Should NOT contain the buggy pattern
        assert "')')('" not in filters

    def test_no_version_filter(self):
        """Test filter when no version is specified."""
        filters = build_gce_filter(None)

        # Should only have the family filter
        assert filters == "(family eq 'scylla(-enterprise)?')"

    def test_all_version_filter(self):
        """Test filter when version is 'all'."""
        filters = build_gce_filter("all")

        # Should only have the family filter
        assert filters == "(family eq 'scylla(-enterprise)?')"

    def test_filter_starts_with_family(self):
        """Test that all filters start with the family filter."""
        for version in ["2025.1", "5.2.1", None, "all", "2024.2.5-0.20250221.cb9e2a54ae6d-1"]:
            filters = build_gce_filter(version)
            assert filters.startswith("(family eq 'scylla(-enterprise)?')"), (
                f"Filter for version={version} should start with family filter: {filters}"
            )
