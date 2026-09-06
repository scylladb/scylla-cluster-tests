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

import pytest
from pydantic import ValidationError

from sdcm.sct_config import GrafanaPanelScreenshot
from sdcm.logcollector import MonitoringStack


# =============================================================================
# GrafanaPanelScreenshot Model Tests
# =============================================================================


def test_grafana_panel_screenshot_valid_input():
    """Test that GrafanaPanelScreenshot accepts valid input."""
    panel = GrafanaPanelScreenshot(dashboard_title="Detailed", panel_title="LSA total memory")
    assert panel.dashboard_title == "Detailed"
    assert panel.panel_title == "LSA total memory"
    assert panel.resolution == (1920, 4000)  # default resolution


def test_grafana_panel_screenshot_custom_resolution():
    """Test that GrafanaPanelScreenshot accepts custom resolution."""
    panel = GrafanaPanelScreenshot(dashboard_title="Overview", panel_title="Total Disk Usage", resolution=(1280, 720))
    assert panel.resolution == (1280, 720)


def test_grafana_panel_screenshot_missing_dashboard_title():
    """Test that GrafanaPanelScreenshot raises ValidationError when dashboard_title is missing."""
    with pytest.raises(ValidationError) as exc_info:
        GrafanaPanelScreenshot(panel_title="LSA total memory")
    assert "dashboard_title" in str(exc_info.value)


def test_grafana_panel_screenshot_missing_panel_title():
    """Test that GrafanaPanelScreenshot raises ValidationError when panel_title is missing."""
    with pytest.raises(ValidationError) as exc_info:
        GrafanaPanelScreenshot(dashboard_title="Detailed")
    assert "panel_title" in str(exc_info.value)


def test_grafana_panel_screenshot_invalid_resolution_type():
    """Test that GrafanaPanelScreenshot validates resolution tuple type."""
    with pytest.raises(ValidationError):
        GrafanaPanelScreenshot(
            dashboard_title="Detailed",
            panel_title="LSA total memory",
            resolution="1920x4000",  # should be tuple, not string
        )


def test_grafana_panel_screenshot_invalid_resolution_length():
    """Test that GrafanaPanelScreenshot validates resolution tuple length."""
    with pytest.raises(ValidationError):
        GrafanaPanelScreenshot(
            dashboard_title="Detailed",
            panel_title="LSA total memory",
            resolution=(1920, 4000, 24),  # should have 2 elements
        )


# =============================================================================
# GrafanaPanelScreenshot Property Tests
# =============================================================================


def test_grafana_panel_screenshot_title_property():
    """Test that .title property returns dashboard_title."""
    panel = GrafanaPanelScreenshot(dashboard_title="Detailed Dashboard", panel_title="LSA total memory")
    assert panel.title == "Detailed Dashboard"


def test_grafana_panel_screenshot_name_property_simple():
    """Test that .name property returns lowercased slug without special chars."""
    panel = GrafanaPanelScreenshot(dashboard_title="Detailed", panel_title="LSA total memory")
    # Expected: "detailed-lsa_total_memory" (spaces and mixed case handled)
    assert panel.name == "detailed-lsa_total_memory"


def test_grafana_panel_screenshot_name_property_sanitizes_slashes():
    """Test that .name property replaces slashes with underscores."""
    panel = GrafanaPanelScreenshot(dashboard_title="Overview/Summary", panel_title="Disk/Usage")
    # Expected: "overview_summary-disk_usage" (slashes replaced with underscores)
    assert "/" not in panel.name
    assert panel.name == "overview_summary-disk_usage"


def test_grafana_panel_screenshot_name_property_sanitizes_parens():
    """Test that .name property replaces parentheses with underscores."""
    panel = GrafanaPanelScreenshot(dashboard_title="Detailed (Production)", panel_title="Memory (GB)")
    # Expected: "detailed__production_-memory__gb_" or similar with parens replaced
    assert "(" not in panel.name
    assert ")" not in panel.name


def test_grafana_panel_screenshot_name_property_sanitizes_colons():
    """Test that .name property replaces colons with underscores."""
    panel = GrafanaPanelScreenshot(dashboard_title="Overview: Stats", panel_title="Time: Latency")
    # Expected: "overview__stats-time__latency"
    assert ":" not in panel.name


@pytest.mark.parametrize(
    "dashboard_title,panel_title,expected_name",
    [
        ("Detailed", "LSA total memory", "detailed-lsa_total_memory"),
        ("Overview/Summary", "Disk Usage", "overview_summary-disk_usage"),
        ("Prod (Main)", "CPU %", "prod__main_-cpu__"),
        ("Node: Status", "Health Check", "node__status-health_check"),
        ("Complex/Test(Env): Data", "Value %/Rate", "complex_test_env___data-value___rate"),
    ],
)
def test_grafana_panel_screenshot_name_property_various_inputs(dashboard_title, panel_title, expected_name):
    """Test that .name property sanitizes various special characters correctly."""
    panel = GrafanaPanelScreenshot(dashboard_title=dashboard_title, panel_title=panel_title)
    assert panel.name == expected_name


# =============================================================================
# MonitoringStack._find_panel_id Tests
# =============================================================================


def test_monitoring_stack_find_panel_id_simple():
    """Test _find_panel_id finds panel at top level."""
    panels = [
        {"id": 1, "title": "LSA total memory"},
        {"id": 2, "title": "Heap usage"},
    ]
    result = MonitoringStack._find_panel_id(panels, "LSA total memory")
    assert result == 1


def test_monitoring_stack_find_panel_id_substring_match():
    """Test _find_panel_id matches on substring."""
    panels = [
        {"id": 1, "title": "LSA total memory usage (bytes)"},
        {"id": 2, "title": "Heap usage"},
    ]
    result = MonitoringStack._find_panel_id(panels, "LSA total memory")
    assert result == 1


def test_monitoring_stack_find_panel_id_not_found():
    """Test _find_panel_id returns None when panel not found."""
    panels = [
        {"id": 1, "title": "LSA total memory"},
        {"id": 2, "title": "Heap usage"},
    ]
    result = MonitoringStack._find_panel_id(panels, "Non-existent panel")
    assert result is None


def test_monitoring_stack_find_panel_id_nested_in_row():
    """Test _find_panel_id finds panel nested inside row panels."""
    panels = [
        {
            "id": 1,
            "title": "Row 1",
            "panels": [
                {"id": 10, "title": "LSA total memory"},
                {"id": 11, "title": "Heap usage"},
            ],
        },
        {
            "id": 2,
            "title": "Row 2",
            "panels": [
                {"id": 20, "title": "CPU usage"},
            ],
        },
    ]
    result = MonitoringStack._find_panel_id(panels, "LSA total memory")
    assert result == 10


def test_monitoring_stack_find_panel_id_deeply_nested():
    """Test _find_panel_id finds deeply nested panels."""
    panels = [
        {
            "id": 1,
            "title": "Row 1",
            "panels": [
                {
                    "id": 10,
                    "title": "Section",
                    "panels": [
                        {"id": 100, "title": "LSA total memory"},
                    ],
                },
            ],
        },
    ]
    result = MonitoringStack._find_panel_id(panels, "LSA total memory")
    assert result == 100


def test_monitoring_stack_find_panel_id_returns_first_match():
    """Test _find_panel_id returns first matching panel."""
    panels = [
        {"id": 1, "title": "LSA total memory"},
        {"id": 2, "title": "LSA total memory (copy)"},
    ]
    result = MonitoringStack._find_panel_id(panels, "LSA total memory")
    assert result == 1  # Should return first match, not second


def test_monitoring_stack_find_panel_id_empty_title():
    """Test _find_panel_id handles panels with empty or missing title."""
    panels = [
        {"id": 1},  # Missing title
        {"id": 2, "title": ""},  # Empty title
        {"id": 3, "title": "LSA total memory"},
    ]
    result = MonitoringStack._find_panel_id(panels, "LSA total memory")
    assert result == 3


def test_monitoring_stack_find_panel_id_empty_panels_list():
    """Test _find_panel_id handles empty panels list."""
    result = MonitoringStack._find_panel_id([], "LSA total memory")
    assert result is None


def test_monitoring_stack_find_panel_id_none_in_nested():
    """Test _find_panel_id handles None or missing nested panels gracefully."""
    panels = [
        {"id": 1, "title": "Row 1", "panels": None},  # panels is None
        {"id": 2, "title": "Row 2"},  # panels key missing
        {"id": 3, "title": "LSA total memory"},
    ]
    result = MonitoringStack._find_panel_id(panels, "LSA total memory")
    assert result == 3


@pytest.mark.parametrize(
    "panels,search_title,expected_id",
    [
        # Simple cases
        ([{"id": 1, "title": "Panel A"}, {"id": 2, "title": "Panel B"}], "Panel A", 1),
        ([{"id": 1, "title": "Panel A"}, {"id": 2, "title": "Panel B"}], "Panel B", 2),
        ([{"id": 1, "title": "Panel A"}], "Not Found", None),
        # Substring matching
        ([{"id": 1, "title": "LSA total memory usage"}], "total memory", 1),
        # Nested panels
        ([{"id": 1, "title": "Row", "panels": [{"id": 10, "title": "Target"}]}], "Target", 10),
        # Multiple nesting levels
        (
            [{"id": 1, "title": "R1", "panels": [{"id": 10, "title": "S1", "panels": [{"id": 100, "title": "Deep"}]}]}],
            "Deep",
            100,
        ),
    ],
)
def test_monitoring_stack_find_panel_id_various_cases(panels, search_title, expected_id):
    """Test _find_panel_id with various panel structures."""
    result = MonitoringStack._find_panel_id(panels, search_title)
    assert result == expected_id


def test_monitoring_stack_find_panel_id_numeric_id():
    """Test _find_panel_id returns numeric panel ID (not string)."""
    panels = [
        {"id": 42, "title": "LSA total memory"},
    ]
    result = MonitoringStack._find_panel_id(panels, "LSA total memory")
    assert result == 42
    assert isinstance(result, int)


# =============================================================================
# Integration-style tests for Resolution and URL Params
# =============================================================================


def test_grafana_panel_screenshot_resolution_for_height():
    """Test that resolution[1] can be used for screenshot height parameter."""
    panel = GrafanaPanelScreenshot(dashboard_title="Overview", panel_title="CPU Usage", resolution=(1920, 800))
    # Verify height would be extracted correctly
    height = panel.resolution[1]
    assert height == 800
    assert isinstance(height, int)


def test_grafana_panel_screenshot_resolution_default_height():
    """Test that default resolution provides sensible height."""
    panel = GrafanaPanelScreenshot(dashboard_title="Overview", panel_title="CPU Usage")
    # Default is (1920, 4000)
    height = panel.resolution[1]
    assert height == 4000
    assert panel.resolution[0] == 1920


@pytest.mark.parametrize(
    "resolution,expected_width,expected_height",
    [
        ((1920, 4000), 1920, 4000),
        ((1280, 720), 1280, 720),
        ((2560, 1440), 2560, 1440),
        ((800, 600), 800, 600),
    ],
)
def test_grafana_panel_screenshot_resolution_extraction(resolution, expected_width, expected_height):
    """Test that resolution tuple can be correctly unpacked for URL parameters."""
    panel = GrafanaPanelScreenshot(dashboard_title="Test", panel_title="Panel", resolution=resolution)
    width, height = panel.resolution
    assert width == expected_width
    assert height == expected_height
