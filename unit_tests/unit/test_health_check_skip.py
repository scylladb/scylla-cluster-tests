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

"""
Unit tests for health check optimization:
Skip health checks when previous nemesis was skipped.
"""

import pytest

from sdcm.sct_events import Severity
from sdcm.sct_events.nemesis import DisruptionEvent


class TestHealthCheckSkip:
    """Test suite for health check optimization."""

    @pytest.fixture
    def skipped_nemesis_event(self):
        """Create a skipped nemesis event."""
        event = DisruptionEvent(
            nemesis_name="TestNemesis",
            node="test_node",
            severity=Severity.NORMAL,
            publish_event=False,
        )
        event.skip(skip_reason="Test condition not met")
        return event

    @pytest.fixture
    def executed_nemesis_event(self):
        """Create a successfully executed nemesis event."""
        event = DisruptionEvent(
            nemesis_name="TestNemesis",
            node="test_node",
            severity=Severity.NORMAL,
            publish_event=False,
        )
        # Don't call skip() - this simulates an executed nemesis
        return event

    def test_nemesis_event_is_skipped_property(self, skipped_nemesis_event, executed_nemesis_event):
        """Test that nemesis event is_skipped property works correctly."""
        # Skipped event
        assert skipped_nemesis_event.is_skipped is True
        assert skipped_nemesis_event.skip_reason == "Test condition not met"

        # Executed event
        assert executed_nemesis_event.is_skipped is False
        assert executed_nemesis_event.skip_reason == ""

    def test_skip_reason_captured_correctly(self):
        """Test that skip reason is properly captured and accessible."""
        event = DisruptionEvent(
            nemesis_name="TestNemesis",
            node="test_node",
            severity=Severity.NORMAL,
            publish_event=False,
        )

        # Initially not skipped
        assert event.is_skipped is False
        assert event.skip_reason == ""

        # After calling skip()
        skip_reason = "Unsupported Scylla version"
        event.skip(skip_reason=skip_reason)
        assert event.is_skipped is True
        assert event.skip_reason == skip_reason
