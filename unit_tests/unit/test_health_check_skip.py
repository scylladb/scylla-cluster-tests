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

    def test_skip_condition_true_when_nemesis_skipped(self, skipped_nemesis_event):
        """Test that skip condition evaluates to True when nemesis was skipped."""
        # This tests the core logic: should_skip = (event exists and event.is_skipped)
        last_nemesis_event = skipped_nemesis_event
        should_skip = last_nemesis_event and last_nemesis_event.is_skipped

        assert should_skip is True
        assert last_nemesis_event.nemesis_name == "TestNemesis"
        assert last_nemesis_event.skip_reason == "Test condition not met"

    def test_skip_condition_false_when_nemesis_executed(self, executed_nemesis_event):
        """Test that skip condition evaluates to False when nemesis was executed."""
        # This tests the core logic: should_skip = (event exists and event.is_skipped)
        last_nemesis_event = executed_nemesis_event
        should_skip = last_nemesis_event and last_nemesis_event.is_skipped

        assert not should_skip

    def test_skip_condition_false_when_no_previous_nemesis(self):
        """Test that skip condition evaluates to falsy when there is no previous nemesis."""
        # This tests the core logic: should_skip = (event exists and event.is_skipped)
        last_nemesis_event = None
        should_skip = last_nemesis_event and last_nemesis_event.is_skipped

        # When last_nemesis_event is None, the expression evaluates to None (falsy)
        assert not should_skip

    def test_nemesis_event_is_skipped_property(self, skipped_nemesis_event, executed_nemesis_event):
        """Test that nemesis event is_skipped property works correctly."""
        # Skipped event
        assert skipped_nemesis_event.is_skipped is True
        assert skipped_nemesis_event.skip_reason == "Test condition not met"

        # Executed event
        assert executed_nemesis_event.is_skipped is False
        assert executed_nemesis_event.skip_reason == ""

    def test_multiple_nemesis_cycles_with_mixed_skip_states(self):
        """Test skip condition across multiple nemesis cycles with varying skip states."""
        # First nemesis: executed
        event1 = DisruptionEvent(
            nemesis_name="Nemesis1",
            node="test_node",
            severity=Severity.NORMAL,
            publish_event=False,
        )
        last_nemesis_event = event1
        should_skip = last_nemesis_event and last_nemesis_event.is_skipped
        assert not should_skip  # Health check should run

        # Second nemesis: skipped
        event2 = DisruptionEvent(
            nemesis_name="Nemesis2",
            node="test_node",
            severity=Severity.NORMAL,
            publish_event=False,
        )
        event2.skip(skip_reason="Resource unavailable")
        last_nemesis_event = event2
        should_skip = last_nemesis_event and last_nemesis_event.is_skipped
        assert should_skip is True  # Health check should be skipped

        # Third nemesis: executed
        event3 = DisruptionEvent(
            nemesis_name="Nemesis3",
            node="test_node",
            severity=Severity.NORMAL,
            publish_event=False,
        )
        last_nemesis_event = event3
        should_skip = last_nemesis_event and last_nemesis_event.is_skipped
        assert not should_skip  # Health check should run

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
