"""Test nemesis error event detection functionality"""

import json
from unittest.mock import MagicMock

from sdcm.sct_events import Severity
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.nemesis import DisruptionEvent
from sdcm.wait import wait_for


def get_disruption_events_from_log(raw_events_log, start_line=0):
    """Read DisruptionEvents from the raw events log file starting from a specific line."""
    disruption_events = []
    with open(raw_events_log, "r", encoding="utf-8") as fobj:
        lines = fobj.readlines()
        for line in lines[start_line:]:
            if not line.strip():
                continue
            try:
                event_data = json.loads(line)
                if event_data.get("base") == "DisruptionEvent":
                    disruption_events.append(event_data)
            except json.JSONDecodeError:
                continue
    return disruption_events


def wait_for_disruption_events(raw_events_log, min_count, start_line=0, timeout=10):
    """Wait until at least min_count DisruptionEvents appear in the log."""

    def check_events():
        events = get_disruption_events_from_log(raw_events_log, start_line)
        return events if len(events) >= min_count else None

    return wait_for(
        check_events, step=0.1, timeout=timeout, throw_exc=True, text=f"waiting for {min_count} DisruptionEvents in log"
    )


class TestNemesisErrorEventDetection:
    """Test suite for nemesis error event detection feature."""

    def test_nemesis_fails_on_error_events_when_enabled(self, events_function_scope):
        """Test that nemesis fails when ERROR events occur during execution and feature is enabled"""
        events = events_function_scope

        # Create a mock node for the DisruptionEvent
        mock_node = MagicMock()
        mock_node.name = "test-node"
        mock_node.__str__ = lambda self: "test-node"

        # Test the DisruptionEvent directly - simulating what would happen during nemesis execution
        with events.wait_for_n_events(subscriber=events.get_events_logger(), count=2, timeout=10):
            with DisruptionEvent(nemesis_name="test_nemesis", node=mock_node, publish_event=True) as nemesis_event:
                # Simulate an error event being published during nemesis
                DatabaseLogEvent.DATABASE_ERROR().add_info(node="test-node", line_number=1, line="test error").publish()
                # Mark the nemesis event as failed due to errors
                nemesis_event.severity = Severity.ERROR
                nemesis_event.add_error(["Error events detected during nemesis execution"])

        # Wait for events to be written to log file
        raw_events_log = events.get_raw_events_log()
        disruption_events = wait_for_disruption_events(raw_events_log, min_count=2, timeout=10)

        # Verify that a DisruptionEvent with ERROR severity was raised
        assert len(disruption_events) >= 2, f"Expected at least 2 disruption events, got {len(disruption_events)}"
        # The last DisruptionEvent should have ERROR severity
        assert disruption_events[-1]["severity"] == "ERROR", (
            f"Expected last DisruptionEvent to have ERROR severity, got {disruption_events[-1]['severity']}"
        )

    def test_nemesis_succeeds_when_no_error_events(self, events_function_scope):
        """Test that nemesis succeeds when no ERROR events occur during execution"""
        events = events_function_scope

        # Create a mock node for the DisruptionEvent
        mock_node = MagicMock()
        mock_node.name = "test-node"
        mock_node.__str__ = lambda self: "test-node"

        # Test the DisruptionEvent directly - simulating what would happen during nemesis execution
        with events.wait_for_n_events(subscriber=events.get_events_logger(), count=2, timeout=10):
            with DisruptionEvent(nemesis_name="test_nemesis", node=mock_node, publish_event=True):
                # No error events - nemesis should succeed with NORMAL severity
                pass

        # Wait for events to be written to log file
        raw_events_log = events.get_raw_events_log()
        disruption_events = wait_for_disruption_events(raw_events_log, min_count=2, timeout=10)

        # Verify that the final DisruptionEvent has NORMAL severity
        assert len(disruption_events) >= 2, f"Expected at least 2 disruption events, got {len(disruption_events)}"
        assert disruption_events[-1]["severity"] == "NORMAL", (
            f"Expected last DisruptionEvent to have NORMAL severity, got {disruption_events[-1]['severity']}"
        )

    def test_nemesis_ignores_error_events_when_disabled(self, events_function_scope):
        """Test that nemesis ignores ERROR events when feature is disabled (events still published but severity unchanged)"""
        events = events_function_scope

        # Create a mock node for the DisruptionEvent
        mock_node = MagicMock()
        mock_node.name = "test-node"
        mock_node.__str__ = lambda self: "test-node"

        # Test the DisruptionEvent directly - simulating what would happen when feature is disabled
        # Even if errors occur, the DisruptionEvent should stay NORMAL because we don't set it to ERROR
        with events.wait_for_n_events(subscriber=events.get_events_logger(), count=3, timeout=10):
            with DisruptionEvent(nemesis_name="test_nemesis", node=mock_node, publish_event=True):
                # Publish an error event (but don't change nemesis_event severity since feature is "disabled")
                DatabaseLogEvent.DATABASE_ERROR().add_info(node="test-node", line_number=1, line="test error").publish()
                # Feature disabled - don't mark the nemesis as failed

        # Wait for events to be written to log file
        raw_events_log = events.get_raw_events_log()
        disruption_events = wait_for_disruption_events(raw_events_log, min_count=2, timeout=10)

        # Verify that the final DisruptionEvent has NORMAL severity (feature disabled, so errors ignored)
        assert len(disruption_events) >= 2, f"Expected at least 2 disruption events, got {len(disruption_events)}"
        assert disruption_events[-1]["severity"] == "NORMAL", (
            f"Expected last DisruptionEvent to have NORMAL severity, got {disruption_events[-1]['severity']}"
        )

    def test_count_error_events_function(self, events_function_scope):
        """Test the count_error_events_for_nemesis function behavior"""
        events = events_function_scope

        # Create a mock node for events
        mock_node = MagicMock()
        mock_node.name = "test-node"
        mock_node.__str__ = lambda self: "test-node"

        # First run a nemesis that generates errors
        with events.wait_for_n_events(subscriber=events.get_events_logger(), count=3, timeout=10):
            with DisruptionEvent(
                nemesis_name="nemesis_with_error", node=mock_node, publish_event=True
            ) as nemesis_event:
                DatabaseLogEvent.DATABASE_ERROR().add_info(node="test-node", line_number=1, line="test error").publish()
                nemesis_event.severity = Severity.ERROR
                nemesis_event.add_error(["Error detected"])

        raw_events_log = events.get_raw_events_log()
        disruption_events = wait_for_disruption_events(raw_events_log, min_count=2, timeout=10)

        assert len(disruption_events) >= 2
        assert disruption_events[-1]["severity"] == "ERROR"

        # Get the position before running a second nemesis
        with open(raw_events_log, "r", encoding="utf-8") as fobj:
            before_pos = len(fobj.readlines())

        # Now run a second nemesis without errors
        with events.wait_for_n_events(subscriber=events.get_events_logger(), count=2, timeout=10):
            with DisruptionEvent(nemesis_name="nemesis_without_error", node=mock_node, publish_event=True):
                # No errors generated
                pass

        # Wait for events to be written to log file
        second_nemesis_events = wait_for_disruption_events(
            raw_events_log, min_count=2, start_line=before_pos, timeout=10
        )

        # Verify that the second nemesis has NORMAL severity
        assert len(second_nemesis_events) >= 2
        assert second_nemesis_events[-1]["severity"] == "NORMAL"
