"""Test nemesis error event detection functionality"""

import json
import time
import threading

from sdcm.nemesis import Nemesis
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.base import Severity
from unit_tests.nemesis.fake_cluster import FakeTester


def test_nemesis_fails_on_error_events_when_enabled(events):
    """Test that nemesis fails when ERROR events occur during execution and feature is enabled"""
    # Setup
    params = {
        "nemesis_interval": 0.001,
        "nemesis_filter_seeds": False,
        "nemesis_fail_on_error_events": True,
    }
    tester = FakeTester(params=params)

    # Create a simple nemesis class for testing
    class TestNemesis(Nemesis):
        def disrupt(self):
            time.sleep(0.2)
            event = DatabaseLogEvent.DATABASE_ERROR(line="test error")
            event.node = self.target_node
            event.publish()
            time.sleep(0.2)

    nemesis = TestNemesis(tester, termination_event=threading.Event())

    # Execute the nemesis method - expect 3 events: begin DisruptionEvent, DatabaseLogEvent, end DisruptionEvent
    with events.wait_for_n_events(subscriber=events.get_events_logger(), count=3, timeout=5):
        nemesis.run(cycles_count=1)

    # Read events from raw_events.log to verify
    raw_events_log = events.get_raw_events_log()
    disruption_events = []
    with open(raw_events_log, "r", encoding="utf-8") as fobj:
        for line in fobj:
            if not line.strip():
                continue
            try:
                event_data = json.loads(line)
                if event_data.get("base") == "DisruptionEvent":
                    disruption_events.append(event_data)
            except json.JSONDecodeError:
                continue

    # Verify that a DisruptionEvent with ERROR severity was raised
    assert len(disruption_events) >= 2  # Begin and End events
    # The last DisruptionEvent should have ERROR severity
    assert disruption_events[-1]["severity"] == "ERROR"


def test_nemesis_succeeds_when_no_error_events(events):
    """Test that nemesis succeeds when no ERROR events occur during execution"""
    # Setup
    params = {
        "nemesis_interval": 0.001,
        "nemesis_filter_seeds": False,
        "nemesis_fail_on_error_events": True,
    }
    tester = FakeTester(params=params)

    # Create a simple nemesis class for testing
    class TestNemesis(Nemesis):
        def disrupt(self):
            # Simulate some work without generating errors
            time.sleep(0.2)

    nemesis = TestNemesis(tester, termination_event=threading.Event())

    # Execute the nemesis method - expect 2 events: begin and end DisruptionEvent
    with events.wait_for_n_events(subscriber=events.get_events_logger(), count=2, timeout=5):
        nemesis.run(cycles_count=1)

    # Read events from raw_events.log to verify
    raw_events_log = events.get_raw_events_log()
    disruption_events = []
    with open(raw_events_log, "r", encoding="utf-8") as fobj:
        for line in fobj:
            if not line.strip():
                continue
            try:
                event_data = json.loads(line)
                if event_data.get("base") == "DisruptionEvent":
                    disruption_events.append(event_data)
            except json.JSONDecodeError:
                continue

    # Verify that the final DisruptionEvent has NORMAL severity
    assert len(disruption_events) >= 2
    assert disruption_events[-1]["severity"] == "NORMAL"


def test_nemesis_ignores_error_events_when_disabled(events):
    """Test that nemesis ignores ERROR events when feature is disabled"""
    # Setup
    params = {
        "nemesis_interval": 0.001,
        "nemesis_filter_seeds": False,
        "nemesis_fail_on_error_events": False,  # Feature disabled
    }
    tester = FakeTester(params=params)

    # Create a simple nemesis class for testing
    class TestNemesis(Nemesis):
        def disrupt(self):
            time.sleep(0.2)
            event = DatabaseLogEvent.DATABASE_ERROR(line="test error")
            event.node = self.target_node
            event.publish()
            time.sleep(0.2)

    nemesis = TestNemesis(tester, termination_event=threading.Event())

    # Execute the nemesis method - expect 3 events: begin DisruptionEvent, DatabaseLogEvent, end DisruptionEvent
    with events.wait_for_n_events(subscriber=events.get_events_logger(), count=3, timeout=5):
        nemesis.run(cycles_count=1)

    # Read events from raw_events.log to verify
    raw_events_log = events.get_raw_events_log()
    disruption_events = []
    with open(raw_events_log, "r", encoding="utf-8") as fobj:
        for line in fobj:
            if not line.strip():
                continue
            try:
                event_data = json.loads(line)
                if event_data.get("base") == "DisruptionEvent":
                    disruption_events.append(event_data)
            except json.JSONDecodeError:
                continue

    # Verify that the final DisruptionEvent has NORMAL severity (feature disabled, so errors ignored)
    assert len(disruption_events) >= 2
    assert disruption_events[-1]["severity"] == "NORMAL"


def test_count_error_events_filters_other_nemesis(events):
    """Test that error event counting filters out events from other nemeses"""
    # Setup
    params = {
        "nemesis_interval": 0.001,
        "nemesis_filter_seeds": False,
        "nemesis_fail_on_error_events": True,
    }
    tester = FakeTester(params=params)

    # Create two different nemesis classes
    class TestNemesis1(Nemesis):
        def disrupt(self):
            time.sleep(0.2)

    class TestNemesis2(Nemesis):
        def disrupt(self):
            event = DatabaseLogEvent.DATABASE_ERROR(line="test error")
            event.node = self.target_node
            event.publish()
            time.sleep(2)

    nemesis1 = TestNemesis1(tester, termination_event=threading.Event())
    nemesis2 = TestNemesis2(tester, termination_event=threading.Event())

    # Execute the disrupt method for nemesis2, which will generate an error
    with events.wait_for_n_events(subscriber=events.get_events_logger(), count=3, timeout=5):
        nemesis2.run(cycles_count=1)

    # Clear the events log to start fresh for nemesis1
    raw_events_log = events.get_raw_events_log()
    # Get the position before running nemesis1
    with open(raw_events_log, "r", encoding="utf-8") as fobj:
        before_pos = len(fobj.readlines())

    # Execute the disrupt method for nemesis1
    # It should not fail, as the error event is not associated with it
    with events.wait_for_n_events(subscriber=events.get_events_logger(), count=2, timeout=5):
        nemesis1.run(cycles_count=1)

    # Read only the new events from raw_events.log to verify
    disruption_events = []
    with open(raw_events_log, "r", encoding="utf-8") as fobj:
        lines = fobj.readlines()
        for line in lines[before_pos:]:
            if not line.strip():
                continue
            try:
                event_data = json.loads(line)
                if event_data.get("base") == "DisruptionEvent":
                    disruption_events.append(event_data)
            except json.JSONDecodeError:
                continue

    # Verify that the final DisruptionEvent for nemesis1 has NORMAL severity
    assert len(disruption_events) >= 2
    assert disruption_events[-1]["severity"] == "NORMAL"

    # Now, create a nemesis that generates its own error and assert it fails
    class TestNemesis3(Nemesis):
        def disrupt(self):
            time.sleep(0.2)
            event = DatabaseLogEvent.DATABASE_ERROR(line="test error")
            event.node = self.target_node
            event.publish()
            time.sleep(0.2)

    # Get the position before running nemesis3
    with open(raw_events_log, "r", encoding="utf-8") as fobj:
        before_pos3 = len(fobj.readlines())

    nemesis3 = TestNemesis3(tester, termination_event=threading.Event())
    with events.wait_for_n_events(subscriber=events.get_events_logger(), count=3, timeout=5):
        nemesis3.run(cycles_count=1)

    # Read only the new events for nemesis3
    nemesis3_events = []
    with open(raw_events_log, "r", encoding="utf-8") as fobj:
        lines = fobj.readlines()
        for line in lines[before_pos3:]:
            if not line.strip():
                continue
            try:
                event_data = json.loads(line)
                if event_data.get("base") == "DisruptionEvent":
                    nemesis3_events.append(event_data)
            except json.JSONDecodeError:
                continue

    assert len(nemesis3_events) >= 2
    assert nemesis3_events[-1]["severity"] == "ERROR"

