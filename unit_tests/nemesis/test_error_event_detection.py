"""Test nemesis error event detection functionality"""

import time
import threading

from sdcm.nemesis import Nemesis
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.nemesis import DisruptionEvent
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

    # Execute the nemesis method and capture events
    with events.wait_for_n_events(subscriber=events.get_events_logger(), count=3) as all_events:
        nemesis.run(cycles_count=1)

    # Verify that a DisruptionEvent with ERROR severity was raised
    disruption_events = [e for e in all_events if isinstance(e, DisruptionEvent)]
    assert len(disruption_events) == 2  # Begin and End events
    assert disruption_events[-1].severity == Severity.ERROR


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
            # Simulate some work
            time.sleep(0.2)

    nemesis = TestNemesis(tester, termination_event=threading.Event())

    # Execute the nemesis method and capture events
    with events.wait_for_n_events(subscriber=events.get_events_logger(), count=2) as all_events:
        nemesis.run(cycles_count=1)

    # Verify that the final DisruptionEvent has NORMAL severity
    disruption_events = [e for e in all_events if isinstance(e, DisruptionEvent)]
    assert len(disruption_events) == 2
    assert disruption_events[-1].severity == Severity.NORMAL


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

    # Execute the nemesis method and capture events
    with events.wait_for_n_events(subscriber=events.get_events_logger(), count=3) as all_events:
        nemesis.run(cycles_count=1)

    # Verify that the final DisruptionEvent has NORMAL severity
    disruption_events = [e for e in all_events if isinstance(e, DisruptionEvent)]
    assert len(disruption_events) == 2
    assert disruption_events[-1].severity == Severity.NORMAL


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
    with events.wait_for_n_events(subscriber=events.get_events_logger(), count=3):
        nemesis2.run(cycles_count=1)

    # Execute the disrupt method for nemesis1
    # It should not fail, as the error event is not associated with it
    with events.wait_for_n_events(subscriber=events.get_events_logger(), count=2) as all_events:
        nemesis1.run(cycles_count=1)

    # Verify that the final DisruptionEvent for nemesis1 has NORMAL severity
    nemesis1_events = [
        e for e in all_events if isinstance(e, DisruptionEvent) and e.nemesis_name == nemesis1.get_class_name()
    ]
    assert len(nemesis1_events) == 2
    assert nemesis1_events[-1].severity == Severity.NORMAL

    # Now, create a nemesis that generates its own error and assert it fails
    class TestNemesis3(Nemesis):
        def disrupt(self):
            time.sleep(0.2)
            event = DatabaseLogEvent.DATABASE_ERROR(line="test error")
            event.node = self.target_node
            event.publish()
            time.sleep(0.2)

    nemesis3 = TestNemesis3(tester, termination_event=threading.Event())
    with events.wait_for_n_events(subscriber=events.get_events_logger(), count=3) as all_events:
        nemesis3.run(cycles_count=1)

    nemesis3_events = [
        e for e in all_events if isinstance(e, DisruptionEvent) and e.nemesis_name == nemesis3.get_class_name()
    ]
    assert len(nemesis3_events) == 2
    assert nemesis3_events[-1].severity == Severity.ERROR
