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
# Copyright (c) 2020 ScyllaDB

import re
import pickle
import unittest

from sdcm.sct_events import Severity
from sdcm.sct_events.filters import DbEventsFilter, EventsFilter, EventsSeverityChangerFilter
from sdcm.sct_events.database import DatabaseLogEvent


class TestDbEventsFilter(unittest.TestCase):
    def test_eval_filter_just_type(self):
        db_events_filter = DbEventsFilter(db_event=DatabaseLogEvent.REACTOR_STALLED)
        self.assertEqual(db_events_filter, pickle.loads(pickle.dumps(db_events_filter)))
        db_events_filter.to_json()
        event1 = DatabaseLogEvent.REACTOR_STALLED()
        event2 = DatabaseLogEvent.NO_SPACE_ERROR()
        self.assertTrue(db_events_filter.eval_filter(event1))
        self.assertFalse(db_events_filter.eval_filter(event2))

    def test_eval_filter_type_with_line(self):
        db_events_filter = DbEventsFilter(db_event=DatabaseLogEvent.BAD_ALLOC, line="y")
        event1 = DatabaseLogEvent.BAD_ALLOC().add_info(node="node1", line="xyz", line_number=1)
        event2 = event1.clone().add_info(node="node2", line="abc", line_number=1)
        event3 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="node1", line="xyz", line_number=1)
        self.assertTrue(db_events_filter.eval_filter(event1))
        self.assertFalse(db_events_filter.eval_filter(event2))
        self.assertFalse(db_events_filter.eval_filter(event3))

    def test_eval_filter_type_with_node(self):
        db_events_filter = DbEventsFilter(db_event=DatabaseLogEvent.BAD_ALLOC, node="node1")
        event1 = DatabaseLogEvent.BAD_ALLOC().add_info(node="node1", line="xyz", line_number=1)
        event2 = event1.clone().add_info(node="node2", line="xyz", line_number=1)
        event3 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="node1", line="xyz", line_number=1)
        self.assertTrue(db_events_filter.eval_filter(event1))
        self.assertFalse(db_events_filter.eval_filter(event2))
        self.assertFalse(db_events_filter.eval_filter(event3))

    def test_eval_filter_type_with_line_and_node(self):
        db_events_filter = DbEventsFilter(db_event=DatabaseLogEvent.BAD_ALLOC, node="node1", line="y")
        event1 = DatabaseLogEvent.BAD_ALLOC().add_info(node="node1", line="xyz", line_number=1)
        event2 = event1.clone().add_info(node="node1", line="abc", line_number=1)
        event3 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="node1", line="xyz", line_number=1)
        self.assertTrue(db_events_filter.eval_filter(event1))
        self.assertFalse(db_events_filter.eval_filter(event2))
        self.assertFalse(db_events_filter.eval_filter(event3))

    def test_eval_filter_type_with_regex_line(self):
        regex = re.compile(
            r".*raft_topology - drain rpc failed, proceed to fence "
            r"old writes:.*connection is closed"
        )
        db_events_filter = DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR, line=regex)
        event1 = DatabaseLogEvent.RUNTIME_ERROR().add_info(
            node="node1",
            line="raft_topology - drain rpc failed, proceed to fence old writes: connection is closed",
            line_number=1,
        )
        event2 = event1.clone().add_info(node="node2", line="unrelated log entry", line_number=1)
        event3 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(
            node="node1",
            line="raft_topology - drain rpc failed, proceed to fence old writes: connection is closed",
            line_number=1,
        )
        self.assertTrue(db_events_filter.eval_filter(event1))
        self.assertFalse(db_events_filter.eval_filter(event2))
        self.assertFalse(db_events_filter.eval_filter(event3))


class TestEventsFilter(unittest.TestCase):
    def test_event_class_and_regex_none(self):
        db_events_filter = EventsFilter(event_class=DatabaseLogEvent, regex=None)
        self.assertEqual(db_events_filter.event_class, "DatabaseLogEvent.")
        self.assertIsNone(db_events_filter.regex)

    def test_regex_pattern(self):
        pattern = re.compile("lalala")
        db_events_filter = EventsFilter(regex=pattern)
        self.assertEqual(db_events_filter._regex, pattern)
        self.assertEqual(db_events_filter.regex, pattern.pattern)
        self.assertEqual(db_events_filter, pickle.loads(pickle.dumps(db_events_filter)))
        db_events_filter.to_json()

    def test_regex_string(self):
        db_events_filter = EventsFilter(regex="lalala")
        self.assertEqual(db_events_filter._regex, re.compile("lalala", re.MULTILINE | re.DOTALL))
        self.assertEqual(db_events_filter._regex.pattern, "lalala")
        self.assertEqual(db_events_filter.regex, "lalala")
        self.assertEqual(db_events_filter, pickle.loads(pickle.dumps(db_events_filter)))
        db_events_filter.to_json()

    def test_eval_filter_event_class(self):
        db_events_filter = EventsFilter(event_class=DatabaseLogEvent.BAD_ALLOC)
        self.assertEqual(db_events_filter, pickle.loads(pickle.dumps(db_events_filter)))
        db_events_filter.to_json()
        event1 = DatabaseLogEvent.BAD_ALLOC()
        event2 = DatabaseLogEvent.NO_SPACE_ERROR()
        self.assertTrue(db_events_filter.eval_filter(event1))
        self.assertFalse(db_events_filter.eval_filter(event2))

    def test_eval_filter_event_class_common_parent(self):
        db_events_filter = EventsFilter(event_class=DatabaseLogEvent)
        self.assertEqual(db_events_filter, pickle.loads(pickle.dumps(db_events_filter)))
        db_events_filter.to_json()
        event1 = DatabaseLogEvent.BAD_ALLOC()
        event2 = DatabaseLogEvent.NO_SPACE_ERROR()
        self.assertTrue(db_events_filter.eval_filter(event1))
        self.assertTrue(db_events_filter.eval_filter(event2))

    def test_eval_filter_regex(self):
        db_events_filter = EventsFilter(regex=".*xyz.*")
        event1 = DatabaseLogEvent.BAD_ALLOC().add_info(node="node1", line="xyz", line_number=1)
        event2 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="node1", line="xyz", line_number=1)
        event3 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="node1", line="abc", line_number=1)
        self.assertTrue(db_events_filter.eval_filter(event1))
        self.assertTrue(db_events_filter.eval_filter(event2))
        self.assertFalse(db_events_filter.eval_filter(event3))

    def test_eval_filter_event_class_and_regex(self):
        db_events_filter = EventsFilter(event_class=DatabaseLogEvent.BAD_ALLOC, regex=".*xyz.*")
        event1 = DatabaseLogEvent.BAD_ALLOC().add_info(node="node1", line="xyz", line_number=1)
        event2 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="node1", line="xyz", line_number=1)
        event3 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="node1", line="abc", line_number=1)
        self.assertTrue(db_events_filter.eval_filter(event1))
        self.assertFalse(db_events_filter.eval_filter(event2))
        self.assertFalse(db_events_filter.eval_filter(event3))


class TestEventsSeverityChangerFilter(unittest.TestCase):
    def test_eval_filter(self):
        db_events_filter = EventsSeverityChangerFilter(new_severity=Severity.NORMAL, event_class=DatabaseLogEvent)
        event = DatabaseLogEvent.BAD_ALLOC()
        self.assertEqual(event.severity, Severity.ERROR)
        db_events_filter.eval_filter(event)
        self.assertEqual(event.severity, Severity.NORMAL)


def test_events_severity_changer_filter_gce_first_boot_bind_race():
    """SCT-545/SCT-411: GCE first-boot posix_listen EADDRNOTAVAIL races (e.g. transient
    Prometheus API server bind failures) should be downgraded to WARNING by the filters
    published from `enable_default_filters()` for the "gce" backend."""
    startup_failed_filter = EventsSeverityChangerFilter(
        new_severity=Severity.WARNING,
        event_class=DatabaseLogEvent,
        regex=r".*init - Startup failed:.*Cannot assign requested address",
    )
    prometheus_bind_filter = EventsSeverityChangerFilter(
        new_severity=Severity.WARNING,
        event_class=DatabaseLogEvent,
        regex=r".*init - Could not start Prometheus API server.*Cannot assign requested address",
    )

    prometheus_bind_event = DatabaseLogEvent.DATABASE_ERROR().add_info(
        node="rolling-upgrade-ubuntu-db-node-b3848dee-0-5",
        line_number=1,
        line="2026-06-27T03:17:49.539Z rolling-upgrade-ubuntu-db-node-b3848dee-0-5 !ERR | scylla[1669] "
        "[shard 0:strm] init - Could not start Prometheus API server on 10.128.0.47:9180: "
        "std::system_error (error system:99, posix_listen failed for address 10.128.0.47:9180: "
        "Cannot assign requested address)",
    )
    assert prometheus_bind_event.severity == Severity.ERROR
    # EventsSeverityChangerFilter.eval_filter() always returns False (it never "consumes"/hides
    # the event) — its only effect on a match is the in-place severity rewrite.
    startup_failed_filter.eval_filter(prometheus_bind_event.clone())  # unrelated regex: no-op
    assert prometheus_bind_event.severity == Severity.ERROR
    prometheus_bind_filter.eval_filter(prometheus_bind_event)
    assert prometheus_bind_event.severity == Severity.WARNING

    startup_failed_event = DatabaseLogEvent.DATABASE_ERROR().add_info(
        node="rolling-upgrade-ubuntu-db-node-b3848dee-0-5",
        line_number=2,
        line="2026-06-27T03:17:49.546Z rolling-upgrade-ubuntu-db-node-b3848dee-0-5 !ERR | scylla[1669] "
        "[shard 0:main] init - Startup failed: std::system_error (error system:99, posix_listen "
        "failed for address 10.128.0.47:9180: Cannot assign requested address)",
    )
    assert startup_failed_event.severity == Severity.ERROR
    startup_failed_filter.eval_filter(startup_failed_event)
    assert startup_failed_event.severity == Severity.WARNING

    # a real "Startup failed" for an unrelated reason must not be swallowed
    unrelated_event = DatabaseLogEvent.DATABASE_ERROR().add_info(
        node="node1",
        line_number=3,
        line="!ERR | scylla[1] [shard 0:main] init - Startup failed: std::runtime_error (config file not found)",
    )
    startup_failed_filter.eval_filter(unrelated_event)
    prometheus_bind_filter.eval_filter(unrelated_event)
    assert unrelated_event.severity == Severity.ERROR
