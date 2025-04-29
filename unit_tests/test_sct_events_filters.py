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
        regex = re.compile(r".*raft_topology - drain rpc failed, proceed to fence "
                           r"old writes:.*connection is closed")
        db_events_filter = DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR, line=regex)
        event1 = DatabaseLogEvent.RUNTIME_ERROR().add_info(
            node="node1",
            line="raft_topology - drain rpc failed, proceed to fence old writes: connection is closed",
            line_number=1
        )
        event2 = event1.clone().add_info(
            node="node2",
            line="unrelated log entry",
            line_number=1
        )
        event3 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(
            node="node1",
            line="raft_topology - drain rpc failed, proceed to fence old writes: connection is closed",
            line_number=1
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
        self.assertEqual(db_events_filter._regex,
                         re.compile("lalala", re.MULTILINE | re.DOTALL))
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
