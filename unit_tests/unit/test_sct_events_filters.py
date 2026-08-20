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
import unittest.mock

from sdcm.sct_events import Severity
from sdcm.sct_events.filters import DbEventsFilter, EventsFilter, EventsSeverityChangerFilter
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.group_common_events import ignore_drop_table_during_repair_errors


def test_db_events_filter_just_type():
    db_events_filter = DbEventsFilter(db_event=DatabaseLogEvent.REACTOR_STALLED)
    assert db_events_filter == pickle.loads(pickle.dumps(db_events_filter))
    db_events_filter.to_json()
    event1 = DatabaseLogEvent.REACTOR_STALLED()
    event2 = DatabaseLogEvent.NO_SPACE_ERROR()
    assert db_events_filter.eval_filter(event1)
    assert not db_events_filter.eval_filter(event2)


def test_db_events_filter_type_with_line():
    db_events_filter = DbEventsFilter(db_event=DatabaseLogEvent.BAD_ALLOC, line="y")
    event1 = DatabaseLogEvent.BAD_ALLOC().add_info(node="node1", line="xyz", line_number=1)
    event2 = event1.clone().add_info(node="node2", line="abc", line_number=1)
    event3 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="node1", line="xyz", line_number=1)
    assert db_events_filter.eval_filter(event1)
    assert not db_events_filter.eval_filter(event2)
    assert not db_events_filter.eval_filter(event3)


def test_db_events_filter_type_with_node():
    db_events_filter = DbEventsFilter(db_event=DatabaseLogEvent.BAD_ALLOC, node="node1")
    event1 = DatabaseLogEvent.BAD_ALLOC().add_info(node="node1", line="xyz", line_number=1)
    event2 = event1.clone().add_info(node="node2", line="xyz", line_number=1)
    event3 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="node1", line="xyz", line_number=1)
    assert db_events_filter.eval_filter(event1)
    assert not db_events_filter.eval_filter(event2)
    assert not db_events_filter.eval_filter(event3)


def test_db_events_filter_type_with_line_and_node():
    db_events_filter = DbEventsFilter(db_event=DatabaseLogEvent.BAD_ALLOC, node="node1", line="y")
    event1 = DatabaseLogEvent.BAD_ALLOC().add_info(node="node1", line="xyz", line_number=1)
    event2 = event1.clone().add_info(node="node1", line="abc", line_number=1)
    event3 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="node1", line="xyz", line_number=1)
    assert db_events_filter.eval_filter(event1)
    assert not db_events_filter.eval_filter(event2)
    assert not db_events_filter.eval_filter(event3)


def test_db_events_filter_type_with_regex_line():
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
    assert db_events_filter.eval_filter(event1)
    assert not db_events_filter.eval_filter(event2)
    assert not db_events_filter.eval_filter(event3)


def test_events_filter_event_class_and_regex_none():
    db_events_filter = EventsFilter(event_class=DatabaseLogEvent, regex=None)
    assert db_events_filter.event_class == "DatabaseLogEvent."
    assert db_events_filter.regex is None


def test_events_filter_regex_pattern():
    pattern = re.compile("lalala")
    db_events_filter = EventsFilter(regex=pattern)
    assert db_events_filter._regex == pattern
    assert db_events_filter.regex == pattern.pattern
    assert db_events_filter == pickle.loads(pickle.dumps(db_events_filter))
    db_events_filter.to_json()


def test_events_filter_regex_string():
    db_events_filter = EventsFilter(regex="lalala")
    assert db_events_filter._regex == re.compile("lalala", re.MULTILINE | re.DOTALL)
    assert db_events_filter._regex.pattern == "lalala"
    assert db_events_filter.regex == "lalala"
    assert db_events_filter == pickle.loads(pickle.dumps(db_events_filter))
    db_events_filter.to_json()


def test_events_filter_eval_filter_event_class():
    db_events_filter = EventsFilter(event_class=DatabaseLogEvent.BAD_ALLOC)
    assert db_events_filter == pickle.loads(pickle.dumps(db_events_filter))
    db_events_filter.to_json()
    event1 = DatabaseLogEvent.BAD_ALLOC()
    event2 = DatabaseLogEvent.NO_SPACE_ERROR()
    assert db_events_filter.eval_filter(event1)
    assert not db_events_filter.eval_filter(event2)


def test_events_filter_eval_filter_event_class_common_parent():
    db_events_filter = EventsFilter(event_class=DatabaseLogEvent)
    assert db_events_filter == pickle.loads(pickle.dumps(db_events_filter))
    db_events_filter.to_json()
    event1 = DatabaseLogEvent.BAD_ALLOC()
    event2 = DatabaseLogEvent.NO_SPACE_ERROR()
    assert db_events_filter.eval_filter(event1)
    assert db_events_filter.eval_filter(event2)


def test_events_filter_eval_filter_regex():
    db_events_filter = EventsFilter(regex=".*xyz.*")
    event1 = DatabaseLogEvent.BAD_ALLOC().add_info(node="node1", line="xyz", line_number=1)
    event2 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="node1", line="xyz", line_number=1)
    event3 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="node1", line="abc", line_number=1)
    assert db_events_filter.eval_filter(event1)
    assert db_events_filter.eval_filter(event2)
    assert not db_events_filter.eval_filter(event3)


def test_events_filter_eval_filter_event_class_and_regex():
    db_events_filter = EventsFilter(event_class=DatabaseLogEvent.BAD_ALLOC, regex=".*xyz.*")
    event1 = DatabaseLogEvent.BAD_ALLOC().add_info(node="node1", line="xyz", line_number=1)
    event2 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="node1", line="xyz", line_number=1)
    event3 = DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="node1", line="abc", line_number=1)
    assert db_events_filter.eval_filter(event1)
    assert not db_events_filter.eval_filter(event2)
    assert not db_events_filter.eval_filter(event3)


def test_events_severity_changer_filter():
    db_events_filter = EventsSeverityChangerFilter(new_severity=Severity.NORMAL, event_class=DatabaseLogEvent)
    event = DatabaseLogEvent.BAD_ALLOC()
    assert event.severity == Severity.ERROR
    db_events_filter.eval_filter(event)
    assert event.severity == Severity.NORMAL


def test_ignore_drop_table_during_repair_errors(events_function_scope):  # noqa: ARG001
    # Capture the real DbEventsFilter instance the context manager builds, so the test
    # exercises the actual filter object rather than reimplementing its regex.
    created_filters = []
    original_init = DbEventsFilter.__init__

    def spy_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        created_filters.append(self)

    with unittest.mock.patch.object(DbEventsFilter, "__init__", spy_init):
        with ignore_drop_table_during_repair_errors():
            pass

    assert len(created_filters) == 1
    db_events_filter = created_filters[0]

    matching_line = (
        "table - Failed to load SSTable /var/lib/scylla/data/drop_table_during_repair_ks_1/"
        "standard1-982af76069f611f1b3409d1699bc3637/mt-3h1c_082m_3sbeo2uuqiz7h5z0ox-big-Data.db "
        "of origin memtable due to seastar::named_gate_closed_exception (named gate closed), "
        "it will be unlinked"
    )
    matching_event = DatabaseLogEvent.DATABASE_ERROR().add_info(node="node1", line=matching_line, line_number=1)
    other_database_error_event = DatabaseLogEvent.DATABASE_ERROR().add_info(
        node="node1", line="some unrelated database error", line_number=2
    )
    other_event_class_with_same_line = DatabaseLogEvent.RUNTIME_ERROR().add_info(
        node="node1", line=matching_line, line_number=3
    )

    assert db_events_filter.eval_filter(matching_event)
    assert not db_events_filter.eval_filter(other_database_error_event)
    assert not db_events_filter.eval_filter(other_event_class_with_same_line)
