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

import upgrade_test
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


def _make_symbolized_oversized_allocation_event(node: str, symbol: str) -> DatabaseLogEvent:
    """Build an event like the real one: the raw log line has only addresses, the decoder fills `backtrace`."""
    event = DatabaseLogEvent.OVERSIZED_ALLOCATION().add_info(
        node=node,
        line_number=1,
        line=f"2026-06-27T03:17:49.539Z {node} !WARNING | scylla[1669] [shard 0:strm] seastar_memory - "
        "oversized allocation: 1048576 bytes. This is non-fatal, but could lead to latency and/or fragmentation "
        "issues. Please report: at 0x5f1a2b3 0x5f1a7c4 0x5f1ad05 0x5f1b296",
    )
    event.backtrace = (
        "void seastar::backtrace<seastar::current_backtrace_tasklocal()::$_0>(...)\n"
        f"{symbol}(seastar::rpc::wait_type, std::chrono::time_point<seastar::lowres_clock>)"
    )
    return event


def test_oversized_whole_run_filter(events_function_scope):  # noqa: ARG001
    """SCT-1048: the decoder writes the symbol into `event.backtrace` and leaves `event.line`
    as the raw log line, so a filter on `event.line` never matches the symbol."""
    published_filters = []
    upgrade_test_stub = unittest.mock.MagicMock()
    upgrade_test_stub.params.scylla_version_upgrade_target = "2026.2.0"

    with unittest.mock.patch.object(EventsFilter, "publish", autospec=True, side_effect=published_filters.append):
        upgrade_test.UpgradeTest.filter_oversized_allocation_for_whole_run(upgrade_test_stub)

    assert len(published_filters) == 1
    events_filter = published_filters[0]
    # `publish` is mocked, so the filter is still flagged ready-to-publish: without this its
    # `__del__` logs "[SCT internal warning] ... has not been published" into whichever test
    # happens to garbage-collect it on the same worker
    events_filter.dont_publish()

    matching_event = _make_symbolized_oversized_allocation_event("node1", "seastar::rpc::client::wait_for_reply")
    other_symbol_event = _make_symbolized_oversized_allocation_event("node1", "seastar::rpc::client::send")
    # `event_class` matches every `DatabaseLogEvent` subtype: only the regex tells the types apart,
    # even when another event quotes the type name in its own text.
    other_type_event = DatabaseLogEvent.BAD_ALLOC().add_info(
        node="node1", line_number=1, line="bad_alloc, see type=OVERSIZED_ALLOCATION"
    )
    other_type_event.backtrace = matching_event.backtrace

    assert events_filter.eval_filter(matching_event)
    assert not events_filter.eval_filter(other_symbol_event)
    assert not events_filter.eval_filter(other_type_event)


def test_upgrade_per_node_oversized_allocation_filter_matches_only_its_node(events_function_scope):  # noqa: ARG001
    upgrade_test_stub = unittest.mock.MagicMock()
    node = unittest.mock.MagicMock()
    node.name = "node-1"

    with unittest.mock.patch.object(EventsFilter, "publish", autospec=True):
        upgrade_test.UpgradeTest.configure_event_filtering(upgrade_test_stub, node)

    (events_filter,) = upgrade_test_stub.stacks[node].enter_context.call_args.args
    # `publish` is mocked here too - see test_oversized_whole_run_filter
    events_filter.dont_publish()

    symbol = "seastar::rpc::client::wait_for_reply"
    assert events_filter.eval_filter(_make_symbolized_oversized_allocation_event("node-1", symbol))
    assert events_filter.eval_filter(
        _make_symbolized_oversized_allocation_event("Node node-1 [10.0.0.1 | 10.0.0.1]", symbol)
    )
    assert not events_filter.eval_filter(_make_symbolized_oversized_allocation_event("node-10", symbol))
    assert not events_filter.eval_filter(_make_symbolized_oversized_allocation_event("node-2", symbol))
    assert not events_filter.eval_filter(_make_symbolized_oversized_allocation_event("node-1-extra", symbol))
    assert not events_filter.eval_filter(
        _make_symbolized_oversized_allocation_event("node-1", "seastar::rpc::client::send")
    )


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
