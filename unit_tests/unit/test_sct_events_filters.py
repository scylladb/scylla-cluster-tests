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

import pytest

import upgrade_test
from sdcm.sct_events import Severity
from sdcm.sct_events.filters import DbEventsFilter, EventsFilter, EventsSeverityChangerFilter
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.group_common_events import (
    ignore_drop_table_during_repair_errors,
    oversized_allocation_filter,
)


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


# Decoded backtrace of a real SCYLLADB-3114 event (rolling-upgrade-oci-image-test #5), trimmed: the symbol the
# filters look for sits many lines below the first line of the event.
ACK2_OVERSIZED_ALLOCATION_BACKTRACE = """\
[Backtrace #0]
void seastar::backtrace<seastar::current_backtrace_tasklocal()::$_0>(seastar::current_backtrace_tasklocal()::$_0&&, bool) at ./build/release/seastar/./seastar/include/seastar/util/backtrace.hh:85
seastar::memory::cpu_pages::warn_large_allocation(unsigned long) at ./build/release/seastar/./seastar/src/core/memory.cc:865
(inlined by) __libc_malloc at ./build/release/seastar/./seastar/src/core/memory.cc:2282
?? at ??:0
fmt::v11::detail::vformat_to(fmt::v11::detail::buffer<char>&, fmt::v11::basic_string_view<char>, fmt::v11::basic_format_args<fmt::v11::context>, fmt::v11::detail::locale_ref) at ??:0
(inlined by) fmt::v11::formatter<gms::endpoint_state, char, void>::format(gms::endpoint_state const&, fmt::v11::context&) const at ./gms/endpoint_state.cc:110
(inlined by) fmt::v11::formatter<gms::gossip_digest_ack2, char, void>::format(gms::gossip_digest_ack2 const&, fmt::v11::context&) const at ./gms/gossip_digest_ack2.cc:18
std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char>> fmt::v11::format<gms::gossip_digest_ack2&>(fmt::v11::fstring<gms::gossip_digest_ack2&>::t, gms::gossip_digest_ack2&) at /usr/include/fmt/format.h:4200
(inlined by) gms::gossiper::do_send_ack2_msg(utils::tagged_uuid<locator::host_id_tag>, utils::chunked_vector<gms::gossip_digest, 131072ul>) at ./gms/gossiper.cc:389
gms::gossiper::handle_ack_msg(utils::tagged_uuid<locator::host_id_tag>, gms::gossip_digest_ack) at ./gms/gossiper.cc:334
"""


def make_oversized_allocation_event(node: str, backtrace: str) -> DatabaseLogEvent:
    """Build an event like the real one: the raw log line has only addresses, the decoder fills `backtrace`."""
    event = DatabaseLogEvent.OVERSIZED_ALLOCATION().add_info(
        node=node,
        line_number=1,
        line=f"2026-06-27T03:17:49.539Z {node} !WARNING | scylla[1669] [shard 0:strm] seastar_memory - "
        "oversized allocation: 1048576 bytes. This is non-fatal, but could lead to latency and/or fragmentation "
        "issues. Please report: at 0x5f1a2b3 0x5f1a7c4 0x5f1ad05 0x5f1b296",
    )
    event.backtrace = backtrace
    return event


def make_rpc_oversized_allocation_event(node: str, symbol: str = "seastar::rpc::client::wait_for_reply"):
    return make_oversized_allocation_event(
        node,
        "void seastar::backtrace<seastar::current_backtrace_tasklocal()::$_0>(...)\n"
        f"{symbol}(seastar::rpc::wait_type, std::chrono::time_point<seastar::lowres_clock>)",
    )


def is_downgraded(events_filter: EventsSeverityChangerFilter, event: DatabaseLogEvent) -> bool:
    """A severity changer never drops the event, it only lowers its severity when it matches."""
    assert event.severity == Severity.ERROR
    assert not events_filter.eval_filter(event)
    return event.severity == Severity.WARNING


def published_filters(publishing_call) -> list:
    published = []
    with unittest.mock.patch.object(EventsFilter, "publish", autospec=True, side_effect=published.append):
        publishing_call()
    # `publish` is mocked, so the filters are still flagged ready-to-publish: without this their
    # `__del__` logs "[SCT internal warning] ... has not been published" into whichever test
    # happens to garbage-collect them on the same worker
    for events_filter in published:
        events_filter.dont_publish()
    return published


def make_other_type_event() -> DatabaseLogEvent:
    # `event_class` matches every `DatabaseLogEvent` subtype: only the regex tells the types apart,
    # even when another event quotes the type name in its own text.
    event = DatabaseLogEvent.BAD_ALLOC().add_info(
        node="node-2", line_number=1, line="bad_alloc, see type=OVERSIZED_ALLOCATION"
    )
    event.backtrace = ACK2_OVERSIZED_ALLOCATION_BACKTRACE
    return event


@pytest.mark.parametrize(
    "symbol,make_event,expected",
    [
        pytest.param(
            "gms::gossiper::do_send_ack2_msg",
            lambda: make_oversized_allocation_event("node-2", ACK2_OVERSIZED_ALLOCATION_BACKTRACE),
            True,
            id="symbol-deep-in-backtrace",
        ),
        pytest.param(
            "gms::gossiper::do_send_ack_msg",
            lambda: make_oversized_allocation_event("node-2", ACK2_OVERSIZED_ALLOCATION_BACKTRACE),
            False,
            id="sibling-symbol",
        ),
        pytest.param("gms::gossiper::do_send_ack2_msg", make_other_type_event, False, id="other-event-type"),
    ],
)
def test_oversized_allocation_filter_matches_symbol_in_multiline_backtrace(symbol, make_event, expected):
    events_filter = oversized_allocation_filter(symbol)
    events_filter.dont_publish()

    assert is_downgraded(events_filter, make_event()) is expected


@pytest.mark.parametrize(
    "node,symbol,expected",
    [
        pytest.param("node-1", "seastar::rpc::client::wait_for_reply", True, id="same-node"),
        pytest.param("Node node-1 [10.0.0.1 | 10.0.0.1]", "seastar::rpc::client::wait_for_reply", True, id="node-repr"),
        pytest.param("node-10", "seastar::rpc::client::wait_for_reply", False, id="node-name-prefix"),
        pytest.param("node-2", "seastar::rpc::client::wait_for_reply", False, id="other-node"),
        pytest.param("node-1-extra", "seastar::rpc::client::wait_for_reply", False, id="node-name-suffix"),
        pytest.param("node-1", "seastar::rpc::client::send", False, id="other-symbol"),
    ],
)
def test_oversized_allocation_filter_for_one_node(node, symbol, expected):
    events_filter = oversized_allocation_filter("seastar::rpc::client::wait_for_reply", node_name="node-1")
    events_filter.dont_publish()

    assert is_downgraded(events_filter, make_rpc_oversized_allocation_event(node, symbol)) is expected


def test_upgrade_oversized_rpc_whole_run_filter(events_function_scope):  # noqa: ARG001
    """SCT-1048: the decoder writes the symbol into `event.backtrace` and leaves `event.line`
    as the raw log line, so a filter on `event.line` never matches the symbol."""
    upgrade_test_stub = unittest.mock.MagicMock()
    upgrade_test_stub.params.scylla_version_upgrade_target = "2026.2.0"

    (events_filter,) = published_filters(
        lambda: upgrade_test.UpgradeTest.filter_oversized_allocation_for_whole_run(upgrade_test_stub)
    )

    assert is_downgraded(events_filter, make_rpc_oversized_allocation_event("node1"))
    assert not is_downgraded(events_filter, make_rpc_oversized_allocation_event("node1", "seastar::rpc::client::send"))


def test_upgrade_oversized_ack2_filter_downgrades_the_real_event(events_function_scope):  # noqa: ARG001
    """SCYLLADB-3114: downgraded for the whole run, whatever the upgrade target is."""
    (events_filter,) = published_filters(
        lambda: upgrade_test.UpgradeTest.filter_oversized_ack2_allocation(unittest.mock.MagicMock())
    )

    assert is_downgraded(events_filter, make_oversized_allocation_event("node-2", ACK2_OVERSIZED_ALLOCATION_BACKTRACE))


def test_upgrade_per_node_oversized_rpc_filter_matches_only_its_node(events_function_scope):  # noqa: ARG001
    upgrade_test_stub = unittest.mock.MagicMock()
    node = unittest.mock.MagicMock()
    node.name = "node-1"

    with unittest.mock.patch.object(EventsFilter, "publish", autospec=True):
        upgrade_test.UpgradeTest.configure_event_filtering(upgrade_test_stub, node)
    (events_filter,) = upgrade_test_stub.stacks[node].enter_context.call_args.args
    # `publish` is mocked here too - see published_filters
    events_filter.dont_publish()

    assert is_downgraded(events_filter, make_rpc_oversized_allocation_event("node-1"))
    assert not is_downgraded(events_filter, make_rpc_oversized_allocation_event("node-1-2"))
