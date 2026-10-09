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

import pytest

from sdcm.sct_events import Severity
from sdcm.sct_events.base import LogEvent, find_log_event
from sdcm.sct_events.database import (
    DatabaseLogEvent,
    FullScanEvent,
    IndexSpecialColumnErrorEvent,
    TOLERABLE_REACTOR_STALL,
    SYSTEM_ERROR_EVENTS,
    SYSTEM_ERROR_EVENTS_PATTERNS,
)
from sdcm.utils.issues_by_keyword.find_known_issue import FindIssuePerBacktrace

NODE_NAME = "db-node-1"
KS_CF = "ks_cf"
MSG = "msg"
DISK_ERROR_LINE = (
    "2022-02-07T06:13:14+00:00 longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4 !     ERR |  [shard 6] storage_service - "
    "Shutting down communications due to I/O errors until operator intervention: Disk error: std::system_error "
    "(error system:61, No data available)"
)
TOO_LONG_QUEUE_LINE = (
    "Nov 25 18:47:00.491639 perf-latency-nemesis-ubuntu-db-node-68c324e8-1 scylla[6012]:  [shard 6:sl:d] seastar - "
    "Too long queue accumulated for sl:default (1049 tasks)"
)


def classify_system_error_line(line: str):
    """Return the system error event of the first pattern matching `line`, like the DB log reader does."""
    for pattern, event in SYSTEM_ERROR_EVENTS_PATTERNS:
        if pattern.search(line):
            return event
    return None


@pytest.mark.parametrize(
    "event_class",
    [
        pytest.param(DatabaseLogEvent.NO_SPACE_ERROR, id="no-space-error"),
        pytest.param(DatabaseLogEvent.UNKNOWN_VERB, id="unknown-verb"),
        pytest.param(DatabaseLogEvent.CLIENT_DISCONNECT, id="client-disconnect"),
        pytest.param(DatabaseLogEvent.SEMAPHORE_TIME_OUT, id="semaphore-time-out"),
        pytest.param(DatabaseLogEvent.LDAP_CONNECTION_RESET, id="ldap-connection-reset"),
        pytest.param(DatabaseLogEvent.SYSTEM_PAXOS_TIMEOUT, id="system-paxos-timeout"),
        pytest.param(DatabaseLogEvent.SERVICE_LEVEL_CONTROLLER, id="service-level-controller"),
        pytest.param(DatabaseLogEvent.RESTARTED_DUE_TO_TIME_OUT, id="restarted-due-to-time-out"),
        pytest.param(DatabaseLogEvent.EMPTY_NESTED_EXCEPTION, id="empty-nested-exception"),
        pytest.param(DatabaseLogEvent.DATABASE_ERROR, id="database-error"),
        pytest.param(DatabaseLogEvent.BAD_ALLOC, id="bad-alloc"),
        pytest.param(DatabaseLogEvent.SCHEMA_FAILURE, id="schema-failure"),
        pytest.param(DatabaseLogEvent.RUNTIME_ERROR, id="runtime-error"),
        pytest.param(DatabaseLogEvent.FILESYSTEM_ERROR, id="filesystem-error"),
        pytest.param(DatabaseLogEvent.STACKTRACE, id="stacktrace"),
        pytest.param(DatabaseLogEvent.BACKTRACE, id="backtrace"),
        pytest.param(DatabaseLogEvent.ABORTING_ON_SHARD, id="aborting-on-shard"),
        pytest.param(DatabaseLogEvent.SEGMENTATION, id="segmentation"),
        pytest.param(DatabaseLogEvent.INTEGRITY_CHECK, id="integrity-check"),
        pytest.param(DatabaseLogEvent.REACTOR_STALLED, id="reactor-stalled"),
        pytest.param(DatabaseLogEvent.SUPPRESSED_MESSAGES, id="suppressed-messages"),
        pytest.param(DatabaseLogEvent.stream_exception, id="stream-exception"),
        pytest.param(DatabaseLogEvent.DISK_ERROR, id="disk-error"),
        pytest.param(DatabaseLogEvent.TOO_LONG_QUEUE_ACCUMULATED, id="too-long-queue-accumulated"),
    ],
)
def test_known_system_errors(event_class):
    """Each known system error is a DatabaseLogEvent subclass, so log scanning raises it as a database event."""
    assert issubclass(event_class, DatabaseLogEvent)


@pytest.mark.parametrize(
    "line, expected_severity",
    [
        pytest.param(f"{TOLERABLE_REACTOR_STALL - 1} ms", Severity.DEBUG, id="below-tolerable-stays-debug"),
        pytest.param(f"{TOLERABLE_REACTOR_STALL} ms", Severity.ERROR, id="at-tolerable-becomes-error"),
    ],
)
def test_reactor_stalled_add_info_stall_duration_sets_severity(line, expected_severity):
    """add_info() raises a reactor stall to ERROR once it reaches TOLERABLE_REACTOR_STALL, so short stalls stay quiet."""
    event = DatabaseLogEvent.REACTOR_STALLED()
    assert event.severity == Severity.DEBUG

    assert event is event.add_info(node="n1", line=line, line_number=1)
    assert event.severity == expected_severity
    assert event.node == "n1"
    assert event.line == line
    assert event.line_number == 1


def test_find_issue_by_reactor_stall(test_data_dir):
    """A reactor stall backtrace resolves to its known GitHub issue, so reports link the existing bug."""
    with (test_data_dir / "reactor_stalls_with_known_issue.log").open(encoding="utf-8") as sct_log:
        backtrace = sct_log.readlines()

    find_issue_obj = FindIssuePerBacktrace()
    event = DatabaseLogEvent.REACTOR_STALLED().add_info("node4", "Reactor stalled for 45 ms", 1)
    issue_url = find_issue_obj.find_issue(backtrace_type=event.type, decoded_backtrace="\n".join(backtrace))
    event.known_issue = issue_url
    assert event.known_issue == "https://github.com/scylladb/scylladb/issues/8828"


@pytest.mark.parametrize(
    "symbol,expected",
    [
        pytest.param(
            "gms::gossiper::do_send_ack2_msg", "https://scylladb.atlassian.net/browse/SCYLLADB-3114", id="ack2-known"
        ),
        pytest.param(
            "std::vector<seastar::httpd::storage_service_json::snapshot>::push_back",
            "https://scylladb.atlassian.net/browse/SCYLLADB-5320",
            id="snapshot-push-back-known",
        ),
        # the sibling SYN path (jira:SCYLLADB-4852) is a different issue
        pytest.param("gms::gossiper::do_send_ack_msg", None, id="ack-syn-not-linked"),
    ],
)
def test_find_issue_by_oversized_allocation(symbol, expected):
    """An oversized allocation backtrace links to its Jira only for the gossiper ACK2 path it was filed for."""
    backtrace = (
        "seastar::memory::cpu_pages::warn_large_allocation(unsigned long) at ./seastar/src/core/memory.cc:865\n"
        f"(inlined by) {symbol}(utils::tagged_uuid<locator::host_id_tag>, "
        "utils::chunked_vector<gms::gossip_digest, 131072ul>) at ./gms/gossiper.cc:389\n"
        "gms::gossiper::handle_ack_msg(utils::tagged_uuid<locator::host_id_tag>, gms::gossip_digest_ack) "
        "at ./gms/gossiper.cc:334\n"
    )

    assert (
        FindIssuePerBacktrace().find_issue(backtrace_type="OVERSIZED_ALLOCATION", decoded_backtrace=backtrace)
        == expected
    )


@pytest.mark.parametrize(
    "event_class",
    [
        pytest.param(DatabaseLogEvent.KERNEL_CALLSTACK, id="kernel-callstack"),
        pytest.param(DatabaseLogEvent.REACTOR_STALLED, id="reactor-stalled-with-callstack-line"),
    ],
)
def test_add_info_kernel_callstack_line_sets_node_and_line_number(event_class):
    """A kernel callstack line starts at DEBUG and add_info() records its node and line number for the report."""
    event = event_class()
    assert event.severity == Severity.DEBUG

    assert event is event.add_info(node="n1", line="kernel callstack 0xffffffffffffff80", line_number=1)
    assert event.node == "n1"
    assert event.line_number == 1


def test_system_error_events_list():
    """SYSTEM_ERROR_EVENTS lists every DatabaseLogEvent subtype, so log scanning matches all of them.

    Since python3.14 also __annotate_func__ need to be excluded from the dir() output.
    """
    assert set(dir(DatabaseLogEvent)) - set(dir(LogEvent)) - {"__annotate_func__"} == {
        ev.type for ev in SYSTEM_ERROR_EVENTS
    }


@pytest.mark.parametrize(
    "event_class, node, log_lines, expected_line_number, expected_line",
    [
        pytest.param(
            DatabaseLogEvent.DISK_ERROR,
            "longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4",
            """2022-02-07T06:13:14+00:00 longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4 !    INFO |  [shard 8] compaction - [Compact keyspace1.standard1 089530c0-87dd-11ec-8382-519d84e34cb0] Compacting [/var/lib/scylla/data/keyspace1/standard1-b8e41570875811ec8382519d84e34cb0/md-287128-big-Data.db:level=2:origin=compaction,/var/lib/scylla/data/keyspace1/standard1-b8e41570875811ec8382519d84e34cb0/md-287112-big-Data.db:level=2:origin=compaction,/var/lib/scylla/data/keyspace1/standard1-b8e41570875811ec8382519d84e34cb0/md-290136-big-Data.db:level=2:origin=compaction,/var/lib/scylla/data/keyspace1/standard1-b8e41570875811ec8382519d84e34cb0/md-291192-big-Data.db:level=1:origin=compaction]
2022-02-07T06:13:14+00:00 longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4 !     ERR | blk_update_request: critical medium error, dev nvme0n11, sector 4141328 op 0x0:(READ) flags 0x0 phys_seg 1 prio class 0
2022-02-07T06:13:14+00:00 longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4 !     ERR |  [shard 6] storage_service - Shutting down communications due to I/O errors until operator intervention: Disk error: std::system_error (error system:61, No data available)
2022-02-07T06:13:14+00:00 longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4 !    INFO |  [shard 0] storage_service - Stop transport: starts
2022-02-07T06:13:14+00:00 longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4 !    INFO |  [shard 0] storage_service - Shutting down native transport
        """,
            2,
            DISK_ERROR_LINE,
            id="disk-error",
        ),
        pytest.param(
            DatabaseLogEvent.TOO_LONG_QUEUE_ACCUMULATED,
            "perf-latency-nemesis-ubuntu-db-node-68c324e8-1",
            """Nov 25 18:47:00.491639 perf-latency-nemesis-ubuntu-db-node-68c324e8-1 scylla[6012]:  [shard 6:sl:d] seastar - Too long queue accumulated for sl:default (1049 tasks)
                     2: N7seastar8internal21coroutine_traits_baseIvE12promise_typeE
         """,
            0,
            TOO_LONG_QUEUE_LINE,
            id="too-long-queue-accumulated",
        ),
    ],
)
def test_log_event_regex_matches_only_the_error_line(event_class, node, log_lines, expected_line_number, expected_line):
    """The event regex picks the one error line out of surrounding log noise, so the event points at the real failure."""
    event = event_class()

    for num, line in enumerate(log_lines.splitlines()):
        if re.search(event.regex, line):
            event.add_info(node, line, num)

    assert event.node == node
    assert event.line_number == expected_line_number
    assert event.line == expected_line


def test_drop_table_during_repair_gate_closed_line_matches_database_error():
    """The benign SSTable-load gate-closed line is classified as DATABASE_ERROR.

    Mirrors the first-match pattern-matching loop in sdcm/db_log_reader.py, which walks
    SYSTEM_ERROR_EVENTS_PATTERNS in order and stops at the first regex that matches.
    """
    line = (
        "2026-06-17T02:54:28.468+00:00 long-100gb-4h-oci-master-db-node-485a782c-us-ashburn-1-4 "
        "!     ERR | scylla[7599]:  [shard 5:strm] table - Failed to load SSTable "
        "/var/lib/scylla/data/drop_table_during_repair_ks_1/"
        "standard1-982af76069f611f1b3409d1699bc3637/mt-3h1c_082m_3sbeo2uuqiz7h5z0ox-big-Data.db "
        "of origin memtable due to seastar::named_gate_closed_exception (named gate closed), "
        "it will be unlinked..."
    )

    matched_event = find_log_event(SYSTEM_ERROR_EVENTS_PATTERNS, line)

    assert matched_event is not None
    assert matched_event.type == DatabaseLogEvent.DATABASE_ERROR.type


def test_system_error_events_partitioned_by_log_level():
    """INFO must sit after every WARNING/NORMAL/DEBUG subevent and before every ERROR/CRITICAL one.

    That ordering is what guarantees an INFO-level Scylla line can never turn into an ERROR event while
    keeping the lower-severity classifications (GATE_CLOSED, COMPACTION_STOPPED, TABLET_*, ...) intact.
    OVERSIZED_ALLOCATION/WARNING are the pre-existing WARN-level pair and are exempt.
    """
    types = [event.type for event in SYSTEM_ERROR_EVENTS]
    info_idx = types.index("INFO")
    before = [e for e in SYSTEM_ERROR_EVENTS[:info_idx] if e.type not in ("OVERSIZED_ALLOCATION", "WARNING")]
    after = SYSTEM_ERROR_EVENTS[info_idx + 1 :]

    error = Severity.ERROR.value
    assert all(e.severity.value < error for e in before), [e.type for e in before if e.severity.value >= error]
    assert all(e.severity.value >= error for e in after), [e.type for e in after if e.severity.value < error]


def test_info_level_runtime_error_line_is_suppressed():
    """An INFO-level line quoting a runtime_error is classified as the suppressed INFO event (SCT-772)."""
    line = (
        "2026-07-30T11:37:43.815 longevity-10gb-3h-master-db-node-41da73ad-eastus-1  !INFO | scylla[12796]  "
        "[shard 0: gms] rpc - client 10.0.0.9:7000: ignoring error response: seastar::rpc::remote_verb_error "
        "(std::runtime_error (Got stream_blob_cmd::error from peer 7336616b-4533-4c59-a3d1-f6d825de4d8a))"
    )
    event = classify_system_error_line(line)
    assert event.type == DatabaseLogEvent.INFO.type
    assert event.severity == Severity.SUPPRESS


@pytest.mark.parametrize(
    "line,expected",
    [
        pytest.param(
            "2026-07-30T11:37:43.815 db-node-1 !ERR | scylla[12796]  [shard 0: gms] raft_topology - "
            "topology change coordinator fiber got error std::runtime_error (connection is closed)",
            DatabaseLogEvent.RUNTIME_ERROR,
            id="error_level_runtime_error",
        ),
        pytest.param(
            "2021-11-26T05:05:08+00:00 db-node-1 !    INFO |  [shard 0] rpc - client 10.0.2.7:62608 msg_id 2:  "
            'exception "gate closed" in no_wait handler ignored',
            DatabaseLogEvent.GATE_CLOSED,
            id="gate_closed",
        ),
        pytest.param(
            "2022-04-17T04:44:07+00:00 db-node-1 !    INFO |  [shard 10] compaction - [Compact keyspace1.standard1 ff8f7410] "
            "Compacting of 2 sstables interrupted due to: sstables::compaction_stopped_exception (user-triggered operation)",
            DatabaseLogEvent.COMPACTION_STOPPED,
            id="compaction_stopped",
        ),
        pytest.param(
            "2026-07-30T11:37:43.815 db-node-1 !INFO | scylla[1] [shard 0: main] tablets - Detected tablet split for table ks.cf",
            DatabaseLogEvent.TABLET_SPLIT,
            id="tablet_split",
        ),
    ],
)
def test_system_error_line_keeps_classification_outside_info_suppression(line, expected):
    """ERROR-level lines and INFO lines with a lower-severity event keep their own classification, not INFO."""
    assert classify_system_error_line(line).type == expected.type


@pytest.mark.parametrize(
    "message, expected_message, expected_pattern",
    [
        pytest.param(None, None, f"node={NODE_NAME} select_from={KS_CF}", id="without-message"),
        pytest.param(MSG, MSG, f"node={NODE_NAME} select_from={KS_CF} message={MSG}", id="with-message"),
    ],
)
def test_full_scan_event(message, expected_message, expected_pattern):
    """FullScanEvent renders node and table, and the message only when one is given."""
    kwargs = {"message": message} if message is not None else {}
    event = FullScanEvent(node=NODE_NAME, ks_cf=KS_CF, **kwargs)
    assert event.message == expected_message
    assert re.match(
        r"\(FullScanEvent Severity\.NORMAL\) period_type=not-set event_id=([\d\w-]{36}) "
        f"{expected_pattern}",
        str(event),
    )


def test_index_special_column_error_event_msgfmt():
    """IndexSpecialColumnErrorEvent renders as a one-time ERROR carrying its message."""
    event = IndexSpecialColumnErrorEvent(message="m1")
    event.event_id = "ac449879-485a-4b06-8596-3fbe58881093"
    assert str(event) == (
        "(IndexSpecialColumnErrorEvent Severity.ERROR) period_type=one-time "
        "event_id=ac449879-485a-4b06-8596-3fbe58881093: message=m1"
    )


def test_log_event_message_carries_build_id_with_raw_backtrace():
    """An undecoded backtrace is only useful later together with the build id it belongs to."""
    event = DatabaseLogEvent.REACTOR_STALLED().add_info("node1", "Reactor stalled for 45 ms", 1)
    event.raw_backtrace = "0x1234\n0x5678"
    event.build_id = "abc123"

    message = str(event)

    assert "build_id=abc123" in message
    assert "0x1234\n0x5678" in message
