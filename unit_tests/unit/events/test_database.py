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
from sdcm.sct_events.base import find_log_event
from sdcm.sct_events.database import (
    SYSTEM_ERROR_EVENTS,
    SYSTEM_ERROR_EVENTS_PATTERNS,
    TOLERABLE_REACTOR_STALL,
    TOLERABLE_TOPOLOGY_BARRIER_STALL,
    DatabaseLogEvent,
    FullScanEvent,
    IndexSpecialColumnErrorEvent,
    get_system_error_events_patterns,
)

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


@pytest.mark.parametrize(
    "held_for,expected_severity",
    [
        pytest.param(f"{TOLERABLE_TOPOLOGY_BARRIER_STALL - 0.001}", Severity.WARNING, id="just_below_threshold"),
        pytest.param(f"{TOLERABLE_TOPOLOGY_BARRIER_STALL}", Severity.ERROR, id="integer_at_threshold"),
        pytest.param(f"{TOLERABLE_TOPOLOGY_BARRIER_STALL}.000", Severity.ERROR, id="fraction_at_threshold"),
    ],
)
def test_topology_barrier_stall_hold_time_sets_severity(held_for, expected_severity):
    """Stalls at or above the tolerable hold time must be ERROR; shorter ones stay WARNING."""
    line = f"token_metadata - topology version 1372 held for {held_for} [s] past expiry, released at: 0x194b7ff"

    event = DatabaseLogEvent.TOPOLOGY_BARRIER_STALL().add_info(node="n1", line=line, line_number=1)

    assert event.severity == expected_severity
    assert event.node == "n1"
    assert event.line_number == 1


def test_topology_barrier_stall_unparseable_hold_time_keeps_default_severity():
    """A line without a readable hold time must still publish, with the default WARNING severity."""
    event = DatabaseLogEvent.TOPOLOGY_BARRIER_STALL().add_info(
        node="n1", line="token_metadata - topology version 1372 held for", line_number=1
    )

    assert event.severity == Severity.WARNING


@pytest.mark.parametrize(
    "enabled,expected_type,expected_severity",
    [
        pytest.param(True, "TOPOLOGY_BARRIER_STALL", Severity.WARNING, id="enabled_publishes_stall_event"),
        pytest.param(False, "WARNING", Severity.SUPPRESS, id="disabled_falls_back_to_suppressed_warning"),
    ],
)
def test_topology_barrier_stall_line_matching_follows_enabled_flag(enabled, expected_type, expected_severity):
    """A stall line is matched as a stall event only when enabled; otherwise it is suppressed as a warning."""
    line = (
        "2026-05-12T12:15:59.522+00:00 node-6 !WARNING | scylla[6531]: [shard 13:sl:d] "
        "token_metadata - topology version 1372 held for 4.567 [s] past expiry, released at: 0x194b7ff"
    )

    matched = next(event for pattern, event in get_system_error_events_patterns(enabled) if pattern.search(line))

    assert matched.type == expected_type
    assert matched.severity == expected_severity


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


def test_raft_transfer_snapshot_not_implemented_line_is_suppressed():
    """Real example (SCYLLADB-2565) that flooded a run with RUNTIME_ERROR/ERROR events."""
    line = (
        "2026-10-26T09:59:48.262 longevity-sc-verify-100gb-4h-sc-ver-db-node-affc9575-2  !ERR | scylla[9182]  "
        "[shard 3: gms] raft - [sc-01be54c2-d122-11f1-932e-9f0ffe1c6b6f] Transferring snapshot to "
        "db9778e7-b231-44bb-ab2e-83a355dd523a failed with: std::runtime_error (transfer_snapshot() not implemented)"
    )
    event = classify_system_error_line(line)
    assert event.type == DatabaseLogEvent.RAFT_TRANSFER_SNAPSHOT_NOT_IMPLEMENTED.type
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
