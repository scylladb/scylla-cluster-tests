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
from sdcm.sct_events.base import LogEvent
from sdcm.sct_events.database import (
    DatabaseLogEvent,
    FullScanEvent,
    IndexSpecialColumnErrorEvent,
    TOLERABLE_REACTOR_STALL,
    SYSTEM_ERROR_EVENTS,
)
from sdcm.utils.issues_by_keyword.find_known_issue import FindIssuePerBacktrace


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
    assert issubclass(event_class, DatabaseLogEvent)


def test_reactor_stalled_severity():
    event1 = DatabaseLogEvent.REACTOR_STALLED()
    assert event1.severity == Severity.DEBUG

    assert event1 is event1.add_info(node="n1", line=f"{TOLERABLE_REACTOR_STALL - 1} ms", line_number=1)
    assert event1.severity == Severity.DEBUG
    assert event1.node == "n1"
    assert event1.line == f"{TOLERABLE_REACTOR_STALL - 1} ms"
    assert event1.line_number == 1

    event2 = DatabaseLogEvent.REACTOR_STALLED()
    assert event2.severity == Severity.DEBUG
    assert event2 is event2.add_info(node="n2", line=f"{TOLERABLE_REACTOR_STALL} ms", line_number=2)
    assert event2.severity == Severity.ERROR
    assert event2.node == "n2"
    assert event2.line == f"{TOLERABLE_REACTOR_STALL} ms"
    assert event2.line_number == 2


def test_find_issue_by_reactor_stall(test_data_dir):
    with (test_data_dir / "reactor_stalls_with_known_issue.log").open(encoding="utf-8") as sct_log:
        backtrace = sct_log.readlines()

    find_issue_obj = FindIssuePerBacktrace()
    event = DatabaseLogEvent.REACTOR_STALLED().add_info("node4", "Reactor stalled for 45 ms", 1)
    issue_url = find_issue_obj.find_issue(backtrace_type=event.type, decoded_backtrace="\n".join(backtrace))
    event.known_issue = issue_url
    assert event.known_issue == "https://github.com/scylladb/scylladb/issues/8828"


def test_kernel_callstack_severity():
    event1 = DatabaseLogEvent.KERNEL_CALLSTACK()
    assert event1.severity == Severity.DEBUG

    assert event1 is event1.add_info(node="n1", line="kernel callstack 0xffffffffffffff80", line_number=1)
    assert event1.node == "n1"
    assert event1.line_number == 1

    event2 = DatabaseLogEvent.REACTOR_STALLED()
    assert event2.severity == Severity.DEBUG
    assert event2 is event2.add_info(node="n2", line="kernel callstack 0xffffffffffffff80", line_number=2)
    assert event2.node == "n2"
    assert event2.line_number == 2


def test_system_error_events_list():
    """Make sure all known system error events are listed in SYSTEM_ERROR_EVENTS.

    Since python3.14 also __annotate_func__ need to be excluded from the dir() output.
    """
    assert set(dir(DatabaseLogEvent)) - set(dir(LogEvent)) - {"__annotate_func__"} == {
        ev.type for ev in SYSTEM_ERROR_EVENTS
    }


def test_disk_error_event():
    disk_error_event = DatabaseLogEvent.DISK_ERROR()

    log_lines = """2022-02-07T06:13:14+00:00 longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4 !    INFO |  [shard 8] compaction - [Compact keyspace1.standard1 089530c0-87dd-11ec-8382-519d84e34cb0] Compacting [/var/lib/scylla/data/keyspace1/standard1-b8e41570875811ec8382519d84e34cb0/md-287128-big-Data.db:level=2:origin=compaction,/var/lib/scylla/data/keyspace1/standard1-b8e41570875811ec8382519d84e34cb0/md-287112-big-Data.db:level=2:origin=compaction,/var/lib/scylla/data/keyspace1/standard1-b8e41570875811ec8382519d84e34cb0/md-290136-big-Data.db:level=2:origin=compaction,/var/lib/scylla/data/keyspace1/standard1-b8e41570875811ec8382519d84e34cb0/md-291192-big-Data.db:level=1:origin=compaction]
2022-02-07T06:13:14+00:00 longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4 !     ERR | blk_update_request: critical medium error, dev nvme0n11, sector 4141328 op 0x0:(READ) flags 0x0 phys_seg 1 prio class 0
2022-02-07T06:13:14+00:00 longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4 !     ERR |  [shard 6] storage_service - Shutting down communications due to I/O errors until operator intervention: Disk error: std::system_error (error system:61, No data available)
2022-02-07T06:13:14+00:00 longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4 !    INFO |  [shard 0] storage_service - Stop transport: starts
2022-02-07T06:13:14+00:00 longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4 !    INFO |  [shard 0] storage_service - Shutting down native transport
        """
    expected_error_data = {
        "line_number": 2,
        "line": "2022-02-07T06:13:14+00:00 longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4 !     ERR |  [shard 6] storage_service - Shutting down communications due to I/O errors until operator intervention: Disk error: std::system_error (error system:61, No data available)",
        "node": "longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4",
    }

    for num, line in enumerate(log_lines.splitlines()):
        if re.search(disk_error_event.regex, line):
            disk_error_event.add_info("longevity-tls-1tb-7d-4-6-db-node-5279f155-0-4", line, num)

    assert expected_error_data["node"] == disk_error_event.node
    assert expected_error_data["line_number"] == disk_error_event.line_number
    assert expected_error_data["line"] == disk_error_event.line


def test_too_long_queue_accumulated_event():
    too_long_queue_accumulated_error_event = DatabaseLogEvent.TOO_LONG_QUEUE_ACCUMULATED()

    log_lines = """Nov 25 18:47:00.491639 perf-latency-nemesis-ubuntu-db-node-68c324e8-1 scylla[6012]:  [shard 6:sl:d] seastar - Too long queue accumulated for sl:default (1049 tasks)
                     2: N7seastar8internal21coroutine_traits_baseIvE12promise_typeE
         """
    expected_error_data = {
        "line_number": 0,
        "line": "Nov 25 18:47:00.491639 perf-latency-nemesis-ubuntu-db-node-68c324e8-1 scylla[6012]:  [shard 6:sl:d] seastar - Too long queue accumulated for sl:default (1049 tasks)",
        "node": "perf-latency-nemesis-ubuntu-db-node-68c324e8-1",
    }

    for num, line in enumerate(log_lines.splitlines()):
        if re.search(too_long_queue_accumulated_error_event.regex, line):
            too_long_queue_accumulated_error_event.add_info("perf-latency-nemesis-ubuntu-db-node-68c324e8-1", line, num)

    assert expected_error_data["node"] == too_long_queue_accumulated_error_event.node
    assert expected_error_data["line_number"] == too_long_queue_accumulated_error_event.line_number
    assert expected_error_data["line"] == too_long_queue_accumulated_error_event.line


NODE_NAME = "db-node-1"
KS_CF = "ks_cf"
MSG = "msg"


@pytest.mark.parametrize(
    "message, expected_message, expected_pattern",
    [
        pytest.param(None, None, f"node={NODE_NAME} select_from={KS_CF}", id="without-message"),
        pytest.param(MSG, MSG, f"node={NODE_NAME} select_from={KS_CF} message={MSG}", id="with-message"),
    ],
)
def test_full_scan_event(message, expected_message, expected_pattern):
    kwargs = {"message": message} if message is not None else {}
    event = FullScanEvent(node=NODE_NAME, ks_cf=KS_CF, **kwargs)
    assert event.message == expected_message
    assert re.match(
        r"\(FullScanEvent Severity\.NORMAL\) period_type=not-set event_id=([\d\w-]{36}) "
        f"{expected_pattern}",
        str(event),
    )


def test_index_special_column_error_event_msgfmt():
    event = IndexSpecialColumnErrorEvent(message="m1")
    event.event_id = "ac449879-485a-4b06-8596-3fbe58881093"
    assert str(event) == (
        "(IndexSpecialColumnErrorEvent Severity.ERROR) period_type=one-time "
        "event_id=ac449879-485a-4b06-8596-3fbe58881093: message=m1"
    )
