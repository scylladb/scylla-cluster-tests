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

import time
from pathlib import Path
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone

import pytest

from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.sct_events.base import EventPeriod
from sdcm.sct_events.filters import DbEventsFilter, EventsFilter, EventsSeverityChangerFilter
from sdcm.sct_events.loaders import YcsbStressEvent
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.event_counter import EventCounterContextManager

from unit_tests.lib.real_events import RealEventsTest

YCSB_ERROR_LINE = (
    "237951 [Thread-47] ERROR site.ycsb.db.DynamoDBClient"
    " -com.amazonaws.AmazonServiceException: Internal server error:"
    " exceptions::unavailable_exception (Cannot achieve consistency"
    " level for cl LOCAL_ONE. Requires 1, alive 0) (Service: AmazonDynamoDBv2;"
    " Status Code: 500; Error Code: Internal Server Error; Request ID: null)"
)
YCSB_NODE = "Node alternator-3h-silence--loader-node-bb90aa05-2 [34.251.153.122 | 10.0.220.55] (seed: False)"
ENOSPC_LINE_1 = (
    "[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  [shard 8] commitlog - Exception in segment "
    "reservation: storage_io_error (Storage I/O error: 28: No space left on device)"
)
ENOSPC_LINE_2 = (
    "2019-10-29T12:19:49+00:00  ip-172-30-0-184 !WARNING | scylla: [shard 2] storage_service - Commitlog "
    "error: std::filesystem::__cxx11::filesystem_error (error system:28, filesystem error: open failed: No "
    "space left on device [/var/lib/scylla/hints/2/172.30.0.116/HintsLog-1-36028797019122576.log])"
)
REPAIR_ABORTED_LINE = (
    "2019-07-28T10:53:29+00:00  ip-10-0-167-91 !INFO    | scylla.bin: [shard 0] repair - "
    "Got error in row level repair: std::runtime_error (repair id 1 is aborted on shard 0)"
)
SCHEMA_LOAD_FAILED_LINE = (
    "!ERR     | scylla:  [shard 3] storage_proxy - Exception when communicating with "
    "10.142.0.56: std::runtime_error (Failed to load schema version "
    "b40e405f-462c-38f2-a90c-6f130ddbf6f3) "
)
OTHER_BACKTRACE_LINE = "other back trace that shouldn't be filtered"


def publish_ycsb_load(error_line):
    """Mimic what YcsbStressThread publishes: begin/end around per-error-line events sharing the event_id."""
    ycsb_event = YcsbStressEvent(node=YCSB_NODE, stress_cmd="ycsb")
    ycsb_event.begin_event()
    error_event = YcsbStressEvent(node=YCSB_NODE, stress_cmd="ycsb", severity=Severity.ERROR, errors=[error_line])
    error_event.event_id = ycsb_event.event_id
    error_event.period_type = EventPeriod.INFORMATIONAL.value
    error_event.publish()
    ycsb_event.end_event()


def make_critical_framework_event(text):
    """Build a CRITICAL TestFrameworkEvent whose source is text."""
    return TestFrameworkEvent(source=text, source_method="", severity=Severity.CRITICAL)


def make_no_space_error_event(text):
    """Build an ERROR NO_SPACE_ERROR db log event whose line is text."""
    return DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="A", line_number=22, line=text)


def use_as_context_manager(counter):
    """Count events by using the counter itself as the context manager."""
    return counter


@contextmanager
def use_start_stop_methods(counter):
    """Count events by calling the counter's start/stop methods around the block."""
    counter.start_event_counter()
    try:
        yield counter
    finally:
        counter.stop_event_counter()


def assert_counter_stat_files(real_events, count_condition_name, expected_nodes):
    """Assert the reactor-stall stat dir of a counter holds one single-line file per node from expected_nodes."""
    reactor_stall_dir = (
        real_events.get_events_counter().events_stat_dir / count_condition_name / "DatabaseLogEvent.REACTOR_STALLED"
    )
    assert reactor_stall_dir.is_dir()
    for file_event in reactor_stall_dir.iterdir():
        assert file_event.name in expected_nodes
        lines = file_event.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1, f"Wrong number of lines in {file_event}: {lines}"


@pytest.fixture(scope="module")
def real_events():
    """Run the real events pipeline (device, logger, counter) shared by all tests in this module."""
    events = RealEventsTest()
    RealEventsTest.setup_class()
    try:
        yield events
    finally:
        RealEventsTest.teardown_class()


@pytest.fixture
def clean_events_log(real_events):
    """Truncate events.log before the test runs.

    ``real_events`` is module-scoped (starting the real event pipeline costs
    ~4s), so events.log accumulates everything published by earlier tests in
    this module. Tests that assert a text is absent from the file need this
    when the text is not unique to them.
    """
    Path(real_events.temp_dir, "events_log", "events.log").write_text("", encoding="utf-8")
    return real_events


@pytest.mark.parametrize(
    "event_class, make_event, unlowered_log_name, extra_time_to_expiration",
    [
        pytest.param(TestFrameworkEvent, make_critical_framework_event, "critical.log", 10, id="test-framework-event"),
        pytest.param(DatabaseLogEvent, make_no_space_error_event, "error.log", 2, id="db-log-event"),
    ],
)
def test_severity_changer(real_events, event_class, make_event, unlowered_log_name, extra_time_to_expiration):
    """EventsSeverityChangerFilter lowers matching events until it expires; events after expiration keep severity."""
    prefix = make_event.__name__
    with real_events.wait_for_n_events(real_events.get_events_logger(), count=5, timeout=3):
        with EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=event_class,
            extra_time_to_expiration=extra_time_to_expiration,
        ):
            make_event(f"{prefix} that should be lowered #1").publish()
        make_event(f"{prefix} that should be lowered #2").publish()
        event = make_event(f"{prefix} that should not be lowered #3")
        event.source_timestamp = time.time() + extra_time_to_expiration
        event.publish()

    log_content = real_events.get_event_log_file("warning.log")
    unlowered_log_content = real_events.get_event_log_file(unlowered_log_name)

    assert f"{prefix} that should be lowered #1" in log_content
    assert f"{prefix} that should be lowered #2" in log_content
    assert f"{prefix} that should not be lowered #3" not in log_content
    assert f"{prefix} that should not be lowered #3" in unlowered_log_content


def test_severity_changer_expiring_does_not_crash_subscribers(real_events):
    """An expired severity changer is removed by the next matching event without crashing subscribers.

    See https://github.com/scylladb/scylla-cluster-tests/issues/2115
    """
    # filter begin/end + 3 events published 1s apart, so the last one is past the 2s expiration.
    with real_events.wait_for_n_events(real_events.get_events_logger(), count=5, timeout=6):
        with EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            extra_time_to_expiration=2,
        ):
            pass
        for _ in range(3):
            time.sleep(1)
            make_no_space_error_event("error that shouldn't be lowered").publish()

    assert "error that shouldn't be lowered" in real_events.get_event_log_file("error.log")


def test_ycsb_filter(real_events):
    """An active YCSB error filter drops only the error event; once it exits, YCSB errors are logged again."""
    with real_events.wait_for_n_events(real_events.get_events_logger(), count=6, timeout=5):
        with EventsFilter(
            event_class=YcsbStressEvent, regex=".*Internal server error: exceptions::unavailable_exception.*"
        ):
            publish_ycsb_load(YCSB_ERROR_LINE)
            TestFrameworkEvent(source="", source_method="").publish()

    log_content = real_events.get_event_log_file("events.log")

    assert "TestFrameworkEvent" in log_content
    # the error event is filtered out, the begin/end of the stress load are kept
    assert "YcsbStressEvent Severity.ERROR" not in log_content
    assert "YcsbStressEvent Severity.NORMAL) period_type=begin" in log_content
    assert "YcsbStressEvent Severity.NORMAL) period_type=end" in log_content

    with real_events.wait_for_n_events(real_events.get_events_logger(), count=3):
        publish_ycsb_load(YCSB_ERROR_LINE)

    log_content = real_events.get_event_log_file("events.log")

    assert "TestFrameworkEvent" in log_content
    assert "YcsbStressEvent Severity.ERROR" in log_content


@pytest.mark.parametrize(
    "filters_kwargs, filtered_events, filtered_text",
    [
        pytest.param(
            [
                {"db_event": DatabaseLogEvent.NO_SPACE_ERROR},
                {"db_event": DatabaseLogEvent.BACKTRACE, "line": "No space left on device"},
                {"db_event": DatabaseLogEvent.DATABASE_ERROR, "line": "No space left on device"},
                {"db_event": DatabaseLogEvent.FILESYSTEM_ERROR, "line": "No space left on device"},
            ],
            [
                (DatabaseLogEvent.NO_SPACE_ERROR, ENOSPC_LINE_1),
                (DatabaseLogEvent.BACKTRACE, ENOSPC_LINE_1),
                (DatabaseLogEvent.FILESYSTEM_ERROR, ENOSPC_LINE_2),
                (DatabaseLogEvent.DATABASE_ERROR, ENOSPC_LINE_1),
                (DatabaseLogEvent.NO_SPACE_ERROR, ENOSPC_LINE_1),
            ],
            "No space left on device",
            id="filter-enospc",
        ),
        pytest.param(
            [
                {
                    "db_event": DatabaseLogEvent.DATABASE_ERROR,
                    "line": "repair's stream failed: streaming::stream_exception",
                },
                {"db_event": DatabaseLogEvent.RUNTIME_ERROR, "line": "Can not find stream_manager"},
                {"db_event": DatabaseLogEvent.RUNTIME_ERROR, "line": "is aborted"},
            ],
            [(DatabaseLogEvent.RUNTIME_ERROR, REPAIR_ABORTED_LINE)] * 2,
            "repair id 1",
            id="filter-repair",
        ),
        pytest.param(
            [{"db_event": DatabaseLogEvent.RUNTIME_ERROR, "line": "Failed to load schema"}],
            [(DatabaseLogEvent.RUNTIME_ERROR, SCHEMA_LOAD_FAILED_LINE)] * 2,
            "Exception when communicating",
            id="filter-upgrade",
        ),
    ],
)
def test_db_events_filter_drops_known_error_lines(clean_events_log, filters_kwargs, filtered_events, filtered_text):
    """Stacked DbEventsFilters drop db events matching known-failure lines while unmatched db events are still logged."""
    real_events = clean_events_log
    # 2 events per filter (begin/end) + the filtered events + 1 unfiltered event.
    expected_event_count = 2 * len(filters_kwargs) + len(filtered_events) + 1
    with real_events.wait_for_n_events(real_events.get_events_logger(), count=expected_event_count, timeout=3):
        with ExitStack() as stack:
            for filter_kwargs in filters_kwargs:
                stack.enter_context(DbEventsFilter(**filter_kwargs))
            for db_event, line in filtered_events:
                db_event().add_info(node="A", line_number=22, line=line).publish()
            DatabaseLogEvent.BAD_ALLOC().add_info(node="B", line_number=22, line="not filtered").publish()

    log_content = real_events.get_event_log_file("events.log")

    assert "not filtered" in log_content
    assert filtered_text not in log_content


def test_filter_expiration(real_events):
    """DbEventsFilter drops events up to its exit time and logs later events from the same node."""
    with real_events.wait_for_n_events(real_events.get_events_logger(), count=4, timeout=10):
        line_prefix = f"{datetime.now(timezone.utc):%Y-%m-%dT%H:%M:%S+00:00}"

        with DbEventsFilter(db_event=DatabaseLogEvent.NO_SPACE_ERROR, node="A"):
            DatabaseLogEvent.NO_SPACE_ERROR().add_info(
                node="A", line_number=22, line=line_prefix + " this is filtered"
            ).publish()

        line_prefix = f"{datetime.fromtimestamp(time.time() + 1, tz=timezone.utc):%Y-%m-%dT%H:%M:%S+00:00}"
        DatabaseLogEvent.NO_SPACE_ERROR().add_info(
            node="A", line_number=22, line=line_prefix + " : this is not filtered"
        ).publish()

    log_content = real_events.get_event_log_file("events.log")

    assert "this is not filtered" in log_content
    assert "this is filtered" not in log_content


def test_failed_stall_during_filter(real_events):
    """Reactor stalls published under unrelated db filters keep their DEBUG severity."""
    with real_events.wait_for_n_events(real_events.get_events_logger(), count=5, timeout=3):
        with (
            DbEventsFilter(db_event=DatabaseLogEvent.NO_SPACE_ERROR),
            DbEventsFilter(db_event=DatabaseLogEvent.BACKTRACE, line="No space left on device"),
        ):
            event = DatabaseLogEvent.REACTOR_STALLED()
            event.add_info(
                node="A", line_number=22, line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  reactor stall 20"
            ).publish()

    assert event.severity == Severity.DEBUG


@pytest.mark.parametrize(
    "use_counter, count_condition_name",
    [
        pytest.param(use_as_context_manager, "test_method", id="context-manager"),
        pytest.param(use_start_stop_methods, "test_method_obj", id="start-stop-methods"),
    ],
)
def test_count_reactor_stall(real_events, use_counter, count_condition_name):
    """EventCounterContextManager counts reactor stalls into ms buckets and writes per-node stat files."""
    counter = EventCounterContextManager(
        DatabaseLogEvent.REACTOR_STALLED, name=count_condition_name, _registry=real_events.events_processes_registry
    )
    assert counter._event_type == (DatabaseLogEvent.REACTOR_STALLED,)
    with real_events.wait_for_n_events(real_events.get_events_counter(), count=4, timeout=5):
        with use_counter(counter):
            DatabaseLogEvent.REACTOR_STALLED().add_info("node4", "Reactor stalled for 13 ms", 111).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node1", "Reactor stalled for 33 ms", 111).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node2", "Reactor stalled for 6 ms", 111).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node3", "Reactor stalled for 33 ms", 111).publish()
            time.sleep(1)

    statistics = counter.get_stats()
    assert len(statistics.keys()) == 1, "Number of events in statistics is wrong"
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["counter"] == 4
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["ms"] == {50: 2, 10: 1, 20: 1}
    assert_counter_stat_files(real_events, count_condition_name, ["node1", "node2", "node3", "node4"])


def test_count_reactor_stall_not_matched_by_regexp(real_events):
    """Reactor stalls whose line lacks a parsable ms value are counted in the 0 ms bucket."""
    count_condition_name = "test_method1"
    statistics = {}
    with real_events.wait_for_n_events(real_events.get_events_counter(), count=4, timeout=5):
        with EventCounterContextManager(
            DatabaseLogEvent.REACTOR_STALLED, name=count_condition_name, _registry=real_events.events_processes_registry
        ) as counter_manager:
            assert counter_manager._event_type == (DatabaseLogEvent.REACTOR_STALLED,)
            DatabaseLogEvent.REACTOR_STALLED().add_info("node4", "Reactor stalled for 13ms", 111).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node1", "Reactor stalled for 33ms", 111).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node2", "Reactor stalled for 6ms", 111).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node3", "Reactor stalled for33000ms", 111).publish()
            time.sleep(1)
            statistics = counter_manager.get_stats().copy()
    assert len(statistics.keys()) == 1, "Number of events in statistics is wrong"
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["counter"] == 4
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["ms"] == {0: 4}


def test_count_several_events(real_events):
    """A counter for several event types keeps separate stats and stat dirs per type."""
    count_condition_name = "test_method2"
    statistics = {}
    with real_events.wait_for_n_events(real_events.get_events_counter(), count=4, timeout=5):
        with EventCounterContextManager(
            (DatabaseLogEvent.REACTOR_STALLED, DatabaseLogEvent.KERNEL_CALLSTACK),
            name=count_condition_name,
            _registry=real_events.events_processes_registry,
        ) as counter_manager:
            assert counter_manager._event_type == (
                DatabaseLogEvent.REACTOR_STALLED,
                DatabaseLogEvent.KERNEL_CALLSTACK,
            )

            DatabaseLogEvent.REACTOR_STALLED().add_info("node1", "Reactor stalled for 33 ms", 111).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node2", "Reactor stalled for 25 ms", 111).publish()
            DatabaseLogEvent.KERNEL_CALLSTACK().add_info("node1", "Kernel Stack: some backtraces", 112).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node3", "Reactor stalled for 33 ms", 111).publish()
            time.sleep(1)
            statistics = counter_manager.get_stats().copy()

    assert len(statistics.keys()) == 2, "Number of events in statistics is wrong"
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["counter"] == 3
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["ms"] == {50: 2, 30: 1}
    assert statistics["DatabaseLogEvent.KERNEL_CALLSTACK"]["counter"] == 1
    assert_counter_stat_files(real_events, count_condition_name, ["node1", "node2", "node3"])


def test_skip_not_required_count_required_events(real_events):
    """A counter counts only its configured event types and ignores others published alongside."""
    count_condition_name = "test_method3"
    statistics = {}
    with real_events.wait_for_n_events(real_events.get_events_counter(), count=6, timeout=5):
        with EventCounterContextManager(
            (DatabaseLogEvent.REACTOR_STALLED, DatabaseLogEvent.KERNEL_CALLSTACK),
            name=count_condition_name,
            _registry=real_events.events_processes_registry,
        ) as counter_manager:
            assert counter_manager._event_type == (
                DatabaseLogEvent.REACTOR_STALLED,
                DatabaseLogEvent.KERNEL_CALLSTACK,
            )

            DatabaseLogEvent.REACTOR_STALLED().add_info("node1", "Reactor stalled for 33 ms", 111).publish()
            DatabaseLogEvent.BACKTRACE().add_info(node="node1", line_number=22, line=OTHER_BACKTRACE_LINE).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node2", "Reactor stalled for 2501 ms", 111).publish()
            DatabaseLogEvent.BACKTRACE().add_info(node="node2", line_number=22, line=OTHER_BACKTRACE_LINE).publish()
            DatabaseLogEvent.KERNEL_CALLSTACK().add_info(
                "node2", "Kernel Stack: asdfasdf ms asdfjasdlfjas dfs", 112
            ).publish()
            DatabaseLogEvent.BACKTRACE().add_info(node="node3", line_number=22, line=OTHER_BACKTRACE_LINE).publish()
            time.sleep(1)
            statistics = counter_manager.get_stats().copy()

    assert len(statistics.keys()) == 2, "Number of events in statistics is wrong"
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["counter"] == 2
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["ms"] == {50: 1, 2501: 1}
    assert statistics["DatabaseLogEvent.KERNEL_CALLSTACK"]["counter"] == 1
    assert_counter_stat_files(real_events, count_condition_name, ["node1", "node2"])


def test_count_with_several_count_managers(real_events):
    """Concurrent counters with overlapping event types each count all their matching events."""
    count_condition_name1 = "test_method4"
    count_condition_name2 = "test_method5"
    statistics1 = {}
    statistics2 = {}
    with real_events.wait_for_n_events(real_events.get_events_counter(), count=6, timeout=5):
        with (
            EventCounterContextManager(
                (DatabaseLogEvent.REACTOR_STALLED, DatabaseLogEvent.KERNEL_CALLSTACK),
                name=count_condition_name1,
                _registry=real_events.events_processes_registry,
            ) as counter_manager1,
            EventCounterContextManager(
                (DatabaseLogEvent.REACTOR_STALLED, DatabaseLogEvent.BACKTRACE),
                name=count_condition_name2,
                _registry=real_events.events_processes_registry,
            ) as counter_manager2,
        ):
            assert counter_manager1._event_type == (
                DatabaseLogEvent.REACTOR_STALLED,
                DatabaseLogEvent.KERNEL_CALLSTACK,
            )
            assert counter_manager2._event_type == (DatabaseLogEvent.REACTOR_STALLED, DatabaseLogEvent.BACKTRACE)

            DatabaseLogEvent.REACTOR_STALLED().add_info("node1", "Reactor stalled for 33 ms", 111).publish()
            DatabaseLogEvent.BACKTRACE().add_info(node="node1", line_number=22, line=OTHER_BACKTRACE_LINE).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node2", "Reactor stalled for 150 ms", 111).publish()
            DatabaseLogEvent.BACKTRACE().add_info(node="node2", line_number=22, line=OTHER_BACKTRACE_LINE).publish()
            DatabaseLogEvent.KERNEL_CALLSTACK().add_info(
                "node2", "Kernel Stack: asdfasdf ms asdfjasdlfjas dfs", 112
            ).publish()
            DatabaseLogEvent.BACKTRACE().add_info(node="node3", line_number=22, line=OTHER_BACKTRACE_LINE).publish()
            time.sleep(1)
            statistics1 = counter_manager1.get_stats().copy()
            statistics2 = counter_manager2.get_stats().copy()

    assert len(statistics1.keys()) == 2, "Number of events in statistics is wrong"
    assert len(statistics2.keys()) == 2, "Number of events in statistics is wrong"
    assert statistics1["DatabaseLogEvent.REACTOR_STALLED"]["counter"] == 2
    assert statistics1["DatabaseLogEvent.REACTOR_STALLED"]["ms"] == {50: 1, 200: 1}
    assert statistics1["DatabaseLogEvent.KERNEL_CALLSTACK"]["counter"] == 1
    assert statistics2["DatabaseLogEvent.REACTOR_STALLED"]["counter"] == 2
    assert statistics2["DatabaseLogEvent.REACTOR_STALLED"]["ms"] == {50: 1, 200: 1}
    assert statistics2["DatabaseLogEvent.BACKTRACE"]["counter"] == 3
    assert_counter_stat_files(real_events, count_condition_name1, ["node1", "node2"])
    assert_counter_stat_files(real_events, count_condition_name2, ["node1", "node2", "node3"])


def test_count_with_several_nested_count_managers(real_events):
    """A nested counter counts only events published while it is active; the outer one counts all."""
    count_condition_name1 = "test_method6"
    count_condition_name2 = "test_method7"
    statistics1 = {}
    statistics2 = {}
    with real_events.wait_for_n_events(real_events.get_events_counter(), count=6, timeout=5):
        with EventCounterContextManager(
            (DatabaseLogEvent.REACTOR_STALLED, DatabaseLogEvent.KERNEL_CALLSTACK),
            name=count_condition_name1,
            _registry=real_events.events_processes_registry,
        ) as counter_manager1:
            assert counter_manager1._event_type == (
                DatabaseLogEvent.REACTOR_STALLED,
                DatabaseLogEvent.KERNEL_CALLSTACK,
            )
            DatabaseLogEvent.REACTOR_STALLED().add_info("node1", "Reactor stalled for 33 ms", 111).publish()
            DatabaseLogEvent.BACKTRACE().add_info(node="node1", line_number=22, line=OTHER_BACKTRACE_LINE).publish()
            time.sleep(1)
            with EventCounterContextManager(
                (DatabaseLogEvent.REACTOR_STALLED, DatabaseLogEvent.BACKTRACE),
                name=count_condition_name2,
                _registry=real_events.events_processes_registry,
            ) as counter_manager2:
                assert counter_manager2._event_type == (
                    DatabaseLogEvent.REACTOR_STALLED,
                    DatabaseLogEvent.BACKTRACE,
                )

                DatabaseLogEvent.REACTOR_STALLED().add_info("node2", "Reactor stalled for 499 ms", 111).publish()
                DatabaseLogEvent.BACKTRACE().add_info(node="node2", line_number=22, line=OTHER_BACKTRACE_LINE).publish()

                DatabaseLogEvent.KERNEL_CALLSTACK().add_info(
                    "node2", "Kernel Stack: asdfasdf ms asdfjasdlfjas dfs", 112
                ).publish()
                DatabaseLogEvent.BACKTRACE().add_info(node="node3", line_number=22, line=OTHER_BACKTRACE_LINE).publish()
                time.sleep(1)
                statistics2 = counter_manager2.get_stats().copy()

            statistics1 = counter_manager1.get_stats().copy()

    assert len(statistics1.keys()) == 2, "Number of events in statistics is wrong"
    assert len(statistics2.keys()) == 2, "Number of events in statistics is wrong"
    assert statistics1["DatabaseLogEvent.REACTOR_STALLED"]["counter"] == 2
    assert statistics1["DatabaseLogEvent.REACTOR_STALLED"]["ms"] == {50: 1, 1000: 1}
    assert statistics1["DatabaseLogEvent.KERNEL_CALLSTACK"]["counter"] == 1
    assert statistics2["DatabaseLogEvent.REACTOR_STALLED"]["counter"] == 1
    assert statistics2["DatabaseLogEvent.REACTOR_STALLED"]["ms"] == {1000: 1}
    assert statistics2["DatabaseLogEvent.BACKTRACE"]["counter"] == 2
    assert_counter_stat_files(real_events, count_condition_name1, ["node1", "node2"])
    assert_counter_stat_files(real_events, count_condition_name2, ["node2"])
