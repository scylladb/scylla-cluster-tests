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
import logging
from pathlib import Path
from contextlib import ExitStack
from datetime import datetime, timezone
from unittest import mock

import pytest

from sdcm.exceptions import UnsupportedNemesis, KillNemesis
from sdcm.sct_events.nodetool import NodetoolEvent
from sdcm.sct_events import Severity
from sdcm.sct_events.system import CoreDumpEvent, TestFrameworkEvent, SoftTimeoutEvent
from sdcm.sct_events.base import EventPeriod
from sdcm.sct_events.filters import DbEventsFilter, EventsFilter, EventsSeverityChangerFilter
from sdcm.sct_events.loaders import YcsbStressEvent
from sdcm.sct_events.nemesis import DisruptionEvent
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.event_counter import EventCounterContextManager

from unit_tests.lib.real_events import RealEventsTest

LOGGER = logging.getLogger(__name__)

YCSB_ERROR_LINE = (
    "237951 [Thread-47] ERROR site.ycsb.db.DynamoDBClient"
    " -com.amazonaws.AmazonServiceException: Internal server error:"
    " exceptions::unavailable_exception (Cannot achieve consistency"
    " level for cl LOCAL_ONE. Requires 1, alive 0) (Service: AmazonDynamoDBv2;"
    " Status Code: 500; Error Code: Internal Server Error; Request ID: null)"
)
YCSB_NODE = "Node alternator-3h-silence--loader-node-bb90aa05-2 [34.251.153.122 | 10.0.220.55] (seed: False)"


def publish_ycsb_load(error_line):
    """Mimic what YcsbStressThread publishes: begin/end around per-error-line events sharing the event_id."""
    ycsb_event = YcsbStressEvent(node=YCSB_NODE, stress_cmd="ycsb")
    ycsb_event.begin_event()
    error_event = YcsbStressEvent(node=YCSB_NODE, stress_cmd="ycsb", severity=Severity.ERROR, errors=[error_line])
    error_event.event_id = ycsb_event.event_id
    error_event.period_type = EventPeriod.INFORMATIONAL.value
    error_event.publish()
    ycsb_event.end_event()


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
    this module. Tests that assert on the file's exact content need this;
    tests that only check for/absence of their own unique substrings don't.
    """
    Path(real_events.temp_dir, "events_log", "events.log").write_text("", encoding="utf-8")
    return real_events


def test_disruption_skipped_event():
    """A skipped disruption ends with NORMAL severity and reports the skip reason in its string form."""
    with (
        pytest.raises(UnsupportedNemesis),
        DisruptionEvent(nemesis_name="DeleteByRowsRange", node="target_node", publish_event=False) as nemesis_event,
    ):
        try:
            raise UnsupportedNemesis("This nemesis can run on scylla_bench test only")
        except UnsupportedNemesis as exc:
            nemesis_event.event_id = "c2561d8b-97ca-44fb-b5b1-8bcc0d437318"
            nemesis_event.skip(skip_reason=str(exc))
            nemesis_event.duration = 15
            raise

    assert str(nemesis_event) == (
        "(DisruptionEvent Severity.NORMAL) period_type=end event_id=c2561d8b-97ca-44fb-b5b1-8bcc0d437318 "
        "duration=15s: nemesis_name=DeleteByRowsRange target_node=target_node skipped skip_reason=This nemesis can "
        "run on scylla_bench test only"
    )


def test_disruption_raised_critical_event():
    """An exception escaping a disruption keeps an explicitly set CRITICAL severity and records the error."""
    with (
        pytest.raises(ZeroDivisionError),
        DisruptionEvent(nemesis_name="DeleteByRowsRange", node="target_node", publish_event=False) as nemesis_event,
    ):
        nemesis_event.event_id = "c2561d8b-97ca-44fb-b5b1-8bcc0d437318"
        assert str(nemesis_event) == (
            "(DisruptionEvent Severity.NORMAL) period_type=begin event_id=c2561d8b-97ca-44fb-b5b1-8bcc0d437318: "
            "nemesis_name=DeleteByRowsRange target_node=target_node"
        )

        try:
            1 / 0
        except ZeroDivisionError:
            nemesis_event.add_error(["division by zero"])
            nemesis_event.full_traceback = "traceback.format_exc()"
            nemesis_event.severity = Severity.CRITICAL
            nemesis_event.duration = 15
            raise

    assert "division by zero" in nemesis_event.errors_formatted
    assert nemesis_event.severity == Severity.CRITICAL
    assert nemesis_event.duration_formatted == "15s"


def test_disruption_raised_error_event():
    """An exception escaping a disruption raises its severity to ERROR and records the error."""
    with (
        pytest.raises(ZeroDivisionError),
        DisruptionEvent(nemesis_name="DeleteByRowsRange", node="target_node", publish_event=False) as nemesis_event,
    ):
        nemesis_event.event_id = "c2561d8b-97ca-44fb-b5b1-8bcc0d437318"
        assert str(nemesis_event) == (
            "(DisruptionEvent Severity.NORMAL) period_type=begin event_id=c2561d8b-97ca-44fb-b5b1-8bcc0d437318: "
            "nemesis_name=DeleteByRowsRange target_node=target_node"
        )

        try:
            1 / 0
        except ZeroDivisionError:
            nemesis_event.add_error(["division by zero"])
            nemesis_event.full_traceback = "traceback.format_exc()"
            nemesis_event.duration = 15
            raise

    assert "division by zero" in nemesis_event.errors_formatted
    assert nemesis_event.severity == Severity.ERROR
    assert nemesis_event.duration_formatted == "15s"


def test_disruption_error_event():
    """A disruption that handles its error and sets ERROR severity renders the errors in its end string."""
    with DisruptionEvent(nemesis_name="DeleteByRowsRange", node="target_node", publish_event=False) as nemesis_event:
        nemesis_event.event_id = "c2561d8b-97ca-44fb-b5b1-8bcc0d437318"
        assert str(nemesis_event) == (
            "(DisruptionEvent Severity.NORMAL) period_type=begin event_id=c2561d8b-97ca-44fb-b5b1-8bcc0d437318: "
            "nemesis_name=DeleteByRowsRange target_node=target_node"
        )

        try:
            1 / 0
        except ZeroDivisionError:
            nemesis_event.add_error(["division by zero"])
            nemesis_event.full_traceback = "traceback.format_exc()"
            nemesis_event.duration = 15
            nemesis_event.severity = Severity.ERROR

    assert (
        "(DisruptionEvent Severity.ERROR) period_type=end event_id=c2561d8b-97ca-44fb-b5b1-8bcc0d437318 "
        "duration=15s: nemesis_name=DeleteByRowsRange target_node=target_node errors=division by zero\n"
    ) in str(nemesis_event)


def test_disruption_normal_event():
    """A clean disruption renders begin and end strings with NORMAL severity and the duration."""
    with DisruptionEvent(nemesis_name="DeleteByRowsRange", node="target_node", publish_event=False) as nemesis_event:
        nemesis_event.event_id = "c2561d8b-97ca-44fb-b5b1-8bcc0d437318"
        assert str(nemesis_event) == (
            "(DisruptionEvent Severity.NORMAL) period_type=begin event_id=c2561d8b-97ca-44fb-b5b1-8bcc0d437318: "
            "nemesis_name=DeleteByRowsRange target_node=target_node"
        )
        nemesis_event.duration = 15

    assert str(nemesis_event) == (
        "(DisruptionEvent Severity.NORMAL) period_type=end event_id=c2561d8b-97ca-44fb-b5b1-8bcc0d437318 "
        "duration=15s: nemesis_name=DeleteByRowsRange target_node=target_node"
    )


def test_filter(clean_events_log):
    """Stacked DbEventsFilters drop every ENOSPC-related db event, leaving events.log empty."""
    real_events = clean_events_log
    enospc_line_1 = (
        "[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  [shard 8] commitlog - Exception in segment "
        "reservation: storage_io_error (Storage I/O error: 28: No space left on device)"
    )
    enospc_line_2 = (
        "2019-10-29T12:19:49+00:00  ip-172-30-0-184 !WARNING | scylla: [shard 2] storage_service - Commitlog "
        "error: std::filesystem::__cxx11::filesystem_error (error system:28, filesystem error: open failed: No "
        "space left on device [/var/lib/scylla/hints/2/172.30.0.116/HintsLog-1-36028797019122576.log])"
    )

    # 13 events in total: 2 events per filter x 4 filters + 5 events.
    with real_events.wait_for_n_events(real_events.get_events_logger(), count=13, timeout=3):
        with (
            DbEventsFilter(db_event=DatabaseLogEvent.NO_SPACE_ERROR),
            DbEventsFilter(db_event=DatabaseLogEvent.BACKTRACE, line="No space left on device"),
            DbEventsFilter(db_event=DatabaseLogEvent.DATABASE_ERROR, line="No space left on device"),
            DbEventsFilter(db_event=DatabaseLogEvent.FILESYSTEM_ERROR, line="No space left on device"),
        ):
            DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="A", line_number=22, line=enospc_line_1).publish()
            DatabaseLogEvent.BACKTRACE().add_info(node="A", line_number=22, line=enospc_line_1).publish()
            DatabaseLogEvent.FILESYSTEM_ERROR().add_info(node="A", line_number=22, line=enospc_line_2).publish()
            DatabaseLogEvent.DATABASE_ERROR().add_info(node="A", line_number=22, line=enospc_line_1).publish()
            DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="A", line_number=22, line=enospc_line_1).publish()

    assert real_events.get_event_log_file("events.log") == ""


@pytest.mark.parametrize(
    "filter_kwargs, download_instructions, filtered_text",
    [
        pytest.param(
            {"event_class": CoreDumpEvent}, "test_general_filter", "test_general_filter", id="general-filter-by-class"
        ),
        pytest.param(
            {"regex": ".*1234567890.*"},
            "gsutil cp gs://upload.scylladb.com/core.scylla-jmx.996.1234567890"
            ".3968.1566979933000/core.scylla-jmx.996.d173729352e34c76aaf8db334"
            "2153c3e.3968.1566979933000000 .",
            "1234567890",
            id="general-filter-regex",
        ),
    ],
)
def test_events_filter_drops_matching_core_dump_event(real_events, filter_kwargs, download_instructions, filtered_text):
    """EventsFilter drops events matching its class or regex from events.log while other events are still logged."""
    with real_events.wait_for_n_events(real_events.get_events_logger(), count=4, timeout=3):
        with EventsFilter(**filter_kwargs):
            CoreDumpEvent(
                corefile_url="http://",
                backtrace="asfasdfsdf",
                node="node xy",
                download_instructions=download_instructions,
                source_timestamp=1578998425.0,
            ).publish()  # Tue 2020-01-14 10:40:25 UTC
            TestFrameworkEvent(source="", source_method="").publish()

    log_content = real_events.get_event_log_file("events.log")

    assert "TestFrameworkEvent" in log_content
    assert filtered_text not in log_content


def test_severity_changer(real_events):
    """EventsSeverityChangerFilter lowers matching events until it expires, events after expiration keep CRITICAL."""
    extra_time_to_expiration = 10
    with real_events.wait_for_n_events(real_events.get_events_logger(), count=5, timeout=3):
        with EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=TestFrameworkEvent,
            extra_time_to_expiration=extra_time_to_expiration,
        ):
            TestFrameworkEvent(
                source="critical that should be lowered #1", source_method="", severity=Severity.CRITICAL
            ).publish()
        TestFrameworkEvent(
            source="critical that should be lowered #2", source_method="", severity=Severity.CRITICAL
        ).publish()
        event = TestFrameworkEvent(
            source="critical that should not be lowered #3", source_method="", severity=Severity.CRITICAL
        )
        event.source_timestamp = time.time() + extra_time_to_expiration
        event.publish()

    log_content = real_events.get_event_log_file("warning.log")
    crit_log_content = real_events.get_event_log_file("critical.log")

    assert "TestFrameworkEvent" in log_content
    assert "critical that should be lowered #1" in log_content
    assert "critical that should be lowered #2" in log_content
    assert "critical that should not be lowered #3" not in log_content
    assert "critical that should not be lowered #3" in crit_log_content


def test_severity_changer_db_log(real_events):
    """Severity changer lowers db log events until expiry and is then removed without crashing subscribers.

    See https://github.com/scylladb/scylla-cluster-tests/issues/2115
    """
    extra_time_to_expiration = 2
    # 1) Lower DatabaseLogEvent to WARNING for 1 sec.
    with real_events.wait_for_n_events(real_events.get_events_logger(), count=5, timeout=3):
        with EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            extra_time_to_expiration=extra_time_to_expiration,
        ):
            DatabaseLogEvent.NO_SPACE_ERROR().add_info(
                node="A", line_number=22, line="error that should be lowered #1"
            ).publish()
        DatabaseLogEvent.NO_SPACE_ERROR().add_info(
            node="A", line_number=22, line="error that should be lowered #2"
        ).publish()
        event = DatabaseLogEvent.NO_SPACE_ERROR().add_info(
            node="A", line_number=22, line="error that should not be lowered #3"
        )
        event.source_timestamp = time.time() + extra_time_to_expiration
        event.publish()

    log_content = real_events.get_event_log_file("warning.log")
    error_log_content = real_events.get_event_log_file("error.log")

    assert "DatabaseLogEvent" in log_content
    assert "error that should be lowered #1" in log_content
    assert "error that should be lowered #2" in log_content
    assert "error that should not be lowered #3" not in log_content
    assert "error that should not be lowered #3" in error_log_content

    # 2) One of the next DatabaseLogEvent event should expire the EventsSeverityChangerFilter
    #    (and not crash all subscribers)
    with real_events.wait_for_n_events(real_events.get_events_logger(), count=2, timeout=3):
        for _ in range(2):
            time.sleep(1)
            DatabaseLogEvent.NO_SPACE_ERROR().add_info(
                node="A", line_number=22, line="error that shouldn't be lowered"
            ).publish()

    log_content = real_events.get_event_log_file("error.log")

    assert "error that shouldn't be lowered" in log_content


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
    "filters_kwargs, known_failure_line, filtered_text",
    [
        pytest.param(
            [
                {
                    "db_event": DatabaseLogEvent.DATABASE_ERROR,
                    "line": "repair's stream failed: streaming::stream_exception",
                },
                {"db_event": DatabaseLogEvent.RUNTIME_ERROR, "line": "Can not find stream_manager"},
                {"db_event": DatabaseLogEvent.RUNTIME_ERROR, "line": "is aborted"},
            ],
            "2019-07-28T10:53:29+00:00  ip-10-0-167-91 !INFO    | scylla.bin: [shard 0] repair - "
            "Got error in row level repair: std::runtime_error (repair id 1 is aborted on shard 0)",
            "repair id 1",
            id="filter-repair",
        ),
        pytest.param(
            [{"db_event": DatabaseLogEvent.RUNTIME_ERROR, "line": "Failed to load schema"}],
            "!ERR     | scylla:  [shard 3] storage_proxy - Exception when communicating with "
            "10.142.0.56: std::runtime_error (Failed to load schema version "
            "b40e405f-462c-38f2-a90c-6f130ddbf6f3) ",
            "Exception when communicating",
            id="filter-upgrade",
        ),
    ],
)
def test_db_events_filter_drops_known_runtime_error_lines(
    real_events, filters_kwargs, known_failure_line, filtered_text
):
    """DbEventsFilter drops runtime errors matching a known-failure line while unmatched db events are still logged."""
    # 2 events per filter (begin/end) + 3 published events.
    with real_events.wait_for_n_events(real_events.get_events_logger(), count=2 * len(filters_kwargs) + 3, timeout=3):
        with ExitStack() as stack:
            for filter_kwargs in filters_kwargs:
                stack.enter_context(DbEventsFilter(**filter_kwargs))
            DatabaseLogEvent.RUNTIME_ERROR().add_info(node="A", line_number=22, line=known_failure_line).publish()
            DatabaseLogEvent.RUNTIME_ERROR().add_info(node="A", line_number=22, line=known_failure_line).publish()
            DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="B", line_number=22, line="not filtered").publish()

    log_content = real_events.get_event_log_file("events.log")

    assert "not filtered" in log_content
    assert filtered_text not in log_content


def test_filter_by_node(real_events):
    """DbEventsFilter with a node drops events from that node only."""
    with real_events.wait_for_n_events(real_events.get_events_logger(), count=4, timeout=3):
        with DbEventsFilter(db_event=DatabaseLogEvent.NO_SPACE_ERROR, node="A"):
            DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="A", line_number=22, line="this is filtered").publish()
            DatabaseLogEvent.NO_SPACE_ERROR().add_info(node="B", line_number=22, line="not filtered").publish()

    log_content = real_events.get_event_log_file("events.log")

    assert "not filtered" in log_content
    assert "this is filtered" not in log_content


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
    "duration_input, duration_formatted",
    [
        pytest.param(None, "", id="none"),
        pytest.param(2, "2s", id="seconds"),
        pytest.param(2.52, "2.52s", id="fractional-seconds"),
        pytest.param(326, "5m26s", id="minutes"),
        pytest.param(4598, "1h16m38s", id="hours"),
        pytest.param(87400, "1d0h16m40s", id="days"),
    ],
)
def test_duration_format(duration_input, duration_formatted):
    """duration_formatted renders seconds as a compact d/h/m/s string, empty when duration is unset."""
    event = NodetoolEvent(
        nodetool_command="scrub --skip-corrupted drop_table_during_repair_ks_0",
        node="1.0.0.121",
        options="more options",
        publish_event=False,
    )

    event.duration = duration_input
    assert event.duration_formatted == duration_formatted


@pytest.mark.parametrize(
    "publish_event",
    [
        pytest.param(True, id="publish-called"),
        pytest.param(False, id="publish-not-called"),
    ],
)
def test_begin_event_publishes_only_when_publish_event_is_set(publish_event):
    """begin_event() publishes the event exactly when it was created with publish_event=True."""
    with mock.patch("sdcm.sct_events.base.SctEvent.publish") as publish:
        event = NodetoolEvent(nodetool_command="scrub", node="1.0.0.121", options="", publish_event=publish_event)
        event.begin_event()
    assert publish.called is publish_event


def test_soft_timeout():
    """SoftTimeoutEvent carries a traceback and reports the operation, its duration and the soft timeout."""
    event = SoftTimeoutEvent(soft_timeout=0.1, operation="long-one", duration=0.2)
    event.publish_or_dump()
    event_data = str(event)

    assert event.trace
    assert "operation 'long-one' is finished and took 0.2s (soft timeout was set to 0.1s)" in event_data


def test_count_reactor_stall(real_events):
    """EventCounterContextManager counts reactor stalls into ms buckets and writes per-node stat files."""
    count_condition_name = "test_method"
    statistics = {}
    with real_events.wait_for_n_events(real_events.get_events_counter(), count=4, timeout=5):
        with EventCounterContextManager(
            DatabaseLogEvent.REACTOR_STALLED, name=count_condition_name, _registry=real_events.events_processes_registry
        ) as counter_manager:
            assert counter_manager._event_type == (DatabaseLogEvent.REACTOR_STALLED,)
            DatabaseLogEvent.REACTOR_STALLED().add_info("node4", "Reactor stalled for 13 ms", 111).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node1", "Reactor stalled for 33 ms", 111).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node2", "Reactor stalled for 6 ms", 111).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node3", "Reactor stalled for 33 ms", 111).publish()
            time.sleep(1)
            statistics = counter_manager.get_stats().copy()

    assert len(statistics.keys()) == 1, "Number of events in statistics is wrong"
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["counter"] == 4
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["ms"] == {50: 2, 10: 1, 20: 1}
    reason_stat_dir = (
        real_events.get_events_counter().events_stat_dir
        / Path(count_condition_name)
        / Path("DatabaseLogEvent.REACTOR_STALLED")
    )
    assert reason_stat_dir.is_dir()
    for file_event in reason_stat_dir.iterdir():
        assert file_event.name in ["node1", "node2", "node3", "node4"]
        with open(file_event, encoding="utf-8") as fp_event:
            assert len(fp_event.readlines()) == 1, "Wrong number of lines"


def test_count_reactor_stall_with_obj_instance(real_events):
    """EventCounterContextManager used via start/stop methods counts the same as the context manager form."""
    count_condition_name = "test_method_obj"
    counter = EventCounterContextManager(
        DatabaseLogEvent.REACTOR_STALLED, name=count_condition_name, _registry=real_events.events_processes_registry
    )

    assert counter._event_type == (DatabaseLogEvent.REACTOR_STALLED,)
    counter.start_event_counter()
    with real_events.wait_for_n_events(real_events.get_events_counter(), count=4, timeout=5):
        DatabaseLogEvent.REACTOR_STALLED().add_info("node4", "Reactor stalled for 13 ms", 111).publish()
        DatabaseLogEvent.REACTOR_STALLED().add_info("node1", "Reactor stalled for 33 ms", 111).publish()
        DatabaseLogEvent.REACTOR_STALLED().add_info("node2", "Reactor stalled for 6 ms", 111).publish()
        DatabaseLogEvent.REACTOR_STALLED().add_info("node3", "Reactor stalled for 33 ms", 111).publish()
        time.sleep(1)

    counter.stop_event_counter()
    statistics = counter.get_stats()
    assert len(statistics.keys()) == 1, "Number of events in statistics is wrong"
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["counter"] == 4
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["ms"] == {50: 2, 10: 1, 20: 1}
    reason_stat_dir = (
        real_events.get_events_counter().events_stat_dir
        / Path(count_condition_name)
        / Path("DatabaseLogEvent.REACTOR_STALLED")
    )
    assert reason_stat_dir.is_dir()
    for file_event in reason_stat_dir.iterdir():
        assert file_event.name in ["node1", "node2", "node3", "node4"]
        with open(file_event, encoding="utf-8") as fp_event:
            assert len(fp_event.readlines()) == 1, "Wrong number of lines"


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
            # assert not counter_manager._counter_device._cm_register, f"{counter_manager._counter_device._cm_register}"
            statistics = counter_manager.get_stats().copy()

    assert len(statistics.keys()) == 2, "Number of events in statistics is wrong"
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["counter"] == 3
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["ms"] == {50: 2, 30: 1}
    assert statistics["DatabaseLogEvent.KERNEL_CALLSTACK"]["counter"] == 1
    reason_stat_dir = real_events.get_events_counter().events_stat_dir / Path(count_condition_name)
    assert reason_stat_dir.is_dir()
    reactor_stall_dir = reason_stat_dir / Path("DatabaseLogEvent.REACTOR_STALLED")
    assert reactor_stall_dir.is_dir()
    for file_event in reactor_stall_dir.iterdir():
        assert file_event.name in ["node1", "node2", "node3"]
        with open(file_event, encoding="utf-8") as fp_event:
            assert len(fp_event.readlines()) == 1, "Wrong number of lines"


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
            DatabaseLogEvent.BACKTRACE().add_info(
                node="node1", line_number=22, line="other back trace that shouldn't be filtered"
            ).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node2", "Reactor stalled for 2501 ms", 111).publish()
            DatabaseLogEvent.BACKTRACE().add_info(
                node="node2", line_number=22, line="other back trace that shouldn't be filtered"
            ).publish()
            DatabaseLogEvent.KERNEL_CALLSTACK().add_info(
                "node2", "Kernel Stack: asdfasdf ms asdfjasdlfjas dfs", 112
            ).publish()
            DatabaseLogEvent.BACKTRACE().add_info(
                node="node3", line_number=22, line="other back trace that shouldn't be filtered"
            ).publish()
            time.sleep(1)
            statistics = counter_manager.get_stats().copy()

    assert len(statistics.keys()) == 2, "Number of events in statistics is wrong"
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["counter"] == 2
    assert statistics["DatabaseLogEvent.REACTOR_STALLED"]["ms"] == {50: 1, 2501: 1}
    assert statistics["DatabaseLogEvent.KERNEL_CALLSTACK"]["counter"] == 1
    reason_stat_dir = real_events.get_events_counter().events_stat_dir / Path(count_condition_name)
    assert reason_stat_dir.is_dir()
    reactor_stall_dir = reason_stat_dir / Path("DatabaseLogEvent.REACTOR_STALLED")
    assert reactor_stall_dir.is_dir()
    for file_event in reactor_stall_dir.iterdir():
        assert file_event.name in ["node1", "node2"]
        with open(file_event, encoding="utf-8") as fp_event:
            assert len(fp_event.readlines()) == 1, "Wrong number of lines"


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
            DatabaseLogEvent.BACKTRACE().add_info(
                node="node1", line_number=22, line="other back trace that shouldn't be filtered"
            ).publish()
            DatabaseLogEvent.REACTOR_STALLED().add_info("node2", "Reactor stalled for 150 ms", 111).publish()
            DatabaseLogEvent.BACKTRACE().add_info(
                node="node2", line_number=22, line="other back trace that shouldn't be filtered"
            ).publish()
            DatabaseLogEvent.KERNEL_CALLSTACK().add_info(
                "node2", "Kernel Stack: asdfasdf ms asdfjasdlfjas dfs", 112
            ).publish()
            DatabaseLogEvent.BACKTRACE().add_info(
                node="node3", line_number=22, line="other back trace that shouldn't be filtered"
            ).publish()
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

    reason_stat_dir = real_events.get_events_counter().events_stat_dir / Path(count_condition_name1)
    assert reason_stat_dir.is_dir()
    reactor_stall_dir = reason_stat_dir / Path("DatabaseLogEvent.REACTOR_STALLED")
    assert reactor_stall_dir.is_dir()

    for file_event in reactor_stall_dir.iterdir():
        assert file_event.name in ["node1", "node2"]
        with open(file_event, encoding="utf-8") as fp_event:
            assert len(fp_event.readlines()) == 1, "Wrong number of lines"

    reason_stat_dir = real_events.get_events_counter().events_stat_dir / Path(count_condition_name2)
    assert reason_stat_dir.is_dir()
    reactor_stall_dir = reason_stat_dir / Path("DatabaseLogEvent.REACTOR_STALLED")
    assert reactor_stall_dir.is_dir()

    for file_event in reactor_stall_dir.iterdir():
        assert file_event.name in ["node1", "node2", "node3"]
        with open(file_event, encoding="utf-8") as fp_event:
            assert len(fp_event.readlines()) == 1, "Wrong number of lines"


def test_count_with_several_nested_count_managers(real_events):
    """A nested counter counts only events published while it is active; the outer one counts all."""
    time.sleep(5)
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
            DatabaseLogEvent.BACKTRACE().add_info(
                node="node1", line_number=22, line="other back trace that shouldn't be filtered"
            ).publish()
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
                DatabaseLogEvent.BACKTRACE().add_info(
                    node="node2", line_number=22, line="other back trace that shouldn't be filtered"
                ).publish()

                DatabaseLogEvent.KERNEL_CALLSTACK().add_info(
                    "node2", "Kernel Stack: asdfasdf ms asdfjasdlfjas dfs", 112
                ).publish()
                DatabaseLogEvent.BACKTRACE().add_info(
                    node="node3", line_number=22, line="other back trace that shouldn't be filtered"
                ).publish()
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

    reason_stat_dir = real_events.get_events_counter().events_stat_dir / Path(count_condition_name1)
    assert reason_stat_dir.is_dir()
    reactor_stall_dir = reason_stat_dir / Path("DatabaseLogEvent.REACTOR_STALLED")
    assert reactor_stall_dir.is_dir()

    for file_event in reactor_stall_dir.iterdir():
        assert file_event.name in ["node1", "node2"]
        with open(file_event, encoding="utf-8") as fp_event:
            lines = fp_event.readlines()
            LOGGER.info("File %s: content: %s", file_event, lines)
            assert len(lines) == 1, f"Wrong number of lines {lines}"

    reason_stat_dir = real_events.get_events_counter().events_stat_dir / Path(count_condition_name2)
    assert reason_stat_dir.is_dir()
    reactor_stall_dir = reason_stat_dir / Path("DatabaseLogEvent.REACTOR_STALLED")
    assert reactor_stall_dir.is_dir()

    for file_event in reactor_stall_dir.iterdir():
        assert file_event.name == "node2"
        with open(file_event, encoding="utf-8") as fp_event:
            lines = fp_event.readlines()
            LOGGER.info("File %s: content: %s", file_event, lines)
            assert len(lines) == 1, f"Wrong number of lines {lines}"


def test_kill_nemesis_during_con_event():
    """KillNemesis escaping a disruption leaves it NORMAL with no errors, since killing is not a failure."""
    with (
        pytest.raises(KillNemesis),
        DisruptionEvent(nemesis_name="SomeNemesis", node="target_node", publish_event=False) as nemesis_event,
    ):
        nemesis_event.event_id = "c2561d8b-97ca-44fb-b5b1-8bcc0d437318"
        assert str(nemesis_event) == (
            "(DisruptionEvent Severity.NORMAL) period_type=begin event_id=c2561d8b-97ca-44fb-b5b1-8bcc0d437318: "
            "nemesis_name=SomeNemesis target_node=target_node"
        )

        try:
            raise KillNemesis()

        except KillNemesis as ex:
            nemesis_event.add_error([str(ex)])
            nemesis_event.duration = 15
            raise

        except Exception:  # noqa: BLE001
            pytest.fail("we shouldn't reach this code path")

    assert nemesis_event.errors_formatted == ""
    assert nemesis_event.severity == Severity.NORMAL
    assert nemesis_event.duration_formatted == "15s"
