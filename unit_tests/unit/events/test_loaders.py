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

import pickle

from itertools import chain

import pytest
from invoke.runners import Result

from sdcm.sct_events import Severity
from sdcm.sct_events.base import LogEvent
from sdcm.sct_events.database import DatabaseLogEvent, SYSTEM_ERROR_EVENTS
from sdcm.sct_events.loaders import (
    GeminiStressEvent,
    CassandraStressEvent,
    ScyllaBenchEvent,
    YcsbStressEvent,
    CassandraHarryEvent,
    NdBenchStressEvent,
    CDCReaderStressEvent,
    KclStressEvent,
    CassandraStressLogEvent,
    ScyllaBenchLogEvent,
    GeminiStressLogEvent,
    CS_ERROR_EVENTS,
    CS_NORMAL_EVENTS,
    SCYLLA_BENCH_ERROR_EVENTS,
    CS_ERROR_EVENTS_PATTERNS,
    CS_NORMAL_EVENTS_PATTERNS,
    SCYLLA_BENCH_NORMAL_EVENTS_PATTERNS,
    SCYLLA_BENCH_ERROR_EVENTS_PATTERNS,
    SCYLLA_BENCH_NORMAL_EVENTS,
)
from sdcm.sct_events.base import find_log_event

EVENT_ID = "14f35b64-2fcc-4b6e-a09d-4aeaf4faa543"
BEGIN_TIMESTAMP = 1623596860.1202102

# Stress events using the continuous event pattern with a `node=...\nstress_cmd=...` message.
CONTINUOUS_STRESS_EVENTS = [
    pytest.param(CassandraStressEvent, id="cassandra_stress"),
    pytest.param(ScyllaBenchEvent, id="scylla_bench"),
    pytest.param(YcsbStressEvent, id="ycsb"),
    pytest.param(CassandraHarryEvent, id="cassandra_harry"),
]
STRESS_BEGIN_MESSAGE = "node=node\nstress_cmd=stress_cmd"

CS_LOG_PATTERNS = list(chain(CS_NORMAL_EVENTS_PATTERNS, CS_ERROR_EVENTS_PATTERNS))
SCYLLA_BENCH_LOG_PATTERNS = list(chain(SCYLLA_BENCH_NORMAL_EVENTS_PATTERNS, SCYLLA_BENCH_ERROR_EVENTS_PATTERNS))


@pytest.fixture
def continuous_stress_event(event_class):
    """Unpublished continuous stress event of the parametrized class with a fixed event_id."""
    event = event_class(node="node", stress_cmd="stress_cmd", log_file_name="log_file_name", publish_event=False)
    event.event_id = EVENT_ID
    return event


def assert_begin_event(begin_event, expected_message):
    """Assert a continuous stress begin event renders its message, keeps its timestamp and survives pickling."""
    begin_event.event_timestamp = begin_event.begin_timestamp = BEGIN_TIMESTAMP
    assert str(begin_event) == (
        f"({type(begin_event).__name__} Severity.NORMAL) period_type=begin event_id={EVENT_ID}: {expected_message}"
    )
    assert begin_event.event_timestamp == BEGIN_TIMESTAMP
    assert begin_event.timestamp == BEGIN_TIMESTAMP
    assert begin_event == pickle.loads(pickle.dumps(begin_event))


def test_gemini_continuous_event_with_error():
    """Gemini end event carries ERROR severity and the error list, so failed runs are reported."""
    gemini_stress_event = GeminiStressEvent(node="node", cmd="gemini_cmd", publish_event=False)
    gemini_stress_event.event_id = EVENT_ID
    assert_begin_event(gemini_stress_event.begin_event(), "node=node gemini_cmd=gemini_cmd")

    gemini_stress_event.add_error(errors=["error1", "error2"])
    gemini_stress_event.severity = Severity.ERROR

    gemini_stress_event.end_event()
    gemini_stress_event.event_timestamp = gemini_stress_event.end_timestamp = 1623597850.6610544
    assert str(gemini_stress_event) == (
        "(GeminiStressEvent Severity.ERROR) period_type=end "
        f"event_id={EVENT_ID} duration=16m30s: node=node gemini_cmd=gemini_cmd"
        "\nerrors=['error1', 'error2']"
    )
    assert gemini_stress_event == pickle.loads(pickle.dumps(gemini_stress_event))


def test_gemini_continuous_event_with_result():
    """add_result() keeps the exit code, trimmed stdout tail and stderr as an error in the end event."""
    gemini_stress_event = GeminiStressEvent(node="node", cmd="cat", publish_event=False)
    gemini_stress_event.event_id = EVENT_ID
    assert_begin_event(gemini_stress_event.begin_event(), "node=node gemini_cmd=cat")

    gemini_stress_event.add_result(
        result=Result(stdout="  \n\nline1\n  line2  \nline3\n  ", stderr="\terr\t", exited=1)
    )

    result = "Exit code: 1\nCommand output: ['  line2  ', 'line3']\n"
    gemini_stress_event.end_event()
    gemini_stress_event.event_timestamp = gemini_stress_event.end_timestamp = 1623599860.1202102
    assert gemini_stress_event.result == result
    assert str(gemini_stress_event) == (
        "(GeminiStressEvent Severity.NORMAL) period_type=end "
        f"event_id={EVENT_ID} duration=50m0s: node=node gemini_cmd=cat\n"
        "result=Exit code: 1\nCommand output: ['  line2  ', 'line3']\n\n"
        "errors=['Command error: \\terr\\t\\n']"
    )

    assert gemini_stress_event == pickle.loads(pickle.dumps(gemini_stress_event))


def test_gemini_continuous_event_without_result():
    """Gemini end event without a result renders only node/cmd and the run duration."""
    gemini_stress_event = GeminiStressEvent(node="node", cmd="cat", publish_event=False)
    gemini_stress_event.event_id = EVENT_ID
    assert_begin_event(gemini_stress_event.begin_event(), "node=node gemini_cmd=cat")

    gemini_stress_event.end_event()
    gemini_stress_event.event_timestamp = gemini_stress_event.end_timestamp = 1623696860.1202102
    assert gemini_stress_event.result == ""
    assert str(gemini_stress_event) == (
        "(GeminiStressEvent Severity.NORMAL) period_type=end "
        f"event_id={EVENT_ID} duration=1d3h46m40s: node=node gemini_cmd=cat"
    )

    assert gemini_stress_event == pickle.loads(pickle.dumps(gemini_stress_event))


def test_cassandra_stress_event_error_after_end_accumulates_errors():
    """A one-time c-s error event after the end event carries all errors so far, raising severity to CRITICAL."""
    cs_event = CassandraStressEvent(
        node="node", stress_cmd="stress_cmd", log_file_name="log_file_name", publish_event=False
    )
    cs_event.event_id = EVENT_ID
    cs_event.begin_event()
    cs_event.add_error(errors=["error1", "error2"])
    cs_event.severity = Severity.ERROR
    cs_event.end_event()
    cs_event.begin_timestamp = cs_event.end_timestamp = BEGIN_TIMESTAMP

    cs_event.add_error(["One more error"])
    cs_event.severity = Severity.CRITICAL
    cs_event.event_error()
    cs_event.event_timestamp = BEGIN_TIMESTAMP

    assert str(cs_event) == (
        "(CassandraStressEvent Severity.CRITICAL) period_type=one-time "
        f"event_id={EVENT_ID} duration=0s: node=node\n"
        "stress_cmd=stress_cmd\nerrors:\n\nerror1\nerror2\nOne more error"
    )
    assert cs_event.log_file_name == "log_file_name"
    assert cs_event.timestamp == BEGIN_TIMESTAMP
    assert cs_event == pickle.loads(pickle.dumps(cs_event))


@pytest.mark.parametrize("event_class", CONTINUOUS_STRESS_EVENTS)
def test_continuous_stress_event_without_error_renders_normal_end(continuous_stress_event):
    """A stress run ending cleanly emits a NORMAL end event with the run duration and survives pickling."""
    assert_begin_event(continuous_stress_event.begin_event(), STRESS_BEGIN_MESSAGE)

    end_event_timestamp = 1623596861.1202102
    continuous_stress_event.end_event()
    continuous_stress_event.event_timestamp = continuous_stress_event.end_timestamp = end_event_timestamp
    assert str(continuous_stress_event) == (
        f"({type(continuous_stress_event).__name__} Severity.NORMAL) period_type=end "
        f"event_id={EVENT_ID} duration=1s: node=node\nstress_cmd=stress_cmd"
    )
    assert continuous_stress_event.event_timestamp == end_event_timestamp
    assert continuous_stress_event.timestamp == end_event_timestamp
    assert continuous_stress_event == pickle.loads(pickle.dumps(continuous_stress_event))


@pytest.mark.parametrize("event_class", CONTINUOUS_STRESS_EVENTS)
def test_continuous_stress_event_with_error_renders_error_end(continuous_stress_event):
    """A failed stress run emits an ERROR end event listing the errors, so the failure reaches the report."""
    assert_begin_event(continuous_stress_event.begin_event(), STRESS_BEGIN_MESSAGE)

    continuous_stress_event.severity = Severity.ERROR
    continuous_stress_event.add_error(["Stress command completed with bad status 1"])

    continuous_stress_event.end_event()
    continuous_stress_event.end_timestamp = continuous_stress_event.event_timestamp = 1623596960.1202102
    assert str(continuous_stress_event) == (
        f"({type(continuous_stress_event).__name__} Severity.ERROR) period_type=end "
        f"event_id={EVENT_ID} duration=1m40s: "
        "node=node\nstress_cmd=stress_cmd\nerrors:"
        "\n\nStress command completed with bad status 1"
    )
    assert continuous_stress_event == pickle.loads(pickle.dumps(continuous_stress_event))


@pytest.mark.parametrize(
    ("event_class", "expected_subevents"),
    [
        pytest.param(CDCReaderStressEvent, {"failure", "error", "start", "finish"}, id="cdc_reader"),
        pytest.param(NdBenchStressEvent, {"failure", "error", "start", "finish"}, id="ndbench"),
        pytest.param(KclStressEvent, {"failure", "start", "finish"}, id="kcl"),
    ],
)
def test_stress_subevents(event_class, expected_subevents):
    """add_stress_subevents() creates only the subevents given a severity, plus the default start/finish."""
    known_subevents = ("failure", "error", "warning", "timeout", "start", "finish")
    assert {name for name in known_subevents if hasattr(event_class, name)} == expected_subevents


@pytest.mark.parametrize(
    ("event_class", "subevent", "severity", "event_id"),
    [
        pytest.param(
            CDCReaderStressEvent, "start", Severity.NORMAL, "aed3946d-33a8-4f68-b56c-1d09f71f5da9", id="cdc_reader"
        ),
        pytest.param(NdBenchStressEvent, "error", Severity.ERROR, "a07b48fa-2706-465b-b139-698d35909cfa", id="ndbench"),
        pytest.param(KclStressEvent, "failure", Severity.ERROR, "1acd4202-3a38-4b0d-9464-62f4825ee148", id="kcl"),
    ],
)
def test_stress_event_without_errors(event_class, subevent, severity, event_id):
    """A subevent built from node/stress_cmd/log_file_name keeps them, has the subevent severity and pickles."""
    event = getattr(event_class, subevent)(node=[], stress_cmd="c-s", log_file_name="1.log")
    assert event.severity == severity
    assert event.node == "[]"
    assert event.stress_cmd == "c-s"
    assert event.log_file_name == "1.log"
    assert not event.errors
    event.event_id = event_id
    assert str(event) == (
        f"({event_class.__name__} {severity}) period_type=not-set "
        f"event_id={event_id}: type={subevent} node=[]\nstress_cmd=c-s"
    )
    assert event == pickle.loads(pickle.dumps(event))


@pytest.mark.parametrize(
    ("event_class", "severity", "event_id"),
    [
        pytest.param(CDCReaderStressEvent, Severity.CRITICAL, "3c5deb9e-7a67-49ee-9295-c7e986b015a9", id="cdc_reader"),
        pytest.param(NdBenchStressEvent, Severity.CRITICAL, "e45b347e-c395-4583-9f19-6e1fcdf31fab", id="ndbench"),
        pytest.param(KclStressEvent, Severity.ERROR, "d169ca02-c119-49f2-9eb7-23f152098cb7", id="kcl"),
    ],
)
def test_stress_event_with_errors(event_class, severity, event_id):
    """A failure subevent renders its errors and carries the class-specific failure severity."""
    event = event_class.failure(node="node1", errors=["e1", "e2"])
    assert event.severity == severity
    assert event.node == "node1"
    assert event.stress_cmd is None
    assert event.log_file_name is None
    assert event.errors == ["e1", "e2"]
    event.event_id = event_id
    assert str(event) == (
        f"({event_class.__name__} {severity}) period_type=not-set "
        f"event_id={event_id}: type=failure node=node1\nerrors:\n\ne1\ne2"
    )
    assert event == pickle.loads(pickle.dumps(event))


# --- stress tool log events ---


@pytest.mark.parametrize(
    ("log_event_class", "listed_events"),
    [
        pytest.param(CassandraStressLogEvent, list(chain(CS_NORMAL_EVENTS, CS_ERROR_EVENTS)), id="cassandra_stress"),
        pytest.param(
            ScyllaBenchLogEvent, list(chain(SCYLLA_BENCH_NORMAL_EVENTS, SCYLLA_BENCH_ERROR_EVENTS)), id="scylla_bench"
        ),
        pytest.param(DatabaseLogEvent, SYSTEM_ERROR_EVENTS, id="database"),
    ],
)
def test_log_event_subevents_all_listed(log_event_class, listed_events):
    """Every subevent of a log event is in the event lists log parsing scans, so each one can be raised.

    Since python3.14 `__annotate_func__` also shows up in dir() and is excluded.
    """
    assert set(dir(log_event_class)) - set(dir(LogEvent)) - {"__annotate_func__"} == {ev.type for ev in listed_events}


@pytest.mark.parametrize(
    ("patterns", "line", "expected_type", "expected_severity"),
    [
        pytest.param(
            CS_LOG_PATTERNS,
            "java.io.IOException: Operation x10 on key(s) [334f37384f4d32303430]: Error executing: "
            "(OverloadedException): Queried host (10.0.3.167/10.0.3.167:9042) was overloaded: Too many "
            "in flight hints: 10490670",
            "TooManyHintsInFlight",
            Severity.ERROR,
            id="cs_too_many_in_flight_hints",
        ),
        pytest.param(
            CS_LOG_PATTERNS,
            "java.io.IOException: Operation x10 on key(s) [334f37384f4d32303430]: Error executing: "
            "(OverloadedException): Queried host (10.0.3.167/10.0.3.167:9042) was overloaded: Too many "
            "hints in flight: 10490670",
            "TooManyHintsInFlight",
            Severity.ERROR,
            id="cs_too_many_hints_in_flight",
        ),
        pytest.param(
            CS_LOG_PATTERNS,
            "java.io.IOException: Operation x10 on key(s) [334f37384f4d32303430]: Error executing: "
            "(OverloadedException): Queried host (10.0.3.167/10.0.3.167:9042) ",
            "OperationOnKey",
            Severity.CRITICAL,
            id="cs_operation_on_key",
        ),
        pytest.param(
            CS_LOG_PATTERNS,
            "java.io.IOException: Connection reset by peer",
            "IOException",
            Severity.ERROR,
            id="cs_io_exception",
        ),
        pytest.param(
            CS_LOG_PATTERNS,
            "03:56:37.572 [cluster1-nio-worker-4] DEBUG com.datastax.driver.core.Connection - Connection["
            "/10.0.3.121:9042-11, inFlight=5, closed=false] Response received on stream 17856 but no "
            "handler set anymore (either the request has timed out or it was closed due to another error). "
            "Received message is ERROR UNAVAILABLE: Cannot achieve consistency level for cl QUORUM. "
            "Requires 2, alive 1",
            "ConsistencyError",
            Severity.ERROR,
            id="cs_consistency_error",
        ),
        pytest.param(
            CS_LOG_PATTERNS,
            "com.datastax.driver.core.Cluster - ===== Using optimized driver!!! =====",
            "ShardAwareDriver",
            Severity.NORMAL,
            id="cs_shard_aware_driver",
        ),
        pytest.param(
            SCYLLA_BENCH_LOG_PATTERNS,
            "[2025-11-01 04:15:03.653] Using provided rack name 'RACK0' for RackAwareRoundRobinPolicy",
            "RackAwarePolicy",
            Severity.NORMAL,
            id="sb_rack_aware_policy",
        ),
    ],
)
def test_stress_log_line_first_matching_pattern_yields_event(patterns, line, expected_type, expected_severity):
    """The first pattern matching a stress-tool log line yields the event type and severity the line maps to."""
    prototype = find_log_event(patterns, line)
    assert prototype is not None, f"Event is not recognized in the line {line}"

    # the events in the pattern lists are module-level prototypes, mutating them leaks across tests
    event = prototype.clone()
    event.add_info(node="self.node", line=line, line_number=1).dont_publish()
    assert event.type == expected_type
    assert event.severity == expected_severity


def test_gemini_log_json_line():
    """A gemini JSON log line is parsed into timestamp, message and logger name, ready to publish."""
    event = GeminiStressLogEvent.GeminiEvent()
    event.add_info(
        node="node1",
        line='{"L":"INFO","T":"2020-06-09T03:40:39.349Z","N":"pump","M":"Test run stopped. Exiting."}',
        line_number=1,
    )
    assert event.timestamp == 1591674039.349
    assert event.severity == Severity.NORMAL
    assert event.node == "node1"
    assert event.line == 'Test run stopped. Exiting. (N="pump")'
    assert event.line_number == 1
    assert event.backtrace is None
    assert event.raw_backtrace is None
    assert event._ready_to_publish
    event.event_id = "3eaf9cb2-b54b-43f5-8472-d0fbf5c25f72"
    assert str(event) == (
        "(GeminiStressLogEvent Severity.NORMAL) period_type=one-time event_id=3eaf9cb2-b54b-43f5-8472-d0fbf5c25f72:"
        ' type=GeminiEvent line_number=1 node=node1\nline=Test run stopped. Exiting. (N="pump")'
    )
    assert event == pickle.loads(pickle.dumps(event))


def test_gemini_log_non_json_line():
    """A non-JSON gemini log line leaves the event CRITICAL and unpopulated, so it is not published as parsed."""
    event = GeminiStressLogEvent.GeminiEvent()
    event_timestamp = event.event_timestamp
    event.add_info(node="node1", line="1961-04-12T06:07:00+00:00 Poyekhalee!", line_number=1)
    assert event.event_timestamp == event_timestamp
    assert event.timestamp == event_timestamp
    assert event.severity == Severity.CRITICAL
    assert event.node is None
    assert event.line is None
    assert event.line_number == 0
    assert event.backtrace is None
    assert event.raw_backtrace is None
    assert not event._ready_to_publish
    event.event_id = "3ce0cdeb-0866-40ce-9a20-25ea3ae08be2"
    assert str(event) == (
        "(GeminiStressLogEvent Severity.CRITICAL) period_type=one-time "
        "event_id=3ce0cdeb-0866-40ce-9a20-25ea3ae08be2: type=GeminiEvent line_number=0 node=None"
    )
    assert event == pickle.loads(pickle.dumps(event))
