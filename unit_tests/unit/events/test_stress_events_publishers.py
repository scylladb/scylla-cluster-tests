import itertools
import re

import pytest

from sdcm.cluster_k8s.operator_monitoring import ScyllaOperatorLogMonitoring
from sdcm.cql_stress_cassandra_stress_thread import CqlStressCassandraStressEventsPublisher
from sdcm.ndbench_thread import NdBenchStressEventsPublisher
from sdcm.scylla_bench_thread import ScyllaBenchStressEventsPublisher
from sdcm.sct_events import loaders
from sdcm.sct_events.base import publish_log_line_event
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.operator import SCYLLA_OPERATOR_EVENT_PATTERNS
from sdcm.stress_thread import CassandraStressEventsPublisher

LINE_PREFIX = "2026-10-14T10:00:00.000 "


@pytest.fixture
def run_publisher(monkeypatch):
    """Return a function that runs a publisher loop over the given lines, then stops it."""

    def run(publisher, lines):
        monkeypatch.setattr("os.path.isfile", lambda _: True)

        def follow_file(_):
            yield from lines
            publisher.stop()

        monkeypatch.setattr(publisher, "follow_file", follow_file)
        publisher.run()

    return run


@pytest.mark.parametrize(
    "publisher_class, line, patterns",
    [
        pytest.param(
            CassandraStressEventsPublisher,
            "Operation x10 on key(s) [abc]: Data returned was not validated",
            list(itertools.chain(loaders.CS_NORMAL_EVENTS_PATTERNS, loaders.CS_ERROR_EVENTS_PATTERNS)),
            id="cassandra-stress",
        ),
        pytest.param(
            ScyllaBenchStressEventsPublisher,
            "data corruption detected",
            list(
                itertools.chain(loaders.SCYLLA_BENCH_ERROR_EVENTS_PATTERNS, loaders.SCYLLA_BENCH_NORMAL_EVENTS_PATTERNS)
            ),
            id="scylla-bench",
        ),
        pytest.param(
            CqlStressCassandraStressEventsPublisher,
            "read validation error",
            loaders.CQL_STRESS_CS_ERROR_EVENTS_PATTERNS,
            id="cql-stress",
        ),
        pytest.param(
            NdBenchStressEventsPublisher, "operation FAILED", loaders.NDBENCH_ERROR_EVENTS_PATTERNS, id="ndbench"
        ),
    ],
)
def test_publisher_run_matching_line_publishes_event_per_stress_run(
    events_function_scope, run_publisher, publisher_class, line, patterns
):
    """Each stress run publishes its own event, leaving the shared pattern untouched for the other runs."""
    for event_id in ("stress-run-1", "stress-run-2"):
        run_publisher(
            publisher_class("node", "stress.log", event_id=event_id),
            [LINE_PREFIX + line, LINE_PREFIX + "nothing interesting"],
        )

    first, second = events_function_scope.published_events
    assert first["event_id"] == "stress-run-1"
    assert second["event_id"] == "stress-run-2"
    for event in (first, second):
        assert event["severity"] == "CRITICAL"
        assert event["node"] == "node"
        assert event["line"] == LINE_PREFIX + line
        assert event["line_number"] == 0
    for _, prototype in patterns:
        assert prototype.line is None


@pytest.mark.parametrize(
    "publisher_class, line",
    [
        pytest.param(
            CassandraStressEventsPublisher,
            "Operation x10 on key(s) [abc]: Data returned was not validated",
            id="cassandra-stress",
        ),
        pytest.param(ScyllaBenchStressEventsPublisher, "data corruption detected", id="scylla-bench"),
    ],
)
def test_publisher_run_critical_line_stop_test_on_failure_disabled_publishes_error(
    events_function_scope, run_publisher, publisher_class, line
):
    """A CRITICAL stress line must not fail the test when stop_test_on_failure is off."""
    publisher = publisher_class("node", "stress.log", event_id="stress-run-1", stop_test_on_failure=False)

    run_publisher(publisher, [LINE_PREFIX + line])

    (event,) = events_function_scope.published_events
    assert event["severity"] == "ERROR"
    assert event["line"] == LINE_PREFIX + line


def test_publish_log_line_event_no_match_publishes_nothing(events_function_scope):
    """A line matching no pattern produces no event."""
    patterns = [(re.compile("error"), DatabaseLogEvent.NO_SPACE_ERROR())]

    assert publish_log_line_event(patterns, LINE_PREFIX + "all fine", "node", 0) is False
    assert not events_function_scope.published_events


def test_publish_log_line_event_overlapping_patterns_publishes_first_match_only(events_function_scope):
    """Overlapping patterns yield exactly one event per line: the first one listed."""
    first, second = DatabaseLogEvent.NO_SPACE_ERROR(), DatabaseLogEvent.BAD_ALLOC()
    patterns = [(re.compile("error"), first), (re.compile("error|alloc"), second)]

    assert publish_log_line_event(patterns, LINE_PREFIX + "an error happened", "node", 0) is True
    (event,) = events_function_scope.published_events
    assert event["type"] == first.type


def test_operator_log_monitoring_check_logs_matching_line_publishes_private_copy(events_function_scope):
    """The operator monitor publishes a copy of the matching prototype, leaving the shared pattern untouched."""
    line = "I1014 10:00:00.000000       1 operator.go:1] please apply your changes to the latest version and try again"

    ScyllaOperatorLogMonitoring.check_logs([line, "I1014 10:00:01.000000       1 operator.go:1] all fine"])

    (event,) = events_function_scope.published_events
    assert event["type"] == "REAPPLY"
    assert event["severity"] == "WARNING"
    assert event["node"] == "N/A"
    assert event["line"] == line
    for _, prototype in SCYLLA_OPERATOR_EVENT_PATTERNS:
        assert prototype.line is None
