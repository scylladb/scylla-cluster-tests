"""
Unit tests for scan_operation_thread.py
contains 3 tests that check scan operations behavior in different conditions

test_scan_positive - positive scenario
test_scan_negative_operation_timed_out - getting operation_timed_out in scan execution
test_scan_negative_exception - getting operation_timed_out in scan execution (with and without nemesis)
"""

from importlib import reload
from threading import Event
from unittest.mock import MagicMock, patch

import pytest
from cassandra import OperationTimedOut, ReadTimeout

import sdcm.scan_operation_thread
from sdcm.scan_operation_thread import PrometheusDBStats, ScanOperationThread, ThreadParams
from sdcm.test_config import TestConfig
from sdcm.utils.decorators import Retry, retrying
from sdcm.utils.issues import SkipPerIssues

# from sdcm.utils.operations_thread import ThreadParams
from unit_tests.lib.fake_cluster import DummyDbCluster, DummyNode


def mock_retrying_decorator(*args, **kwargs):
    """Decorate by doing nothing."""
    return retrying(1, 1, allowed_exceptions=(Retry,))


with patch("sdcm.utils.decorators.retrying", mock_retrying_decorator):
    reload(sdcm.scan_operation_thread)

DEFAULT_PARAMS = {
    "termination_event": Event(),
    "user": "sla_role_name",
    "user_password": "sla_role_password",
    "duration": 10,
    "interval": 0,
    "validate_data": True,
}


class DBCluster(DummyDbCluster):
    """Dummy DB cluster that serves a fixed table list and the given CQL connection mock."""

    def __init__(self, connection_mock, nodes, params):
        super().__init__(nodes, params=params)
        self.connection_mock = connection_mock
        self.params = {"nemesis_seed": [1]}

    def get_non_system_ks_cf_list(*args, **kwargs):
        """Return two fixed keyspace.table names."""
        return ["test", "a.b"]

    def cql_connection_patient(self, *args, **kwargs):
        """Return the injected connection mock."""
        return self.connection_mock


def get_event_log_file(events):
    """Return the formatted event lines, or an empty string when there are none."""
    return events.get_formatted_event_lines() or ""


@pytest.fixture(scope="function", autouse=True)
def cleanup_event_log_file(events):
    """Clear the main event device before each test so only this test's events are read."""
    events.events_main_device.clear()


@pytest.fixture(scope="module", autouse=True)
def mock_get_partition_keys():
    """Make get_partition_keys return a fixed key list for every test in the module."""
    with patch("sdcm.scan_operation_thread.get_partition_keys", return_value=["a", "b"]):
        yield


@pytest.fixture(scope="module")
def node():
    """Dummy node with an SSH key file and no parent cluster until a test sets one."""
    return DummyNode(
        name="test_node", parent_cluster=None, ssh_login_info=dict(key_file="~/.ssh/scylla_test_id_ed25519")
    )


class MockCqlConnectionPatient(MagicMock):
    """CQL connection mock whose execute_async returns a future that calls back with one row."""

    def execute_async(*args, **kwargs):
        """Return a future that invokes the callback immediately with one row."""

        class MockFuture:
            """Single-page future that completes synchronously."""

            has_more_pages = False

            def add_callbacks(self, callback, errback):
                """Call callback with one row."""
                callback([MagicMock()])

        return MockFuture()

    events = ["Dispatching forward_request to 1 endpoints"]


class ExecuteExceptionMockCqlConnectionPatient(MockCqlConnectionPatient):
    """CQL connection mock whose execute always raises."""

    def execute(*args, **kwargs):
        """Raise a generic Exception."""
        raise Exception("Exception")


class ExecuteAsyncExceptionMockCqlConnectionPatient(MockCqlConnectionPatient):
    """CQL connection mock whose execute_async always raises."""

    def execute_async(*args, **kwargs):
        """Raise a generic Exception."""
        raise Exception("Exception")


@pytest.fixture(scope="module")
def cluster(node):
    """DB cluster with one node, a mock CQL connection and a stub tester_obj for SkipPerIssues."""
    db_cluster = DBCluster(MockCqlConnectionPatient(), [node], {})
    node.parent_cluster = db_cluster

    def tester_obj():
        """Return a stub tester whose params and attribute lookups all resolve to itself."""

        class Monitors:
            """Stub that answers any attribute with itself, except params and external_address."""

            def __getattribute__(self, item):
                """Return MagicMock params, 'test' for external_address, otherwise self."""
                if item == "params":
                    return MagicMock(scylla_version=None)
                if item not in "external_address":
                    return self
                else:
                    return "test"

            def __getitem__(self, item):
                """Return self for any key."""
                return self

        return Monitors()

    db_cluster.test_config.tester_obj = tester_obj
    return db_cluster


@pytest.mark.parametrize(
    "mode",
    [
        pytest.param("table", id="table"),
        pytest.param("partition", id="partition"),
        pytest.param("aggregate", id="aggregate"),
    ],
)
def test_scan_positive(mode, events, cluster):
    """A scan that finds all expected values emits begin and end events with NORMAL severity."""
    default_params = ThreadParams(db_cluster=cluster, ks_cf="a.b", mode=mode, **DEFAULT_PARAMS)
    with (
        patch.object(PrometheusDBStats, "__init__", return_value=None),
        patch.object(PrometheusDBStats, "query", return_value=[{"values": [[0, "1"], [1, "2"]]}]),
    ):
        ScanOperationThread(default_params)._run_next_operation()
        all_events = get_event_log_file(events)
        assert "Severity.NORMAL" in all_events[0] and "period_type=begin" in all_events[0]
        assert "Severity.NORMAL" in all_events[1] and "period_type=end" in all_events[1]
        if mode == "aggregate":
            assert "MockCqlConnectionPatient" in all_events[1]


def test_negative_prometheus_validation_error(events, cluster):
    """A Prometheus count mismatch in aggregate mode emits an ERROR end event naming the missing metric."""
    default_params = ThreadParams(db_cluster=cluster, ks_cf="a.b", mode="aggregate", **DEFAULT_PARAMS)
    with (
        patch.object(PrometheusDBStats, "__init__", return_value=None),
        patch.object(PrometheusDBStats, "query", return_value=[{"values": [[0, "1"], [1, "1"]]}]),
    ):
        ScanOperationThread(default_params)._run_next_operation()
        all_events = get_event_log_file(events)
        assert "Severity.NORMAL" in all_events[0] and "period_type=begin" in all_events[0]
        severity = "Severity.ERROR"
        if SkipPerIssues("https://github.com/scylladb/scylladb/issues/21578", TestConfig().tester_obj().params):
            severity = "Severity.WARNING"
        assert (
            severity in all_events[1]
            and "period_type=end" in all_events[1]
            and "Fullscan failed - 'mapreduce_service_requests_dispatched_to_other_nodes' was not triggered"
            in all_events[1]
        )


@pytest.mark.parametrize(
    "exception",
    [
        pytest.param(ReadTimeout("Operation timed out"), id="read-timeout"),
        pytest.param(Exception("Host has been marked down or removed"), id="host-marked-down"),
        pytest.param(OperationTimedOut("timeout"), id="operation-timed-out"),
    ],
)
@pytest.mark.parametrize(
    "mode",
    [
        pytest.param("table", id="table"),
        pytest.param("partition", id="partition"),
        pytest.param("aggregate", id="aggregate"),
    ],
)
def test_scan_negative_execution_errors(mode, exception, events, node):
    """An execution error during the scan emits an end event with WARNING severity."""
    if mode == "partition":

        class Connection(MockCqlConnectionPatient):
            """Connection whose execute_async raises the parametrized exception."""

            def execute_async(*args, **kwargs):
                """Raise the parametrized exception."""
                raise exception
    else:

        class Connection(MockCqlConnectionPatient):
            """Connection whose execute raises the parametrized exception."""

            def execute(*args, **kwargs):
                """Raise the parametrized exception."""
                raise exception

    connection = Connection()
    db_cluster = DBCluster(connection, [node], {})
    node.parent_cluster = db_cluster
    default_params = ThreadParams(
        db_cluster=db_cluster,
        ks_cf="a.b",
        mode=mode,
        full_scan_aggregates_operation_limit=60 * 30,
        full_scan_operation_limit=300,
        **DEFAULT_PARAMS,
    )
    ScanOperationThread(default_params)._run_next_operation()
    all_events = get_event_log_file(events)
    assert "Severity.NORMAL" in all_events[0] and "period_type=begin" in all_events[0]
    assert "Severity.WARNING" in all_events[1] and "period_type=end" in all_events[1]


@pytest.mark.parametrize(
    ("running_nemesis", "severity"),
    [
        pytest.param(True, "WARNING", id="nemesis-running"),
        pytest.param(False, "ERROR", id="no-nemesis"),
    ],
)
@pytest.mark.parametrize(
    ("mode", "execute_mock"),
    [
        pytest.param("partition", "execute_async", id="partition-async"),
        pytest.param("aggregate", "execute", id="aggregate-sync"),
        pytest.param("table", "execute", id="table-sync"),
    ],
)
def test_scan_negative_running_nemesis(mode, severity, running_nemesis, execute_mock, events, node):
    """A scan error emits WARNING while a nemesis runs on the node, and ERROR otherwise."""
    if running_nemesis:
        node.running_nemesis = MagicMock()
    else:
        node.running_nemesis = None
    if execute_mock == "execute_async":
        connection = ExecuteAsyncExceptionMockCqlConnectionPatient()
    else:
        connection = ExecuteExceptionMockCqlConnectionPatient()
    db_cluster = DBCluster(connection, [node], {})
    node.parent_cluster = db_cluster
    default_params = ThreadParams(db_cluster=db_cluster, ks_cf="a.b", mode=mode, **DEFAULT_PARAMS)
    ScanOperationThread(default_params)._run_next_operation()
    all_events = get_event_log_file(events)
    assert "Severity.NORMAL" in all_events[0] and "period_type=begin" in all_events[0]
    assert f"Severity.{severity}" in all_events[1] and "period_type=end" in all_events[1]
