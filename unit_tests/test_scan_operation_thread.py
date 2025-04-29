"""
Unit tests for scan_operation_thread.py
contains 3 tests that check scan operations behavior in different conditions

test_scan_positive - positive scenario
test_scan_negative_operation_timed_out - getting operation_timed_out in scan execution
test_scan_negative_exception - getting operation_timed_out in scan execution (with and without nemesis)
"""
from pathlib import Path
import os
from threading import Event
from importlib import reload
from unittest.mock import MagicMock, patch
import pytest
from cassandra import OperationTimedOut, ReadTimeout

# from sdcm.utils.operations_thread import ThreadParams
from unit_tests.test_cluster import DummyDbCluster, DummyNode
from sdcm.utils.decorators import retrying, Retry
from sdcm.utils.issues import SkipPerIssues
from sdcm.test_config import TestConfig
import sdcm.scan_operation_thread
from sdcm.scan_operation_thread import ScanOperationThread, ThreadParams, PrometheusDBStats


def mock_retrying_decorator(*args, **kwargs):
    """Decorate by doing nothing."""
    return retrying(1, 1, allowed_exceptions=(Retry, ))


with patch("sdcm.utils.decorators.retrying", mock_retrying_decorator):
    reload(sdcm.scan_operation_thread)

DEFAULT_PARAMS = {
    "termination_event": Event(),
    "user": "sla_role_name",
    "user_password": "sla_role_password",
    "duration": 10,
    "interval": 0,
    "validate_data": True
}


class DBCluster(DummyDbCluster):

    def __init__(self, connection_mock, nodes, params):
        super().__init__(nodes, params=params)
        self.connection_mock = connection_mock
        self.params = {"nemesis_seed": 1}

    def get_non_system_ks_cf_list(*args, **kwargs):

        return ["test", "a.b"]

    def cql_connection_patient(self, *args, **kwargs):

        return self.connection_mock


def get_event_log_file(events):
    if (log_file := Path(events.temp_dir, "events_log", "events.log")).exists():
        return log_file.read_text(encoding="utf-8").rstrip().split("\n")
    return ""


@pytest.fixture(scope="function", autouse=True)
def cleanup_event_log_file(events):
    with open(os.path.join(events.temp_dir, "events_log", "events.log"), "r+", encoding="utf-8") as file:
        file.truncate(0)


@pytest.fixture(scope="module", autouse=True)
def mock_get_partition_keys():
    with patch("sdcm.scan_operation_thread.get_partition_keys"):
        yield


@pytest.fixture(scope="module")
def node():
    return DummyNode(name="test_node",
                     parent_cluster=None,
                     ssh_login_info=dict(key_file="~/.ssh/scylla-test"))


class MockCqlConnectionPatient(MagicMock):
    def execute_async(*args, **kwargs):

        class MockFuture:

            has_more_pages = False

            def add_callbacks(self, callback, errback):

                callback([MagicMock()])
        return MockFuture()

    events = ["Dispatching forward_request to 1 endpoints"]


@pytest.fixture(scope="module", name="cluster")
def new_cluster(node):
    db_cluster = DBCluster(MockCqlConnectionPatient(), [node], {})
    node.parent_cluster = db_cluster

    def tester_obj():
        class Monitors:
            def __getattribute__(self, item):
                if item not in "external_address":
                    return self
                else:
                    return "test"

            def __getitem__(self, item):
                return self
        return Monitors()

    db_cluster.test_config.tester_obj = tester_obj
    return db_cluster


@pytest.mark.parametrize("mode", ["table", "partition", "aggregate"])
def test_scan_positive(mode, events, cluster):
    default_params = ThreadParams(
        db_cluster=cluster,
        ks_cf="a.b",
        mode=mode,
        **DEFAULT_PARAMS
    )
    with patch.object(PrometheusDBStats, "__init__", return_value=None):
        with patch.object(PrometheusDBStats, "query", return_value=[{"values": [[0, "1"], [1, "2"]]}]):
            with events.wait_for_n_events(events.get_events_logger(), count=2, timeout=10):
                ScanOperationThread(default_params)._run_next_operation()
            all_events = get_event_log_file(events)
            assert "Severity.NORMAL" in all_events[0] and "period_type=begin" in all_events[0]
            assert "Severity.NORMAL" in all_events[1] and "period_type=end" in all_events[1]
            if mode == "aggregate":
                assert "MockCqlConnectionPatient" in all_events[1]


def test_negative_prometheus_validation_error(events, cluster):
    default_params = ThreadParams(
        db_cluster=cluster,
        ks_cf="a.b",
        mode="aggregate",
        **DEFAULT_PARAMS
    )
    with patch.object(PrometheusDBStats, "__init__", return_value=None):
        with patch.object(PrometheusDBStats, "query", return_value=[{"values": [[0, "1"], [1, "1"]]}]):
            with events.wait_for_n_events(events.get_events_logger(), count=2, timeout=2):
                ScanOperationThread(default_params)._run_next_operation()
            all_events = get_event_log_file(events)
            assert "Severity.NORMAL" in all_events[0] and "period_type=begin" in all_events[0]
            severity = "Severity.ERROR"
            if SkipPerIssues("https://github.com/scylladb/scylladb/issues/21578", TestConfig().tester_obj().params):
                severity = "Severity.WARNING"
            assert severity in all_events[1] and "period_type=end" in all_events[
                1] and "Fullscan failed - 'mapreduce_service_requests_dispatched_to_other_nodes' was not triggered" in all_events[1]


@pytest.mark.parametrize("exception", [ReadTimeout("Operation timed out"), Exception("Host has been marked down or removed"), OperationTimedOut("timeout")])
@pytest.mark.parametrize("mode", ["table", "partition", "aggregate"])
def test_scan_negative_execution_errors(mode, exception, events, node):

    if mode == "partition":
        class Connection(MockCqlConnectionPatient):
            def execute_async(*args, **kwargs):

                raise exception
    else:
        class Connection(MockCqlConnectionPatient):
            def execute(*args, **kwargs):

                raise exception
    connection = Connection()
    db_cluster = DBCluster(connection, [node], {})
    node.parent_cluster = db_cluster
    default_params = ThreadParams(
        db_cluster=db_cluster,
        ks_cf="a.b",
        mode=mode,
        full_scan_aggregates_operation_limit=60*30,
        full_scan_operation_limit=300,
        **DEFAULT_PARAMS
    )
    with events.wait_for_n_events(events.get_events_logger(), count=2, timeout=10):
        ScanOperationThread(default_params)._run_next_operation()
    all_events = get_event_log_file(events)
    assert "Severity.NORMAL" in all_events[0] and "period_type=begin" in all_events[0]
    assert "Severity.WARNING" in all_events[1] and "period_type=end" in all_events[1]


class ExecuteExceptionMockCqlConnectionPatient(MockCqlConnectionPatient):
    def execute(*args, **kwargs):

        raise Exception("Exception")


class ExecuteAsyncExceptionMockCqlConnectionPatient(MockCqlConnectionPatient):
    def execute_async(*args, **kwargs):

        raise Exception("Exception")


@pytest.mark.parametrize(("running_nemesis", "severity"), [[True, "WARNING"], [False, "ERROR"]])
@pytest.mark.parametrize(("mode", "execute_mock"), [
    ["partition", "execute_async"],
    ["aggregate", "execute"],
    ["table", "execute"]])
def test_scan_negative_running_nemesis(mode, severity, running_nemesis, execute_mock, events, node):

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
    default_params = ThreadParams(
        db_cluster=db_cluster,
        ks_cf="a.b",
        mode=mode,
        ** DEFAULT_PARAMS
    )
    with events.wait_for_n_events(events.get_events_logger(), count=2, timeout=10):
        ScanOperationThread(default_params)._run_next_operation()
    all_events = get_event_log_file(events)
    assert "Severity.NORMAL" in all_events[0] and "period_type=begin" in all_events[0]
    assert f"Severity.{severity}" in all_events[1] and "period_type=end" in all_events[1]
