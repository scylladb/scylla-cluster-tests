import json
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import ServiceUnavailable

from argus.client.generic_result import ColumnMetadata, ResultType
from sdcm.exceptions import UnsupportedNemesis
from sdcm.provision.provisioner import ProvisionUnrecoverableError
from sdcm.sct_events import Severity
from sdcm.utils.decorators import (
    _find_hdr_tags,
    critical_on_capacity_issues,
    latency_calculator_decorator,
    skip_on_capacity_issues,
)


HDR_TAGS1 = ["foohdr1", "foohdr2"]
HDR_TAGS2 = ["barhdr1", "barhdr2"]


def test__find_hdr_tags_in_dict():
    assert _find_hdr_tags({"foo": HDR_TAGS1, "hdr_tags": HDR_TAGS2}) == HDR_TAGS2


def test__find_hdr_tags_in_object_attr():
    obj_with_hdr_tags = type("FakeStressQueue", (), {"fake_hdr_tags": HDR_TAGS1, "hdr_tags": HDR_TAGS2})
    res = _find_hdr_tags(obj_with_hdr_tags)
    assert res == HDR_TAGS2


def test__find_hdr_tags_in_tuple():
    obj_with_hdr_tags = type("FakeStressQueue", (), {"hdr_tags": HDR_TAGS1})
    params = ("foo", 5, obj_with_hdr_tags)
    res = _find_hdr_tags(params)
    assert res == HDR_TAGS1


def test__find_hdr_tags_in_tuple_of_lists():
    obj_with_hdr_tags1 = type("FakeStressQueue", (), {"hdr_tags": HDR_TAGS1})
    obj_with_hdr_tags2 = type("FakeStressQueue", (), {"hdr_tags": HDR_TAGS2})
    params = ("foo", 5, (["a", "b"], [obj_with_hdr_tags1, obj_with_hdr_tags2]))
    res = _find_hdr_tags(params)
    assert res == HDR_TAGS1


def test__find_hdr_tags_error():
    try:
        _find_hdr_tags({"no_hdr_tags": ["foo"]})
    except ValueError:
        pass
    else:
        assert False, "Expected 'ValueError'"


def _raise_stuck_vm_give_up():
    raise ProvisionUnrecoverableError("Azure VM(s) node-x stuck in provisioning, giving up after 3 recovery attempts")


def _raise_service_unavailable():
    raise ServiceUnavailable("GCE capacity issue")


def test_skip_on_capacity_issues_converts_stuck_vm_give_up_to_nemesis_skip():
    """Stuck VM recovery give-up on a balanced cluster must skip the nemesis, not fail it."""
    with patch("sdcm.utils.decorators.check_cluster_layout", return_value=True):
        with pytest.raises(UnsupportedNemesis, match="Capacity Issue"):
            skip_on_capacity_issues(db_cluster=MagicMock())(_raise_stuck_vm_give_up)()


def test_skip_on_capacity_issues_reraises_stuck_vm_give_up_on_unbalanced_cluster():
    """On an unbalanced cluster the give-up must publish a CRITICAL event AND re-raise, never return None."""
    with patch("sdcm.utils.decorators.check_cluster_layout", return_value=False):
        with patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock:
            with pytest.raises(ProvisionUnrecoverableError):
                skip_on_capacity_issues(db_cluster=MagicMock())(_raise_stuck_vm_give_up)()
    assert event_mock.call_args.kwargs["severity"] == Severity.CRITICAL
    event_mock.return_value.publish.assert_called_once()


def test_skip_on_capacity_issues_reraises_service_unavailable_on_unbalanced_cluster():
    """A GCE ServiceUnavailable on an unbalanced cluster must publish a CRITICAL event AND re-raise."""
    with patch("sdcm.utils.decorators.check_cluster_layout", return_value=False):
        with patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock:
            with pytest.raises(ServiceUnavailable):
                skip_on_capacity_issues(db_cluster=MagicMock())(_raise_service_unavailable)()
    assert event_mock.call_args.kwargs["severity"] == Severity.CRITICAL
    event_mock.return_value.publish.assert_called_once()


def test_critical_on_capacity_issues_publishes_critical_on_stuck_vm_give_up():
    """Stuck VM recovery give-up in a must-succeed topology change must raise a critical event."""
    with patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock:
        with pytest.raises(ProvisionUnrecoverableError):
            critical_on_capacity_issues(_raise_stuck_vm_give_up)()
    assert event_mock.call_args.kwargs["severity"] == Severity.CRITICAL
    event_mock.return_value.publish.assert_called_once()


def test_critical_on_capacity_issues_reraises_on_service_unavailable():
    """A GCE ServiceUnavailable in a must-succeed topology change must publish CRITICAL and re-raise."""
    with patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock:
        with pytest.raises(ServiceUnavailable):
            critical_on_capacity_issues(_raise_service_unavailable)()
    assert event_mock.call_args.kwargs["severity"] == Severity.CRITICAL
    event_mock.return_value.publish.assert_called_once()


# latency_calculator_decorator: the monitoring set is optional, see 'sdcm.utils.decorators'

HDR_SUMMARY = {"READ--fn--search": {"percentile_50": 0.24, "percentile_99": 0.47, "throughput": 6414}}
REACTOR_STALLS = {
    "DatabaseLogEvent.REACTOR_STALLED": {
        "event": "DatabaseLogEvent.REACTOR_STALLED",
        "counter": 3,
        "ms": {10: 2, 20: 1},
    }
}


class FakeDbCluster:
    nodes = []


class FakeMonitorSet:
    """Monitoring set without nodes, the same as 'sdcm.cluster.NoMonitorSet' or a 'n_monitor_nodes: 0' set."""

    nodes = []

    def __init__(self):
        self.screenshot_requests = []

    def get_grafana_screenshots(self, node, test_start_time):
        self.screenshot_requests.append((node, test_start_time))
        return ["https://cloudius-jenkins-test.s3.amazonaws.com/fake-screenshot.png"]


class FakeMonitorNode:
    name = "monitor-node-1"
    external_address = "127.0.0.1"


class FakeMonitorSetWithNode(FakeMonitorSet):
    nodes = [FakeMonitorNode()]


class FakeEventCounter:
    """Stands in for 'EventCounterContextManager' which needs the default events registry."""

    def __init__(self, *_, **__):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    @staticmethod
    def get_stats():
        return REACTOR_STALLS


class FakeLatencyTester:
    """Provides the 'latency_calculator_decorator' dependencies only, without the ClusterTester machinery."""

    def __init__(self, latency_results_file, monitors):
        self.db_cluster = FakeDbCluster()
        self.monitors = monitors
        self.params = {"use_hdrhistogram": True}
        self.latency_results_file = str(latency_results_file)
        self.test_config = MagicMock()

    def get_hdrhistogram_by_interval(self, **_):
        return [HDR_SUMMARY]

    def get_hdrhistogram(self, **_):
        return HDR_SUMMARY


def _run_decorated_search(tester):
    @latency_calculator_decorator(workload_type="read", legend="fts search", cycle_name="fts_search", row_name="row-1")
    def _do_search(_self):
        return {"hdr_tags": ["fn--search"]}

    # NOTE: the decorator resolves the tester from the first positional argument
    return _do_search(tester)


def test_latency_calculator_decorator_without_monitoring_set(tmp_path):
    tester = FakeLatencyTester(latency_results_file=tmp_path / "latency_results.json", monitors=FakeMonitorSet())

    with (
        patch("sdcm.tester.ClusterTester", FakeLatencyTester),
        patch("sdcm.utils.decorators.EventCounterContextManager", FakeEventCounter),
        patch("sdcm.utils.latency.collect_latency") as collect_latency_mock,
        patch("sdcm.utils.decorators.send_result_to_argus") as send_to_argus_mock,
    ):
        res = _run_decorated_search(tester)

    assert res == {"hdr_tags": ["fn--search"]}
    collect_latency_mock.assert_not_called()
    assert not tester.monitors.screenshot_requests
    # NOTE: the HDR results, the reactor stalls and the Argus reporting are the point of the decorator,
    #       they must survive
    send_to_argus_mock.assert_called_once()
    cycle = send_to_argus_mock.call_args.kwargs["result"]
    assert cycle["hdr_summary"] == HDR_SUMMARY
    assert cycle["cycle_hdr_throughput"] == 6414
    assert cycle["reactor_stalls_stats"] == REACTOR_STALLS
    assert cycle["screenshots"] == []

    latency_results = json.loads((tmp_path / "latency_results.json").read_text(encoding="utf-8"))
    assert latency_results["fts_search"]["legend"] == "fts search"
    assert len(latency_results["fts_search"]["cycles"]) == 1


def test_latency_calculator_decorator_with_monitoring_set(tmp_path):
    tester = FakeLatencyTester(
        latency_results_file=tmp_path / "latency_results.json", monitors=FakeMonitorSetWithNode()
    )

    with (
        patch("sdcm.tester.ClusterTester", FakeLatencyTester),
        patch("sdcm.utils.decorators.EventCounterContextManager", FakeEventCounter),
        patch("sdcm.utils.latency.collect_latency", return_value={"Scylla P99_read - node-1": 1.5}) as collect_mock,
        patch("sdcm.utils.decorators.send_result_to_argus") as send_to_argus_mock,
    ):
        _run_decorated_search(tester)

    collect_mock.assert_called_once()
    assert collect_mock.call_args.args[0] is FakeMonitorSetWithNode.nodes[0]
    cycle = send_to_argus_mock.call_args.kwargs["result"]
    assert cycle["Scylla P99_read - node-1"] == 1.5
    assert len(cycle["screenshots"]) == 1
    assert tester.monitors.screenshot_requests


# error_thresholds override / extra_columns / extra_values -- used by fts_test.py so that a plan-
# supplied expected latency becomes the table's validation rule and per-row metadata columns,
# without requiring every caller to route through 'latency_decorator_error_thresholds'.


def test_error_thresholds_override_bypasses_test_params(tmp_path):
    tester = FakeLatencyTester(latency_results_file=tmp_path / "latency_results.json", monitors=FakeMonitorSet())
    tester.params["latency_decorator_error_thresholds"] = {"read": {"default": {"P99 read": {"fixed_limit": 999}}}}
    override = {"read": {"default": {"P99 read": {"fixed_limit": 50}}}}

    @latency_calculator_decorator(
        workload_type="read", legend="fts search", cycle_name="fts_search", row_name="row-1", error_thresholds=override
    )
    def _do_search(_self):
        return {"hdr_tags": ["fn--search"]}

    with (
        patch("sdcm.tester.ClusterTester", FakeLatencyTester),
        patch("sdcm.utils.decorators.EventCounterContextManager", FakeEventCounter),
        patch("sdcm.utils.latency.collect_latency"),
        patch("sdcm.utils.decorators.send_result_to_argus") as send_to_argus_mock,
    ):
        _do_search(tester)

    assert send_to_argus_mock.call_args.kwargs["error_thresholds"] == override


def test_error_thresholds_defaults_to_test_params_when_not_overridden(tmp_path):
    tester = FakeLatencyTester(latency_results_file=tmp_path / "latency_results.json", monitors=FakeMonitorSet())
    configured = {"read": {"default": {"P99 read": {"fixed_limit": 10}}}}
    tester.params["latency_decorator_error_thresholds"] = configured

    with (
        patch("sdcm.tester.ClusterTester", FakeLatencyTester),
        patch("sdcm.utils.decorators.EventCounterContextManager", FakeEventCounter),
        patch("sdcm.utils.latency.collect_latency"),
        patch("sdcm.utils.decorators.send_result_to_argus") as send_to_argus_mock,
    ):
        _run_decorated_search(tester)

    assert send_to_argus_mock.call_args.kwargs["error_thresholds"] == configured


def test_extra_columns_and_extra_values_are_forwarded(tmp_path):
    tester = FakeLatencyTester(latency_results_file=tmp_path / "latency_results.json", monitors=FakeMonitorSet())
    extra_columns = [ColumnMetadata(name="query_example", unit="", type=ResultType.TEXT)]

    @latency_calculator_decorator(
        workload_type="read",
        legend="fts search",
        cycle_name="fts_search",
        row_name="row-1",
        extra_columns=extra_columns,
    )
    def _do_search(_self):
        return {"hdr_tags": ["fn--search"], "extra_values": {"query_example": "hello world"}}

    with (
        patch("sdcm.tester.ClusterTester", FakeLatencyTester),
        patch("sdcm.utils.decorators.EventCounterContextManager", FakeEventCounter),
        patch("sdcm.utils.latency.collect_latency"),
        patch("sdcm.utils.decorators.send_result_to_argus") as send_to_argus_mock,
    ):
        _do_search(tester)

    assert send_to_argus_mock.call_args.kwargs["extra_columns"] == extra_columns
    assert send_to_argus_mock.call_args.kwargs["extra_values"] == {"query_example": "hello world"}


def test_extra_columns_defaults_to_none_for_existing_callers(tmp_path):
    """Callers that do not pass 'extra_columns' must not change the call to 'send_result_to_argus'."""
    tester = FakeLatencyTester(latency_results_file=tmp_path / "latency_results.json", monitors=FakeMonitorSet())

    with (
        patch("sdcm.tester.ClusterTester", FakeLatencyTester),
        patch("sdcm.utils.decorators.EventCounterContextManager", FakeEventCounter),
        patch("sdcm.utils.latency.collect_latency"),
        patch("sdcm.utils.decorators.send_result_to_argus") as send_to_argus_mock,
    ):
        _run_decorated_search(tester)

    assert send_to_argus_mock.call_args.kwargs["extra_columns"] is None
    assert send_to_argus_mock.call_args.kwargs["extra_values"] is None
