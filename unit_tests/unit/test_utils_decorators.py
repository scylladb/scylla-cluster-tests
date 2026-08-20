import json
from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import ServiceUnavailable

from sdcm.exceptions import UnsupportedNemesis
from sdcm.provision.provisioner import InstanceConfigurationError, ProvisionUnrecoverableError
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


def _raise_instance_configuration_error():
    raise InstanceConfigurationError(
        "Failed to create instance node-x due to configuration error: "
        "[pd-standard, pd-ssd, n4-standard-16] features are not compatible for creating instance."
    )


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


def test_skip_on_capacity_issues_does_not_skip_on_instance_configuration_error():
    """An invalid instance configuration is not a capacity issue: it must fail, not skip the nemesis.

    `InstanceConfigurationError` is a `ProvisionUnrecoverableError`, so without its own clause it
    would be softened into `UnsupportedNemesis("Capacity Issue")` and the real misconfiguration
    would never surface.
    """
    with patch("sdcm.utils.decorators.check_cluster_layout", return_value=True):
        with patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock:
            with pytest.raises(InstanceConfigurationError, match="not compatible for creating instance"):
                skip_on_capacity_issues(db_cluster=MagicMock())(_raise_instance_configuration_error)()
    event_mock.return_value.publish.assert_not_called()


def test_skip_on_capacity_issues_reports_configuration_error_on_unbalanced_cluster():
    """On an unbalanced cluster it still publishes CRITICAL and re-raises, but not as a capacity issue."""
    with patch("sdcm.utils.decorators.check_cluster_layout", return_value=False):
        with patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock:
            with pytest.raises(InstanceConfigurationError):
                skip_on_capacity_issues(db_cluster=MagicMock())(_raise_instance_configuration_error)()
    assert event_mock.call_args.kwargs["severity"] == Severity.CRITICAL
    assert "invalid instance configuration" in event_mock.call_args.kwargs["message"]
    assert "capacity" not in event_mock.call_args.kwargs["message"]
    event_mock.return_value.publish.assert_called_once()


def test_critical_on_capacity_issues_reports_configuration_error_as_such():
    """A must-succeed topology change still fails critically, with the actual cause in the message."""
    with patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock:
        with pytest.raises(InstanceConfigurationError):
            critical_on_capacity_issues(_raise_instance_configuration_error)()
    assert event_mock.call_args.kwargs["severity"] == Severity.CRITICAL
    assert "invalid instance configuration" in event_mock.call_args.kwargs["message"]
    assert "capacity" not in event_mock.call_args.kwargs["message"]
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
