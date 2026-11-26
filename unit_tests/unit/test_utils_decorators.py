import json
from unittest.mock import ANY, MagicMock, patch

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

# latency_calculator_decorator: the monitoring set is optional, see 'sdcm.utils.decorators'

HDR_SUMMARY = {"READ--fn--search": {"percentile_50": 0.24, "percentile_99": 0.47, "throughput": 6414}}
REACTOR_STALLS = {
    "DatabaseLogEvent.REACTOR_STALLED": {
        "event": "DatabaseLogEvent.REACTOR_STALLED",
        "counter": 3,
        "ms": {10: 2, 20: 1},
    }
}


def raise_stuck_vm_give_up():
    """Raise the ProvisionUnrecoverableError of a stuck VM recovery that gave up."""
    raise ProvisionUnrecoverableError("Azure VM(s) node-x stuck in provisioning, giving up after 3 recovery attempts")


def raise_service_unavailable():
    """Raise the GCE ServiceUnavailable error of a capacity issue."""
    raise ServiceUnavailable("GCE capacity issue")


def raise_instance_configuration_error():
    """Raise the InstanceConfigurationError of incompatible disk and machine types."""
    raise InstanceConfigurationError(
        "Failed to create instance node-x due to configuration error: "
        "[pd-standard, pd-ssd, n4-standard-16] features are not compatible for creating instance."
    )


class FakeDbCluster:
    """Cluster without nodes for the latency decorator."""

    nodes = []


class FakeMonitorSet:
    """Monitoring set without nodes, the same as 'sdcm.cluster.NoMonitorSet' or a 'n_monitor_nodes: 0' set."""

    nodes = []

    def __init__(self):
        self.screenshot_requests = []

    def get_grafana_screenshots(self, node, test_start_time):
        """Record the screenshot request and return one screenshot URL."""
        self.screenshot_requests.append((node, test_start_time))
        return ["https://cloudius-jenkins-test.s3.amazonaws.com/fake-screenshot.png"]


class FakeMonitorNode:
    """Monitor node with the name and address the decorator reads."""

    name = "monitor-node-1"
    external_address = "127.0.0.1"


class FakeMonitorSetWithNode(FakeMonitorSet):
    """Monitoring set with one monitor node."""

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
        """Return the reactor stalls that the decorator reports."""
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
        """Return the HDR summary as a one-interval list."""
        return [HDR_SUMMARY]

    def get_hdrhistogram(self, **_):
        """Return the HDR summary."""
        return HDR_SUMMARY


def run_decorated_search(tester):
    """Call a decorated read search on the tester and return what it returns."""

    @latency_calculator_decorator(workload_type="read", legend="fts search", cycle_name="fts_search", row_name="row-1")
    def _do_search(_self):
        return {"hdr_tags": ["fn--search"]}

    # NOTE: the decorator resolves the tester from the first positional argument
    return _do_search(tester)


def test__find_hdr_tags_in_dict():
    """_find_hdr_tags reads the hdr_tags key of a dict."""
    assert _find_hdr_tags({"foo": HDR_TAGS1, "hdr_tags": HDR_TAGS2}) == HDR_TAGS2


def test__find_hdr_tags_in_object_attr():
    """_find_hdr_tags reads the hdr_tags attribute of an object."""
    obj_with_hdr_tags = type("FakeStressQueue", (), {"fake_hdr_tags": HDR_TAGS1, "hdr_tags": HDR_TAGS2})
    res = _find_hdr_tags(obj_with_hdr_tags)
    assert res == HDR_TAGS2


def test__find_hdr_tags_in_tuple():
    """_find_hdr_tags looks inside a tuple argument for an object with hdr_tags."""
    obj_with_hdr_tags = type("FakeStressQueue", (), {"hdr_tags": HDR_TAGS1})
    params = ("foo", 5, obj_with_hdr_tags)
    res = _find_hdr_tags(params)
    assert res == HDR_TAGS1


def test__find_hdr_tags_in_tuple_of_lists():
    """_find_hdr_tags searches nested tuples and lists and returns the first hdr_tags it finds."""
    obj_with_hdr_tags1 = type("FakeStressQueue", (), {"hdr_tags": HDR_TAGS1})
    obj_with_hdr_tags2 = type("FakeStressQueue", (), {"hdr_tags": HDR_TAGS2})
    params = ("foo", 5, (["a", "b"], [obj_with_hdr_tags1, obj_with_hdr_tags2]))
    res = _find_hdr_tags(params)
    assert res == HDR_TAGS1


def test__find_hdr_tags_error():
    """_find_hdr_tags raises ValueError when no hdr_tags are present."""
    try:
        _find_hdr_tags({"no_hdr_tags": ["foo"]})
    except ValueError:
        pass
    else:
        assert False, "Expected 'ValueError'"


def test__find_hdr_tags_falls_back_when_the_kwarg_is_none():
    """A caller with an optional 'hdr_tags' argument always forwards it, so an unset one arrives
    as an explicit None. That must not shadow the tags the stress threads set on themselves.
    """
    stress_queue = type("FakeStressQueue", (), {"hdr_tags": HDR_TAGS1})
    res = _find_hdr_tags({"stress_cmds": ["cassandra-stress write"], "hdr_tags": None}, (["result"], [stress_queue]))
    assert res == HDR_TAGS1


def test__find_hdr_tags_falls_back_when_an_object_attr_is_none():
    """_find_hdr_tags skips an object whose hdr_tags is None and takes the next one."""
    obj_with_none = type("FakeStressQueue", (), {"hdr_tags": None})
    obj_with_hdr_tags = type("FakeStressQueue", (), {"hdr_tags": HDR_TAGS2})
    assert _find_hdr_tags(obj_with_none, obj_with_hdr_tags) == HDR_TAGS2


def test__find_hdr_tags_keeps_returning_an_empty_list():
    """A nemesis carries an empty 'hdr_tags' until a test populates it, and a cycle reported
    without HDR histograms is a valid outcome - only None is unusable to the caller.
    """
    nemesis = type("FakeNemesis", (), {"hdr_tags": []})
    assert _find_hdr_tags(nemesis) == []


def test__find_hdr_tags_error_on_a_none_kwarg_with_nothing_to_fall_back_to():
    """Never return the None itself: the caller iterates over what it gets back."""
    with pytest.raises(ValueError, match="Failed to find 'hdr_tags'"):
        _find_hdr_tags({"hdr_tags": None}, ([], []))


def test_skip_on_capacity_issues_converts_stuck_vm_give_up_to_nemesis_skip():
    """Stuck VM recovery give-up on a balanced cluster must skip the nemesis, not fail it."""
    with (
        patch("sdcm.utils.decorators.check_cluster_layout", return_value=True),
        pytest.raises(UnsupportedNemesis, match="Capacity Issue"),
    ):
        skip_on_capacity_issues(db_cluster=MagicMock())(raise_stuck_vm_give_up)()


def test_skip_on_capacity_issues_reraises_stuck_vm_give_up_on_unbalanced_cluster():
    """On an unbalanced cluster the give-up must publish a CRITICAL event AND re-raise, never return None."""
    with (
        patch("sdcm.utils.decorators.check_cluster_layout", return_value=False),
        patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock,
        pytest.raises(ProvisionUnrecoverableError),
    ):
        skip_on_capacity_issues(db_cluster=MagicMock())(raise_stuck_vm_give_up)()
    assert event_mock.call_args.kwargs["severity"] == Severity.CRITICAL
    event_mock.return_value.publish.assert_called_once_with()


def test_skip_on_capacity_issues_reraises_service_unavailable_on_unbalanced_cluster():
    """A GCE ServiceUnavailable on an unbalanced cluster must publish a CRITICAL event AND re-raise."""
    with (
        patch("sdcm.utils.decorators.check_cluster_layout", return_value=False),
        patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock,
        pytest.raises(ServiceUnavailable),
    ):
        skip_on_capacity_issues(db_cluster=MagicMock())(raise_service_unavailable)()
    assert event_mock.call_args.kwargs["severity"] == Severity.CRITICAL
    event_mock.return_value.publish.assert_called_once_with()


def test_critical_on_capacity_issues_publishes_critical_on_stuck_vm_give_up():
    """Stuck VM recovery give-up in a must-succeed topology change must raise a critical event."""
    with (
        patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock,
        pytest.raises(ProvisionUnrecoverableError),
    ):
        critical_on_capacity_issues(raise_stuck_vm_give_up)()
    assert event_mock.call_args.kwargs["severity"] == Severity.CRITICAL
    event_mock.return_value.publish.assert_called_once_with()


def test_skip_on_capacity_issues_does_not_skip_on_instance_configuration_error():
    """An invalid instance configuration is not a capacity issue: it must fail, not skip the nemesis.

    `InstanceConfigurationError` is a `ProvisionUnrecoverableError`, so without its own clause it
    would be softened into `UnsupportedNemesis("Capacity Issue")` and the real misconfiguration
    would never surface.
    """
    with (
        patch("sdcm.utils.decorators.check_cluster_layout", return_value=True),
        patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock,
        pytest.raises(InstanceConfigurationError, match="not compatible for creating instance"),
    ):
        skip_on_capacity_issues(db_cluster=MagicMock())(raise_instance_configuration_error)()
    event_mock.return_value.publish.assert_not_called()


def test_skip_on_capacity_issues_reports_configuration_error_on_unbalanced_cluster():
    """On an unbalanced cluster it still publishes CRITICAL and re-raises, but not as a capacity issue."""
    with (
        patch("sdcm.utils.decorators.check_cluster_layout", return_value=False),
        patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock,
        pytest.raises(InstanceConfigurationError),
    ):
        skip_on_capacity_issues(db_cluster=MagicMock())(raise_instance_configuration_error)()
    assert event_mock.call_args.kwargs["severity"] == Severity.CRITICAL
    assert "invalid instance configuration" in event_mock.call_args.kwargs["message"]
    assert "capacity" not in event_mock.call_args.kwargs["message"]
    event_mock.return_value.publish.assert_called_once_with()


def test_critical_on_capacity_issues_reports_configuration_error_as_such():
    """A must-succeed topology change still fails critically, with the actual cause in the message."""
    with (
        patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock,
        pytest.raises(InstanceConfigurationError),
    ):
        critical_on_capacity_issues(raise_instance_configuration_error)()
    assert event_mock.call_args.kwargs["severity"] == Severity.CRITICAL
    assert "invalid instance configuration" in event_mock.call_args.kwargs["message"]
    assert "capacity" not in event_mock.call_args.kwargs["message"]
    event_mock.return_value.publish.assert_called_once_with()


def test_critical_on_capacity_issues_reraises_on_service_unavailable():
    """A GCE ServiceUnavailable in a must-succeed topology change must publish CRITICAL and re-raise."""
    with (
        patch("sdcm.utils.decorators.TestFrameworkEvent") as event_mock,
        pytest.raises(ServiceUnavailable),
    ):
        critical_on_capacity_issues(raise_service_unavailable)()
    assert event_mock.call_args.kwargs["severity"] == Severity.CRITICAL
    event_mock.return_value.publish.assert_called_once_with()


def test_latency_calculator_decorator_without_monitoring_set(tmp_path):
    """Without a monitoring set the decorator still reports HDR results, stalls and Argus data, but no latency."""
    tester = FakeLatencyTester(latency_results_file=tmp_path / "latency_results.json", monitors=FakeMonitorSet())

    with (
        patch("sdcm.tester.ClusterTester", FakeLatencyTester),
        patch("sdcm.utils.decorators.EventCounterContextManager", FakeEventCounter),
        patch("sdcm.utils.latency.collect_latency") as collect_latency_mock,
        patch("sdcm.utils.decorators.send_result_to_argus") as send_to_argus_mock,
    ):
        res = run_decorated_search(tester)

    assert res == {"hdr_tags": ["fn--search"]}
    collect_latency_mock.assert_not_called()
    assert not tester.monitors.screenshot_requests
    # NOTE: the HDR results, the reactor stalls and the Argus reporting are the point of the decorator,
    #       they must survive
    # NOTE: 'result' holds a wall-clock duration and 'start_time' is time.time(), so both are ANY
    send_to_argus_mock.assert_called_once_with(
        argus_client=tester.test_config.argus_client.return_value,
        workload="read",
        name="fts_search",
        description="fts search",
        cycle="row-1",
        result=ANY,
        start_time=ANY,
        error_thresholds=None,
    )
    cycle = send_to_argus_mock.call_args.kwargs["result"]
    assert cycle["hdr_summary"] == HDR_SUMMARY
    assert cycle["cycle_hdr_throughput"] == 6414
    assert cycle["reactor_stalls_stats"] == REACTOR_STALLS
    assert cycle["screenshots"] == []

    latency_results = json.loads((tmp_path / "latency_results.json").read_text(encoding="utf-8"))
    assert latency_results["fts_search"]["legend"] == "fts search"
    assert len(latency_results["fts_search"]["cycles"]) == 1


def test_latency_calculator_decorator_with_monitoring_set(tmp_path):
    """With a monitor node the decorator collects latency and screenshots into the Argus cycle."""
    tester = FakeLatencyTester(
        latency_results_file=tmp_path / "latency_results.json", monitors=FakeMonitorSetWithNode()
    )

    with (
        patch("sdcm.tester.ClusterTester", FakeLatencyTester),
        patch("sdcm.utils.decorators.EventCounterContextManager", FakeEventCounter),
        patch("sdcm.utils.latency.collect_latency", return_value={"Scylla P99_read - node-1": 1.5}) as collect_mock,
        patch("sdcm.utils.decorators.send_result_to_argus") as send_to_argus_mock,
    ):
        run_decorated_search(tester)

    # NOTE: start and end are time.time() values
    collect_mock.assert_called_once_with(FakeMonitorSetWithNode.nodes[0], ANY, ANY, "read", tester.db_cluster, [])
    cycle = send_to_argus_mock.call_args.kwargs["result"]
    assert cycle["Scylla P99_read - node-1"] == 1.5
    assert len(cycle["screenshots"]) == 1
    assert tester.monitors.screenshot_requests
