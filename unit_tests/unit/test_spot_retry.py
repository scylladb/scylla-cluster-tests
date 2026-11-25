"""Tests for the spot-termination retry decision (SCT-707).

The Jenkins loop in `vars/runWithSpotRetry.groovy` only acts on what `sct.py spot-retry-decision` prints,
so every rule that decides whether a build spends another attempt is pinned down here.
"""

import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from azure.core.exceptions import ResourceNotFoundError
from click.testing import CliRunner

import sct
from argus.common.enums import TestStatus as ArgusStatus  # not "Test*", pytest would try to collect it
from sdcm import cluster as sdcm_cluster
from sdcm.provision.azure.provisioner import AzureProvisioner
from sdcm.provision.provisioner import OperationPreemptedError, PricingModel, ProvisionError
from sdcm.sct_provision.instances_provider import provision_instances_with_fallback
from sdcm.tester import ClusterTester, CriticalTestFailure, teardown_on_exception
from sdcm.utils import spot_retry
from sdcm.utils.argus import ReplayOnlyArgusSCTClient
from sdcm.utils.parallel_object import ParallelObjectException

SPOT_EVENT = {
    "event_type": "SpotTerminationEvent",
    "severity": "CRITICAL",
    "message": "(SpotTerminationEvent Severity.CRITICAL) node=Node db-node-1 (Type: n2d-standard-2) "
    "message=Instance was preempted.",
    "ts": "2026-10-16T22:57:18.811Z",  # the format Argus returns
}
OTHER_FAILURE_EVENT = {
    "event_type": "DatabaseLogEvent",
    "severity": "CRITICAL",
    "message": "Scylla failed to start: bad repo",
    "ts": "2026-10-16T22:57:18.811Z",
}


def other_failure(seconds_before_the_reclaim):
    """Return OTHER_FAILURE_EVENT stamped the given seconds before the reclaim."""
    ts = datetime.fromisoformat(SPOT_EVENT["ts"]) - timedelta(seconds=seconds_before_the_reclaim)
    return OTHER_FAILURE_EVENT | {"ts": ts.isoformat()}


def setup_failing_with(exc):
    """Return a ClusterTester setup method that raises exc, wrapped by teardown_on_exception."""

    @teardown_on_exception
    def fake_setup(self):
        raise exc

    return fake_setup


def preemption_wrapped_in_provision_error():
    """Return a ProvisionError chained from an OperationPreemptedError, as provisioning raises it."""
    try:
        raise OperationPreemptedError("Spot instance db-node-1 preempted during provisioning: STOPPING")
    except OperationPreemptedError as err:
        try:
            raise ProvisionError("could not create db nodes") from err
        except ProvisionError as wrapped:
            return wrapped


def run_cli(monkeypatch, policy, events=None, argus_error=None, replay_only=False, status_error=None):
    """Return the printed decision line and the Argus client."""
    params = {"spot_termination_retry": policy, "test_id": "09240165-4b31-4379-8058-29e0f46fe068"}
    config = MagicMock()
    config.get.side_effect = params.get
    test_config = MagicMock()
    argus_client = MagicMock(spec=ReplayOnlyArgusSCTClient) if replay_only else MagicMock()
    argus_client.set_sct_run_status.side_effect = status_error
    test_config.argus_client.return_value = argus_client
    monkeypatch.setattr(sct, "SCTConfiguration", lambda: config)
    monkeypatch.setattr(sct, "get_test_config", lambda: test_config)
    monkeypatch.setattr(sct, "add_file_logger", lambda: None)
    if argus_error:
        monkeypatch.setattr(spot_retry, "fetch_run_events", MagicMock(side_effect=argus_error))
    else:
        monkeypatch.setattr(spot_retry, "fetch_run_events", MagicMock(return_value=events or []))
    result = CliRunner().invoke(sct.spot_retry_decision, ["--attempt", "1"])
    assert result.exit_code == 0, result.output
    return result.output.strip().splitlines()[-1], argus_client


class SubtestCase(ClusterTester):
    """ClusterTester without its real setup, run by unittest to exercise subTest."""

    __test__ = False  # run by the test below
    ran_after_kill = False

    def __init__(self, method_name):  # skip ClusterTester's setup of a real test
        unittest.TestCase.__init__(self, method_name)

    def setUp(self):
        """Skip ClusterTester's real setup."""

    def tearDown(self):
        """Skip ClusterTester's real teardown."""

    def test_killed_inside_a_subtest(self):
        """Raise a critical failure in one subtest, then mark the next subtest as run."""
        with self.subTest("reclaimed"):
            raise CriticalTestFailure("Critical Error has failed the test")
        with self.subTest("next check"):
            type(self).ran_after_kill = True


@pytest.mark.parametrize(
    "policy, attempt, events, expected",
    [
        # opted out: never retry, even after a reclaim
        pytest.param([], 1, [SPOT_EVENT], "stop", id="opted-out"),
        # single retry goes straight to on-demand
        pytest.param(["on_demand"], 1, [SPOT_EVENT], "on_demand", id="on-demand-first-attempt"),
        pytest.param(["on_demand"], 2, [SPOT_EVENT], "stop", id="on-demand-exhausted"),
        # the long chain
        pytest.param(["spot", "on_demand"], 1, [SPOT_EVENT], "spot", id="chain-first-attempt"),
        pytest.param(["spot", "on_demand"], 2, [SPOT_EVENT], "on_demand", id="chain-second-attempt"),
        pytest.param(["spot", "on_demand"], 3, [SPOT_EVENT], "stop", id="chain-exhausted"),
        # any failure that is not a reclaim is final
        pytest.param(["spot", "on_demand"], 1, [OTHER_FAILURE_EVENT], "stop", id="not-a-reclaim"),
        pytest.param(["spot", "on_demand"], 1, [], "stop", id="no-events"),
        # failures the reclaim caused: after it, or just before it was reported
        pytest.param(["spot", "on_demand"], 1, [SPOT_EVENT, other_failure(-600)], "spot", id="failure-after-reclaim"),
        pytest.param(["spot", "on_demand"], 1, [other_failure(30), SPOT_EVENT], "spot", id="failure-before-reclaim"),
        # the test had already failed when the node was reclaimed (in teardown): a retry would hide it
        pytest.param(["spot", "on_demand"], 1, [other_failure(600), SPOT_EVENT], "stop", id="failed-before-reclaim"),
    ],
)
def test_decide(policy, attempt, events, expected):
    """The next pricing model, or stop, follows from the policy, attempt number and run events."""
    assert spot_retry.decide(policy, attempt, events) == expected


def test_decide_raises_on_an_event_without_a_timestamp():
    """The CLI answers any error with `stop`."""
    with pytest.raises(KeyError):
        spot_retry.decide(["spot", "on_demand"], 1, [SPOT_EVENT, {"event_type": "DatabaseLogEvent"}])


def test_decide_never_exceeds_max_attempts_even_with_a_longer_policy():
    """The ceiling holds even for a policy that skipped validation."""
    policy = ["spot"] * 5
    assert spot_retry.decide(policy, spot_retry.MAX_ATTEMPTS, [SPOT_EVENT]) == "stop"


@pytest.mark.parametrize(
    "policy",
    [
        pytest.param([], id="opted-out"),
        pytest.param(["on_demand"], id="on-demand"),
        pytest.param(["spot", "on_demand"], id="spot-then-on-demand"),
        pytest.param(["on_demand", "on_demand"], id="two-on-demand"),
    ],
)
def test_validate_policy_accepts(policy):
    """validate_policy accepts an empty policy and any chain that ends with on_demand."""
    spot_retry.validate_policy(policy)


@pytest.mark.parametrize(
    "policy, error",
    [
        pytest.param(["spot"], "must end with 'on_demand'", id="ends-with-spot"),
        pytest.param(["on_demand", "spot"], "must end with 'on_demand'", id="on-demand-then-spot"),
        pytest.param(["spot", "spot", "on_demand"], "at most 2 retries", id="too-many-retries"),
        pytest.param(["spot_fleet", "on_demand"], "must be one of", id="unknown-type"),
    ],
)
def test_validate_policy_rejects(policy, error):
    """validate_policy raises ValueError naming the broken rule."""
    with pytest.raises(ValueError, match=error):
        spot_retry.validate_policy(policy)


def test_fetch_run_events_reads_critical_events():
    """fetch_run_events reads the run's CRITICAL events from Argus and checks the response."""
    client = MagicMock()
    client.run_id = "09240165-4b31-4379-8058-29e0f46fe068"
    client.get.return_value.json.return_value = {"response": [SPOT_EVENT]}

    assert spot_retry.fetch_run_events(client) == [SPOT_EVENT]
    assert client.get.call_args.kwargs["endpoint"] == "/sct/$id/events/CRITICAL/get"
    client.check_response.assert_called_once_with(client.get.return_value)


@pytest.mark.parametrize(
    "wrapped",
    [
        pytest.param(False, id="direct"),
        pytest.param(True, id="wrapped-in-provision-error"),
    ],
)
def test_setup_preemption_publishes_spot_termination_event(wrapped):
    """A preemption that reaches setUp is what the decision reads as a provisioning-time reclaim."""
    tester = MagicMock(argus_heartbeat_stop_signal=MagicMock())
    if wrapped:
        exc = preemption_wrapped_in_provision_error()
    else:
        exc = OperationPreemptedError("Spot instance db-node-1 preempted during provisioning: STOPPING")

    with (
        patch("sdcm.tester.TestFrameworkEvent"),
        patch("sdcm.tester.SpotTerminationEvent") as spot_event,
        pytest.raises(type(exc)),
    ):
        setup_failing_with(exc)(tester)

    assert "preempted during provisioning" in spot_event.call_args.kwargs["message"]
    spot_event.return_value.publish_or_dump.assert_called_once_with()


def test_other_setup_failures_publish_no_spot_termination_event():
    """A setUp failure that is not a preemption publishes no SpotTerminationEvent."""
    tester = MagicMock(argus_heartbeat_stop_signal=MagicMock())
    with (
        patch("sdcm.tester.TestFrameworkEvent"),
        patch("sdcm.tester.SpotTerminationEvent") as spot_event,
        pytest.raises(ValueError),
    ):
        setup_failing_with(ValueError("bad scylla_repo"))(tester)

    spot_event.assert_not_called()


def test_cli_prints_retry_after_a_reclaim_and_marks_the_run_error(monkeypatch):
    """ERROR ranks below PASSED in Argus's per-build status, so the retry decides how the build shows."""
    decision, argus_client = run_cli(monkeypatch, ["spot", "on_demand"], [SPOT_EVENT])
    assert decision == "SPOT_RETRY_DECISION=spot"
    argus_client.set_sct_run_status.assert_called_once_with(ArgusStatus.ERROR)


def test_cli_prints_stop_when_not_opted_in(monkeypatch):
    """A run that did not opt in prints stop and leaves the Argus run status alone."""
    decision, argus_client = run_cli(monkeypatch, [], [SPOT_EVENT])
    assert decision == "SPOT_RETRY_DECISION=stop"
    argus_client.set_sct_run_status.assert_not_called()


def test_cli_keeps_the_status_of_a_run_that_is_not_retried(monkeypatch):
    """A run without a reclaim prints stop and keeps its Argus run status."""
    decision, argus_client = run_cli(monkeypatch, ["spot", "on_demand"], [])
    assert decision == "SPOT_RETRY_DECISION=stop"
    argus_client.set_sct_run_status.assert_not_called()


def test_cli_prints_stop_when_argus_is_unreachable(monkeypatch):
    """No evidence of a reclaim means no retry."""
    decision, _ = run_cli(monkeypatch, ["spot", "on_demand"], argus_error=ConnectionError("argus down"))
    assert decision == "SPOT_RETRY_DECISION=stop"


def test_cli_prints_stop_when_the_run_status_cannot_be_set(monkeypatch):
    """A retried attempt left FAILED would keep the build red in Argus even when the retry passes."""
    decision, _ = run_cli(monkeypatch, ["spot", "on_demand"], [SPOT_EVENT], status_error=ConnectionError("down"))
    assert decision == "SPOT_RETRY_DECISION=stop"


def test_cli_prints_stop_with_a_replay_only_argus_client(monkeypatch):
    """SCT falls back to a replay-only client when Argus is unreachable; its GETs return nothing real."""
    decision, _ = run_cli(monkeypatch, ["spot", "on_demand"], [SPOT_EVENT], replay_only=True)
    assert decision == "SPOT_RETRY_DECISION=stop"


def test_critical_failure_inside_a_subtest_ends_the_test():
    """A reclaim mid-test must end it, not just fail one subtest (Azure waited out the job timeout)."""
    result = unittest.TestResult()
    SubtestCase("test_killed_inside_a_subtest").run(result)

    assert not SubtestCase.ran_after_kill
    assert len(result.failures) + len(result.errors) == 2  # the subtest, and the test itself


def test_spot_monitoring_starts_before_the_cloud_init_wait(monkeypatch):
    """A reclaim while cloud-init runs must publish SpotTerminationEvent, or the attempt is not retried."""
    node = MagicMock(logdir=None, ssh_login_info={"hostname": None})
    node.wait_for_cloud_init.side_effect = StopIteration  # nothing after it matters here
    monkeypatch.setattr(sdcm_cluster.RemoteCmdRunnerBase, "create_remoter", lambda **_: MagicMock())

    with pytest.raises(StopIteration):
        sdcm_cluster.BaseNode.init(node)

    called = [name for name, *_ in node.mock_calls]
    assert called.index("start_spot_monitoring_if_needed") < called.index("wait_for_cloud_init")


def test_reclaimed_node_gets_no_more_scylla_commands():
    """Teardown diagnostics on a reclaimed node wait out SSH timeouts: 40 minutes per attempt on Azure."""
    node = MagicMock(spot_reclaimed=False)
    assert sdcm_cluster.BaseNode._is_node_ready_run_scylla_commands(node)

    with patch("sdcm.cluster.SpotTerminationEvent") as event:
        sdcm_cluster.BaseNode.report_spot_termination(node, "Instance was preempted.")

    event.return_value.publish.assert_called_once_with()
    assert not sdcm_cluster.BaseNode._is_node_ready_run_scylla_commands(node)


@pytest.mark.parametrize(
    "pricing_model, reclaimed, published",
    [
        pytest.param(PricingModel.SPOT, True, True, id="spot-reclaimed"),
        pytest.param(PricingModel.SPOT, False, False, id="spot-not-reclaimed"),
        pytest.param(PricingModel.ON_DEMAND, True, False, id="on-demand"),
        # the cloud API failing must not replace the cloud-init error
        pytest.param(PricingModel.SPOT, ConnectionError("azure api down"), False, id="cloud-api-down"),
    ],
)
def test_reclaim_during_the_cloud_init_wait_publishes_spot_termination_event(pricing_model, reclaimed, published):
    """Azure waits for cloud-init before any node exists: only the cloud can tell a reclaim from a broken setup."""
    provisioner = MagicMock()
    provisioner.get_or_create_instances.return_value = [MagicMock(public_ip_address="1.2.3.4")]
    if isinstance(reclaimed, Exception):
        provisioner.was_reclaimed.side_effect = reclaimed
    else:
        provisioner.was_reclaimed.return_value = reclaimed
    with (
        patch("sdcm.sct_provision.instances_provider.RemoteCmdRunnerBase"),
        patch("sdcm.sct_provision.instances_provider.wait_cloud_init_completes", side_effect=TimeoutError("ssh")),
        patch("sdcm.sct_provision.instances_provider.SpotTerminationEvent") as event,
        # the waits run in parallel, so the cloud-init error of each node arrives in one exception
        pytest.raises(ParallelObjectException, match="TimeoutError: ssh"),
    ):
        provision_instances_with_fallback(
            provisioner,
            definitions=[MagicMock(use_public_ip=True)],
            pricing_model=pricing_model,
            fallback_on_demand=False,
        )

    assert event.called is published


@pytest.mark.parametrize(
    "lookup, expected",
    [
        (ResourceNotFoundError("gone"), True),
        (MagicMock(provisioning_state="Deleting"), True),
        (MagicMock(provisioning_state="Succeeded"), False),
    ],
    ids=["deleted", "being-deleted", "running"],
)
def test_azure_reports_an_evicted_spot_vm_as_reclaimed(lookup, expected):
    """SCT's Azure spot VMs use evictionPolicy=Delete, so an evicted VM is being deleted or already gone."""
    provisioner = AzureProvisioner.__new__(AzureProvisioner)
    provisioner._azure_service = MagicMock()
    provisioner._test_id, provisioner._region, provisioner._az = "test-id", "eastus", ""
    get = provisioner._azure_service.compute.virtual_machines.get
    get.side_effect = lookup if isinstance(lookup, Exception) else None
    get.return_value = lookup

    assert provisioner.was_reclaimed("db-node-1") is expected
