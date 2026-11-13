"""Tests for the spot-termination retry decision (SCT-707).

The Jenkins loop in `vars/runWithSpotRetry.groovy` only acts on what `sct.py spot-retry-decision` prints,
so every rule that decides whether a build spends another attempt is pinned down here.
"""

import unittest
from unittest.mock import MagicMock, patch

import pytest
from azure.core.exceptions import ResourceNotFoundError
from click.testing import CliRunner

import sct
from sdcm import cluster as sdcm_cluster
from sdcm.provision.azure.provisioner import AzureProvisioner
from sdcm.provision.provisioner import OperationPreemptedError, PricingModel, ProvisionError
from sdcm.sct_provision.instances_provider import provision_instances_with_fallback
from sdcm.tester import ClusterTester, CriticalTestFailure, teardown_on_exception
from sdcm.utils import spot_retry
from sdcm.utils.argus import ReplayOnlyArgusSCTClient

SPOT_EVENT = {
    "event_type": "SpotTerminationEvent",
    "severity": "CRITICAL",
    "message": "(SpotTerminationEvent Severity.CRITICAL) node=Node db-node-1 (Type: n2d-standard-2) "
    "message=Instance was preempted.",
}
OTHER_FAILURE_EVENT = {
    "event_type": "DatabaseLogEvent",
    "severity": "ERROR",
    "message": "Scylla failed to start: bad repo",
}


@pytest.mark.parametrize(
    "policy, attempt, events, expected",
    [
        # opted out: never retry, even after a reclaim
        ([], 1, [SPOT_EVENT], "stop"),
        # single retry goes straight to on-demand
        (["on_demand"], 1, [SPOT_EVENT], "on_demand"),
        (["on_demand"], 2, [SPOT_EVENT], "stop"),
        # the long chain
        (["spot", "on_demand"], 1, [SPOT_EVENT], "spot"),
        (["spot", "on_demand"], 2, [SPOT_EVENT], "on_demand"),
        (["spot", "on_demand"], 3, [SPOT_EVENT], "stop"),
        # any failure that is not a reclaim is final
        (["spot", "on_demand"], 1, [OTHER_FAILURE_EVENT], "stop"),
        (["spot", "on_demand"], 1, [], "stop"),
        # a reclaim among other errors still counts
        (["spot", "on_demand"], 1, [OTHER_FAILURE_EVENT, SPOT_EVENT], "spot"),
    ],
)
def test_decide(policy, attempt, events, expected):
    assert spot_retry.decide(policy, attempt, events) == expected


def test_decide_never_exceeds_max_attempts_even_with_a_longer_policy():
    """The ceiling holds even for a policy that skipped validation."""
    policy = ["spot"] * 5
    assert spot_retry.decide(policy, spot_retry.MAX_ATTEMPTS, [SPOT_EVENT]) == "stop"


@pytest.mark.parametrize("policy", [[], ["on_demand"], ["spot", "on_demand"], ["on_demand", "on_demand"]])
def test_validate_policy_accepts(policy):
    spot_retry.validate_policy(policy)


@pytest.mark.parametrize(
    "policy, error",
    [
        (["spot"], "must end with 'on_demand'"),
        (["on_demand", "spot"], "must end with 'on_demand'"),
        (["spot", "spot", "on_demand"], "at most 2 retries"),
        (["spot_fleet", "on_demand"], "must be one of"),
    ],
)
def test_validate_policy_rejects(policy, error):
    with pytest.raises(ValueError, match=error):
        spot_retry.validate_policy(policy)


def test_fetch_run_events_reads_critical_events():
    client = MagicMock()
    client.run_id = "09240165-4b31-4379-8058-29e0f46fe068"
    client.get.return_value.json.return_value = {"response": [SPOT_EVENT]}

    assert spot_retry.fetch_run_events(client) == [SPOT_EVENT]
    assert client.get.call_args.kwargs["endpoint"] == "/sct/$id/events/CRITICAL/get"
    client.check_response.assert_called_once()


def _setup_failing_with(exc):
    @teardown_on_exception
    def fake_setup(self):
        raise exc

    return fake_setup


def _preemption_wrapped_in_provision_error():
    try:
        raise OperationPreemptedError("Spot instance db-node-1 preempted during provisioning: STOPPING")
    except OperationPreemptedError as err:
        raise ProvisionError("could not create db nodes") from err


@pytest.mark.parametrize("wrapped", [False, True])
def test_setup_preemption_publishes_spot_termination_event(wrapped):
    """A preemption that reaches setUp is what the decision reads as a provisioning-time reclaim."""
    tester = MagicMock(argus_heartbeat_stop_signal=MagicMock())
    if wrapped:
        try:
            _preemption_wrapped_in_provision_error()
        except ProvisionError as err:
            exc = err
    else:
        exc = OperationPreemptedError("Spot instance db-node-1 preempted during provisioning: STOPPING")

    with (
        patch("sdcm.tester.TestFrameworkEvent"),
        patch("sdcm.tester.SpotTerminationEvent") as spot_event,
        pytest.raises(type(exc)),
    ):
        _setup_failing_with(exc)(tester)

    assert "preempted during provisioning" in spot_event.call_args.kwargs["message"]
    spot_event.return_value.publish_or_dump.assert_called_once()


def test_other_setup_failures_publish_no_spot_termination_event():
    tester = MagicMock(argus_heartbeat_stop_signal=MagicMock())
    with (
        patch("sdcm.tester.TestFrameworkEvent"),
        patch("sdcm.tester.SpotTerminationEvent") as spot_event,
        pytest.raises(ValueError),
    ):
        _setup_failing_with(ValueError("bad scylla_repo"))(tester)

    spot_event.assert_not_called()


def _run_cli(monkeypatch, policy, events=None, argus_error=None, replay_only=False):
    params = {"spot_termination_retry": policy, "test_id": "09240165-4b31-4379-8058-29e0f46fe068"}
    config = MagicMock()
    config.get.side_effect = params.get
    test_config = MagicMock()
    test_config.argus_client.return_value = MagicMock(spec=ReplayOnlyArgusSCTClient) if replay_only else MagicMock()
    monkeypatch.setattr(sct, "SCTConfiguration", lambda: config)
    monkeypatch.setattr(sct, "get_test_config", lambda: test_config)
    monkeypatch.setattr(sct, "add_file_logger", lambda: None)
    if argus_error:
        monkeypatch.setattr(spot_retry, "fetch_run_events", MagicMock(side_effect=argus_error))
    else:
        monkeypatch.setattr(spot_retry, "fetch_run_events", MagicMock(return_value=events or []))
    result = CliRunner().invoke(sct.spot_retry_decision, ["--attempt", "1"])
    assert result.exit_code == 0, result.output
    return result.output.strip().splitlines()[-1]


def test_cli_prints_retry_after_a_reclaim(monkeypatch):
    assert _run_cli(monkeypatch, ["spot", "on_demand"], [SPOT_EVENT]) == "SPOT_RETRY_DECISION=spot"


def test_cli_prints_stop_when_not_opted_in(monkeypatch):
    assert _run_cli(monkeypatch, [], [SPOT_EVENT]) == "SPOT_RETRY_DECISION=stop"


def test_cli_prints_stop_when_argus_is_unreachable(monkeypatch):
    """No evidence of a reclaim means no retry."""
    assert _run_cli(monkeypatch, ["spot", "on_demand"], argus_error=ConnectionError("argus down")) == (
        "SPOT_RETRY_DECISION=stop"
    )


def test_cli_prints_stop_with_a_replay_only_argus_client(monkeypatch):
    """SCT falls back to a replay-only client when Argus is unreachable; its GETs return nothing real."""
    assert _run_cli(monkeypatch, ["spot", "on_demand"], [SPOT_EVENT], replay_only=True) == ("SPOT_RETRY_DECISION=stop")


class _SubtestCase(ClusterTester):
    __test__ = False  # run by the test below
    ran_after_kill = False

    def __init__(self, method_name):  # skip ClusterTester's setup of a real test
        unittest.TestCase.__init__(self, method_name)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_killed_inside_a_subtest(self):
        with self.subTest("reclaimed"):
            raise CriticalTestFailure("Critical Error has failed the test")
        with self.subTest("next check"):
            type(self).ran_after_kill = True


def test_critical_failure_inside_a_subtest_ends_the_test():
    """A reclaim mid-test must end it, not just fail one subtest (Azure waited out the job timeout)."""
    result = unittest.TestResult()
    _SubtestCase("test_killed_inside_a_subtest").run(result)

    assert not _SubtestCase.ran_after_kill
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


@pytest.mark.parametrize(
    "pricing_model, reclaimed, published",
    [
        (PricingModel.SPOT, True, True),
        (PricingModel.SPOT, False, False),
        (PricingModel.ON_DEMAND, True, False),
        # the cloud API failing must not replace the cloud-init error
        (PricingModel.SPOT, ConnectionError("azure api down"), False),
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
        pytest.raises(TimeoutError),
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
