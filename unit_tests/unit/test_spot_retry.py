"""Tests for the spot-termination retry decision (SCT-707).

The Jenkins loop in `vars/runWithSpotRetry.groovy` only acts on what `sct.py spot-retry-decision` prints,
so every rule that decides whether a build spends another attempt is pinned down here.
"""

from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

import sct
from sdcm.utils import spot_retry
from sdcm.utils.argus import ReplayOnlyArgusSCTClient

SPOT_EVENT = {
    "event_type": "SpotTerminationEvent",
    "severity": "CRITICAL",
    "message": "(SpotTerminationEvent Severity.CRITICAL) node=Node db-node-1 (Type: n2d-standard-2) "
    "message=Instance was preempted.",
}
PROVISION_PREEMPTED_EVENT = {
    "event_type": "TestFrameworkEvent",
    "severity": "ERROR",
    "message": "sdcm.provision.provisioner.OperationPreemptedError: Spot instance db-node-1 preempted during "
    "provisioning",
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
        # a reclaim during provisioning, with no on-demand fallback to save the run
        (["spot", "on_demand"], 1, [PROVISION_PREEMPTED_EVENT], "spot"),
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


def test_fetch_run_events_reads_critical_and_error():
    client = MagicMock()
    client.run_id = "09240165-4b31-4379-8058-29e0f46fe068"
    client.get.return_value.json.side_effect = [{"response": [SPOT_EVENT]}, {"response": [OTHER_FAILURE_EVENT]}]

    assert spot_retry.fetch_run_events(client) == [SPOT_EVENT, OTHER_FAILURE_EVENT]
    severities = [call.kwargs["location_params"]["severity"] for call in client.get.call_args_list]
    assert severities == ["CRITICAL", "ERROR"]
    assert client.check_response.call_count == 2


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
