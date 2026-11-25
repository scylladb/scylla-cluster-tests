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
# Copyright (c) 2026 ScyllaDB

"""Tests for spot-vs-on-demand provisioning observability (SCT-850).

A spot request that fails falls through to on-demand and the run continues, so without an explicit record a
run "configured for spot" can be billed on-demand with nothing noting it. These tests pin that record down.
"""

import gc
import logging
from unittest.mock import MagicMock, patch

import pytest

import sct
from sdcm.provision.common.provision_plan import ProvisionPlan
from sdcm.provision.common.provisioner import InstanceProvisionerBase, ProvisionParameters
from sdcm.sct_events import Severity
from sdcm.sct_events.base import add_severity_limit_rules, max_severity
from sdcm.sct_events.system import SpotProvisionOutcomeEvent
from sdcm.test_config import TestConfig


class StubProvisioner(InstanceProvisionerBase):
    """Returns a canned result per provisioning step; `ProvisionPlan` validates the provisioner's type."""

    results: list = []

    def provision(self, *args, **kwargs):  # noqa: ARG002
        """Pop the next canned result, or an empty list once they run out."""
        return self.results.pop(0) if self.results else []


def make_steps(*names):
    """Build one spot or on-demand ProvisionParameters per name; 'OnDemand' is the only non-spot name."""
    return [
        ProvisionParameters(name=name, region_name="eu-west-1", availability_zone="a", spot=name != "OnDemand")
        for name in names
    ]


def make_plan(*names, results=None):
    """Build a ProvisionPlan over the given steps, answering provisioning with the canned results."""
    return ProvisionPlan(provision_steps=make_steps(*names), provisioner=StubProvisioner(results=list(results or [])))


def run_plan(plan, instance_type="i4i.2xlarge"):
    """Run the plan and return the kwargs the outcome event was constructed with."""
    instance_params = MagicMock()
    instance_params.InstanceType = instance_type
    with patch("sdcm.provision.common.spot_outcome.SpotProvisionOutcomeEvent") as mock_event:
        instances = plan.provision_instances(
            instance_parameters=instance_params, node_count=6, node_tags=[{}] * 6, node_names=["n"] * 6
        )
    return instances, mock_event


def run_plan_real(plan, instance_type="i4i.2xlarge"):
    """Run the plan WITHOUT patching the event class.

    `run_plan` replaces SpotProvisionOutcomeEvent with a MagicMock, so `event.downgraded` is a truthy Mock and any
    assertion about log level would pass regardless. Anything checking real event behaviour must use this.
    """
    instance_params = MagicMock()
    instance_params.InstanceType = instance_type
    return plan.provision_instances(
        instance_parameters=instance_params, node_count=6, node_tags=[{}] * 6, node_names=["n"] * 6
    )


def provision_for_handoff(results):
    """Clear the handoff list, then run a Spot-then-OnDemand plan with the real event class.

    `provision-resources` has no events device, so outcomes reach Argus via an explicit handoff. Without it the
    spot-vs-on-demand split, the entire point of the event, stays unmeasured in a log nobody aggregates.
    """
    TestConfig.SPOT_PROVISION_OUTCOMES.clear()
    run_plan_real(make_plan("Spot", "OnDemand", results=results))


def test_event_records_spot_success():
    """A spot success publishes requested and realized as Spot, with the instance type and count."""
    instances, mock_event = run_plan(make_plan("Spot", "OnDemand", results=[["i-123"]]))

    assert instances == ["i-123"]
    kwargs = mock_event.call_args.kwargs
    assert kwargs["requested"] == "Spot"
    assert kwargs["realized"] == "Spot"
    assert kwargs["instance_type"] == "i4i.2xlarge"
    assert kwargs["count"] == 6


def test_event_flags_silent_downgrade_to_on_demand():
    """The core measurement: spot asked for, on-demand actually billed."""
    instances, mock_event = run_plan(make_plan("Spot", "OnDemand", results=[[], ["i-123"]]))

    assert instances == ["i-123"]
    kwargs = mock_event.call_args.kwargs
    assert kwargs["requested"] == "Spot"
    assert kwargs["realized"] == "OnDemand"


def test_event_records_total_failure():
    """When both spot and on-demand return nothing, realized is None and no instances are returned."""
    instances, mock_event = run_plan(make_plan("Spot", "OnDemand", results=[[], []]))

    assert instances == []
    assert mock_event.call_args.kwargs["realized"] is None


def test_event_published_exactly_once_per_plan():
    """A plan that falls back to on-demand publishes exactly one outcome event."""
    _, mock_event = run_plan(make_plan("Spot", "OnDemand", results=[[], ["i-123"]]))
    assert mock_event.call_count == 1


def test_on_demand_only_plan_reports_no_downgrade():
    """An on-demand-only plan reports on-demand as both requested and realized."""
    _, mock_event = run_plan(make_plan("OnDemand", results=[["i-123"]]))

    kwargs = mock_event.call_args.kwargs
    assert kwargs["requested"] == "OnDemand"
    assert kwargs["realized"] == "OnDemand"


@pytest.mark.parametrize(
    "requested, realized, downgraded, severity",
    [
        pytest.param("Spot", "Spot", False, Severity.NORMAL, id="spot-ok"),
        pytest.param("Spot", "OnDemand", True, Severity.WARNING, id="silent-downgrade"),
        pytest.param("OnDemand", "OnDemand", False, Severity.NORMAL, id="on-demand-only"),
        pytest.param("Spot", None, False, Severity.WARNING, id="total-failure"),
    ],
)
def test_event_severity_and_downgrade_flag(requested, realized, downgraded, severity):
    """The event sets the downgrade flag and a severity that the severities.yaml cap does not clamp."""
    event = SpotProvisionOutcomeEvent(
        requested=requested, realized=realized, region="eu-west-1", availability_zone="a", count=3
    )
    assert event.downgraded is downgraded
    assert event.severity is severity
    # severities.yaml caps this event type; a cap below the constructed severity would silently clamp it,
    # making the WARNING-on-total-failure branch unreachable in practice.
    add_severity_limit_rules([])  # load defaults/severities.yaml
    assert max_severity(event).value >= severity.value


def test_event_message_is_greppable():
    """The rendered message carries key=value fragments that grep can match."""
    event = SpotProvisionOutcomeEvent(
        requested="Spot",
        realized="OnDemand",
        region="eu-west-1",
        availability_zone="c",
        instance_type="i4i.2xlarge",
        count=6,
    )
    message = str(event)
    for fragment in ("requested=Spot", "realized=OnDemand", "downgraded=True", "eu-west-1", "az=c", "count=6"):
        assert fragment in message


def test_missing_instance_type_does_not_break_the_event():
    """An event built without instance_type falls back to 'unknown' instead of failing."""
    event = SpotProvisionOutcomeEvent(requested="Spot", realized="Spot", region="eu-west-1", availability_zone="a")
    assert event.instance_type == "unknown"


def test_no_internal_warning_when_no_events_device(caplog):
    """`provision-resources` runs in a process with no events device.

    Two noise regressions have come from this path already: publishing with a `default_logger` printed a
    NORMAL outcome as ERROR, and skipping publication entirely left the event flagged ready-to-publish so
    `SctEvent.__del__` warned "has not been published or dumped" once per cluster.
    """
    with caplog.at_level(logging.DEBUG):
        run_plan_real(make_plan("Spot", "OnDemand", results=[["i-123"]]))
        gc.collect()

    assert "[SCT internal warning]" not in caplog.text
    assert "Unable to get events main device" not in caplog.text


def test_outcome_logged_at_info_when_not_downgraded(caplog):
    """A spot success logs the provisioning outcome at INFO."""
    with caplog.at_level(logging.INFO, logger="sdcm.provision.common.provision_plan"):
        run_plan_real(make_plan("Spot", "OnDemand", results=[["i-123"]]))

    record = next(r for r in caplog.records if "Spot provisioning outcome" in r.message)
    assert record.levelno == logging.INFO


@pytest.mark.parametrize(
    "results",
    [
        pytest.param([[], ["i-1"]], id="downgrade"),
        pytest.param([[], []], id="total-failure"),
    ],
)
def test_downgrade_and_failure_logged_at_warning(caplog, results):
    """A silent spot->on-demand downgrade is the thing being measured; it must not hide at INFO."""
    with caplog.at_level(logging.INFO, logger="sdcm.provision.common.provision_plan"):
        run_plan_real(make_plan("Spot", "OnDemand", results=results))

    record = next(r for r in caplog.records if "Spot provisioning outcome" in r.message)
    assert record.levelno == logging.WARNING


def test_outcome_is_recorded_for_handoff():
    """A spot success is appended to the handoff list with realized set to Spot."""
    provision_for_handoff([["i-1"]])
    assert len(TestConfig.SPOT_PROVISION_OUTCOMES) == 1
    assert TestConfig.SPOT_PROVISION_OUTCOMES[0]["realized"] == "Spot"


def test_downgrade_recorded_with_warning_severity():
    """A silent downgrade at NORMAL would be invisible in Argus - that is the case being measured."""
    provision_for_handoff([[], ["i-1"]])
    outcome = TestConfig.SPOT_PROVISION_OUTCOMES[0]
    assert outcome["downgraded"] is True
    assert outcome["severity"] == "WARNING"


def test_message_is_not_double_prefixed():
    """The handoff message does not repeat the SpotProvisionOutcomeEvent class name."""
    provision_for_handoff([["i-1"]])
    assert TestConfig.SPOT_PROVISION_OUTCOMES[0]["message"].count("SpotProvisionOutcomeEvent") == 0


def test_submitter_drains_and_submits():
    """The Argus submitter sends one WARNING payload per downgraded outcome and empties the handoff list."""
    provision_for_handoff([[], ["i-1"]])
    test_config = MagicMock()
    client = MagicMock()
    test_config.argus_client.return_value = client
    sct._report_spot_provision_outcomes_to_argus(params=MagicMock(), test_config=test_config)

    assert client.submit_event.call_count == 1
    payload = client.submit_event.call_args[0][0]
    assert payload["event_type"] == "SpotProvisionOutcomeEvent"
    assert payload["severity"] == "WARNING"
    assert "downgraded=True" in payload["message"]
    assert TestConfig.SPOT_PROVISION_OUTCOMES == [], "must drain, or a retry double-reports"


def test_argus_failure_never_breaks_provisioning():
    """Provisioning already succeeded by this point; reporting must not turn it into a failure."""
    provision_for_handoff([["i-1"]])
    test_config = MagicMock()
    test_config.init_argus_client.side_effect = RuntimeError("argus down")
    sct._report_spot_provision_outcomes_to_argus(params=MagicMock(), test_config=test_config)


def test_submit_event_failure_is_swallowed():
    """An exception raised by submit_event is caught and does not escape the reporter."""
    provision_for_handoff([["i-1"]])
    test_config = MagicMock()
    test_config.argus_client.return_value.submit_event.side_effect = RuntimeError("boom")
    sct._report_spot_provision_outcomes_to_argus(params=MagicMock(), test_config=test_config)


def test_nothing_submitted_when_no_outcomes():
    """With an empty handoff list the reporter never initialises the Argus client."""
    TestConfig.SPOT_PROVISION_OUTCOMES.clear()
    test_config = MagicMock()
    sct._report_spot_provision_outcomes_to_argus(params=MagicMock(), test_config=test_config)
    test_config.init_argus_client.assert_not_called()


def test_missing_placement_fields_render_as_unknown():
    """`record_spot_provision_outcome` accepts None for region/AZ/type, so the event must handle it.

    The annotations used to promise `str` while the recorder's defaults were None, which `str.format`
    papered over by rendering a bare "None" into the message. Normalising here keeps the message readable
    and stops the type signature lying to whoever next reaches for `region` expecting a string.
    """
    event = SpotProvisionOutcomeEvent(requested="spot", realized="spot")

    assert event.region == "unknown"
    assert event.availability_zone == "unknown"
    assert event.instance_type == "unknown"
    assert "None" not in str(event)
