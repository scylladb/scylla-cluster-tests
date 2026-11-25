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

"""Spot-vs-on-demand observability on the `Provisioner` ABC path shared by GCE, Azure and OCI (SCT-896).

SCT-850 closed this blind spot on AWS only, because the event was published from `ProvisionPlan`, which no
other backend constructs. GCE/Azure/OCI go through `provision_instances_with_fallback()` instead, where a
spot->on-demand downgrade left no trace at all.
"""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.provision.provisioner import (
    InstanceDefinition,
    OperationPreemptedError,
    PricingModel,
    ProvisionError,
)
from sdcm.sct_provision.instances_provider import provision_instances_with_fallback


def make_definition(name: str, instance_type: str = "n2-highmem-16") -> InstanceDefinition:
    """Build an InstanceDefinition with a fixed image and user, and a mocked SSH key."""
    return InstanceDefinition(
        name=name,
        image_id="image-1",
        type=instance_type,
        user_name="scyllaadm",
        ssh_key=MagicMock(),
    )


def make_provisioner() -> MagicMock:
    """Build a mocked provisioner placed in us-east1-b."""
    provisioner = MagicMock()
    provisioner.region = "us-east1"
    provisioner.availability_zone = "b"
    return provisioner


def run_provision(pricing_model=PricingModel.SPOT, fallback_on_demand=True, side_effects=None):
    """Drive the shared path with a stubbed provisioner; return the recorded outcome kwargs."""
    provisioner = make_provisioner()
    definitions = [make_definition("node-1"), make_definition("node-2")]
    with (
        patch(
            "sdcm.sct_provision.instances_provider.provision_with_retry",
            side_effect=side_effects or [None],
        ),
        patch("sdcm.sct_provision.instances_provider.record_spot_provision_outcome") as recorder,
        patch("sdcm.sct_provision.instances_provider.wait_cloud_init_completes"),
        patch("sdcm.sct_provision.instances_provider.RemoteCmdRunnerBase"),
    ):
        provisioner.get_or_create_instances.return_value = []
        try:
            provision_instances_with_fallback(
                provisioner=provisioner,
                definitions=definitions,
                pricing_model=pricing_model,
                fallback_on_demand=fallback_on_demand,
            )
        except Exception as exc:  # noqa: BLE001
            return exc, recorder
    return None, recorder


def test_spot_success_records_no_downgrade():
    """A spot run that provisions on the first try is recorded as spot realized as spot, for every node."""
    error, recorder = run_provision()

    assert error is None
    kwargs = recorder.call_args.kwargs
    assert kwargs["requested"] == "spot"
    assert kwargs["realized"] == "spot"
    assert kwargs["count"] == 2


def test_silent_downgrade_to_on_demand_is_recorded():
    """The whole point: preemption during provisioning quietly falls back, and used to leave no trace."""
    error, recorder = run_provision(side_effects=[OperationPreemptedError("preempted"), None])

    assert error is None
    kwargs = recorder.call_args.kwargs
    assert kwargs["requested"] == "spot"
    assert kwargs["realized"] == "on_demand"


def test_total_failure_records_before_reraising():
    """This path raises rather than returning [], so the record must precede the raise."""
    error, recorder = run_provision(side_effects=[OperationPreemptedError("preempted"), ProvisionError("no capacity")])

    assert isinstance(error, ProvisionError), "the original error must still propagate"
    assert recorder.call_args.kwargs["realized"] is None


def test_failure_without_fallback_configured_is_recorded():
    """With fallback disabled, a preemption is re-raised and still recorded with no realized type."""
    error, recorder = run_provision(fallback_on_demand=False, side_effects=[OperationPreemptedError("preempted")])

    assert isinstance(error, OperationPreemptedError)
    assert recorder.call_args.kwargs["realized"] is None


def test_non_preemption_failure_is_recorded_too():
    """Only preemption triggers the on-demand fallback, but every failure is worth a record."""
    error, recorder = run_provision(side_effects=[ProvisionError("zone exhausted")])

    assert isinstance(error, ProvisionError)
    assert recorder.call_args.kwargs["realized"] is None


def test_on_demand_run_is_recorded_as_requested():
    """An on-demand run is not a downgrade; it has to be distinguishable from one in the numbers."""
    _, recorder = run_provision(pricing_model=PricingModel.ON_DEMAND)

    kwargs = recorder.call_args.kwargs
    assert kwargs["requested"] == "on_demand"
    assert kwargs["realized"] == "on_demand"


def test_record_carries_placement_and_machine_type():
    """Without these the record cannot be grouped by where capacity actually ran out."""
    _, recorder = run_provision()

    kwargs = recorder.call_args.kwargs
    assert kwargs["region"] == "us-east1"
    assert kwargs["availability_zone"] == "b"
    assert kwargs["instance_type"] == "n2-highmem-16"


@pytest.mark.parametrize(
    "recording_side_effect",
    [
        pytest.param(RuntimeError("boom"), id="recording-raises"),
        pytest.param(None, id="recording-succeeds"),
    ],
)
def test_recording_failure_never_breaks_provisioning(recording_side_effect):
    """`record_spot_provision_outcome` swallows its own errors; assert the contract holds from here too."""
    provisioner = make_provisioner()
    provisioner.get_or_create_instances.return_value = []
    with (
        patch("sdcm.sct_provision.instances_provider.provision_with_retry"),
        patch(
            "sdcm.provision.common.spot_outcome.SpotProvisionOutcomeEvent",
            side_effect=recording_side_effect,
        ),
        patch("sdcm.sct_provision.instances_provider.wait_cloud_init_completes"),
    ):
        assert (
            provision_instances_with_fallback(
                provisioner=provisioner,
                definitions=[make_definition("node-1")],
                pricing_model=PricingModel.SPOT,
                fallback_on_demand=True,
            )
            == []
        )


def test_mixed_batch_each_instance_type_is_reported_separately():
    """A region's batch is DB nodes + loaders + monitor together, not one machine type.

    Reported as a single record it takes the name of whichever definition sorts first, which on a failure
    reads as an accusation against the wrong machine type: a real run logged
    `instance_type=z3-highmem-8-highlssd count=9` for a *loader* that could not be created, and the
    natural reading of that line - "the DB type cannot do spot" - was wrong.
    """
    provisioner = make_provisioner()
    definitions = [
        make_definition("db-1", "z3-highmem-8-highlssd"),
        make_definition("db-2", "z3-highmem-8-highlssd"),
        make_definition("loader-1", "n4a-standard-4"),
        make_definition("monitor-1", "e2-standard-2"),
    ]

    with patch("sdcm.provision.common.spot_outcome.SpotProvisionOutcomeEvent") as event:
        provision_instances_with_fallback(
            provisioner, definitions=definitions, pricing_model=PricingModel.SPOT, fallback_on_demand=True
        )

    reported = {call.kwargs["instance_type"]: call.kwargs["count"] for call in event.call_args_list}
    assert reported == {"z3-highmem-8-highlssd": 2, "n4a-standard-4": 1, "e2-standard-2": 1}


def test_mixed_batch_failure_path_names_every_type():
    """The failure path is the one that misled a reader, so it is the one that most needs to be right."""
    provisioner = make_provisioner()
    provisioner.get_or_create_instances.side_effect = ProvisionError("ZONE_RESOURCE_POOL_EXHAUSTED")
    definitions = [make_definition("db-1", "z3-highmem-8-highlssd"), make_definition("loader-1", "n4a-standard-4")]

    with (
        patch("sdcm.provision.common.spot_outcome.SpotProvisionOutcomeEvent") as event,
        pytest.raises(ProvisionError),
    ):
        provision_instances_with_fallback(
            provisioner, definitions=definitions, pricing_model=PricingModel.SPOT, fallback_on_demand=False
        )

    reported = {call.kwargs["instance_type"] for call in event.call_args_list}
    assert reported == {"z3-highmem-8-highlssd", "n4a-standard-4"}
    assert all(call.kwargs["realized"] is None for call in event.call_args_list)
