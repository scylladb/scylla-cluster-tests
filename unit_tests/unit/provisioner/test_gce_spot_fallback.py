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

"""GCE spot provisioning: the SPOT scheduling policy, and the spot -> on-demand fallback.

The two halves belong together: the fallback is only reachable if the VM was actually requested as
spot in the first place, and a family whose scheduling silently drops the SPOT provisioning model
would make the whole path look covered while it never runs.
"""

from unittest.mock import MagicMock, patch

import google.api_core.exceptions
import pytest

from sdcm.keystore import SSHKey
from sdcm.provision.gce.instance_provider import VirtualMachineProvider, build_scheduling
from sdcm.provision.provisioner import (
    InstanceDefinition,
    OperationPreemptedError,
    PricingModel,
    ProvisionError,
    ZoneResourcesExhaustedError,
)
from sdcm.sct_provision.instances_provider import provision_instances_with_fallback


FAKE_SSH_KEY = SSHKey(name="test_key", public_key=b"ssh-rsa AAAA fake\n", private_key=b"fake-private\n")


def _definition(instance_type: str = "n2-standard-2", name: str = "node-1") -> InstanceDefinition:
    return InstanceDefinition(
        name=name,
        image_id="projects/scylla-images/global/images/test-image",
        type=instance_type,
        user_name="scylla-test",
        ssh_key=FAKE_SSH_KEY,
        root_disk_size=50,
        root_disk_type="pd-ssd",
    )


# --------------------------------------------------------------------------- SPOT scheduling policy


@pytest.mark.parametrize(
    "instance_type",
    [
        "n2-standard-2",
        # e2 does support spot; its non-spot MIGRATE policy used to be applied first and swallowed
        # the spot request entirely, handing back an on-demand VM the test believed was spot.
        "e2-standard-8",
        "n4-standard-16",
    ],
)
def test_spot_pricing_sets_the_spot_provisioning_model(instance_type):
    scheduling = build_scheduling(instance_type, PricingModel.SPOT)

    assert scheduling.provisioning_model == "SPOT"
    # A spot VM cannot live-migrate, so TERMINATE is mandatory even for families that prefer MIGRATE.
    assert scheduling.on_host_maintenance == "TERMINATE"
    assert scheduling.instance_termination_action == "STOP"


@pytest.mark.parametrize("pricing_model", [PricingModel.SPOT, PricingModel.ON_DEMAND], ids=["spot", "on-demand"])
def test_bundled_local_ssd_family_is_never_requested_as_spot(pricing_model):
    """z3 has no spot form: GCE rejects the insert with "OnHostMaintenance must be set to MIGRATE".

    Asking for it anyway fails provisioning outright, so a spot request is served on-demand instead.
    """
    scheduling = build_scheduling("z3-highmem-8-highlssd", pricing_model)

    assert scheduling.on_host_maintenance == "MIGRATE"
    assert not scheduling.provisioning_model
    assert not scheduling.instance_termination_action


def test_e2_keeps_migrate_when_not_spot():
    scheduling = build_scheduling("e2-standard-8", PricingModel.ON_DEMAND)

    assert scheduling.on_host_maintenance == "MIGRATE"
    assert not scheduling.provisioning_model


def test_ordinary_family_on_demand_does_not_migrate():
    scheduling = build_scheduling("n2-standard-2", PricingModel.ON_DEMAND)

    assert scheduling.on_host_maintenance == "TERMINATE"
    assert scheduling.automatic_restart is False
    assert not scheduling.provisioning_model


# ------------------------------------------------------------------- spot -> on-demand fallback


@pytest.fixture(autouse=True)
def _skip_post_provision_ssh():
    """The fallback decision is made before any VM is reachable; stub the cloud-init wait out."""
    with (
        patch("sdcm.sct_provision.instances_provider.RemoteCmdRunnerBase"),
        patch("sdcm.sct_provision.instances_provider.wait_cloud_init_completes"),
    ):
        yield


def _provisioner(*, create_side_effect=None):
    provisioner = MagicMock()
    instance = MagicMock(public_ip_address="1.2.3.4", private_ip_address="10.0.0.1")
    provisioner.get_or_create_instances.side_effect = create_side_effect or (lambda **_: [instance])
    return provisioner


def _pricing_models_requested(provisioner):
    return [call.kwargs.get("pricing_model") for call in provisioner.get_or_create_instances.call_args_list]


def test_preempted_spot_is_reprovisioned_on_demand():
    """A spot VM reclaimed mid-provisioning re-issues the request at on-demand pricing."""
    calls = []

    def create(**kwargs):
        calls.append(kwargs.get("pricing_model"))
        if kwargs.get("pricing_model") is PricingModel.SPOT:
            raise OperationPreemptedError("Instance failed to start due to preemption.")
        return [MagicMock(public_ip_address="1.2.3.4", private_ip_address="10.0.0.1")]

    provisioner = _provisioner(create_side_effect=create)

    provision_instances_with_fallback(
        provisioner,
        definitions=[MagicMock(use_public_ip=True)],
        pricing_model=PricingModel.SPOT,
        fallback_on_demand=True,
    )

    # The failed spot attempt, the on-demand retry, and the final collect call - which must not ask
    # for spot again, or a definition missing from the provisioner cache is re-created at the very
    # pricing that just failed.
    assert calls == [PricingModel.SPOT, PricingModel.ON_DEMAND, PricingModel.ON_DEMAND]


def test_preemption_is_raised_when_fallback_is_disabled():
    provisioner = _provisioner(
        create_side_effect=OperationPreemptedError("Instance failed to start due to preemption.")
    )

    with pytest.raises(OperationPreemptedError):
        provision_instances_with_fallback(
            provisioner,
            definitions=[MagicMock(use_public_ip=True)],
            pricing_model=PricingModel.SPOT,
            fallback_on_demand=False,
        )


def test_preemption_of_an_on_demand_request_is_not_retried_as_on_demand():
    """Guard the `is_spot()` condition: an on-demand request has nothing cheaper to fall back to."""
    provisioner = _provisioner(create_side_effect=OperationPreemptedError("Code: PREEMPTED"))

    with pytest.raises(OperationPreemptedError):
        provision_instances_with_fallback(
            provisioner,
            definitions=[MagicMock(use_public_ip=True)],
            pricing_model=PricingModel.ON_DEMAND,
            fallback_on_demand=True,
        )
    assert provisioner.get_or_create_instances.call_count == 1


def test_successful_spot_run_collects_instances_as_spot():
    """No preemption: the collect call must not silently downgrade the run to on-demand."""
    provisioner = _provisioner()

    provision_instances_with_fallback(
        provisioner,
        definitions=[MagicMock(use_public_ip=True)],
        pricing_model=PricingModel.SPOT,
        fallback_on_demand=True,
    )

    assert set(_pricing_models_requested(provisioner)) == {PricingModel.SPOT}


def test_non_preemption_failure_still_propagates(monkeypatch):
    """Only preemption routes to on-demand; an ordinary ProvisionError keeps failing the run."""
    monkeypatch.setattr("time.sleep", lambda _: None)
    provisioner = _provisioner(create_side_effect=ProvisionError("transient API failure"))

    with pytest.raises(ProvisionError):
        provision_instances_with_fallback(
            provisioner,
            definitions=[MagicMock(use_public_ip=True)],
            pricing_model=PricingModel.SPOT,
            fallback_on_demand=True,
        )


# ----------------------------------------- GCE provider: preemption must reach the fallback caller


@pytest.fixture
def vm_provider():
    """A VirtualMachineProvider with its GCE clients stubbed out."""
    with patch("sdcm.provision.gce.instance_provider.get_gce_compute_instances_client") as client:
        client.return_value = (MagicMock(), {"project_id": "test-project"})
        yield VirtualMachineProvider(
            project_id="test-project",
            zone="us-east1-b",
            test_id="test-123",
            disk_provider=MagicMock(),
            network_provider=MagicMock(),
        )


PREEMPTION_ERROR = google.api_core.exceptions.BadRequest("Instance failed to start due to preemption.")


def test_preemption_on_insert_is_reported_as_preempted(vm_provider):
    """A preemption must not be reported as zone exhaustion - the zone is fine, the pricing is not."""
    with patch.object(vm_provider, "_build_and_insert_instance", side_effect=PREEMPTION_ERROR):
        with pytest.raises(OperationPreemptedError):
            vm_provider.get_or_create(definitions=[_definition()], pricing_model=PricingModel.SPOT)


def test_preemption_while_waiting_is_not_retried_as_spot(vm_provider):
    """The generic retry sleeps 15 minutes between attempts; spot must skip it and drop to on-demand."""
    with (
        patch.object(vm_provider, "_build_and_insert_instance", return_value=MagicMock()),
        patch("sdcm.provision.gce.instance_provider.wait_for_extended_operation", side_effect=PREEMPTION_ERROR) as wait,
        patch.object(vm_provider, "delete"),
    ):
        with pytest.raises(OperationPreemptedError):
            vm_provider.get_or_create(definitions=[_definition()], pricing_model=PricingModel.SPOT)

    assert wait.call_count == 1


def test_preemption_of_an_on_demand_vm_keeps_the_generic_handling(vm_provider):
    """`is_spot()` gates the classification: an on-demand VM has no cheaper model to retry with."""
    with patch.object(vm_provider, "_build_and_insert_instance", side_effect=PREEMPTION_ERROR):
        with pytest.raises(ProvisionError):
            vm_provider.get_or_create(definitions=[_definition()], pricing_model=PricingModel.ON_DEMAND)


def test_zone_exhaustion_is_still_zone_exhaustion_for_spot(vm_provider):
    """Guard the new branch: a capacity shortage must keep routing to zone/region fallback."""
    exhausted = google.api_core.exceptions.BadRequest("ZONE_RESOURCE_POOL_EXHAUSTED")
    with patch.object(vm_provider, "_build_and_insert_instance", side_effect=exhausted):
        with pytest.raises(ZoneResourcesExhaustedError):
            vm_provider.get_or_create(definitions=[_definition()], pricing_model=PricingModel.SPOT)


# `Code: PREEMPTED` is the form GCE uses when the VM is reclaimed after the insert was accepted; the
# sentence form comes back on the insert request itself. Both have to be handled the same way.
PREEMPTED_CODE_ERROR = google.api_core.exceptions.BadRequest("Code: PREEMPTED")


@pytest.mark.parametrize("error", [PREEMPTION_ERROR, PREEMPTED_CODE_ERROR], ids=["sentence-form", "code-form"])
def test_preempted_spot_vm_is_deleted_whichever_form_gce_reports(vm_provider, error):
    """The stopped spot VM has to go, or the on-demand retry gets AlreadyExists for its name."""
    with (
        patch.object(vm_provider, "_build_and_insert_instance", return_value=MagicMock()),
        patch("sdcm.provision.gce.instance_provider.wait_for_extended_operation", side_effect=error),
        patch.object(vm_provider, "delete") as delete,
    ):
        with pytest.raises(OperationPreemptedError):
            vm_provider.get_or_create(definitions=[_definition()], pricing_model=PricingModel.SPOT)

    delete.assert_called_once_with("node-1", wait=True)


def test_preemption_mid_batch_drains_the_operations_still_in_flight(vm_provider):
    """A multi-node batch aborts on the first preemption, but not before resolving what it submitted.

    An insert that is never awaited leaves a name that is neither cached nor free, so the on-demand
    retry would ask GCE to create it again and get AlreadyExists instead of a VM.
    """
    definitions = [_definition(name=f"node-{index}") for index in range(1, 4)]
    operations = [MagicMock() for _ in definitions]

    def wait(operation, _description):
        if operation is operations[1]:
            raise PREEMPTION_ERROR

    with (
        patch.object(vm_provider, "_build_and_insert_instance", side_effect=operations),
        patch("sdcm.provision.gce.instance_provider.wait_for_extended_operation", side_effect=wait) as waited,
        patch.object(vm_provider, "delete"),
    ):
        with pytest.raises(OperationPreemptedError):
            vm_provider.get_or_create(definitions=definitions, pricing_model=PricingModel.SPOT)

    # node-3 was submitted before the abort, so its operation is waited out too and cached.
    assert [call.args[0] for call in waited.call_args_list] == operations
    assert sorted(vm_provider._cache) == ["node-1", "node-3"]
