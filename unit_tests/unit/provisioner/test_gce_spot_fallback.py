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
    InstanceConfigurationError,
    InstanceDefinition,
    InstanceNameTakenError,
    OperationPreemptedError,
    PricingModel,
    ProvisionError,
    ZoneResourcesExhaustedError,
)
from sdcm.sct_provision.instances_provider import provision_instances_with_fallback, provision_with_retry

FAKE_SSH_KEY = SSHKey(name="test_key", public_key=b"ssh-rsa AAAA fake\n", private_key=b"fake-private\n")
PREEMPTION_ERROR = google.api_core.exceptions.BadRequest("Instance failed to start due to preemption.")
# `Code: PREEMPTED` is the form GCE uses when the VM is reclaimed after the insert was accepted; the
# sentence form comes back on the insert request itself. Both have to be handled the same way.
PREEMPTED_CODE_ERROR = google.api_core.exceptions.BadRequest("Code: PREEMPTED")


@pytest.fixture
def make_definition():
    """Return a factory for a node definition with a fake SSH key and image."""

    def build(instance_type: str = "n2-standard-2", name: str = "node-1") -> InstanceDefinition:
        return InstanceDefinition(
            name=name,
            image_id="projects/scylla-images/global/images/test-image",
            type=instance_type,
            user_name="scylla-test",
            ssh_key=FAKE_SSH_KEY,
            root_disk_size=50,
            root_disk_type="pd-ssd",
        )

    return build


@pytest.fixture(autouse=True)
def skip_post_provision_ssh():
    """The fallback decision is made before any VM is reachable; stub the cloud-init wait out."""
    with (
        patch("sdcm.sct_provision.instances_provider.RemoteCmdRunnerBase"),
        patch("sdcm.sct_provision.instances_provider.wait_cloud_init_completes"),
    ):
        yield


@pytest.fixture
def make_provisioner():
    """Return a factory for a provisioner mock; it returns one instance unless create_side_effect is given."""

    def build(*, create_side_effect=None):
        provisioner = MagicMock()
        instance = MagicMock(public_ip_address="1.2.3.4", private_ip_address="10.0.0.1")
        provisioner.get_or_create_instances.side_effect = create_side_effect or (lambda **_: [instance])
        return provisioner

    return build


def pricing_models_requested(provisioner):
    """Return the pricing_model of each get_or_create_instances call, in call order."""
    return [call.kwargs.get("pricing_model") for call in provisioner.get_or_create_instances.call_args_list]


@pytest.fixture
def vm_provider():
    """A VirtualMachineProvider with its GCE clients stubbed out."""
    with patch("sdcm.provision.gce.instance_provider.get_gce_compute_instances_client") as client:
        instances_client = MagicMock()
        instances_client.get.return_value.status = "RUNNING"
        client.return_value = (instances_client, {"project_id": "test-project"})
        yield VirtualMachineProvider(
            project_id="test-project",
            zone="us-east1-b",
            test_id="test-123",
            disk_provider=MagicMock(),
            network_provider=MagicMock(),
        )


# --------------------------------------------------------------------------- SPOT scheduling policy


@pytest.mark.parametrize(
    "instance_type",
    [
        pytest.param("n2-standard-2", id="n2-standard-2"),
        # e2 does support spot; its non-spot MIGRATE policy used to be applied first and swallowed
        # the spot request entirely, handing back an on-demand VM the test believed was spot.
        pytest.param("e2-standard-8", id="e2-standard-8"),
        pytest.param("n4-standard-16", id="n4-standard-16"),
    ],
)
def test_spot_pricing_sets_the_spot_provisioning_model(instance_type):
    """SPOT pricing sets the SPOT provisioning model and TERMINATE maintenance for every family."""
    scheduling = build_scheduling(instance_type, PricingModel.SPOT)

    assert scheduling.provisioning_model == "SPOT"
    # A spot VM cannot live-migrate, so TERMINATE is mandatory even for families that prefer MIGRATE.
    assert scheduling.on_host_maintenance == "TERMINATE"
    assert scheduling.instance_termination_action == "STOP"


@pytest.mark.parametrize(
    "pricing_model",
    [
        pytest.param(PricingModel.SPOT, id="spot"),
        pytest.param(PricingModel.ON_DEMAND, id="on-demand"),
    ],
)
@pytest.mark.parametrize(
    "instance_type",
    [
        pytest.param("z3-highmem-8-highlssd", id="z3-highmem-8-highlssd"),
        pytest.param("z3-highmem-88", id="z3-highmem-88"),
        pytest.param("z4d-highmem-16-highlssd", id="z4d-highmem-16-highlssd"),
        pytest.param("z4d-highmem-16-standardlssd", id="z4d-highmem-16-standardlssd"),
    ],
)
def test_bundled_local_ssd_family_is_never_requested_as_spot(instance_type, pricing_model):
    """z3 and z4d have no spot form: GCE rejects the insert with "OnHostMaintenance must be set to MIGRATE"
    (z3), or "Scheduling must have onHostMaintenance be one of the following valid types: [MIGRATE]" (z4d).

    Asking for it anyway fails provisioning outright, so a spot request is served on-demand instead.
    """
    scheduling = build_scheduling(instance_type, pricing_model)

    assert scheduling.on_host_maintenance == "MIGRATE"
    assert not scheduling.provisioning_model
    assert not scheduling.instance_termination_action


def test_e2_keeps_migrate_when_not_spot():
    """ON_DEMAND e2 keeps the MIGRATE maintenance policy and requests no provisioning model."""
    scheduling = build_scheduling("e2-standard-8", PricingModel.ON_DEMAND)

    assert scheduling.on_host_maintenance == "MIGRATE"
    assert not scheduling.provisioning_model


def test_ordinary_family_on_demand_does_not_migrate():
    """ON_DEMAND ordinary family terminates on host maintenance and does not restart automatically."""
    scheduling = build_scheduling("n2-standard-2", PricingModel.ON_DEMAND)

    assert scheduling.on_host_maintenance == "TERMINATE"
    assert scheduling.automatic_restart is False
    assert not scheduling.provisioning_model


# ------------------------------------------------------------------- spot -> on-demand fallback


def test_preempted_spot_is_reprovisioned_on_demand(make_provisioner):
    """A spot VM reclaimed mid-provisioning re-issues the request at on-demand pricing."""
    calls = []

    def create(**kwargs):
        calls.append(kwargs.get("pricing_model"))
        if kwargs.get("pricing_model") is PricingModel.SPOT:
            raise OperationPreemptedError("Instance failed to start due to preemption.")
        return [MagicMock(public_ip_address="1.2.3.4", private_ip_address="10.0.0.1")]

    provisioner = make_provisioner(create_side_effect=create)

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


def test_failed_on_demand_fallback_does_not_chain_the_preemption(make_provisioner):
    """`spot-retry-decision` reads `OperationPreemptedError` in the failure text as a spot reclaim."""

    def create(**kwargs):
        if kwargs.get("pricing_model") is PricingModel.SPOT:
            raise OperationPreemptedError("Instance failed to start due to preemption.")
        raise InstanceConfigurationError("quota exceeded")

    with pytest.raises(InstanceConfigurationError) as error:
        provision_instances_with_fallback(
            make_provisioner(create_side_effect=create),
            definitions=[MagicMock(use_public_ip=True)],
            pricing_model=PricingModel.SPOT,
            fallback_on_demand=True,
        )
    assert error.value.__context__ is None


def test_preemption_is_raised_when_fallback_is_disabled(make_provisioner):
    """Preemption propagates unchanged when fallback_on_demand is off."""
    provisioner = make_provisioner(
        create_side_effect=OperationPreemptedError("Instance failed to start due to preemption.")
    )

    with pytest.raises(OperationPreemptedError):
        provision_instances_with_fallback(
            provisioner,
            definitions=[MagicMock(use_public_ip=True)],
            pricing_model=PricingModel.SPOT,
            fallback_on_demand=False,
        )


def test_preemption_of_an_on_demand_request_is_not_retried_as_on_demand(make_provisioner):
    """Guard the `is_spot()` condition: an on-demand request has nothing cheaper to fall back to."""
    provisioner = make_provisioner(create_side_effect=OperationPreemptedError("Code: PREEMPTED"))

    with pytest.raises(OperationPreemptedError):
        provision_instances_with_fallback(
            provisioner,
            definitions=[MagicMock(use_public_ip=True)],
            pricing_model=PricingModel.ON_DEMAND,
            fallback_on_demand=True,
        )
    assert provisioner.get_or_create_instances.call_count == 1


def test_successful_spot_run_collects_instances_as_spot(make_provisioner):
    """No preemption: the collect call must not silently downgrade the run to on-demand."""
    provisioner = make_provisioner()

    provision_instances_with_fallback(
        provisioner,
        definitions=[MagicMock(use_public_ip=True)],
        pricing_model=PricingModel.SPOT,
        fallback_on_demand=True,
    )

    assert set(pricing_models_requested(provisioner)) == {PricingModel.SPOT}


def test_non_preemption_failure_still_propagates(make_provisioner, monkeypatch):
    """Only preemption routes to on-demand; an ordinary ProvisionError keeps failing the run."""
    monkeypatch.setattr("time.sleep", lambda _: None)
    provisioner = make_provisioner(create_side_effect=ProvisionError("transient API failure"))

    with pytest.raises(ProvisionError):
        provision_instances_with_fallback(
            provisioner,
            definitions=[MagicMock(use_public_ip=True)],
            pricing_model=PricingModel.SPOT,
            fallback_on_demand=True,
        )


# ----------------------------------------- GCE provider: preemption must reach the fallback caller


def test_preemption_on_insert_is_reported_as_preempted(vm_provider, make_definition):
    """A preemption must not be reported as zone exhaustion - the zone is fine, the pricing is not."""
    with (
        patch.object(vm_provider, "_build_and_insert_instance", side_effect=PREEMPTION_ERROR),
        pytest.raises(OperationPreemptedError),
    ):
        vm_provider.get_or_create(definitions=[make_definition()], pricing_model=PricingModel.SPOT)


def test_preemption_while_waiting_is_not_retried_as_spot(vm_provider, make_definition):
    """The generic retry sleeps 15 minutes between attempts; spot must skip it and drop to on-demand."""
    with (
        patch.object(vm_provider, "_build_and_insert_instance", return_value=MagicMock()),
        patch("sdcm.provision.gce.instance_provider.wait_for_extended_operation", side_effect=PREEMPTION_ERROR) as wait,
        patch.object(vm_provider, "delete"),
        pytest.raises(OperationPreemptedError),
    ):
        vm_provider.get_or_create(definitions=[make_definition()], pricing_model=PricingModel.SPOT)

    assert wait.call_count == 1


def test_preemption_of_an_on_demand_vm_keeps_the_generic_handling(vm_provider, make_definition):
    """`is_spot()` gates the classification: an on-demand VM has no cheaper model to retry with."""
    with (
        patch.object(vm_provider, "_build_and_insert_instance", side_effect=PREEMPTION_ERROR),
        pytest.raises(ProvisionError),
    ):
        vm_provider.get_or_create(definitions=[make_definition()], pricing_model=PricingModel.ON_DEMAND)


def test_zone_exhaustion_is_still_zone_exhaustion_for_spot(vm_provider, make_definition):
    """Guard the new branch: a capacity shortage must keep routing to zone/region fallback."""
    exhausted = google.api_core.exceptions.BadRequest("ZONE_RESOURCE_POOL_EXHAUSTED")
    with (
        patch.object(vm_provider, "_build_and_insert_instance", side_effect=exhausted),
        pytest.raises(ZoneResourcesExhaustedError),
    ):
        vm_provider.get_or_create(definitions=[make_definition()], pricing_model=PricingModel.SPOT)


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(PREEMPTION_ERROR, id="sentence-form"),
        pytest.param(PREEMPTED_CODE_ERROR, id="code-form"),
    ],
)
def test_preempted_spot_vm_is_deleted_whichever_form_gce_reports(vm_provider, make_definition, error):
    """The stopped spot VM has to go, or the on-demand retry gets AlreadyExists for its name."""
    with (
        patch.object(vm_provider, "_build_and_insert_instance", return_value=MagicMock()),
        patch("sdcm.provision.gce.instance_provider.wait_for_extended_operation", side_effect=error),
        patch.object(vm_provider, "delete") as delete,
        pytest.raises(OperationPreemptedError),
    ):
        vm_provider.get_or_create(definitions=[make_definition()], pricing_model=PricingModel.SPOT)

    delete.assert_called_once_with("node-1", wait=True)


def test_preemption_mid_batch_drains_the_operations_still_in_flight(vm_provider, make_definition):
    """A multi-node batch aborts on the first preemption, but not before resolving what it submitted.

    An insert that is never awaited leaves a name that is neither cached nor free, so the on-demand
    retry would ask GCE to create it again and get AlreadyExists instead of a VM.
    """
    definitions = [make_definition(name=f"node-{index}") for index in range(1, 4)]
    operations = [MagicMock() for _ in definitions]

    def wait(operation, _description, **_):
        if operation is operations[1]:
            raise PREEMPTION_ERROR

    with (
        patch.object(vm_provider, "_build_and_insert_instance", side_effect=operations),
        patch("sdcm.provision.gce.instance_provider.wait_for_extended_operation", side_effect=wait) as waited,
        patch.object(vm_provider, "delete"),
        pytest.raises(OperationPreemptedError),
    ):
        vm_provider.get_or_create(definitions=definitions, pricing_model=PricingModel.SPOT)

    # node-3 was submitted before the abort, so its operation is waited out too and cached.
    assert [call.args[0] for call in waited.call_args_list] == operations
    assert sorted(vm_provider._cache) == ["node-1", "node-3"]


def test_zone_exhaustion_mid_batch_drains_the_operations_still_in_flight(vm_provider, make_definition):
    """SCT-1551: the zone fallback deletes this zone's VMs and recreates the same names elsewhere.

    A delete sent while an insert still runs queues behind it and can outlive its wait, so the next
    zone's insert of that name gets AlreadyExists. The inserts must be resolved before the raise.
    """
    definitions = [make_definition(name=f"node-{index}") for index in range(1, 4)]
    operations = [MagicMock() for _ in definitions]

    def wait(operation, _description, **_):
        if operation is operations[0]:
            raise google.api_core.exceptions.BadRequest("ZONE_RESOURCE_POOL_EXHAUSTED")

    with (
        patch.object(vm_provider, "_build_and_insert_instance", side_effect=operations),
        patch("sdcm.provision.gce.instance_provider.wait_for_extended_operation", side_effect=wait) as waited,
        patch.object(vm_provider, "delete"),
        pytest.raises(ZoneResourcesExhaustedError),
    ):
        vm_provider.get_or_create(definitions=definitions, pricing_model=PricingModel.ON_DEMAND)

    # node-2 and node-3 came up after the abort, so the zone cleanup deletes them with no insert in the way.
    assert [call.args[0] for call in waited.call_args_list] == operations
    assert sorted(vm_provider._cache) == ["node-2", "node-3"]


def test_taken_instance_name_is_not_retried(vm_provider, make_definition, monkeypatch):
    """SCT-1551: a VM left in another zone holds the name; retrying it 5 s later can only get 409 again."""
    monkeypatch.setattr("time.sleep", lambda _: None)
    taken = google.api_core.exceptions.Conflict("The resource 'node-1' already exists")
    provisioner = MagicMock()
    provisioner.get_or_create_instances.side_effect = vm_provider.get_or_create

    with (
        patch.object(vm_provider, "_build_and_insert_instance", side_effect=taken),
        pytest.raises(InstanceNameTakenError),
    ):
        provision_with_retry(provisioner, definitions=[make_definition()], pricing_model=PricingModel.ON_DEMAND)

    assert provisioner.get_or_create_instances.call_count == 1


@pytest.mark.parametrize(
    "drain_error",
    [
        pytest.param(TimeoutError("timed out"), id="timeout"),
        pytest.param(RuntimeError("operation failed"), id="runtime-error"),
    ],
)
def test_drain_errors_do_not_mask_the_preemption(vm_provider, make_definition, drain_error):
    """`wait_for_extended_operation` can also raise TimeoutError/RuntimeError; the drain must swallow those.

    Otherwise the drain error replaces OperationPreemptedError and the on-demand fallback never runs.
    """
    definitions = [make_definition(name=f"node-{index}") for index in range(1, 3)]
    operations = [MagicMock() for _ in definitions]

    def wait(operation, _description, **_):
        if operation is operations[0]:
            raise PREEMPTION_ERROR
        raise drain_error

    with (
        patch.object(vm_provider, "_build_and_insert_instance", side_effect=operations),
        patch("sdcm.provision.gce.instance_provider.wait_for_extended_operation", side_effect=wait),
        patch.object(vm_provider, "_release_instance_name") as released,
        pytest.raises(OperationPreemptedError),
    ):
        vm_provider.get_or_create(definitions=definitions, pricing_model=PricingModel.SPOT)

    # node-2 never came up, so its name is freed for the on-demand retry.
    released.assert_any_call("node-2", operations[1])
    assert "node-2" not in vm_provider._cache
