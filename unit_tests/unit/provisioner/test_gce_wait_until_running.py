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

"""SCT-1115: a GCE instance is handed back only once it is RUNNING.

A completed insert operation leaves the VM PROVISIONING or STAGING for a while. On real GCE that
is seconds; on minicloud it lasts as long as an uncached image takes to download - over 10 minutes
for the monitor image - and the cloud-init wait that runs next gives up long before that.
"""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.keystore import SSHKey
from sdcm.provision.gce import instance_provider
from sdcm.provision.gce.instance_provider import VirtualMachineProvider
from sdcm.provision.provisioner import InstanceDefinition, OperationPreemptedError, PricingModel, ProvisionError


FAKE_SSH_KEY = SSHKey(name="test_key", public_key=b"ssh-rsa AAAA fake\n", private_key=b"fake-private\n")


def _definition(name: str = "node-1") -> InstanceDefinition:
    return InstanceDefinition(
        name=name,
        image_id="projects/scylla-images/global/images/test-image",
        type="n2-standard-2",
        user_name="scylla-test",
        ssh_key=FAKE_SSH_KEY,
        root_disk_size=50,
        root_disk_type="pd-ssd",
    )


def _instance(status: str) -> MagicMock:
    instance = MagicMock()
    instance.status = status
    return instance


class FakeClock:
    """Stands in for time.monotonic/time.sleep, so a 20-minute wait takes no real time."""

    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture(name="clock")
def fixture_clock(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(instance_provider.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(instance_provider.time, "sleep", clock.sleep)
    return clock


@pytest.fixture(name="vm_provider")
def fixture_vm_provider():
    with patch("sdcm.provision.gce.instance_provider.get_gce_compute_instances_client") as client:
        client.return_value = (MagicMock(), {"project_id": "test-project"})
        provider = VirtualMachineProvider(
            project_id="test-project",
            zone="us-east1-b",
            test_id="test-123",
            disk_provider=MagicMock(),
            network_provider=MagicMock(),
        )
    with (
        patch.object(provider, "_build_and_insert_instance", return_value=MagicMock()),
        patch("sdcm.provision.gce.instance_provider.wait_for_extended_operation"),
        patch.object(provider, "_set_instance_labels"),
    ):
        yield provider


def _create(provider: VirtualMachineProvider, pricing_model: PricingModel = PricingModel.ON_DEMAND):
    return provider.get_or_create(definitions=[_definition()], pricing_model=pricing_model)


def test_running_instance_is_returned_without_waiting(vm_provider, clock):
    """Real GCE: the instance is RUNNING by the time the insert operation completes."""
    running = _instance("RUNNING")
    vm_provider._instances_client.get.return_value = running

    assert _create(vm_provider) == [running]
    assert not clock.sleeps
    assert vm_provider._cache["node-1"] is running


def test_staging_instance_is_waited_for_until_running(vm_provider, clock):
    """minicloud with a cold image cache: STAGING for the whole download, then RUNNING."""
    running = _instance("RUNNING")
    vm_provider._instances_client.get.side_effect = [
        _instance("PROVISIONING"),
        *[_instance("STAGING") for _ in range(65)],  # ~11 minutes, what the monitor image took
        running,
    ]

    assert _create(vm_provider) == [running]
    assert len(clock.sleeps) == 66
    # The instance that is cached and handed on is the RUNNING one, not an early STAGING snapshot.
    assert vm_provider._cache["node-1"] is running


@pytest.mark.parametrize("status", ["TERMINATED", "STOPPING", "SUSPENDED"])
def test_instance_that_cannot_start_fails_without_waiting_and_is_deleted(vm_provider, clock, status):
    vm_provider._instances_client.get.return_value = _instance(status)

    with (
        patch.object(vm_provider, "delete") as delete,
        pytest.raises(ProvisionError, match=f"node-1 did not start: its status is {status}"),
    ):
        _create(vm_provider)

    assert not clock.sleeps
    # Deleted, so the ProvisionError retry can create the name again instead of hitting AlreadyExists.
    delete.assert_called_once_with("node-1", wait=True)
    assert "node-1" not in vm_provider._cache


def test_instance_that_never_leaves_staging_times_out_and_is_deleted(vm_provider, clock):
    vm_provider._instances_client.get.return_value = _instance("STAGING")

    with (
        patch.object(vm_provider, "delete") as delete,
        pytest.raises(ProvisionError, match=r"node-1 did not start: still STAGING after 1200s"),
    ):
        _create(vm_provider)

    assert clock.now >= instance_provider.INSTANCE_RUNNING_TIMEOUT
    assert (
        len(clock.sleeps)
        == instance_provider.INSTANCE_RUNNING_TIMEOUT // instance_provider.INSTANCE_RUNNING_POLL_INTERVAL
    )
    delete.assert_called_once_with("node-1", wait=True)


def test_retry_path_waits_for_running_too(vm_provider, clock):
    """`_create_instance_with_retry` re-creates a failed VM; the one it hands back must be RUNNING as well."""
    running = _instance("RUNNING")
    vm_provider._instances_client.get.side_effect = [_instance("STAGING"), running]

    assert vm_provider._create_instance_with_retry(_definition(), PricingModel.ON_DEMAND) is running
    assert len(clock.sleeps) == 1


@pytest.mark.parametrize("status", ["TERMINATED", "STOPPING"])
def test_spot_instance_preempted_while_booting_raises_preempted(vm_provider, clock, status):
    """A preempted spot VM must reach the on-demand fallback, which only reacts to OperationPreemptedError."""
    vm_provider._instances_client.get.return_value = _instance(status)

    with (
        patch.object(vm_provider, "delete") as delete,
        pytest.raises(OperationPreemptedError, match=f"node-1 preempted before it started: its status is {status}"),
    ):
        _create(vm_provider, PricingModel.SPOT)

    assert not clock.sleeps
    delete.assert_called_once_with("node-1", wait=True)


def test_spot_instance_that_never_leaves_staging_is_not_treated_as_preempted(vm_provider, clock):
    vm_provider._instances_client.get.return_value = _instance("STAGING")

    with patch.object(vm_provider, "delete"), pytest.raises(ProvisionError, match="still STAGING"):
        _create(vm_provider, PricingModel.SPOT)


def test_failed_start_drains_the_rest_of_the_batch(vm_provider, clock):
    """A VM that never starts must not abandon its siblings' in-flight inserts.

    The sibling is waited out until RUNNING and cached, so the ProvisionError retry reuses it
    instead of re-issuing an insert for a name GCE already holds (AlreadyExists).
    """
    running = _instance("RUNNING")
    statuses = {"node-1": iter([_instance("TERMINATED")]), "node-2": iter([_instance("STAGING"), running])}
    vm_provider._instances_client.get.side_effect = lambda project, zone, instance: next(statuses[instance])

    with patch.object(vm_provider, "delete") as delete, pytest.raises(ProvisionError, match="node-1 did not start"):
        vm_provider.get_or_create(
            definitions=[_definition("node-1"), _definition("node-2")], pricing_model=PricingModel.ON_DEMAND
        )

    delete.assert_called_once_with("node-1", wait=True)
    # Cached only once RUNNING, not as the STAGING snapshot the insert operation left behind.
    assert vm_provider._cache["node-2"] is running
    assert "node-1" not in vm_provider._cache


def test_drained_instance_that_does_not_start_is_not_cached(vm_provider, clock):
    statuses = {"node-1": iter([_instance("TERMINATED")]), "node-2": iter([_instance("SUSPENDED")])}
    vm_provider._instances_client.get.side_effect = lambda project, zone, instance: next(statuses[instance])

    with patch.object(vm_provider, "delete") as delete, pytest.raises(ProvisionError, match="node-1 did not start"):
        vm_provider.get_or_create(
            definitions=[_definition("node-1"), _definition("node-2")], pricing_model=PricingModel.ON_DEMAND
        )

    # Each VM is deleted exactly once, by _wait_until_running; the drain does not delete it again.
    assert [call.args[0] for call in delete.call_args_list] == ["node-1", "node-2"]
    assert not vm_provider._cache
