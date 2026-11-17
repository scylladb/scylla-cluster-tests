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

"""GCE provider: a zone abort resolves its in-flight inserts, and a taken name is not retried (SCT-1551)."""

from unittest.mock import MagicMock, patch

import google.api_core.exceptions
import pytest

from sdcm.keystore import SSHKey
from sdcm.provision.gce.instance_provider import VirtualMachineProvider
from sdcm.provision.provisioner import (
    InstanceNameTakenError,
    InstanceDefinition,
    PricingModel,
    ZoneResourcesExhaustedError,
)
from sdcm.sct_provision.instances_provider import provision_with_retry

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


def test_zone_exhaustion_mid_batch_drains_the_operations_still_in_flight(vm_provider):
    """SCT-1551: the zone fallback deletes this zone's VMs and recreates the same names elsewhere.

    A delete sent while an insert still runs queues behind it and can outlive its wait, so the next
    zone's insert of that name gets AlreadyExists. The inserts must be resolved before the raise.
    """
    definitions = [_definition(name=f"node-{index}") for index in range(1, 4)]
    operations = [MagicMock() for _ in definitions]

    def wait(operation, _description, **_):
        if operation is operations[0]:
            raise google.api_core.exceptions.BadRequest("ZONE_RESOURCE_POOL_EXHAUSTED")

    with (
        patch.object(vm_provider, "_build_and_insert_instance", side_effect=operations),
        patch("sdcm.provision.gce.instance_provider.wait_for_extended_operation", side_effect=wait) as waited,
        patch.object(vm_provider, "delete"),
    ):
        with pytest.raises(ZoneResourcesExhaustedError):
            vm_provider.get_or_create(definitions=definitions, pricing_model=PricingModel.ON_DEMAND)

    # node-2 and node-3 came up after the abort, so the zone cleanup deletes them with no insert in the way.
    assert [call.args[0] for call in waited.call_args_list] == operations
    assert sorted(vm_provider._cache) == ["node-2", "node-3"]


def test_taken_instance_name_is_not_retried(vm_provider, monkeypatch):
    """SCT-1551: a VM left in another zone holds the name; retrying it 5 s later can only get 409 again."""
    monkeypatch.setattr("time.sleep", lambda _: None)
    taken = google.api_core.exceptions.Conflict("The resource 'node-1' already exists")
    provisioner = MagicMock()
    provisioner.get_or_create_instances.side_effect = vm_provider.get_or_create

    with patch.object(vm_provider, "_build_and_insert_instance", side_effect=taken):
        with pytest.raises(InstanceNameTakenError):
            provision_with_retry(provisioner, definitions=[_definition()], pricing_model=PricingModel.ON_DEMAND)

    assert provisioner.get_or_create_instances.call_count == 1
