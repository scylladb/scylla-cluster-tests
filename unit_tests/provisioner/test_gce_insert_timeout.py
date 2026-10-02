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

"""GCE insert that outlives its waiter: the VM it leaves behind must not keep its name taken.

The insert keeps running on GCE after `wait_for_extended_operation` times out. Left alone, it produces
a VM that SCT neither tracks nor deletes, and every later attempt to create a node of that name fails
with `409 AlreadyExists` - which is how one slow insert used to fail every following add-node nemesis.
"""

from unittest.mock import MagicMock, patch

import google.api_core.exceptions
import pytest

from sdcm.keystore import SSHKey
from sdcm.provision.gce.instance_provider import (
    ABANDONED_INSTANCE_DELETE_ATTEMPTS,
    INSTANCE_CREATION_TIMEOUT,
    VirtualMachineProvider,
)
from sdcm.provision.provisioner import InstanceDefinition, PricingModel, ProvisionError

FAKE_SSH_KEY = SSHKey(name="test_key", public_key=b"ssh-rsa AAAA fake\n", private_key=b"fake-private\n")
WAIT = "sdcm.provision.gce.instance_provider.wait_for_extended_operation"


def _definition(name: str) -> InstanceDefinition:
    return InstanceDefinition(
        name=name,
        image_id="projects/scylla-images/global/images/test-image",
        type="n2-standard-2",
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


@pytest.fixture(autouse=True)
def _no_retry_sleep():
    with patch("sdcm.utils.decorators.time.sleep"):
        yield


def test_insert_is_given_more_than_the_generic_operation_timeout(vm_provider):
    with (
        patch.object(vm_provider, "_build_and_insert_instance", return_value=MagicMock()),
        patch(WAIT) as waited,
    ):
        vm_provider.get_or_create(definitions=[_definition("node-1")], pricing_model=PricingModel.ON_DEMAND)

    assert waited.call_args.kwargs["timeout"] == INSTANCE_CREATION_TIMEOUT
    assert INSTANCE_CREATION_TIMEOUT > 300


def test_timed_out_insert_is_deleted_and_reported_as_retryable(vm_provider):
    """ProvisionError is what `provision_with_retry` retries; the retry needs the name free again."""
    with (
        patch.object(vm_provider, "_build_and_insert_instance", return_value=MagicMock()),
        patch(WAIT, side_effect=[TimeoutError("insert timed out"), None]),
    ):
        with pytest.raises(ProvisionError, match="node-1 was not created"):
            vm_provider.get_or_create(definitions=[_definition("node-1")], pricing_model=PricingModel.ON_DEMAND)

    vm_provider._instances_client.delete.assert_called_once_with(
        project="test-project", zone="us-east1-b", instance="node-1"
    )
    assert "node-1" not in vm_provider._cache


def test_delete_is_retried_while_gce_still_holds_the_insert(vm_provider):
    not_ready = google.api_core.exceptions.BadRequest("The resource 'node-1' is not ready")
    vm_provider._instances_client.delete.side_effect = [not_ready, not_ready, MagicMock()]

    with patch(WAIT):
        vm_provider._release_instance_name("node-1", MagicMock())

    assert vm_provider._instances_client.delete.call_count == 3


def test_delete_of_an_insert_that_never_produced_a_vm_is_not_an_error(vm_provider):
    vm_provider._instances_client.delete.side_effect = google.api_core.exceptions.NotFound("node-1")
    insert_operation = MagicMock()
    insert_operation.done.return_value = True

    vm_provider._release_instance_name("node-1", insert_operation)

    vm_provider._instances_client.delete.assert_called_once()


def test_delete_is_retried_while_the_insert_has_not_created_the_vm_yet(vm_provider):
    """A pending insert has no instance to delete yet, but it can still create one and take the name."""
    vm_provider._instances_client.delete.side_effect = [
        google.api_core.exceptions.NotFound("node-1"),
        google.api_core.exceptions.NotFound("node-1"),
        MagicMock(),
    ]
    insert_operation = MagicMock()
    insert_operation.done.return_value = False

    with patch(WAIT) as waited:
        vm_provider._release_instance_name("node-1", insert_operation)

    assert vm_provider._instances_client.delete.call_count == 3
    waited.assert_called_once()


def test_delete_that_times_out_is_not_repeated(vm_provider):
    """GCE took the delete; issuing another one would only add another full operation wait."""
    with patch(WAIT, side_effect=TimeoutError("delete timed out")):
        vm_provider._release_instance_name("node-1", MagicMock())

    vm_provider._instances_client.delete.assert_called_once()


def test_failure_to_free_the_name_does_not_mask_the_timeout(vm_provider):
    vm_provider._instances_client.delete.side_effect = google.api_core.exceptions.BadRequest("not ready")

    with (
        patch.object(vm_provider, "_build_and_insert_instance", return_value=MagicMock()),
        patch(WAIT, side_effect=TimeoutError("insert timed out")),
    ):
        with pytest.raises(ProvisionError, match="node-1 was not created"):
            vm_provider.get_or_create(definitions=[_definition("node-1")], pricing_model=PricingModel.ON_DEMAND)

    assert vm_provider._instances_client.delete.call_count == ABANDONED_INSTANCE_DELETE_ATTEMPTS
