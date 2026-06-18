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

"""Tests for Azure stuck-VM detection and whole-node recreate (SCT-434)"""

import time
import uuid
from unittest.mock import patch

import pytest

from sdcm.keystore import KeyStore
from sdcm.provision.azure.virtual_machine_provider import VirtualMachineProvider
from sdcm.provision.provisioner import (
    InstanceDefinition,
    ProvisionError,
    ProvisionUnrecoverableError,
    provisioner_factory,
)
from unit_tests.provisioner.fake_azure_service import FakeVirtualMachines, VMCreateBehavior


@pytest.fixture(autouse=True)
def _no_nic_propagation_sleep():
    """Skip NIC propagation sleep so recreate attempts stay fast in tests."""
    with patch("sdcm.provision.azure.network_interface_provider.time.sleep"):
        yield


def _make_provisioner(azure_service, recreate_attempts: int = 2, timeout: float = 0.5, poll: float = 0.01):
    provisioner = provisioner_factory.create_provisioner(
        backend="azure",
        test_id=str(uuid.uuid4()),
        region="eastus",
        availability_zone="",
        azure_service=azure_service,
        azure_provision_stuck_vm_recreate_attempts=recreate_attempts,
    )
    provisioner._vm_provider._stuck_vm_timeout = timeout
    provisioner._vm_provider._stuck_vm_poll_interval = poll
    return provisioner


def _definition(name: str) -> InstanceDefinition:
    return InstanceDefinition(
        name=name,
        image_id="OpenLogic:CentOS:7_9:latest",
        type="Standard_L8s_v4",
        user_name="tester",
        ssh_key=KeyStore().get_ec2_ssh_key_pair(),
        tags={"NodeType": "scylla-db"},
        user_data=None,
        use_public_ip=True,
    )


def _stuck_events(events, severity_name: str, expected: int = 0) -> list[str]:
    # on this branch the events_function_scope fixture runs the real (asynchronous) events
    # infrastructure, so the file logger is reached via get_events_logger() and the published
    # events may need a moment to be flushed to disk before they can be read back. Wait until at
    # least `expected` stuck events of the requested severity are visible (or the timeout expires).
    file_logger = events.get_events_logger()
    deadline = time.time() + 10

    def _matching() -> list[str]:
        by_category = file_logger.get_events_by_category()
        return [line for line in by_category.get(severity_name, []) if "InstanceProvisionStuckEvent" in line]

    matching = _matching()
    while time.time() < deadline and len(matching) < expected:
        time.sleep(0.1)
        matching = _matching()
    return matching


def test_slow_but_recovering_vm_is_not_redeployed(azure_service):
    """A VM that recovers before the timeout must not be redeployed."""
    FakeVirtualMachines.set_provision_script("node-slow", [VMCreateBehavior(stuck=True, recover_after_polls=3)])
    provisioner = _make_provisioner(azure_service)
    with patch.object(provisioner, "_redeploy_stuck_node", wraps=provisioner._redeploy_stuck_node) as redeploy_spy:
        instances = provisioner.get_or_create_instances([_definition("node-slow")])
    assert [instance.name for instance in instances] == ["node-slow"]
    redeploy_spy.assert_not_called()


def test_stuck_vm_is_redeployed_and_succeeds(azure_service, events_function_scope):
    """A stuck VM should be redeployed once and then succeed (no delete/recreate needed)."""
    FakeVirtualMachines.set_provision_script("node-stuck", [VMCreateBehavior(stuck=True)])
    provisioner = _make_provisioner(azure_service, recreate_attempts=2)
    with (
        patch.object(provisioner, "_redeploy_stuck_node", wraps=provisioner._redeploy_stuck_node) as redeploy_spy,
        patch.object(provisioner, "_delete_stuck_node", wraps=provisioner._delete_stuck_node) as delete_spy,
    ):
        instances = provisioner.get_or_create_instances([_definition("node-stuck")])

    assert [instance.name for instance in instances] == ["node-stuck"]
    redeploy_spy.assert_called_once_with("node-stuck")
    delete_spy.assert_not_called()
    assert len(_stuck_events(events_function_scope, "NORMAL", expected=1)) == 1
    assert _stuck_events(events_function_scope, "WARNING") == []


def test_stuck_vm_redeploy_fails_then_recreated_and_succeeds(azure_service, events_function_scope):
    """When redeploy fails, the stuck VM is deleted and recreated, then succeeds."""
    FakeVirtualMachines.set_provision_script(
        "node-x",
        [VMCreateBehavior(stuck=True), VMCreateBehavior(redeploy_fails=True), VMCreateBehavior()],
    )
    provisioner = _make_provisioner(azure_service, recreate_attempts=2)
    with (
        patch.object(provisioner, "_redeploy_stuck_node", wraps=provisioner._redeploy_stuck_node) as redeploy_spy,
        patch.object(provisioner, "_delete_stuck_node", wraps=provisioner._delete_stuck_node) as delete_spy,
    ):
        instances = provisioner.get_or_create_instances([_definition("node-x")])

    assert [instance.name for instance in instances] == ["node-x"]
    redeploy_spy.assert_called_once_with("node-x")
    delete_spy.assert_called_once_with("node-x")
    assert len(_stuck_events(events_function_scope, "NORMAL", expected=1)) == 1
    assert _stuck_events(events_function_scope, "WARNING") == []


def test_permanently_stuck_vm_raises_unrecoverable_after_configured_attempts(azure_service, events_function_scope):
    FakeVirtualMachines.set_provision_script("node-dead", default=VMCreateBehavior(stuck=True))
    provisioner = _make_provisioner(azure_service, recreate_attempts=2)
    with (
        patch.object(provisioner, "_redeploy_stuck_node", wraps=provisioner._redeploy_stuck_node) as redeploy_spy,
        patch.object(provisioner, "_delete_stuck_node", wraps=provisioner._delete_stuck_node) as delete_spy,
    ):
        with pytest.raises(ProvisionUnrecoverableError) as exc_info:
            provisioner.get_or_create_instances([_definition("node-dead")])

    assert not isinstance(exc_info.value, ProvisionError)
    assert redeploy_spy.call_count == 2
    assert delete_spy.call_count == 2
    assert len(_stuck_events(events_function_scope, "WARNING", expected=1)) == 1
    assert len(_stuck_events(events_function_scope, "NORMAL", expected=2)) == 2


def test_only_stuck_vm_in_mixed_batch_is_redeployed(azure_service, events_function_scope):
    FakeVirtualMachines.set_provision_script("node-bad", [VMCreateBehavior(stuck=True)])
    provisioner = _make_provisioner(azure_service, recreate_attempts=2)
    definitions = [_definition("node-good-1"), _definition("node-bad"), _definition("node-good-2")]
    with patch.object(provisioner, "_redeploy_stuck_node", wraps=provisioner._redeploy_stuck_node) as redeploy_spy:
        instances = provisioner.get_or_create_instances(definitions)

    assert [instance.name for instance in instances] == ["node-good-1", "node-bad", "node-good-2"]
    redeploy_spy.assert_called_once_with("node-bad")
    assert len(_stuck_events(events_function_scope, "NORMAL", expected=1)) == 1


def test_discovery_does_not_cache_still_creating_vm(azure_service):
    """A VM still in Creating state must not be rediscovered as ready."""
    vm_name = "ghost"
    resource_group = f"SCT-{uuid.uuid4()}-eastus"
    vm_parameters = {
        "location": "eastus",
        "tags": {"NodeType": "scylla-db"},
        "hardware_profile": {"vm_size": "Standard_L8s_v4"},
        "network_profile": {
            "network_interfaces": [{"id": "/fake/nic/ghost-nic", "properties": {"deleteOption": "Detach"}}]
        },
        "priority": "Regular",
    }

    azure_service.resource.resource_groups.create_or_update(resource_group, {"location": "eastus"})
    FakeVirtualMachines.set_provision_script(vm_name, [VMCreateBehavior(stuck=True)])
    azure_service.compute.virtual_machines.begin_create_or_update(resource_group, vm_name, vm_parameters)

    provider = VirtualMachineProvider(resource_group, "eastus", "", False, azure_service)
    assert vm_name not in provider._cache
    assert provider.list() == []
