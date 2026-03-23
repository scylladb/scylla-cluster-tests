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
# Copyright (c) 2024 ScyllaDB

from uuid import uuid4

import pytest

from sdcm.sct_runner import AzureSctRunner
from sdcm.provision import AzureProvisioner
from sdcm.utils.decorators import retrying

pytestmark = [
    pytest.mark.integration,
    pytest.mark.provisioning,
]


@pytest.fixture
def azure_vm():
    """
    Fixture to provision a real Azure VM using AzureSctRunner and clean it up after the test.
    Yields (vm_name, resource_group, region, az, instance, provider)
    """
    region = "eastus"
    az = "1"
    test_id = str(uuid4())
    test_name = "reboot-integration"
    test_duration = 30  # minutes

    runner = AzureSctRunner(region_name=region, availability_zone=az, params=None)
    instance = runner.create_instance(
        test_id=test_id,
        test_name=test_name,
        test_duration=test_duration,
    )
    assert instance is not None, "Failed to provision Azure VM for integration test."

    vm_name = instance.name if hasattr(instance, "name") else instance.vm_name
    provisioner = AzureProvisioner.discover_regions(test_id=test_id)
    resource_group = provisioner[0]._resource_group_name

    try:
        yield vm_name, resource_group, region, az, instance, provisioner[0]
    finally:
        provisioner[0].cleanup(wait=True)


def test_reboot_integration(azure_vm):
    """
    Integration test: Provision a real Azure VM using AzureSctRunner, reboot it, and verify it comes back online.
    Requires valid Azure credentials and may incur cloud costs.
    """
    vm_name, resource_group, region, az, instance, provisioner = azure_vm
    provisioner.reboot_instance(vm_name, wait=True, hard=False)

    @retrying(
        n=5,
        sleep_time=5,
        allowed_exceptions=(AssertionError,),
        message="Waiting for node to come back up after reboot.",
    )
    def wait_for_node_up():
        statuses = provisioner._azure_service.compute.virtual_machines.instance_view(resource_group, vm_name).statuses
        assert any(s.display_status == "VM running" for s in statuses), "Node did not come back up after reboot."

    wait_for_node_up()
