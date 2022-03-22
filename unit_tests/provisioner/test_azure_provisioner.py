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
# Copyright (c) 2022 ScyllaDB
import uuid

import pytest
from azure.core.exceptions import ResourceNotFoundError

from sdcm.keystore import KeyStore  # pylint: disable=import-error
from sdcm.provision.azure.utils import get_scylla_images  # pylint: disable=import-error
from sdcm.provision.provisioner import InstanceDefinition, provisioner_factory  # pylint: disable=import-error
from sdcm.utils.azure_utils import AzureService  # pylint: disable=import-error
from unit_tests.provisioner.fake_azure_service import FakeAzureService  # pylint: disable=import-error


class TestProvisionScyllaInstanceAzureE2E:
    """this is rather e2e test - takes around 8 minutes (2m provisioning, 6 min cleanup with wait=True)"""

    @pytest.fixture(scope="session")
    def azure_service(self, tmp_path_factory) -> AzureService:  # pylint: disable=no-self-use
        run_on_real_azure = False   # make it True to test with real Azure
        if run_on_real_azure:
            # When true this becomes e2e test - takes around 8 minutes (2m provisioning, 6 min cleanup with wait=True)
            return AzureService()
        return FakeAzureService(tmp_path_factory.mktemp("azure-provision"))

    @pytest.fixture(scope='session')
    def test_id(self):  # pylint: disable=no-self-use
        return f"unit-test-{str(uuid.uuid4())}"

    @pytest.fixture(scope='session')
    def region(self):  # pylint: disable=no-self-use
        return "eastus"

    @pytest.fixture(scope="session")
    def image_id(self):  # pylint: disable=no-self-use
        return get_scylla_images("master:latest", "eastus")[0].id

    @pytest.fixture(scope='session')
    def definition(self, image_id):  # pylint: disable=no-self-use
        return InstanceDefinition(
            name="test-vm-1",
            image_id=image_id,
            type="Standard_D2s_v3",
            user_name="tester",
            ssh_public_key=KeyStore().get_ec2_ssh_key_pair().public_key.decode(),
            tags={'test-tag': 'test_value'}
        )

    @pytest.fixture(scope="session")
    def provisioner(self, test_id, region, azure_service):  # pylint: disable=no-self-use
        return provisioner_factory.create_provisioner("azure", test_id, region, azure_service=azure_service)

    def test_can_provision_scylla_vm(self, region, definition, provisioner):  # pylint: disable=no-self-use
        v_m = provisioner.get_or_create_instance(definition)
        assert v_m.name == definition.name
        assert v_m.region == region
        assert v_m.user_name == definition.user_name
        assert v_m.public_ip_address
        assert v_m.private_ip_address
        assert v_m.tags == definition.tags

        assert v_m == provisioner.list_instances()[0]

    @pytest.mark.timeout(2)
    def test_can_discover_existing_resources_for_test_id(self, region, test_id, definition, azure_service):  # pylint: disable=no-self-use
        """should read from cache instead creating anything - so should be fast (after provisioner initialized)"""
        # create provisioner from scratch to test resources discovery
        provisioner = provisioner_factory.create_provisioner("azure", test_id, region, azure_service=azure_service)
        assert provisioner.list_instances()[0]
        v_m = provisioner.get_or_create_instance(definition)
        assert v_m.name == definition.name
        assert v_m.region == region
        assert v_m.user_name == definition.user_name
        assert v_m.public_ip_address
        assert v_m.private_ip_address
        assert v_m.tags == definition.tags

        assert v_m == provisioner.list_instances()[0]

    def test_can_add_tags(self, provisioner, definition, azure_service):  # pylint: disable=no-self-use
        provisioner.add_instance_tags(definition.name, {"tag_key": "tag_value"})
        assert provisioner.get_or_create_instance(definition).tags.get("tag_key") == "tag_value"

        resource_group = provisioner._rg_provider.get_or_create()  # pylint: disable=protected-access
        v_m = azure_service.compute.virtual_machines.get(resource_group.name, definition.name)
        assert v_m.tags.get("tag_key") == "tag_value"

    def test_can_terminate_vm_instance(self, provisioner, definition, azure_service):  # pylint: disable=no-self-use
        """should read from cache instead creating anything - so should be fast (after provisioner initialized)"""
        resource_group = provisioner._rg_provider.get_or_create()  # pylint: disable=protected-access
        nic = provisioner._nic_provider.get(definition.name)  # pylint: disable=protected-access
        ip = provisioner._ip_provider.get(definition.name)  # pylint: disable=protected-access
        provisioner.terminate_instance(definition.name, wait=True)

        # validate cache has been cleaned up
        assert not provisioner.list_instances()
        with pytest.raises(KeyError):
            assert not provisioner._nic_provider.get(definition.name)  # pylint: disable=protected-access
        with pytest.raises(KeyError):
            assert not provisioner._ip_provider.get(definition.name)  # pylint: disable=protected-access

        # verify that truly vm, nic and ip got deleted - not only cache
        with pytest.raises(ResourceNotFoundError):
            azure_service.compute.virtual_machines.get(resource_group.name, definition.name)
        with pytest.raises(ResourceNotFoundError):
            azure_service.network.network_interfaces.get(resource_group.name, nic.name)
        with pytest.raises(ResourceNotFoundError):
            azure_service.network.public_ip_addresses.get(resource_group.name, ip.name)

    def test_can_trigger_cleanup(self, test_id, provisioner, azure_service):  # pylint: disable=no-self-use
        resource_group = provisioner._rg_provider.get_or_create()  # pylint: disable=protected-access
        provisioner_factory.discover_provisioners("azure", test_id)
        provisioner.cleanup(wait=True)

        with pytest.raises(ResourceNotFoundError):
            azure_service.resource.resource_groups.get(resource_group.name)
        assert not provisioner.list_instances(), "failed cleaning up resources"
        assert provisioner._rg_provider._cache is None, "resource group was not deleted"  # pylint: disable=protected-access
