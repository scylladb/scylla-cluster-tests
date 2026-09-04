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

"""Provisioning Azure VMs with more than one network interface."""

import uuid

import pytest

from sdcm.keystore import KeyStore
from sdcm.provision.azure.provisioner import AZURE_SUPPORTED_NETWORK_INTERFACES
from sdcm.provision.provisioner import InstanceDefinition, PricingModel, ProvisionError, provisioner_factory
from sdcm.utils.azure_utils import max_network_interfaces
from unit_tests.unit.provisioner.fake_azure_service import FakeResourceSkus

REGION = "eastus"


def nic_specs(count: int) -> list[dict]:
    """'azure_network_interfaces' for `count` IPv4 interfaces, public IP on the primary one only."""
    return [
        {
            "subnet": "default" if index == 0 else f"nic{index}",
            "public_ip": index == 0,
            "ipv6": False,
            "public_ipv6": False,
        }
        for index in range(count)
    ]


@pytest.fixture(name="make_provisioner")
def fixture_make_provisioner(azure_service):
    def _make(nic_count: int):
        return provisioner_factory.create_provisioner(
            backend="azure",
            test_id=str(uuid.uuid4()),
            region=REGION,
            availability_zone="a",
            azure_service=azure_service,
            azure_network_interfaces=nic_specs(nic_count),
        )

    return _make


def definition(name: str, instance_type: str = "Standard_L8s_v3") -> InstanceDefinition:
    return InstanceDefinition(
        name=name,
        image_id="/subscriptions/6c268694-47ab-43ab-b306-3c5514bc4112/resourceGroups/scylla-images/providers"
        "/Microsoft.Compute/images/scylla-4.4.4",
        type=instance_type,
        user_name="tester",
        ssh_key=KeyStore().get_ssh_key_pair(name="scylla_test_id_ed25519"),
        tags={"test-tag": "test_value"},
        user_data=None,
        use_public_ip=True,
    )


def nic_names(provisioner, name: str) -> list[str]:
    return [nic.name for nic in provisioner._nic_provider.get_all(name)]  # noqa: SLF001


@pytest.mark.parametrize("nic_count", [1, 2, 3])
def test_provisions_the_requested_number_of_nics(make_provisioner, nic_count):
    provisioner = make_provisioner(nic_count)
    instance = provisioner.get_or_create_instance(definition("multi-nic-vm"), PricingModel.ON_DEMAND)

    assert len(nic_names(provisioner, instance.name)) == nic_count


def test_primary_nic_keeps_its_historical_name(make_provisioner):
    """A single-NIC run must produce exactly the resource names it produced before multi-NIC support."""
    provisioner = make_provisioner(1)
    instance = provisioner.get_or_create_instance(definition("legacy-name-vm"), PricingModel.ON_DEMAND)

    assert nic_names(provisioner, instance.name) == ["legacy-name-vm-nic"]
    assert provisioner._ip_provider.get(instance.name).name == "legacy-name-vm-ipv4"  # noqa: SLF001


def test_secondary_nics_are_named_and_ordered_by_device_index(make_provisioner):
    provisioner = make_provisioner(3)
    instance = provisioner.get_or_create_instance(definition("ordered-vm"), PricingModel.ON_DEMAND)

    assert nic_names(provisioner, instance.name) == ["ordered-vm-nic", "ordered-vm-nic1", "ordered-vm-nic2"]


def test_each_nic_lands_in_its_own_subnet(make_provisioner):
    provisioner = make_provisioner(3)
    instance = provisioner.get_or_create_instance(definition("subnet-vm"), PricingModel.ON_DEMAND)

    subnets = [
        nic.ip_configurations[0].subnet.id.rsplit("/", 1)[-1]
        for nic in provisioner._nic_provider.get_all(instance.name)  # noqa: SLF001
    ]
    assert subnets == ["default", "nic1", "nic2"]


def test_public_ip_is_attached_to_the_primary_nic_only(make_provisioner):
    provisioner = make_provisioner(2)
    instance = provisioner.get_or_create_instance(definition("public-ip-vm"), PricingModel.ON_DEMAND)

    primary, secondary = provisioner._nic_provider.get_all(instance.name)  # noqa: SLF001
    assert primary.ip_configurations[0].public_ip_address is not None
    assert secondary.ip_configurations[0].public_ip_address is None


def test_private_address_of_the_instance_comes_from_the_primary_nic(make_provisioner):
    provisioner = make_provisioner(2)
    instance = provisioner.get_or_create_instance(definition("primary-addr-vm"), PricingModel.ON_DEMAND)

    assert instance.private_ip_address == "10.0.0.4"


def test_vm_is_created_with_every_nic_and_a_single_primary(make_provisioner, azure_service):
    provisioner = make_provisioner(3)
    instance = provisioner.get_or_create_instance(definition("profile-vm"), PricingModel.ON_DEMAND)

    v_m = azure_service.compute.virtual_machines.get(provisioner.resource_group_name, instance.name)
    interfaces = v_m.network_profile.network_interfaces
    assert len(interfaces) == 3
    assert [interface.primary for interface in interfaces] == [True, False, False]


def test_terminate_forgets_every_nic_and_public_ip(make_provisioner):
    """Terminating leaves the Azure resources to the resource group deletion, as it always has.

    What must hold is that *every* NIC is dropped, not only the primary one: a stale cache entry
    would make a re-provisioned node reuse a NIC that the resource group cleanup has removed.
    """
    provisioner = make_provisioner(3)
    instance = provisioner.get_or_create_instance(definition("teardown-vm"), PricingModel.ON_DEMAND)

    provisioner.terminate_instance(instance.name, wait=True)

    assert provisioner._nic_provider.get_all(instance.name) == []  # noqa: SLF001
    assert provisioner._ip_provider.get(instance.name).id is None  # noqa: SLF001


def test_stuck_node_deletion_removes_every_nic_from_azure(make_provisioner, azure_service):
    """Recreating a stuck VM does delete its resources, so none of its NICs may be left behind."""
    provisioner = make_provisioner(3)
    instance = provisioner.get_or_create_instance(definition("stuck-vm"), PricingModel.ON_DEMAND)
    resource_group = provisioner.resource_group_name

    provisioner._delete_stuck_node(instance.name)  # noqa: SLF001

    remaining = [nic.name for nic in azure_service.network.network_interfaces.list(resource_group)]
    assert not [name for name in remaining if name.startswith(instance.name)]


def test_no_ipv6_resource_is_created_for_an_ipv4_only_node(make_provisioner):
    """IPv6 costs a billed Public IP on Azure, so nothing IPv6 may appear unless a test asks for it."""
    provisioner = make_provisioner(2)
    instance = provisioner.get_or_create_instance(definition("ipv4-only-vm"), PricingModel.ON_DEMAND)

    for nic in provisioner._nic_provider.get_all(instance.name):  # noqa: SLF001
        versions = [config.private_ip_address_version for config in nic.ip_configurations]
        assert versions == ["IPv4"]


def test_more_nics_than_the_vm_size_allows_is_rejected(make_provisioner):
    """Standard_D2_v4 carries 2 NICs, so a third one must fail before any resource is created."""
    provisioner = make_provisioner(3)
    with pytest.raises(ProvisionError, match="supports at most 2 network interface"):
        provisioner.get_or_create_instance(
            definition("too-many-nics-vm", instance_type="Standard_D2_v4"), PricingModel.ON_DEMAND
        )


def test_nic_count_at_the_vm_size_limit_is_accepted(make_provisioner):
    provisioner = make_provisioner(2)
    instance = provisioner.get_or_create_instance(
        definition("at-limit-vm", instance_type="Standard_D2_v4"), PricingModel.ON_DEMAND
    )

    assert len(nic_names(provisioner, instance.name)) == 2


class TestMaxNetworkInterfaces:
    """Reading the per-VM-size NIC limit out of the Azure SKU listing."""

    def test_reads_the_capability_of_the_requested_size(self, azure_service):
        assert max_network_interfaces("Standard_L8s_v3", REGION, azure_service) == 4

    def test_returns_none_for_a_size_absent_from_the_location(self, azure_service):
        assert max_network_interfaces("Standard_NotAThing_v9", REGION, azure_service) is None

    def test_the_listing_is_cached_per_size_and_location(self, azure_service, monkeypatch):
        """Listing a region's SKUs is a slow call, so it must happen once per size and location."""
        max_network_interfaces.cache_clear()
        calls = []
        original = FakeResourceSkus.list
        # patched on the class: FakeAzureService.compute hands out a new Compute object every access
        monkeypatch.setattr(
            FakeResourceSkus,
            "list",
            lambda self, *args, **kwargs: calls.append(1) or original(self, *args, **kwargs),
        )

        for _ in range(3):
            assert max_network_interfaces("Standard_L8s_v3", REGION, azure_service) == 4

        assert len(calls) == 1


def test_more_nics_than_sct_supports_is_rejected(make_provisioner):
    """SCT carves one /24 per NIC out of the test VNet, and Azure caps a VM at 8 NICs anyway."""
    provisioner = make_provisioner(AZURE_SUPPORTED_NETWORK_INTERFACES + 1)
    with pytest.raises(ProvisionError, match="SCT supports at most 8"):
        provisioner.get_or_create_instance(definition("over-sct-cap-vm"), PricingModel.ON_DEMAND)


def test_unknown_vm_size_does_not_block_provisioning(make_provisioner, caplog):
    """A size Azure does not report a limit for must warn, not guess a limit and fail the run."""
    provisioner = make_provisioner(2)
    instance = provisioner.get_or_create_instance(
        definition("unknown-size-vm", instance_type="Standard_Unlisted_v1"), PricingModel.ON_DEMAND
    )

    assert len(nic_names(provisioner, instance.name)) == 2
    assert "does not report 'MaxNetworkInterfaces'" in caplog.text
