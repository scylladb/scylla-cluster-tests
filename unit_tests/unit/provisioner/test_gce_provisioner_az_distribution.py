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

"""Tests for GCE provisioner multi-AZ placement.

A comma separated `availability_zone` ('a,b,c') means the nodes are spread evenly over those
zones, one rack per zone. Which zone a node goes to is derived from the node itself - its rack
index, or its node index - never from its position in the batch being provisioned.
"""

from unittest.mock import patch, MagicMock

import pytest
from google.cloud import compute_v1

from sdcm.provision.gce.provisioner import GceProvisioner
from sdcm.provision.provisioner import InstanceDefinition, PricingModel
from sdcm.keystore import SSHKey


FAKE_SSH_KEY = SSHKey(
    name="test_key",
    public_key=b"ssh-rsa AAAA fake\n",
    private_key=b"fake-private\n",
)


def _make_definition(node_index: int, rack_index: int | None = None) -> InstanceDefinition:
    return InstanceDefinition(
        name=f"node-{node_index}",
        image_id="projects/scylla-images/global/images/test-image",
        type="n2-highmem-8",
        user_name="scylla-test",
        ssh_key=FAKE_SSH_KEY,
        root_disk_size=50,
        root_disk_type="pd-ssd",
        tags={"NodeIndex": str(node_index)},
        rack_index=rack_index,
    )


def _make_fake_instance(name: str, zone: str) -> compute_v1.Instance:
    instance = MagicMock(spec=compute_v1.Instance)
    instance.name = name
    instance.zone = f"projects/test-project/zones/{zone}"
    instance.status = "RUNNING"
    instance.network_interfaces = [
        MagicMock(
            network_i_p="10.0.0.1",
            access_configs=[MagicMock(nat_i_p="35.0.0.1")],
        )
    ]
    instance.metadata = MagicMock(
        items=[
            MagicMock(key="ssh_user", value="scylla-test"),
            MagicMock(key="ssh_key", value="test_key"),
        ]
    )
    instance.labels = {"sct_test_id": "test-123"}
    instance.creation_timestamp = "2026-01-01T00:00:00.000-00:00"
    instance.machine_type = "zones/us-east1-a/machineTypes/n2-highmem-8"
    instance.scheduling = MagicMock(provisioning_model="STANDARD")
    instance.disks = []
    return instance


@pytest.fixture
def gce_providers():
    """Mock GCE dependencies and expose the per-zone VM provider mocks by zone name."""
    providers = {}
    created_per_zone = {}

    def make_provider(project_id, zone, test_id, disk_provider, network_provider):
        provider = MagicMock()
        provider.list.return_value = []

        def track_create(definitions, pricing_model, user_data_list, startup_script_list):
            created_per_zone.setdefault(zone, []).extend(definition.name for definition in definitions)
            return [_make_fake_instance(definition.name, zone) for definition in definitions]

        provider.get_or_create.side_effect = track_create
        providers[zone] = provider
        return provider

    with (
        patch("sdcm.provision.gce.provisioner.KeyStore") as mock_keystore_cls,
        patch("sdcm.provision.gce.provisioner.get_gce_compute_instances_client") as mock_client,
        patch("sdcm.provision.gce.provisioner.DiskProvider"),
        patch("sdcm.provision.gce.provisioner.NetworkProvider"),
        patch("sdcm.provision.gce.provisioner.VirtualMachineProvider") as mock_vm_provider_cls,
    ):
        mock_keystore_cls.return_value.get_gcp_credentials.return_value = {"project_id": "test-project"}
        mock_client.return_value = (MagicMock(), {"project_id": "test-project"})
        mock_vm_provider_cls.side_effect = make_provider

        yield {"providers": providers, "created_per_zone": created_per_zone}


def _provisioner(availability_zone: str) -> GceProvisioner:
    return GceProvisioner(
        test_id="test-123",
        region="us-east1",
        availability_zone=availability_zone,
        network_name="qa-vpc",
    )


def test_nodes_are_placed_by_node_index(gce_providers):
    """With AZ='a,b,c' and 6 nodes, each zone gets the nodes whose index maps to it."""
    provisioner = _provisioner("a,b,c")

    provisioner.get_or_create_instances([_make_definition(idx) for idx in range(1, 7)], PricingModel.ON_DEMAND)

    assert gce_providers["created_per_zone"] == {
        "us-east1-a": ["node-1", "node-4"],
        "us-east1-b": ["node-2", "node-5"],
        "us-east1-c": ["node-3", "node-6"],
    }


def test_placement_is_stable_when_nodes_are_provisioned_one_at_a_time(gce_providers):
    """Growing a cluster a node at a time must place each node in the zone of its index.

    This is what a nemesis grow does: a batch position based rule would send every added node to
    the first zone.
    """
    provisioner = _provisioner("a,b,c")

    for node_index in range(1, 7):
        provisioner.get_or_create_instances([_make_definition(node_index)], PricingModel.ON_DEMAND)

    assert gce_providers["created_per_zone"] == {
        "us-east1-a": ["node-1", "node-4"],
        "us-east1-b": ["node-2", "node-5"],
        "us-east1-c": ["node-3", "node-6"],
    }


def test_placement_is_unchanged_when_some_nodes_already_exist(gce_providers):
    """A retry after a partial failure must not move the remaining nodes to other zones."""
    provisioner = _provisioner("a,b,c")
    definitions = [_make_definition(idx) for idx in range(1, 7)]

    # first attempt created nodes 1 and 2 only
    provisioner.get_or_create_instances(definitions[:2], PricingModel.ON_DEMAND)
    provisioner.get_or_create_instances(definitions, PricingModel.ON_DEMAND)

    assert gce_providers["created_per_zone"] == {
        "us-east1-a": ["node-1", "node-4"],
        "us-east1-b": ["node-2", "node-5"],
        "us-east1-c": ["node-3", "node-6"],
    }


def test_rack_index_selects_the_zone(gce_providers):
    """An explicit rack index wins over the node index - it is how the cluster picks the AZ."""
    provisioner = _provisioner("a,b,c")

    provisioner.get_or_create_instances(
        [_make_definition(idx, rack_index=2) for idx in range(1, 4)], PricingModel.ON_DEMAND
    )

    assert gce_providers["created_per_zone"] == {"us-east1-c": ["node-1", "node-2", "node-3"]}


def test_instances_are_returned_in_the_requested_order(gce_providers):
    """Callers zip the result with the definitions they passed in, so the order must be kept."""
    provisioner = _provisioner("a,b,c")
    definitions = [_make_definition(idx) for idx in range(1, 7)]

    instances = provisioner.get_or_create_instances(definitions, PricingModel.ON_DEMAND)

    assert [instance.name for instance in instances] == [definition.name for definition in definitions]


def test_terminate_uses_the_zone_of_the_instance(gce_providers):
    """A node living in the second zone must not be deleted through the first zone's provider."""
    provisioner = _provisioner("a,b,c")
    provisioner.get_or_create_instances([_make_definition(2)], PricingModel.ON_DEMAND)

    provisioner.terminate_instance("node-2", wait=True)

    gce_providers["providers"]["us-east1-b"].delete.assert_called_once_with("node-2", wait=True)
    gce_providers["providers"]["us-east1-a"].delete.assert_not_called()


def test_reboot_and_tagging_use_the_zone_of_the_instance(gce_providers):
    provisioner = _provisioner("a,b,c")
    provisioner.get_or_create_instances([_make_definition(3)], PricingModel.ON_DEMAND)

    provisioner.reboot_instance("node-3", wait=False)
    provisioner.add_instance_tags("node-3", {"keep": "alive"})

    gce_providers["providers"]["us-east1-c"].reboot.assert_called_once_with("node-3", wait=False, hard=False)
    gce_providers["providers"]["us-east1-c"].add_tags.assert_called_once_with("node-3", {"keep": "alive"})


def test_cleanup_deletes_instances_in_every_zone(gce_providers):
    """Cleanup must reach the nodes of all zones, or they leak."""
    provisioner = _provisioner("a,b,c")
    provisioner.get_or_create_instances([_make_definition(idx) for idx in range(1, 7)], PricingModel.ON_DEMAND)

    provisioner.cleanup(wait=True)

    deleted_per_zone = {
        zone: sorted(call.args[0] for call in provider.delete.call_args_list)
        for zone, provider in gce_providers["providers"].items()
    }
    assert deleted_per_zone == {
        "us-east1-a": ["node-1", "node-4"],
        "us-east1-b": ["node-2", "node-5"],
        "us-east1-c": ["node-3", "node-6"],
    }
    for provider in gce_providers["providers"].values():
        provider.clear_cache.assert_called_once()


def test_existing_instances_are_cached_with_their_own_zone(gce_providers):
    """Instances discovered at startup must keep the zone they were found in."""

    def list_for_zone(zone):
        return lambda: [_make_fake_instance(f"node-in-{zone}", zone)]

    with patch("sdcm.provision.gce.provisioner.VirtualMachineProvider") as mock_vm_provider_cls:
        providers = {}

        def make_provider(project_id, zone, test_id, disk_provider, network_provider):
            provider = MagicMock()
            provider.list.side_effect = list_for_zone(zone)
            providers[zone] = provider
            return provider

        mock_vm_provider_cls.side_effect = make_provider
        provisioner = _provisioner("a,b")

        provisioner.terminate_instance("node-in-us-east1-b")

        providers["us-east1-b"].delete.assert_called_once_with("node-in-us-east1-b", wait=False)
        providers["us-east1-a"].delete.assert_not_called()


def test_single_az_places_every_node_in_that_zone(gce_providers):
    provisioner = _provisioner("b")

    provisioner.get_or_create_instances([_make_definition(idx) for idx in range(1, 4)], PricingModel.ON_DEMAND)

    assert gce_providers["created_per_zone"] == {"us-east1-b": ["node-1", "node-2", "node-3"]}
    assert provisioner.availability_zones == ["us-east1-b"]


@pytest.mark.parametrize("availability_zone", ["a, b , c", "a,,b,c,"], ids=["spaces", "empty-parts"])
def test_zone_letters_are_normalized(gce_providers, availability_zone):
    assert _provisioner(availability_zone).availability_zones == ["us-east1-a", "us-east1-b", "us-east1-c"]


@patch("sdcm.provision.gce.provisioner.random_zone", return_value="c")
def test_empty_az_uses_random_zone(mock_random_zone, gce_providers):
    """When availability_zone is empty, a random zone is selected."""
    provisioner = _provisioner("")

    assert provisioner.availability_zones == ["us-east1-c"]
    assert provisioner.availability_zone == "us-east1-c"
    mock_random_zone.assert_called_once_with("us-east1")
