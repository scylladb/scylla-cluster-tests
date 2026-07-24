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

"""Tests for GCE provisioner multi-AZ distribution."""

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


def _make_definition(name: str) -> InstanceDefinition:
    return InstanceDefinition(
        name=name,
        image_id="projects/scylla-images/global/images/test-image",
        type="n2-highmem-8",
        user_name="scylla-test",
        ssh_key=FAKE_SSH_KEY,
        root_disk_size=50,
        root_disk_type="pd-ssd",
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
def mock_gce_deps():
    """Mock all GCE dependencies to avoid real API calls."""
    with (
        patch("sdcm.provision.gce.provisioner.KeyStore") as mock_keystore_cls,
        patch("sdcm.provision.gce.provisioner.get_gce_compute_instances_client") as mock_client,
        patch("sdcm.provision.gce.provisioner.DiskProvider"),
        patch("sdcm.provision.gce.provisioner.NetworkProvider"),
        patch("sdcm.provision.gce.provisioner.VirtualMachineProvider") as mock_vm_provider_cls,
    ):
        mock_keystore_cls.return_value.get_gcp_credentials.return_value = {"project_id": "test-project"}
        mock_client.return_value = (MagicMock(), {"project_id": "test-project"})
        mock_vm_provider_cls.return_value.list.return_value = []

        yield {
            "vm_provider_cls": mock_vm_provider_cls,
        }


def test_6_nodes_across_3_azs_distributes_2_per_az(mock_gce_deps):
    """With AZ='a,b,c' and 6 nodes, each AZ gets exactly 2 nodes."""
    vm_provider_cls = mock_gce_deps["vm_provider_cls"]

    created_per_zone = {}

    def make_provider(project_id, zone, test_id, disk_provider, network_provider):
        provider = MagicMock()
        provider.list.return_value = []

        def track_create(definitions, pricing_model, user_data_list, startup_script_list):
            created_per_zone[zone] = definitions
            return [_make_fake_instance(d.name, zone) for d in definitions]

        provider.get_or_create.side_effect = track_create
        return provider

    vm_provider_cls.side_effect = make_provider

    provisioner = GceProvisioner(
        test_id="test-123",
        region="us-east1",
        availability_zone="a,b,c",
        network_name="qa-vpc",
    )

    definitions = [_make_definition(f"node-{i}") for i in range(6)]
    provisioner.get_or_create_instances(definitions, PricingModel.ON_DEMAND)

    assert len(created_per_zone) == 3
    assert len(created_per_zone["us-east1-a"]) == 2
    assert len(created_per_zone["us-east1-b"]) == 2
    assert len(created_per_zone["us-east1-c"]) == 2

    # Round-robin: node-0->a, node-1->b, node-2->c, node-3->a, node-4->b, node-5->c
    assert [d.name for d in created_per_zone["us-east1-a"]] == ["node-0", "node-3"]
    assert [d.name for d in created_per_zone["us-east1-b"]] == ["node-1", "node-4"]
    assert [d.name for d in created_per_zone["us-east1-c"]] == ["node-2", "node-5"]


@patch("sdcm.provision.gce.provisioner.random_zone", return_value="c")
def test_empty_az_uses_random_zone(mock_random_zone, mock_gce_deps):
    """When availability_zone is empty, a random zone is selected."""
    provisioner = GceProvisioner(
        test_id="test-123",
        region="us-east1",
        availability_zone="",
        network_name="qa-vpc",
    )

    assert provisioner._zone_names == ["us-east1-c"]
    mock_random_zone.assert_called_once_with("us-east1")


def test_single_az_all_nodes_in_one_zone(mock_gce_deps):
    """With AZ='b', all nodes are created in that single zone."""
    vm_provider_cls = mock_gce_deps["vm_provider_cls"]

    created_per_zone = {}

    def make_provider(project_id, zone, test_id, disk_provider, network_provider):
        provider = MagicMock()
        provider.list.return_value = []

        def track_create(definitions, pricing_model, user_data_list, startup_script_list):
            created_per_zone[zone] = definitions
            return [_make_fake_instance(d.name, zone) for d in definitions]

        provider.get_or_create.side_effect = track_create
        return provider

    vm_provider_cls.side_effect = make_provider

    provisioner = GceProvisioner(
        test_id="test-123",
        region="us-east1",
        availability_zone="b",
        network_name="qa-vpc",
    )

    definitions = [_make_definition(f"node-{i}") for i in range(3)]
    provisioner.get_or_create_instances(definitions, PricingModel.ON_DEMAND)

    assert len(created_per_zone) == 1
    assert len(created_per_zone["us-east1-b"]) == 3
