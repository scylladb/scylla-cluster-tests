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


import json
import logging

import pytest

from sdcm.provision.azure.utils import (
    get_arch_from_azure_instance_type,
    get_image_tags,
    get_scylla_images,
    get_scylla_images_private_galleries,
)
from sdcm.utils.azure_utils import AzureService
from sdcm.utils.vm_arch import VmArch
from unit_tests.unit.provisioner.fake_azure_service import FakeAzureService


@pytest.fixture
def azure_service(test_data_dir):
    """Provide a FakeAzureService that serves the image lists stored in the provisioner test data."""
    return FakeAzureService(test_data_dir)


def test_can_get_scylla_images_based_on_branch(azure_service):
    """A 'branch:latest' spec returns only the newest x86 image of that branch."""
    images = get_scylla_images("master:latest", "eastus", azure_service=azure_service)
    assert images[0].name == "2026.0108.151759"
    assert len(images) == 1


@pytest.mark.parametrize("sha, expected_names", [("abc1234", ["2026.0108.151759"]), ("0000000", [])])
def test_can_get_scylla_images_based_on_branch_scylla_sha(azure_service, sha, expected_names):
    """A 'branch:sha' spec returns the images built from that Scylla commit, or none for an unknown one."""
    images = get_scylla_images(f"master:{sha}", "eastus", azure_service=azure_service)
    assert [image.name for image in images] == expected_names


def test_can_get_scylla_images_based_on_branch_arm64(azure_service):
    """A 'branch:latest' spec with ARM arch returns the newest ARM image of that branch."""
    images = get_scylla_images("master:latest", "eastus", azure_service=azure_service, arch=VmArch.ARM)
    assert images[0].name == "2026.0108.145545"
    assert len(images) == 1


def test_private_gallery_falls_back_to_old_naming(azure_service):
    """Private gallery lookup finds a branch's images under the old gallery image naming."""
    images = get_scylla_images_private_galleries("2024.2:latest", "eastus", azure_service=azure_service)
    assert len(images) == 1
    assert images[0].name == "2024.1201.120000"


def test_private_gallery_fallback_returns_empty_for_nonexistent_branch(azure_service):
    """Private gallery lookup returns an empty list for a branch with no gallery images."""
    images = get_scylla_images_private_galleries("nonexistent:latest", "eastus", azure_service=azure_service)
    assert images == []


def test_can_get_scylla_images_based_on_scylla_version(azure_service):
    """A Scylla version returns all images of that version, latest build first."""
    images = get_scylla_images("4.6.4", "eastus", azure_service=azure_service)
    assert images[0].name == "ScyllaDB-4.6.4-0.20220718.b60f14601-1-build-28"
    assert len(images) == 2


def test_can_get_scylla_images_based_on_revision_id(azure_service):
    """A revision id returns the image built from that revision, ignoring anything after the id."""
    images = get_scylla_images("11f008e8f<ignored>", "eastus", azure_service=azure_service)
    assert len(images) == 1
    assert images[0].name == "ScyllaDB-4.6.4-0.20220516.11f008e8f-1-build-27"


def test_unparsable_scylla_versions_are_logged(azure_service, caplog):
    """Images whose Scylla version cannot be parsed are reported in a warning."""
    with caplog.at_level(logging.WARNING):
        get_scylla_images("unparsable:latest", "eastus", azure_service=azure_service)
    assert "Couldn't parse scylla version from images: ['ScyllaDB-5.-98ad.1.dev_0: ScyllaDB-5.']" in caplog.text


@pytest.mark.parametrize(
    "instance_type,expected_arch",
    [
        ("Standard_D8ps_v5", VmArch.ARM),
        ("Standard_D4ps_v5", VmArch.ARM),
        ("Standard_E8ps_v5", VmArch.ARM),
        ("Standard_D2pds_v5", VmArch.ARM),
        ("Standard_L8s_v3", VmArch.X86),
        ("Standard_D2_v4", VmArch.X86),
        ("Standard_F4s_v2", VmArch.X86),
        ("", VmArch.X86),
        (None, VmArch.X86),
    ],
)
def test_get_arch_from_azure_instance_type(instance_type, expected_arch):
    """Azure sizes with 'p' right after the vCPU digits map to ARM; all others, empty and None map to x86."""
    assert get_arch_from_azure_instance_type(instance_type) == expected_arch


def generate_images_json_file(test_data_dir):
    """Generates azure_images_list.json based on real Azure images for unit tests purposes."""
    resource_group = "SCYLLA-IMAGES"
    images = AzureService().compute.images.list_by_resource_group(
        resource_group_name=resource_group,
    )
    with open(test_data_dir / resource_group / "azure_images_list.json", "w", encoding="utf-8") as images_file:
        serialized_images = [image.to_dict() | {"name": image.name} for image in images]
        images_file.write(json.dumps(serialized_images, indent=2))


def generate_gallery_images_json_file(test_data_dir, gallery_name="scylladb_dev", image_name="master-x64"):
    """Generates azure_images_list.json based on real Azure images for unit tests purposes."""
    resource_group = "SCYLLA-IMAGES"
    images = AzureService().compute.gallery_image_versions.list_by_gallery_image(
        resource_group_name=resource_group,
        gallery_name=gallery_name,
        gallery_image_name=image_name,
    )
    with open(
        test_data_dir / resource_group / f"{gallery_name}_{image_name}_gallery_images_list.json",
        "w",
        encoding="utf-8",
    ) as images_file:
        serialized_images = [image.to_dict() | {"name": image.name} for image in images]
        images_file.write(json.dumps(serialized_images, indent=2))


def test_get_image_tags_from_marketplace_urn():
    """get_image_tags derives the Scylla version and user-data format version from a marketplace URN."""
    tags = get_image_tags("scylladb1631195365807:scylladb-enterprise-vm:scylla-enterprise-vm:2026.2.5")
    assert tags == {"scylla_version": "2026.2.5", "user_data_format_version": "3"}
