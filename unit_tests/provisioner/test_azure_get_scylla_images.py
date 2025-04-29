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
from pathlib import Path

import pytest

from sdcm.provision.azure.utils import get_scylla_images
from sdcm.utils.azure_utils import AzureService
from unit_tests.provisioner.fake_azure_service import FakeAzureService


@pytest.fixture
def azure_service():
    return FakeAzureService(Path(__file__).parent / "test_data")


def test_can_get_scylla_images_based_on_branch(azure_service):
    images = get_scylla_images("master:latest", "eastus", azure_service=azure_service)
    assert images[0].name == "ScyllaDB-2022.1.rc9-0.20220721.9c95c3a8c-1-build-11"
    assert len(images) == 1


def test_can_get_scylla_images_based_on_scylla_version(azure_service):
    images = get_scylla_images("4.6.4", "eastus", azure_service=azure_service)
    assert images[0].name == "ScyllaDB-4.6.4-0.20220718.b60f14601-1-build-28"
    assert len(images) == 2


def test_can_get_scylla_images_based_on_revision_id(azure_service):
    images = get_scylla_images("11f008e8f<ignored>", "eastus", azure_service=azure_service)
    assert len(images) == 1
    assert images[0].name == "ScyllaDB-4.6.4-0.20220516.11f008e8f-1-build-27"


def test_unparsable_scylla_versions_are_logged(azure_service, caplog):
    with caplog.at_level(logging.WARNING):
        get_scylla_images("unparsable:latest", "eastus", azure_service=azure_service)
    assert "Couldn't parse scylla version from images: ['ScyllaDB-5.-98ad.1.dev_0: ScyllaDB-5.']" in caplog.text


def generate_images_json_file():
    """generates azure_images_list.json based on real Azure images for unit tests purposes. """
    resource_group = "SCYLLA-IMAGES"
    images = AzureService().compute.images.list_by_resource_group(
        resource_group_name=resource_group,
    )
    with open(Path(__file__).parent / "test_data" / resource_group / "azure_images_list.json", "w", encoding="utf-8") as images_file:
        serialized_images = [image.serialize() | {"name": image.name} for image in images]
        images_file.write(json.dumps(serialized_images, indent=2))
