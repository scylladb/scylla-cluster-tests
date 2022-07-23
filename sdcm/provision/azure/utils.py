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
import logging
from contextlib import suppress

from azure.core.exceptions import ResourceNotFoundError as AzureResourceNotFoundError
from azure.mgmt.compute.models import GalleryImageVersion

from sdcm.provision.provisioner import VmArch
from sdcm.utils.azure_utils import AzureService
from sdcm.utils.version_utils import SCYLLA_VERSION_GROUPED_RE


LOGGER = logging.getLogger(__name__)


def get_scylla_images(  # pylint: disable=too-many-branches,too-many-locals
        scylla_version: str,
        region_name: str,
        arch: VmArch = VmArch.X86,
        azure_service: AzureService = AzureService()
) -> list[GalleryImageVersion]:
    version_bucket = scylla_version.split(":", 1)
    only_latest = False
    tags_to_search = {
        'arch': arch.value
    }
    if len(version_bucket) == 1:
        if '.' in scylla_version:
            # Plain version, like 4.5.0
            tags_to_search['ScyllaVersion'] = lambda ver: ver and ver.startswith(scylla_version)
        else:
            # commit id
            tags_to_search['ScyllaVersion'] = lambda ver: ver and scylla_version[:9] in ver
    else:
        # Branched version, like master:latest
        branch, build_id = version_bucket
        tags_to_search['branch'] = branch
        if build_id == 'latest':
            only_latest = True
        elif build_id == 'all':
            pass
        else:
            tags_to_search['build-id'] = build_id
    output = []
    unparsable_scylla_versions = []
    with suppress(AzureResourceNotFoundError):
        gallery_image_versions = azure_service.compute.images.list_by_resource_group(
            resource_group_name="SCYLLA-IMAGES",
        )
        for image in gallery_image_versions:
            if image.location != region_name or image.name.startswith('debug-'):
                continue

            # Filter by tags
            for tag_name, expected_value in tags_to_search.items():
                actual_value = image.tags.get(tag_name)
                if callable(expected_value):
                    if not expected_value(actual_value):
                        break
                elif expected_value != actual_value:
                    break
            else:
                # example image name: ScyllaDB-5.1.dev-0.20220603.72f629c2b6e8-1-build-157
                if SCYLLA_VERSION_GROUPED_RE.match(image.name.split("ScyllaDB-", 1)[-1]):
                    output.append(image)
                else:
                    unparsable_scylla_versions.append(image.name)
    if unparsable_scylla_versions:
        LOGGER.warning("Couldn't parse scylla version from images: %s", str(unparsable_scylla_versions))
    output.sort(key=lambda img: int(img.tags.get('build_id', "0")))
    output.sort(key=lambda img: int(SCYLLA_VERSION_GROUPED_RE.match(
        img.name.split("ScyllaDB-", 1)[-1]).group("date")))

    if only_latest:
        return output[-1:]
    return output
