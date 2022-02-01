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

from contextlib import suppress

from azure.core.exceptions import ResourceNotFoundError as AzureResourceNotFoundError
from azure.mgmt.compute.models import GalleryImageVersion
from sdcm.provision.provisioner import VmArch
from sdcm.utils.azure_utils import AzureService


def get_scylla_images(  # pylint: disable=too-many-branches
        scylla_version: str,
        region_name: str,
        arch: VmArch = VmArch.X86
) -> list[GalleryImageVersion]:
    version_bucket = scylla_version.split(":", 1)
    only_latest = False
    tags_to_search = {
        'arch': arch
    }
    if len(version_bucket) == 1:
        if '.' in scylla_version:
            # Plain version, like 4.5.0
            tags_to_search['ScyllaVersion'] = lambda ver: ver and ver.startswith(scylla_version)
        else:
            # Commit id d28c3ee75183a6de3e9b474127b8c0b4d01bbac2
            tags_to_search['scylla-git-commit'] = scylla_version
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
    with suppress(AzureResourceNotFoundError):
        gallery_image_versions = AzureService().compute.images.list_by_resource_group(
            resource_group_name="scylla-images",
        )
        for image in gallery_image_versions:
            # Filter by region
            if image.location != region_name:
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
                output.append(image)

    output = sorted(output, key=lambda img: img.tags.get('build_id'))
    if only_latest:
        return output[-1:]
    return output
