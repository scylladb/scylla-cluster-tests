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
import pprint
import re
from contextlib import suppress

from azure.core.exceptions import ResourceNotFoundError as AzureResourceNotFoundError
from azure.mgmt.compute.models import GalleryImageVersion, CommunityGalleryImageVersion

from sdcm.provision.provisioner import VmArch
from sdcm.utils.azure_utils import AzureService
from sdcm.utils.version_utils import (
    SCYLLA_VERSION_GROUPED_RE,
    is_enterprise,
    ComparableScyllaVersion,
)

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
            tags_to_search['scylla_version'] = lambda ver: ver and ver.replace("~", "-").startswith(scylla_version)\
                and '-dev' not in ver
        else:
            # commit id
            tags_to_search['scylla_version'] = lambda ver: ver and scylla_version[:9] in ver
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
            image.tags["scylla_version"] = image.tags.get('scylla_version', image.tags.get('ScyllaVersion'))
            # Filter by tags
            for tag_name, expected_value in tags_to_search.items():
                actual_value = image.tags.get(tag_name).replace('~', '-')
                if callable(expected_value):
                    if not expected_value(actual_value):
                        break
                elif expected_value != actual_value:
                    break
            else:
                if SCYLLA_VERSION_GROUPED_RE.match(image.tags.get("scylla_version")):
                    output.append(image)
                else:
                    unparsable_scylla_versions.append(f"{image.name}: {image.tags.get('scylla_version')}")
    if unparsable_scylla_versions:
        LOGGER.warning("Couldn't parse scylla version from images: %s", str(unparsable_scylla_versions))
    output.sort(key=lambda img: int(SCYLLA_VERSION_GROUPED_RE.match(
        img.tags.get('scylla_version')).group("date")))

    if only_latest:
        return output[-1:]
    return output[::-1]


def get_released_scylla_images(  # pylint: disable=unused-argument
        scylla_version: str,
        region_name: str,
        arch: VmArch = VmArch.X86,
        azure_service: AzureService = AzureService()

) -> list[CommunityGalleryImageVersion]:
    branch_version = '.'.join(scylla_version.split('.')[:2])
    if is_enterprise(branch_version) and ComparableScyllaVersion(branch_version) < '2025.1.0':
        gallery_image_name = f'scylla-enterprise-{branch_version}'
    else:
        gallery_image_name = f'scylla-{branch_version}'
    community_gallery_images = azure_service.compute.community_gallery_image_versions.list(
        location=region_name,
        gallery_image_name=gallery_image_name,
        public_gallery_name='scylladb-7e8d8a04-23db-487d-87ec-0e175c0615bb',
    )
    community_gallery_images: list[CommunityGalleryImageVersion] = list(community_gallery_images)
    community_gallery_images.sort(key=lambda x: x.published_date, reverse=True)

    # a specific version was asked, return only that version
    if branch_version != scylla_version:
        specific_version = [image for image in community_gallery_images if image.name == scylla_version]
        assert specific_version, f"`{scylla_version}` wasn't found in:\n{pprint.pformat(community_gallery_images)}"
        return specific_version

    return community_gallery_images


IMAGE_URL_REGEX = re.compile(
    r'.*/resourceGroups/(?P<resource_group_name>.*)/providers/Microsoft.Compute/images/(?P<image_name>.*)')


COMMUNITY_IMAGE_URL_REGEX = re.compile(
    r'.*/CommunityGalleries/(?P<public_gallery_name>.*)/Images/(?P<gallery_image_name>.*)/Versions/(?P<version>.*)')


def get_image_tags(link: str) -> dict:
    if match := IMAGE_URL_REGEX.search(link):
        params = match.groupdict()
        azure_image: GalleryImageVersion = AzureService().compute.images.get(**params)
        return azure_image.tags
    elif match := COMMUNITY_IMAGE_URL_REGEX.search(link):
        params = match.groupdict()
        return dict(scylla_version=params.get('version'), user_data_format_version='3')
    return {}
