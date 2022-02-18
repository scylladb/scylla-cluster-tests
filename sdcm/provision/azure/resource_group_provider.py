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
from dataclasses import dataclass, field
from typing import Optional

from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.resource.resources.models import ResourceGroup

from sdcm.utils.azure_utils import AzureService

LOGGER = logging.getLogger(__name__)


@dataclass
class ResourceGroupProvider:
    """Class for providing resource groups and taking care about discovery existing ones."""
    _name: str
    _region: str
    _azure_service: AzureService = AzureService()
    _cache: Optional[ResourceGroup] = field(default=None)

    def __post_init__(self):
        """Discover existing resource group for this provider."""
        try:
            resource_group = self._azure_service.resource.resource_groups.get(self._name)
            assert resource_group.location == self._region, \
                f"resource group {resource_group.name} does not belong to {self._region} region (location)"
            self._cache = resource_group
        except ResourceNotFoundError:
            pass

    def get_or_create(self) -> ResourceGroup:
        if self._cache is not None:
            LOGGER.debug("Found resource group: {name} in cache".format(name=self._name))
            return self._cache
        LOGGER.info("Creating {name} SCT resource group in region {region}...".format(
            name=self._name, region=self._region))
        resource_group = self._azure_service.resource.resource_groups.create_or_update(
            resource_group_name=self._name,
            parameters={
                "location": self._region
            },
        )
        LOGGER.info("Provisioned resource group {name} in the {region} region".format(
            name=resource_group.name, region=resource_group.location))
        self._cache = resource_group
        return resource_group

    def delete(self, wait: bool = False):
        """Deletes resource group along with all contained resources."""
        LOGGER.info("Initiating cleanup of resource group: {name}...".format(name=self._name))
        task = self._azure_service.resource.resource_groups.begin_delete(self._name)
        LOGGER.info("Cleanup initiated")
        self._cache = None
        if wait is True:
            LOGGER.info("Waiting for cleanup completion")
            task.wait()
