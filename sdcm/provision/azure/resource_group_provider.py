import logging
from dataclasses import dataclass, field
from typing import Dict

from azure.mgmt.resource.resources.models import ResourceGroup

from sdcm.utils.azure_utils import AzureService

LOGGER = logging.getLogger(__name__)


@dataclass
class ResourceGroupProvider:
    """Class for providing resource groups and taking care about discovery existing ones."""
    _prefix: str
    _azure_service: AzureService = AzureService()
    _cache: Dict[str, ResourceGroup] = field(default_factory=dict)

    def __post_init__(self):
        """Discover existing rg's for this provider."""
        rg_names = [rg.name for rg in list(self._azure_service.resource.resource_groups.list()) if
                    rg.name.startswith(self._prefix)]
        for resource_group_name in rg_names:
            LOGGER.info("getting resources for {}...".format(resource_group_name))
            resource_group = self._azure_service.resource.resource_groups.get(resource_group_name)
            self._cache[resource_group.name] = resource_group

    def groups(self) -> Dict[str, ResourceGroup]:
        """Returns list of discovered/created resources groups for this provider."""
        return self._cache

    def get_or_create(self, region: str) -> ResourceGroup:
        name = f"{self._prefix}-{region.lower()}"
        if name in self._cache:
            LOGGER.debug("Found resource group: {name} in cache".format(name=name))
            return self._cache[name]
        LOGGER.info("Creating {name} SCT resource group in region {region}...".format(name=name, region=region))
        resource_group = self._azure_service.resource.resource_groups.create_or_update(
            resource_group_name=name,
            parameters={
                "location": region
            },
        )
        LOGGER.info("Provisioned resource group {name} in the {region} region".format(
            name=resource_group.name, region=resource_group.location))
        self._cache[name] = resource_group
        return resource_group

    def clean_all(self, wait: bool = False):
        tasks = []
        LOGGER.info("Initiating cleanup of all resources...")
        for resource_group_name in self.groups():
            tasks.append(self._azure_service.resource.resource_groups.begin_delete(resource_group_name))
        LOGGER.info("Initiated cleanup of all resources")
        if wait is True:
            LOGGER.info("Waiting for completion of all resources cleanup")
            for task in tasks:
                task.wait()
