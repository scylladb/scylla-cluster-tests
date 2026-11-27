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

"""Jenkins builders on Azure, through the Azure VM Agents plugin.

`configure()` creates everything from scratch: the region network, the builder subnet and NSG,
the builder resource group, the runner image, both Jenkins credentials, and the Jenkins cloud.
See docs/plans/jenkins/azure-builders/spec.md.
"""

import logging
from functools import cached_property
from pathlib import Path
from string import Template

import click
import requests

from sdcm.keystore import KeyStore
from sdcm.sct_runner import BUILDER_KEEP_HOURS, AzureSctRunner
from sdcm.utils.azure_region import AzureRegion
from sdcm.utils.builder_access import get_builder_ssh_sources

LOGGER = logging.getLogger(__name__)

AZURE_JENKINS_CONFIG_TEMPLATE = Template(
    (Path(__file__).parent / "builder_setup_groovy/azure_vm_agents_config.groovy.tmpl").read_text()
)
SUCCESS_LINE = "Azure cloud configured programmatically"
KEEP_ALIVE_TAGS = {"keep": "alive"}


def groovy_str(value: str) -> str:
    """Quote a value as a single-quoted Groovy string (no `$` interpolation)."""
    escaped = value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "")
    return f"'{escaped}'"


class AzureBuilder:
    VERSION = "v1"
    # getJenkinsLabels.groovy routes only eastus to Azure builders
    REGIONS = ["eastus"]
    PLUGIN = "azure-vm-agents"
    PLUGIN_MIN_VERSION = 1124
    VM_SIZE = "Standard_D2_v4"
    MAX_VMS = 20  # same as GCE and OCI; one artifacts run takes 8
    SP_CREDENTIALS_ID = "azure-sct-sp"
    SSH_CREDENTIALS_ID = "user-jenkins_scylla_test_id_ed25519.pem"
    SSH_KEY_NAME = "scylla_test_id_ed25519"

    def __init__(self, region: AzureRegion):
        self.region = region
        self.jenkins_info = KeyStore().get_json("jenkins.json")
        self.runner = AzureSctRunner(region_name=region.region_name, availability_zone="", params=None)

    @cached_property
    def name(self) -> str:
        return f"azure-{self.region.location}-qa-builder-{self.VERSION}"

    @cached_property
    def jenkins_labels(self) -> str:
        return f"azure-sct-builders-{self.region.location}-{self.VERSION}"

    @cached_property
    def template_name(self) -> str:
        # becomes the VM name prefix, keep it short and alphanumeric
        return f"sctbuilder{self.VERSION}"

    @cached_property
    def resource_group_name(self) -> str:
        return f"SCT-builders-{self.region.location}"

    @property
    def _auth(self) -> tuple[str, str]:
        return self.jenkins_info["username"], self.jenkins_info["password"]

    def check_plugin(self) -> None:
        resp = requests.get(
            f"{self.jenkins_info['url']}/pluginManager/api/json",
            params={"depth": 1, "tree": "plugins[shortName,version,active]"},
            auth=self._auth,
            timeout=30,
        )
        resp.raise_for_status()
        plugin = next((p for p in resp.json()["plugins"] if p["shortName"] == self.PLUGIN), None)
        if not plugin or not plugin["active"]:
            raise RuntimeError(f"Jenkins plugin {self.PLUGIN} is not installed or not active")
        major = int(plugin["version"].split(".")[0])
        if major < self.PLUGIN_MIN_VERSION:
            raise RuntimeError(f"{self.PLUGIN} {plugin['version']} is older than {self.PLUGIN_MIN_VERSION}")

    def _ensure_resource_group(self) -> None:
        resource_groups = self.region.azure_service.resource.resource_groups
        if resource_groups.check_existence(self.resource_group_name):
            group = resource_groups.get(self.resource_group_name)
            if (group.tags or {}).get("keep") == "alive":
                return
            tags = (group.tags or {}) | KEEP_ALIVE_TAGS
        else:
            tags = KEEP_ALIVE_TAGS
        click.secho(f"{self.region.region_name}: resource group {self.resource_group_name}, tags {tags}")
        resource_groups.create_or_update(self.resource_group_name, {"location": self.region.location, "tags": tags})

    def _ensure_runner_image(self) -> None:
        if not self.runner.image:
            click.secho(f"{self.region.region_name}: building the SCT runner image")
            self.runner.create_image()
        if not self.runner.image:
            raise RuntimeError(f"No SCT runner image {self.runner.GALLERY_IMAGE_VERSION} in {self.region.region_name}")

    def groovy_script(self) -> str:
        azure = KeyStore().get_azure_credentials()
        ssh_key = KeyStore().get_ssh_key_pair(name=self.SSH_KEY_NAME)
        gallery_region = AzureRegion(region_name=AzureRegion.SCT_GALLERY_REGION)
        strings = {
            "sp_credentials_id": self.SP_CREDENTIALS_ID,
            "subscription_id": azure["subscription_id"],
            "client_id": azure["client_id"],
            "client_secret": azure["client_secret"],
            "tenant_id": azure["tenant_id"],
            "ssh_credentials_id": self.SSH_CREDENTIALS_ID,
            "ssh_private_key": ssh_key.private_key.decode(),
            "gallery_name": gallery_region.sct_gallery_name,
            "gallery_image_definition": self.runner.GALLERY_IMAGE_NAME,
            "gallery_image_version": self.runner.GALLERY_IMAGE_VERSION,
            "gallery_resource_group": gallery_region.sct_gallery_resource_group_name,
            "virtual_network_name": self.region.sct_virtual_network_name,
            "virtual_network_resource_group": self.region.sct_resource_group_name,
            "subnet_name": AzureRegion.BUILDER_SUBNET_NAME,
            "template_name": self.template_name,
            "jenkins_labels": self.jenkins_labels,
            "location": self.region.location,
            "vm_size": self.VM_SIZE,
            "cloud_name": self.name,
            "builder_resource_group": self.resource_group_name,
            "keep_hours": BUILDER_KEEP_HOURS,
        }
        return AZURE_JENKINS_CONFIG_TEMPLATE.substitute(
            {key: groovy_str(value) for key, value in strings.items()},
            max_vms=self.MAX_VMS,
        )

    def _add_cloud_configuration_to_jenkins(self) -> None:
        click.secho(f"{self.region.region_name}: adding cloud {self.name} to Jenkins")
        response = requests.post(
            url=f"{self.jenkins_info['url']}/scriptText",
            auth=self._auth,
            data={"script": self.groovy_script()},
            timeout=60,
        )
        response.raise_for_status()
        # The script console answers 200 even when the script throws; the error is in the body.
        if SUCCESS_LINE not in response.text:
            raise RuntimeError(f"Jenkins script console failed to configure {self.name}:\n{response.text}")
        click.secho(response.text.strip(), fg="green")

    def configure(self) -> None:
        self.check_plugin()
        self.region.configure(tags=KEEP_ALIVE_TAGS)
        self.region.ensure_builder_network(get_builder_ssh_sources())
        self._ensure_resource_group()
        self._ensure_runner_image()
        self._add_cloud_configuration_to_jenkins()

    @classmethod
    def configure_in_all_region(cls, regions: list[str] | None = None) -> None:
        regions = regions or cls.REGIONS
        failed = []
        for region_name in regions:
            try:
                click.secho(f"\nConfiguring Azure builder in region: {region_name}", fg="cyan")
                cls(region=AzureRegion(region_name=region_name)).configure()
            except Exception:
                LOGGER.exception("Failed to configure the Azure builder in %s", region_name)
                # Continue with the next region, and fail once all were tried
                failed.append(region_name)
        if failed:
            raise RuntimeError(f"Failed to configure Azure builders in: {', '.join(failed)}")
