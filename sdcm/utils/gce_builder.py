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
# Copyright (c) 2023 ScyllaDB
import sys
import logging
from typing import Any
from functools import cached_property

import click
import requests
from google.oauth2 import service_account
from google.cloud import compute_v1
from google.api_core.extended_operation import ExtendedOperation
import google.api_core.exceptions

from sdcm.utils.gce_region import GceRegion
from sdcm.utils.gce_utils import SUPPORTED_PROJECTS, SUPPORTED_REGIONS
from sdcm.utils.context_managers import environment
from sdcm.sct_runner import GceSctRunner
from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)

JENKINS_CONFIG_FORMAT = """
// just modify this config other code just logic
config = [
    name: "{name}",
    region: "{region_name}",
    projectId: "{project_id}",
    credentialsId: "{project_id}",
    instanceCapStr: "{max_instances}",
    labels: "{jenkins_labels}",
]

import com.google.jenkins.plugins.computeengine.ComputeEngineCloud
import com.google.jenkins.plugins.computeengine.InstanceConfiguration
import com.google.jenkins.plugins.computeengine.SshConfiguration
import hudson.plugins.sshslaves.SSHConnector
import hudson.plugins.sshslaves.verifiers.NonVerifyingKeyVerificationStrategy
import hudson.model.*
import jenkins.model.Jenkins

ComputeEngineCloud gcpCloud = new ComputeEngineCloud(config.name, config.projectId, config.credentialsId, config.instanceCapStr)
gcpCloud.setNoDelayProvisioning(true)

InstanceConfiguration instanceConfiguration = new InstanceConfiguration()
instanceConfiguration.setNumExecutorsStr("1")
instanceConfiguration.setOneShot(false)
instanceConfiguration.setNamePrefix("builders")
instanceConfiguration.setDescription("SCT Jenkins builders")
instanceConfiguration.setRegion("https://www.googleapis.com/compute/v1/projects/{project_id}/regions/{region_name}")
instanceConfiguration.setZone("https://www.googleapis.com/compute/v1/projects/{project_id}/zones/{region_name}-b")
instanceConfiguration.setLabelString(config.labels)
instanceConfiguration.setMode(Node.Mode.NORMAL)
instanceConfiguration.setTemplate("https://www.googleapis.com/compute/v1/projects/{project_id}/global/instanceTemplates/{name}")
instanceConfiguration.setRetentionTimeMinutesStr("6")
instanceConfiguration.setLaunchTimeoutSecondsStr("300")
instanceConfiguration.setRunAsUser("jenkins")
instanceConfiguration.setSshConfiguration(SshConfiguration.builder().customPrivateKeyCredentialsId("user-jenkins_scylla_test_id_ed25519.pem").build())

gcpCloud.addConfiguration(instanceConfiguration)

// get Jenkins instance
Jenkins jenkins = Jenkins.get()

// clear old configuration
jenkins.clouds.removeAll {{ it.name == "gce-${{config.name}}" }}

res = jenkins.clouds.add(gcpCloud)

// save current Jenkins state to disk
jenkins.save()
"""


class GceBuilder:
    """
    This class is for configuration our Jenkins setup for GCE

    It creates a launch template based on sct-runner image, and adds configuration needed in Jenkins to use it
    """
    VERSION = 'v6'

    def __init__(self, region: GceRegion):
        self.region = region
        self.jenkins_info = KeyStore().get_json("jenkins.json")
        self.runner = GceSctRunner(region_name=self.region.region_name,
                                   availability_zone="a")

        info = KeyStore().get_gcp_credentials()
        self.credentials = service_account.Credentials.from_service_account_info(info)

    @cached_property
    def name(self):
        # example: gce-sct-project-1-us-east1-qa-builder
        return f"gce-{self.region.project}-{self.region.region_name}-qa-builder-{self.VERSION}"

    @cached_property
    def jenkins_labels(self):
        return f"gcp-{self.region.project}-builders-{self.region.region_name}-template-{self.VERSION}"

    def _add_cloud_configuration_to_jenkins(self):
        click.echo(f"{self.region.project}: {self.region.region_name}: add_cloud_configuration_to_jenkins")
        res = requests.post(url=f"{self.jenkins_info['url']}/scriptText",
                            auth=(self.jenkins_info['username'], self.jenkins_info['password']),
                            params=dict(script=JENKINS_CONFIG_FORMAT.format(name=self.name,
                                                                            project_id=self.region.project,
                                                                            region_name=self.region.region_name,
                                                                            max_instances=20,
                                                                            jenkins_labels=self.jenkins_labels)))
        res.raise_for_status()
        logging.info(res.text)
        assert not res.text

    @staticmethod
    def _wait_for_extended_operation(
            operation: ExtendedOperation, verbose_name: str = "operation", timeout: int = 300
    ) -> Any:
        """
        This method will wait for the extended (long-running) operation to
        complete. If the operation is successful, it will return its result.
        If the operation ends with an error, an exception will be raised.
        If there were any warnings during the execution of the operation
        they will be printed to sys.stderr.

        Args:
            operation: a long-running operation you want to wait on.
            verbose_name: (optional) a more verbose name of the operation,
                used only during error and warning reporting.
            timeout: how long (in seconds) to wait for operation to finish.
                If None, wait indefinitely.

        Returns:
            Whatever the operation.result() returns.

        Raises:
            This method will raise the exception received from `operation.exception()`
            or RuntimeError if there is no exception set, but there is an `error_code`
            set for the `operation`.

            In case of an operation taking longer than `timeout` seconds to complete,
            a `concurrent.futures.TimeoutError` will be raised.
        """
        result = operation.result(timeout=timeout)

        if operation.error_code:
            click.echo(
                f"Error during {verbose_name}: [Code: {operation.error_code}]: {operation.error_message}",
                file=sys.stderr,
            )
            click.echo(f"Operation ID: {operation.name}", file=sys.stderr)
            raise operation.exception() or RuntimeError(operation.error_message)

        if operation.warnings:
            click.echo(f"Warnings during {verbose_name}:\n", file=sys.stderr)
            for warning in operation.warnings:
                click.echo(f" - {warning.code}: {warning.message}", file=sys.stderr)

        return result

    def _create_launch_template(self) -> compute_v1.InstanceTemplate:
        """
        Create a new instance template with the provided name and a specific
        instance configuration.

        Returns:
            InstanceTemplate object that represents the new instance template.
        """
        # The template describes the size and source image of the boot disk
        # to attach to the instance.
        disk = compute_v1.AttachedDisk()
        initialize_params = compute_v1.AttachedDiskInitializeParams()
        initialize_params.source_image = (
            self.runner.image.self_link
        )
        initialize_params.disk_type = 'pd-standard'
        initialize_params.disk_size_gb = 80
        disk.initialize_params = initialize_params
        disk.auto_delete = True
        disk.boot = True

        # The template connects the instance to the `default` network,
        # without specifying a subnetwork.
        network_interface = compute_v1.NetworkInterface()
        network_interface.network = self.region.network.self_link

        # The template lets the instance use an external IP address.
        access_config = compute_v1.AccessConfig()
        access_config.name = "External NAT"
        access_config.type_ = "ONE_TO_ONE_NAT"
        access_config.network_tier = "PREMIUM"
        network_interface.access_configs = [access_config]

        template = compute_v1.InstanceTemplate()
        template.name = self.name
        template.properties.disks = [disk]
        template.properties.machine_type = "e2-medium"
        template.properties.network_interfaces = [network_interface]

        metadata = compute_v1.Metadata()
        metadata.items += [{"key": "RunByUser", "value": "QA"}]
        metadata.items += [{"key": "NodeType", "value": "builder"}]
        metadata.items += [{"key": "keep", "value": "alive"}]
        template.properties.metadata = metadata

        template.properties.labels = {"runbyuser": "qa", "nodetype": "builder", "keep": "alive"}

        template_client = compute_v1.InstanceTemplatesClient(credentials=self.credentials)
        try:
            operation = template_client.insert(
                project=self.region.project, instance_template_resource=template
            )

            self._wait_for_extended_operation(operation, "instance template creation")
        except google.api_core.exceptions.Conflict:
            click.echo(f'template: {template.name} already exists. '
                       'if change affecting were made, delete it and run this again')

        return template_client.get(project=self.region.project, instance_template=self.name)

    def configure(self):
        self._create_launch_template()
        self._add_cloud_configuration_to_jenkins()

    @classmethod
    def configure_in_all_region(cls, regions=None):
        for project in SUPPORTED_PROJECTS:
            with environment(SCT_GCE_PROJECT=project):
                regions = regions or SUPPORTED_REGIONS.keys()
                for region_name in regions:
                    gce_builder = cls(GceRegion(region_name))
                    gce_builder.configure()
