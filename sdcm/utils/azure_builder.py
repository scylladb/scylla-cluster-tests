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
import logging
from functools import cached_property

import click
import requests

from sdcm.utils.azure_region import AzureRegion
from sdcm.sct_runner import AzureSctRunner
from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)

JENKINS_CONFIG_FORMAT = """
// just modify this config other code just logic
config = [
    name: "{name}",
    instanceCapStr: "{max_instances}",
    labels: "{jenkins_labels}",
]
import com.microsoft.azure.vmagent.builders.*
import com.microsoft.azure.vmagent.AzureTagPair
import com.microsoft.azure.vmagent.AzureVMCloudRetensionStrategy

tags = List.of(new AzureTagPair("NodeType", "builder"),
               new AzureTagPair("keep", "alive"),
               new AzureTagPair("RunByUser", "QA"))

def azureCloud = new AzureVMCloudBuilder()
.withCloudName("${{config.name}}")
.withAzureCredentialsId("azure_credentials")
.withNewResourceGroupName("SCT-eastus")
.withMaxVirtualMachinesLimit("20")
.addNewTemplate()
    .withName("sct-builder")
    .withLabels(config.labels)
    .withLocation("{region_name}")
    .withVirtualMachineSize("Standard_E2s_v3")
    .withTags(tags)
    .withNewStorageAccount("jnubj2xeah7wwdkfhjrf1rkg")
    .withRetentionStrategy(new AzureVMCloudRetensionStrategy(10))
/*  Can't use sct-runner image, since the Azure plugin can't work with ssh keys
    see: https://github.com/jenkinsci/azure-vm-agents-plugin/issues/233#issuecomment-1422227202
     .addNewAdvancedImage()
        .withGalleryImage("{runner.azure_region.sct_gallery_name}",
                          "{runner.GALLERY_IMAGE_NAME}",
                          "{runner.GALLERY_IMAGE_VERSION}",
                          false,
                          '6c268694-47ab-43ab-b306-3c5514bc4112',
                          '{runner.azure_region.sct_gallery_resource_group_name}')
        .withNumberOfExecutors("4")
    .endAdvancedImage()
*/
    .addNewAdvancedImage()
        .withReferenceImage("canonical", "0001-com-ubuntu-server-jammy", "22_04-lts-gen2", "latest")
        .withInitScript('''
            # Make sure that cloud-init finished running.
            until [ -f /var/lib/cloud/instance/boot-finished ]; do sleep 1; done

            echo "fs.aio-max-nr = {{AIO_MAX_NR_RECOMMENDED_VALUE}}" >> /etc/sysctl.conf
            echo "jenkins soft nofile 4096" >> /etc/security/limits.conf
            echo "root soft nofile 4096" >> /etc/security/limits.conf

             # Disable apt-key warnings and set non-interactive frontend.
            export APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1
            export DEBIAN_FRONTEND=noninteractive

            apt-get -qq clean
            apt-get -qq update
            apt-get -qq install --no-install-recommends python3-pip htop screen tree
            pip3 install awscli

            # Install Docker.
            apt-get -qq install --no-install-recommends \
                apt-transport-https ca-certificates curl gnupg-agent software-properties-common
            curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
            add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
            apt-get -qq install --no-install-recommends docker-ce docker-ce-cli containerd.io

            # Install kubectl.
            curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
            install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

            # Configure Jenkins user.
            apt-get -qq install --no-install-recommends openjdk-11-jre-headless  # https://www.jenkins.io/doc/administration/requirements/java/
            adduser --disabled-password --gecos "" jenkins || true
            usermod -aG docker jenkins
            mkdir -p /home/jenkins/.ssh
            chmod 600 /home/jenkins/.ssh/authorized_keys
            chown -R jenkins:jenkins /home/jenkins
            echo "jenkins ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/jenkins

            # Jenkins pipelines run /bin/sh for some reason.
            ln -sf /bin/bash /bin/sh
        ''')
        .withNumberOfExecutors("4")
    .endAdvancedImage()
    .withAdminCredential("azure-builder")
.endTemplate()
.build()

// get Jenkins instance
Jenkins jenkins = Jenkins.get()

// clear old configuration
jenkins.clouds.removeAll {{ it.name == "${{config.name}}" }}

res = jenkins.clouds.add(azureCloud)

// save current Jenkins state to disk
jenkins.save()
"""


class AzureBuilder:
    """
    This class is for configuration our Jenkins setup for Azure

    It creates a launch template based on sct-runner image, and adds configuration needed in Jenkins to use it
    """

    def __init__(self, region: AzureRegion, number=1):
        self.region = region
        self.number = number
        self.jenkins_info = KeyStore().get_json("jenkins.json")
        self.runner = AzureSctRunner(region_name=self.region.region_name,
                                     availability_zone="a")

    @cached_property
    def name(self):
        # example: azure-us-east1-qa-builder
        return f"azure-{self.region.location}-qa-builder"

    @cached_property
    def jenkins_labels(self):
        return f"azure-builders-{self.region.location}"

    def _add_cloud_configuration_to_jenkins(self):
        click.echo(f"{self.region.location}: add_cloud_configuration_to_jenkins")
        res = requests.post(url=f"{self.jenkins_info['url']}/scriptText",
                            auth=(self.jenkins_info['username'], self.jenkins_info['password']),
                            params=dict(script=JENKINS_CONFIG_FORMAT.format(name=self.name,
                                                                            region_name=self.region.region_name,
                                                                            max_instances=20,
                                                                            runner=self.runner,
                                                                            jenkins_labels=self.jenkins_labels)))
        res.raise_for_status()
        click.echo(res.text)
        assert not res.text

    def configure(self):
        self._add_cloud_configuration_to_jenkins()

    @classmethod
    def configure_in_all_region(cls, regions=None):
        regions = regions or ["East US"]
        for region_name in regions:
            azure_builder = cls(AzureRegion(region_name))
            azure_builder.configure()


if __name__ == "__main__":
    AzureBuilder.configure_in_all_region()
