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
from functools import cached_property

import jenkins
import click
from mypy_boto3_ec2 import EC2ServiceResource

from sdcm.utils.aws_region import AwsRegion
from sdcm.utils.common import all_aws_regions
from sdcm.sct_runner import AwsSctRunner
from sdcm.utils.sct_cmd_helpers import CloudRegion, add_file_logger
from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)


class AwsBuilder:
    def __init__(self, region: AwsRegion, number=1):
        self.region = region
        self.number = number
        self.jenkins = jenkins.Jenkins(**KeyStore().get_json("jenkins.json"))
        self.runner = AwsSctRunner(region_name=self.region.region_name,
                                   availability_zone=self.region.availability_zones[0][-1])

    @cached_property
    def name(self):
        # example: aws-eu-central-1-qa-builder-v2-1
        return f"aws-{self.region.region_name}-qa-builder-v2-{self.number}"

    @cached_property
    def jenkins_labels(self):
        return f"aws-sct-builders-{self.region.region_name}-v2"

    @cached_property
    def instance(self) -> EC2ServiceResource.Instance:

        if not self.runner.image:
            LOGGER.error("SCT Runner image was not found in %s! "
                         "Use `hydra create-runner-image --cloud-provider %s --region %s'",
                         self.region.region_name, self.runner.CLOUD_PROVIDER, self.region.region_name)
            return None

        instances = self.region.client.describe_instances(Filters=[{"Name": "tag:Name",
                                                                    "Values": [self.name]},
                                                                   {"Name": "instance-state-name",
                                                                    "Values": ["running"]}])

        if instances["Reservations"] and instances["Reservations"][0]["Instances"]:
            return self.region.resource.Instance(instances["Reservations"][0]["Instances"][0]['InstanceId'])

        tags = {
            "bastion": "true",
            "NodeType": "builder",
            "RunByUser": "QA",
            "keep": "alive"
        }

        return self.runner._create_instance(  # pylint: disable=protected-access
            instance_type=self.runner.REGULAR_TEST_INSTANCE_TYPE,
            base_image=self.runner._get_base_image(self.runner.image),  # pylint: disable=protected-access
            tags=tags,
            instance_name=self.name,
            region_az=self.region.availability_zones[0],
        )

    def add_to_jenkins(self):
        LOGGER.info("Adding builder to jenkins:")
        ssh_params = {
            'port': '22',
            'username': 'jenkins',
            'credentialsId': 'user-jenkins_scylla-qa-ec2.pem',
            'host': self.elastic_ip.public_ip,
            "sshHostKeyVerificationStrategy": {
                "$class": "hudson.plugins.sshslaves.verifiers.NonVerifyingKeyVerificationStrategy",
                "stapler-class": "hudson.plugins.sshslaves.verifiers.NonVerifyingKeyVerificationStrategy"
            },
        }
        try:
            self.jenkins.create_node(
                self.name,
                numExecutors=15,
                nodeDescription='QA Builder',
                remoteFS='/home/jenkins/slave',
                labels=self.jenkins_labels,
                launcher=jenkins.LAUNCHER_SSH,
                launcher_params=ssh_params)
            LOGGER.info("%s added to jenkins successfully", self.name)
        except jenkins.JenkinsException as ex:
            if 'already exists' not in str(ex):
                raise
            LOGGER.info("%s was already added to jenkins", self.name)

    @property
    def elastic_ip(self) -> EC2ServiceResource.VpcAddress:
        name = self.name
        addresses = self.region.client.describe_addresses(Filters=[{"Name": "tag:Name",
                                                                    "Values": [self.name]}])
        LOGGER.debug("Found Address: %s", addresses)
        existing_addresses = addresses.get("Addresses", [])
        if len(existing_addresses) == 0:
            return None
        assert len(existing_addresses) == 1, \
            f"More than 1 VpcAddress with {name} found " \
            f"in {self.region.region_name}: {existing_addresses}!"
        return self.region.resource.VpcAddress(existing_addresses[0]["AllocationId"])  # pylint: disable=no-member

    def create_elastic_ip(self):
        LOGGER.info("Creating elastic address...")
        if eip := self.elastic_ip:
            LOGGER.warning("elastic ip '%s' already exists! Id: '%s'.",
                           self.name, eip.allocation_id)
        else:
            self.region.client.allocate_address(Domain='vpc', TagSpecifications=[{
                'ResourceType': 'elastic-ip',
                'Tags': [
                    {'Key': 'Name', 'Value': self.name},
                    {'Key': 'NodeType', 'Value': 'builder'},
                    {'Key': 'RunByUser', 'Value': 'QA'}
                ]
            }])

    def associate_elastic_ip(self):
        LOGGER.info("associate addresss '%s' with '%s'", self.elastic_ip.public_ip, self.name)
        self.region.client.associate_address(AllocationId=self.elastic_ip.allocation_id,
                                             InstanceId=self.instance.instance_id)

    def configure(self):
        self.region.create_sct_builders_security_group()
        self.create_elastic_ip()
        self.associate_elastic_ip()
        self.add_to_jenkins()

    @classmethod
    def configure_in_all_region(cls, regions=None):
        regions = regions or all_aws_regions(cached=True)
        for region_name in regions:
            region = cls(AwsRegion(region_name))
            region.configure()


configure_jenkins_builders: click.Command


@click.command("configure-jenkins-builders", help="Configure all required jenkins builders for SCT")
@click.option("-r", "--regions", type=CloudRegion(cloud_provider='aws'),
              default=[], help="Cloud regions", multiple=True)
def configure_jenkins_builders(regions):
    add_file_logger()
    AwsBuilder.configure_in_all_region(regions=regions)


if __name__ == '__main__':
    configure_jenkins_builders()  # pylint: disable=no-value-for-parameter
