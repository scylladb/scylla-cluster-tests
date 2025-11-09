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
import boto3
import botocore
import requests

from sdcm.utils.aws_region import AwsRegion
from sdcm.sct_runner import AwsSctRunner, AwsFipsSctRunner
from sdcm.keystore import KeyStore
from sdcm.utils.common import wait_ami_available

LOGGER = logging.getLogger(__name__)

ASG_CONFIG_FORMAT = """
import com.amazonaws.services.ec2.model.InstanceType
import hudson.plugins.sshslaves.SSHConnector
import hudson.plugins.sshslaves.verifiers.NonVerifyingKeyVerificationStrategy
import hudson.model.*
import com.amazon.jenkins.ec2fleet.EC2FleetCloud
import jenkins.model.Jenkins

// just modify this config other code just logic
config = [
    name: "{name}",
    region: "{region_name}",
    fleetId: "{asg_id}",
    idleMinutes: 10,
    minSize: {min_size},
    maxSize: {max_size},
    numExecutors: {num_executors},
    labels: "{jenkins_labels}",
]

// find detailed information about parameters on plugin config page or
// https://github.com/jenkinsci/ec2-fleet-plugin/blob/master/src/main/java/com/amazon/jenkins/ec2fleet/EC2FleetCloud.java
EC2FleetCloud ec2FleetCloud = new EC2FleetCloud(
  config.name, // fleetCloudName
  "jenkins2 aws account", // awsCredentialsId
  "", // credentialsId
  config.region,
  "", // endpoint
  config.fleetId,
  config.labels,  // labels
  "/tmp/jenkins/", // fs root
  new SSHConnector(22,
                   "user-jenkins_scylla_test_id_ed25519.pem", "-Djdk.httpclient.maxLiteralWithIndexing=0 -Djdk.httpclient.maxNonFinalResponses=0", "", "", "", null, null, null,
                   new NonVerifyingKeyVerificationStrategy()),
  false, // privateIpUsed
  true, // alwaysReconnect
  config.idleMinutes, // if need to allow downscale set > 0 in min
  config.minSize, // minSize
  config.maxSize, // maxSize
  0,  // minSpareSize
  config.numExecutors, // numExecutors
  false, // addNodeOnlyIfRunning
  true, // restrictUsage allow execute only jobs with proper label
  "10", // maxTotalUses
  true, // disableTaskResubmit,
  600, // initOnlineTimeoutSec,
  60, // initOnlineCheckIntervalSec,
  60, // cloudStatusIntervalSec,
  true, // noDelayProvision
  false, // scaleExecutorsByWeight
  new EC2FleetCloud.NoScaler(), // EC2FleetCloud.ExecutorScaler executorScaler
)

// get Jenkins instance
Jenkins jenkins = Jenkins.get()

// clear old configuration
jenkins.clouds.removeAll {{ it.name == config.name }}

// add cloud configuration to Jenkins
jenkins.clouds.add(ec2FleetCloud)

// save current Jenkins state to disk
jenkins.save()
"""


class AwsBuilder:
    NUM_CPUS = 2
    NUM_EXECUTORS = 1
    VERSION = 'v3'

    def __init__(self, region: AwsRegion, params=None, number=1):
        self.region = region
        self.number = number
        self.params = params
        self.jenkins_info = KeyStore().get_json("jenkins.json")
        self.jenkins = jenkins.Jenkins(**self.jenkins_info)

    @cached_property
    def name(self):
        # example: aws-eu-central-1-qa-builder-v2-1
        return f"aws-{self.region.region_name}-qa-builder-{self.VERSION}-{self.number}"

    @cached_property
    def jenkins_labels(self):
        return f"aws-sct-builders-{self.region.region_name}-{self.VERSION}-asg"

    @property
    def launch_template_name(self):
        return f"aws-sct-builders-{self.VERSION}"

    def get_root_ebs_info_from_ami(self, ami_id: str) -> str:
        res = self.region.resource.Image(ami_id)
        return res.block_device_mappings[0].get('Ebs', {})

    def get_launch_template_data(self, runner: AwsSctRunner) -> dict:
        wait_ami_available(self.region.client, runner.image.id)
        return dict(
            LaunchTemplateData={
                "BlockDeviceMappings": [{"DeviceName": "/dev/sda1", "Ebs": self.get_root_ebs_info_from_ami(runner.image.id) | {"Iops": 3000, "VolumeType": "gp3", "Throughput": 125}}],
                "ImageId": runner.image.id,
                "KeyName": self.region.SCT_KEY_PAIR_NAME,
                "SecurityGroupIds": [self.region.sct_ssh_security_group.id, self.region.sct_security_group.id],
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": "Name", "Value": self.launch_template_name},
                            {"Key": "RunByUser", "Value": "QA"},
                        ],
                    },
                ],
            },

        )

    def update_launch_template_if_needed(self, runner):
        click.secho(f"{self.region.region_name}: checking if template needs update")

        launch_template_data = self.get_launch_template_data(runner)

        res = self.region.client.describe_launch_template_versions(
            LaunchTemplateName=self.launch_template_name,
        )
        default_version = [ver for ver in res['LaunchTemplateVersions'] if ver.get('DefaultVersion')][0]
        curr_launch_template_data = default_version.get('LaunchTemplateData')

        if launch_template_data.get('LaunchTemplateData') != curr_launch_template_data:
            try:
                click.secho(f"{self.region.region_name}: updating template")
                res = self.region.client.create_launch_template_version(
                    LaunchTemplateName=self.launch_template_name,
                    SourceVersion=str(default_version.get('VersionNumber')),
                    **launch_template_data
                )
                version_number = res.get('LaunchTemplateVersion', {}).get('VersionNumber')
                self.region.client.modify_launch_template(LaunchTemplateName=self.launch_template_name,
                                                          DefaultVersion=str(version_number))
            except botocore.exceptions.ClientError as error:
                LOGGER.debug(error.response)
                if not error.response['Error']['Code'] == 'InvalidLaunchTemplateName.AlreadyExistsException':
                    raise

    @property
    def sct_runner(self):
        return AwsSctRunner(region_name=self.region.region_name, availability_zone='a', params=None)

    def create_launch_template(self):
        click.secho(f"{self.region.region_name}: create_launch_template")
        runner = self.sct_runner
        if not runner.image:
            runner.create_image()
        try:
            self.region.client.create_launch_template(
                LaunchTemplateName=self.launch_template_name,
                **self.get_launch_template_data(runner)
            )
        except botocore.exceptions.ClientError as error:
            LOGGER.debug(error.response)
            if error.response['Error']['Code'] == 'InvalidLaunchTemplateName.AlreadyExistsException':
                self.update_launch_template_if_needed(runner)
            else:
                raise

    def create_auto_scaling_group(self):
        click.secho(f"{self.region.region_name}: create_auto_scaling_group")
        try:
            asg_client = boto3.client('autoscaling', region_name=self.region.region_name)
            subnet_ids = [self.region.sct_subnet(region_az=az).subnet_id for az in self.region.availability_zones]
            asg_client.create_auto_scaling_group(
                AutoScalingGroupName=self.name,
                MinSize=0,
                MaxSize=200,
                AvailabilityZones=self.region.availability_zones,
                VPCZoneIdentifier=",".join(subnet_ids),
                MixedInstancesPolicy={
                    "LaunchTemplate": {
                        "LaunchTemplateSpecification": {"LaunchTemplateName": self.launch_template_name, "Version": "$Latest"},
                        "Overrides": [
                            {
                                "InstanceRequirements": {
                                    "VCpuCount": {"Min": self.NUM_CPUS, "Max": self.NUM_CPUS},
                                    "MemoryMiB": {"Min": 4096, "Max": 8192},
                                }
                            }
                        ],
                    },
                    "InstancesDistribution": {"OnDemandAllocationStrategy": "lowest-price", "OnDemandBaseCapacity": 0, "OnDemandPercentageAboveBaseCapacity": 100, "SpotAllocationStrategy": "price-capacity-optimized"},
                },
                Tags=[
                    {"Key": "Name", "Value": "sct-jenkins-builder-asg", "PropagateAtLaunch": True},
                    {"Key": "NodeType", "Value": "builder", "PropagateAtLaunch": True},
                    {"Key": "RunByUser", "Value": "qa", "PropagateAtLaunch": True},
                    {"Key": "keep", "Value": "alive", "PropagateAtLaunch": True},
                    {"Key": "keep_action", "Value": "terminate", "PropagateAtLaunch": True},
                ],
            )

        except botocore.exceptions.ClientError as error:
            LOGGER.info(error.response)
            if not error.response['Error']['Code'] in ['AlreadyExists',]:
                raise

    def add_scaling_group_to_jenkins(self):
        click.secho(f"{self.region.region_name}: add_scaling_group_to_jenkins")
        res = requests.post(url=f"{self.jenkins_info['url']}/scriptText",
                            auth=(self.jenkins_info['username'], self.jenkins_info['password']),
                            params=dict(script=ASG_CONFIG_FORMAT.format(name=self.name,
                                                                        num_executors=self.NUM_EXECUTORS,
                                                                        region_name=self.region.region_name,
                                                                        min_size=0,
                                                                        max_size=100,
                                                                        asg_id=self.name,
                                                                        jenkins_labels=self.jenkins_labels)))
        res.raise_for_status()
        LOGGER.debug(res.text)
        assert not res.text

    def configure_auto_scaling_group(self):
        self.create_launch_template()
        self.create_auto_scaling_group()
        self.add_scaling_group_to_jenkins()

    @classmethod
    def configure_in_all_region(cls, regions=None):
        regions = regions or ['eu-west-1', 'eu-west-2', 'eu-north-1', 'eu-central-1', 'us-east-1', 'us-west-2']
        for region_name in regions:
            region = cls(AwsRegion(region_name))
            region.configure_auto_scaling_group()


class AwsCiBuilder(AwsBuilder):
    NUM_CPUS = 2
    NUM_EXECUTORS = 1

    @cached_property
    def name(self):
        # example: aws-eu-central-1-qa-builder-v2-1
        return f"aws-{self.region.region_name}-qa-builder-{self.VERSION}-{self.number}-CI"

    @cached_property
    def jenkins_labels(self):
        return f"aws-sct-builders-{self.region.region_name}-{self.VERSION}-CI"


class AwsFipsCiBuilder(AwsBuilder):
    NUM_CPUS = 2
    NUM_EXECUTORS = 1
    VERSION = 'v4-fibs'

    @cached_property
    def name(self):
        # example: aws-eu-central-1-qa-builder-v2-1
        return f"aws-{self.region.region_name}-qa-builder-{self.VERSION}-{self.number}-CI-FIPS"

    @cached_property
    def jenkins_labels(self):
        return f"aws-sct-builders-{self.region.region_name}-{self.VERSION}-CI-FIPS"

    @property
    def sct_runner(self):
        return AwsFipsSctRunner(region_name=self.region.region_name, availability_zone='a', params=None)
