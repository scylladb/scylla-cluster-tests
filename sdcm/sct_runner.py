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
# Copyright (c) 2021 ScyllaDB


from __future__ import annotations

import logging
import string
import random
import tempfile
import datetime
import glob
from contextlib import suppress
from enum import Enum
from functools import cached_property
from itertools import chain
from math import ceil
from typing import TYPE_CHECKING
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from textwrap import dedent

import boto3
import pytz
from azure.core.exceptions import ResourceNotFoundError as AzureResourceNotFoundError
from azure.mgmt.compute.models import GalleryImageVersion
from azure.mgmt.compute.models import VirtualMachine
from azure.mgmt.resource.resources.models import TagsPatchResource, TagsPatchOperation
import google.api_core.exceptions
from google.cloud import compute_v1
from mypy_boto3_ec2 import EC2Client
from mypy_boto3_ec2.service_resource import Instance

from sct_ssh import ssh_run_cmd
from sdcm.keystore import KeyStore
from sdcm.provision.provisioner import InstanceDefinition, PricingModel, VmInstance, provisioner_factory
from sdcm.remote import RemoteCmdRunnerBase, shell_script_cmd
from sdcm.utils.common import (
    aws_tags_to_dict,
    gce_meta_to_dict,
    list_instances_aws,
    list_instances_gce,
    str_to_bool, convert_name_to_ami_if_needed,
)
from sdcm.utils.aws_utils import ec2_instance_wait_public_ip, ec2_ami_get_root_device_name, tags_as_ec2_tags, EC2NetworkConfiguration
from sdcm.utils.aws_region import AwsRegion
from sdcm.utils.gce_utils import (
    SUPPORTED_PROJECTS,
    get_gce_compute_addresses_client,
    get_gce_compute_images_client,
    get_gce_compute_instances_client,
    create_instance,
    disk_from_image,
    wait_for_extended_operation,
    random_zone,
    gce_set_labels,
    gce_public_addresses,
)
from sdcm.utils.azure_utils import AzureService, list_instances_azure
from sdcm.utils.azure_region import AzureOsState, AzureRegion, region_name_to_location
from sdcm.utils.context_managers import environment
from sdcm.test_config import TestConfig
from sdcm.node_exporter_setup import NodeExporterSetup
from sdcm.cluster_docker import AIO_MAX_NR_RECOMMENDED_VALUE

if TYPE_CHECKING:

    from typing import Optional, Any, Type

    from mypy_boto3_ec2.literals import InstanceTypeType

    from sdcm.keystore import SSHKey
    from sdcm.sct_config import SCTConfiguration


LAUNCH_TIME_FORMAT = "%B %d, %Y, %H:%M:%S"

LOGGER = logging.getLogger(__name__)


class ImageType(Enum):
    SOURCE = "source"
    GENERAL = "general"


def get_current_datetime_formatted() -> str:
    return datetime.datetime.now(tz=pytz.utc).strftime(LAUNCH_TIME_FORMAT)


def datetime_from_formatted(date_string: str) -> datetime.datetime:
    return datetime.datetime.strptime(date_string, LAUNCH_TIME_FORMAT).replace(tzinfo=pytz.utc)


@dataclass
class SctRunnerInfo:
    sct_runner_class: Type[SctRunner] = field(repr=False)
    cloud_service_instance: EC2Client | AzureService | None = field(repr=False)
    region_az: str
    instance: VirtualMachine | compute_v1.Instance | Any = field(repr=False)
    instance_name: str
    public_ips: list[str]
    test_id: str | None = None
    launch_time: Optional[datetime.datetime] = None
    keep: Optional[str] = None
    keep_action: Optional[str] = None
    logs_collected: bool = False

    @property
    def cloud_provider(self) -> str:
        return self.sct_runner_class.CLOUD_PROVIDER

    def __str__(self) -> str:
        bits = [
            f"[{self.cloud_provider}/{self.region_az}] {self.instance_name}",
        ]
        if self.launch_time:
            bits.append(f"launched at {self.launch_time:{LAUNCH_TIME_FORMAT}} UTC")
        if self.keep:
            bits.append(f"keep: {self.keep}")
        if self.keep_action:
            bits.append(f"keep_action: {self.keep_action}")
        if self.public_ips:
            bits.append(f"public IPs are {self.public_ips}")
        return ", ".join(bits)

    def terminate(self) -> None:
        LOGGER.info("Terminate %s", self)
        self.sct_runner_class.terminate_sct_runner_instance(sct_runner_info=self)


class SctRunner(ABC):
    """Provision and configure the SCT runner."""
    VERSION = "1.15"  # Version of the Image
    NODE_TYPE = "sct-runner"
    RUNNER_NAME = "SCT-Runner"
    LOGIN_USER = "ubuntu"
    IMAGE_DESCRIPTION = f"SCT runner image {VERSION}"

    CLOUD_PROVIDER: str
    BASE_IMAGE: Any
    SOURCE_IMAGE_REGION: str
    IMAGE_BUILDER_INSTANCE_TYPE: str
    REGULAR_TEST_INSTANCE_TYPE: str
    LONGTERM_TEST_INSTANCE_TYPE: str

    def __init__(self, region_name: str, availability_zone: str = "", params: SCTConfiguration | None = None):
        self.region_name = region_name
        self.availability_zone = availability_zone
        self._instance = None
        self._ssh_pkey_file = None
        self.params = params

    @abstractmethod
    def region_az(self, region_name: str, availability_zone: str) -> str:
        ...

    @cached_property
    @abstractmethod
    def image_name(self) -> str:
        ...

    def instance_type(self, test_duration) -> str:
        if test_duration > 7 * 60:
            return self.LONGTERM_TEST_INSTANCE_TYPE
        return self.REGULAR_TEST_INSTANCE_TYPE

    @staticmethod
    def instance_root_disk_size(test_duration) -> int:
        disk_size = 80  # GB
        if test_duration and test_duration > 1.5 * 24 * 60:  # 1.5 days
            # add 40G more space for jobs which test_duration is longer than 1.5 days
            return disk_size + 40
        return disk_size

    @cached_property
    @abstractmethod
    def key_pair(self) -> SSHKey:
        ...

    def get_remoter(self, host, connect_timeout: Optional[float] = None) -> RemoteCmdRunnerBase:
        self._ssh_pkey_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        self._ssh_pkey_file.write(self.key_pair.private_key.decode())
        self._ssh_pkey_file.flush()
        return RemoteCmdRunnerBase.create_remoter(hostname=host, user=self.LOGIN_USER,
                                                  key_file=self._ssh_pkey_file.name, connect_timeout=connect_timeout)

    def install_prereqs(self, public_ip: str, connect_timeout: Optional[int] = None) -> None:
        LOGGER.info("Connecting instance...")
        remoter = self.get_remoter(host=public_ip, connect_timeout=connect_timeout)

        LOGGER.info("Installing required packages...")
        login_user = self.LOGIN_USER
        public_key = self.key_pair.public_key.decode()
        result = remoter.sudo(shell_script_cmd(quote="'", cmd=dedent(f"""\
            # Make sure that cloud-init finished running.
            until [ -f /var/lib/cloud/instance/boot-finished ]; do sleep 1; done

            echo "fs.aio-max-nr = {AIO_MAX_NR_RECOMMENDED_VALUE}" >> /etc/sysctl.conf
            echo "{login_user} soft nofile 4096" >> /etc/security/limits.conf
            echo "jenkins soft nofile 4096" >> /etc/security/limits.conf
            echo "root soft nofile 4096" >> /etc/security/limits.conf

            # Configure default user account.
            sudo -u {login_user} mkdir -p /home/{login_user}/.ssh || true
            echo "{public_key}" >> /home/{login_user}/.ssh/authorized_keys
            chmod 600 /home/{login_user}/.ssh/authorized_keys
            mkdir -p -m 777 /home/{login_user}/sct-results
            echo "cd ~/sct-results" >> /home/{login_user}/.bashrc
            chown -R {login_user}:{login_user} /home/{login_user}/

             # Disable apt-key warnings and set non-interactive frontend.
            export APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1
            export DEBIAN_FRONTEND=noninteractive
            export PIP_BREAK_SYSTEM_PACKAGES=true

            apt-get -qq clean
            apt-get -qq update
            apt-get -qq install --no-install-recommends python3-pip htop screen tree systemd-coredump rng-tools
            pip3 install awscli

            # disable unattended-upgrades
            sudo systemctl disable --now unattended-upgrades.service apt-daily.timer apt-daily-upgrade.timer apt-daily.service apt-daily-upgrade.service

            # Install Docker.
            apt-get -qq install --no-install-recommends \
                apt-transport-https ca-certificates curl gnupg-agent software-properties-common
            curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
            add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
            apt-get -qq install --no-install-recommends docker-ce docker-ce-cli containerd.io
            usermod -aG docker {login_user}

            # Configure Docker to use Google Container Registry mirrors.
            mkdir -p /etc/docker
            printf "%s\n" "{{" "  \\"registry-mirrors\\": [" "    \\"https://mirror.gcr.io\\"" "  ]" "}}" > /etc/docker/daemon.json

            # Install kubectl.
            curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
            install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

            # Configure Jenkins user.
            apt-get -qq install --no-install-recommends openjdk-21-jre-headless  # https://www.jenkins.io/doc/administration/requirements/java/
            adduser --disabled-password --gecos "" jenkins || true
            usermod -aG docker jenkins
            mkdir -p /home/jenkins/.ssh
            echo "{public_key}" >> /home/jenkins/.ssh/authorized_keys
            chmod 600 /home/jenkins/.ssh/authorized_keys
            chown -R jenkins:jenkins /home/jenkins
            echo "jenkins ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/jenkins

            # Jenkins pipelines run /bin/sh for some reason.
            ln -sf /bin/bash /bin/sh
        """)), ignore_status=True)

        node_exporter_setup = NodeExporterSetup()
        node_exporter_setup.install(remoter=remoter)

        remoter.stop()
        if result.ok:
            LOGGER.info("All packages successfully installed.")
        else:
            raise Exception(f"Unable to install required packages:\n{result.stdout}{result.stderr}")

    @abstractmethod
    def _image(self, image_type: ImageType = ImageType.SOURCE) -> Any:
        ...

    @property
    def source_image(self) -> Any:
        return self._image(image_type=ImageType.SOURCE)

    @property
    def image(self) -> Any:
        return self._image(image_type=ImageType.GENERAL)

    @cached_property
    def image_tags(self) -> dict[str, str]:
        return {
            "Name": self.image_name,
            "Version": self.VERSION,
        }

    @property
    @abstractmethod
    def instance(self):
        ...

    @instance.setter
    @abstractmethod
    def instance(self, new_instance_value):
        ...

    @abstractmethod
    def _create_instance(self,
                         instance_type: str,
                         base_image: Any,
                         tags: dict[str, str],
                         instance_name: str,
                         root_disk_size_gb: int = 0,
                         region_az: str = "",
                         test_duration: int | None = None,
                         address_pool: str | None = None) -> Any:
        ...

    @staticmethod
    @abstractmethod
    def set_tags(sct_runner_info: SctRunnerInfo,
                 tags: dict):
        ...

    @abstractmethod
    def _stop_image_builder_instance(self, instance: Any) -> None:
        ...

    @abstractmethod
    def _terminate_image_builder_instance(self, instance: Any) -> None:
        ...

    @abstractmethod
    def _get_instance_id(self) -> Any:
        ...

    @abstractmethod
    def get_instance_public_ip(self, instance: Any) -> str:
        ...

    @abstractmethod
    def _create_image(self, instance: Any) -> Any:
        ...

    @abstractmethod
    def _get_image_id(self, image: Any) -> Any:
        ...

    @abstractmethod
    def _copy_source_image_to_region(self) -> None:
        ...

    def create_image(self) -> None:
        """Create SCT Runner image in specified region.

        If the image exists in SOURCE_REGION it will be copied to the destination region.

        WARNING: this can't run in parallel!
        """
        LOGGER.info("Looking for source SCT Runner Image in `%s'...", self.SOURCE_IMAGE_REGION)
        source_image = self.source_image
        if not source_image:
            LOGGER.info("Source SCT Runner Image not found. Creating...")
            instance = self._create_instance(
                instance_type=self.IMAGE_BUILDER_INSTANCE_TYPE,
                base_image=self.BASE_IMAGE,
                tags={
                    "RunByUser": "qa",
                    "keep": "1",
                    "keep_action": "terminate",
                    "Version": self.VERSION,
                },
                instance_name=f"{self.image_name}-builder-{self.SOURCE_IMAGE_REGION}",
                region_az=self.region_az(
                    region_name=self.SOURCE_IMAGE_REGION,
                    availability_zone=self.availability_zone,
                ),
            )
            self.install_prereqs(public_ip=self.get_instance_public_ip(instance=instance), connect_timeout=120)

            LOGGER.info("Stopping the SCT Image Builder instance...")
            self._stop_image_builder_instance(instance=instance)

            LOGGER.info("SCT Image Builder instance stopped.\nCreating image...")
            self._create_image(instance=instance)

            builder_instance_id = self._get_instance_id()
            try:
                LOGGER.info("Terminating SCT Image Builder instance `%s'...", builder_instance_id)
                self._terminate_image_builder_instance(instance=instance)
            except Exception as ex:  # noqa: BLE001
                LOGGER.warning("Was not able to terminate `%s': %s\nPlease terminate manually!!!",
                               builder_instance_id, ex)
        else:
            LOGGER.info("SCT Runner image exists in the source region `%s'! ID: %s",
                        self.SOURCE_IMAGE_REGION, self._get_image_id(image=source_image))

        if self.region_name != self.SOURCE_IMAGE_REGION and self.image is None:
            LOGGER.info("Copying %s to %s...\nNOTE: it can take 5-15 minutes.",
                        self.image_name, self.region_name)
            self._copy_source_image_to_region()
            LOGGER.info("Finished copying %s to %s", self.image_name, self.region_name)
        else:
            LOGGER.info("No need to copy SCT Runner image since it already exists in `%s'", self.region_name)

    @abstractmethod
    def _get_base_image(self, image: Optional[Any] = None) -> Any:
        ...

    def create_instance(self,
                        test_id: str,
                        test_name: str,
                        test_duration: int,
                        instance_type: str = "",
                        root_disk_size_gb: int = 0,
                        restore_monitor: bool = False,
                        restored_test_id: str = "",
                        address_pool: str | None = None) -> Any:
        LOGGER.info("Creating SCT Runner instance...")
        image = self.image
        if not image:
            LOGGER.error("SCT Runner image was not found in %s! "
                         "Use `hydra create-runner-image --cloud-provider %s --region %s'",
                         self.region_name, self.CLOUD_PROVIDER, self.region_name)
            return None

        tags = TestConfig.common_tags()
        tags.update({
            "TestId": test_id,
            "TestName": test_name,
            "NodeType": self.NODE_TYPE,
            "keep": str(ceil(test_duration / 60) + 6),  # keep SCT Runner for 6h more than test_duration
            "keep_action": "terminate",
            "UserName": self.LOGIN_USER,
            "bastion": "true",
        })
        if restore_monitor and restored_test_id:
            tags.update({"RestoredTestId": restored_test_id})

        return self._create_instance(
            instance_type=instance_type or self.instance_type(test_duration=test_duration),
            base_image=self._get_base_image(self.image),
            root_disk_size_gb=root_disk_size_gb,
            tags=tags,
            instance_name=f"{self.image_name}-{'restored-monitor-' if restore_monitor else ''}instance-{test_id[:8]}",
            region_az=self.region_az(
                region_name=self.region_name,
                availability_zone=self.availability_zone,
            ),
            test_duration=test_duration,
            address_pool=address_pool,
        )

    @classmethod
    @abstractmethod
    def list_sct_runners(cls, verbose: bool = True) -> list[SctRunnerInfo]:
        ...

    @staticmethod
    @abstractmethod
    def terminate_sct_runner_instance(sct_runner_info: SctRunnerInfo) -> None:
        ...


class AwsSctRunner(SctRunner):
    """Provision and configure the SCT Runner on AWS."""
    CLOUD_PROVIDER = "aws"
    BASE_IMAGE = "ami-02f921fd5f09a1812"  # Canonical, Ubuntu, 24.04 LTS, amd64 numbat image build on 2024-08-06
    SOURCE_IMAGE_REGION = "eu-west-2"  # where the source Runner image will be created and copied to other regions
    IMAGE_BUILDER_INSTANCE_TYPE = "t3.small"
    REGULAR_TEST_INSTANCE_TYPE = "m7i-flex.large"  # 2 vcpus, 8G
    LONGTERM_TEST_INSTANCE_TYPE = "m7i-flex.xlarge"  # 4 vcpus, 16G

    def __init__(self, region_name: str, availability_zone: str, params: SCTConfiguration | None = None):
        super().__init__(region_name=region_name, availability_zone=availability_zone, params=params)
        if region_name.endswith(tuple(string.ascii_lowercase)):
            region_name = region_name[:-1]
        self.aws_region = AwsRegion(region_name=region_name)
        self.aws_region_source = AwsRegion(region_name=self.SOURCE_IMAGE_REGION)

    def region_az(self, region_name: str, availability_zone: str) -> str:
        return f"{region_name}{availability_zone}"

    @cached_property
    def image_name(self) -> str:
        return f"sct-runner-{self.VERSION}"

    @property
    def instance(self) -> Instance:
        return self._instance

    @instance.setter
    def instance(self, new_instance_value: Instance):
        self._instance = new_instance_value

    @cached_property
    def key_pair(self) -> SSHKey:
        return KeyStore().get_ec2_ssh_key_pair()

    def _image(self, image_type: ImageType = ImageType.SOURCE) -> Any:
        if image_type == ImageType.SOURCE:
            aws_region = self.aws_region_source
        elif image_type == ImageType.GENERAL:
            aws_region = self.aws_region
        else:
            raise ValueError(f"Unknown Image type: {image_type}")

        amis = aws_region.client.describe_images(
            Owners=["self"],
            Filters=[
                {"Name": "tag:Name", "Values": [self.image_name]},
                {"Name": "tag:Version", "Values": [self.VERSION]},
            ],
        )
        LOGGER.debug("Found SCT Runner AMIs: %s", amis)

        existing_amis = amis.get("Images")

        if not existing_amis:
            return None

        assert len(existing_amis) == 1, \
            f"More than 1 SCT Runner AMI with {self.image_name}:{self.VERSION} " \
            f"found in {self.region_name}: {existing_amis}"

        return aws_region.resource.Image(existing_amis[0]["ImageId"])

    def _create_instance(self,
                         instance_type: InstanceTypeType,
                         base_image: Any,
                         tags: dict[str, str],
                         instance_name: str,
                         root_disk_size_gb: int = 0,
                         region_az: str = "",
                         test_duration: int | None = None,
                         address_pool: str | None = None) -> Any:
        if region_az.startswith(self.SOURCE_IMAGE_REGION):
            aws_region: AwsRegion = self.aws_region_source
        else:
            aws_region: AwsRegion = self.aws_region
        ec2_network_configuration_subnets = EC2NetworkConfiguration(
            regions=[aws_region.region_name], availability_zones=[self.availability_zone], params=self.params).subnets_per_region
        assert ec2_network_configuration_subnets, f"No SCT subnet found in the source region. " \
            f"Use `hydra prepare-regions --cloud-provider aws --region-name {aws_region.region_name}' " \
            f"to create cloud env!"

        subnets = ec2_network_configuration_subnets[aws_region.region_name][self.availability_zone]

        interfaces = []
        for i, subnet in enumerate(subnets):
            interfaces.append({
                "DeviceIndex": i,
                "SubnetId": subnet,
                "Groups": [aws_region.sct_security_group.group_id,
                           aws_region.sct_ssh_security_group.group_id],
                "DeleteOnTermination": True,
            })
            if len(subnets) == 1:
                interfaces[-1]["AssociatePublicIpAddress"] = not address_pool

        LOGGER.info("Creating instance...")
        base_image = convert_name_to_ami_if_needed(
            ami_id_param=base_image, region_names=tuple([aws_region.region_name]))

        result = aws_region.resource.create_instances(
            ImageId=base_image,
            InstanceType=instance_type,
            MinCount=1,
            MaxCount=1,
            KeyName=aws_region.SCT_KEY_PAIR_NAME,
            NetworkInterfaces=interfaces,
            TagSpecifications=[{
                "ResourceType": "instance",
                "Tags": [{"Key": key, "Value": value} for key, value in tags.items()] +
                        [{"Key": "Name", "Value": instance_name}],
            }],
            BlockDeviceMappings=[{
                "DeviceName": ec2_ami_get_root_device_name(image_id=base_image, region_name=aws_region.region_name),
                "Ebs": {
                    "VolumeSize": root_disk_size_gb or self.instance_root_disk_size(test_duration),
                    "VolumeType": "gp3"
                }
            }]
        )
        instance = result[0]

        LOGGER.info("Instance created. Waiting until it becomes running... ")
        instance.wait_until_running()

        if address_pool:
            LOGGER.info("Associating an EIP from `%s' pool...", address_pool)
            unassigned_addresses = [
                addr for addr in aws_region.resource.vpc_addresses.filter(
                    Filters=[{"Name": "tag:sct-runner-pool", "Values": [address_pool]}]
                ) if not addr.instance_id
            ]
            if not unassigned_addresses:
                raise Exception(f"There are no unassigned EIPs in `{address_pool}' pool")
            aws_region.client.associate_address(
                AllocationId=random.choice(unassigned_addresses).allocation_id,
                InstanceId=instance.instance_id,
                AllowReassociation=False,
            )
            instance = aws_region.resource.Instance(id=instance.instance_id)

        if len(instance.network_interfaces) > 1:
            for interface in instance.network_interfaces:
                if interface.attachment['DeviceIndex'] == 0 and interface.association_attribute is None:
                    response = aws_region.client.allocate_address(Domain='vpc')
                    eip_allocation_id = response['AllocationId']
                    aws_region.client.associate_address(
                        AllocationId=eip_allocation_id,
                        NetworkInterfaceId=interface.id,
                    )
                    aws_region.resource.create_tags(
                        Resources=[eip_allocation_id], Tags=tags_as_ec2_tags(TestConfig().common_tags()))
                    break

        LOGGER.info("Instance `%s' is running. Waiting for public IP...", instance.instance_id)
        ec2_instance_wait_public_ip(instance=instance)

        LOGGER.info("Got public IP: %s", instance.public_ip_address)
        self.instance = instance

        # If the SCT runner instance is used as a Docker backend for tests
        if self.params.get("cluster_backend") == "docker":
            LOGGER.info("Configure unused disks to use as a persistent storage for Docker.")
            LOGGER.debug("Connecting to instance...")
            remoter = self.get_remoter(host=instance.public_ip_address, connect_timeout=120)

            LOGGER.debug("Getting unused disks...")
            disks = self._get_unused_disks(remoter)
            LOGGER.debug("The following unused disks are detected: %s", disks)
            if disks:
                mount_point = "/mnt/docker-persistent-storage"
                self._setup_disks(remoter, disks, mount_point)
                LOGGER.debug("Disks are configured.")
                self._set_docker_data_root(remoter, mount_point)
                LOGGER.debug("Persistent storage for Docker is configured.")
            else:
                LOGGER.debug("No unused disks found.")

            remoter.stop()

        return instance

    @staticmethod
    def _get_unused_disks(remoter: Any) -> list[str]:
        result = remoter.run("lsblk -dnpo NAME,TYPE | grep disk | cut -d' ' -f1")
        devices = result.stdout.strip().splitlines()

        unused = []
        for device in devices:
            dev = device.replace("/dev/", '')
            if len(glob.glob("/sys/class/block/{dev}/{dev}*".format(dev=dev))) == 0:
                unused.append(device)
        return unused

    @staticmethod
    def _setup_disks(remoter: Any, disks: list, mount_point: str) -> None:
        raid_device = "/dev/md0"
        raid_level = 0
        # Create RAID if more than one unused disk is available
        if len(disks) != 1:
            remoter.sudo(f"mdadm --create --verbose {raid_device} --level={raid_level} "
                         f"--raid-devices={len(disks)} " + " ".join(disks))
            remoter.sudo(f"mkfs.xfs {raid_device}")
        else:
            remoter.sudo(f"mkfs.xfs {disks[0]}")

        mount_dev = disks[0] if len(disks) == 1 else raid_device
        remoter.sudo(f"mkdir -p {mount_point}")
        remoter.sudo(f"mount {mount_dev} {mount_point}")

        # Ensure that the configured device mounts on boot
        remoter.sudo(f"sh -c \"echo '{mount_dev} {mount_point} xfs defaults 0 0' >> /etc/fstab\"")

    @staticmethod
    def _set_docker_data_root(remoter: Any, docker_data_dir: str) -> None:
        remoter.sudo("systemctl stop docker")
        remoter.sudo(f"rsync -a /var/lib/docker/ {docker_data_dir}")
        remoter.sudo("rm -rf /var/lib/docker")
        remoter.sudo(f"ln -s {docker_data_dir} /var/lib/docker")
        remoter.sudo("systemctl start docker")

    def _stop_image_builder_instance(self, instance: Any) -> None:
        instance.stop()
        instance.wait_until_stopped()

    def _terminate_image_builder_instance(self, instance: Any) -> None:
        instance.terminate()

    def _get_instance_id(self) -> Any:
        return self.instance.instance_id

    def get_instance_public_ip(self, instance: Any) -> str:
        return instance.public_ip_address

    def tag_image(self, image_id, image_type) -> None:
        if image_type == ImageType.SOURCE:
            aws_region = self.aws_region_source
        elif image_type == ImageType.GENERAL:
            aws_region = self.aws_region
        else:
            raise ValueError(f"Unknown Image type: {image_type}")

        runner_image = aws_region.resource.Image(image_id)
        runner_image.wait_until_exists()

        LOGGER.info("Image '%s' exists and ready. Tagging...", image_id)
        runner_image.create_tags(Tags=[{"Key": key, "Value": value} for key, value in self.image_tags.items()])
        LOGGER.info("Tagging completed.")

    def _create_image(self, instance: Any) -> Any:
        result = self.aws_region_source.client.create_image(
            Description=self.IMAGE_DESCRIPTION,
            InstanceId=instance.instance_id,
            Name=self.image_name,
            NoReboot=False
        )
        self.tag_image(image_id=result["ImageId"], image_type=ImageType.SOURCE)

    def _get_image_id(self, image: Any) -> Any:
        return image.image_id

    def _copy_source_image_to_region(self) -> None:
        result = self.aws_region.client.copy_image(
            Description=self.IMAGE_DESCRIPTION,
            Name=self.image_name,
            SourceImageId=self.source_image.image_id,
            SourceRegion=self.SOURCE_IMAGE_REGION
        )
        LOGGER.info("Image copied, id: `%s'.", result["ImageId"])
        self.tag_image(image_id=result["ImageId"], image_type=ImageType.GENERAL)

    def _get_base_image(self, image=None):
        if image is None:
            image = self.image
        return image.id

    @classmethod
    def list_sct_runners(cls, verbose: bool = True) -> list[SctRunnerInfo]:
        all_instances = list_instances_aws(tags_dict={"NodeType": cls.NODE_TYPE}, group_as_region=True, verbose=verbose)
        sct_runners = []
        for region_name, instances in all_instances.items():
            client = boto3.client("ec2", region_name=region_name)
            for instance in instances:
                tags = aws_tags_to_dict(instance.get("Tags"))
                instance_name = instance["InstanceId"]
                if "Name" in tags:
                    instance_name = f"{tags['Name']} ({instance_name})"
                sct_runners.append(SctRunnerInfo(
                    sct_runner_class=cls,
                    cloud_service_instance=client,
                    region_az=instance["Placement"]["AvailabilityZone"],
                    instance=instance,
                    test_id=tags.get("TestId"),
                    instance_name=instance_name,
                    public_ips=[public_ip, ] if (public_ip := instance.get("PublicIpAddress")) else [],
                    launch_time=instance["LaunchTime"],
                    keep=tags.get("keep"),
                    keep_action=tags.get("keep_action"),
                    logs_collected=str_to_bool(tags.get("logs_collected")),
                ))
        return sct_runners

    @staticmethod
    def terminate_sct_runner_instance(sct_runner_info: SctRunnerInfo) -> None:
        sct_runner_info.cloud_service_instance.terminate_instances(
            InstanceIds=[sct_runner_info.instance["InstanceId"], ],
        )

    @staticmethod
    def set_tags(sct_runner_info: SctRunnerInfo,
                 tags: dict) -> None:
        tags_to_create = []
        for key, value in tags.items():
            tags_to_create.append({"Key": str(key), "Value": str(value)})

        cloud_instance: EC2Client = sct_runner_info.cloud_service_instance
        # extract instance id from SctRunner.instance_name, e.g. sct-runner-1.5-instance-81d39f6f (i-02e72ea7f5aac9d65)
        cloud_instance_name = sct_runner_info.instance_name.split("(")[1].replace(")", "")
        LOGGER.info("Updating SCT Runner %s with tags: %s...", cloud_instance_name, tags_to_create)
        cloud_instance.create_tags(Resources=[cloud_instance_name], Tags=tags_to_create)
        LOGGER.info("Updated SCT Runner %s with tags: %s successfully.", cloud_instance_name, tags_to_create)


class GceSctRunner(SctRunner):
    """Provision and configure the SCT runner on GCE."""

    CLOUD_PROVIDER = "gce"
    BASE_IMAGE = "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/family/ubuntu-2404-lts-amd64"
    SOURCE_IMAGE_REGION = "us-east1"  # where the source Runner image will be created and copied to other regions
    IMAGE_BUILDER_INSTANCE_TYPE = "e2-standard-2"
    REGULAR_TEST_INSTANCE_TYPE = "e2-standard-2"  # 2 vcpus, 8G
    LONGTERM_TEST_INSTANCE_TYPE = "e2-standard-4"  # 2 vcpus, 16G,

    FAMILY = "sct-runner-image"
    SCT_NETWORK = "qa-vpc"

    def __init__(self, region_name: str, availability_zone: str,  params: SCTConfiguration | None = None):
        availability_zone = params.get("availability_zone") or random_zone(region_name)
        super().__init__(region_name=region_name, availability_zone=availability_zone, params=params)
        self.gce_region = region_name
        self.gce_source_region = self.SOURCE_IMAGE_REGION
        self.images_client, info = get_gce_compute_images_client()
        self.project_name = info['project_id']
        self.instances_client, _ = get_gce_compute_instances_client()
        self._instance_name = None

    def region_az(self, region_name: str, availability_zone: str) -> str:
        if availability_zone:
            return f"{region_name}-{availability_zone}"
        return region_name

    @cached_property
    def zone(self) -> str:
        return self.region_az(region_name=self.region_name, availability_zone=self.availability_zone)

    @property
    def instance_name(self) -> str:
        return self._instance_name

    @instance_name.setter
    def instance_name(self, new_name: str) -> None:
        self._instance_name = new_name

    @property
    def instance(self) -> compute_v1.Instance:
        return self.instances_client.get(project=self.project_name,
                                         zone=self.zone,
                                         instance=self.instance_name)

    @cached_property
    def image_name(self) -> str:
        return f"sct-runner-{self.VERSION.replace('.', '-')}"

    @cached_property
    def key_pair(self) -> SSHKey:
        return KeyStore().get_gce_ssh_key_pair()  # scylla-test

    def _image(self, image_type: ImageType = ImageType.SOURCE) -> Any:
        try:
            return self.images_client.get(image=self.image_name, project=self.project_name)
        except google.api_core.exceptions.NotFound:
            return None

    @staticmethod
    def set_tags(sct_runner_info: SctRunnerInfo, tags: dict):
        tags_to_create = {str(k): str(v).lower() for k, v in tags.items()}
        LOGGER.info("Setting SCT runner labels to: %s", tags_to_create)
        instances_client, info = get_gce_compute_instances_client()
        gce_set_labels(instances_client=instances_client,
                       instance=sct_runner_info.instance,
                       new_labels=tags_to_create,
                       project=info['project_id'],
                       zone=sct_runner_info.region_az)
        LOGGER.info("SCT runner tags set to: %s", tags_to_create)

    @staticmethod
    def tags_to_labels(tags: dict[str, str]) -> dict[str, str]:
        return {key.lower(): value.lower().replace(".", "_") for key, value in tags.items()}

    def _create_instance(self,
                         instance_type: str,
                         base_image: Any,
                         tags: dict[str, str],
                         instance_name: str,
                         root_disk_size_gb: int = 0,
                         region_az: str = "",
                         test_duration: int | None = None,
                         address_pool: str | None = None) -> Any:
        LOGGER.info("Creating instance...")
        disks = [disk_from_image(disk_type=f"projects/{self.project_name}/zones/{region_az}/diskTypes/pd-ssd",
                                 disk_size_gb=root_disk_size_gb or self.instance_root_disk_size(test_duration),
                                 boot=True,
                                 source_image=base_image,
                                 auto_delete=True)]

        if address_pool:
            LOGGER.info("Use External IP address from `%s' pool...", address_pool)
            addresses_client, info = get_gce_compute_addresses_client()
            unassigned_addresses = list(
                addresses_client.list(request=compute_v1.ListAddressesRequest(
                    project=info["project_id"],
                    region=region_az.rsplit("-", maxsplit=1)[0],
                    filter=f"labels.sct-runner-pool={address_pool} AND status=RESERVED",
                ))
            )
            if not unassigned_addresses:
                raise Exception(f"There are no unassigned External IP addresses in `{address_pool}' pool")
            external_ipv4 = random.choice(unassigned_addresses).address
        else:
            external_ipv4 = None

        instance = create_instance(project_id=self.project_name, zone=region_az,
                                   machine_type=instance_type,
                                   instance_name=instance_name,
                                   network_name=self.SCT_NETWORK,
                                   disks=disks,
                                   external_access=True,
                                   external_ipv4=external_ipv4,
                                   metadata=tags | {
                                       "launch_time": get_current_datetime_formatted(),
                                       "block-project-ssh-keys": "true",
                                       "ssh-keys": f"{self.LOGIN_USER}:{self.key_pair.public_key.decode()}",
                                   },
                                   service_accounts=[{
                                       'email': KeyStore().get_gcp_credentials()['client_email'],
                                       'scopes': ['https://www.googleapis.com/auth/cloud-platform'],
                                   }],)
        LOGGER.info("Got public IP: %s", self.get_instance_public_ip(instance))
        self.instance_name = instance_name

        return instance

    def _stop_image_builder_instance(self, instance: Any) -> None:
        self.instances_client.stop(instance=instance.name, project=self.project_name, zone=self.zone)

    def _terminate_image_builder_instance(self, instance: Any) -> None:
        self.instances_client.delete(instance=instance.name, project=self.project_name, zone=self.zone)

    def _get_instance_id(self) -> Any:
        return self.instance.name

    def get_instance_public_ip(self, instance: Any) -> str:
        return gce_public_addresses(instance)[0]

    def _create_image(self, instance: Any) -> Any:
        images_client, _ = get_gce_compute_images_client()

        image = compute_v1.Image()
        image.name = self.image_name
        image.source_disk = instance.disks[0].source
        image.description = self.IMAGE_DESCRIPTION
        image.family = self.FAMILY
        image.labels = self.tags_to_labels(tags=self.image_tags)

        operation = images_client.insert(project=self.project_name, image_resource=image)
        wait_for_extended_operation(operation, "Wait for image creation")

        return images_client.get(project=self.project_name, image=self.image_name)

    def _get_image_id(self, image: Any) -> Any:
        return image.id

    def _copy_source_image_to_region(self) -> None:
        LOGGER.debug("gce images are global, not need to copy")

    def _get_base_image(self, image: Optional[Any] = None) -> Any:
        if image is None:
            image = self.image
        return image.self_link

    @classmethod
    def list_sct_runners(cls, verbose: bool = True) -> list[SctRunnerInfo]:
        sct_runners = []
        instances = []
        for project in SUPPORTED_PROJECTS:
            with environment(SCT_GCE_PROJECT=project):
                instances += list_instances_gce(tags_dict={"NodeType": cls.NODE_TYPE}, verbose=verbose)
        for instance in instances:
            tags = gce_meta_to_dict(instance.metadata)
            region = instance.zone.split('/')[-1]
            if launch_time := tags.get("launch_time"):
                try:
                    launch_time = datetime_from_formatted(date_string=launch_time)
                except ValueError as exc:
                    LOGGER.warning("Value of `launch_time' tag is invalid: %s", exc)
                    launch_time = None
            if not launch_time:
                create_time = instance.creation_timestamp
                LOGGER.info("`launch_time' tag is empty or invalid, fallback to creation time: %s", create_time)
                launch_time = datetime.datetime.fromisoformat(create_time)
            sct_runners.append(SctRunnerInfo(
                sct_runner_class=cls,
                cloud_service_instance=None,  # we don't need it for GCE
                region_az=region,
                instance=instance,
                instance_name=instance.name,
                public_ips=gce_public_addresses(instance),
                test_id=tags.get("TestId"),
                launch_time=launch_time,
                keep=tags.get("keep"),
                keep_action=tags.get("keep_action"),
                logs_collected=str_to_bool(tags.get("logs_collected")),
            ))
        return sct_runners

    @staticmethod
    def terminate_sct_runner_instance(sct_runner_info: SctRunnerInfo) -> None:
        instance = sct_runner_info.instance
        instances_client, info = get_gce_compute_instances_client()
        res = instances_client.delete(instance=instance.name,
                                      project=info['project_id'],
                                      zone=instance.zone.split('/')[-1])
        res.done()


class AzureSctRunner(SctRunner):
    """Provision and configure the SCT runner on Azure."""

    CLOUD_PROVIDER = "azure"
    GALLERY_IMAGE_NAME = "sct-runner"
    GALLERY_IMAGE_VERSION = f"{SctRunner.VERSION}.0"  # Azure requires to have it in `X.Y.Z' format
    BASE_IMAGE = {
        "publisher": "canonical",
        "offer": "ubuntu-24_04-lts",
        "sku": "server",
        "version": "latest",
    }
    SOURCE_IMAGE_REGION = AzureRegion.SCT_GALLERY_REGION
    IMAGE_BUILDER_INSTANCE_TYPE = "Standard_D2_v4"
    REGULAR_TEST_INSTANCE_TYPE = "Standard_D2_v4"  # 2 vcpus, 8G, recommended by Ubuntu 20.04 LTS image publisher
    LONGTERM_TEST_INSTANCE_TYPE = "Standard_E2s_v3"  # 2 vcpus, 16G, recommended by Ubuntu 20.04 LTS image publisher

    def __init__(self, region_name: str, availability_zone: str, params: SCTConfiguration):
        super().__init__(region_name=region_name, availability_zone=availability_zone, params=params)
        self.azure_region = AzureRegion(region_name=region_name)
        self.azure_region_source = AzureRegion(region_name=self.SOURCE_IMAGE_REGION)
        self.azure_service = self.azure_region.azure_service
        self._instance = None

    def region_az(self, region_name: str, availability_zone: str) -> str:
        return f"{region_name}-{availability_zone}"

    @property
    def instance(self) -> VmInstance | VirtualMachine:
        return self._instance

    @instance.setter
    def instance(self, new_instance_value: VmInstance | VirtualMachine):
        self._instance = new_instance_value

    @cached_property
    def image_name(self) -> str:
        return f"sct-runner-{self.VERSION}"

    @cached_property
    def key_pair(self) -> SSHKey:
        return KeyStore().get_azure_ssh_key_pair()  # scylla-test

    def _image(self, image_type: ImageType = ImageType.SOURCE) -> Any:
        with suppress(AzureResourceNotFoundError):
            gallery_image_version = self.azure_region.get_gallery_image_version(
                gallery_image_name=self.GALLERY_IMAGE_NAME,
                gallery_image_version_name=self.GALLERY_IMAGE_VERSION,
            )
            if image_type is ImageType.SOURCE:
                return gallery_image_version
            elif image_type is ImageType.GENERAL:
                for target_region in gallery_image_version.publishing_profile.target_regions:
                    if region_name_to_location(target_region.name) == self.azure_region.location:
                        return gallery_image_version
        return None

    def _create_instance(self,
                         instance_type: str,
                         base_image: Any,
                         tags: dict[str, str],
                         instance_name: str,
                         root_disk_size_gb: int = 0,
                         region_az: str = "",
                         test_duration: int | None = None,
                         address_pool: str | None = None) -> Any:
        if address_pool:
            raise NotImplementedError("--address-pool is not implement for Azure yet")

        if base_image is self.BASE_IMAGE:
            azure_region = self.azure_region_source
            additional_kwargs = {
                "os_state": AzureOsState.GENERALIZED,
                "computer_name": self.image_name.replace(".", "-"),
                "admin_username": self.LOGIN_USER,
                "admin_public_key": self.key_pair.public_key.decode(),
            }
            self.instance = azure_region.create_virtual_machine(
                vm_name=instance_name,
                vm_size=instance_type,
                image=base_image,
                disk_size=root_disk_size_gb or self.instance_root_disk_size(test_duration=test_duration),
                tags=tags | {"launch_time": get_current_datetime_formatted()},
                **additional_kwargs,
            )

            return self.instance
        else:
            test_id = tags["TestId"]
            provisioner = provisioner_factory.create_provisioner(backend="azure", test_id=test_id,
                                                                 region=self.azure_region.location,
                                                                 availability_zone=self.availability_zone)
            vm_params = InstanceDefinition(name=instance_name,
                                           image_id=base_image["id"],
                                           type=instance_type,
                                           user_name=self.LOGIN_USER,
                                           ssh_key=self.key_pair,
                                           tags=tags | {"launch_time": get_current_datetime_formatted()},
                                           root_disk_size=root_disk_size_gb or self.instance_root_disk_size(
                                               test_duration=test_duration),
                                           user_data=None,
                                           use_public_ip=True,
                                           )
            self.instance = provisioner.get_or_create_instance(definition=vm_params,
                                                               pricing_model=PricingModel.ON_DEMAND)
            return self.instance

    def _stop_image_builder_instance(self, instance: Any) -> None:
        self.azure_region_source.deallocate_virtual_machine(vm_name=instance.name)

    def _terminate_image_builder_instance(self, instance: Any) -> None:
        self.azure_service.delete_virtual_machine(virtual_machine=instance)

    def _get_instance_id(self) -> Any:
        return self.instance.id or self.instance.vm_id

    def get_instance_public_ip(self, instance: Any) -> str:
        if isinstance(instance, VmInstance):
            return instance.public_ip_address
        return self.azure_service.get_virtual_machine_ips(virtual_machine=instance).public_ip

    def _create_image(self, instance: Any) -> Any:
        self.azure_region.create_gallery_image(
            gallery_image_name=self.GALLERY_IMAGE_NAME,
            os_state=AzureOsState.SPECIALIZED,
        )
        self.azure_region.create_gallery_image_version(
            gallery_image_name=self.GALLERY_IMAGE_NAME,
            gallery_image_version_name=self.GALLERY_IMAGE_VERSION,
            source_id=instance.id,
            tags=self.image_tags,
        )

    def _get_image_id(self, image: Any) -> Any:
        return image.name

    def _copy_source_image_to_region(self) -> None:
        self.azure_region.append_target_region_to_image_version(
            gallery_image_name=self.GALLERY_IMAGE_NAME,
            gallery_image_version_name=self.GALLERY_IMAGE_VERSION,
            region_name=self.region_name,
        )

    def _get_base_image(self, image: Optional[Any] = None) -> Any:
        if image is None:
            image = self.image
        if isinstance(image, GalleryImageVersion):
            return {"id": image.id}
        return image

    @classmethod
    def list_sct_runners(cls, verbose: bool = True) -> list[SctRunnerInfo]:
        azure_service = AzureService()
        sct_runners = []
        for instance in list_instances_azure(tags_dict={"NodeType": cls.NODE_TYPE}, verbose=verbose):
            if launch_time := instance.tags.get("launch_time") or None:
                try:
                    launch_time = datetime_from_formatted(date_string=launch_time)
                except ValueError as exc:
                    LOGGER.warning("Value of `launch_time' tag is invalid: %s", exc)
                    launch_time = None
            region_az = instance.location + "" if not instance.zones else instance.zones[0]
            sct_runners.append(SctRunnerInfo(
                sct_runner_class=cls,
                cloud_service_instance=azure_service,
                region_az=region_az,
                instance=instance,
                instance_name=instance.name,
                public_ips=[azure_service.get_virtual_machine_ips(virtual_machine=instance).public_ip],
                launch_time=launch_time,
                test_id=instance.tags.get("TestId"),
                keep=instance.tags.get("keep"),
                keep_action=instance.tags.get("keep_action"),
                logs_collected=str_to_bool(instance.tags.get("logs_collected")),
            ))
        return sct_runners

    @staticmethod
    def terminate_sct_runner_instance(sct_runner_info: SctRunnerInfo) -> None:
        sct_runner_info.cloud_service_instance.delete_virtual_machine(virtual_machine=sct_runner_info.instance)

    @staticmethod
    def set_tags(sct_runner_info: SctRunnerInfo, tags: dict):
        resource_mgmt_client = sct_runner_info.cloud_service_instance.resource
        instance: VirtualMachine = sct_runner_info.instance

        tags_to_create = {str(k): str(v) for k, v in tags.items()}
        params = TagsPatchResource.from_dict(
            {
                "operation": TagsPatchOperation.MERGE.value,
                "properties": {
                    "tags": tags_to_create,
                }
            }
        )
        LOGGER.info("Setting SCT runner labels to: %s", tags_to_create)
        resource_mgmt_client.tags.begin_update_at_scope(scope=instance.id, parameters=params)
        LOGGER.info("SCT runner tags set to: %s", tags_to_create)


def get_sct_runner(cloud_provider: str, region_name: str, availability_zone: str = "", params: SCTConfiguration | None = None) -> SctRunner:
    if cloud_provider == "aws":
        return AwsSctRunner(region_name=region_name, availability_zone=availability_zone, params=params)
    if cloud_provider == "gce":
        return GceSctRunner(region_name=region_name, availability_zone=availability_zone, params=params)
    if cloud_provider == "azure":
        return AzureSctRunner(region_name=region_name, availability_zone=availability_zone, params=params)
    raise Exception(f'Unsupported Cloud provider: `{cloud_provider}')


def _get_runner_user_tag(sct_runner_info: SctRunnerInfo) -> str | None:
    if sct_runner_info.cloud_provider == "aws":
        tags = aws_tags_to_dict(sct_runner_info.instance.get("Tags", []))
        return tags.get("RunByUser")
    elif sct_runner_info.cloud_provider == "gce":
        tags = gce_meta_to_dict(sct_runner_info.instance.metadata)
        return tags.get("RunByUser")
    elif sct_runner_info.cloud_provider == "azure":
        if hasattr(sct_runner_info.instance, 'tags') and sct_runner_info.instance.tags:
            return sct_runner_info.instance.tags.get("RunByUser")
    return None


def list_sct_runners(backend: str = None, test_runner_ip: str = None, user: str = None, test_id: str | tuple = None, verbose: bool = True) -> list[SctRunnerInfo]:
    if verbose:
        log = LOGGER.info
    else:
        log = LOGGER.debug
    log("Looking for SCT runner instances (backend is '%s')...", backend)
    if "aws" in (backend or "") or backend in ("k8s-eks", "docker"):
        sct_runner_classes = (AwsSctRunner, )
    elif "gce" in (backend or "") or backend == "k8s-gke":
        sct_runner_classes = (GceSctRunner, )
    elif "azure" in (backend or ""):
        sct_runner_classes = (AzureSctRunner, )
    else:
        sct_runner_classes = (AwsSctRunner, GceSctRunner, AzureSctRunner, )
    sct_runners = chain.from_iterable(cls.list_sct_runners(verbose=False) for cls in sct_runner_classes)

    filtered_runners = []
    for runner in sct_runners:
        if test_runner_ip and test_runner_ip not in runner.public_ips:
            continue
        if user and _get_runner_user_tag(runner) != user:
            continue
        if test_id:
            if isinstance(test_id, (tuple, list)) and runner.test_id not in test_id:
                continue
            elif runner.test_id != test_id:
                continue

        filtered_runners.append(runner)

    if not filtered_runners:
        if test_runner_ip:
            LOGGER.warning("No SCT Runners were found (Backend: '%s', IP: '%s')", backend, test_runner_ip)
        elif user or test_id:
            filter_desc = []
            if user:
                filter_desc.append(f"User: '{user}'")
            if test_id:
                filter_desc.append(
                    f"TestIds: {list(test_id) if isinstance(test_id, (tuple, list)) else f'TestId: {test_id}'}")
            LOGGER.warning("No SCT Runners were found (Backend: '%s', Filters: %s)", backend, ", ".join(filter_desc))
        return []

    log("%d SCT runner(s) found:\n    %s", len(filtered_runners), "\n    ".join(map(str, filtered_runners)))

    return filtered_runners


def update_sct_runner_tags(backend: str = None, test_runner_ip: str = None, test_id: str = None, tags: dict = None):
    LOGGER.info("Test runner ip in update_sct_runner_tags: %s; test_id: %s", test_runner_ip, test_id)
    if not test_runner_ip and not test_id:
        raise ValueError("update_sct_runner_tags requires either the "
                         "test_runner_ip or test_id argument to find the runner")

    runner_to_update = None

    if test_runner_ip:
        runner_to_update = list_sct_runners(backend=backend, test_runner_ip=test_runner_ip)
    elif test_id:
        runner_to_update = list_sct_runners(backend=backend, test_id=test_id, verbose=False)

    if not runner_to_update:
        LOGGER.warning("Could not find SCT runner with IP: %s, test_id: %s to update tags for.",
                       test_runner_ip, test_id)
        return

    try:
        runner_to_update = runner_to_update[0]
        runner_to_update.sct_runner_class.set_tags(runner_to_update, tags=tags)
        LOGGER.info("Tags on SCT runner updated with: %s", tags)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Could not set SCT runner tags to: %s due to exc:\n%s", tags, exc)


def _manage_runner_keep_tag_value(utc_now: datetime,
                                  timeout_flag: bool,
                                  test_status: str,
                                  sct_runner_info: SctRunnerInfo,
                                  dry_run: bool = False) -> SctRunnerInfo:
    LOGGER.info("Managing runner's tags. Timeout flag: %s, logs_collected: %s, dry_run: %s",
                timeout_flag, sct_runner_info.logs_collected, dry_run)
    current_run_time_hrs = int((utc_now - sct_runner_info.launch_time).total_seconds() // 3600)
    if timeout_flag and sct_runner_info.logs_collected:
        new_keep_value = int(sct_runner_info.keep) - current_run_time_hrs + 6

        if new_keep_value > 0:
            if not dry_run:
                sct_runner_info.sct_runner_class.set_tags(sct_runner_info, {"keep": str(new_keep_value)})
            sct_runner_info.keep = new_keep_value
        return sct_runner_info

    if sct_runner_info.logs_collected:
        if not dry_run:
            sct_runner_info.sct_runner_class.set_tags(sct_runner_info, {"keep": "0", "keep-action": "terminate"})
        sct_runner_info.keep = 0
        sct_runner_info.keep_action = "terminate"
        return sct_runner_info
    else:
        new_keep_value = current_run_time_hrs + 48
        if not dry_run:
            sct_runner_info.sct_runner_class.set_tags(sct_runner_info, {"keep": str(new_keep_value)})
        sct_runner_info.keep = new_keep_value
        return sct_runner_info


def clean_sct_runners(test_status: str,
                      test_runner_ip: str = None,
                      backend: str = None,
                      user: str = None,
                      test_id: str | tuple = None,
                      dry_run: bool = False,
                      force: bool = False) -> None:

    sct_runners_list = list_sct_runners(backend=backend, test_runner_ip=test_runner_ip, user=user, test_id=test_id)
    timeout_flag = False
    runners_terminated = 0
    end_message = ""

    if not dry_run and test_runner_ip and force:
        sct_runner_info = sct_runners_list[0]
        sct_runner_info.terminate()
        LOGGER.info("Forcibly terminated runner: %s", sct_runner_info)
        return

    for sct_runner_info in sct_runners_list:
        LOGGER.info("Managing SCT runner: %s in region: %s",
                    sct_runner_info.instance_name, sct_runner_info.region_az)
        cmd = 'cat /home/ubuntu/sct-results/latest/events_log/critical.log | grep "TestTimeoutEvent"'

        if sct_runner_info.cloud_provider == "aws":
            sct_runner_name = sct_runner_info.instance_name.split(" ")[0]
        else:
            sct_runner_name = sct_runner_info.instance_name

        # ssh_run_cmd currently only works on AWS, no point of wasting time
        # trying to lookup on AWS instance from GCP/Azure
        if sct_runner_info.public_ips and sct_runner_info.cloud_provider == "aws":
            ssh_run_cmd_result = ssh_run_cmd(command=cmd, test_id=sct_runner_info.test_id,
                                             node_name=sct_runner_name, force_use_public_ip=True)

            timeout_flag = bool(ssh_run_cmd_result.stdout) if ssh_run_cmd_result else False

        utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
        LOGGER.info("UTC now: %s", utc_now)

        if not dry_run and test_runner_ip:
            _manage_runner_keep_tag_value(test_status=test_status, utc_now=utc_now,
                                          timeout_flag=timeout_flag, sct_runner_info=sct_runner_info,
                                          dry_run=dry_run)

        if not force and sct_runner_info.keep:
            if "alive" in str(sct_runner_info.keep):
                LOGGER.info("Skip %s because `keep' == `alive. No runners have been terminated'", sct_runner_info)
                continue
            if sct_runner_info.keep_action != "terminate":
                LOGGER.info("Skip %s because keep_action `keep_action' != `terminate'", sct_runner_info)
                continue
            if sct_runner_info.launch_time is None:
                LOGGER.info("Skip %s because `launch_time' is not set", sct_runner_info)
                continue
            try:
                if (utc_now - sct_runner_info.launch_time).total_seconds() < int(sct_runner_info.keep) * 3600:
                    LOGGER.info("Skip %s, too early to terminate", sct_runner_info)
                    continue
            except ValueError as exc:
                LOGGER.warning("Value of `keep' tag is invalid: %s", exc)

        if dry_run:
            LOGGER.info("Skip %s because of dry-run", sct_runner_info)
            continue
        try:
            sct_runner_info.terminate()
            runners_terminated += 1
            end_message = f"Number of cleaned runners: {runners_terminated}"
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Exception raised during termination of %s: %s", sct_runner_info, exc)
            end_message = "No runners have been terminated"

    LOGGER.info(end_message)


class AwsFipsSctRunner(AwsSctRunner):
    VERSION = f"{SctRunner.VERSION}-v1-fips"
    BASE_IMAGE = 'resolve:ssm:/aws/service/marketplace/prod-k6fgbnayirmrc/latest'
