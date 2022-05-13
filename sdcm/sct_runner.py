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
import tempfile
import time
import datetime
from contextlib import suppress
from enum import Enum
from functools import cached_property
from itertools import chain
from math import ceil
from typing import TYPE_CHECKING
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import boto3
import pytz
from libcloud.common.google import ResourceNotFoundError as GoogleResourceNotFoundError
from azure.core.exceptions import ResourceNotFoundError as AzureResourceNotFoundError
from azure.mgmt.compute.models import GalleryImageVersion

from sdcm.keystore import KeyStore
from sdcm.provision.provisioner import InstanceDefinition, PricingModel, VmInstance, provisioner_factory
from sdcm.remote import RemoteCmdRunnerBase, shell_script_cmd
from sdcm.utils.common import list_instances_aws, aws_tags_to_dict, list_instances_gce, gce_meta_to_dict
from sdcm.utils.aws_utils import ec2_instance_wait_public_ip, ec2_ami_get_root_device_name
from sdcm.utils.aws_region import AwsRegion
from sdcm.utils.gce_utils import get_gce_service
from sdcm.utils.azure_utils import AzureService, list_instances_azure
from sdcm.utils.azure_region import AzureOsState, AzureRegion, region_name_to_location
from sdcm.utils.get_username import get_username

if TYPE_CHECKING:
    # pylint: disable=ungrouped-imports
    from typing import Optional, Any, Type

    from mypy_boto3_ec2.literals import InstanceTypeType

    from sdcm.keystore import SSHKey


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
class SctRunnerInfo:  # pylint: disable=too-many-instance-attributes
    sct_runner_class: Type[SctRunner] = field(repr=False)
    cloud_service_instance: Any = field(repr=False)
    region_az: str
    instance: Any = field(repr=False)
    instance_name: str
    public_ips: list[str]
    launch_time: Optional[datetime.datetime] = None
    keep: Optional[str] = None
    keep_action: Optional[str] = None

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
    VERSION = "1.5"  # Version of the Image
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

    def __init__(self, region_name: str, availability_zone: str = ""):
        self.region_name = region_name
        self.availability_zone = availability_zone
        self._ssh_pkey_file = None

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
        if test_duration and test_duration > 3 * 24 * 60:  # 3 days
            # add 40G more space for jobs which test_duration is longer than 3 days
            return disk_size + 40
        return disk_size

    @cached_property
    @abstractmethod
    def key_pair(self) -> SSHKey:
        ...

    def get_remoter(self, host, connect_timeout: Optional[float] = None) -> RemoteCmdRunnerBase:
        self._ssh_pkey_file = tempfile.NamedTemporaryFile(mode="w", delete=False)  # pylint: disable=consider-using-with
        self._ssh_pkey_file.write(self.key_pair.private_key.decode())
        self._ssh_pkey_file.flush()
        return RemoteCmdRunnerBase.create_remoter(hostname=host, user=self.LOGIN_USER,
                                                  key_file=self._ssh_pkey_file.name, connect_timeout=connect_timeout)

    def install_prereqs(self, public_ip: str, connect_timeout: Optional[int] = None) -> None:
        from sdcm.cluster_docker import AIO_MAX_NR_RECOMMENDED_VALUE  # pylint: disable=import-outside-toplevel

        LOGGER.info("Connecting instance...")
        remoter = self.get_remoter(host=public_ip, connect_timeout=connect_timeout)

        LOGGER.info("Installing required packages...")
        login_user = self.LOGIN_USER
        public_key = self.key_pair.public_key.decode()
        result = remoter.sudo(shell_script_cmd(quote="'", cmd=f"""\
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
            usermod -aG docker {login_user}

            # Install kubectl.
            curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
            install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

            # Configure Jenkins user.
            apt-get -qq install --no-install-recommends openjdk-11-jre-headless  # https://www.jenkins.io/doc/administration/requirements/java/
            adduser --disabled-password --gecos "" jenkins || true
            usermod -aG docker jenkins
            mkdir -p /home/jenkins/.ssh
            echo "{public_key}" >> /home/jenkins/.ssh/authorized_keys
            chmod 600 /home/jenkins/.ssh/authorized_keys
            chown -R jenkins:jenkins /home/jenkins
            echo "jenkins ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/jenkins

            # Jenkins pipelines run /bin/sh for some reason.
            ln -sf /bin/bash /bin/sh
        """), ignore_status=True)
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

    @abstractmethod
    # pylint: disable=too-many-arguments
    def _create_instance(self,
                         instance_type: str,
                         base_image: Any,
                         tags: dict[str, str],
                         instance_name: str,
                         region_az: str = "",
                         test_duration: Optional[int] = None) -> Any:
        ...

    @abstractmethod
    def _stop_image_builder_instance(self, instance: Any) -> None:
        ...

    @abstractmethod
    def _terminate_image_builder_instance(self, instance: Any) -> None:
        ...

    @abstractmethod
    def _get_instance_id(self, instance: Any) -> Any:
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

            builder_instance_id = self._get_instance_id(instance=instance)
            try:
                LOGGER.info("Terminating SCT Image Builder instance `%s'...", builder_instance_id)
                self._terminate_image_builder_instance(instance=instance)
            except Exception as ex:  # pylint: disable=broad-except
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

    def create_instance(self, test_id: str, test_duration: int, instance_type: str = "",  # pylint: disable=too-many-arguments
                        restore_monitor: bool = False, restored_test_id: str = "") -> Any:
        LOGGER.info("Creating SCT Runner instance...")
        image = self.image
        if not image:
            LOGGER.error("SCT Runner image was not found in %s! "
                         "Use `hydra create-runner-image --cloud-provider %s --region %s'",
                         self.region_name, self.CLOUD_PROVIDER, self.region_name)
            return None

        tags = {
            "TestId": test_id,
            "NodeType": self.NODE_TYPE,
            "RunByUser": get_username(),
            "keep": str(ceil(test_duration / 60) + 6),  # keep SCT Runner for 6h more than test_duration
            "keep_action": "terminate",
        }
        if restore_monitor and restored_test_id:
            tags.update({"RestoredTestId": restored_test_id})

        return self._create_instance(
            instance_type=instance_type or self.instance_type(test_duration=test_duration),
            base_image=self._get_base_image(self.image),
            tags=tags,
            instance_name=f"{self.image_name}-{'restored-monitor-' if restore_monitor else ''}instance-{test_id[:8]}",
            region_az=self.region_az(
                region_name=self.region_name,
                availability_zone=self.availability_zone,
            ),
            test_duration=test_duration,
        )

    @classmethod
    @abstractmethod
    def list_sct_runners(cls) -> list[SctRunnerInfo]:
        ...

    @staticmethod
    @abstractmethod
    def terminate_sct_runner_instance(sct_runner_info: SctRunnerInfo) -> None:
        ...


class AwsSctRunner(SctRunner):
    """Provision and configure the SCT Runner on AWS."""
    CLOUD_PROVIDER = "aws"
    BASE_IMAGE = "ami-0c4a211d2b7c38400"  # ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-20210907
    SOURCE_IMAGE_REGION = "eu-west-2"  # where the source Runner image will be created and copied to other regions
    IMAGE_BUILDER_INSTANCE_TYPE = "t2.small"
    REGULAR_TEST_INSTANCE_TYPE = "t3.large"  # 2 vcpus, 8G, 36 CPU credits/hour
    LONGTERM_TEST_INSTANCE_TYPE = "r5.large"  # 2 vcpus, 16G

    def __init__(self, region_name: str, availability_zone: str):
        super().__init__(region_name=region_name, availability_zone=availability_zone)
        self.aws_region = AwsRegion(region_name=region_name)
        self.aws_region_source = AwsRegion(region_name=self.SOURCE_IMAGE_REGION)

    def region_az(self, region_name: str, availability_zone: str) -> str:
        return f"{region_name}{availability_zone}"

    @cached_property
    def image_name(self) -> str:
        return f"sct-runner-{self.VERSION}"

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

    # pylint: disable=too-many-arguments
    def _create_instance(self,
                         instance_type: InstanceTypeType,
                         base_image: Any,
                         tags: dict[str, str],
                         instance_name: str,
                         region_az: str = "",
                         test_duration: Optional[int] = None) -> Any:
        if region_az.startswith(self.SOURCE_IMAGE_REGION):
            aws_region = self.aws_region_source
        else:
            aws_region = self.aws_region
        subnet = aws_region.sct_subnet(region_az=region_az)
        assert subnet, f"No SCT subnet found in the source region. " \
                       f"Use `hydra prepare-region --cloud-provider aws --region-name {aws_region.region_name}' " \
                       f"to create cloud env!"

        LOGGER.info("Creating instance...")
        result = aws_region.resource.create_instances(
            ImageId=base_image,
            InstanceType=instance_type,
            MinCount=1,
            MaxCount=1,
            KeyName=aws_region.SCT_KEY_PAIR_NAME,
            NetworkInterfaces=[{
                "DeviceIndex": 0,
                "AssociatePublicIpAddress": True,
                "SubnetId": subnet.subnet_id,
                "Groups": [aws_region.sct_security_group.group_id],
                "DeleteOnTermination": True,
            }],
            TagSpecifications=[{
                "ResourceType": "instance",
                "Tags": [{"Key": key, "Value": value} for key, value in tags.items()] +
                        [{"Key": "Name", "Value": instance_name}],
            }],
            BlockDeviceMappings=[{
                "DeviceName": ec2_ami_get_root_device_name(image_id=base_image, region=aws_region.region_name),
                "Ebs": {
                    "VolumeSize": self.instance_root_disk_size(test_duration),
                    "VolumeType": "gp2"
                }
            }]
        )
        instance = result[0]

        LOGGER.info("Instance created. Waiting until it becomes running... ")
        instance.wait_until_running()

        LOGGER.info("Instance `%s' is running. Waiting for public IP...", instance.instance_id)
        ec2_instance_wait_public_ip(instance=instance)

        LOGGER.info("Got public IP: %s", instance.public_ip_address)

        return instance

    def _stop_image_builder_instance(self, instance: Any) -> None:
        instance.stop()
        instance.wait_until_stopped()

    def _terminate_image_builder_instance(self, instance: Any) -> None:
        instance.terminate()

    def _get_instance_id(self, instance: Any) -> Any:
        return instance.instance_id

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
        result = self.aws_region.client.copy_image(  # pylint: disable=no-member
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
    def list_sct_runners(cls) -> list[SctRunnerInfo]:
        all_instances = list_instances_aws(tags_dict={"NodeType": cls.NODE_TYPE}, group_as_region=True, verbose=True)
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
                    instance_name=instance_name,
                    public_ips=[instance.get("PublicIpAddress"), ],
                    launch_time=instance["LaunchTime"],
                    keep=tags.get("keep"),
                    keep_action=tags.get("keep_action"),
                ))
        return sct_runners

    @staticmethod
    def terminate_sct_runner_instance(sct_runner_info: SctRunnerInfo) -> None:
        sct_runner_info.cloud_service_instance.terminate_instances(
            InstanceIds=[sct_runner_info.instance["InstanceId"], ],
        )


class GceSctRunner(SctRunner):
    """Provision and configure the SCT runner on GCE."""
    CLOUD_PROVIDER = "gce"
    BASE_IMAGE = "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/family/ubuntu-2004-lts"
    SOURCE_IMAGE_REGION = "us-east1"  # where the source Runner image will be created and copied to other regions
    IMAGE_BUILDER_INSTANCE_TYPE = "e2-standard-2"
    REGULAR_TEST_INSTANCE_TYPE = "e2-standard-2"  # 2 vcpus, 8G
    LONGTERM_TEST_INSTANCE_TYPE = "e2-standard-4"  # 2 vcpus, 16G,

    FAMILY = "sct-runner-image"
    SCT_NETWORK = "qa-vpc"

    def __init__(self, region_name: str, availability_zone: str):
        super().__init__(region_name=region_name, availability_zone=availability_zone)
        self.gce_service = get_gce_service(region=region_name)
        self.gce_service_source = get_gce_service(region=self.SOURCE_IMAGE_REGION)
        self.project_name = self.gce_service.ex_get_project().name

    def region_az(self, region_name: str, availability_zone: str) -> str:
        if availability_zone:
            return f"{region_name}-{availability_zone}"
        return region_name

    @cached_property
    def image_name(self) -> str:
        return f"sct-runner-{self.VERSION.replace('.', '-')}"

    @cached_property
    def key_pair(self) -> SSHKey:
        return KeyStore().get_gce_ssh_key_pair()  # scylla-test

    def _image(self, image_type: ImageType = ImageType.SOURCE) -> Any:
        if image_type == ImageType.SOURCE:
            gce_service = self.gce_service_source
        elif image_type == ImageType.GENERAL:
            gce_service = self.gce_service
        else:
            raise ValueError(f"Unknown Image type: {image_type}")
        try:
            return gce_service.ex_get_image(self.image_name)
        except GoogleResourceNotFoundError:
            return None

    @staticmethod
    def tags_to_labels(tags: dict[str, str]) -> dict[str, str]:
        return {key.lower(): value.lower().replace(".", "_") for key, value in tags.items()}

    # pylint: disable=too-many-arguments
    def _create_instance(self,
                         instance_type: str,
                         base_image: Any,
                         tags: dict[str, str],
                         instance_name: str,
                         region_az: str = "",
                         test_duration: Optional[int] = None) -> Any:
        LOGGER.info("Creating instance...")
        if region_az.startswith(self.SOURCE_IMAGE_REGION):
            gce_service = self.gce_service_source
        else:
            gce_service = self.gce_service
        instance = gce_service.create_node(
            name=instance_name,
            size=instance_type,
            image=base_image,
            ex_network=self.SCT_NETWORK,
            ex_tags="keep-alive",
            ex_disks_gce_struct=[{
                "type": "PERSISTENT",
                "deviceName": f"{instance_name}-root-pd-ssd",
                "initializeParams": {
                    "diskName": f"{instance_name}-root-pd-ssd",
                    "diskType": f"projects/{self.project_name}/zones/{gce_service.zone.name}/diskTypes/pd-ssd",
                    "diskSizeGb": self.instance_root_disk_size(test_duration),
                    "sourceImage": base_image,
                },
                "boot": True,
                "autoDelete": True,
            }],
            ex_metadata=tags | {
                "launch_time": get_current_datetime_formatted(),
                "block-project-ssh-keys": "true",
                "ssh-keys": f"{self.LOGIN_USER}:{self.key_pair.public_key.decode()}",
            },
        )
        time.sleep(30)  # wait until the public IPs are available.

        LOGGER.info("Got public IP: %s", instance.public_ips[0])

        return instance

    def _stop_image_builder_instance(self, instance: Any) -> None:
        self.gce_service_source.ex_stop_node(node=instance)

    def _terminate_image_builder_instance(self, instance: Any) -> None:
        self.gce_service_source.destroy_node(instance)

    def _get_instance_id(self, instance: Any) -> Any:
        return instance.id

    def get_instance_public_ip(self, instance: Any) -> str:
        return instance.public_ips[0]

    def _create_image(self, instance: Any) -> Any:
        return self.gce_service_source.ex_create_image(
            name=self.image_name,
            volume=self.gce_service_source.ex_get_volume(f"{instance.name}-root-pd-ssd"),
            description=self.IMAGE_DESCRIPTION,
            family=self.FAMILY,
            ex_labels=self.tags_to_labels(tags=self.image_tags),
        )

    def _get_image_id(self, image: Any) -> Any:
        return image.id

    def _get_image_url(self, image_id) -> str:
        return f"https://www.googleapis.com/compute/alpha/projects/{self.project_name}/global/images/{image_id}"

    def _copy_source_image_to_region(self) -> None:
        image = self.gce_service.ex_copy_image(
            self.image_name,
            self._get_image_url(self.source_image.id),
            description=self.IMAGE_DESCRIPTION,
            family=self.FAMILY,
        )
        LOGGER.info("Image copied, id: `%s'.", image.id)

    def _get_base_image(self, image: Optional[Any] = None) -> Any:
        if image is None:
            image = self.image
        return self._get_image_url(image.id)

    @classmethod
    def list_sct_runners(cls) -> list[SctRunnerInfo]:
        sct_runners = []
        for instance in list_instances_gce(tags_dict={"NodeType": cls.NODE_TYPE}, verbose=True):
            tags = gce_meta_to_dict(instance.extra["metadata"])
            region = instance.extra["zone"].name
            if launch_time := tags.get("launch_time"):
                try:
                    launch_time = datetime_from_formatted(date_string=launch_time)
                except ValueError as exc:
                    LOGGER.warning("Value of `launch_time' tag is invalid: %s", exc)
                    launch_time = None
            if not launch_time:
                create_time = instance.extra["creationTimestamp"]
                LOGGER.info("`launch_time' tag is empty or invalid, fallback to creation time: %s", create_time)
                launch_time = datetime.datetime.fromisoformat(create_time)
            sct_runners.append(SctRunnerInfo(
                sct_runner_class=cls,
                cloud_service_instance=None,  # we don't need it for GCE
                region_az=region,
                instance=instance,
                instance_name=instance.name,
                public_ips=instance.public_ips,
                launch_time=launch_time,
                keep=tags.get("keep"),
                keep_action=tags.get("keep_action"),
            ))
        return sct_runners

    @staticmethod
    def terminate_sct_runner_instance(sct_runner_info: SctRunnerInfo) -> None:
        sct_runner_info.instance.destroy()


class AzureSctRunner(SctRunner):
    """Provision and configure the SCT runner on Azure."""
    CLOUD_PROVIDER = "azure"
    GALLERY_IMAGE_NAME = "sct-runner"
    GALLERY_IMAGE_VERSION = f"{SctRunner.VERSION}.0"  # Azure requires to have it in `X.Y.Z' format
    BASE_IMAGE = {
        "publisher": "canonical",
        "offer": "0001-com-ubuntu-server-focal",
        "sku": "20_04-lts-gen2",
        "version": "latest",
    }
    SOURCE_IMAGE_REGION = AzureRegion.SCT_GALLERY_REGION
    IMAGE_BUILDER_INSTANCE_TYPE = "Standard_D2s_v3"
    REGULAR_TEST_INSTANCE_TYPE = "Standard_D2s_v3"  # 2 vcpus, 8G, recommended by Ubuntu 20.04 LTS image publisher
    LONGTERM_TEST_INSTANCE_TYPE = "Standard_E2s_v3"  # 2 vcpus, 16G, recommended by Ubuntu 20.04 LTS image publisher

    def __init__(self, region_name: str):
        super().__init__(region_name=region_name)
        self.azure_region = AzureRegion(region_name=region_name)
        self.azure_region_source = AzureRegion(region_name=self.SOURCE_IMAGE_REGION)
        self.azure_service = self.azure_region.azure_service

    def region_az(self, region_name: str, availability_zone: str) -> str:
        return region_name

    @cached_property
    def image_name(self) -> str:
        return f"sct-runner-{self.VERSION}"

    @cached_property
    def key_pair(self) -> SSHKey:
        return KeyStore().get_gce_ssh_key_pair()  # scylla-test

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

    def _create_instance(self,  # pylint: disable=too-many-arguments
                         instance_type: str,
                         base_image: Any,
                         tags: dict[str, str],
                         instance_name: str,
                         region_az: str = "",
                         test_duration: Optional[int] = None) -> Any:
        if base_image is self.BASE_IMAGE:
            azure_region = self.azure_region_source
            additional_kwargs = {
                "os_state": AzureOsState.GENERALIZED,
                "computer_name": self.image_name.replace(".", "-"),
                "admin_username": self.LOGIN_USER,
                "admin_public_key": self.key_pair.public_key.decode(),
            }
            return azure_region.create_virtual_machine(
                vm_name=instance_name,
                vm_size=instance_type,
                image=base_image,
                disk_size=self.instance_root_disk_size(test_duration=test_duration),
                tags=tags | {"launch_time": get_current_datetime_formatted()},
                **additional_kwargs,
            )
        else:
            test_id = tags["TestId"]
            provisioner = provisioner_factory.create_provisioner(backend="azure", test_id=test_id,
                                                                 region=self.azure_region.location)
            vm_params = InstanceDefinition(name=instance_name,
                                           image_id=base_image["id"],
                                           type=instance_type,
                                           user_name=self.LOGIN_USER,
                                           ssh_key=self.key_pair,
                                           tags=tags | {"launch_time": get_current_datetime_formatted()},
                                           root_disk_size=self.instance_root_disk_size(test_duration=test_duration),
                                           user_data=None)
            return provisioner.get_or_create_instance(definition=vm_params,
                                                      pricing_model=PricingModel.ON_DEMAND)

    def _stop_image_builder_instance(self, instance: Any) -> None:
        self.azure_region_source.deallocate_virtual_machine(vm_name=instance.name)

    def _terminate_image_builder_instance(self, instance: Any) -> None:
        self.azure_service.delete_virtual_machine(virtual_machine=instance)

    def _get_instance_id(self, instance: Any) -> Any:
        return instance.id

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
    def list_sct_runners(cls) -> list[SctRunnerInfo]:
        azure_service = AzureService()
        sct_runners = []
        for instance in list_instances_azure(tags_dict={"NodeType": cls.NODE_TYPE}, verbose=True):
            if launch_time := instance.tags.get("launch_time") or None:
                try:
                    launch_time = datetime_from_formatted(date_string=launch_time)
                except ValueError as exc:
                    LOGGER.warning("Value of `launch_time' tag is invalid: %s", exc)
                    launch_time = None
            sct_runners.append(SctRunnerInfo(
                sct_runner_class=cls,
                cloud_service_instance=azure_service,
                region_az=instance.location,
                instance=instance,
                instance_name=instance.name,
                public_ips=[azure_service.get_virtual_machine_ips(virtual_machine=instance).public_ip],
                launch_time=launch_time,
                keep=instance.tags.get("keep"),
                keep_action=instance.tags.get("keep_action"),
            ))
        return sct_runners

    @staticmethod
    def terminate_sct_runner_instance(sct_runner_info: SctRunnerInfo) -> None:
        sct_runner_info.cloud_service_instance.delete_virtual_machine(virtual_machine=sct_runner_info.instance)


def get_sct_runner(cloud_provider: str, region_name: str, availability_zone: str = "") -> SctRunner:
    if cloud_provider == "aws":
        return AwsSctRunner(region_name=region_name, availability_zone=availability_zone)
    if cloud_provider == "gce":
        return GceSctRunner(region_name=region_name, availability_zone=availability_zone)
    if cloud_provider == "azure":
        return AzureSctRunner(region_name=region_name)
    raise Exception(f'Unsupported Cloud provider: `{cloud_provider}')


def list_sct_runners(test_status: Optional[str] = None, test_runner_ip: str = None) -> list[SctRunnerInfo]:
    LOGGER.info("Looking for SCT runner instances...")
    sct_runner_classes = (AwsSctRunner, GceSctRunner, AzureSctRunner, )
    sct_runners = chain.from_iterable(cls.list_sct_runners() for cls in sct_runner_classes)

    if test_runner_ip:
        if sct_runner_info := next((runner for runner in sct_runners if test_runner_ip in runner.public_ips), None):
            if test_status == "SUCCESS":
                sct_runner_info.keep = None  # force SCT Runner termination
            sct_runners = [sct_runner_info, ]
        else:
            LOGGER.warning("No SCT Runners found to remove (IP: %s)", test_runner_ip)
            return []
    else:
        sct_runners = list(sct_runners)

    LOGGER.info("%d SCT runner(s) found:\n    %s", len(sct_runners), "\n    ".join(map(str, sct_runners)))

    return sct_runners


def clean_sct_runners(test_status: Optional[str] = None, test_runner_ip: str = None, dry_run: bool = False) -> None:
    utc_now = datetime.datetime.now(tz=datetime.timezone.utc)
    LOGGER.info("UTC now: %s", utc_now)

    runners_cleaned = 0

    for sct_runner_info in list_sct_runners(test_status=test_status, test_runner_ip=test_runner_ip):
        if sct_runner_info.keep:
            if "alive" in sct_runner_info.keep:
                LOGGER.info("Skip %s because `keep' == `alive'", sct_runner_info)
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
            runners_cleaned += 1
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning("Exception raised during termination of %s: %s", sct_runner_info, exc)

    if runners_cleaned:
        LOGGER.info("Cleaned %d runners", runners_cleaned)
    else:
        LOGGER.info("No runners have been terminated")
