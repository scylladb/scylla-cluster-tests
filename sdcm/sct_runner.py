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

import logging
import tempfile
import time
import datetime
from enum import Enum
from functools import cached_property
from math import ceil
from typing import Optional, Any
from abc import ABC, abstractmethod

import pytz
from libcloud.common.google import ResourceNotFoundError as GoogleResourceNotFoundError

from sdcm.keystore import KeyStore, SSHKey
from sdcm.remote import RemoteCmdRunnerBase, shell_script_cmd
from sdcm.utils.aws_utils import ec2_instance_wait_public_ip, ec2_ami_get_root_device_name
from sdcm.utils.get_username import get_username
from sdcm.utils.aws_region import AwsRegion
from sdcm.utils.gce_utils import get_gce_service
from sdcm.cluster_docker import AIO_MAX_NR_RECOMMENDED_VALUE


LOGGER = logging.getLogger(__name__)


class ImageType(Enum):
    SOURCE = "source"
    GENERAL = "general"


def get_current_datetime_formatted():
    return datetime.datetime.now(tz=pytz.utc).strftime("%B %d, %Y, %H:%M:%S")


class SctRunner(ABC):
    """Provision and configure the SCT runner."""
    VERSION = "1.5"  # Version of the Image
    NODE_TYPE = "sct-runner"
    RUNNER_NAME = "SCT-Runner"
    LOGIN_USER = "ubuntu"
    IMAGE_DESCRIPTION = f"SCT runner image {VERSION}"

    BASE_IMAGE: Any
    SOURCE_IMAGE_REGION: str
    IMAGE_BUILDER_INSTANCE_TYPE: str
    REGULAR_TEST_INSTANCE_TYPE: str
    LONGTERM_TEST_INSTANCE_TYPE: str

    def __init__(self, cloud_provider: str, region_name: str, availability_zone: str = ""):
        self.cloud_provider = cloud_provider
        self.region_name = region_name
        self.availability_zone = availability_zone
        self._ssh_pkey_file = None

    @abstractmethod
    def region_az(self, region_name: str, availability_zone: str) -> str:
        ...

    @property
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

    @property
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
        LOGGER.info("Connecting instance...")
        remoter = self.get_remoter(host=public_ip, connect_timeout=connect_timeout)

        LOGGER.info("Installing required packages...")
        login_user = self.LOGIN_USER
        public_key = self.key_pair.public_key.decode()
        result = remoter.sudo(shell_script_cmd(quote="'", cmd=f"""\
            echo "fs.aio-max-nr = {AIO_MAX_NR_RECOMMENDED_VALUE}" >> /etc/sysctl.conf
            echo "{login_user} soft nofile 4096" >> /etc/security/limits.conf
            echo "jenkins soft nofile 4096" >> /etc/security/limits.conf
            echo "root soft nofile 4096" >> /etc/security/limits.conf
            sudo -u {login_user} mkdir -p /home/{login_user}/.ssh || true
            echo "{public_key}" >> /home/{login_user}/.ssh/authorized_keys
            chmod 600 /home/{login_user}/.ssh/authorized_keys
            mkdir -p -m 777 /home/{login_user}/sct-results
            echo "cd ~/sct-results" >> /home/{login_user}/.bashrc
            chown -R {login_user}:{login_user} /home/{login_user}/
            apt-get clean
            apt-get update
            apt-get install -y python3-pip htop screen tree
            pip3 install awscli

            # Install Docker.
            apt-get install -y apt-transport-https ca-certificates curl gnupg-agent software-properties-common
            curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
            apt-key fingerprint 0EBFCD88
            add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
            apt-get update
            apt-get install -y docker-ce docker-ce-cli containerd.io
            usermod -aG docker {login_user}

            # Install kubectl.
            curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
            install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

            # Configure Jenkins user.
            apt-get install -y openjdk-11-jre-headless  # https://www.jenkins.io/doc/administration/requirements/java/
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
            LOGGER.info("Done.")
        else:
            LOGGER.info("No need to copy SCT Runner image since it already exists in `%s'", self.region_name)

    @abstractmethod
    def _get_base_image(self, image: Optional[Any] = None) -> Any:
        ...

    def create_instance(self, test_id: str, test_duration: int, instance_type: str = "") -> Any:
        LOGGER.info("Creating SCT Runner instance...")
        image = self.image
        if not image:
            LOGGER.error("SCT Runner image was not found in %s! "
                         "Use `hydra create-runner-image --cloud-provider %s --region %s'",
                         self.region_name, self.cloud_provider, self.region_name)
            return None
        return self._create_instance(
            instance_type=instance_type or self.instance_type(test_duration=test_duration),
            base_image=self._get_base_image(self.image),
            tags={
                "TestId": test_id,
                "NodeType": self.NODE_TYPE,
                "RunByUser": get_username(),
                "keep": str(ceil(test_duration / 60) + 6),  # keep SCT Runner for 6h more than test_duration
                "keep_action": "terminate",
            },
            instance_name=f"{self.image_name}-instance-{test_id[:8]}",
            region_az=self.region_az(
                region_name=self.region_name,
                availability_zone=self.availability_zone,
            ),
            test_duration=test_duration,
        )


class AwsSctRunner(SctRunner):
    """Provision and configure the SCT Runner on AWS."""
    BASE_IMAGE = "ami-0ffac660dd0cb2973"  # ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-20200609
    SOURCE_IMAGE_REGION = "eu-west-2"  # where the source Runner image will be created and copied to other regions
    IMAGE_BUILDER_INSTANCE_TYPE = "t2.small"
    REGULAR_TEST_INSTANCE_TYPE = "t3.large"  # has 7h 12m CPU burst
    LONGTERM_TEST_INSTANCE_TYPE = "r5.large"

    def __init__(self, region_name: str, availability_zone: str, cloud_provider: str = "aws"):
        super().__init__(cloud_provider=cloud_provider, region_name=region_name, availability_zone=availability_zone)
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
            raise ValueError("Unknown Image type")

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
                         instance_type: str,
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
            raise ValueError("Unknown Image type")

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


class GceSctRunner(SctRunner):
    """Provision and configure the SCT runner on GCE."""
    BASE_IMAGE = "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/family/ubuntu-2004-lts"
    SOURCE_IMAGE_REGION = "us-east1"  # where the source Runner image will be created and copied to other regions
    IMAGE_BUILDER_INSTANCE_TYPE = "e2-standard-2"
    REGULAR_TEST_INSTANCE_TYPE = "e2-standard-4"  # 2 vcpus, 16G
    LONGTERM_TEST_INSTANCE_TYPE = "e2-standard-2"  # 2 vcpus, 8G

    FAMILY = "sct-runner-image"
    SCT_NETWORK = "qa-vpc"

    def __init__(self, region_name: str, availability_zone: str, cloud_provider: str = "gce"):
        super().__init__(cloud_provider=cloud_provider, region_name=region_name, availability_zone=availability_zone)
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
            raise ValueError("Unknown Image type")
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


def get_sct_runner(cloud_provider: str, region_name: str, availability_zone: str = "") -> SctRunner:
    if cloud_provider == "aws":
        return AwsSctRunner(region_name=region_name, availability_zone=availability_zone)
    if cloud_provider == "gce":
        return GceSctRunner(region_name=region_name, availability_zone=availability_zone)
    raise Exception(f'Unsupported Cloud provider: `{cloud_provider}')
