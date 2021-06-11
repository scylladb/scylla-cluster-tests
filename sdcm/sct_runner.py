import logging
import random
import sys
import tempfile
import time
import datetime
from enum import Enum
from functools import lru_cache, cached_property
from math import ceil
from textwrap import dedent
from typing import Optional
from abc import ABC, abstractmethod
import pytz

import boto3
from libcloud.common.google import ResourceNotFoundError

from sdcm.keystore import KeyStore, SSHKey
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.utils.aws_utils import ec2_instance_wait_public_ip, ec2_ami_get_root_device_name
from sdcm.utils.get_username import get_username
from sdcm.utils.prepare_region import AwsRegion
from sdcm.utils.gce_utils import get_gce_service


LOGGER = logging.getLogger(__name__)


class ImageType(Enum):
    SOURCE = "source"
    GENERAL = "general"


class SctRunner(ABC):
    """Provisions and configures the SCT runner"""
    VERSION = 1.4  # Version of the Image
    NODE_TYPE = "sct-runner"
    RUNNER_NAME = "SCT-Runner"
    LOGIN_USER = "ubuntu"
    IMAGE_DESCRIPTION = f"SCT runner image {VERSION}"

    def __init__(self, cloud_provider: str, region_name: str):
        self.cloud_provider = cloud_provider
        self.region_name = region_name
        self._ssh_pkey_file = None

    @cached_property
    @abstractmethod
    def image_name(self) -> str:
        ...

    @staticmethod
    @abstractmethod
    def instance_type(test_duration) -> str:
        ...

    @staticmethod
    def instance_root_disk_size(test_duration) -> int:
        disk_size = 80  # GB
        if test_duration and test_duration > 3 * 24 * 60:  # 3 days
            # add 40G more space for jobs which test_duration is longer than 3 days
            return disk_size + 40
        return disk_size

    @staticmethod
    @abstractmethod
    @lru_cache(maxsize=None)
    def key_pair() -> SSHKey:
        ...

    def get_remoter(self, host, connect_timeout: Optional[float] = None) -> RemoteCmdRunnerBase:
        self._ssh_pkey_file = tempfile.NamedTemporaryFile(mode="w", delete=False)  # pylint: disable=consider-using-with
        self._ssh_pkey_file.write(self.key_pair().private_key.decode())
        self._ssh_pkey_file.flush()
        return RemoteCmdRunnerBase.create_remoter(hostname=host, user=self.LOGIN_USER,
                                                  key_file=self._ssh_pkey_file.name, connect_timeout=connect_timeout)

    def install_prereqs(self, public_ip: str, connect_timeout: Optional[int] = None) -> None:
        prereqs_script = dedent(f"""
            echo "fs.aio-max-nr = 65536" >> /etc/sysctl.conf
            echo "ubuntu soft nofile 4096" >> /etc/security/limits.conf
            echo "jenkins soft nofile 4096" >> /etc/security/limits.conf
            echo "root soft nofile 4096" >> /etc/security/limits.conf
            sudo -u ubuntu mkdir -p /home/ubuntu/.ssh || true
            echo "{self.key_pair().public_key.decode()}" >> /home/ubuntu/.ssh/authorized_keys
            chmod 600 /home/ubuntu/.ssh/authorized_keys
            mkdir -p -m 777 /home/ubuntu/sct-results
            echo "cd ~/sct-results" >> /home/ubuntu/.bashrc
            chown -R ubuntu:ubuntu /home/ubuntu/
            apt clean
            apt update
            apt install -y python3-pip htop screen tree
            pip3 install awscli
            # docker
            apt-get install -y apt-transport-https ca-certificates curl gnupg-agent software-properties-common
            curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
            apt-key fingerprint 0EBFCD88
            add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
            apt update
            apt install -y docker-ce docker-ce-cli containerd.io
            usermod -aG docker {self.LOGIN_USER}
            # add kubectl
            curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
            sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
            usermod -aG docker ubuntu || true
            # configure Jenkins user
            apt install -y openjdk-14-jre-headless
            adduser --disabled-password --gecos "" jenkins || true
            usermod -aG docker jenkins
            mkdir -p /home/jenkins/.ssh
            echo "{self.key_pair().public_key.decode()}" >> /home/jenkins/.ssh/authorized_keys
            chmod 600 /home/jenkins/.ssh/authorized_keys
            chown -R jenkins:jenkins /home/jenkins
            echo "jenkins ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/jenkins
            # Jenkins pipelines run /bin/sh for some reason
            unlink /bin/sh
            ln -s /bin/bash /bin/sh
        """)
        LOGGER.info("Connecting instance...")
        remoter = self.get_remoter(host=public_ip, connect_timeout=connect_timeout)
        LOGGER.info("Installing required packages...")
        result = remoter.run(f"sudo bash -cxe '{prereqs_script}'", ignore_status=True)

        remoter.stop()
        if result.exit_status == 0:
            LOGGER.info("All packages successfully installed.")
        else:
            raise Exception("Unable to install required packages:\n%s" % (result.stdout + result.stderr))

    @abstractmethod
    def _image(self, image_type=ImageType.SOURCE):
        ...

    @property
    def source_image(self):
        return self._image(image_type=ImageType.SOURCE)

    @property
    def image(self):
        return self._image(image_type=ImageType.GENERAL)

    @abstractmethod
    def _create_instance(self, instance_type, base_image, tags_list, instance_name=None, region_az="", test_duration=None):
        ...

    @abstractmethod
    def create_image(self) -> None:
        ...

    @abstractmethod
    def _get_base_image(self, image=None):
        ...

    def create_instance(self, test_id: str, test_duration: int, region_az: str):
        """
            :param test_duration: used to set keep-alive flags, measured in MINUTES
        """
        LOGGER.info("Creating SCT Runner instance...")
        image = self.image
        if not image:
            LOGGER.error(f"SCT Runner image was not found in {self.region_name}! "
                         f"Use hydra create-runner-image --cloud-privider {self.cloud_provider} --region {self.region_name}")
            sys.exit(1)
        lt_datetime = datetime.datetime.now(tz=pytz.utc)
        return self._create_instance(
            instance_type=self.instance_type(test_duration=test_duration),
            base_image=self._get_base_image(self.image),
            tags_list=[
                {"Key": "Name", "Value": self.RUNNER_NAME},
                {"Key": "TestId", "Value": test_id},
                {"Key": "NodeType", "Value": self.NODE_TYPE},
                {"Key": "RunByUser", "Value": get_username()},
                {"Key": "keep", "Value": str(ceil(test_duration / 60) + 6)},
                {"Key": "keep_action", "Value": "terminate"},
                {"Key": "launch_time", "Value": lt_datetime.strftime("%B %d, %Y, %H:%M:%S")},
            ],
            instance_name=f"{self.image_name}-instance-{test_id[:8]}",
            region_az=region_az,
            test_duration=test_duration,
        )


class AwsSctRunner(SctRunner):
    """Provisions and configures the SCT runner on AWS"""
    BASE_IMAGE = "ami-0ffac660dd0cb2973"  # ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-20200609
    SOURCE_IMAGE_REGION = "eu-west-2"  # where the source Runner image will be created and copied to other regions

    def __init__(self, region_name: str, availability_zone: str, cloud_provider: str = 'aws'):
        super().__init__(cloud_provider=cloud_provider, region_name=region_name)
        self.availability_zone = availability_zone
        self.ec2_client = boto3.client("ec2", region_name=region_name)
        self.ec2_client_source = boto3.client("ec2", region_name=self.SOURCE_IMAGE_REGION)
        self.ec2_resource = boto3.resource("ec2", region_name=region_name)
        self.ec2_resource_source = boto3.resource("ec2", region_name=self.SOURCE_IMAGE_REGION)

    @cached_property
    def image_name(self) -> str:
        return f"sct-runner-{self.VERSION}"

    @staticmethod
    def instance_type(test_duration) -> str:
        if test_duration > 7 * 60:
            return "r5.large"
        return "t3.large"  # has 7h 12m CPU burst

    @staticmethod
    @lru_cache(maxsize=None)
    def key_pair() -> SSHKey:
        ks = KeyStore()
        return ks.get_ec2_ssh_key_pair()

    def _image(self, image_type=ImageType.SOURCE):
        if image_type == ImageType.SOURCE:
            client = self.ec2_client_source
        elif image_type == ImageType.GENERAL:
            client = self.ec2_client
        else:
            raise ValueError("Unknown Image type")
        amis = client.describe_images(Owners=["self"],
                                      Filters=[{"Name": "tag:Name", "Values": [self.image_name]},
                                               {"Name": "tag:Version", "Values": [str(self.VERSION)]}])
        LOGGER.debug("Found SCT Runner AMIs: %s", amis)
        existing_amis = amis.get("Images", [])
        if len(existing_amis) == 0:
            return None
        assert len(existing_amis) == 1, \
            f"More than 1 SCT Runner AMI with {self.image_name}:{self.VERSION} " \
            f"found in {self.region_name}: {existing_amis}"
        return self.ec2_resource.Image(existing_amis[0]["ImageId"])  # pylint: disable=no-member

    def _create_instance(self, instance_type, base_image, tags_list, instance_name=None, region_az="", test_duration=None):
        region = region_az[:-1]
        aws_region = AwsRegion(region_name=region)
        region_az = region_az if region_az else random.choice(aws_region.availability_zones)
        subnet = aws_region.sct_subnet(region_az=region_az)
        assert subnet, f"No SCT subnet found in the source region. " \
                       f"Use hydra prepare-region --region-name '{self.region_name}' to create cloud env!"
        LOGGER.info("Creating instance...")
        ec2_resource = boto3.resource("ec2", region_name=region)
        result = ec2_resource.create_instances(
            ImageId=base_image,
            InstanceType=instance_type,
            MinCount=1,
            MaxCount=1,
            KeyName=aws_region.KEY_PAIR_NAME,
            NetworkInterfaces=[{
                "DeviceIndex": 0,
                "AssociatePublicIpAddress": True,
                "SubnetId": subnet.subnet_id,
                "Groups": [aws_region.sct_security_group.group_id],
                "DeleteOnTermination": True,
            }],
            TagSpecifications=[{"ResourceType": "instance", "Tags": tags_list}],
            BlockDeviceMappings=[{
                "DeviceName": ec2_ami_get_root_device_name(image_id=base_image, region=region),
                "Ebs": {
                    "VolumeSize": self.instance_root_disk_size(test_duration),
                    "VolumeType": "gp2"
                }
            }]
        )
        instance = result[0]
        LOGGER.info("Instance created. Waiting until it becomes running... ")
        instance.wait_until_running()
        LOGGER.info("Instance '%s' is running. Waiting for public IP...", instance.instance_id)
        ec2_instance_wait_public_ip(instance=instance)
        LOGGER.info("Got public IP: %s", instance.public_ip_address)
        return instance

    def tag_image(self, image_id, image_type) -> None:
        if image_type == ImageType.SOURCE:
            ec2_resource = self.ec2_resource_source
        elif image_type == ImageType.GENERAL:
            ec2_resource = self.ec2_resource
        image_tags = [
            {"Key": "Name", "Value": self.image_name},
            {"Key": "Version", "Value": str(self.VERSION)},
        ]
        runer_image = ec2_resource.Image(image_id)
        runer_image.wait_until_exists()
        LOGGER.info("Image '%s' exists and ready. Tagging...", image_id)
        runer_image.create_tags(Tags=image_tags)
        LOGGER.info("Tagging completed.")
        LOGGER.info("SCT Runner image created in '%s'. Id [%s].", self.region_name, image_id)

    def create_image(self) -> None:
        """
            Create an Image for SCT Runner in specified region. If the Image exists in SOURCE_REGION
            it will be copied to the destination region.
            Warning: this can't run in parallel!
        """
        LOGGER.info("Looking for source SCT Runner Image in '%s'...", self.SOURCE_IMAGE_REGION)
        source_image = self.source_image
        if not source_image:
            LOGGER.info("Source SCT Runner Image not found. Creating...")
            instance = self._create_instance(
                instance_type="t3.small",
                base_image=self.BASE_IMAGE,
                tags_list=[{"Key": "Name", "Value": "sct-image-builder"},
                           {"Key": "keep", "Value": "1"},
                           {"Key": "keep_action", "Value": "terminate"},
                           {"Key": "Version", "Value": str(self.VERSION)},
                           ],
                region_az=self.SOURCE_IMAGE_REGION + self.availability_zone  # pylint: disable=no-member
            )
            self.install_prereqs(public_ip=instance.public_ip_address)
            LOGGER.info("Stopping the SCT Image Builder instance...")
            instance.stop()
            instance.wait_until_stopped()
            LOGGER.info("SCT Image Builder instance stopped.\nCreating image...")
            result = self.ec2_client_source.create_image(
                Description=self.IMAGE_DESCRIPTION,
                InstanceId=instance.instance_id,
                Name=self.image_name,
                NoReboot=False
            )
            self.tag_image(image_id=result["ImageId"], image_type=ImageType.SOURCE)
            try:
                LOGGER.info("Terminating image builder instance '%s'...", instance.instance_id)
                instance.terminate()
            except Exception as ex:  # pylint: disable=broad-except
                LOGGER.warning("Was not able to terminate '%s': %s\n"
                               "Please terminate manually!!!", instance.instance_id, ex)

        else:
            LOGGER.info("SCT Runner image exists in the source region '%s'! "
                        "ID: %s", self.SOURCE_IMAGE_REGION, source_image.image_id)

        if self.region_name != self.SOURCE_IMAGE_REGION and self.image is None:
            LOGGER.info(f"Copying {self.image_name} to {self.region_name}...\nNote: It can take 5-15 minutes.")
            result = self.ec2_client.copy_image(  # pylint: disable=no-member
                Description=self.IMAGE_DESCRIPTION,
                Name=self.image_name,
                SourceImageId=self.source_image.image_id,
                SourceRegion=self.SOURCE_IMAGE_REGION
            )
            LOGGER.info("Image copied, id: '%s'.", result["ImageId"])
            self.tag_image(image_id=result["ImageId"], image_type=ImageType.GENERAL)
            LOGGER.info("Done.")
        else:
            LOGGER.info("No need to copy SCT Runner image since it already exists in '%s'.", self.region_name)

    def _get_base_image(self, image=None):
        if image is None:
            image = self.image
        return image.id


class GceSctRunner(SctRunner):
    """Provisions and configures the SCT runner on GCE"""
    BASE_IMAGE = "https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/family/ubuntu-2004-lts"
    SOURCE_IMAGE_REGION = "us-east1"  # where the source Runner image will be created and copied to other regions
    FAMILY = "sct-runner-image"
    LOGIN_USER = "scylla-test"

    def __init__(self, datacenter: str, availability_zone: str, cloud_provider: str = 'gce'):
        super().__init__(cloud_provider=cloud_provider, region_name=datacenter)
        self.availability_zone = availability_zone
        self.gce_service = get_gce_service(datacenter)
        self.gce_service_source = get_gce_service(self.SOURCE_IMAGE_REGION)
        self._project = self.gce_service.ex_get_project()
        self.project_name = self._project.name

    @cached_property
    def image_name(self) -> str:
        return f"sct-runner-{str(self.VERSION).replace('.', '-')}"

    @staticmethod
    def instance_type(test_duration) -> str:
        if test_duration > 7 * 60:
            return "e2-standard-4"  # 2 vcpus, 16G
        return "e2-standard-2"  # 2 vcpus, 8G

    def _get_image_url(self, image_id) -> str:
        return f"https://www.googleapis.com/compute/alpha/projects/{self.project_name}/global/images/{image_id}"

    @staticmethod
    @lru_cache(maxsize=None)
    def key_pair() -> SSHKey:
        ks = KeyStore()
        return ks.get_gce_ssh_key_pair()  # scylla-test

    def create_image(self) -> None:
        """
             Create an Image for SCT Runner in specified region. If the Image exists in SOURCE_REGION
             it will be copied to the destination region.
             Warning: this can't run in parallel!
         """
        LOGGER.info(f"Looking for source SCT Runner Image in {self.SOURCE_IMAGE_REGION}...")
        source_image = self.source_image
        if not source_image:
            # GCE doesn't allow repeat name in multiple datacenter
            instance_name = f"{self.image_name}-builder-{self.SOURCE_IMAGE_REGION}"
            LOGGER.info(f"Source SCT Runner Image not found. Creating...")
            if self.availability_zone != "":
                region_az = f"{self.SOURCE_IMAGE_REGION}-{self.availability_zone}"
            else:
                region_az = self.SOURCE_IMAGE_REGION
            lt_datetime = datetime.datetime.now(tz=pytz.utc)
            instance = self._create_instance(
                instance_type="e2-standard-2",
                base_image=self.BASE_IMAGE,
                tags_list=[{"Key": "Name", "Value": "sct-image-builder"},
                           {"Key": "keep", "Value": "1"},
                           {"Key": "keep_action", "Value": "terminate"},
                           {"Key": "Version", "Value": str(self.VERSION)},
                           {"Key": "launch_time", "Value": lt_datetime.strftime("%B %d, %Y, %H:%M:%S")},
                           ],
                instance_name=instance_name,
                region_az=region_az
            )
            time.sleep(30)  # wait until the public ips are available.
            self.install_prereqs(public_ip=instance.public_ips[0], connect_timeout=120)

            LOGGER.info("Stopping the SCT Image Builder instance...")
            self.gce_service_source.ex_stop_node(instance)
            LOGGER.info("SCT Image Builder instance stopped.\nCreating image...")
            source_volume = self.gce_service_source.ex_get_volume(f"{instance_name}-root-pd-ssd")
            new_image = self.gce_service_source.ex_create_image(self.image_name,
                                                                source_volume,
                                                                description=self.IMAGE_DESCRIPTION,
                                                                family=self.FAMILY,
                                                                ex_labels={"name": self.image_name,
                                                                           "version": str(self.VERSION).replace('.', '_')})
            try:
                LOGGER.info("Terminating image builder instance '%s'...", instance.id)
                self.gce_service_source.destroy_node(instance)
            except Exception as ex:  # pylint: disable=broad-except
                LOGGER.warning(f"Was not able to terminate '{instance.id}': {ex}\n"
                               "Please terminate manually!!!")

        else:
            LOGGER.info(f"SCT Runner image exists in the source region '{self.SOURCE_IMAGE_REGION}'! "
                        f"ID: {source_image.id}")

        if self.region_name != self.SOURCE_IMAGE_REGION and self.image is None:
            LOGGER.info(f"Copying {self.image_name} to {self.region_name}...\nNote: It can take 5-15 minutes.")
            new_image = self.gce_service.ex_copy_image(  # pylint: disable=no-member
                self.image_name,
                self._get_image_url(self.source_image.id),
                description=self.IMAGE_DESCRIPTION,
                family=self.FAMILY
            )
            LOGGER.info("Image copied, id: '%s'.", new_image.id)
            LOGGER.info("Done.")
        else:
            LOGGER.info("No need to copy SCT Runner image since it's available for '%s'.", self.region_name)

    def _get_base_image(self, image=None):
        """
        GCE needs image object in creating instance
        """
        if image is None:
            image = self.image
        return self._get_image_url(image.id)

    def _create_instance(self, instance_type, base_image, tags_list, instance_name=None, region_az="", test_duration=None):
        if instance_name is None:
            instance_name = f"{self.image_name}-instance"
        ex_disks_gce_struct = [{"type": "PERSISTENT",
                                "deviceName": f"{instance_name}-root-pd-ssd",
                                "initializeParams": {
                                    "diskName": f"{instance_name}-root-pd-ssd",
                                    "diskType": f"projects/{self.project_name}/zones/{self.gce_service_source.zone.name}/diskTypes/pd-ssd",
                                    "diskSizeGb": self.instance_root_disk_size(test_duration),
                                    "sourceImage": base_image,
                                },
                                "boot": True,
                                "autoDelete": True},
                               ]
        labels = {}
        metadata = {}
        for tag_dict in tags_list:
            if tag_dict['Key'] != 'launch_time':
                labels[tag_dict['Key'].lower()] = str(tag_dict['Value']).lower().replace('.', '_')
            metadata[tag_dict['Key']] = tag_dict['Value']
        LOGGER.debug(f"Create node ({instance_name}) by image ({base_image})")
        return self.gce_service_source.create_node(name=instance_name, size=instance_type,
                                                   image=base_image,
                                                   ex_network='qa-vpc',
                                                   ex_disks_gce_struct=ex_disks_gce_struct,
                                                   ex_labels=labels,
                                                   ex_metadata=metadata)

    def _image(self, image_type=ImageType.SOURCE):
        if image_type == ImageType.SOURCE:
            driver = self.gce_service_source
        elif image_type == ImageType.GENERAL:
            driver = self.gce_service
        else:
            raise ValueError("Unknown Image type")

        try:
            return driver.ex_get_image(self.image_name)
        except ResourceNotFoundError as ex:
            return None


if __name__ == "__main__":
    TEST_REGION = "eu-west-2"
    TEST_ZONE = "a"
    SCT_RUNNER = AwsSctRunner(region_name=TEST_REGION, availability_zone=TEST_ZONE)
    SCT_RUNNER.create_image()
    SCT_RUNNER.create_instance(test_id="byakabuka", test_duration=60, region_az=f"{TEST_REGION}{TEST_ZONE}")
