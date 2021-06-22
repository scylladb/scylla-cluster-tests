import logging
import random
import sys
import tempfile
from enum import Enum
from functools import lru_cache
from math import ceil
from textwrap import dedent
from typing import Optional

import boto3

from sdcm.keystore import KeyStore
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.utils.aws_utils import ec2_instance_wait_public_ip, ec2_ami_get_root_device_name
from sdcm.utils.get_username import get_username
from sdcm.utils.prepare_region import AwsRegion


LOGGER = logging.getLogger(__name__)


class ImageType(Enum):
    source = "source"
    general = "general"


class SctRunner:
    """Provisions and configures the SCT runner"""
    VERSION = 1.4  # Version of the Image
    IMAGE_NAME = f"sct-runner-{VERSION}"
    NODE_TYPE = "sct-runner"
    RUNNER_NAME = "SCT-Runner"
    BASE_IMAGE_ID = "ami-0ffac660dd0cb2973"  # ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-20200609
    SOURCE_IMAGE_REGION = "eu-west-2"  # where the source Runner image will be created and copied to other regions
    LOGIN_USER = "ubuntu"
    IMAGE_DESCRIPTION = "SCT runner image"

    def __init__(self, region_name: str):
        self.region_name = region_name
        self.ec2_client = boto3.client("ec2", region_name=region_name)
        self.ec2_client_source = boto3.client("ec2", region_name=self.SOURCE_IMAGE_REGION)
        self.ec2_resource = boto3.resource("ec2", region_name=region_name)
        self.ec2_resource_source = boto3.resource("ec2", region_name=self.SOURCE_IMAGE_REGION)
        self._ssh_pkey_file = None

    @staticmethod
    def instance_type(test_duration):
        if test_duration > 7 * 60:
            return "r5.large"
        return "t3.large"  # has 7h 12m CPU burst

    @staticmethod
    def instance_root_disk_size(test_duration):
        disk_size = 80  # GB
        if test_duration and test_duration > 3 * 24 * 60:  # 3 days
            # add 20G more space for jobs which test_duration is longer than 3 days
            return disk_size + 20
        return disk_size

    @staticmethod
    @lru_cache(maxsize=None)
    def key_pair():
        ks = KeyStore()
        return ks.get_ec2_ssh_key_pair()

    def get_remoter(self, host, connect_timeout: Optional[float] = None):
        self._ssh_pkey_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
        self._ssh_pkey_file.write(self.key_pair().private_key.decode())
        self._ssh_pkey_file.flush()
        return RemoteCmdRunnerBase.create_remoter(hostname=host, user=self.LOGIN_USER, key_file=self._ssh_pkey_file.name, connect_timeout=connect_timeout)

    def install_prereqs(self, public_ip):
        LOGGER.info("Installing required packages...")
        prereqs_script = dedent(f"""
            echo "fs.aio-max-nr = 65536" >> /etc/sysctl.conf
            echo "ubuntu soft nofile 4096" >> /etc/security/limits.conf
            echo "jenkins soft nofile 4096" >> /etc/security/limits.conf
            echo "root soft nofile 4096" >> /etc/security/limits.conf
            mkdir -p -m 777 /home/ubuntu/sct-results
            chown ubuntu:ubuntu /home/ubuntu/sct-results
            echo "cd ~/sct-results" >> /home/ubuntu/.bashrc
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
            # configure Jenkins user
            apt install -y openjdk-14-jre-headless
            adduser --disabled-password --gecos "" jenkins
            usermod -aG docker jenkins
            mkdir -p /home/jenkins/.ssh
            echo "{self.key_pair().public_key.decode()}" > /home/jenkins/.ssh/authorized_keys
            chmod 600 /home/jenkins/.ssh/authorized_keys
            chown -R jenkins:jenkins /home/jenkins
            echo "jenkins ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/jenkins
            # Jenkins pipelines run /bin/sh for some reason
            unlink /bin/sh
            ln -s /bin/bash /bin/sh
        """)
        remoter = self.get_remoter(host=public_ip)
        result = remoter.run(f"sudo bash -cxe '{prereqs_script}'", ignore_status=True)

        remoter.stop()
        if result.exit_status == 0:
            LOGGER.info("All packages successfully installed.")
        else:
            raise Exception("Unable to install required packages:\n%s" % (result.stdout + result.stderr))

    def _image(self, image_type=ImageType.source):
        if image_type == ImageType.source:
            client = self.ec2_client_source
        elif image_type == ImageType.general:
            client = self.ec2_client
        else:
            raise ValueError("Unknown Image type")
        amis = client.describe_images(Owners=["self"],
                                      Filters=[{"Name": "tag:Name", "Values": [self.IMAGE_NAME]},
                                               {"Name": "tag:Version", "Values": [str(self.VERSION)]}])
        LOGGER.debug(f"Found SCT Runner AMIs: {amis}")
        existing_amis = amis.get("Images", [])
        if len(existing_amis) == 0:
            return None
        assert len(existing_amis) == 1, \
            f"More than 1 SCT Runner AMI with {self.IMAGE_NAME}:{self.VERSION} " \
            f"found in {self.region_name}: {existing_amis}"
        return self.ec2_resource.Image(existing_amis[0]["ImageId"])  # pylint: disable=no-member

    @property
    def source_image(self):
        return self._image(image_type=ImageType.source)

    @property
    def image(self):
        return self._image(image_type=ImageType.general)

    def _create_instance(self, instance_type, image_id, tags_list, region_az="", test_duration=None):
        region = region_az[:-1]
        aws_region = AwsRegion(region_name=region)
        region_az = region_az if region_az else random.choice(aws_region.availability_zones)
        subnet = aws_region.sct_subnet(region_az=region_az)
        assert subnet, f"No SCT subnet found in the source region. " \
                       f"Use hydra prepare-region --region-name '{self.region_name}' to create cloud env!"
        LOGGER.info("Creating instance...")
        ec2_resource = boto3.resource("ec2", region_name=region)
        result = ec2_resource.create_instances(
            ImageId=image_id,
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
                "DeviceName": ec2_ami_get_root_device_name(image_id=image_id, region=region),
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

    def tag_image(self, image_id, image_type):
        if image_type == ImageType.source:
            ec2_resource = self.ec2_resource_source
        elif image_type == ImageType.general:
            ec2_resource = self.ec2_resource
        image_tags = [
            {"Key": "Name", "Value": self.IMAGE_NAME},
            {"Key": "Version", "Value": str(self.VERSION)},
        ]
        runer_image = ec2_resource.Image(image_id)
        runer_image.wait_until_exists()
        LOGGER.info("Image '%s' exists and ready. Tagging...", image_id)
        runer_image.create_tags(Tags=image_tags)
        LOGGER.info("Tagging completed.")
        LOGGER.info("SCT Runner image created in '%s'. Id [%s].", self.region_name, image_id)

    def create_image(self):
        """
            Create an Image for SCT Runner in specified region. If the Image exists in SOURCE_REGION
            it will be copied to the destination region.
            Warning: this can't run in parallel!
        """
        LOGGER.info(f"Looking for source SCT Runner Image in {self.SOURCE_IMAGE_REGION}...")
        source_image = self.source_image
        if not source_image:
            LOGGER.info(f"Source SCT Runner Image not found. Creating...")
            instance = self._create_instance(
                instance_type="t3.small",
                image_id=self.BASE_IMAGE_ID,
                tags_list=[{"Key": "Name", "Value": "sct-image-builder"},
                           {"Key": "keep", "Value": "1"},
                           {"Key": "keep_action", "Value": "terminate"},
                           {"Key": "Version", "Value": str(self.VERSION)},
                           ],
                region_az=self.SOURCE_IMAGE_REGION + "a"
            )
            self.install_prereqs(public_ip=instance.public_ip_address)
            LOGGER.info("Stopping the SCT Image Builder instance...")
            instance.stop()
            instance.wait_until_stopped()
            LOGGER.info("SCT Image Builder instance stopped.\nCreating image...")
            result = self.ec2_client_source.create_image(
                Description=self.IMAGE_DESCRIPTION,
                InstanceId=instance.instance_id,
                Name=self.IMAGE_NAME,
                NoReboot=False
            )
            self.tag_image(image_id=result["ImageId"], image_type=ImageType.source)
            try:
                LOGGER.info("Terminating image builder instance '%s'...", instance.instance_id)
                instance.terminate()
            except Exception as ex:  # pylint: disable=broad-except
                LOGGER.warning(f"Was not able to terminate '{instance.instance_id}': {ex}\n"
                               "Please terminate manually!!!")

        else:
            LOGGER.info(f"SCT Runner image exists in the source region '{self.SOURCE_IMAGE_REGION}'! "
                        f"ID: {source_image.image_id}")

        if self.region_name != self.SOURCE_IMAGE_REGION and self.image is None:
            LOGGER.info(f"Copying {self.IMAGE_NAME} to {self.region_name}...\nNote: It can take 5-15 minutes.")
            result = self.ec2_client.copy_image(  # pylint: disable=no-member
                Description=self.IMAGE_DESCRIPTION,
                Name=self.IMAGE_NAME,
                SourceImageId=self.source_image.image_id,
                SourceRegion=self.SOURCE_IMAGE_REGION
            )
            LOGGER.info("Image copied, id: '%s'.", result["ImageId"])
            self.tag_image(image_id=result["ImageId"], image_type=ImageType.general)
            LOGGER.info("Done.")
        else:
            LOGGER.info("No need to copy SCT Runner image since it  already exists in '%s'.", self.region_name)

    def create_instance(self, test_id: str, test_duration: int, region_az: str):
        """
            :param test_duration: used to set keep-alive flags, measured in MINUTES
        """
        LOGGER.info("Creating SCT Runner instance...")
        image = self.image
        if not image:
            LOGGER.error(f"SCT Runner image was not found in {self.region_name}! "
                         f"Use hydra create-runner-image --cloud-privider aws --region {self.region_name}")
            sys.exit(1)
        return self._create_instance(
            instance_type=self.instance_type(test_duration=test_duration),
            image_id=self.image.image_id,
            tags_list=[
                {"Key": "Name", "Value": self.RUNNER_NAME},
                {"Key": "TestId", "Value": test_id},
                {"Key": "NodeType", "Value": self.NODE_TYPE},
                {"Key": "RunByUser", "Value": get_username()},
                {"Key": "keep", "Value": str(ceil(test_duration / 60) + 6)},
                {"Key": "keep_action", "Value": "terminate"},
            ],
            region_az=region_az,
            test_duration=test_duration,
        )


if __name__ == "__main__":
    TEST_REGION = "eu-west-2"
    SCT_RUNNER = SctRunner(region_name=TEST_REGION)
    SCT_RUNNER.create_image()
    SCT_RUNNER.create_instance(test_id="byakabuka", test_duration=60, region_az=TEST_REGION)
