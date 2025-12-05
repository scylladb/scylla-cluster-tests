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
from pathlib import Path
from textwrap import dedent
from itertools import chain
from collections import namedtuple

from sdcm.utils.common import list_resources_docker
from sdcm.utils.docker_utils import ContainerManager, DockerException
from sdcm.utils.get_username import get_username


AWS_MOCK_IP_FILE = Path("aws_mock_ip")
AWS_MOCK_NODE_TYPE = "aws-mock"
AWS_MOCK_IMAGE = "scylladb/aws_mock:latest"

DEFAULT_MOCKED_HOSTS = (
    "scylla-qa-keystore.s3.amazonaws.com",
    "ec2.eu-west-2.amazonaws.com",
)

LOGGER = logging.getLogger(__name__)


class AwsMock:
    aws_mock_container_image_tag = AWS_MOCK_IMAGE

    def __init__(self, test_id: str, regions: list[str]):
        self._containers = {}
        self.tags = {
            "TestId": test_id,
            "NodeType": AWS_MOCK_NODE_TYPE,
            "RunByUser": get_username(),
        }
        self.regions = regions
        self.test_id = test_id

        # empty params, now part of ContainerManager api to allow access to SCT configuration
        self.parent_cluster = namedtuple("cluster", field_names="params")(params={})

    def aws_mock_container_run_args(self) -> dict:
        return {
            "name": f"aws_mock-{self.test_id}",
            "tty": True,
            "environment": {
                "AWS_MOCK_HOSTS": " ".join(
                    chain(
                        DEFAULT_MOCKED_HOSTS,
                        (f"ec2.{region}.amazonaws.com" for region in self.regions if region != "eu-west-2"),
                    )
                ),
            },
            "pull": True,
        }

    def run(self, force: bool = False) -> str:
        if not force and AWS_MOCK_IP_FILE.exists():
            LOGGER.warning("%s found, don't run a new container and return AWS Mock IP from it", AWS_MOCK_IP_FILE)
            return AWS_MOCK_IP_FILE.read_text(encoding="utf-8")

        container = ContainerManager.run_container(self, "aws_mock")
        res = container.exec_run(
            [
                "bash",
                "-cxe",
                dedent("""\
            mkdir -p /src/s3/scylla-qa-keystore
            ssh-keygen -q -b 2048 -t ed25519 -N "" -C aws_mock -f /src/s3/scylla-qa-keystore/scylla_test_id_ed25519
            chown -R nginx:nginx /src/s3/scylla-qa-keystore
            useradd ubuntu
            mkdir -m 700 -p /home/ubuntu/.ssh
            cp /src/s3/scylla-qa-keystore/scylla_test_id_ed25519.pub /home/ubuntu/.ssh/authorized_keys
            chown -R ubuntu:ubuntu /home/ubuntu/.ssh
        """),
            ]
        )
        if res.exit_code:
            raise DockerException(f"{container}: {res.output.decode('utf-8')}")

        aws_mock_ip = ContainerManager.get_ip_address(self, "aws_mock")
        AWS_MOCK_IP_FILE.write_text(aws_mock_ip, encoding="utf-8")

        return aws_mock_ip

    @staticmethod
    def clean(
        test_id: str | None = None, all_mocks: bool = False, verbose: bool = False, dry_run: bool = False
    ) -> None:
        filters = {"NodeType": AWS_MOCK_NODE_TYPE}
        if not all_mocks and test_id:
            filters["TestId"] = test_id
        containers = list_resources_docker(tags_dict=filters, builder_name="local", verbose=verbose).get(
            "containers", []
        )

        aws_mock_ip = None

        if not all_mocks and not test_id:  # default action: remove a container with IP address as in `aws_mock_ip' file
            if not AWS_MOCK_IP_FILE.exists():
                LOGGER.info("No AWS Mock requested to clean")
                return
            aws_mock_ip = AWS_MOCK_IP_FILE.read_text(encoding="utf-8")
            for container in containers:
                container.reload()
                if container.attrs["NetworkSettings"]["IPAddress"] == aws_mock_ip:
                    containers = [container]
                    break
            else:
                LOGGER.warning("No AWS Mock server with IP=%s found", aws_mock_ip)
                return

        for container in containers:
            if not dry_run:
                LOGGER.info("Clean AWS Mock Docker container: %s", container)
                container.remove(v=True, force=True)
            else:
                LOGGER.info("%s should be removed", container)

        if aws_mock_ip:
            AWS_MOCK_IP_FILE.unlink(missing_ok=True)
