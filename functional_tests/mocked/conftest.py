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

import socket

import pytest

from sdcm.utils.aws_region import AwsRegion


AWS_REGION = "us-east-1"


@pytest.fixture(scope="session", autouse=True)
def check_aws_mock() -> None:
    try:
        aws_mock_ip = socket.gethostbyname("aws-mock.itself")
    except socket.gaierror:
        pytest.exit("Unable to resolve aws_mock IP. Probably, it's not running")
    ec2_endpoint = f"ec2.{AWS_REGION}.amazonaws.com"
    try:
        ec2_endpoint_ip = socket.gethostbyname(ec2_endpoint)
    except socket.gaierror:
        pytest.exit(f"Unable to resolve {ec2_endpoint}")
    if ec2_endpoint_ip != aws_mock_ip:
        pytest.exit(f"{AWS_REGION} not mocked by the running aws_mock")


@pytest.fixture(scope="session")
def aws_region() -> AwsRegion:
    return AwsRegion(region_name=AWS_REGION)
