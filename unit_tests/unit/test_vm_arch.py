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
# Copyright (c) 2026 ScyllaDB

from typing import get_args

from sdcm.utils.aws_utils import AwsArchType
from sdcm.utils.vm_arch import AWS_ARCH_TYPES


def test_aws_arch_types_match_boto():
    assert sorted(AWS_ARCH_TYPES) == sorted(get_args(AwsArchType))
