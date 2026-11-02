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

# Dependency-free on purpose: sct.py builds click choices from it at import time.
from enum import Enum


class VmArch(Enum):
    X86 = "x86_64"
    ARM = "aarch64"

    def __str__(self):
        return self.value


# AWS's own names, the same as `AwsArchType` (mypy_boto3_ec2's ArchitectureTypeType). Importing that package
# costs ~0.3s, too slow for building sct.py's click choices; a unit test keeps the two in sync.
AWS_ARCH_TYPES = ("arm64", "arm64_mac", "i386", "x86_64", "x86_64_mac")
