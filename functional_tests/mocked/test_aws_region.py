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

from __future__ import annotations

import typing

if typing.TYPE_CHECKING:
    from sdcm.utils.aws_region import AwsRegion


def test_prepare_region(aws_region: AwsRegion) -> None:
    aws_region.configure()
