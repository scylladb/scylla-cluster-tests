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

"""Instance lifecycle (on-demand vs spot).

A leaf module with no imports of its own, so pricing can depend on it without dragging in
the cloud-monitor package.
"""

from enum import Enum


class InstanceLifecycle(Enum):
    ON_DEMAND = "on-demand"
    SPOT = "spot"
