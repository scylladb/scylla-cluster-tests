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

Kept in its own leaf module, and deliberately free of imports: pricing lookups need it,
and pulling it from `sdcm.utils.cloud_monitor.common` made `cloud_catalog.pricing`
unimportable on its own — `cloud_monitor/__init__.py` eagerly imports the whole monitor
stack, which imports back into `cloud_catalog.pricing`.
"""

from enum import Enum


class InstanceLifecycle(Enum):
    ON_DEMAND = "on-demand"
    SPOT = "spot"
