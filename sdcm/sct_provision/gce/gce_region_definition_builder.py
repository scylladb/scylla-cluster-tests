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

"""GCE region definition builder for SCT provisioning."""

from typing import Dict, List

from sdcm.sct_provision.common.types import NodeTypeType
from sdcm.sct_provision.region_definition_builder import ConfigParamsMap, DefinitionBuilder

# Configuration parameter mappings for different node types
db_map = ConfigParamsMap(
    image_id="gce_image_db",
    type="gce_instance_type_db",
    user_name="gce_image_username",
    root_disk_size="root_disk_size_db",
)

loader_map = ConfigParamsMap(
    image_id="gce_image_loader",
    type="gce_instance_type_loader",
    user_name="gce_image_username",
    root_disk_size="root_disk_size_loader",
)

monitor_map = ConfigParamsMap(
    image_id="gce_image_monitor",
    type="gce_instance_type_monitor",
    user_name="gce_image_username",
    root_disk_size="root_disk_size_monitor",
)

mapper: Dict[NodeTypeType, ConfigParamsMap] = {
    "scylla-db": db_map,
    "loader": loader_map,
    "monitor": monitor_map,
}


class GceDefinitionBuilder(DefinitionBuilder):
    """Definition builder for GCE backend."""

    BACKEND = "gce"
    SCT_PARAM_MAPPER = mapper
    REGION_MAP = "gce_datacenter"

    @property
    def regions(self) -> List[str]:
        """Return regions as a list, ensuring gce_datacenter is always treated as a list."""
        gce_datacenter = self.params.get(self.REGION_MAP)
        if isinstance(gce_datacenter, list):
            return gce_datacenter
        return [gce_datacenter]
