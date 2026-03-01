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

from typing import Dict, List, Any

from sdcm.sct_provision.common.types import NodeTypeType
from sdcm.sct_provision.region_definition_builder import ConfigParamsMap, DefinitionBuilder
from sdcm.utils.gce_utils import gce_instance_name

# Configuration parameter mappings for different node types
db_map = ConfigParamsMap(
    image_id="gce_image_db",
    type="gce_instance_type_db",
    user_name="gce_image_username",
    root_disk_size="root_disk_size_db",
    local_ssd_count="gce_n_local_ssd_disk_db",
)

loader_map = ConfigParamsMap(
    image_id="gce_image_loader",
    type="gce_instance_type_loader",
    user_name="gce_image_username",
    root_disk_size="root_disk_size_loader",
    local_ssd_count="gce_n_local_ssd_disk_loader",
)

monitor_map = ConfigParamsMap(
    image_id="gce_image_monitor",
    type="gce_instance_type_monitor",
    user_name="gce_image_username",
    root_disk_size="root_disk_size_monitor",
    local_ssd_count="gce_n_local_ssd_disk_monitor",
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

    @property
    def regions(self) -> List[str]:
        """Return regions as a list, using params.gce_datacenters which properly handles str or list."""
        return self.params.gce_datacenters

    def instance_name(self, user_prefix, node_type_short, short_test_id, region, index, dc_idx: int = 0):
        """
        Generate instance name including dc_idx for multi-datacenter support.
        Uses the shared gce_instance_name utility to ensure consistency with cluster_gce.py.
        Format: {node_prefix}-{dc_idx}-{index} where node_prefix = {user_prefix}-{node_type_short}-node-{short_test_id}
        """
        node_prefix = f"{user_prefix}-{node_type_short}-node-{short_test_id}"
        return gce_instance_name(node_prefix=node_prefix, dc_idx=dc_idx, node_index=index)

    def get_provisioner_config(self) -> Dict[str, Any]:
        """Return GCE-specific provisioner configuration."""
        return {"network_name": self.params.get("gce_network")}

    def _get_node_count_for_each_region(self, n_str: str) -> List[int]:
        """Generate node count for each region from configuration parameter string.

        Overrides base class method to use self.regions property instead of REGION_MAP,
        since GCE uses gce_datacenters parameter.

        E.g. regions: 'us-east1 us-west1' and n_db_nodes: '2 1' - will generate [2, 1] list
        """
        region_count = len(self.regions)
        return ([int(v) for v in str(n_str).split()] + [0] * region_count)[:region_count]
