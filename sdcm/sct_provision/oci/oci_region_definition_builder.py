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
import logging
from typing import Dict

from sdcm.utils.oci_utils import (
    get_oci_compartment_id,
    get_ubuntu_image_ocid,
)
from sdcm.sct_provision.common.types import NodeTypeType
from sdcm.sct_provision.region_definition_builder import ConfigParamsMap, DefinitionBuilder

db_map = ConfigParamsMap(
    image_id="oci_image_db",
    type="oci_instance_type_db",
    user_name="oci_image_username",
    root_disk_size="root_disk_size_db",
)

loader_map = ConfigParamsMap(
    image_id="oci_image_loader",
    type="oci_instance_type_loader",
    user_name="ami_loader_user",
    root_disk_size="root_disk_size_loader",
)

monitor_map = ConfigParamsMap(
    image_id="oci_image_monitor",
    type="oci_instance_type_monitor",
    user_name="ami_monitor_user",
    root_disk_size="root_disk_size_monitor",
)

LOGGER = logging.getLogger(__name__)
mapper: Dict[NodeTypeType, ConfigParamsMap] = {"scylla-db": db_map, "loader": loader_map, "monitor": monitor_map}


class OciDefinitionBuilder(DefinitionBuilder):
    BACKEND = "oci"
    SCT_PARAM_MAPPER = mapper
    REGION_MAP = "oci_region_name"

    def build_instance_definition(self, region: str, node_type: NodeTypeType, index: int, instance_type: str = None):
        definition = super().build_instance_definition(region, node_type, index, instance_type)

        # NOTE: set latest ubuntu image for loader and monitor nodes if not defined
        if not definition.image_id and "db" not in node_type:
            ubuntu_version = "24.04"
            LOGGER.info("Image ID for the '%s' was not provided, using latest ubuntu-%s", node_type, ubuntu_version)
            definition.image_id = get_ubuntu_image_ocid(get_oci_compartment_id(), version=ubuntu_version)

        if "db" in node_type:
            definition.data_volume_disk_num = self.params.get("data_volume_disk_num") or 0
            definition.data_volume_disk_size = self.params.get("data_volume_disk_size")
            definition.data_volume_disk_iops = self.params.get("data_volume_disk_iops")
            definition.data_volume_disk_throughput = self.params.get("data_volume_disk_throughput")
            definition.data_volume_disk_type = self.params.get("data_volume_disk_type")
        return definition
