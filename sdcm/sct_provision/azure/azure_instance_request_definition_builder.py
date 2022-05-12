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
from dataclasses import dataclass
from typing import List, Dict

from sdcm.cluster import DEFAULT_USER_PREFIX
from sdcm.keystore import KeyStore
from sdcm.provision.provisioner import InstanceDefinition
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_provision.instances_request_definition_builder import InstancesRequest, NodeTypeType
from sdcm.test_config import TestConfig


@dataclass
class ConfigParamsMap:
    """Maps basic params to sct configuration parameters in yaml files."""
    image_id: str
    type: str
    user_name: str
    root_disk_size: str


db_map = ConfigParamsMap(image_id="azure_image_db",
                         type="azure_instance_type_db",
                         user_name="azure_image_username",
                         root_disk_size="azure_root_disk_size_db")

loader_map = ConfigParamsMap(image_id="azure_image_loader",
                             type="azure_instance_type_loader",
                             user_name="ami_loader_user",
                             root_disk_size="azure_root_disk_size_loader")

monitor_map = ConfigParamsMap(image_id="azure_image_monitor",
                              type="azure_instance_type_monitor",
                              user_name="ami_monitor_user",
                              root_disk_size="azure_root_disk_size_monitor")

mappers: Dict[str, ConfigParamsMap] = {"scylla-db": db_map,
                                       "loader": loader_map,
                                       "monitor": monitor_map}


def get_node_count_for_each_region(sct_config: SCTConfiguration, n_str: str) -> List[int]:
    """generates node count for each region from configuration parameter string (e.g. n_db_nodes).
    When parameter string has less regions defined than regions, fills with zero for each missing region.

    E.g. regions: 'eastus westus centralus' and n_db_nodes: '2 1' - will generate [2, 1, 0] list"""
    regions = sct_config.get("azure_region_name")
    region_count = len(regions)
    return ([int(v) for v in str(n_str).split()] + [0] * region_count)[:region_count]


def generate_instance_definition(sct_config: SCTConfiguration,
                                 node_type: NodeTypeType, region: str,
                                 index: int) -> InstanceDefinition:
    """Generates parameters for InstanceDefinition based on node_type and SCT Configuration."""
    user_prefix = sct_config.get('user_prefix') or DEFAULT_USER_PREFIX
    common_tags = TestConfig.common_tags()
    node_type_short = "db" if "db" in node_type else node_type
    name = f"{user_prefix}-{node_type_short}-node-{region}-{index}".lower()
    action = sct_config.get(f"post_behavior_{node_type_short}_nodes")
    tags = common_tags | {"NodeType": node_type,
                          "keep_action": "terminate" if action == "destroy" else "",
                          "NodeIndex": str(index)}
    ssh_key = KeyStore().get_gce_ssh_key_pair()
    mapper = mappers[node_type]
    return InstanceDefinition(name=name,
                              image_id=sct_config.get(mapper.image_id),
                              type=sct_config.get(mapper.type),
                              user_name=sct_config.get(mapper.user_name),
                              root_disk_size=sct_config.get(mapper.root_disk_size),
                              tags=tags,
                              ssh_key=ssh_key)


def azure_instance_request_builder(sct_config: SCTConfiguration) -> List[InstancesRequest]:
    """Generates all information needed to create instances for given test based on SCT configuration."""
    requests = []
    n_db_nodes = get_node_count_for_each_region(sct_config, str(sct_config.get("n_db_nodes")))
    n_loader_nodes = get_node_count_for_each_region(sct_config, str(sct_config.get("n_loaders")))
    n_monitor_nodes = get_node_count_for_each_region(sct_config, str(sct_config.get("n_monitor_nodes")))

    for region, db_nodes, loader_nodes, monitor_nodes in zip(
            sct_config.get("azure_region_name"), n_db_nodes, n_loader_nodes, n_monitor_nodes):
        definitions = []
        for idx in range(db_nodes):
            definitions.append(
                generate_instance_definition(sct_config=sct_config, node_type="scylla-db", region=region, index=idx+1)
            )

        for idx in range(loader_nodes):
            definitions.append(
                generate_instance_definition(sct_config=sct_config, node_type="loader", region=region, index=idx+1)
            )

        for idx in range(monitor_nodes):
            definitions.append(
                generate_instance_definition(sct_config=sct_config, node_type="monitor", region=region, index=idx+1)
            )

        requests.append(
            InstancesRequest(backend="azure", test_id=sct_config.get("test_id"), region=region, definitions=definitions)
        )
    return requests
