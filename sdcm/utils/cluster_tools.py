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
# Copyright (c) 2025 ScyllaDB
from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from collections import defaultdict
from functools import partial

from sdcm.utils.parallel_object import ParallelObject


if TYPE_CHECKING:
    from sdcm.cluster import BaseCluster, BaseNode, BaseMonitorSet
    from sdcm.sct_config import SCTConfiguration

LOGGER = logging.getLogger(__name__)


def group_nodes_by_dc_idx(nodes: list[BaseNode]) -> dict[int, list[BaseNode]]:  # noqa: F821
    """Group nodes by dc_idx"""
    nodes_by_dc_idx = defaultdict(list)
    for node in nodes:
        nodes_by_dc_idx[node.dc_idx].append(node)
    return nodes_by_dc_idx


def check_cluster_layout(db_cluster: BaseCluster) -> bool:  # noqa: F821
    """
    Check if the cluster layout is balanced according to the initial configuration.
    """
    nodes_by_dc_idx = group_nodes_by_dc_idx(db_cluster.nodes)
    capacity_errors_check_mode = db_cluster.params.get("capacity_errors_check_mode") or "per-initial_config"

    n_db_nodes = db_cluster.params.get("n_db_nodes")
    db_nodes_config_count = n_db_nodes if isinstance(n_db_nodes, list) else [n_db_nodes]

    if capacity_errors_check_mode == "per-initial_config":
        for dc_idx, nodes_in_dc in nodes_by_dc_idx.items():
            racks = defaultdict(int)
            for node in nodes_in_dc:
                racks[node.rack] += 1
            # Check if the number of nodes in each rack matches the initial configuration
            if len(racks) != db_cluster.racks_count:
                LOGGER.debug(f"Datacenter {dc_idx=} rack distribution: {dict(racks)}")
                return False
            try:
                current_dc_config_count = db_nodes_config_count[dc_idx]
            except IndexError:
                # if this index isn't in config, treat it as 0 nodes
                current_dc_config_count = 0
            # Check if all racks have the same number of nodes
            if current_dc_config_count != len(nodes_in_dc):
                LOGGER.debug(
                    f"Datacenter {dc_idx=} rack distribution: {dict(racks)}, config count: {current_dc_config_count}, {len(nodes_in_dc)=}"
                )
                return False
        return True
    elif capacity_errors_check_mode == "disabled":
        # If capacity errors check is disabled, we assume the cluster is balanced
        return True
    else:
        raise ValueError(
            f"Unknown capacity_errors_check_mode: {capacity_errors_check_mode}. "
            "Supported modes are: 'per-initial_config', 'disabled'."
        )


# TODO: For the following functions, consider adding a "num_workers=len(cluster.data_nodes)" parameter to ParallelObject
# to align the number of concurrent operations to cluster nodes number, since it can be higher that default workers number.
def flush_nodes(cluster, keyspace: str):
    LOGGER.debug("Run a flush on cluster data nodes")
    triggers = [
        partial(
            node.run_nodetool,
            sub_cmd=f"flush -- {keyspace}",
        )
        for node in cluster.data_nodes
    ]
    ParallelObject(objects=triggers, timeout=1200).call_objects()


def major_compaction_nodes(cluster, keyspace: str, table: str):
    LOGGER.debug("Run a major compaction on cluster data nodes")
    triggers = [
        partial(
            node.run_nodetool,
            sub_cmd="compact",
            args=f"{keyspace} {table}",
        )
        for node in cluster.data_nodes
    ]
    ParallelObject(objects=triggers, timeout=3000).call_objects()


def clear_snapshot_nodes(cluster):
    LOGGER.debug("Run a clear-snapshot command on cluster data nodes")
    triggers = [
        partial(
            node.run_nodetool,
            sub_cmd="clearsnapshot",
        )
        for node in cluster.data_nodes
    ]
    ParallelObject(objects=triggers, timeout=1200).call_objects()


def expand_cluster_heterogeneous(
    db_cluster: BaseCluster,
    monitors: BaseMonitorSet,
    params: SCTConfiguration,
) -> list[BaseNode]:
    """Add nodes with different instance types to the cluster.

    This function adds nodes using the instance type specified by 'nemesis_grow_shrink_instance_type'
    configuration parameter. The number of nodes to add is controlled by 'nemesis_add_node_cnt'.
    This is useful for creating heterogeneous clusters with mixed instance types.

    Args:
        db_cluster: The database cluster to expand.
        monitors: The monitor set to reconfigure after adding nodes.
        params: The SCT configuration parameters.

    Returns:
        List of newly added nodes.
    """
    # Import here to avoid circular imports
    from sdcm.cluster import MAX_TIME_WAIT_FOR_NEW_NODE_UP  # noqa: PLC0415
    from sdcm.utils.adaptive_timeouts import adaptive_timeout, Operations  # noqa: PLC0415

    new_nodes = db_cluster.add_nodes(
        count=params.get("nemesis_add_node_cnt"),
        instance_type=params.get("nemesis_grow_shrink_instance_type"),
        enable_auto_bootstrap=True,
        rack=None,
    )
    monitors.reconfigure_scylla_monitoring()
    up_timeout = MAX_TIME_WAIT_FOR_NEW_NODE_UP
    with adaptive_timeout(Operations.NEW_NODE, node=db_cluster.data_nodes[0], timeout=up_timeout):
        db_cluster.wait_for_init(node_list=new_nodes, timeout=up_timeout, check_node_health=False)
    db_cluster.set_seeds()
    db_cluster.update_seed_provider()
    db_cluster.wait_for_nodes_up_and_normal(nodes=new_nodes)
    return new_nodes
