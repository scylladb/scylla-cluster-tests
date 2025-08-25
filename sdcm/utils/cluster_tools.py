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


if TYPE_CHECKING:
    from sdcm.cluster import BaseCluster, BaseNode

LOGGER = logging.getLogger(__name__)


def group_nodes_by_dc_idx(nodes: list[BaseNode]) -> dict[int, list[BaseNode]]:  # noqa: F821
    """ Group nodes by dc_idx """
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

    db_nodes_config_count = [int(i) for i in str(db_cluster.params.get("n_db_nodes") or 0).split(" ")]

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
                    f"Datacenter {dc_idx=} rack distribution: {dict(racks)}, config count: {current_dc_config_count}, {len(nodes_in_dc)=}")
                return False
        return True
    elif capacity_errors_check_mode == "disabled":
        # If capacity errors check is disabled, we assume the cluster is balanced
        return True
    else:
        raise ValueError(f"Unknown capacity_errors_check_mode: {capacity_errors_check_mode}. "
                         "Supported modes are: 'per-initial_config', 'disabled'.")
