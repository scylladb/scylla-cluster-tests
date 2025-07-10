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
import logging
from collections import defaultdict
from functools import partial

from sdcm.utils.common import ParallelObject


LOGGER = logging.getLogger(__name__)


def group_nodes_by_dc_idx(nodes: list['BaseNode']) -> dict[int, list['BaseNode']]:  # noqa: F821
    """ Group nodes by dc_idx """
    nodes_by_dc_idx = defaultdict(list)
    for node in nodes:
        nodes_by_dc_idx[node.dc_idx].append(node)
    return nodes_by_dc_idx


def flush_nodes(cluster, keyspace: str):
    LOGGER.debug("Run a flush on cluster data nodes")
    triggers = [partial(node.run_nodetool, sub_cmd=f"flush -- {keyspace}", )
                for node in cluster.data_nodes]
    ParallelObject(objects=triggers, timeout=1200).call_objects()


def major_compaction_nodes(cluster, keyspace: str, table: str):
    LOGGER.debug("Run a major compaction on cluster data nodes")
    triggers = [partial(node.run_nodetool, sub_cmd="compact", args=f"{keyspace} {table}", ) for
                node in cluster.data_nodes]
    ParallelObject(objects=triggers, timeout=3000).call_objects()


def clear_snapshot_nodes(cluster):
    LOGGER.debug("Run a clear-snapshot command on cluster data nodes")
    triggers = [partial(node.run_nodetool, sub_cmd="clearsnapshot", )
                for node in cluster.data_nodes]
    ParallelObject(objects=triggers, timeout=1200).call_objects()
