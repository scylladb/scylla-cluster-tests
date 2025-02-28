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
from collections import defaultdict


def group_nodes_by_dc_idx(nodes: list['BaseNode']) -> dict[int, list['BaseNode']]:  # noqa: F821
    """ Group nodes by dc_idx """
    nodes_by_dc_idx = defaultdict(list)
    for node in nodes:
        nodes_by_dc_idx[node.dc_idx].append(node)
    return nodes_by_dc_idx
