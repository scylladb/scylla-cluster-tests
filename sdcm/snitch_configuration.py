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
# Copyright (c) 2023 ScyllaDB

import re
from typing import List


class SnitchConfig:
    """Keeps all cassandra-rackdc.properties settings and function to apply them"""

    def __init__(self, node: "sdcm.cluster.BaseNode", datacenters: List[str]):  # noqa: F821
        self._node = node
        self._is_multi_dc = len(datacenters) > 1
        self._rack = f"RACK{node.rack}"
        self._datacenter = datacenters[node.dc_idx]
        self._dc_prefix = self._get_dc_prefix()
        self._dc_suffix = self._get_dc_suffix()

    def _get_dc_prefix(self) -> str:
        if self._datacenter.endswith("-1"):
            # from files in the repo we can see -1 is dropped, but docs don't mention it and sometimes there are discrepancies in repo
            return self._datacenter[:-2]
        return self._datacenter

    def _get_dc_suffix(self) -> str:
        if self._is_multi_dc:
            ret = re.findall('-([a-z]+).*-', self._datacenter)
            if ret:
                dc_suffix = 'scylla_node_{}'.format(ret[0])
            else:
                dc_suffix = self._dc_prefix.replace('-', '_')
            return dc_suffix
        else:
            return ""

    def apply(self) -> None:
        """
        Apply snitch configuration to the node (only in cassandra-rackdc.properties, scylla.yaml must be updated separately.).

        Returns `True` if require Scylla restart to make the effect.
        """
        properties = {
            'dc': self._datacenter,
            'rack': self._rack,
            'prefer_local': 'true',
        }
        if self._dc_suffix:
            properties['dc_suffix'] = self._dc_suffix

        with self._node.remote_cassandra_rackdc_properties() as properties_file:
            for key, value in properties.items():
                # in case when property is already set (e.g. it is forced by AddRemoveDc nemesis), skip setting it
                properties_file.setdefault(key, value)
