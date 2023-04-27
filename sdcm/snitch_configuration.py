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
from typing import List, Type


class SnitchConfig:  # pylint: disable=too-few-public-methods
    """Keeps all snitch-related settings and function to apply them"""
    SNITCH_NAME = ""

    def __init__(self, node: "sdcm.cluster.BaseNode", datacenters: List[str]):
        self._node = node
        self._is_multi_dc = len(datacenters) > 1
        self._rack = node.rack
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

    def apply(self) -> bool:
        """
        Apply snitch configuration to the node (only in cassandra-rackdc.properties, scylla.yaml must be updated separately.).

        Returns `True` if require Scylla restart to make the effect.
        """
        requires_restart = False
        properties = {name: str(getattr(self, name)).lower() for name in dir(self)
                      if isinstance(getattr(self.__class__, name, None), property)}
        with self._node.remote_cassandra_rackdc_properties() as properties_file:
            if properties:
                requires_restart = True
            properties_file.update(**properties)
        return requires_restart


class GossipingPropertyFileSnitchConfig(SnitchConfig):

    @property
    def dc(self) -> str:
        return f"{self._dc_prefix}{self._dc_suffix}"

    @property
    def rack(self) -> str:
        return f"RACK{self._rack}"

    @property
    def prefer_local(self) -> bool:
        return True


class Ec2SnitchConfig(SnitchConfig):

    @property
    def dc_suffix(self):
        return self._dc_suffix


def get_snitch_config_class(params: dict) -> Type[SnitchConfig]:
    if params.get('simulated_racks') > 1:
        return GossipingPropertyFileSnitchConfig
    if params.get('cluster_backend') == 'aws':
        return Ec2SnitchConfig
    return SnitchConfig  # default, basically doing nothing
