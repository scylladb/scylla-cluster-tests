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

from functools import cached_property

from cachetools import cached

from sdcm.remote import RemoteCmdRunner


class NodeLoadInfoService:
    """
    Service to get information about node load through running commands on node like getting metrics from localhost:9180/9100,
    nodetool status, uptime (load), vmstat (disk utilization). Command responses are cached for some time to avoid too much requests.
    Available as @cached_property of BaseNode e.g. node.load_info_service
    """

    def __init__(self, remoter: RemoteCmdRunner):
        self._remoter = remoter

    @cached_property
    def _io_properties_yaml(self):
        pass

    @cached_property  # with ttl
    def _metrics(self):
        """we can get metrics from localhost:9180/9100 and use them to calculate load/sizes"""

    @cached  # with ttl
    def _cf_stats(self, keyspace):
        pass

    @cached_property  # with ttl
    def _nodetool_info(self):
        pass

    @property
    def node_data_size(self) -> int:
        pass

    @property
    def data_size(self) -> float:
        """based on _nodetool_info"""

    @property
    def load_factor(self) -> float:
        """In case we want to take into account current cpu/disk load"""

    @property
    def shards_count(self) -> int:
        pass

    def read_bandwidth(self, remoter: RemoteCmdRunner) -> float:
        """based on io_properties.yaml"""

    def write_bandwidth(self, remoter: RemoteCmdRunner) -> float:
        """based on io_properties.yaml"""

    def read_iops(self, remoter: RemoteCmdRunner) -> float:
        """based on io_properties.yaml"""

    def write_iops(self, remoter: RemoteCmdRunner) -> float:
        """based on io_properties.yaml"""


class AdaptiveTimeout:
    """
    Base class for adaptive timeout - timeout relative to data size, node performance (and maybe current load?)
    using classmethod to have easy discoverable api like: AdaptiveTimeout.resharding(node.load_info_service)
    """
    @classmethod
    def resharding(cls, nlis: NodeLoadInfoService) -> int:
        """Gets resharding timeout for a given node and keyspace"""
        base_timeout = nlis.data_size / nlis.shards_count / nlis.write_bandwidth
        return base_timeout * 1.5  # TODO: define factor it based on historical resharding time to tune out timeout, make some margin

    @classmethod
    def flush(cls, nis: NodeLoadInfoService):
        """should depend on memtable data size, pending flushes(?) and disk write speed"""
