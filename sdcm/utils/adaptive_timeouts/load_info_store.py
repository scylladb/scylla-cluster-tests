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
import logging
import time
import uuid
from collections import defaultdict
from datetime import datetime
from functools import cached_property
import re
from typing import Any

import yaml
from cachetools import cached, TTLCache

from sdcm.es import ES
from sdcm.remote import RemoteCmdRunner
from sdcm.test_config import TestConfig
from sdcm.utils.decorators import retrying
from sdcm.utils.metaclasses import Singleton

LOGGER = logging.getLogger(__name__)


def convert_to_mb(value) -> int:
    pattern = re.compile(r'^(\d+(\.\d+)?) *([KMGT]?B)$')
    match = pattern.match(value)
    if match:
        number = float(match.group(1))
        suffix = match.group(3)
        match suffix:
            case 'KB':
                number /= 1024
            case 'GB':
                number *= 1024
            case 'TB':
                number *= 1024 * 1024
            case 'PB':
                number *= 1024 * 1024 * 1024
        return int(number)
    raise ValueError(f"Couldn't parse value {value} to MB")


class NodeLoadInfoService:
    """
    Service to get information about node load through running commands on node like getting metrics from localhost:9180/9100,
    nodetool status, uptime (load), vmstat (disk utilization). Command responses are cached for some time to avoid too much requests.
    """

    def __init__(self, remoter: RemoteCmdRunner, name: str, scylla_version: str):
        self.remoter = remoter
        self._name = name
        self._scylla_version = scylla_version

    @cached_property
    def _io_properties(self):
        return yaml.safe_load(self.remoter.run('cat /etc/scylla.d/io_properties.yaml', verbose=False).stdout)

    @cached(cache=TTLCache(maxsize=1024, ttl=300))
    def _cf_stats(self, keyspace):
        pass

    @cached(cache=TTLCache(maxsize=1024, ttl=300))
    def _get_nodetool_info(self):
        return self.remoter.run('nodetool info', verbose=False).stdout

    @cached(cache=TTLCache(maxsize=1024, ttl=60))
    def _get_node_load(self) -> tuple[float, float, float]:
        try:
            metrics = self._get_node_exporter_metrics()
            load_1 = float(metrics['node_load1'])
            load_5 = float(metrics['node_load5'])
            load_15 = float(metrics['node_load15'])
            return load_1, load_5, load_15
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.debug("Couldn't get node load from prometheus metrics. Error: %s", exc)
            # fallback to uptime
            load_1, load_5, load_15 = self.remoter.run('uptime').stdout.split("load average: ")[1].split(",")
            return float(load_1), float(load_5), float(load_15)

    def get_node_boot_time_seconds(self) -> float:
        metrics = self._get_node_exporter_metrics()
        mem_available = float(metrics['node_boot_time_seconds'])
        return mem_available

    @retrying(n=5, sleep_time=1, allowed_exceptions=(ValueError,))
    def _get_metrics(self, port):
        metrics = self.remoter.run(f'curl -s localhost:{port}/metrics', verbose=False).stdout
        metrics_dict = {}
        for line in metrics.splitlines():
            if line and not line.startswith('#'):
                try:
                    key, value = line.rsplit(' ', 1)
                    metrics_dict[key] = value
                except ValueError:
                    LOGGER.debug("Couldn't parse line: %s", line)
        return metrics_dict

    @cached(cache=TTLCache(maxsize=1024, ttl=60))
    def _get_scylla_metrics(self):
        return self._get_metrics(port=9180)

    @cached(cache=TTLCache(maxsize=1024, ttl=60))
    def _get_node_exporter_metrics(self):
        return self._get_metrics(port=9100)

    @property
    def node_data_size_mb(self) -> int:
        for line in self._get_nodetool_info().splitlines():
            if line.startswith('Load'):
                return convert_to_mb(line.split(':')[1].strip())
        raise ValueError("Couldn't find Load in nodetool info response")

    @property
    def cpu_load_5(self) -> float:
        return self._get_node_load()[1]

    @cached_property
    def shards_count(self) -> int:
        return len([key for key in self._get_scylla_metrics() if key.startswith('scylla_lsa_free_space')])

    @cached_property
    def scheduler_regex(self) -> re.compile:
        return re.compile(r".*group=\"(?P<group>.*)\",shard=\"(?P<shard>\d+)")

    def scylla_scheduler_shares(self) -> dict:
        """
        output example: {"sl:sl200": [200, 200], "sl:default": [1000, 1000]}
        """
        scheduler_group_shares = defaultdict(list)
        all_metrics = self._get_metrics(port=9180)
        for key, value in all_metrics.items():
            if key.startswith('scylla_scheduler_shares'):
                try:
                    match = self.scheduler_regex.match(key)
                    try:
                        scheduler_group_shares[match.groups()[0]].append(int(value.split(".")[0]))
                    except ValueError as details:
                        LOGGER.error("Failed to to convert value %s to integer. Error: %s", value, details)
                except AttributeError as error:
                    LOGGER.error("Failed to match metric: %s. Error: %s", key, error)

        return scheduler_group_shares

    @cached_property
    def read_bandwidth_mb(self) -> float:
        """based on io_properties.yaml in MB/s """
        return self._io_properties["disks"][0]["read_bandwidth"] / 1024 / 1024

    @cached_property
    def write_bandwidth_mb(self) -> float:
        """based on io_properties.yaml in MB/s"""
        return self._io_properties["disks"][0]["write_bandwidth"] / 1024 / 1024

    @cached_property
    def read_iops(self) -> float:
        return self._io_properties["disks"][0]["read_iops"]

    @cached_property
    def write_iops(self) -> float:
        return self._io_properties["disks"][0]["write_iops"]

    def as_dict(self):
        return {
            "node_name": self._name,
            "cpu_load_5": self.cpu_load_5,
            "shards_count": self.shards_count,
            "read_bandwidth_mb": self.read_bandwidth_mb,
            "write_bandwidth_mb": self.write_bandwidth_mb,
            "read_iops": self.read_iops,
            "write_iops": self.write_iops,
            "node_data_size_mb": self.node_data_size_mb,
            "scylla_version": self._scylla_version,
        }


class AdaptiveTimeoutStore(metaclass=Singleton):
    """Class for storing metrics and other info related to adaptive timeouts.

    Used for future reference/node operations time tracking and calculations optimization."""

    # pylint: disable=too-many-arguments
    def store(self, metrics: dict[str, Any], operation: str, duration: int | float, timeout: int,
              timeout_occurred: bool) -> None:
        pass

    def get(self, operation: str | None, timeout_occurred: bool = False):
        pass


class ESAdaptiveTimeoutStore(AdaptiveTimeoutStore):
    """AdaptiveTimeoutStore implementation backed by Elasticsearch."""

    def __init__(self):
        self._index = "sct-adaptive-timeouts"

    @cached_property
    def _es(self):
        return ES()

    # pylint: disable=too-many-arguments
    def store(self, metrics: dict[str, Any], operation: str, duration: float, timeout: float,
              timeout_occurred: bool):
        body = metrics
        body["test_id"] = TestConfig.test_id()
        body["end_time"] = datetime.utcfromtimestamp(time.time())
        body["operation"] = operation
        body["duration"] = duration
        body["timeout"] = timeout
        body["timeout_occurred"] = timeout_occurred
        load_id = str(uuid.uuid4())
        LOGGER.debug("Storing adaptive timeout info: %s=%s", load_id, body)
        if not self._es:
            LOGGER.debug("ESAdaptiveTimeoutStore is not initialized, skipping store")
            return
        # pylint: disable=unexpected-keyword-arg,no-value-for-parameter
        self._es.create(index=self._index, id=load_id, document=body)

    def get(self, operation: str | None, timeout_occurred: bool | None = None):
        """Get adaptive timeout info from ES.

        Example usage:
            >>> from sdcm.utils.adaptive_timeouts.load_info_store import ESAdaptiveTimeoutStore
            >>> from sdcm.utils.adaptive_timeouts import Operations
            >>> ESAdaptiveTimeoutStore().get(operation=Operations.DECOMMISSION.name)
        """
        if not self._es:
            return []
        filters = []
        if operation is not None:
            filters.append({"match": {"operation": operation}})
        if timeout_occurred is not None:
            filters.append({"match": {"timeout_occurred": timeout_occurred}})
        query = {
            "query": {
                "bool": {
                    "must": filters
                }
            }
        }
        res = self._es.search(index=self._index, body=query)
        return [hit["_source"] for hit in res["hits"]["hits"]]


class NodeLoadInfoServices(metaclass=Singleton):  # pylint: disable=too-few-public-methods
    """Cache for NodeLoadInfoService instances."""

    def __init__(self):
        self._services: dict[str, NodeLoadInfoService] = {}

    def get(self, node: "BaseNode") -> NodeLoadInfoService:  # noqa: F821
        if node not in self._services:
            self._services[node.name] = NodeLoadInfoService(node.remoter, node.name, node.scylla_version_detailed)
        if self._services[node.name].remoter != node.remoter:
            self._services[node.name] = NodeLoadInfoService(node.remoter, node.name, node.scylla_version_detailed)
        return self._services[node.name]
