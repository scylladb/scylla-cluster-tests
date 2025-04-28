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
# pylint: disable-all
import logging
import time
import uuid
from typing import Any
from unittest import mock

import pytest
from invoke import Result

from sdcm.remote import RemoteCmdRunnerBase
from sdcm.utils.adaptive_timeouts.load_info_store import AdaptiveTimeoutStore
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout, TABLETS_HARD_TIMEOUT
from unit_tests.test_cluster import DummyDbCluster

LOGGER = logging.getLogger(__name__)


class FakeNode():

    def __init__(self, name: str, remoter):
        self.name = name
        self.remoter = remoter
        self.scylla_version_detailed = "2042.1.12-0.20220620.e23889f17"
        self.parent_cluster = DummyDbCluster(nodes=[self], params={'n_db_nodes': 1})


class MemoryAdaptiveTimeoutStore(AdaptiveTimeoutStore):

    def __init__(self):
        self._data = {}

    def store(self, metrics: dict[str, Any], operation: str, duration: int, timeout: int,
              timeout_occurred: bool) -> None:
        metrics.update({
            "operation": operation,
            "duration": duration,
            "timeout": timeout,
            "timeout_occurred": timeout_occurred
        })
        key = str(uuid.uuid4())
        self._data[key] = metrics

    def get(self, operation: str | None, timeout_occurred: bool = False):
        matching_items = []
        for item in self._data.values():
            if (operation is None or item["operation"] == operation) and item["timeout_occurred"] == timeout_occurred:
                matching_items.append(item)
        return matching_items

    def clear(self):
        self._data = {}


@pytest.fixture
def adaptive_timeout_remoter(fake_remoter):
    io_properties = """
       disks:
         - mountpoint: /var/lib/scylla
           read_iops: 540317
           read_bandwidth: 1914761472
           write_iops: 300319
           write_bandwidth: 116202240
    """
    scylla_metrics = """
scylla_lsa_free_space{shard="0"} 3749052416.000000
scylla_lsa_free_space{shard="1"} 3750100992.000000
scylla_lsa_free_space{shard="2"} 3750887424.000000
    """
    nodetool_info = """
Using /etc/scylla/scylla.yaml as the config file
ID                     : aa7409d6-4129-4e6a-96f8-e571abdabe7c
Gossip active          : true
Thrift active          : true
Native Transport active: true
Load                   : 100.00 GB
Generation No          : 1678343052
Uptime (seconds)       : 35061
Heap Memory (MB)       : 40.31 / 247.50
Off Heap Memory (MB)   : 4.76
Data Center            : datacenter1
Rack                   : rack1
Exceptions             : 0
Key Cache              : entries 0, size 0 bytes, capacity 0 bytes, 0 hits, 0 requests, 0.000 recent hit rate, 0 save period in seconds
Row Cache              : entries 72, size 72 bytes, capacity 477.08 KiB, 4402 hits, 5043 requests, 0.873 recent hit rate, 0 save period in seconds
Counter Cache          : entries 0, size 0 bytes, capacity 0 bytes, 0 hits, 0 requests, 0.000 recent hit rate, 0 save period in seconds
Percent Repaired       : 0.0%
Token                  : (invoke with -T/--tokens to see all 256 tokens)
"""
    fake_remoter.result_map = {
        r"cat /etc/scylla.d/io_properties.yaml": Result(stdout=io_properties, stderr="", exited=0),
        r"curl -s localhost:9180/metrics": Result(stdout=scylla_metrics, exited=0),
        r"nodetool info": Result(stdout=nodetool_info, exited=0),
        r"uptime": Result(stdout=" 10:00:00 up 1 day,  1:00,  1 user,  load average: 1.20, 2.30, 1.60", exited=0),
    }
    return RemoteCmdRunnerBase.create_remoter("test-node-host")


@pytest.fixture
def fake_node(adaptive_timeout_remoter):
    return FakeNode("test-node", adaptive_timeout_remoter)


@pytest.fixture
def adaptive_timeout_store():
    store = MemoryAdaptiveTimeoutStore()
    store.clear()
    return store


@pytest.fixture(autouse=True)
def mock_tablets_feature():
    with mock.patch('sdcm.utils.adaptive_timeouts.is_tablets_feature_enabled') as mock_feature:
        mock_feature.return_value = False
        yield mock_feature


@mock.patch('sdcm.sct_events.base.SctEvent.publish_or_dump')
def test_soft_timeout_is_raised_when_timeout_reached(publish_or_dump, fake_node, adaptive_timeout_store):
    with adaptive_timeout(operation=Operations.SOFT_TIMEOUT, node=fake_node, timeout=0.1, stats_storage=adaptive_timeout_store) as timeout:
        assert timeout == 0.1
        time.sleep(0.2)
    publish_or_dump.assert_called_once()
    metrics = MemoryAdaptiveTimeoutStore().get(operation=Operations.SOFT_TIMEOUT.name, timeout_occurred=True)
    assert metrics[0]["duration"] > 0.2
    assert metrics[0]["timeout"] == 0.1
    assert metrics[0]["timeout_occurred"] is True
    assert metrics[0]["operation"] == "SOFT_TIMEOUT"
    assert metrics[0]["node_name"] == "test-node"
    assert metrics[0]["shards_count"] == 3


@mock.patch('sdcm.sct_events.base.SctEvent.publish_or_dump')
def test_soft_timeout_is_not_raised_when_timeout_not_reached(publish_or_dump, fake_node, adaptive_timeout_store):
    with adaptive_timeout(operation=Operations.SOFT_TIMEOUT, node=fake_node, timeout=1, stats_storage=adaptive_timeout_store) as timeout:
        assert timeout == 1
        time.sleep(0.2)
    publish_or_dump.assert_not_called()
    # still we store the metrics even when there's no timeout
    assert MemoryAdaptiveTimeoutStore().get(operation="SOFT_TIMEOUT", timeout_occurred=False)


@mock.patch('sdcm.sct_events.base.SctEvent.publish_or_dump')
def test_decommission_timeout_is_calculated_and_stored(publish_or_dump, fake_node, adaptive_timeout_store):
    with adaptive_timeout(operation=Operations.DECOMMISSION, node=fake_node, stats_storage=adaptive_timeout_store) as timeout:
        assert timeout == 7200  # based on data size
    publish_or_dump.assert_not_called()
    assert MemoryAdaptiveTimeoutStore().get(operation=Operations.DECOMMISSION.name, timeout_occurred=False)


@mock.patch('sdcm.sct_events.system.SoftTimeoutEvent.publish_or_dump')
@mock.patch('sdcm.sct_events.system.HardTimeoutEvent.publish_or_dump')
def test_tablets_decommission_uses_predefined_timeouts(hard_timeout_mock, soft_timeout_mock,
                                                       fake_node, adaptive_timeout_store, mock_tablets_feature):
    mock_tablets_feature.return_value = True
    with adaptive_timeout(operation=Operations.DECOMMISSION, node=fake_node,
                          stats_storage=adaptive_timeout_store) as timeout:
        assert timeout == TABLETS_HARD_TIMEOUT

    soft_timeout_mock.assert_not_called()
    hard_timeout_mock.assert_not_called()
    metrics = adaptive_timeout_store.get(operation=Operations.DECOMMISSION.name)
    assert metrics[0].get("tablets_enabled") is True


@mock.patch('sdcm.sct_events.system.SoftTimeoutEvent.publish_or_dump')
@mock.patch('sdcm.sct_events.system.HardTimeoutEvent.publish_or_dump')
def test_tablets_new_node_uses_predefined_timeouts(hard_timeout_mock, soft_timeout_mock,
                                                   fake_node, adaptive_timeout_store, mock_tablets_feature):
    mock_tablets_feature.return_value = True
    with adaptive_timeout(operation=Operations.NEW_NODE, node=fake_node, stats_storage=adaptive_timeout_store,
                          timeout=9999) as timeout:
        assert timeout == TABLETS_HARD_TIMEOUT

    soft_timeout_mock.assert_not_called()
    hard_timeout_mock.assert_not_called()
    metrics = adaptive_timeout_store.get(operation=Operations.NEW_NODE.name)
    assert metrics[0].get("tablets_enabled") is True
