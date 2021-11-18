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
# Copyright (c) 2020 ScyllaDB


import unittest
from copy import deepcopy

from sdcm.sct_events import Severity
from sdcm.utils.health_checker import check_nodes_status, check_nulls_in_peers, \
    check_node_status_in_gossip_and_nodetool_status, check_schema_version


NODES_STATUS = {
    "127.0.0.1": {"status": "UN", "dc": "datacenter1", },
    "127.0.0.2": {"status": "DN", "dc": "datacenter1", },
    "127.0.0.3": {"status": "UN", "dc": "datacenter1", },
}

PEERS_INFO = {
    '127.0.0.2': {
        'data_center': 'datacenter1',
        'host_id': 'b231fe54-8093-4d5c-9a35-b5e34dc81500',
        'rack': 'rack1',
        'release_version': '3.0.8',
        'rpc_address': '127.0.0.2',
        'schema_version': 'cbe15453-33f3-3387-aaf1-4120548f41e8',
    },
    '127.0.0.3': {
        'data_center': 'datacenter1',
        'host_id': 'e11cb4ea-a129-48aa-a9e9-7815dcd2828c',
        'rack': 'rack1',
        'release_version': '3.0.8',
        'rpc_address': '127.0.0.3',
        'schema_version': 'cbe15453-33f3-3387-aaf1-4120548f41e8',
    },
}

GOSSIP_INFO = {
    '127.0.0.1': {
        'schema': 'cbe15453-33f3-3387-aaf1-4120548f41e8',
        'status': 'NORMAL',
        'dc': 'datacenter1'
    },
    '127.0.0.2': {
        'schema': 'cbe15453-33f3-3387-aaf1-4120548f41e8',
        'status': 'shutdown',
        'dc': 'datacenter1'
    },
    '127.0.0.3': {
        'schema': 'cbe15453-33f3-3387-aaf1-4120548f41e8',
        'status': 'NORMAL',
        'dc': 'datacenter1'
    },
}


class Node:
    GOSSIP_STATUSES_FILTER_OUT = ["FILTERED", ]

    ip_address = "127.0.0.1"
    name = "node-0"
    running_nemesis = None

    @staticmethod
    def print_node_running_nemesis(node_ip):
        return ""

    @staticmethod
    def run_cqlsh(cmd, split, verbose):
        pass


class TestHealthChecker(unittest.TestCase):
    def test_check_nodes_status_no_removed(self):
        event = next(check_nodes_status(NODES_STATUS, Node), None)
        self.assertIsNotNone(event)
        self.assertEqual(event.type, "NodeStatus")
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertEqual(event.node, "node-0")
        self.assertEqual(event.message, "")
        self.assertNotEqual(event.error, "")

    def test_check_nodes_status_removed(self):
        event = next(check_nodes_status(NODES_STATUS, Node, ["127.0.0.2", ]), None)
        self.assertIsNotNone(event)
        self.assertEqual(event.type, "NodeStatus")
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertEqual(event.node, "node-0")
        self.assertEqual(event.message, "")
        self.assertNotEqual(event.error, "")

    def test_check_nulls_in_peers_no_nulls(self):
        event = next(check_nulls_in_peers(GOSSIP_INFO, PEERS_INFO, Node), None)
        self.assertIsNone(event)

    def test_check_nulls_in_peers(self):
        peers_info = deepcopy(PEERS_INFO)
        peers_info["127.0.0.2"]["data_center"] = "null"
        event = next(check_nulls_in_peers(GOSSIP_INFO, peers_info, Node), None)
        self.assertEqual(event.type, "NodePeersNulls")
        self.assertEqual(event.severity, Severity.ERROR)
        self.assertEqual(event.node, "node-0")
        self.assertEqual(event.message, "")
        self.assertNotEqual(event.error, "")

    def test_check_nulls_in_peers_filtered_status(self):
        peers_info = deepcopy(PEERS_INFO)
        peers_info["127.0.0.2"]["data_center"] = "null"
        gossip_info = deepcopy(GOSSIP_INFO)
        gossip_info["127.0.0.2"]["status"] = "FILTERED"
        event = next(check_nulls_in_peers(gossip_info, peers_info, Node), None)
        self.assertIsNone(event)

    def test_check_nulls_in_peers_not_in_gossip(self):
        peers_info = deepcopy(PEERS_INFO)
        peers_info["127.0.0.4"] = {"data_center": "null", }
        event = next(check_nulls_in_peers(GOSSIP_INFO, peers_info, Node), None)
        self.assertIsNone(event)

    def test_check_node_status_in_gossip_and_nodetool_status_all_ok(self):
        event = next(check_node_status_in_gossip_and_nodetool_status(GOSSIP_INFO, NODES_STATUS, Node), None)
        self.assertIsNone(event)

    def test_check_schema_version_all_ok(self):
        event = next(check_schema_version(GOSSIP_INFO, PEERS_INFO, NODES_STATUS, Node), None)
        self.assertIsNone(event)
