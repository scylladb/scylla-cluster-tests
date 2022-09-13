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

from sdcm.sct_events import Severity
from sdcm.utils.health_checker import (
    check_node_status_in_gossip_and_nodetool_status,
    check_nodes_status,
    check_nulls_in_peers,
    check_schema_agreement_in_gossip_and_peers,
    check_schema_version,
)


class Node:
    GOSSIP_STATUSES_FILTER_OUT = ["FILTERED", ]

    def __init__(self, ip_address, name):
        self.ip_address = ip_address
        self.name = name
        self.running_nemesis = None

    @staticmethod
    def print_node_running_nemesis(_):
        return ""

    @staticmethod
    def run_cqlsh(cmd, split, verbose):
        pass

    @staticmethod
    def get_gossip_info():
        return GOSSIP_INFO

    @staticmethod
    def get_peers_info():
        return PEERS_INFO


node1 = Node('127.0.0.1', "node-0")
node2 = Node('127.0.0.2', "node-1")
node3 = Node('127.0.0.3', "node-2")
node4 = Node('127.0.0.4', "node-3")


NODES_STATUS = {
    node1: {"status": "UN", "dc": "datacenter1", },
    node2: {"status": "DN", "dc": "datacenter1", },
    node3: {"status": "UN", "dc": "datacenter1", },
}

PEERS_INFO = {
    node2: {
        'data_center': 'datacenter1',
        'host_id': 'b231fe54-8093-4d5c-9a35-b5e34dc81500',
        'rack': 'rack1',
        'release_version': '3.0.8',
        'rpc_address': '127.0.0.2',
        'schema_version': 'cbe15453-33f3-3387-aaf1-4120548f41e8',
    },
    node3: {
        'data_center': 'datacenter1',
        'host_id': 'e11cb4ea-a129-48aa-a9e9-7815dcd2828c',
        'rack': 'rack1',
        'release_version': '3.0.8',
        'rpc_address': '127.0.0.3',
        'schema_version': 'cbe15453-33f3-3387-aaf1-4120548f41e8',
    },
}

GOSSIP_INFO = {
    node1: {
        'schema': 'cbe15453-33f3-3387-aaf1-4120548f41e8',
        'status': 'NORMAL',
        'dc': 'datacenter1'
    },
    node2: {
        'schema': 'cbe15453-33f3-3387-aaf1-4120548f41e8',
        'status': 'shutdown',
        'dc': 'datacenter1'
    },
    node3: {
        'schema': 'cbe15453-33f3-3387-aaf1-4120548f41e8',
        'status': 'NORMAL',
        'dc': 'datacenter1'
    },
}


class TestHealthChecker(unittest.TestCase):
    def test_check_nodes_status_no_removed(self):
        event = next(check_nodes_status(NODES_STATUS, node1), None)
        self.assertIsNotNone(event)
        self.assertEqual(event.type, "NodeStatus")
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertEqual(event.node, "node-0")
        self.assertEqual(event.message, "")
        self.assertNotEqual(event.error, "")

    def test_check_nodes_status_removed(self):
        event = next(check_nodes_status(NODES_STATUS, node1, [node1, ]), None)
        self.assertIsNotNone(event)
        self.assertEqual(event.type, "NodeStatus")
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertEqual(event.node, "node-0")
        self.assertEqual(event.message, "")
        self.assertNotEqual(event.error, "")

    def test_check_nulls_in_peers_no_nulls(self):
        event = next(check_nulls_in_peers(GOSSIP_INFO, PEERS_INFO, node1), None)
        self.assertIsNone(event)

    def test_check_nulls_in_peers(self):
        # Due to commit
        # https://github.com/scylladb/scylla-cluster-tests/pull/4375/commits/0d2657291b9152c92b68a893b46b146abba62be6
        # node object is used as key instead of string. "deepcopy" change the object and we can't use it
        data_center = PEERS_INFO[node2]["data_center"]
        PEERS_INFO[node2]["data_center"] = "null"
        event = next(check_nulls_in_peers(GOSSIP_INFO, PEERS_INFO, node1), None)
        self.assertEqual(event.type, "NodePeersNulls")
        self.assertEqual(event.severity, Severity.ERROR)
        self.assertEqual(event.node, "node-0")
        self.assertEqual(event.message, "")
        self.assertNotEqual(event.error, "")

        PEERS_INFO[node2]["data_center"] = data_center

    def test_check_nulls_in_peers_filtered_status(self):
        # Due to commit
        # https://github.com/scylladb/scylla-cluster-tests/pull/4375/commits/0d2657291b9152c92b68a893b46b146abba62be6
        # node object is used as key instead of string. "deepcopy" change the object and we can't use it
        data_center = PEERS_INFO[node2]["data_center"]
        PEERS_INFO[node2]["data_center"] = "null"

        gossip_status = GOSSIP_INFO[node2]["status"]
        GOSSIP_INFO[node2]["status"] = "FILTERED"

        event = next(check_nulls_in_peers(GOSSIP_INFO, PEERS_INFO, node1), None)
        self.assertIsNone(event)

        PEERS_INFO[node2]["data_center"] = data_center
        GOSSIP_INFO[node2]["status"] = gossip_status

    def test_check_nulls_in_peers_not_in_gossip(self):
        PEERS_INFO[node4] = {"data_center": "null", }
        event = next(check_nulls_in_peers(GOSSIP_INFO, PEERS_INFO, node1), None)
        self.assertIsNone(event)
        PEERS_INFO.pop(node4)

    def test_check_node_status_in_gossip_and_nodetool_status_all_ok(self):
        event = next(check_node_status_in_gossip_and_nodetool_status(GOSSIP_INFO, NODES_STATUS, node1), None)
        self.assertIsNone(event)

    def test_check_schema_version_all_ok(self):
        event = next(check_schema_version(GOSSIP_INFO, PEERS_INFO, NODES_STATUS, node1), None)
        self.assertIsNone(event)

    def test_check_schema_agreement_in_gossip_and_peers(self):
        err = check_schema_agreement_in_gossip_and_peers(node1, 1)
        self.assertIsInstance(err, str)
        self.assertFalse(err)

    def test_check_schema_agreement_in_gossip_and_peers_error(self):
        class ProblematicNode(Node):
            def get_gossip_info(self):
                return {
                    node1: {
                        'schema': 'cbe15453-33f3-3387-aaf1-4120548f41e8',
                        'status': 'NORMAL',
                        'dc': 'datacenter1'
                    },
                    node2: {
                        'schema': 'who_cares_about_this_node_is_shutdown',
                        'status': 'shutdown',
                        'dc': 'datacenter1'
                    },
                    node3: {
                        'schema': 'bad-schema',
                        'status': 'NORMAL',
                        'dc': 'datacenter1'
                    },
                }
        problem_node = ProblematicNode('127.0.0.1', "node-0")
        err = check_schema_agreement_in_gossip_and_peers(problem_node, 1)
        self.assertIsInstance(err, str)
        self.assertTrue(err)
