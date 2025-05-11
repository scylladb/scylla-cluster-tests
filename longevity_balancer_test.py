#!/usr/bin/env python

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


from longevity_test import LongevityTest
from sdcm.cluster import MAX_TIME_WAIT_FOR_NEW_NODE_UP, BaseNode
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout
from sdcm.utils.tablets.common import wait_no_tablets_migration_running


def get_node_disk_usage(node: BaseNode) -> int:
    """Returns disk usage data for a node"""
    result = node.remoter.run("df -h -BG --output=pcent /var/lib/scylla | sed 1d | sed 's/%//'")
    return int(result.stdout.strip())


class LongevityBalancerTest(LongevityTest):
    def expand_cluster_heterogenous(self):
        # add nodes to the cluster
        new_nodes = []
        for instance_type in ["i4i.xlarge", "i4i.2xlarge"]:
            new_nodes += self.db_cluster.add_nodes(count=self.db_cluster.racks_count,
                                                   instance_type=instance_type, enable_auto_bootstrap=True, rack=None)
        self.monitors.reconfigure_scylla_monitoring()
        up_timeout = MAX_TIME_WAIT_FOR_NEW_NODE_UP
        with adaptive_timeout(Operations.NEW_NODE, node=self.db_cluster.data_nodes[0], timeout=up_timeout):
            self.db_cluster.wait_for_init(node_list=new_nodes, timeout=up_timeout, check_node_health=False)
        self.db_cluster.set_seeds()
        self.db_cluster.update_seed_provider()
        self.db_cluster.wait_for_nodes_up_and_normal(nodes=new_nodes)

    def check_cluster_balance(self):
        self.log.info("Checking disk usage")
        usages = {node: get_node_disk_usage(node) for node in self.db_cluster.nodes}
        for node, usage in usages.items():
            self.log.info(f"Node {node.name} has storage utilization: {usage}%")

        # check if the utilization is balanced by comparing min and max utilization
        # Assuming a threshold of 5% for balance
        threshold = 5
        min_utilization = min(usages.values())
        max_utilization = max(usages.values())
        self.log.info(f"Min utilization: {min_utilization}, Max utilization: {max_utilization}")
        if max_utilization - min_utilization > threshold:
            TestFrameworkEvent(source="longevity_balancer_test",
                               message=f"Storage utilization is not balanced. Min: {min_utilization}, Max: {max_utilization}",
                               severity=Severity.CRITICAL).publish()

    def wait_for_balancer(self):
        for node in self.db_cluster.data_nodes:
            wait_no_tablets_migration_running(node, timeout=3600 * 2)

    def test_load_balance(self):
        """
        Test to ensure that the cluster is balanced correctly in difficult conditions:
            - heterogeneous nodes with different disk sizes.
            - multiple tables with different partition sizes.

        This test will:
        1. Expand the cluster by adding new nodes.
        2. Run the original test_custom_time to populate the cluster with data.
        3. Wait for tablet migration to finish.
        4. Check the disk usage of each node to ensure they are balanced.
        """
        self.expand_cluster_heterogenous()
        self.test_custom_time()
        self.wait_for_balancer()
        self.check_cluster_balance()
