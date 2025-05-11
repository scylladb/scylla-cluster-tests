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


from collections import defaultdict
from time import time
from longevity_test import LongevityTest
from sdcm.cluster import MAX_TIME_WAIT_FOR_DECOMMISSION, MAX_TIME_WAIT_FOR_NEW_NODE_UP, BaseNode
from sdcm.db_stats import PrometheusDBStats
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout
from sdcm.utils.common import ParallelObject
from sdcm.utils.tablets.common import wait_no_tablets_migration_running

SOFT_BALANCE_THRESHOLD = 5
HARD_BALANCE_THRESHOLD = 10


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

    def wait_for_balance(self):
        for _ in range(3):
            for node in self.db_cluster.data_nodes:
                wait_no_tablets_migration_running(node, timeout=3600 * 2)

    def get_disk_usage(self, node: BaseNode, end_time: float = None) -> float:
        """
        Get the disk usage of a node in percentage.

        :param node: The node to get the disk usage for.
        :param end_time: The end time for the query, defaults to current time.
        :return: The disk usage in percentage, or -1 if the query fails."""
        self.prometheus_db: PrometheusDBStats
        end = end_time or time()
        start = end - 60  # Query the last minute of data to ensure we have recent metrics
        avail_query = f'sum(node_filesystem_avail_bytes{{mountpoint="/var/lib/scylla", instance=~".*?{node.private_ip_address}.*?", job=~"node_exporter.*"}})'
        size_query = f'sum(node_filesystem_size_bytes{{mountpoint="/var/lib/scylla", instance=~".*?{node.private_ip_address}.*?", job=~"node_exporter.*"}})'
        full_query = f'1 - ({avail_query} / {size_query})'
        results = self.prometheus_db.query(query=full_query, start=start, end=end)

        try:
            disk_usage = float(results[0]['values'][-1][1])
            if disk_usage < 0 or disk_usage > 1:
                return -1  # Invalid disk usage value

            return 100 * disk_usage
        except (IndexError, ValueError, TypeError):
            # Catch any errors in case the results are malformed
            return -1

    def check_final_balance(self):
        rack_usages = defaultdict(list)
        for node in self.db_cluster.data_nodes:
            rack_usages[node.rack].append(self.get_disk_usage(node))

        for rack, usages in rack_usages.items():
            min_utilization = min(usages)
            max_utilization = max(usages)
            if max_utilization - min_utilization > SOFT_BALANCE_THRESHOLD:
                TestFrameworkEvent(source="longevity_balancer_test",
                                   message=f"Storage utilization is not balanced in rack {rack}. Min: {min_utilization:.2f}%, Max: {max_utilization:.2f}%",
                                   severity=Severity.CRITICAL).publish()

    def scale_out(self):
        added_nodes = self.db_cluster.add_nodes(
            count=self.db_cluster.racks_count,
            instance_type=self.params.get("instance_type_db"),
            enable_auto_bootstrap=True,
            rack=None)
        self.monitors.reconfigure_scylla_monitoring()
        up_timeout = MAX_TIME_WAIT_FOR_NEW_NODE_UP
        with adaptive_timeout(Operations.NEW_NODE, node=self.db_cluster.data_nodes[0], timeout=up_timeout):
            self.db_cluster.wait_for_init(node_list=added_nodes, timeout=up_timeout, check_node_health=False)
        self.db_cluster.set_seeds()
        self.db_cluster.update_seed_provider()
        self.db_cluster.wait_for_nodes_up_and_normal(nodes=added_nodes)
        return added_nodes

    def scale_in(self, nodes: list[BaseNode]):
        parallel_obj = ParallelObject(objects=nodes, timeout=MAX_TIME_WAIT_FOR_DECOMMISSION, num_workers=len(nodes))
        parallel_obj.run(self.db_cluster.decommission, ignore_exceptions=False, unpack_objects=True)
        self.monitors.reconfigure_scylla_monitoring()

    def test_load_balance(self):
        """
        Test to ensure that the cluster is balanced correctly in difficult conditions:
            - heterogeneous nodes with different disk sizes.
            - multiple tables with different partition sizes.

        This test will:
        1. Expand the cluster by adding new nodes of different types.
            No possible to start with heterogeneous nodes, as the cluster is created with a single type.
        2. Populate the cluster with data.
        3. Add some nodes to the cluster.
        4. Write more data to the cluster.
        5. Remove the added nodes.
        6. Wait for the cluster to balance.
        7. Check the final balance of the cluster.
        """
        self.expand_cluster_heterogenous()
        self.run_prepare_write_cmd()
        new_nodes = self.scale_out()
        self.assemble_and_run_all_stress_cmd([], self.params.get('stress_cmd'), self.params.get('keyspace_num'))
        self.scale_in(new_nodes)
        self.wait_for_balance()
        self.check_final_balance()
