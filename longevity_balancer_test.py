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
from contextlib import ExitStack, contextmanager
import contextlib
from time import sleep, strftime, time
from argus.client.base import ArgusClientError
from argus.client.generic_result import ColumnMetadata, ResultType, StaticGenericResultTable, Status
from longevity_test import LongevityTest
from sdcm.cluster import MAX_TIME_WAIT_FOR_DECOMMISSION, MAX_TIME_WAIT_FOR_NEW_NODE_UP, BaseNode
from sdcm.db_stats import PrometheusDBStats
from sdcm.sct_events import Severity
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout
from sdcm.utils.common import ParallelObject
from sdcm.utils.tablets.common import wait_no_tablets_migration_running
from threading import Thread

SOFT_BALANCE_THRESHOLD = 5
HARD_BALANCE_THRESHOLD = 10


@contextmanager
def ignore_oversized_allocation():
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.NORMAL,
            event_class=DatabaseLogEvent,
            regex=".*seastar_memory - oversized allocation.*",
            extra_time_to_expiration=60
        ))
        yield


class LongevityBalancerTest(LongevityTest):
    def setUp(self):
        super().setUp()
        self.stack = contextlib.ExitStack()
        self.stack.enter_context(ignore_oversized_allocation())

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

    def get_disk_usage(self, node: BaseNode) -> float:
        """
        Get the disk usage of a node in percentage.

        :param node: The node to get the disk usage for.
        :return: The disk usage in percentage, or -1 if the query fails."""
        self.prometheus_db: PrometheusDBStats
        start, end = time() - 60, time()  # Query the last minute of data
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

    def disk_usage_to_argus(self):
        """
        Collect disk usage for each node and submit the results to Argus.

        :return: A dictionary with racks as keys and their disk usages as values.
        """
        shortname = lambda node: f"node-{node.name.split('-')[-1]}"
        rack_groups = defaultdict(list)
        for node in self.db_cluster.data_nodes:
            rack_groups[node.rack].append(node)

        columns = []
        for rack, rack_nodes in rack_groups.items():
            for node in rack_nodes:
                columns.append(ColumnMetadata(name=shortname(node), unit="%", type=ResultType.FLOAT))
            columns.append(ColumnMetadata(name=f"rack-{rack}", unit="%", type=ResultType.FLOAT))

        class DiskUsageResult(StaticGenericResultTable):
            class Meta:
                name = "Disk Usage"
                description = f"""
                    The disk usage of the nodes in the cluster and the balance status per rack.
                    The status is determined based on the balance of disk usage inside a rack.
                    GREEN: balanced within {SOFT_BALANCE_THRESHOLD}%
                    YELLOW: balanced within {HARD_BALANCE_THRESHOLD}%
                    RED: unbalanced, more than {HARD_BALANCE_THRESHOLD}% difference in disk usage
                    """
                Columns = columns

        label = strftime('%Y-%m-%d %H:%M:%S')
        data_table = DiskUsageResult()

        rack_usages = defaultdict(list)
        for rack, rack_nodes in rack_groups.items():
            for node in rack_nodes:
                usage = self.get_disk_usage(node)
                rack_usages[rack].append(usage)
                data_table.add_result(column=shortname(node), row=label, value=usage,
                                      status=Status.UNSET)
            # Calculate the average usage for the rack
            if rack_usages[rack]:
                rack_delta = max(rack_usages[rack]) - min(rack_usages[rack])
                status = (Status.PASS if rack_delta <= SOFT_BALANCE_THRESHOLD else
                          Status.WARNING if rack_delta <= HARD_BALANCE_THRESHOLD else
                          Status.ERROR)
                data_table.add_result(column=f"rack-{rack}", row=label, value=rack_delta, status=status)

        try:
            self.test_config.argus_client().submit_results(data_table)
        except ArgusClientError as exc:
            if exc.args[1] == "DataValidationError":
                pass  # Ignore validation errors, the status is just used to color the table in Argus
            else:
                raise
        return rack_usages

    @contextmanager
    def periodic_disk_usage_to_argus(self, interval=600):
        """
        Periodically collect disk usage and submit it to Argus.
        """
        def collect_disk_usage():
            while True:
                self.disk_usage_to_argus()
                sleep(interval)

        thread = Thread(target=collect_disk_usage, daemon=True)
        thread.start()
        yield
        thread.join(timeout=1)

    def check_final_balance(self):
        rack_usages = defaultdict(list)
        for node in self.db_cluster.data_nodes:
            rack_usages[node.rack].append(self.get_disk_usage(node))

        for rack, usages in rack_usages.items():
            min_utilization = min(usages)
            max_utilization = max(usages)
            if max_utilization - min_utilization > HARD_BALANCE_THRESHOLD:
                TestFrameworkEvent(source="longevity_balancer_test",
                                   message=f"Storage utilization is not balanced in rack {rack}. Min: {min_utilization}, Max: {max_utilization}",
                                   severity=Severity.CRITICAL).publish()

    def test_load_balance(self):
        """
        Test to ensure that the cluster is balanced correctly in difficult conditions:
            - heterogeneous nodes with different disk sizes.
            - multiple tables with different partition sizes.

        This test will:
        1. Expand the cluster by adding new nodes.
        2. Run the original test_custom_time to populate the cluster with data.
        3. Wait for tablet migration to finish.
        4. Periodically check the disk usage of each node to ensure they are balanced.
        """
        self.expand_cluster_heterogenous()
        sleep(600)  # wait for new nodes to have prometheus_db metrics
        with self.periodic_disk_usage_to_argus(interval=600):
            self.test_custom_time()
            self.wait_for_balance()
            self.check_final_balance()

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

    def grow_shrink(self):
        # This method is used instead of GrowShrinkClusterNemesis because the nemesis
        # will wait for tablet migration to finish after scale out, which is not desired in this test.
        sleep(15*60)
        new_nodes = self.scale_out()
        sleep(15*60)
        self.scale_in(new_nodes)

    def test_load_balance_grow_shrink(self):
        """
        Test to ensure that the cluster is balanced correctly in difficult conditions:
            - heterogeneous nodes with different disk sizes.
            - multiple tables with different partition sizes.

        This test will:
        1. Expand the cluster by adding new nodes.
        2. Run the original test_custom_time to populate the cluster with data.
        3. During the test, nodes will be added and removed.
        4. Wait for tablet migration to finish.
        5. Periodically check the disk usage of each node to ensure they are balanced.
        """
        self.expand_cluster_heterogenous()
        sleep(600)  # wait for new nodes to have prometheus_db metrics
        with self.periodic_disk_usage_to_argus(interval=600):
            ParallelObject(objects=[self.test_custom_time, self.grow_shrink], timeout=7200).call_objects()
            self.wait_for_balance()
            self.check_final_balance()
