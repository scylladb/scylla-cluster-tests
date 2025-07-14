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


from contextlib import contextmanager
from time import sleep, strftime, time
from argus.client.base import ArgusClient
from argus.client.generic_result import ColumnMetadata, ResultType, StaticGenericResultTable, Status
from longevity_test import LongevityTest
from sdcm.argus_results import submit_results_to_argus
from sdcm.cluster import MAX_TIME_WAIT_FOR_NEW_NODE_UP, BaseNode
from sdcm.db_stats import PrometheusDBStats
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout
from sdcm.utils.tablets.common import wait_no_tablets_migration_running
from threading import Thread

SOFT_BALANCE_THRESHOLD = 5
HARD_BALANCE_THRESHOLD = 10


def get_disk_usage(node: BaseNode, prometheus: PrometheusDBStats) -> float:
    """
    Get the disk usage of a node in percentage.

    :param node: The node to get the disk usage for.
    :param prometheus: The PrometheusDBStats instance to query.
    :return: The disk usage in percentage, or -1 if the query fails."""
    start, end = time() - 60, time()  # Query the last minute of data
    avail_query = f'sum(node_filesystem_avail_bytes{{mountpoint="/var/lib/scylla", instance=~".*?{node.private_ip_address}.*?", job=~"node_exporter.*"}})'
    avail_results = prometheus.query(query=avail_query, start=start, end=end)
    size_query = f'sum(node_filesystem_size_bytes{{mountpoint="/var/lib/scylla", instance=~".*?{node.private_ip_address}.*?", job=~"node_exporter.*"}})'
    size_results = prometheus.query(query=size_query, start=start, end=end)

    try:
        avail = float(avail_results[0]['values'][-1][1])
        size = float(size_results[0]['values'][-1][1])

        if avail < 0 or size <= 0:
            return -1

        return 100 * (1 - avail / size)
    except (IndexError, ValueError, TypeError):
        # Catch any errors in case the results are malformed
        return -1


def get_balance_status(node: BaseNode, usages: dict[BaseNode, float]) -> Status:
    """
    Determine the balance status of a node based on its disk usage compared its rack.

    :param node: The node to check.
    :param usages: A dictionary with nodes as keys and their disk usage as values.
    :return: Status.PASS if the node's disk usage is within the soft balance threshold,
             Status.WARNING if within the hard balance threshold,
             Status.ERROR if it exceeds the hard threshold,
             and STATUS.UNSET if a node's usage is -1 (indicating an error in fetching usage).
    """
    this_rack = [v for n, v in usages.items() if n.rack == node.rack]
    if -1 in this_rack:
        return Status.UNSET
    delta_usage = max(this_rack) - min(this_rack)
    if delta_usage <= SOFT_BALANCE_THRESHOLD:
        return Status.PASS
    elif delta_usage <= HARD_BALANCE_THRESHOLD:
        return Status.WARNING
    else:
        return Status.ERROR


def disk_usage_to_argus(nodes: list[BaseNode], argus_client: ArgusClient, prometheus: PrometheusDBStats):
    """
    Collect disk usage for each node and submit the results to Argus.

    :param nodes: List of nodes to collect disk usage from.
    :param argus_client: The Argus client to submit results to.
    :param prometheus: The PrometheusDBStats instance to query.
    :return: A dictionary with node names as keys and their disk usage as values.
    """
    nodes = sorted(nodes, key=lambda n: n.rack)

    class DiskUsageResult(StaticGenericResultTable):
        class Meta:
            name = "Disk Usage"
            description = "The disk usage of the nodes in the cluster"
            Columns = [ColumnMetadata(name=f"node-{node.name[-1]}", unit="%", type=ResultType.FLOAT) for node in nodes]

    label = strftime('%Y-%m-%d %H:%M:%S')
    data_table = DiskUsageResult()
    usages = {node: get_disk_usage(node, prometheus) for node in nodes}
    for node in nodes:
        data_table.add_result(column=f"node-{node.name[-1]}", row=label,
                              value=usages[node], status=get_balance_status(node, usages))
    submit_results_to_argus(argus_client, data_table)
    return usages


@contextmanager
def periodic_disk_usage_to_argus(nodes: list[BaseNode], argus_client: ArgusClient, prometheus: PrometheusDBStats, interval: int = 600):
    """
    Periodically collect disk usage and submit to Argus.

    :param nodes: List of nodes to collect disk usage from.
    :param argus_client: The Argus client to submit results to.
    :param prometheus: The PrometheusDBStats instance to query.
    :param interval: Time interval in seconds to collect disk usage.
    """
    def collect_disk_usage():
        while True:
            disk_usage_to_argus(nodes, argus_client, prometheus)
            sleep(interval)

    thread = Thread(target=collect_disk_usage, daemon=True)
    thread.start()
    yield
    thread.join(timeout=1)


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
        self.log.info("Checking final disk usage")
        usages = disk_usage_to_argus(self.db_cluster.data_nodes, self.test_config.argus_client(), self.prometheus_db)

        # check if the utilization is balanced by comparing min and max utilization
        min_utilization = min(usages.values())
        max_utilization = max(usages.values())
        if max_utilization - min_utilization > HARD_BALANCE_THRESHOLD:
            TestFrameworkEvent(source="longevity_balancer_test",
                               message=f"Storage utilization is not balanced. Min: {min_utilization}, Max: {max_utilization}",
                               severity=Severity.CRITICAL).publish()

    def wait_for_balancer(self):
        self.log.info("Waiting for tablet migration to finish on all nodes")
        for node in self.db_cluster.data_nodes:
            wait_no_tablets_migration_running(node, timeout=3600 * 2)

    @contextmanager
    def disk_usage_to_argus(self, interval=600):
        with periodic_disk_usage_to_argus(self.db_cluster.data_nodes, self.test_config.argus_client(), self.prometheus_db, interval=interval):
            yield

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
        sleep(600)
        with self.disk_usage_to_argus(interval=600):
            self.test_custom_time()
            self.wait_for_balancer()
            self.check_cluster_balance()
