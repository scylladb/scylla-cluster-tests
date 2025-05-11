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
from contextlib import ExitStack
from longevity_test import LongevityTest
from sdcm.argus_results import PeriodicDiskUsageToArgus
from sdcm.cluster import MAX_TIME_WAIT_FOR_DECOMMISSION, MAX_TIME_WAIT_FOR_NEW_NODE_UP, BaseNode
from sdcm.sct_events import Severity
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.sct_events.system import HardTimeoutEvent, InfoEvent, SoftTimeoutEvent, TestFrameworkEvent
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout
from sdcm.utils.common import ParallelObject, get_node_disk_usage
from sdcm.utils.tablets.common import wait_no_tablets_migration_running

BALANCE_THRESHOLD = 5


def ignore_decommission_timeout():
    # Due to the test writing a lot of data and the recent `stream_io_throughput_mb_per_sec` changes
    # decommission can take a long time and hit the soft/hard timeout.
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.NORMAL,
            event_class=SoftTimeoutEvent,
            extra_time_to_expiration=60
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.NORMAL,
            event_class=HardTimeoutEvent,
            extra_time_to_expiration=60
        ))
        yield


class LongevityBalancerTest(LongevityTest):
    def expand_cluster_heterogenous(self):
        new_nodes = self.db_cluster.add_nodes(
            count=self.params.get("nemesis_add_node_cnt"),
            instance_type=self.params.get("nemesis_grow_shrink_instance_type"),
            enable_auto_bootstrap=True,
            rack=None)
        self.monitors.reconfigure_scylla_monitoring()
        up_timeout = MAX_TIME_WAIT_FOR_NEW_NODE_UP
        with adaptive_timeout(Operations.NEW_NODE, node=self.db_cluster.data_nodes[0], timeout=up_timeout):
            self.db_cluster.wait_for_init(node_list=new_nodes, timeout=up_timeout, check_node_health=False)
        self.db_cluster.set_seeds()
        self.db_cluster.update_seed_provider()
        self.db_cluster.wait_for_nodes_up_and_normal(nodes=new_nodes)

    def wait_for_balance(self):
        # run multiple times because `storage_service/quiesce_topology` only returns when
        # the topology operations that were ongoing when the command was issued are done
        # but new operations can start right after that
        for _ in range(3):
            ParallelObject(objects=self.db_cluster.data_nodes, timeout=3600).run(wait_no_tablets_migration_running)

    def check_final_balance(self):
        rack_usages = defaultdict(list)
        for node in self.db_cluster.data_nodes:
            rack_usages[node.rack].append(get_node_disk_usage(node))

        for rack, usages in rack_usages.items():
            min_utilization = min(usages)
            max_utilization = max(usages)
            if max_utilization - min_utilization > BALANCE_THRESHOLD:
                TestFrameworkEvent(source="longevity_balancer_test",
                                   message=f"Storage utilization is not balanced in rack {rack}. Min: {min_utilization:.2f}%, Max: {max_utilization:.2f}%",
                                   severity=Severity.CRITICAL).publish()

    def scale_out(self):
        added_nodes = self.db_cluster.add_nodes(
            count=self.db_cluster.racks_count,
            instance_type=self.params.get("nemesis_grow_shrink_instance_type"),
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
        for node in nodes:
            self.nemesis_allocator.set_running_nemesis(node, 'decommissioning')
        parallel_obj = ParallelObject(objects=nodes, timeout=MAX_TIME_WAIT_FOR_DECOMMISSION, num_workers=len(nodes))
        InfoEvent(f'Started decommissioning {[node for node in nodes]}').publish()
        with ignore_decommission_timeout():
            parallel_obj.run(self.db_cluster.decommission, ignore_exceptions=False, unpack_objects=True)
        InfoEvent(f'Finished decommissioning {[node for node in nodes]}').publish()
        self.monitors.reconfigure_scylla_monitoring()

    def run_stress_command(self):
        stress_queue = []
        self.assemble_and_run_all_stress_cmd(stress_queue, self.params.get(
            'stress_cmd'), self.params.get('keyspace_num'))
        for stress in stress_queue:
            self.verify_stress_thread(stress)

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
        with PeriodicDiskUsageToArgus(self.db_cluster, self.test_config.argus_client(), interval=600):
            self.run_prepare_write_cmd()
            new_nodes = self.scale_out()
            self.run_stress_command()
            self.scale_in(new_nodes)
            self.wait_for_balance()
            self.check_final_balance()
