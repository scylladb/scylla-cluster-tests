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
import contextlib
from time import sleep
from longevity_test import LongevityTest
from sdcm.argus_results import PeriodicDiskUsageToArgus
from sdcm.cluster import MAX_TIME_WAIT_FOR_DECOMMISSION, MAX_TIME_WAIT_FOR_NEW_NODE_UP, BaseNode
from sdcm.sct_events import Severity
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.sct_events.loaders import CassandraStressEvent
from sdcm.sct_events.system import HardTimeoutEvent, InfoEvent, SoftTimeoutEvent, TestFrameworkEvent
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout
from sdcm.utils.common import ParallelObject, get_node_disk_usage
from sdcm.utils.decorators import latency_calculator_decorator
from sdcm.utils.tablets.common import wait_no_tablets_migration_running

BALANCE_THRESHOLD = 5


@contextlib.contextmanager
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
    hdr_tags: list[str] = []

    def scale_out(self, instance_type_param='nemesis_grow_shrink_instance_type', count_param='nemesis_add_node_cnt') -> list[BaseNode]:
        added_nodes = self.db_cluster.add_nodes(
            count=self.params.get(count_param),
            instance_type=self.params.get(instance_type_param),
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

    def run_background_load(self):
        stress_queue = []
        self.assemble_and_run_all_stress_cmd(stress_queue, self.params.get(
            'stress_cmd'), self.params.get('keyspace_num'))
        self.hdr_tags.extend(tag for s in stress_queue for tag in s.hdr_tags)

    def run_bigger_load(self):
        stress_queue = []
        self.assemble_and_run_all_stress_cmd(stress_queue, self.params.get(
            'stress_cmd_m'), self.params.get('keyspace_num'))
        old_hdr_tags = self.hdr_tags.copy()
        self.hdr_tags.extend(tag for s in stress_queue for tag in s.hdr_tags)
        self.calculate_latencies(row_name='#2 scaled_load_scaled_cluster')
        for stress in stress_queue:
            self.verify_stress_thread(stress)
        self.hdr_tags = old_hdr_tags

    def calculate_latencies(self, row_name: str):
        calc_duration = self.db_cluster.params.get('nemesis_interval') * 60
        sleep_time = self.db_cluster.params.get('nemesis_sequence_sleep_between_ops') * 60
        sleep(sleep_time)  # postpone measure latency to skip c-s start period when latency is high
        latency_calculator_decorator(cycle_name='workload_latencies',
                                     row_name=row_name)(lambda _: sleep(calc_duration))(self)

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
        self.scale_out()
        with PeriodicDiskUsageToArgus(self.db_cluster, self.test_config.argus_client(), interval=600):
            self.run_prepare_write_cmd()
            new_nodes = self.scale_out()
            self.run_stress_command()
            self.scale_in(new_nodes)
            self.wait_for_balance()
            self.check_final_balance()

    def test_workload_latencies(self):
        """
        This test will:
        1. Fill a cluster to 90%
        2. Start a background workload that runs for the entire test duration
        3. Change the cluster's topology by adding new nodes and removing some of the original nodes
        while keeping the cluster disk size constant, but with more CPUs
        4. Increase the background workload
        5. Before returning to the original cluster topology, got back to the original background workload
        6. Return to the original cluster topology
        7. Let the background workload run for a while
        """
        self.run_prepare_write_cmd()
        InfoEvent('BALANCER: Finished prepare writes').publish()

        # wait for compactions to finish
        self.wait_no_compactions_running(n=720)
        InfoEvent('BALANCER: Finished waiting for compactions').publish()

        # wait for tablet migrations to finish
        self.wait_for_balance()
        InfoEvent('BALANCER: Finished waiting for tablet migrations').publish()

        # base load in background
        self.run_background_load()
        InfoEvent('BALANCER: Started background load').publish()

        # get steady latency for base load with original cluster configuration
        self.calculate_latencies(row_name='#1 steady_base_load_base_cluster')
        InfoEvent('BALANCER: Latency calc #1 done (steady base load)').publish()

        # scale out
        original_nodes = list(self.db_cluster.data_nodes)
        new_nodes = self.scale_out('nemesis_grow_shrink_instance_type', 'nemesis_add_node_cnt')
        InfoEvent('BALANCER: Added new nodes').publish()
        self.scale_in(original_nodes)
        InfoEvent('BALANCER: Removed original nodes').publish()

        # increased the workload and measure latency
        self.run_bigger_load()
        InfoEvent('BALANCER: Latency calc #2 done (with increased load)').publish()

        # get latency for base load with new cluster configuration
        self.calculate_latencies(row_name='#3 base_load_scaled_cluster')
        InfoEvent('BALANCER: Latency calc #3 done (base load with scaled cluster)').publish()

        # scale back to original capacity
        self.scale_out('instance_type_db', 'n_db_nodes')
        InfoEvent('BALANCER: Added original nodes back').publish()
        self.scale_in(new_nodes)
        InfoEvent('BALANCER: Removed new nodes').publish()

        # get latency for base load with original cluster configuration
        self.calculate_latencies(row_name='#4 base_load_base_cluster')
        InfoEvent('BALANCER: Latency calc #4 done (base load with original cluster)').publish()

        # kill the background load to end the test
        with EventsSeverityChangerFilter(new_severity=Severity.NORMAL, event_class=CassandraStressEvent, extra_time_to_expiration=60):
            self.loaders.kill_stress_thread()
        InfoEvent('BALANCER: Killed base load, test should end now').publish()
