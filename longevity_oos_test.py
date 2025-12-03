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
from itertools import cycle
from contextlib import ExitStack, contextmanager
from time import sleep, time
from longevity_test import LongevityTest
from sdcm.cluster import MAX_TIME_WAIT_FOR_NEW_NODE_UP, BaseNode
from sdcm.db_stats import PrometheusDBStats
from sdcm.exceptions import WaitForTimeoutError
from sdcm.mgmt.common import ScyllaManagerError, TaskStatus
from sdcm.sct_events import Severity
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.sct_events.loaders import CassandraStressEvent, CassandraStressLogEvent
from sdcm.sct_events.nodetool import NodetoolEvent
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout
from sdcm.utils.decorators import retrying
from sdcm.utils.nemesis_utils.indexes import create_index, verify_query_by_index_works, wait_for_index_to_be_built
from sdcm.utils.tablets.common import wait_no_tablets_migration_running
from threading import Thread


@contextmanager
def ignore_stress_errors():
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.NORMAL,
            event_class=CassandraStressEvent,
            extra_time_to_expiration=60
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.NORMAL,
            event_class=CassandraStressLogEvent,
            extra_time_to_expiration=60
        ))
        yield


@contextmanager
def ignore_repair_errors():
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.NORMAL,
            event_class=DatabaseLogEvent,
            regex=".*Repair service is disabled. No repairs will be started until it's re-enabled.*",
            extra_time_to_expiration=60
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.NORMAL,
            event_class=NodetoolEvent,
            regex=".*Repair service is disabled. No repairs will be started until it's re-enabled.*",
            extra_time_to_expiration=60
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.NORMAL,
            event_class=TestFrameworkEvent,
            regex=".*Failed on waiting until task.*",
            extra_time_to_expiration=60
        ))
        yield


class LongevityOutOfSpaceTest(LongevityTest):
    def tearDown(self):
        # an extra failure check for disk usage
        for node in self.db_cluster.nodes:
            max_usage = self.get_node_max_disk_usage(node, start=self.start_time, end=time())
            if max_usage >= 98.5:
                TestFrameworkEvent(source=self.__class__.__name__,
                                   message=f"Node {node.name} ({node.private_ip_address}) max disk usage was {max_usage:.2f}%.",
                                   severity=Severity.ERROR).publish()

        super().tearDown()

    def _query_disk_usage(self, node: BaseNode, start: float = None, end: float = None) -> float:
        """
        :param node: The node to get the disk usage for.
        :param start: The start time for the query as a timestamp. Defaults to end - 60.
        :param end: The end time for the query as a timestamp. Defaults to current time.
        :return: The results of the query.
        """
        self.prometheus_db: PrometheusDBStats
        end = end or time()
        start = start or (end - 60)
        avail_query = f'sum(node_filesystem_avail_bytes{{mountpoint="/var/lib/scylla", instance=~".*?{node.private_ip_address}.*?", job=~"node_exporter.*"}})'
        size_query = f'sum(node_filesystem_size_bytes{{mountpoint="/var/lib/scylla", instance=~".*?{node.private_ip_address}.*?", job=~"node_exporter.*"}})'
        full_query = f'100 * (1 - ({avail_query} / {size_query}))'
        return self.prometheus_db.query(query=full_query, start=start, end=end)

    def get_disk_usage(self, node: BaseNode) -> float:
        """
        :param node: The node to get the disk usage for.
        :return: The disk usage in percentage, or -1 if the query fails.
        """
        results = self._query_disk_usage(node)

        try:
            return float(results[0]['values'][-1][1])
        except (IndexError, ValueError, TypeError):
            # Catch any errors in case the results are malformed
            return -1

    @retrying(n=3, sleep_time=60)
    def get_node_max_disk_usage(self, node: BaseNode, start: float, end: float) -> float:
        """
        :param node: The node to get the max disk usage for.
        :param start: The start time for the query as a timestamp.
        :param end: The end time for the query as a timestamp.
        :return: The max disk usage in percentage.
        """
        results = self._query_disk_usage(node, start=start, end=end)
        return max(float(v[1]) for v in results[0]['values'])

    def run_read_stress(self):
        stress_queue = []
        self.assemble_and_run_all_stress_cmd(stress_queue, self.params.get(
            'stress_cmd_r'), self.params.get('keyspace_num'))
        return all(self.verify_stress_thread(stress) for stress in stress_queue)

    def run_write_stress(self):
        stress_queue = []
        self.assemble_and_run_all_stress_cmd(stress_queue, self.params.get(
            'stress_cmd_w'), self.params.get('keyspace_num'))
        return all(self.verify_stress_thread(stress) for stress in stress_queue)

    def scale_out(self, nr_nodes=None, rack=None):
        """
        Scale out the cluster by adding new nodes.

        :param nr_nodes: Number of nodes to add, defaults to one per rack.
        :param rack: The rack to add the nodes to, defaults to round robin through the available racks.
        """
        instance_type = self.params.get("instance_type_db")
        nr_nodes = nr_nodes or self.db_cluster.racks_count
        added_nodes = self.db_cluster.add_nodes(
            count=nr_nodes, instance_type=instance_type, enable_auto_bootstrap=True, rack=rack)
        self.monitors.reconfigure_scylla_monitoring()
        up_timeout = MAX_TIME_WAIT_FOR_NEW_NODE_UP
        with adaptive_timeout(Operations.NEW_NODE, node=self.db_cluster.data_nodes[0], timeout=up_timeout):
            self.db_cluster.wait_for_init(node_list=added_nodes, timeout=up_timeout, check_node_health=False)
        self.db_cluster.set_seeds()
        self.db_cluster.update_seed_provider()
        self.db_cluster.wait_for_nodes_up_and_normal(nodes=added_nodes)
        for node in self.db_cluster.nodes:
            wait_no_tablets_migration_running(node, timeout=7200)

    def get_compactions(self, node: BaseNode, interval: int) -> int:
        """
        Get the number of compactions that occurred on the node during the specified interval.

        :param node: The node to get the compactions for.
        :param interval: The time interval for the query, as last X seconds.
        :return: The number of compactions that occurred on the node during the specified interval.
        """
        compaction_query = f'sum(scylla_compaction_manager_compactions{{instance=~".*?{node.private_ip_address}.*?"}})'
        now = time()
        results = self.prometheus_db.query(query=compaction_query, start=now - interval, end=now)
        self.log.info(f"Compactions on node {node.name} ({node.private_ip_address}): {results}")
        # results is of this form:
        # [{'metric': {}, 'values': [[1749638594.936, '0'], [1749638614.936, '0'], [1749638634.936, '0'], [1749638654.936, '0']]}]
        return sum(int(v[1]) for v in results[0]['values']) if results else 0

    def prepare(self):
        self.run_prepare_write_cmd()
        for node in self.db_cluster.nodes:
            usage = self.get_disk_usage(node)
            if usage <= 85:
                TestFrameworkEvent(source=self.__class__.__name__,
                                   message=f"Node {node.name} ({node.private_ip_address}) max disk is only {usage:.2f}% after prepare.",
                                   severity=Severity.CRITICAL).publish()

    def test_oos_write(self):
        """
        Fill the cluster to 90%
        Start another write, that would need more space than available
        Write should fail, but the cluster should not run out of space
        Scale out the cluster
        Continue writing data
        Write should succeed after scale out
        """
        self.prepare()

        with ignore_stress_errors():
            result = self.run_write_stress()
            if result:
                TestFrameworkEvent(source=self.__class__.__name__,
                                   message="Writes should have failed, but it succeeded",
                                   severity=Severity.ERROR).publish()

        self.scale_out()

        self.run_write_stress()

    def test_oos_restart(self):
        """
        Fill the cluster to 90%
        Start another write, that would need more space than available
        During this write, restart a node
        The restarted node should not run out of space
        """
        self.prepare()

        with ignore_stress_errors():
            stress_thread = Thread(target=self.run_write_stress)
            stress_thread.start()

            restarted = False
            while stress_thread.is_alive() and not restarted:
                for node in self.db_cluster.nodes:
                    disk_usage = self.get_disk_usage(node)
                    if disk_usage >= 97:
                        self.log.info(f"Node {node.name} has reached 97% disk usage, restarting it.")
                        node.stop_scylla(verify_down=True)
                        node.start_scylla(verify_up=True)
                        restarted = True
                        break
                sleep(60)

            if not restarted:
                TestFrameworkEvent(source=self.__class__.__name__,
                                   message="No node reached 97% disk usage to restart.",
                                   severity=Severity.CRITICAL).publish()

            stress_thread.join()

    def test_oos_repair(self):
        """
        Fill the cluster to 90%; Restart nodes during fill
        Start another write, that would need more space than available
        At 97% disk usage, start a repair task
        Repair should have status RUNNING and not finish
        Scale out the cluster
        Repair task should finish with status DONE
        Verify the repair by reading with CL=THREE
        """
        prepare_thread = Thread(target=self.prepare)
        prepare_thread.start()
        nodes = cycle(self.db_cluster.nodes)
        while prepare_thread.is_alive():
            # every 15 minutes, cycle thorough the nodes restart them
            sleep(900)
            node = next(nodes)
            if self.get_disk_usage(node) > 70:
                break
            self.log.info(f"Restarting node {node.name}.")
            node.stop_scylla(verify_down=True)
            node.start_scylla(verify_up=True)
            self.db_cluster.wait_for_nodes_up_and_normal(nodes=[node])
            self.log.info(f"Node {node.name} has restarted.")
        prepare_thread.join()
        self.log.info("Prepare write command finished.")

        with ignore_stress_errors():
            stress_thread = Thread(target=self.run_write_stress)
            stress_thread.start()
            self.log.info("Started stress write thread.")

            while stress_thread.is_alive():
                if any(self.get_disk_usage(node) >= 97 for node in self.db_cluster.nodes):
                    break
                sleep(60)

            mgr_cluster = self.db_cluster.get_cluster_manager()
            repair_task = mgr_cluster.create_repair_task()
            self.log.info(f"Repair task {repair_task.id} created.")

            stress_thread.join()

        repair_timeout = 3 * 3600  # 3 hours
        try:
            task_final_status = repair_task.wait_and_get_final_status(timeout=repair_timeout)
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"Repair should not finish. Status: {task_final_status}",
                               severity=Severity.CRITICAL).publish()
        except WaitForTimeoutError:
            self.log.info(f"Repair task {repair_task.id} did not finish as expected, continuing with scale out.")

        self.scale_out()

        task_final_status = repair_task.wait_and_get_final_status(timeout=repair_timeout)
        if task_final_status != TaskStatus.DONE:
            progress_full_string = repair_task.progress_string(
                parse_table_res=False, is_verify_errorless_result=True).stdout
            if task_final_status != TaskStatus.ERROR_FINAL:
                repair_task.stop()
            raise ScyllaManagerError(
                f"Task: {repair_task.id} final status is: {task_final_status}.\nTask progress string: {progress_full_string}")
        self.log.info(f"Task: {repair_task.id} is done.")

        self.run_read_stress()

    def test_oos_compaction(self):
        """
        Fill the cluster to 90%
        Start another write, that would need more space than available
        Write should fail, but the cluster should not run out of space
        There should be no running compactions on the node that reached 98%
        Scale out the cluster
        There should be running compactions on the node that reached 98%
        """
        # fill to 90%
        self.prepare()

        # fill to 98%
        with ignore_stress_errors():
            self.run_write_stress()

        # Check that the node that got to 98% does not have running compactions
        sleep(1200)
        threshold_node = max(self.db_cluster.nodes, key=self.get_disk_usage)
        if self.get_compactions(threshold_node, interval=600) != 0:
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"There should be no running compactions on node {threshold_node.name}",
                               severity=Severity.ERROR).publish()

        start_of_scale_out = time()
        self.scale_out()
        end_of_scale_out = time()

        # Check that the node that got to 98% has running compactions
        interval = int(end_of_scale_out - start_of_scale_out)
        if self.get_compactions(threshold_node, interval=interval) == 0:
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"There should have been running compactions on node {threshold_node.name} after scale out",
                               severity=Severity.ERROR).publish()

    def test_oos_secondary_index(self):
        """
        Fill the cluster to 90%
        Create a secondary index on a column
        Index creation should not finish, as there is not enough space
        Scale out the cluster
        Index creation should finish
        Check that the index works by querying it
        """
        self.prepare()

        # create index
        ks = "keyspace1"
        cf = "standard1"
        column = "C0"
        node = self.db_cluster.nodes[0]
        timeout = 12 * 3600

        with self.db_cluster.cql_connection_patient(node, connect_timeout=300) as session:
            index_name = create_index(session, ks, cf, column)

        try:
            # here, the correct way would be to wait for the full timeout
            # but that just makes the test longer with no benefit
            wait_for_index_to_be_built(node, ks, index_name, timeout=3600)
            TestFrameworkEvent(source=self.__class__.__name__,
                               message="Index creation should not finish.", severity=Severity.CRITICAL).publish()
        except TimeoutError:
            self.log.info(f"Index {index_name} creation timed out as expected")

        self.scale_out()

        try:
            wait_for_index_to_be_built(node, ks, index_name, timeout=timeout)
        except TimeoutError:
            TestFrameworkEvent(source=self.__class__.__name__,
                               message="Index creation should finish after scale out.", severity=Severity.CRITICAL).publish()

        with self.db_cluster.cql_connection_patient(node, connect_timeout=300) as session:
            verify_query_by_index_works(session, ks, cf, column)
