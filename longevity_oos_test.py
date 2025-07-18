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
import contextlib
from itertools import cycle
import re
from cassandra.query import SimpleStatement
from contextlib import ExitStack, contextmanager
from time import sleep, time
from longevity_test import LongevityTest
from sdcm.cluster import MAX_TIME_WAIT_FOR_DECOMMISSION, MAX_TIME_WAIT_FOR_NEW_NODE_UP, BaseNode
from sdcm.exceptions import WaitForTimeoutError
from sdcm.mgmt.common import ScyllaManagerError, TaskStatus
from sdcm.remote.libssh2_client.exceptions import UnexpectedExit
from sdcm.sct_events import Severity
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.sct_events.loaders import CassandraStressEvent, CassandraStressLogEvent
from sdcm.sct_events.nodetool import NodetoolEvent
from sdcm.sct_events.system import InfoEvent, TestFrameworkEvent
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout
from sdcm.utils.nemesis_utils.indexes import create_index, get_column_names, get_random_column_name, verify_query_by_index_works, wait_for_index_to_be_built, wait_for_view_to_be_built
from sdcm.utils.replication_strategy_utils import NetworkTopologyReplicationStrategy, ReplicationStrategy
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


def get_node_disk_usage(node: BaseNode) -> int:
    """Returns disk usage data for a node"""
    result = node.remoter.run("df -h -BG --output=pcent /var/lib/scylla | sed 1d | sed 's/%//'")
    return int(result.stdout.strip())


def is_node_at_critical_disk_utilization(node: BaseNode):
    if not hasattr(is_node_at_critical_disk_utilization, "_marks"):
        is_node_at_critical_disk_utilization._marks = defaultdict(int)
    marks = is_node_at_critical_disk_utilization._marks

    pattern = re.compile(r"Setting critical disk utilization mode: (true|false)")
    cdu_mode = False

    with open(node.system_log, encoding="utf-8") as logfile:
        logfile.seek(marks[node])
        for line in logfile:
            if match := pattern.search(line):
                cdu_mode = match.group(1) == "true"
        marks[node] = logfile.tell()

    return cdu_mode


class LongevityOutOfSpaceTest(LongevityTest):
    def setUp(self):
        super().setUp()
        self.stack = contextlib.ExitStack()
        self.stack.enter_context(ignore_oversized_allocation())

    def run_stress(self, cmd_name="stress_cmd"):
        round_robin = self.params.get("round_robin")
        stress_cmd = self.params.get(cmd_name)

        if isinstance(stress_cmd, str):
            stress_cmd = [stress_cmd]

        params = {"stress_cmd": stress_cmd, "round_robin": round_robin}
        stress_queue = []
        self._run_all_stress_cmds(stress_queue, params)

        result = True
        for stress in stress_queue:
            result = result and self.verify_stress_thread(stress)
        return result

    def scale_out(self, rack=None):
        instance_type = self.params.get("instance_type_db")
        nr_nodes = self.db_cluster.racks_count if rack is None else 1
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
        compaction_query = f'sum(scylla_compaction_manager_compactions{{instance=~".*?{node.private_ip_address}.*?"}})'
        now = time()
        results = self.prometheus_db.query(query=compaction_query, start=now - interval, end=now)
        self.log.info(f"Compactions on node {node.name} ({node.private_ip_address}): {results}")
        # results is of this form:
        # [{'metric': {}, 'values': [[1749638594.936, '0'], [1749638614.936, '0'], [1749638634.936, '0'], [1749638654.936, '0']]}]
        return sum(int(v[1]) for v in results[0]['values']) if results else 0

    def test_oos_write(self):
        """
        Fill the cluster to 90%
        Start another write, that would need more space than available
        Write should fail, but the cluster should not crash
        Scale out the cluster
        Wait for the new nodes to be up
        Continue writing data
        """
        self.run_prepare_write_cmd()

        with ignore_stress_errors():
            self.run_stress()

        self.scale_out()

        self.run_stress()

    def test_oos_restart(self):
        """
        Fill the cluster to 90%
        Start another write, that would need more space than available
        During this write, restart a node
        Write should fail, but the cluster should not crash
        """
        self.run_prepare_write_cmd()

        with ignore_stress_errors():
            stress_thread = Thread(target=self.run_stress)
            stress_thread.start()

            restarted = False
            while stress_thread.is_alive() and not restarted:
                for node in self.db_cluster.nodes:
                    disk_usage = get_node_disk_usage(node)
                    if disk_usage >= 97:
                        self.log.info(f"Node {node.name} has reached 97% disk usage, restarting it.")
                        node.stop_scylla(verify_down=True)
                        node.start_scylla(verify_up=True)
                        restarted = True
                        break
                sleep(60)

            stress_thread.join()

    def test_oos_repair(self):
        """
        Fill the cluster to 90%
        Start a repair task
        Fill the cluster to 98%
        Repair should have status RUNNING and not finish
        Scale out the cluster
        Wait until the repair task has status DONE
        """
        prepare_thread = Thread(target=self.run_prepare_write_cmd)
        prepare_thread.start()
        nodes = cycle(self.db_cluster.nodes)
        while prepare_thread.is_alive():
            # every 10 minutes, cycle thorough the nodes restart them
            sleep(600)
            node = next(nodes)
            if get_node_disk_usage(node) > 80:
                break
            self.log.info(f"Restarting node {node.name}")
            node.stop_scylla(verify_down=True)
            node.start_scylla(verify_up=True)

        with ignore_stress_errors():
            stress_thread = Thread(target=self.run_stress)
            stress_thread.start()

            mgr_cluster = self.db_cluster.get_cluster_manager()
            repair_task = None
            while stress_thread.is_alive() and repair_task is None:
                for node in self.db_cluster.nodes:
                    disk_usage = get_node_disk_usage(node)
                    if disk_usage >= 97:
                        self.log.info(f"Node {node.name} has reached 97% disk usage, starting repair task.")
                        repair_task = mgr_cluster.create_repair_task()
                        break
                sleep(60)
            stress_thread.join()

        repair_timeout = 3 * 3600  # 3 hours
        try:
            task_final_status = repair_task.wait_and_get_final_status(timeout=repair_timeout)
            TestFrameworkEvent(source=self.__class__.__name__, message="Repair should not finish.",
                               severity=Severity.CRITICAL).publish()
        except WaitForTimeoutError:
            self.log.info(f"Repair task {repair_task.id} did not finish as expected, continuing with scale out.")
        assert repair_task.status == TaskStatus.RUNNING, f"Repair task {repair_task.id} status is {repair_task.status}, expected RUNNING"

        self.scale_out()

        task_final_status = repair_task.wait_and_get_final_status(timeout=repair_timeout)
        if task_final_status != TaskStatus.DONE:
            progress_full_string = repair_task.progress_string(
                parse_table_res=False, is_verify_errorless_result=True).stdout
            if task_final_status != TaskStatus.ERROR_FINAL:
                repair_task.stop()
            raise ScyllaManagerError(
                f'Task: {repair_task.id} final status is: {str(task_final_status)}.\nTask progress string: '
                f'{progress_full_string}')
        self.log.info('Task: {} is done.'.format(repair_task.id))

        self.run_stress("stress_cmd_r")

    def test_oos_compaction(self):
        """
        Fill the cluster to 90%
        Fill the cluster to 98%
        There should be no running compactions on the node that reached 98%
        Scale out the cluster
        There should be running compactions on the node that reached 98%
        """
        # fill to 90%
        self.run_prepare_write_cmd()

        # fill to 98%
        with ignore_stress_errors():
            self.run_stress()

        # Check that the node that got to 98% does not have running compactions
        sleep(1200)
        threshold_node = max(self.db_cluster.nodes, key=get_node_disk_usage)
        assert self.get_compactions(threshold_node, interval=600) == 0

        start_of_scale_out = time()
        self.scale_out()
        end_of_scale_out = time()

        # Check that the node that got to 98% has running compactions
        interval = int(end_of_scale_out - start_of_scale_out)
        assert self.get_compactions(threshold_node, interval=interval) != 0

    def test_oos_streaming_rf_change(self):
        """
        Fill the cluster to 90% with RF=2
        Alter the keyspaces's RF to 3 to force file-based streaming
        Start a read with CL=THREE, it should fail
        Scale out the cluster
        Start a read with CL=THREE, it should succeed
        """
        self.run_prepare_write_cmd()
        sleep(600)

        # alter the table's RF to force file-based streaming
        status = self.db_cluster.get_nodetool_status()
        network_topology_strategy = NetworkTopologyReplicationStrategy(**{dc: 3 for dc in status})
        node1 = self.db_cluster.nodes[0]
        try:
            node1.run_cqlsh(f"ALTER KEYSPACE keyspace1 WITH replication = {network_topology_strategy}", timeout=3600)
        except UnexpectedExit as exc:
            self.log.error(f"Alter keyspace times out as expected: {exc}")

        replication_strategy = ReplicationStrategy.get(node1, 'keyspace1')
        self.log.info(f"Replication strategy for keyspace keyspace1: {replication_strategy}")
        assert replication_strategy.replication_factors == [3]

        with ignore_stress_errors():
            result = self.run_stress()
            assert not result, "Reading with CL=THREE should have failed"

        self.scale_out()

        self.run_stress()

    def test_oos_streaming_decommission(self):
        """
        Fill the cluster to 90%
        Decommission one node
        """
        self.run_prepare_write_cmd()
        sleep(600)

        node1 = self.db_cluster.nodes[0]
        decommission_thread = Thread(target=self.db_cluster.decommission, args=(node1,),
                                     kwargs={"timeout": MAX_TIME_WAIT_FOR_DECOMMISSION})
        decommission_thread.start()

        while decommission_thread.is_alive():
            if is_node_at_critical_disk_utilization(node1):
                self.log.info(f"Node {node1.name} is at critical disk utilization, scaling out the cluster.")
                self.scale_out(rack=node1.rack)
                break
            sleep(60)

        decommission_thread.join()

    def test_oos_secondary_index(self):
        """
        Fill the cluster to 90%
        Create a secondary index on a column
        Index creation should not finish, as there is not enough space
        Scale out the cluster
        Index creation should finish
        Check that the index works by querying it
        """
        self.run_prepare_write_cmd()

        # create index
        ks = "keyspace1"
        cf = "standard1"
        column = "C0"
        node = self.db_cluster.nodes[0]
        timeout = 2 * 3600

        with self.db_cluster.cql_connection_patient(node, connect_timeout=300) as session:
            index_name = create_index(session, ks, cf, column)

        try:
            wait_for_index_to_be_built(node, ks, index_name, timeout=timeout)
            TestFrameworkEvent(message="Index creation should not finish.", severity=Severity.CRITICAL).publish()
        except TimeoutError:
            self.log.info(f"Index {index_name} creation timed out as expected")

        self.scale_out()

        wait_for_index_to_be_built(node, ks, index_name, timeout=timeout)

        with self.db_cluster.cql_connection_patient(node, connect_timeout=300) as session:
            verify_query_by_index_works(session, ks, cf, column)

    def test_oos_materialized_view(self):
        """
        Fill the cluster to 90%
        Create a materialized view on a table
        View creation should not finish, as there is not enough space
        Scale out the cluster
        View creation should finish
        Check that the view works by querying it
        """
        self.run_prepare_write_cmd()

        # create index
        ks = "keyspace1"
        cf = "standard1"
        view_name = f'{cf}_view'
        node = self.db_cluster.nodes[0]
        timeout = 2 * 3600

        with self.db_cluster.cql_connection_patient(node, connect_timeout=300) as session:
            primary_key_columns = get_column_names(session=session, ks=ks, cf=cf, is_primary_key=True)
            column = get_random_column_name(session=session, ks=ks,
                                            cf=cf, filter_out_collections=True, filter_out_static_columns=True)
            column = f'"{column}"'
            self.create_materialized_view(ks, cf, view_name, [column], primary_key_columns, session, mv_columns=[
                column] + primary_key_columns)

        try:
            wait_for_view_to_be_built(node, ks, view_name, timeout=timeout)
            TestFrameworkEvent(message="Materialized view creation should not finish.",
                               severity=Severity.CRITICAL).publish()
        except TimeoutError:
            self.log.info(f"Materialized view {view_name} creation timed out as expected")

        self.scale_out()

        wait_for_view_to_be_built(node, ks, view_name, timeout=timeout)

        try:
            with self.db_cluster.cql_connection_patient(node, connect_timeout=300) as session:
                query = SimpleStatement(f'SELECT * FROM {ks}.{view_name} limit 1', fetch_size=10)
                self.log.debug("Verifying query by materialized view works: %s", query)
                result = session.execute(query)
        except Exception as exc:  # noqa: BLE001
            InfoEvent(message=f"Materialized view {ks}.{view_name} does not work in query: {query}. Reason: {exc}",
                      severity=Severity.ERROR).publish()
        if len(list(result)) == 0:
            InfoEvent(message=f"Materialized view {ks}.{view_name} does not work. No rows returned for query {query}",
                      severity=Severity.ERROR).publish()

    def test_oos_tablet_rebuild(self):
        """
        https://github.com/scylladb/scylladb/issues/24858
        Scenario:

            lose multiple nodes in a rack
            nodetool removenode those nodes
            bootstrap new nodes
            If tablet rebuild is faster than bootstrap (or if the operator doesn't bootstrap enough nodes) then rebuild will consume all free space.
        """
        self.run_prepare_write_cmd()
        # lose all but one node in the rack
        rack1 = [n for n in self.db_cluster.nodes if n.rack == self.db_cluster.nodes[0].rack]
        for node in rack1[1:]:
            self.log.info(f"Stopping node {node.name} in rack {node.rack}")
            node.stop_scylla()

        # nodetool removenode those nodes
        node1 = rack1[0]
        for node in rack1[1:]:
            with adaptive_timeout(Operations.REMOVE_NODE, node1, timeout=3600):
                node1.run_nodetool(f"removenode {node.host_id}", ignore_status=True,
                                   verbose=True, long_running=True, retry=0)
        self.monitors.reconfigure_scylla_monitoring()

        # bootstrap new nodes
        for _ in rack1[1:]:
            self.scale_out(rack=node1.rack)
