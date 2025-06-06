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
import re
from cassandra.query import SimpleStatement
from contextlib import ExitStack, contextmanager
from time import sleep
from longevity_test import LongevityTest
from sdcm.cluster import MAX_TIME_WAIT_FOR_NEW_NODE_UP, BaseNode
from sdcm.remote.libssh2_client.exceptions import Failure, UnexpectedExit
from sdcm.sct_events import Severity
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.sct_events.loaders import CassandraStressEvent, CassandraStressLogEvent
from sdcm.sct_events.nodetool import NodetoolEvent
from sdcm.sct_events.system import InfoEvent, TestFrameworkEvent
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout
from sdcm.utils.nemesis_utils.indexes import create_index, get_column_names, get_random_column_name, verify_query_by_index_works, wait_for_index_to_be_built, wait_for_view_to_be_built
from sdcm.utils.replication_strategy_utils import NetworkTopologyReplicationStrategy
from sdcm.utils.tablets.common import wait_no_tablets_migration_running


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
        yield


def get_node_disk_usage(node: BaseNode) -> int:
    """Returns disk usage data for a node"""
    result = node.remoter.run("df -h -BG --output=pcent /var/lib/scylla | sed 1d | sed 's/%//'")
    return int(result.stdout.strip())


_pending_tasks_pattern = re.compile(r'- (?P<ks>\w+)\.(?P<cf>\w+): (?P<tasks>\d+)')
_active_tasks_pattern = re.compile(r'\s*([\w-]+)\s+\w+\s+(?P<ks>\w+)\s+(?P<cf>\w+)\s+\d+\s+\d+\s+\w+\s+\d+\.\d+%')


def get_compactions(node: BaseNode, keyspace: str = None, column_family: str = None):
    """
    Returns the total number of tasks

    `nodetool compactionstats` prints the compaction stats like:
    ```
    pending tasks: 42
    - ks1.cf1: 13
    - ks2.cf2: 19
    - ks3.cf3: 10

    id                                   compaction type keyspace table completed total unit progress
    55eaee80-7445-11ef-9197-2931a44dadc4 COMPACTION      ks3      cf3   32116     55680 keys 57.68%
    55e8f2b0-7445-11ef-b438-2930a44dadc4 COMPACTION      ks4      cf4   46789     55936 keys 83.65%
    ```
    """
    output = node.run_nodetool("compactionstats").stdout
    lines = output.strip().splitlines()
    tasks = defaultdict(int)

    for line in map(str.strip, lines):
        if match := _pending_tasks_pattern.match(line):
            tasks[(match.group("ks"), match.group("cf"))] += int(match.group("tasks"))
        elif match := _active_tasks_pattern.match(line):
            tasks[(match.group("ks"), match.group("cf"))] += 1

    if keyspace is None and column_family is None:
        return sum(tasks.values())
    elif keyspace is not None and column_family is None:
        return sum(num_tasks for (ks, _), num_tasks in tasks.items() if ks == keyspace)
    elif keyspace is not None and column_family is not None:
        return tasks.get((keyspace, column_family), 0)


class LongevityOutOfSpaceTest(LongevityTest):
    def write_data(self):
        round_robin = self.params.get("round_robin")
        stress_cmd = self.params.get("stress_cmd_w")

        if isinstance(stress_cmd, str):
            stress_cmd = [stress_cmd]

        params = {"stress_cmd": stress_cmd, "round_robin": round_robin}
        stress_queue = []
        self._run_all_stress_cmds(stress_queue, params)

        for stress in stress_queue:
            self.verify_stress_thread(stress)

    def scale_out(self):
        instance_type = self.params.get("instance_type_db")
        added_nodes = self.db_cluster.add_nodes(count=self.db_cluster.racks_count,
                                                instance_type=instance_type, enable_auto_bootstrap=True, rack=None)
        self.monitors.reconfigure_scylla_monitoring()
        up_timeout = MAX_TIME_WAIT_FOR_NEW_NODE_UP
        with adaptive_timeout(Operations.NEW_NODE, node=self.db_cluster.data_nodes[0], timeout=up_timeout):
            self.db_cluster.wait_for_init(node_list=added_nodes, timeout=up_timeout, check_node_health=False)
        self.db_cluster.set_seeds()
        self.db_cluster.update_seed_provider()
        self.db_cluster.wait_for_nodes_up_and_normal(nodes=added_nodes)
        for node in self.db_cluster.nodes:
            wait_no_tablets_migration_running(node)

    def test_oos_write_scale_out(self):
        """
        Fill the cluster to 90%
        Start another write, that would need more space than available
        Write should fail, but the cluster should not crash
        Scale out the cluster
        Wait for the new nodes to be up
        Continue writing data
        """
        self.run_prepare_write_cmd()

        sleep(600)
        with ignore_stress_errors():
            self.write_data()

        sleep(600)
        self.scale_out()

        sleep(600)
        self.write_data()

    def test_oos_repair_scale_out(self):
        """
        Fill the cluster to 98%
        Repair should fail on the node that reached the 98% threshold
        Scale out the cluster
        Repair should succeed on the node that reached the 98% threshold
        """
        with ignore_stress_errors():
            self.run_prepare_write_cmd()

        sleep(600)
        threshold_node = max(self.db_cluster.nodes, key=get_node_disk_usage)
        with ignore_repair_errors():
            result = threshold_node.run_nodetool("repair", warning_event_on_exception=(
                UnexpectedExit, Failure), error_message="Expected error.")
            assert result is None, "Repair be disabled at this point"

        sleep(600)
        self.scale_out()

        sleep(600)
        result = threshold_node.run_nodetool("repair")
        assert result is not None, "Repair should succeed after scale out"

    def test_oos_compaction_scale_out(self):
        """
        Disable autocompaction on all nodes
        Fill the cluster to 98%
        Enable autocompaction on all nodes
        There should be no running compactions on the node that reached 98%
        Scale out the cluster
        There should be running compactions on the node that reached 98%
        """
        # keyspace needs to exist to disable autocompaction
        self.run_pre_create_keyspace()
        # disable autocompaction on all nodes
        for node in self.db_cluster.nodes:
            node.run_nodetool('disableautocompaction')
        # fill to 98%
        with ignore_stress_errors():
            self.run_prepare_write_cmd()

        for node in self.db_cluster.nodes:
            node.run_nodetool('enableautocompaction')
        sleep(60)

        # Check that the node that got to 98% does not have running compactions
        threshold_node = max(self.db_cluster.nodes, key=get_node_disk_usage)
        assert get_compactions(threshold_node) == 0

        sleep(600)
        self.scale_out()

        # Check that the node that got to 98% has running compactions
        assert get_compactions(threshold_node) != 0

    def test_oos_file_based_streaming_scale_out(self):
        """
        Fill the cluster to 90% with RF=2
        Alter the keyspaces's RF to 3 to force file-based streaming
        Scale out the cluster
        """
        self.run_prepare_write_cmd()

        sleep(600)
        # alter the table's RF to force file-based streaming
        node = self.db_cluster.nodes[0]
        status = self.db_cluster.get_nodetool_status()
        network_topology_strategy = NetworkTopologyReplicationStrategy(**{dc: 3 for dc in status})
        try:
            node.run_cqlsh(f"ALTER KEYSPACE keyspace1 WITH replication = {network_topology_strategy}", timeout=3600)
        except Exception as exc:  # noqa: BLE001
            self.log.error(f"Alter keyspace times out as expected: {exc}")

        # sleep longer before scale out
        sleep(3600)
        self.scale_out()

    def test_oos_si_scale_out(self):
        """
        Fill the cluster to 90%
        Create a secondary index on a column
        Index creation should not finish, as there is not enough space
        Scale out the cluster
        Index creation should finish
        Check that the index works by querying it
        """
        self.run_prepare_write_cmd()

        sleep(600)
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

        sleep(600)
        self.scale_out()

        sleep(600)
        wait_for_index_to_be_built(node, ks, index_name, timeout=timeout)

        sleep(600)
        with self.db_cluster.cql_connection_patient(node, connect_timeout=300) as session:
            verify_query_by_index_works(session, ks, cf, column)

    def test_oos_mv_scale_out(self):
        """
        Fill the cluster to 90%
        Create a materialized view on a table
        View creation should not finish, as there is not enough space
        Scale out the cluster
        View creation should finish
        Check that the view works by querying it
        """
        self.run_prepare_write_cmd()

        sleep(600)
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

        sleep(600)
        self.scale_out()

        sleep(600)
        wait_for_view_to_be_built(node, ks, view_name, timeout=timeout)

        sleep(600)
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
