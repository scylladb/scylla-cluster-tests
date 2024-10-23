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
# Copyright (c) 2019 ScyllaDB

import time
from textwrap import dedent

import six

from sdcm.tester import ClusterTester
from sdcm.utils.decorators import measure_time, retrying, optional_stage
from test_lib.scylla_bench_tools import create_scylla_bench_table_query

THOUSAND = 1000
MILLION = THOUSAND ** 2
BILLION = THOUSAND ** 3


class PerformanceRegressionRowLevelRepairTest(ClusterTester):
    """
    Test Scylla row-level-repair performance regression with cassandra-stress/scylla-bench.

    """

    # Util functions ===============================================================================================

    @measure_time
    def _run_repair(self, node):
        self.log.info('Running nodetool repair on {}'.format(node.name))
        node.run_nodetool(sub_cmd='repair')

    def _pre_create_schema_large_scale(self, keyspace_num=1, scylla_encryption_options=None):
        """
        For cases we are testing many keyspaces and tables, It's a possibility that we will do it better and faster than
        cassandra-stress.
        """

        self.log.debug('Pre Creating Schema for c-s with {} keyspaces'.format(keyspace_num))
        for i in range(1, keyspace_num + 1):
            keyspace_name = 'keyspace{}'.format(i)
            self.create_keyspace(keyspace_name=keyspace_name, replication_factor=3)
            self.log.debug('{} Created'.format(keyspace_name))
            table_name = "{}.standard1".format(keyspace_name)
            self.create_table(name=table_name, key_type='blob', read_repair=0.0,
                              columns={'"C0"': 'blob'},
                              scylla_encryption_options=scylla_encryption_options)

    def _update_cl_in_stress_cmd(self, str_stress_cmd, consistency_level):
        for param in str_stress_cmd.split():
            if param.startswith('cl='):
                return str_stress_cmd.replace(param, "cl={}".format(consistency_level))
        self.log.debug("Could not find a 'cl' parameter in stress command: {}".format(str_stress_cmd))
        return str_stress_cmd

    @optional_stage('perf_preload_data')
    def preload_data(self, consistency_level=None):
        # if test require a pre-population of data

        prepare_write_cmd = self.params.get('prepare_write_cmd')
        if prepare_write_cmd:
            self.create_test_stats(sub_type='write-prepare')
            stress_queue = []
            params = {'prefix': 'preload-'}
            # Check if the prepare_cmd is a list of commands
            if not isinstance(prepare_write_cmd, six.string_types) and len(prepare_write_cmd) > 1:
                # Check if it should be round_robin across loaders
                if self.params.get('round_robin').lower() == 'true':
                    self.log.debug('Populating data using round_robin')
                    params.update({'stress_num': 1, 'round_robin': True})

                for stress_cmd in prepare_write_cmd:
                    if consistency_level:
                        stress_cmd = self._update_cl_in_stress_cmd(  # noqa: PLW2901
                            str_stress_cmd=stress_cmd, consistency_level=consistency_level)
                    params.update({'stress_cmd': stress_cmd})

                    # Run all stress commands
                    params.update(dict(stats_aggregate_cmds=False))
                    self.log.debug('RUNNING stress cmd: {}'.format(stress_cmd))
                    stress_queue.append(self.run_stress_thread(**params))

            # One stress cmd command
            else:
                stress_cmd = prepare_write_cmd if not consistency_level else self._update_cl_in_stress_cmd(
                    str_stress_cmd=prepare_write_cmd, consistency_level=consistency_level)
                stress_queue.append(self.run_stress_thread(stress_cmd=stress_cmd, stress_num=1,
                                                           prefix='preload-', stats_aggregate_cmds=False))

            for stress in stress_queue:
                self.get_stress_results(queue=stress, store_results=False)

            self.update_test_details()
        else:
            self.log.warning("No prepare command defined in YAML!")

    @retrying(n=80, sleep_time=60, allowed_exceptions=(AssertionError,))
    def _wait_no_compactions_running(self):
        compactions_query = "sum(scylla_compaction_manager_compactions{})"
        now = time.time()
        results = self.prometheus_db.query(query=compactions_query, start=now - 60, end=now)
        self.log.debug(msg="scylla_compaction_manager_compactions: %s" % results)
        # if all are zeros the result will be False, otherwise there are still compactions
        if results:
            assert any(float(v[1]) for v in results[0]["values"]) is False, \
                "Waiting until all compactions settle down"

    def _disable_hinted_handoff(self):

        yaml_file = "/etc/scylla/scylla.yaml"
        tmp_yaml_file = "/tmp/scylla.yaml"
        disable_hinted_handoff = dedent("""
                                                    grep -v hinted_handoff_enabled {0} > {1}
                                                    echo hinted_handoff_enabled: false >> {1}
                                                    cp -f {1} {0}
                                                """.format(yaml_file, tmp_yaml_file))
        for node in self.db_cluster.nodes:  # disable hinted handoff on all nodes
            node.remoter.run('sudo bash -cxe "%s"' % disable_hinted_handoff)
            self.log.debug("Scylla YAML configuration read from: {} {} is:".format(node.public_ip_address, yaml_file))
            node.remoter.run('sudo cat {}'.format(yaml_file))

            node.stop_scylla_server()
            node.start_scylla_server()

    def _pre_create_schema_scylla_bench(self):
        node = self.db_cluster.nodes[0]
        create_table_query = create_scylla_bench_table_query()
        # pylint: disable=no-member
        with self.db_cluster.cql_connection_patient(node) as session:
            session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS scylla_bench WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}
            """)
            session.execute(create_table_query)

    def _run_scylla_bench_on_single_node(self, node, stress_cmd):
        self.log.info('Stopping all other nodes before updating {}'.format(node.name))
        self.stop_all_nodes_except_for(node=node)
        self.log.info('Updating cluster data only for {}'.format(node.name))
        self.log.info("Run stress command of: {}".format(stress_cmd))
        stress_queue = self.run_stress_thread_bench(stress_cmd=stress_cmd, stats_aggregate_cmds=False,
                                                    round_robin=True)
        self.get_stress_results_bench(queue=stress_queue)
        self.start_all_nodes()

    # Util functions ============================================================================================

    def test_row_level_repair_single_node_diff(self):
        """
        Start 3 nodes, create keyspace with rf = 3, disable hinted hand off
        requires: export SCT_HINTED_HANDOFF_DISABLED=true
        :return:
        """
        stats = {}

        self._pre_create_schema_large_scale()
        node1 = self.db_cluster.nodes[-1]

        for node in self.db_cluster.nodes:
            used_capacity = self.get_used_capacity(node=node)
            self.log.debug("Node {} initial used capacity is: {}".format(node.public_ip_address, used_capacity))

        self._disable_hinted_handoff()

        self.log.info('Stopping node-3 ({}) before updating cluster data'.format(node1.name))
        node1.stop_scylla_server()
        self.log.info('Updating cluster data when node3 ({}) is down'.format(node1.name))
        self.log.info('Starting c-s/s-b write workload')
        self.preload_data()
        self.wait_no_compactions_running()

        self.log.info('Starting node-3 ({}) after updated cluster data'.format(node1.name))
        node1.start_scylla_server()

        for node in self.db_cluster.nodes:
            used_capacity = self.get_used_capacity(node=node)
            self.log.debug(
                "Node {} used capacity after pre-load data is: {}".format(node.public_ip_address, used_capacity))

        self.log.info('Run Repair on node: {} , 0% synced'.format(node1.name))
        repair_time = self._run_repair(node=node1)[0]  # pylint: disable=unsubscriptable-object
        self.log.info('Repair (0% synced) time on node: {} is: {}'.format(node1.name, repair_time))

        stats['repair_runtime_all_diff'] = repair_time

        self.wait_no_compactions_running()

        self.log.info('Run Repair on node: {} , 100% synced'.format(node1.name))
        repair_time = self._run_repair(node=node1)[0]  # pylint: disable=unsubscriptable-object
        self.log.info('Repair (100% synced) time on node: {} is: {}'.format(node1.name, repair_time))

        stats['repair_runtime_no_diff'] = repair_time
        self.update_test_details(scylla_conf=True, extra_stats=stats)

        self.check_specified_stats_regression(stats)

    def test_row_level_repair_3_nodes_small_diff(self):
        """
        Start 3 nodes, create keyspace with rf = 3, disable hinted hand off
        requires: export SCT_HINTED_HANDOFF_DISABLED=true
        :return:
        """
        base_distinct_write_cmd = "cassandra-stress write no-warmup cl=ONE n=1000000 " \
                                  "-schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' -mode cql3 native " \
                                  "-rate threads=200 -col 'size=FIXED(1024) n=FIXED(1)'"
        sequence_current_index = BILLION
        sequence_range = MILLION

        self._pre_create_schema_large_scale()
        node1, node2, node3 = self.db_cluster.nodes
        self._disable_hinted_handoff()
        self.print_nodes_used_capacity()
        for node in [node1, node2, node3]:
            self.log.info('Stopping all other nodes before updating {}'.format(node.name))
            self.stop_all_nodes_except_for(node=node)
            self.log.info('Updating cluster data only for {}'.format(node.name))
            distinct_write_cmd = "{} -pop seq={}..{} -node {}".format(base_distinct_write_cmd,
                                                                      sequence_current_index + 1,
                                                                      sequence_current_index + sequence_range,
                                                                      node.private_ip_address)
            self.log.info("Run stress command of: {}".format(distinct_write_cmd))
            stress_thread = self.run_stress_thread(stress_cmd=distinct_write_cmd, round_robin=True)
            self.verify_stress_thread(cs_thread_pool=stress_thread)
            self.start_all_nodes()
            sequence_current_index += sequence_range

        self._wait_no_compactions_running()
        self.log.debug("Nodes distinct used capacity is")
        self.print_nodes_used_capacity()

        self.log.info('Starting c-s/s-b write workload')
        self.preload_data(consistency_level='ALL')
        self._wait_no_compactions_running()

        self.log.debug("Nodes total used capacity before starting repair is:")
        self.print_nodes_used_capacity()

        self.log.info('Run Repair on node: {} , 99.8% synced'.format(node3.name))
        repair_time = self._run_repair(node=node3)[0]  # pylint: disable=unsubscriptable-object

        self.log.debug("Nodes total used capacity after repair end is:")
        self.print_nodes_used_capacity()

        self.log.info('Repair (99.8% synced) time on node: {} is: {}'.format(node3.name, repair_time))

        stats = {'repair_runtime_small_diff': repair_time}

        self.update_test_details(scylla_conf=True, extra_stats=stats)

        self.check_specified_stats_regression(stats)

    def _populate_scylla_bench_data_in_parallel(self, base_cmd, partition_count, clustering_row_count,
                                                consistency_level="all"):

        n_loaders = int(self.params.get('n_loaders'))
        partitions_per_loader = partition_count // n_loaders
        str_additional_args = "-partition-count={} -clustering-row-count={} -consistency-level={}".format(
            partitions_per_loader, clustering_row_count, consistency_level)
        write_queue = []
        offset = 0
        for _ in range(n_loaders):
            str_offset = "-partition-offset {}".format(offset)
            stress_cmd = " ".join(
                [base_cmd, str_additional_args, str_offset])
            self.log.debug('Scylla-bench stress command to execute: {}'.format(stress_cmd))
            write_queue.append(self.run_stress_thread_bench(stress_cmd=stress_cmd, stats_aggregate_cmds=False,
                                                            round_robin=True))
            offset += partitions_per_loader
            time.sleep(0.2)

        for stress_queue in write_queue:
            self.get_stress_results_bench(queue=stress_queue)

        return write_queue

    def test_row_level_repair_large_partitions(self):
        """
        Start 3 nodes, create keyspace with rf = 3, disable hinted hand off
        requires: export SCT_HINTED_HANDOFF_DISABLED=true
        :return:
        """

        assert self.params.get('stress_cmd').startswith('scylla-bench'), "This test supports only scylla-bench"

        node3 = self.db_cluster.nodes[-1]
        self._disable_hinted_handoff()
        self.print_nodes_used_capacity()
        self._pre_create_schema_scylla_bench()

        self.log.info('Starting scylla-bench large-partitions write workload')
        partition_count = 2000
        clustering_row_count = 40960
        clustering_row_size = 2048
        partition_count_per_node = partition_count // 10 // 3
        clustering_row_count_per_node = clustering_row_count // 100 * 3
        scylla_bench_base_cmd = self.params.get('stress_cmd') + f" -clustering-row-size={clustering_row_size}"
        self._populate_scylla_bench_data_in_parallel(base_cmd=scylla_bench_base_cmd, partition_count=partition_count,
                                                     clustering_row_count=clustering_row_count)

        self._wait_no_compactions_running()

        offset = 0  # per node increased with interval of: partition_count_per_node * clustering_row_count_per_node * 10
        str_additional_args = "-partition-count={} -clustering-row-count={} -consistency-level=ALL".format(
            partition_count_per_node, clustering_row_count_per_node)

        for node in self.db_cluster.nodes:
            str_offset = "-partition-offset {}".format(offset)
            stress_cmd = " ".join(
                [scylla_bench_base_cmd, str_additional_args, str_offset])
            self._run_scylla_bench_on_single_node(node=node, stress_cmd=stress_cmd)
            offset += partition_count_per_node

        self._wait_no_compactions_running()
        self.log.debug("Nodes total used capacity before starting repair is:")
        self.print_nodes_used_capacity()

        self.log.info('Run Repair on node: {}'.format(node3.name))
        repair_time = self._run_repair(node=node3)[0]  # pylint: disable=unsubscriptable-object

        self.log.debug("Nodes total used capacity after repair end is:")
        self.print_nodes_used_capacity()

        self.log.info('Repair (with large partitions) time on node: {} is: {}'.format(node3.name, repair_time))

        stats = {'repair_runtime_large_partitions': repair_time}

        self.update_test_details(scylla_conf=True, extra_stats=stats)

        self.check_specified_stats_regression(stats)

    def test_row_level_repair_during_load(self, preload_data=True):
        """
        Start 3 nodes, create keyspace with rf = 3, disable hinted hand off
        requires: export SCT_HINTED_HANDOFF_DISABLED=true
        :return:
        """
        background_stress_cmds = [
            "cassandra-stress write no-warmup cl=QUORUM duration=140m"
            " -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' "
            "-mode cql3 native -rate threads=25 -col 'size=FIXED(1024) n=FIXED(1)'",
            "cassandra-stress read no-warmup cl=QUORUM duration=140m"
            " -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' "
            "-mode cql3 native -rate threads=6 -col 'size=FIXED(1024) n=FIXED(1)'"]

        node1 = self.db_cluster.nodes[-1]
        self._disable_hinted_handoff()
        self.print_nodes_used_capacity()

        if preload_data:
            self._pre_create_schema_large_scale()
            self.log.info('Starting c-s/s-b write workload')
            self.preload_data(consistency_level='ALL')

        self.log.debug("Nodes total used capacity before starting repair is:")
        self.print_nodes_used_capacity()

        self.log.debug("Start a background stress load")
        stress_queue = []
        params = {'prefix': 'background-load-'}
        params.update({'stress_num': 1, 'round_robin': True})
        for stress_cmd in background_stress_cmds:
            params.update({'stress_cmd': stress_cmd})
            # Run stress command
            params.update(dict(stats_aggregate_cmds=False))
            self.log.debug('RUNNING stress cmd: {}'.format(stress_cmd))
            stress_queue.append(self.run_stress_thread(**params))

        self.log.info('Run Repair on node: {} , during r/w load'.format(node1.name))
        repair_time = self._run_repair(node=node1)[0]  # pylint: disable=unsubscriptable-object

        self.log.debug("Nodes total used capacity after repair end is:")
        self.print_nodes_used_capacity()

        self.log.info('Repair (during r/w load) time on node: {} is: {}'.format(node1.name, repair_time))

        stats = {'repair_runtime_during_load': repair_time}

        self.update_test_details(scylla_conf=True, extra_stats=stats)

        self.check_specified_stats_regression(stats)
