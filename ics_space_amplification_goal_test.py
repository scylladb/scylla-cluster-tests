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
# Copyright (c) 2020 ScyllaDB

import math
import time
from functools import partial
from textwrap import dedent

from longevity_test import LongevityTest
from sdcm.cluster import BaseNode
from sdcm.db_stats import AVAIL_SIZE_METRIC, AVAIL_SIZE_METRIC_OLD, GB_SIZE
from sdcm.sct_events import Severity
from sdcm.sct_events.system import InfoEvent, TestFrameworkEvent
from sdcm.utils.common import ParallelObject, skip_optional_stage
from test_lib.compaction import CompactionStrategy, LOGGER

KEYSPACE_NAME = 'keyspace1'
TABLE_NAME = 'standard1'


class IcsSpaceAmplificationTest(LongevityTest):

    def _get_filesystem_available_size_list(self, node, start_time):
        """
        :returns a list of file-system free-capacity on point in time from 'start_time' up to now.
        """
        node_capacity_query_postfix = self.prometheus_db.generate_node_capacity_query_postfix(node)
        available_capacity_query = f'{AVAIL_SIZE_METRIC}{node_capacity_query_postfix}'
        available_size_res = self.prometheus_db.query(query=available_capacity_query, start=int(start_time),
                                                      end=int(time.time()))
        assert available_size_res, "No results from Prometheus"
        if not available_size_res[0]:  # if no returned values - try the old metric names.
            available_capacity_query = f'{AVAIL_SIZE_METRIC_OLD}{node_capacity_query_postfix}'
            available_size_res = self.prometheus_db.query(query=available_capacity_query, start=int(start_time),
                                                          end=int(time.time()))
        assert available_size_res[0], "Could not resolve available-size query result."
        return available_size_res[0]["values"]

    def _get_max_used_capacity_over_time_gb(self, node, start_time) -> float:
        """

        :param node: DB node under test.
        :param start_time: the start interval to search max-used-capacity from.
        :return:
        """
        fs_size_gb = self.prometheus_db.get_filesystem_total_size_gb(node=node)
        end_time = time.time()
        time_interval_minutes = int(math.ceil((end_time - start_time) / 60))  # convert time to minutes and round up.
        min_available_capacity_gb = min(
            [int(val[1]) for val in
             self._get_filesystem_available_size_list(node=node, start_time=start_time)]) / GB_SIZE
        max_used_capacity_gb = fs_size_gb - min_available_capacity_gb
        self.log.debug("The maximum used filesystem capacity of %s for the last %s minutes is: %s GB/ %s GB",
                       node.private_ip_address, time_interval_minutes, max_used_capacity_gb, fs_size_gb)
        return max_used_capacity_gb

    def _get_nodes_space_ampl_over_time_gb(self, dict_nodes_initial_capacity, start_time,
                                           written_data_size_gb=0):
        dict_nodes_space_amplification = {}
        dict_nodes_used_capacity = self._get_nodes_used_capacity()
        for node in self.db_cluster.nodes:
            node_initial_capacity = dict_nodes_initial_capacity[node.private_ip_address]
            node_used_capacity = dict_nodes_used_capacity[node.private_ip_address]
            node_max_used_capacity_gb = self._get_max_used_capacity_over_time_gb(node=node,
                                                                                 start_time=start_time)
            dict_nodes_space_amplification[node.private_ip_address] = \
                round(node_max_used_capacity_gb - written_data_size_gb - node_initial_capacity, 2)
            self.log.info("Node %s used capacity changed from %s to %s.",
                          node.private_ip_address, node_initial_capacity, node_used_capacity)
            self.log.info("Space amplification is: %s GB", dict_nodes_space_amplification[node.private_ip_address])
        return dict_nodes_space_amplification

    def measure_nodes_space_amplification_after_write(self, dict_nodes_initial_capacity, written_data_size_gb,
                                                      start_time):
        dict_nodes_space_amplification = self._get_nodes_space_ampl_over_time_gb(
            dict_nodes_initial_capacity=dict_nodes_initial_capacity,
            written_data_size_gb=written_data_size_gb, start_time=start_time)
        self.log.info("Space amplification results after a write of: %s are: %s",
                      written_data_size_gb, dict_nodes_space_amplification)
        InfoEvent(message=f"Space amplification results after a write of: {written_data_size_gb} are: "
                          f"{dict_nodes_space_amplification}").publish()

    def _get_nodes_used_capacity(self) -> dict:
        """
        :rtype: dictionary with capacity per node-ip
        """
        dict_nodes_used_capacity = {}
        for node in self.db_cluster.nodes:
            dict_nodes_used_capacity[node.private_ip_address] = self.prometheus_db.get_used_capacity_gb(node=node)
        return dict_nodes_used_capacity

    def _alter_table_compaction(self, compaction_strategy=CompactionStrategy.INCREMENTAL, table_name=TABLE_NAME,
                                keyspace_name=KEYSPACE_NAME,
                                additional_compaction_params: dict = None):
        """
         Alters table compaction like: ALTER TABLE mykeyspace.mytable WITH
                                        compaction = {'class' : 'IncrementalCompactionStrategy'}
        """

        base_query = f"ALTER TABLE {keyspace_name}.{table_name} WITH compaction = "
        dict_requested_compaction = {'class': compaction_strategy.value}
        if additional_compaction_params:
            dict_requested_compaction.update(additional_compaction_params)

        full_alter_query = base_query + str(dict_requested_compaction)
        LOGGER.debug("Alter table query is: %s", full_alter_query)
        node1: BaseNode = self.db_cluster.nodes[0]
        node1.run_cqlsh(cmd=full_alter_query)
        InfoEvent(message=f"Altered table by: {full_alter_query}").publish()

    def _set_enforce_min_threshold_true(self):

        yaml_file = "/etc/scylla/scylla.yaml"
        tmp_yaml_file = "/tmp/scylla.yaml"
        set_enforce_min_threshold = dedent("""
                                                    grep -v compaction_enforce_min_threshold {0} > {1}
                                                    echo compaction_enforce_min_threshold: true >> {1}
                                                    cp -f {1} {0}
                                                """.format(yaml_file, tmp_yaml_file))
        for node in self.db_cluster.nodes:  # set compaction_enforce_min_threshold on all nodes
            node.remoter.run('sudo bash -cxe "%s"' % set_enforce_min_threshold)
            self.log.debug("Scylla YAML configuration read from: {} {} is:".format(node.public_ip_address, yaml_file))
            node.remoter.run('sudo cat {}'.format(yaml_file))

            node.stop_scylla_server()
            node.start_scylla_server()

    def test_ics_space_amplification_goal(self):  # pylint: disable=too-many-locals  # noqa: PLR0914
        """
        (1) writing new data. wait for compactions to finish.
        (2) over-writing existing data.
        (3) measure space amplification after over-writing with SAG=1.5,1.2,None
        (3.1) The expected disk-usage for the above values is:
        prepare: 1.15TB
        SAG 1.5: 1.8TB
        SAG 1.2: 1.3TB - 1.6TB
        No SAG:  2.2TB
        Example test output:
        Nodes used capacity before start overwriting data: {'10.4.1.43': 1307.47, '10.4.1.34': 1665.81, '10.4.1.144': 1297.73}
        """

        self._set_enforce_min_threshold_true()
        # (1) writing new data.
        prepare_write_cmd = self.params.get('prepare_write_cmd')
        InfoEvent(message=f"Starting C-S prepare load: {prepare_write_cmd}").publish()
        self.run_prepare_write_cmd()
        InfoEvent(message="Wait for compactions to finish after write is done.").publish()
        self.wait_no_compactions_running()

        stress_cmd = self.params.get('stress_cmd')
        sag_testing_values = ['1.5', '1.2', None]
        column_size = 205
        num_of_columns = 5
        # the below number is 1TB (yaml stress cmd total write) in bytes / 205 (column_size) / 5 (num_of_columns)
        overwrite_ops_num = 1072694271
        total_data_to_overwrite_gb = round(overwrite_ops_num * column_size * num_of_columns / (1024 ** 3), 2)
        min_threshold = '4'
        max_delta_capacity = 1.15  # The max allowed used-capacity deviation delta, after a major compaction
        capacity_exceed_msg = "Node {} capacity exceeded allowed delta: {} / {}"
        nodes_original_capacity_with_delta = self._get_nodes_used_capacity()
        for node in nodes_original_capacity_with_delta:
            nodes_original_capacity_with_delta[node] = nodes_original_capacity_with_delta[node] * max_delta_capacity
        # (2) over-writing existing data.
        for sag in sag_testing_values:
            additional_compaction_params = {'min_threshold': min_threshold}
            if sag:
                additional_compaction_params.update({'space_amplification_goal': sag})
            self.log.info('Run a major compaction on nodes and wait for compactions end')
            triggers = [partial(node.run_nodetool, sub_cmd="compact", args=f"{KEYSPACE_NAME} {TABLE_NAME}", ) for
                        node in self.db_cluster.nodes]
            ParallelObject(objects=triggers, timeout=3000).call_objects()
            self.wait_compactions_are_running()
            self.wait_no_compactions_running(n=120)
            dict_nodes_capacity_before_overwrite_data = self._get_nodes_used_capacity()
            InfoEvent(
                message=f"Nodes used capacity before start overwriting data:"
                        f" {dict_nodes_capacity_before_overwrite_data}").publish()
            for node, capacity in dict_nodes_capacity_before_overwrite_data.items():
                if capacity > nodes_original_capacity_with_delta[node]:
                    TestFrameworkEvent(source=self.__class__.__name__,
                                       message=capacity_exceed_msg.format(node, capacity,
                                                                          nodes_original_capacity_with_delta[node]),
                                       severity=Severity.ERROR).publish()

            # (3) Altering compaction with SAG=1.5,1.2,None
            self._alter_table_compaction(additional_compaction_params=additional_compaction_params)
            stress_queue = []
            InfoEvent(message=f"Starting C-S over-write load: {stress_cmd}").publish()

            start_time = time.time()
            params = {'keyspace_num': 1, 'stress_cmd': stress_cmd,
                      'round_robin': self.params.get('round_robin')}
            if not skip_optional_stage('main_load'):
                self._run_all_stress_cmds(stress_queue, params)

            for stress in stress_queue:
                self.verify_stress_thread(stress)

            InfoEvent(message="Wait for compactions to finish after over-write is done.").publish()
            self.wait_no_compactions_running()
            # (3) measure space amplification for the re-written data
            self.measure_nodes_space_amplification_after_write(
                dict_nodes_initial_capacity=dict_nodes_capacity_before_overwrite_data,
                written_data_size_gb=total_data_to_overwrite_gb, start_time=start_time)

            dict_nodes_capacity_after_overwrite_data = self._get_nodes_used_capacity()
            InfoEvent(
                message=f"Nodes used capacity before start overwriting data:"
                        f" {dict_nodes_capacity_after_overwrite_data}").publish()

        InfoEvent(message="Space-amplification-goal testing cycles are done.").publish()
