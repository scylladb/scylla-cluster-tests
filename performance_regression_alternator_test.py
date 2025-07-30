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

import time
import contextlib
import os
import traceback
import re
import uuid

from sdcm.utils import alternator
from sdcm.utils.decorators import optional_stage, latency_calculator_decorator
from sdcm.sct_events import Severity
from sdcm.sct_events.system import InfoEvent
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.sct_events.loaders import YcsbStressEvent
from sdcm.sct_events.group_common_events import ignore_operation_errors, ignore_alternator_client_errors

from performance_regression_test import PerformanceRegressionTest
from upgrade_test import UpgradeTest
from typing import Optional, Literal

class PerformanceRegressionAlternatorTest(PerformanceRegressionTest):
    '''
Performance tests for alternator.
Following workloads are supposed:
- test_full - all tests below
- test_latency - all latency tests (read write mixed)
- test_latency_read(self) - read only latency tests
- test_latency_write(self) - write only latency tests
- test_latency_mixed - read 50% write 50% latency tests
- test_throughput - all throughput tests
- test_throughput_read - read only throughput tests
- test_throughput_write - write only throughput tests
all tests are run with cql and alternator, with FORBID_RMW isolation and with ALWAYS_USE_LWT
    '''
    def setUp(self):
        super().setUp()

        # suppress YCSB client error and timeout to warnings for all the test in this class
        self.stack = contextlib.ExitStack()
        self.stack.enter_context(ignore_alternator_client_errors())
        self.stack.enter_context(ignore_operation_errors())

    def _prepare_and_execute_workload_with_latency_calculator_decorator(self, *, test_name, row_name, stress_num=1, **kwargs):
        # test_name must end with one of '_read', '_write', '_mixed', '_throughput_read', '_throughput_write'
        # indicating the type of workload that will be run
        if test_name.endswith('_throughput_read'):
            self.params['workload_name'] = 'throughput'
            cycle_name = 'throughput-read'
        elif test_name.endswith('_throughput_write'):
            self.params['workload_name'] = 'throughput'
            cycle_name = 'throughput-write'
        elif test_name.endswith('_read'):
            self.params['workload_name'] = 'read'
            cycle_name = '100% read'
        elif test_name.endswith('_write'):
            self.params['workload_name'] = 'write'
            cycle_name = '100% write'
        elif test_name.endswith('_mixed'):
            self.params['workload_name'] = 'mixed'
            cycle_name = '50% read 50% write'
        else:
            raise ValueError(f"Unknown test_name {test_name} for workload, only test_name values ending with '_read', '_write', '_mixed', '_throughput_read', or '_throughput_write' are supported.")

        @latency_calculator_decorator(cycle_name=cycle_name, row_name=row_name)
        def execute_workload_with_latency_calculator_decorator(self, *args, **kwargs):
            return self._workload(*args, **kwargs)

        ret = execute_workload_with_latency_calculator_decorator(self, test_name=test_name, stress_num=stress_num, **kwargs)
        return ret
    
    def _workload(self, stress_cmd, stress_num, test_name=None, sub_type=None, keyspace_num=1, prefix='', debug_message='',
                  save_stats=True, is_alternator=True):
        if not is_alternator:
            stress_cmd = stress_cmd.replace('dynamodb', 'cassandra-cql')

        if debug_message:
            self.log.debug(debug_message)

        if save_stats:
            self.create_test_stats(test_name=test_name, sub_type=sub_type,
                                   doc_id_with_timestamp=True, append_sub_test_to_name=False)
        self.log.info(f'Starting stress cmd: {stress_cmd}')
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stress_num=stress_num, keyspace_num=keyspace_num,
                                              prefix=prefix, stats_aggregate_cmds=False)
        self.log.info(f'Started stress cmd: {stress_cmd}, waiting for results')
        try:
            self.get_stress_results(queue=stress_queue, store_results=True)
        except:
            self.log.exception(f'Stress cmd failed: {stress_cmd}')
            self.log.error(traceback.format_exc())
            raise
        self.log.info(f'Completed stress cmd: {stress_cmd}')
        self.build_histogram('<unused>', hdr_tags=self.hdr_tags)
        if save_stats:
            self.update_test_details(scylla_conf=True, alternator=is_alternator)

    def create_cql_ks_and_table(self, field_number):
        node = self.db_cluster.nodes[0]
        with self.db_cluster.cql_connection_patient(node) as session:
            session.execute(
                """create keyspace ycsb WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'replication_factor': 3 };""")
            fields = ', '.join([f"field{i} varchar" for i in range(field_number)])
            session.execute(f"""CREATE TABLE ycsb.usertable (
                                y_id varchar primary key,
                                {fields});""")

    @optional_stage('perf_preload_data')
    def preload_data(self, compaction_strategy=None):
        # if test require a pre-population of data
        prepare_write_cmd = self.params.get('prepare_write_cmd')
        self.log.info(f'Preloading data with `{prepare_write_cmd}` write command.')
        if prepare_write_cmd:
            # create new document in ES with doc_id = test_id + timestamp
            # allow to correctly save results for future compare
            self.create_test_stats(sub_type='write-prepare', doc_id_with_timestamp=True)
            stress_queue = []
            params = {'prefix': 'preload-'}
            for stress_type in ['dynamodb', 'cassandra-cql']:
                # Check if the prepare_cmd is a list of commands
                if not isinstance(prepare_write_cmd, str) and len(prepare_write_cmd) > 1:
                    # Check if it should be round_robin across loaders
                    if self.params.get('round_robin'):
                        self.log.debug('Populating data using round_robin')
                        params.update({'stress_num': 1, 'round_robin': True})

                    for stress_cmd in prepare_write_cmd:
                        params.update({
                            'stress_cmd': stress_cmd.replace('dynamodb', stress_type),
                            'duration': self.params.get('prepare_stress_duration'),
                        })

                        # Run all stress commands
                        params.update(dict(stats_aggregate_cmds=False))
                        self.log.debug('RUNNING stress cmd: {}'.format(stress_cmd.replace('dynamodb', stress_type)))
                        stress_queue.append(self.run_stress_thread(**params))

                # One stress cmd command
                else:
                    stress_queue.append(self.run_stress_thread(
                        stress_cmd=prepare_write_cmd.replace('dynamodb', stress_type),
                        duration=self.params.get('prepare_stress_duration'),
                        stress_num=1,
                        prefix='preload-',
                        stats_aggregate_cmds=False,
                    ))

            self.log.debug(f'Waiting for loaders...')
            for stress in stress_queue:
                self.get_stress_results(queue=stress, store_results=False)
            self.log.debug(f'Loaders completed.')
            self.build_histogram('<unused>', hdr_tags=['_tag_'])
            self.update_test_details()
        else:
            self.log.warning("No prepare command defined in YAML!")

    def test_full(self):
        self.run_test_suite_by_configuration_name('full')

    def test_latency(self):
        self.run_test_suite_by_configuration_name('basic-read')

    def test_latency_read(self):
        self.run_test_suite_by_configuration_name('basic-read')

    def test_latency_write(self):
        self.run_test_suite_by_configuration_name('basic-write')

    def test_latency_mixed(self):
        self.run_test_suite_by_configuration_name('basic-mixed')

    def test_throughput(self):
        self.run_test_suite_by_configuration_name('basic-throughput')

    def test_throughput_read(self):
        self.run_test_suite_by_configuration_name('basic-throughput-read')

    def test_throughput_write(self):
        self.run_test_suite_by_configuration_name('basic-throughput-write')

    def run_test_suite_by_configuration_name(self, mode):
        """
        Test steps:

         1. Prepare cluster with data.
         2. Run READ workload with cql.
         3. Run READ workload without lwt.
         4. Run WRITE workload with cql.
         5. Run WRITE workload without lwt.
         6. Run WRITE workload with lwt.
         7. Run MIXED workload with cql.
         8. Run MIXED workload without lwt.
         9. Run MIXED workload with lwt.
        10. Run READ workload with cql - calculate throughput.
        11. Run READ workload without lwt - calculate throughput.
        12. Run WRITE workload with cql - calculate throughput.
        13. Run WRITE workload without lwt - calculate throughput.

        Tests 2-9 are run with fixed duration and fixed op rate, P90 and P99 are measured and returned to Argus.
        Tests 10-13 are run with fixed duration, without throughput throttling. P90 and P99 are ignored (they will be off the chart),
        throughput is measured and returned to Argus.
        """
        node = self.db_cluster.nodes[0]

        base_cmd_w = self.params.get('stress_cmd_w')
        base_cmd_r = self.params.get('stress_cmd_r')
        base_cmd_m = self.params.get('stress_cmd_m')

        is_basic = mode.startswith('basic')
        run_read = mode in ('full', 'basic', 'basic-read')
        run_write = mode in ('full', 'basic', 'basic-write')
        run_mixed = mode in ('full', 'basic', 'basic-mixed')
        run_read_throughput = mode in ('full', 'basic', 'basic-throoughput', 'basic-throughput-read')
        run_write_throughput = mode in ('full', 'basic', 'basic-throoughput', 'basic-throughput-write')
        self.hdr_tags = [ 'read', 'write' ]

        def run_read_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_read', sub_type='cql', stress_cmd=base_cmd_r + cmd_add_params, stress_num=1,
                keyspace_num=1, is_alternator=False, row_name = 'cql')

        def run_read_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_read', sub_type='without-lwt', stress_cmd=base_cmd_r + cmd_add_params, stress_num=1,
                keyspace_num=1, row_name = 'alternator-no-lwt')

        def run_write_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_write', sub_type='cql', stress_cmd=base_cmd_w + cmd_add_params,
                stress_num=1, keyspace_num=1, is_alternator=False, row_name = 'cql')

        def run_write_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_write', sub_type='without-lwt', stress_cmd=base_cmd_w + cmd_add_params,
                stress_num=1, keyspace_num=1, row_name = 'alternator-no-lwt')

        def run_write_alternator_with_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_write', sub_type='with-lwt', stress_cmd=base_cmd_w + cmd_add_params,
                stress_num=1, keyspace_num=1, row_name = 'alternator-always-lwt')

        def run_mixed_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_mixed', sub_type='cql', stress_cmd=base_cmd_m + cmd_add_params,
                stress_num=1, keyspace_num=1, is_alternator=False, row_name = 'cql')

        def run_mixed_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(test_name=self.id() + '_mixed', sub_type='without-lwt',
                           stress_cmd=base_cmd_m + cmd_add_params, stress_num=1, keyspace_num=1, row_name = 'alternator-no-lwt')

        def run_mixed_alternator_with_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(test_name=self.id() + '_mixed', sub_type='with-lwt',
                           stress_cmd=base_cmd_m + cmd_add_params, stress_num=1, keyspace_num=1, row_name = 'alternator-always-lwt')

        def run_read_throughput_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_throughput_read', sub_type='cql', stress_cmd=base_cmd_r + cmd_add_throughput_params,
                stress_num=1, keyspace_num=1, is_alternator=False, row_name = 'cql')

        def run_read_throughput_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(test_name=self.id() + '_throughput_read', sub_type='without-lwt',
                           stress_cmd=base_cmd_r + cmd_add_throughput_params, stress_num=1, keyspace_num=1, row_name = 'alternator-no-lwt')

        def run_write_throughput_cql():
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_throughput_write', sub_type='cql', stress_cmd=base_cmd_w + cmd_add_throughput_params,
                stress_num=1, keyspace_num=1, is_alternator=False, row_name = 'cql')

        def run_write_throughput_alternator_no_lwt():
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(test_name=self.id() + '_throughput_write', sub_type='without-lwt',
                           stress_cmd=base_cmd_w + cmd_add_throughput_params, stress_num=1, keyspace_num=1, row_name = 'alternator-no-lwt')

        tests_to_run = tuple( test for test, condition in (
            ( run_read_cql, not is_basic and run_read),
            ( run_read_alternator_no_lwt, run_read),
            ( run_write_cql, not is_basic and run_write),
            ( run_write_alternator_no_lwt, run_write),
            ( run_write_alternator_with_lwt, run_write),
            ( run_mixed_cql, not is_basic and run_mixed),
            ( run_mixed_alternator_no_lwt, run_mixed),
            ( run_mixed_alternator_with_lwt, run_mixed),
            ( run_read_throughput_cql, not is_basic and run_read_throughput),
            ( run_read_throughput_alternator_no_lwt, run_read_throughput),
            ( run_write_throughput_cql, not is_basic and run_write_throughput),
            ( run_write_throughput_alternator_no_lwt, run_write_throughput),
        ) if condition )
        
        single_test_duration_in_seconds = int(60 * self.params.get('stress_duration') / len(tests_to_run))
        target_ops_per_sec_for_unlimited_scenario = 999999
        try:
            target_ops_per_sec = self.params.get('alternator_stress_rate')
            self.log.info(f"Using target {target_ops_per_sec} ops/s for stress tests.")
        except KeyError:
            target_ops_per_sec = 15000
            self.log.info(f"Parameter alternator_stress_rate not found, using default target {target_ops_per_sec} ops/s for stress tests.")
        if single_test_duration_in_seconds < 60:
            self.log.warning(f"Increasing single stress duration to 1 minute - after calculating got {self.params.get('stress_duration')}s. Total runtime will be around {len(tests_to_run)} minutes.")
            single_test_duration_in_seconds = 60
        cmd_add_params = f" -target {target_ops_per_sec} -p maxexecutiontime={single_test_duration_in_seconds}"
        cmd_add_throughput_params = f" -target {target_ops_per_sec_for_unlimited_scenario} -p maxexecutiontime={single_test_duration_in_seconds}"

        self.pre_create_alternator_tables()
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)

        self.create_cql_ks_and_table(field_number=10)
        self.run_fstrim_on_all_db_nodes()
        self.preload_data()

        for func in tests_to_run:
            self.wait_no_compactions_running(n=120, sleep_time=10)
            func()

