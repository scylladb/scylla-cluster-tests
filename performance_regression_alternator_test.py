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

from performance_regression_test import PerformanceRegressionTest, PerformanceTestWorkload
from upgrade_test import UpgradeTest
from typing import Optional, Literal

work_types = {
    'READ': 'read', 
    'SCAN': 'read',
    'UPDATE': 'write',
    'INSERT': 'write',
    'DELETE': 'write',
    'WRITE': 'write',
}

class PerformanceRegressionAlternatorTest(PerformanceRegressionTest):
    def setUp(self):
        super().setUp()

        # suppress YCSB client error and timeout to warnings for all the test in this class
        self.stack = contextlib.ExitStack()
        self.stack.enter_context(ignore_alternator_client_errors())
        self.stack.enter_context(ignore_operation_errors())

    # def make_uuid_dir_for_hdr_files(self):
    #     uuid_val = uuid.uuid4()
    #     dir_name = os.path.join(self.loaders.logdir, f'hdrh-{uuid_val}')
    #     self.directory_for_hdr_files = dir_name
    #     os.makedirs(dir_name, exist_ok=True)

    # def remove_old_hdr_files(self, stress_num):
    #     for loader in self.loaders.nodes:
    #         loader_index = loader.node_index
    #         for work_type in work_types:
    #             for stress_idx in range(0, stress_num):
    #                 dst_pth = os.path.join(self.directory_for_hdr_files, f'hdrh-{stress_idx}-{work_type}-{loader_index}.hdr')
    #                 if os.path.isfile(dst_pth):
    #                     self.log.debug(f'Removing hdr file {dst_pth}')
    #                     try:
    #                         os.remove(dst_pth)
    #                     except Exception:
    #                         pass
    #     del self.directory_for_hdr_files

    # def initialize_hdr_loggers(self, stress_num):
    #     self.hdrh_logger_contextes = []
    #     for loader in self.loaders.nodes:
    #         loader_index = loader.node_index
    #         for work_type in work_types:
    #             for stress_idx in range(0, stress_num):
    #                 hdrh_logger = HDRHistogramFileLogger(
    #                     node=loader,
    #                     remote_log_file=f'/tmp/hdr-output-directory/{loader_index}/{stress_idx}/hdrh-{work_type}.hdr',
    #                     target_log_file=os.path.join(self.directory_for_hdr_files, f'hdrh-{stress_idx}-{work_type}-{loader_index}.hdr_'),
    #                 )
    #                 hdrh_logger.start()
    #                 self.hdrh_logger_contextes.append(hdrh_logger)

    # def terminate_hdr_loggers(self):
    #     for hdrh_logger in self.hdrh_logger_contextes:
    #         hdrh_logger.stop()

    # def update_hdr_files(self, stress_num):
    #     allowed_chars = frozenset('+,./0123456789=ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz')
    #     for loader in self.loaders.nodes:
    #         loader_index = loader.node_index
    #         for work_type, tag in work_types.items():
    #             tag_text = f'Tag={tag}'
    #             for stress_idx in range(0, stress_num):
    #                 dst_pth = os.path.join(self.directory_for_hdr_files, f'hdrh-{stress_idx}-{work_type}-{loader_index}.hdr')
    #                 src_pth = dst_pth + '_'
    #                 if os.path.isfile(src_pth):
    #                     with open(src_pth, 'r', encoding='utf8') as input:
    #                         data = input.readlines()
    #                     data2 = []
    #                     ignored_tail_lines = 0
    #                     for d in data:
    #                         d = d.strip()
    #                         if d.startswith('#[Logging for:'):
    #                             if data2:
    #                                 self.log.debug(f'File {src_pth} removing previous content ({len(data2)} lines)')
    #                             data2 = []
    #                         if d.startswith('#') or d.startswith("'"):
    #                             data2.append(d)
    #                         elif d.startswith('tail: '):
    #                             ignored_tail_lines += 1
    #                         else:
    #                             d2 = set(d)
    #                             if d2 - allowed_chars:
    #                                 self.log.warning(f'File {src_pth} ignoring line `{d}`, because of invalid characters')
    #                             else:
    #                                 if d[0].isdigit() or d[0] == '.':
    #                                     data2.append(f'{tag_text},{d}')
    #                                 else:
    #                                     data2.append(d)
    #                     removed = len(data) - len(data2)
    #                     self.log.debug(f'File {src_pth} removed {removed} lines ({ignored_tail_lines} tail lines), left {len(data2)} lines')
    #                     if data2:
    #                         with open(dst_pth, 'w', encoding='utf8') as output:
    #                             for d in data2:
    #                                 output.write(d)
    #                                 output.write('\n')
    #                 else:
    #                     self.log.debug(f'File {src_pth} does not exist, skipping update')

    def _prepare_and_execute_workload_with_latency_calculator_decorator(self, *, test_name, row_name, stress_num=1, **kwargs):
        #self.make_uuid_dir_for_hdr_files()
        self.hdr_tags = ['read', 'write']
        self.row_name_override = row_name
        if test_name.endswith('_read'):
            self.params['workload_name'] = 'read'
            cycle_name = '100% read'
            hdr_workload = PerformanceTestWorkload.READ
        elif test_name.endswith('_write'):
            self.params['workload_name'] = 'write'
            cycle_name = '100% write'
            hdr_workload = PerformanceTestWorkload.WRITE
        elif test_name.endswith('_mixed'):
            self.params['workload_name'] = 'mixed'
            cycle_name = '50% read 50% write'
            hdr_workload = PerformanceTestWorkload.MIXED
        elif test_name.endswith('_throughput'):
            self.params['workload_name'] = 'throughput'
            cycle_name = 'throughput'
            hdr_workload = PerformanceTestWorkload.READ
        else:
            self.log.error(f'unknown test_name {test_name} - some things might not work as expected')
            
        @latency_calculator_decorator(cycle_name=cycle_name)
        def execute_workload_with_latency_calculator_decorator(self, *args, **kwargs):
            return self._workload(*args, **kwargs)

        ret = execute_workload_with_latency_calculator_decorator(self, hdr_workload=hdr_workload, test_name=test_name, stress_num=stress_num, **kwargs)
        #self.remove_old_hdr_files(stress_num=stress_num)
        return ret
    
    def _workload(self, stress_cmd, stress_num=1, test_name=None, sub_type=None, keyspace_num=1, prefix='', debug_message='',  # pylint: disable=too-many-arguments,arguments-differ
                  save_stats=True, is_alternator=True, nemesis=False, hdr_workload=None):
        if not is_alternator:
            stress_cmd = stress_cmd.replace('dynamodb', 'cassandra-cql')

        if debug_message:
            self.log.debug(debug_message)

        if save_stats:
            self.create_test_stats(test_name=test_name, sub_type=sub_type,
                                   doc_id_with_timestamp=True, append_sub_test_to_name=False)
        if nemesis:
            interval = self.params.get('nemesis_interval')
            time.sleep(interval * 60)  # Sleeping one interval (in minutes) before starting the nemesis
            self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(), tester_obj=self)
            self.db_cluster.start_nemesis(interval=interval, cycles_count=1)
            self._stop_load_when_nemesis_threads_end()

        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stress_num=stress_num, keyspace_num=keyspace_num,
                                              prefix=prefix, stats_aggregate_cmds=False)
        self.get_stress_results(queue=stress_queue, store_results=True)
        if self.params['use_hdrhistogram']:
            assert hdr_workload is not None, "hdr_workload must be provided when use_hdrhistogram is True"
            self.build_histogram(hdr_workload, hdr_tags=self.hdr_tags)

        if save_stats:
            self.update_test_details(scylla_conf=True, alternator=is_alternator)


    def create_alternator_table(self, schema, alternator_write_isolation):
        node = self.db_cluster.nodes[0]

        if self.params.get("alternator_enforce_authorization"):
            self.log.info("Create the role for Alternator authorization")
            with self.db_cluster.cql_connection_patient(node) as session:
                session.execute(
                    "CREATE ROLE %s WITH PASSWORD = %s AND login = true AND superuser = true",
                    (
                        self.params.get("alternator_access_key_id"),
                        self.params.get("alternator_secret_access_key"),
                    )
                )

        # drop tables
        table_name = alternator.consts.TABLE_NAME
        if self.alternator.is_table_exists(node=node, table_name=table_name):
            self.alternator.delete_table(node=node, table_name=table_name)
        # create new tables
        self.log.info("Going to create alternator tables")
        self.alternator.create_table(node=node, schema=schema, isolation=alternator_write_isolation)

        self.run_fstrim_on_all_db_nodes()
        self.wait_no_compactions_running()

    def drop_cql_table(self):
        node = self.db_cluster.nodes[0]
        with self.db_cluster.cql_connection_patient(node) as session:
            try:
                session.execute("DROP TABLE IF EXISTS ycsb.usertable;")
                session.execute("DROP KEYSPACE IF EXISTS ycsb;")
            except Exception as e:
                self.log.error(f"Failed to drop CQL table: {e}")
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

            for stress in stress_queue:
                self.get_stress_results(queue=stress, store_results=False)

            self.update_test_details()
        else:
            self.log.warning("No prepare command defined in YAML!")

    def test_write(self):
        """
        Test steps:

        1. Run a write workload with cql
        2. Run a write workload without lwt
        3. Run a write workload with lwt enabled
        """
        # run a write workload
        base_cmd_w = self.params.get('stress_cmd_w')
        stress_multiplier = self.params.get('stress_multiplier')

        self.create_cql_ks_and_table(field_number=10)

        self._workload(sub_type='cql', stress_cmd=base_cmd_w,
                       stress_num=stress_multiplier, keyspace_num=1, is_alternator=False)

        schema = self.params.get("dynamodb_primarykey_type")
        # run a workload without lwt as baseline
        self.create_alternator_table(
            schema=schema, alternator_write_isolation=alternator.enums.WriteIsolation.FORBID_RMW)

        self._workload(sub_type='without-lwt', stress_cmd=base_cmd_w, stress_num=stress_multiplier, keyspace_num=1)

        # run a workload with lwt
        self.create_alternator_table(
            schema=schema, alternator_write_isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
        self._workload(sub_type='with-lwt', stress_cmd=base_cmd_w, stress_num=stress_multiplier, keyspace_num=1)

        self.check_regression_with_baseline('cql')

    def test_read(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a read workload with cql
        3. Run a read workload without lwt
        4. Run a read workload with lwt enabled
        """
        node = self.db_cluster.nodes[0]

        base_cmd_r = self.params.get('stress_cmd_r')
        stress_multiplier = self.params.get('stress_multiplier')
        self.run_fstrim_on_all_db_nodes()
        # run a prepare write workload
        self.create_cql_ks_and_table(field_number=10)

        self.create_alternator_table(schema=self.params.get("dynamodb_primarykey_type"),
                                     alternator_write_isolation=alternator.enums.WriteIsolation.FORBID_RMW)

        self.preload_data()

        self._workload(sub_type='cql', stress_cmd=base_cmd_r,
                       stress_num=stress_multiplier, keyspace_num=1, is_alternator=False)

        # run a workload without lwt as baseline
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
        self._workload(sub_type='without-lwt', stress_cmd=base_cmd_r, stress_num=stress_multiplier, keyspace_num=1)

        # run a workload with lwt
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
        self._workload(sub_type='with-lwt', stress_cmd=base_cmd_r, stress_num=stress_multiplier, keyspace_num=1)

        self.check_regression_with_baseline('cql')

    def test_mixed(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a mixed workload with cql
        3. Run a mixed workload without lwt
        4. Run a mixed workload with lwt
        """
        node = self.db_cluster.nodes[0]

        base_cmd_m = self.params.get('stress_cmd_m')
        stress_multiplier = self.params.get('stress_multiplier')
        self.run_fstrim_on_all_db_nodes()

        self.create_cql_ks_and_table(field_number=10)
        self.create_alternator_table(schema=self.params.get("dynamodb_primarykey_type"),
                                     alternator_write_isolation=alternator.enums.WriteIsolation.FORBID_RMW)

        # run a write workload as a preparation
        self.preload_data()

        self._workload(sub_type='cql', stress_cmd=base_cmd_m,
                       stress_num=stress_multiplier, keyspace_num=1, is_alternator=False)

        # run a mixed workload
        # run a workload without lwt as baseline
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
        self._workload(sub_type='without-lwt', stress_cmd=base_cmd_m, stress_num=stress_multiplier, keyspace_num=1)

        # run a workload with lwt
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
        self._workload(sub_type='with-lwt', stress_cmd=base_cmd_m, stress_num=stress_multiplier, keyspace_num=1)

        self.check_regression_with_baseline('cql')

    def test_basic(self):
        self.run_basic_or_full_test('basic')

    def test_full(self):
        self.run_basic_or_full_test('full')

    def test_basic_read(self):
        self.run_basic_or_full_test('basic-read')

    def test_basic_write(self):
        self.run_basic_or_full_test('basic-write')

    def test_basic_mixed(self):
        self.run_basic_or_full_test('basic-mixed')

    def test_basic_throughput(self):
        self.run_basic_or_full_test('basic-throughput')

    def run_basic_or_full_test(self, mode):
        """
        Test steps:

         1. Prepare cluster with data (reach steady_state of compactions and ~x10 capacity than RAM.)
            with round_robin and list of stress_cmd - the data will load several times faster.
         2. Run READ workload with cql.
         3. Run READ workload without lwt.
         4. Run READ workload with lwt.
         5. Run WRITE workload with cql.
         6. Run WRITE workload without lwt.
         7. Run WRITE workload with lwt.
         8. Run MIXED workload with cql.
         9. Run MIXED workload without lwt.
        10. Run MIXED workload with lwt.
        11. Run READ workload with cql - calculate throughput.
        12. Run READ workload without lwt - calculate throughput.
        13. Run READ workload with lwt - calculate throughput.
        """
        node = self.db_cluster.nodes[0]

        base_cmd_w = self.params.get('stress_cmd_w')
        base_cmd_r = self.params.get('stress_cmd_r')
        base_cmd_m = self.params.get('stress_cmd_m')

        stress_multiplier = 1

        is_basic = mode.startswith('basic')
        run_read = mode == 'full' or mode == 'basic' or mode == 'basic-read'
        run_write = mode == 'full' or mode == 'basic' or mode == 'basic-write'
        run_mixed = mode == 'full' or mode == 'basic' or mode == 'basic-mixed'
        run_throughput = mode == 'full' or mode == 'basic' or mode == 'basic-throoughput'

        try:
            running_time_modifier = float(os.environ.get('ALTERNATOR_RUNNING_TIME_MULTIPLIER', '1'))
            self.log.debug(f'ALTERNATOR_RUNNING_TIME_MULTIPLIER set to value {running_time_modifier}')
        except:
            running_time_modifier = 1.0
            v = os.environ.get("ALTERNATOR_RUNNING_TIME_MULTIPLIER", "1")
            self.log.warning(f'ALTERNATOR_RUNNING_TIME_MULTIPLIER value "{v}", using default value {running_time_modifier}')

        if is_basic:
            cmd_add_params = f" -target 15000 -p maxexecutiontime={int(360 * running_time_modifier)} -p operationcount={int(2000000 * running_time_modifier)}"
            cmd_add_throughput_params = f" -target 999999 -p maxexecutiontime={int(360 * running_time_modifier)} -p operationcount={int(4000000 * running_time_modifier)}"
        else:
            cmd_add_params = f" -target 15000 -p maxexecutiontime={int(2400 * running_time_modifier)} -p operationcount={int(20000000 * running_time_modifier)}"
            cmd_add_throughput_params = f" -target 999999 -p maxexecutiontime={int(2400 * running_time_modifier)} -p operationcount={int(40000000 * running_time_modifier)}"

        def run_read_cql():
            if is_basic: return
            if not run_read: return
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_read', sub_type='cql', stress_cmd=base_cmd_r + cmd_add_params, stress_num=stress_multiplier,
                keyspace_num=1, is_alternator=False, row_name = 'cql')
        def run_read_alternator_no_lwt():
            if not run_read: return
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_read', sub_type='without-lwt', stress_cmd=base_cmd_r + cmd_add_params, stress_num=stress_multiplier,
                keyspace_num=1, row_name = 'alternator-no-lwt')
        def run_read_alternator_with_lwt():
            if not run_read: return
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_read', sub_type='with-lwt', stress_cmd=base_cmd_r + cmd_add_params, stress_num=stress_multiplier,
                keyspace_num=1, row_name = 'alternator-always-lwt')
        def run_write_cql():
            if is_basic: return
            if not run_write: return
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_write', sub_type='cql', stress_cmd=base_cmd_w + cmd_add_params,
                stress_num=stress_multiplier, keyspace_num=1, is_alternator=False, row_name = 'cql')
        def run_write_alternator_no_lwt():
            if not run_write: return
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_write', sub_type='without-lwt', stress_cmd=base_cmd_w + cmd_add_params,
                stress_num=stress_multiplier, keyspace_num=1, row_name = 'alternator-no-lwt')
        def run_write_alternator_with_lwt():
            if not run_write: return
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_write', sub_type='with-lwt', stress_cmd=base_cmd_w + cmd_add_params,
                stress_num=stress_multiplier, keyspace_num=1, row_name = 'alternator-always-lwt')
        def run_mixed_cql():
            if is_basic: return
            if not run_mixed: return
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_mixed', sub_type='cql', stress_cmd=base_cmd_m + cmd_add_params,
                stress_num=stress_multiplier, keyspace_num=1, is_alternator=False, row_name = 'cql')
        def run_mixed_alternator_no_lwt():
            if not run_mixed: return
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(test_name=self.id() + '_mixed', sub_type='without-lwt',
                           stress_cmd=base_cmd_m + cmd_add_params, stress_num=stress_multiplier, keyspace_num=1, row_name = 'alternator-no-lwt')
        def run_mixed_alternator_with_lwt():
            if not run_mixed: return
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(test_name=self.id() + '_mixed', sub_type='with-lwt',
                           stress_cmd=base_cmd_m + cmd_add_params, stress_num=stress_multiplier, keyspace_num=1, row_name = 'alternator-always-lwt')
        def run_throughput_cql():
            if is_basic: return
            if not run_throughput: return
            self._prepare_and_execute_workload_with_latency_calculator_decorator(
                test_name=self.id() + '_throghput', sub_type='cql', stress_cmd=base_cmd_r + cmd_add_throughput_params,
                stress_num=stress_multiplier, keyspace_num=1, is_alternator=False, row_name = 'cql')
        def run_throughput_alternator_no_lwt():
            if not run_throughput: return
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(test_name=self.id() + '_throghput', sub_type='without-lwt',
                           stress_cmd=base_cmd_r + cmd_add_throughput_params, stress_num=stress_multiplier, keyspace_num=1, row_name = 'alternator-no-lwt')
        def run_throughput_alternator_with_lwt():
            if not run_throughput: return
            self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT)
            self._prepare_and_execute_workload_with_latency_calculator_decorator(test_name=self.id() + '_throghput', sub_type='with-lwt',
                           stress_cmd=base_cmd_r + cmd_add_throughput_params, stress_num=stress_multiplier, keyspace_num=1, row_name = 'alternator-always-lwt')

        self.create_alternator_table(schema=self.params.get("dynamodb_primarykey_type"),
                                    alternator_write_isolation=alternator.enums.WriteIsolation.FORBID_RMW)
        self.alternator.set_write_isolation(node=node, isolation=alternator.enums.WriteIsolation.FORBID_RMW)

        self.create_cql_ks_and_table(field_number=10)
        self.run_fstrim_on_all_db_nodes()
        self.preload_data()

        for func in [
            run_read_cql,
            run_read_alternator_no_lwt,
            run_read_alternator_with_lwt,
            run_write_cql,
            run_write_alternator_no_lwt,
            run_write_alternator_with_lwt,
            run_mixed_cql,
            run_mixed_alternator_no_lwt,
            run_mixed_alternator_with_lwt,
            run_throughput_cql,
            run_throughput_alternator_no_lwt,
            run_throughput_alternator_with_lwt
        ]:
            self.wait_no_compactions_running(n=120)
            func()

    def test_latency_write_with_nemesis(self):
        self.create_alternator_table(
            schema=self.params.get("dynamodb_primarykey_type"),
            alternator_write_isolation=alternator.enums.WriteIsolation.FORBID_RMW,
        )
        self.create_cql_ks_and_table(field_number=10)

        self.wait_no_compactions_running()
        self.run_fstrim_on_all_db_nodes()
        self.alternator.set_write_isolation(
            node=self.db_cluster.nodes[0],
            isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT,
        )
        self._workload(
            test_name=self.id() + "_write",
            sub_type="with-lwt",
            stress_cmd=self.params.get("stress_cmd_w") + " -target 3000",
            nemesis=True,
        )
        self.check_latency_during_ops()

    def test_latency_read_with_nemesis(self):
        self.create_alternator_table(
            schema=self.params.get("dynamodb_primarykey_type"),
            alternator_write_isolation=alternator.enums.WriteIsolation.FORBID_RMW,
        )
        self.create_cql_ks_and_table(field_number=10)

        # Run a write workload as a preparation.
        self.preload_data()

        self.wait_no_compactions_running()
        self.run_fstrim_on_all_db_nodes()
        self.alternator.set_write_isolation(
            node=self.db_cluster.nodes[0],
            isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT,
        )
        self._workload(
            test_name=self.id() + "_read",
            sub_type="with-lwt",
            stress_cmd=self.params.get("stress_cmd_r") + " -target 3000",
            nemesis=True,
        )
        self.check_latency_during_ops()

    def test_latency_mixed_with_nemesis(self):
        self.create_alternator_table(
            schema=self.params.get("dynamodb_primarykey_type"),
            alternator_write_isolation=alternator.enums.WriteIsolation.FORBID_RMW,
        )
        self.create_cql_ks_and_table(field_number=10)

        # Run a write workload as a preparation.
        self.preload_data()

        self.wait_no_compactions_running()
        self.run_fstrim_on_all_db_nodes()
        self.alternator.set_write_isolation(
            node=self.db_cluster.nodes[0],
            isolation=alternator.enums.WriteIsolation.ALWAYS_USE_LWT,
        )
        self._workload(
            test_name=self.id() + "_mixed",
            sub_type="with-lwt",
            stress_cmd=self.params.get("stress_cmd_m") + " -target 3000",
            nemesis=True,
        )
        self.check_latency_during_ops()


class PerformanceRegressionAlternatorUpgradeTest(PerformanceRegressionAlternatorTest, UpgradeTest):  # pylint: disable=too-many-ancestors
    def get_email_data(self) -> dict:
        return PerformanceRegressionAlternatorTest.get_email_data(self)

    @latency_calculator_decorator(legend="Upgrade Node")
    def upgrade_node(self, node) -> None:  # pylint: disable=arguments-differ
        InfoEvent(message=f"Upgrade Node {node.name} begin").publish()
        self._upgrade_node(node)
        InfoEvent(message=f"Upgrade Node {node.name} ended").publish()

    def _stop_stress_when_finished(self) -> None:  # pylint: disable=no-self-use
        with EventsSeverityChangerFilter(new_severity=Severity.NORMAL,  # killing stress creates Critical error
                                         event_class=YcsbStressEvent,
                                         extra_time_to_expiration=60):
            self.loaders.kill_stress_thread()

    @latency_calculator_decorator
    def steady_state_latency(self) -> None:  # pylint: disable=no-self-use
        sleep_time = self.db_cluster.params.get("nemesis_interval") * 60
        InfoEvent(message=f"Starting Steady State calculation for {sleep_time}s").publish()
        time.sleep(sleep_time)
        InfoEvent(message=f"Ended Steady State calculation. Took {sleep_time}s").publish()

    @latency_calculator_decorator
    def post_upgrades_steady_state(self) -> None:
        sleep_time = self.db_cluster.params.get("nemesis_interval") * 60
        InfoEvent(message=f"Starting Post-Upgrade Steady State calculation for {sleep_time}s").publish()
        time.sleep(sleep_time)
        InfoEvent(message=f"Ended Post-Upgrade Steady State calculation. Took {sleep_time}s").publish()

    def run_workload_and_upgrade(self, stress_cmd: str, sub_type: str) -> None:
        # next 3 lines, is a workaround to have it working inside `latency_calculator_decorator`
        self.cluster = self.db_cluster  # pylint: disable=attribute-defined-outside-init
        self.tester = self  # pylint: disable=attribute-defined-outside-init
        self.monitoring_set = self.monitors  # pylint: disable=attribute-defined-outside-init

        test_index = f"alternator-latency-during-upgrade-{sub_type}"
        self.create_test_stats(
            sub_type=sub_type,
            append_sub_test_to_name=False,
            test_index=test_index,
        )
        stress_queue = self.run_stress_thread(
            stress_cmd=stress_cmd,
            stats_aggregate_cmds=False,
        )
        time.sleep(60)  # postpone measure steady state latency to skip start period when latency is high
        self.steady_state_latency()
        versions_list = []

        def _get_version_and_build_id_from_node(scylla_node) -> tuple[str, str]:
            version = scylla_node.remoter.run("scylla --version")
            build_id = scylla_node.remoter.run("scylla --build-id")
            return version.stdout.strip(), build_id.stdout.strip()

        for node in self.db_cluster.nodes:
            base_version, base_build_id = _get_version_and_build_id_from_node(node)
            self.upgrade_node(node)
            target_version, target_build_id = _get_version_and_build_id_from_node(node)
            versions_list.append({
                "base_version": base_version,
                "base_build_id": base_build_id,
                "target_version": target_version,
                "target_build_id": target_build_id,
                "node_name": node.name,
            })
            time.sleep(120)  # sleeping 2 min to give time for cache to re-heat
        self.post_upgrades_steady_state()

        # TODO: check if all `base_version` and all `target_version` are the same
        self.update({"base_target_versions": versions_list})
        self._stop_stress_when_finished()
        results = self.get_stress_results(queue=stress_queue)
        self.update_test_details(scrap_metrics_step=60, alternator=True)
        self.display_results(results, test_name="test_alternator_latency_with_upgrade")
        self.update_test_details(scrap_metrics_step=60, alternator=True)
        self.display_results(results, test_name="test_alternator_latency_during_upgrade")
        self.check_latency_during_ops()

    def _prepare_latency_with_upgrade(self) -> None:
        self.run_fstrim_on_all_db_nodes()
        self.create_alternator_table(
            schema=self.params.get("dynamodb_primarykey_type"),
            alternator_write_isolation=alternator.enums.WriteIsolation.FORBID_RMW,
        )
        self.create_cql_ks_and_table(field_number=10)
        self.preload_data()
        self.wait_no_compactions_running()
        self.run_fstrim_on_all_db_nodes()

    def test_latency_write_with_upgrade(self) -> None:
        self._prepare_latency_with_upgrade()
        self.run_workload_and_upgrade(
            stress_cmd=self.params.get("stress_cmd_w") + " -target 3000",
            sub_type="write",
        )

    def test_latency_read_with_upgrade(self) -> None:
        self._prepare_latency_with_upgrade()
        self.run_workload_and_upgrade(
            stress_cmd=self.params.get("stress_cmd_r") + " -target 3000",
            sub_type="read",
        )

    def test_latency_mixed_with_upgrade(self) -> None:
        self._prepare_latency_with_upgrade()
        self.run_workload_and_upgrade(
            stress_cmd=self.params.get("stress_cmd_m") + " -target 3000",
            sub_type="mixed",
        )
