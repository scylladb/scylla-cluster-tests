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
# Copyright (c) 2016 ScyllaDB


import os
import time
import yaml

from sdcm.sct_events.system import InfoEvent
from sdcm.tester import ClusterTester

KB = 1024


class BasePerformanceRegression(ClusterTester):
    str_pattern = '%8s%16s%10s%14s%16s%12s%12s%14s%16s%16s'

    def __init__(self, *args):
        # need to remove the email_data.json file, as in the builders, it will accumulate and it will send multiple
        # emails for each test. When we move to use SCT Runners, it won't be necessary.
        self._clean_email_data()
        super().__init__(*args)

    @staticmethod
    def _clean_email_data():
        email_data_path = 'email_data.json'
        with open(file=email_data_path, mode='w', encoding="utf-8"):
            pass

    def preload_data(self, prepare_write_cmd: str = ""):
        # if test require a pre-population of data
        if not prepare_write_cmd:
            self.log.warning("No prepare command defined in YAML!")
            return

        # create new document in ES with doc_id = test_id + timestamp
        # allow to correctly save results for future compare
        self.create_test_stats(sub_type='write-prepare', doc_id_with_timestamp=True)
        stress_queue = []
        params = {'prefix': 'preload-'}
        # Check if the prepare_cmd is a list of commands
        if isinstance(prepare_write_cmd, list) and len(prepare_write_cmd) == 1:
            # One stress cmd command
            stress_queue.append(
                self.run_stress_thread(stress_cmd=prepare_write_cmd[0], stress_num=1, prefix='preload-',
                                       stats_aggregate_cmds=False))
            prepare_write_cmd = prepare_write_cmd[0]
        elif isinstance(prepare_write_cmd, list):
            # Check if it should be round_robin across loaders
            if self.params.get('round_robin'):
                self.log.debug('Populating data using round_robin')
                params.update({'stress_num': 1, 'round_robin': True})

            for stress_cmd in prepare_write_cmd:
                params.update({'stress_cmd': stress_cmd})
                # Run all stress commands
                params.update(dict(stats_aggregate_cmds=False))
                self.log.debug('RUNNING stress cmd: {}'.format(stress_cmd))
                stress_queue.append(self.run_stress_thread(**params))

        for stress in stress_queue:
            self.get_stress_results(queue=stress, store_results=False)

        self.update_test_details()

    # Helpers
    def display_single_result(self, result):
        self.log.info(self.str_pattern, result['op rate'],
                      result['partition rate'],
                      result['row rate'],
                      result['latency mean'],
                      result['latency median'],
                      result['latency 95th percentile'],
                      result['latency 99th percentile'],
                      result['latency 99.9th percentile'],
                      result['Total partitions'],
                      result['Total errors'])

    def get_test_xml(self, result, test_name=''):
        test_content = """
  <test name="%s: (%s) Loader%s CPU%s Keyspace%s" executed="yes">
    <description>"%s test, ami_id: %s, scylla version:
    %s", hardware: %s</description>
    <targets>
      <target threaded="yes">target-ami_id-%s</target>
      <target threaded="yes">target-version-%s</target>
    </targets>
    <platform name="AWS platform">
      <hardware>%s</hardware>
    </platform>

    <result>
      <success passed="yes" state="1"/>
      <performance unit="kbs" mesure="%s" isRelevant="true" />
      <metrics>
        <op-rate unit="op/s" mesure="%s" isRelevant="true" />
        <partition-rate unit="pk/s" mesure="%s" isRelevant="true" />
        <row-rate unit="row/s" mesure="%s" isRelevant="true" />
        <latency-mean unit="mean" mesure="%s" isRelevant="true" />
        <latency-median unit="med" mesure="%s" isRelevant="true" />
        <l-95th-pct unit=".95" mesure="%s" isRelevant="true" />
        <l-99th-pct unit=".99" mesure="%s" isRelevant="true" />
        <l-99.9th-pct unit=".999" mesure="%s" isRelevant="true" />
        <total_partitions unit="total_partitions" mesure="%s" isRelevant="true" />
        <total_errors unit="total_errors" mesure="%s" isRelevant="true" />
      </metrics>
    </result>
  </test>
""" % (test_name, result['loader_idx'],
            result['loader_idx'],
            result['cpu_idx'],
            result['keyspace_idx'],
            test_name,
            self.params.get('ami_id_db_scylla'),
            self.params.get('ami_id_db_scylla_desc'),
            self.params.get('instance_type_db'),
            self.params.get('ami_id_db_scylla'),
            self.params.get('ami_id_db_scylla_desc'),
            self.params.get('instance_type_db'),
            result['op rate'],
            result['op rate'],
            result['partition rate'],
            result['row rate'],
            result['latency mean'],
            result['latency median'],
            result['latency 95th percentile'],
            result['latency 99th percentile'],
            result['latency 99.9th percentile'],
            result['Total partitions'],
            result['Total errors'])

        return test_content

    def display_results(self, results, test_name=''):
        self.log.info(self.str_pattern, 'op-rate', 'partition-rate',
                      'row-rate', 'latency-mean',
                      'latency-median', 'l-94th-pct',
                      'l-99th-pct', 'l-99.9th-pct',
                      'total-partitions', 'total-err')

        test_xml = ""
        try:
            for single_result in results:
                self.display_single_result(single_result)
                test_xml += self.get_test_xml(single_result, test_name=test_name)

            file_path = os.path.join(self.logdir, 'jenkins_perf_PerfPublisher.xml')
            with open(file=file_path, mode='w', encoding='utf-8') as pref_file:
                content = """<report name="%s report" categ="none">%s</report>""" % (test_name, test_xml)
                pref_file.write(content)
        except Exception as ex:  # pylint: disable=broad-except
            self.log.debug('Failed to display results: {0}'.format(results))
            self.log.debug('Exception: {0}'.format(ex))

    def _run_workload(self, stress_cmd: str, sub_type: str, nemesis: bool = False,  # pylint: disable=too-many-arguments
                      scylla_conf: bool = False, scrap_metrics_step: int = None):
        # create new document in ES with doc_id = test_id + timestamp
        # allow to correctly save results for future compare
        self.create_test_stats(sub_type=sub_type, doc_id_with_timestamp=True)
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stress_num=1, stats_aggregate_cmds=False)
        if nemesis:
            interval = self.params.get('nemesis_interval')
            time.sleep(interval * 60)  # Sleeping one interval (in minutes) before starting the nemesis
            self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(), tester_obj=self)
            self.db_cluster.start_nemesis(interval=interval)
        results = self.get_stress_results(queue=stress_queue)
        self.update_test_details(scylla_conf=scylla_conf, scrap_metrics_step=scrap_metrics_step)
        self.display_results(results, test_name='test_latency' if not nemesis else 'test_latency_with_nemesis')
        if nemesis:
            self.check_latency_during_ops()
        else:
            self.check_regression()

    def run_workload(self, stress_cmd: str, sub_type: str, nemesis: bool = False,  # pylint: disable=too-many-arguments
                     scylla_conf: bool = False, scrap_metrics_step: int = None):
        self.wait_no_compactions_running()
        self.run_fstrim_on_all_db_nodes()
        self._run_workload(stress_cmd=stress_cmd, sub_type=sub_type, nemesis=nemesis, scylla_conf=scylla_conf,
                           scrap_metrics_step=scrap_metrics_step)

    def _latency_read_with_nemesis(self, stress_cmd: str, sub_type: str, scrap_metrics_step: int = 60):
        self.run_fstrim_on_all_db_nodes()
        self.preload_data()
        self.run_workload(stress_cmd=self.params.get(stress_cmd), sub_type=sub_type, nemesis=True,
                          scrap_metrics_step=scrap_metrics_step)


class PerformanceRegressionTest(BasePerformanceRegression):  # pylint: disable=too-many-public-methods

    """
    Test Scylla performance regression with cassandra-stress.
    """

    ops_threshold_prc = 200

    def _workload(self, stress_cmd, stress_num, test_name, sub_type=None, keyspace_num=1, prefix='', debug_message='',  # pylint: disable=too-many-arguments
                  save_stats=True):
        if debug_message:
            self.log.debug(debug_message)

        if save_stats:
            if not self.exists():
                self.create_test_stats(sub_type=sub_type)
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stress_num=stress_num, keyspace_num=keyspace_num,
                                              prefix=prefix, stats_aggregate_cmds=False)
        results = self.get_stress_results(queue=stress_queue, store_results=True)
        if save_stats:
            self.update_test_details(scylla_conf=True)
            self.display_results(results, test_name=test_name)
            self.check_regression()
            total_ops = self._get_total_ops()
            self.log.debug('Total ops: {}'.format(total_ops))
            return total_ops
        return None

    def _get_total_ops(self):
        return self._stats['results']['stats_total']['op rate']

    def prepare_mv(self, on_populated=False):
        with self.db_cluster.cql_connection_patient_exclusive(self.db_cluster.nodes[0]) as session:

            ks_name = 'keyspace1'
            base_table_name = 'standard1'
            if not on_populated:
                # Truncate base table before materialized view creation
                self.log.debug('Truncate base table: {0}.{1}'.format(ks_name, base_table_name))
                self.truncate_cf(ks_name, base_table_name, session)

            # Create materialized view
            view_name = base_table_name + '_mv'
            self.log.debug('Create materialized view: {0}.{1}'.format(ks_name, view_name))
            self.create_materialized_view(ks_name, base_table_name, view_name, ['"C0"'], ['key'], session,
                                          mv_columns=['"C0"', 'key'])

            # Wait for the materialized view is built
            self._wait_for_view(self.db_cluster, session, ks_name, view_name)

    def _write_with_mv(self, on_populated):
        """
        Test steps:

        1. Run a write workload
        2. Create materialized view
        3. Run a write workload
        """
        test_name = 'test_write_with_mv_{}populated'.format('' if on_populated else 'not_')
        base_cmd_w = self.params.get('stress_cmd_w')

        # Run a write workload without MV
        ops_without_mv = self._workload(stress_cmd=base_cmd_w, stress_num=2, sub_type='write_without_mv',
                                        test_name=test_name, keyspace_num=1,
                                        debug_message='First write cassandra-stress command: {}'.format(base_cmd_w))

        # Create MV
        self.prepare_mv(on_populated=on_populated)

        # Start cassandra-stress writes again now with MV
        ops_with_mv = self._workload(stress_cmd=base_cmd_w, stress_num=2, sub_type='write_with_mv',
                                     test_name=test_name, keyspace_num=1,
                                     debug_message='Second write cassandra-stress command: {}'.format(base_cmd_w))

        self.assert_mv_performance(ops_without_mv, ops_with_mv,
                                   'Throughput of run with materialized view is more than {} times lower then '
                                   'throughput of run without materialized view'.format(self.ops_threshold_prc/100))

    def _read_with_mv(self, on_populated):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a read workload
        3. Create MV
        4. Run a read workload again
        """
        test_name = 'test_read_with_mv_{}populated'.format('' if on_populated else 'not_')
        base_cmd_p = self.params.get('prepare_write_cmd')
        base_cmd_w = self.params.get('stress_cmd_w')
        base_cmd_r = self.params.get('stress_cmd_r')

        self.create_test_stats()
        # prepare schema and data before read
        self._workload(stress_cmd=base_cmd_p, stress_num=2, test_name=test_name, prefix='preload-', keyspace_num=1,
                       debug_message='Prepare the test, run cassandra-stress command: {}'.format(base_cmd_p),
                       save_stats=False)

        # run a read workload
        ops_without_mv = self._workload(stress_cmd=base_cmd_r, stress_num=2, sub_type='read_without_mv',
                                        test_name=test_name, keyspace_num=1,
                                        debug_message='First read cassandra-stress command: {}'.format(base_cmd_r))

        self.prepare_mv(on_populated=on_populated)

        # If the MV was created on the empty base table, populate it before reads
        if not on_populated:
            self._workload(stress_cmd=base_cmd_w, stress_num=2, test_name=test_name, prefix='preload-', keyspace_num=1,
                           debug_message='Prepare test before second cassandra-stress command: {}'.format(base_cmd_w),
                           save_stats=False)

        # run a read workload
        ops_with_mv = self._workload(stress_cmd=base_cmd_r, stress_num=2, sub_type='read_with_mv',
                                     test_name=test_name, keyspace_num=1,
                                     debug_message='Second read cassandra-stress command: {}'.format(base_cmd_r))

        self.assert_mv_performance(ops_without_mv, ops_with_mv,
                                   'Throughput of run with materialized view is more than {} times lower then '
                                   'throughput of run without materialized view'.format(self.ops_threshold_prc/100))

    def _mixed_with_mv(self, on_populated):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a mixed workload
        """
        test_name = 'test_mixed_with_mv_{}populated'.format('' if on_populated else 'not_')
        base_cmd_p = self.params.get('prepare_write_cmd')
        base_cmd_m = self.params.get('stress_cmd_m')

        self.create_test_stats()
        # run a write workload as a preparation
        self._workload(stress_cmd=base_cmd_p, stress_num=2, test_name=test_name, keyspace_num=1, prefix='preload-',
                       debug_message='Prepare the test, run cassandra-stress command: {}'.format(base_cmd_p),
                       save_stats=False)

        # run a mixed workload without MV
        ops_without_mv = self._workload(stress_cmd=base_cmd_m, stress_num=2, sub_type='mixed_without_mv',
                                        test_name=test_name, keyspace_num=1,
                                        debug_message='First mixed cassandra-stress command: {}'.format(base_cmd_m))

        self.prepare_mv(on_populated=on_populated)

        # run a mixed workload with MV
        ops_with_mv = self._workload(stress_cmd=base_cmd_p, stress_num=2, sub_type='mixed_with_mv',
                                     test_name=test_name, keyspace_num=1,
                                     debug_message='Second start of mixed cassandra-stress command: {}'.format(
                                         base_cmd_p))

        self.assert_mv_performance(ops_without_mv, ops_with_mv,
                                   'Throughput of stress run with materialized view is more than {} times lower then '
                                   'throughput of stress run without materialized view'.format(
                                       self.ops_threshold_prc / 100))

    def assert_mv_performance(self, ops_without_mv, ops_with_mv, failure_message):
        self.log.debug('Performance results. Ops without MV: {0}; Ops with MV: {1}'.format(ops_without_mv, ops_with_mv))
        self.assertLessEqual(ops_without_mv, (ops_with_mv * self.ops_threshold_prc) / 100, failure_message)

    def _scylla_bench_prepare_table(self):
        node = self.db_cluster.nodes[0]
        with self.db_cluster.cql_connection_patient(node) as session:
            session.execute("""
                CREATE KEYSPACE scylla_bench WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}
                AND durable_writes = true;
            """)
            session.execute("""
                CREATE TABLE scylla_bench.test (
                    pk bigint,
                    ck bigint,
                    v blob,
                    PRIMARY KEY (pk, ck)
                ) WITH CLUSTERING ORDER BY (ck ASC)
                    AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '60',
                    'compaction_window_unit': 'MINUTES'}
                    AND bloom_filter_fp_chance = 0.01
                    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
                    AND comment = ''
                    AND compression = {}
                    AND crc_check_chance = 1.0
                    AND dclocal_read_repair_chance = 0.1
                    AND default_time_to_live = 86400
                    AND gc_grace_seconds = 0
                    AND max_index_interval = 2048
                    AND memtable_flush_period_in_ms = 0
                    AND min_index_interval = 128
                    AND read_repair_chance = 0.0
                    AND speculative_retry = 'NONE';
            """)

    # Base Tests
    def test_write(self):
        """
        Test steps:

        1. Run a write workload
        """
        # run a write workload
        base_cmd_w = self.params.get('stress_cmd_w')
        # create new document in ES with doc_id = test_id + timestamp
        # allow to correctly save results for future compare
        self.create_test_stats(doc_id_with_timestamp=True)
        self.run_fstrim_on_all_db_nodes()
        # run a workload
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd_w, stress_num=2, keyspace_num=1,
                                              stats_aggregate_cmds=False)
        results = self.get_stress_results(queue=stress_queue)

        self.update_test_details(scylla_conf=True)
        self.display_results(results, test_name='test_write')
        self.check_regression()

    def test_read(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a read workload
        """

        base_cmd_r = self.params.get('stress_cmd_r')
        self.run_fstrim_on_all_db_nodes()
        # run a write workload
        self.preload_data()

        # create new document in ES with doc_id = test_id + timestamp
        # allow to correctly save results for future compare
        self.create_test_stats(doc_id_with_timestamp=True)
        # wait compactions will be finished
        self.wait_no_compactions_running(n=240, sleep_time=180)
        self.run_fstrim_on_all_db_nodes()
        # run a read workload
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd_r, stress_num=2, stats_aggregate_cmds=False)
        results = self.get_stress_results(queue=stress_queue)

        self.update_test_details(scylla_conf=True)
        self.display_results(results, test_name='test_read')
        self.check_regression()

    def test_mixed(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a mixed workload
        """

        base_cmd_m = self.params.get('stress_cmd_m')
        self.run_fstrim_on_all_db_nodes()
        # run a write workload as a preparation
        self.preload_data()
        # run a mixed workload
        # create new document in ES with doc_id = test_id + timestamp
        # allow to correctly save results for future compare
        self.create_test_stats(doc_id_with_timestamp=True)
        # wait compactions will be finished
        self.wait_no_compactions_running(n=240, sleep_time=180)
        self.run_fstrim_on_all_db_nodes()
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd_m, stress_num=2, stats_aggregate_cmds=False)
        results = self.get_stress_results(queue=stress_queue)

        self.update_test_details(scylla_conf=True)
        self.display_results(results, test_name='test_mixed')
        self.check_regression()

    def test_latency(self):
        """
        Test steps:

        1. Prepare cluster with data (reach steady_stet of compactions and ~x10 capacity than RAM.
        with round_robin and list of stress_cmd - the data will load several times faster.
        2. Run WRITE workload with gauss population.
        """
        self.run_fstrim_on_all_db_nodes()
        self.preload_data()
        self.run_workload(stress_cmd="stress_cmd_r", sub_type="read")
        self.run_workload(stress_cmd="stress_cmd_w", sub_type="write")
        self.run_workload(stress_cmd="stress_cmd_m",  sub_type="mixed", scylla_conf=True)

    def test_latency_read_with_nemesis(self):
        self._latency_read_with_nemesis(stress_cmd="stress_cmd_r", sub_type="read")

    def test_latency_write_with_nemesis(self):
        self._latency_read_with_nemesis(stress_cmd="stress_cmd_w", sub_type="write")

    def test_latency_mixed_with_nemesis(self):
        self._latency_read_with_nemesis(stress_cmd="stress_cmd_m", sub_type="mixed")

    # MV Tests
    def test_mv_write(self):
        """
        Test steps:

        1. Run WRITE workload on base table without materialized view
        2. Run WRITE workload with materialized view when view is on partition key is the same host as partition key
        3. Drop MV
        4. Run WRITE workload with materialized view when view is on clustering key is the same host as partition key
        5. Drop MV
        """
        def run_workload(stress_cmd, user_profile):
            self.log.debug('Run stress test with user profile {}'.format(user_profile))
            assert os.path.exists(user_profile), 'File not found: {}'.format(user_profile)
            self.log.debug('Stress cmd: {}'.format(stress_cmd))
            stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stress_num=1, profile=user_profile,
                                                  stats_aggregate_cmds=False)
            results = self.get_stress_results(queue=stress_queue)
            self.update_test_details(scylla_conf=True)
            self.display_results(results, test_name=test_name)
            self.check_regression()
            self.log.debug('Finish stress test with user profile {}'.format(user_profile))

        def get_mv_name(user_profile):

            # Get materialized view name from user profile

            with open(user_profile, encoding="utf-8") as fobj:
                user_profile_yaml = yaml.safe_load(fobj)
            mv_name = ''

            for k in user_profile_yaml:
                if isinstance(k, tuple) and k[0] == 'extra_definitions':
                    mv_name = k[1][0].split(' AS')[0].split(' ')[-1]
                    break

            if not mv_name:
                assert False, 'Failed to recognoze materialized view name from {0}: {1}'.format(
                    user_profile, user_profile_yaml)

            return mv_name

        def drop_mv(mv_name):
            # drop MV
            self.log.debug('Start dropping materialized view {}'.format(mv_name))
            query = 'drop materialized view {}'.format(mv_name)

            try:
                with self.db_cluster.cql_connection_patient_exclusive(self.db_cluster.nodes[0]) as session:
                    self.log.debug('Run query: {}'.format(query))
                    session.execute(query)
            except Exception as ex:
                self.log.debug('Failed to drop materialized view using query {0}. Error: {1}'.format(query, str(ex)))
                raise

            self.log.debug('Finish dropping materialized view {}'.format(mv_name))

        test_name = 'test_mv_write'
        duration = self.params.get('test_duration')
        self.log.debug('Start materialized views performance test. Test duration {} minutes'.format(duration))
        self.create_test_stats()
        cmd_no_mv = self.params.get('stress_cmd_no_mv')
        cmd_no_mv_profile = self.params.get('stress_cmd_no_mv_profile')

        # Run WRITE workload without materialized view
        run_workload(cmd_no_mv, cmd_no_mv_profile)

        # Run WRITE workload with materialized view
        mv_commands = self.params.get("stress_cmd_mv")
        # mv_commands structure (created in correctly parses yaml):
        #   [
        #    [('cmd', <cassandra-stress command line>), ('profile', <profile file name with path>)],
        #    [('cmd', <cassandra-stress command line>), ('profile', <profile file name with path>)]
        #   ]
        for cmd in mv_commands:
            cmd_mv, cmd_mv_profile = cmd[0][1], cmd[1][1]
            run_workload(cmd_mv, cmd_mv_profile)
            drop_mv(get_mv_name(cmd_mv_profile))
            time.sleep(60)

    def test_mv_write_populated(self):
        self._write_with_mv(on_populated=True)

    def test_mv_write_not_populated(self):
        self._write_with_mv(on_populated=False)

    def test_mv_read_populated(self):
        self._read_with_mv(on_populated=True)

    def test_mv_read_not_populated(self):
        self._read_with_mv(on_populated=False)

    def test_mv_mixed_populated(self):
        self._mixed_with_mv(on_populated=True)

    def test_mv_mixed_not_populated(self):
        self._mixed_with_mv(on_populated=False)

    # Counter Tests
    def test_uniform_counter_update_bench(self):  # pylint: disable=invalid-name
        """
        Test steps:

        1. Run workload: -workload uniform -mode counter_update -duration 30m
        """
        base_cmd_r = ("scylla-bench -workload uniform -mode counter_update -duration 30m "
                      "-partition-count 50000000 -clustering-row-count 1 -connection-count "
                      "32 -concurrency 512 -replication-factor 3")

        self.create_test_stats()
        stress_queue = self.run_stress_thread_bench(stress_cmd=base_cmd_r, stats_aggregate_cmds=False)
        results = self.get_stress_results_bench(queue=stress_queue)

        self.update_test_details(scylla_conf=True)
        self.display_results(results, test_name='test_read_bench')
        self.check_regression()

    # Large Partition Tests
    def test_timeseries_bench(self):
        """
        Timeseries write/read workload
        """
        cmd_w = ("scylla-bench -workload=timeseries -mode=write -replication-factor=3 "
                 "-partition-count=5000 -clustering-row-count=1000000 -clustering-row-size=200 "
                 "-concurrency=48 -max-rate=150000 -rows-per-request=5000")

        self.create_test_stats(sub_type='write')
        self._scylla_bench_prepare_table()
        self.run_stress_thread_bench(stress_cmd=cmd_w, stats_aggregate_cmds=False)
        start_timestamp = int(time.time())
        self.db_cluster.wait_total_space_used_per_node(700 * KB * KB * KB, 'scylla_bench.test')  # 700GB

        cmd_r = ("scylla-bench -workload=timeseries -mode=read -partition-count=5000 -concurrency=1 "
                 "-replication-factor=3 -write-rate=30 -clustering-row-count=1000000 -clustering-row-size=200 "
                 "-rows-per-request=1000000 -no-lower-bound -start-timestamp=%s -duration=60m" % start_timestamp)

        self.create_test_stats(sub_type='read')
        stress_queue = self.run_stress_thread_bench(stress_cmd=cmd_r, stats_aggregate_cmds=False)
        results = self.get_stress_results_bench(queue=stress_queue)
        self.update_test_details()
        self.display_results(results, test_name='test_timeseries_read_bench')
        self.check_regression()
        self.kill_stress_thread()


class YCSBPerformanceRegressionTest(BasePerformanceRegression):
    yaml_params = {
        "SCYLLA_CONNECTIONS": "240",  # number of connections per node
        "TARGET_SIZE": "120000",  # 120K operation per seconds
    }
    ycsb_workloads = {
        "a": "Update Heavy (50/50 read/write ratio)",
        "b": "Read Mostly (95/5 read/write ratio)",
        "c": "Read Only (100/0 read/write ratio)",
        "d": "Read Latest (95/0/5 read/update/insert ratio)",
        "e": "Short Range (95/5 scan/insert ratio)",
        "f": "Read-Modify-Write (50/50 read/read-modify-write ratio)",
    }
    latency_stress_format = "stress_latency_workload{}"

    def __init__(self, *args):
        super().__init__(*args)
        self.params["records_size"] = 1_000_000
        self.yaml_params["THREADS_SIZE"] = self.params.get("n_db_nodes") * int(self.yaml_params["SCYLLA_CONNECTIONS"])
        self.yaml_params["RECORDS_SIZE"] = self.params.get("records_size")
        self.prepare_ycsb_commands()

    def prepare_ycsb_commands(self):
        def create_dynamic_ycsb_cmd(cmd):
            for key, value in self.yaml_params.items():
                cmd = cmd.replace(key, str(value))
            return cmd

        self.params["prepare_write_cmd"] = create_dynamic_ycsb_cmd(self.params["prepare_write_cmd"])
        for workload_type in self.ycsb_workloads:
            self.params[self.latency_stress_format.format(workload_type)] = \
                create_dynamic_ycsb_cmd(self.params["stress_cmd_m"])

    def test_latency(self):
        """
        Test steps:

        1. Prepare cluster with data (reach steady_stet of compactions and ~x10 capacity than RAM.
        with round_robin and list of stress_cmd - the data will load several times faster.
        2. Run WRITE workload with gauss population.
        """
        self.run_fstrim_on_all_db_nodes()
        self.preload_data()

        for workload_type, workload_details in self.ycsb_workloads.items():
            InfoEvent(message="Starting YCSB workload%s (%s)" % (workload_type, workload_details)).publish()
            self.run_workload(stress_cmd=self.latency_stress_format.format(workload_type), sub_type=workload_details)

    def test_latency_workload_a_with_nemesis(self):
        self._latency_read_with_nemesis(
            stress_cmd=self.latency_stress_format.format("a"), sub_type=self.ycsb_workloads["a"])

    def test_latency_workload_b_with_nemesis(self):
        self._latency_read_with_nemesis(
            stress_cmd=self.latency_stress_format.format("b"), sub_type=self.ycsb_workloads["b"])

    def test_latency_workload_c_with_nemesis(self):
        self._latency_read_with_nemesis(
            stress_cmd=self.latency_stress_format.format("c"), sub_type=self.ycsb_workloads["c"])

    def test_latency_workload_d_with_nemesis(self):
        self._latency_read_with_nemesis(
            stress_cmd=self.latency_stress_format.format("d"), sub_type=self.ycsb_workloads["d"])

    def test_latency_workload_e_with_nemesis(self):
        self._latency_read_with_nemesis(
            stress_cmd=self.latency_stress_format.format("e"), sub_type=self.ycsb_workloads["e"])

    def test_latency_workload_f_with_nemesis(self):
        self._latency_read_with_nemesis(
            stress_cmd=self.latency_stress_format.format("f"), sub_type=self.ycsb_workloads["f"])
