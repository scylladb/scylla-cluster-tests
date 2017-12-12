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

from avocado import main
from sdcm.tester import ClusterTester


class PerformanceRegressionTest(ClusterTester):

    """
    Test Scylla performance regression with cassandra-stress.

    :avocado: enable
    """

    str_pattern = '%8s%16s%10s%14s%16s%12s%12s%14s%16s%16s'

    def __init__(self, *args, **kwargs):
        super(PerformanceRegressionTest, self).__init__(*args, **kwargs)
        self.create_stats = False

    def display_single_result(self, result):
        self.log.info(self.str_pattern % (result['op rate'],
                                          result['partition rate'],
                                          result['row rate'],
                                          result['latency mean'],
                                          result['latency median'],
                                          result['latency 95th percentile'],
                                          result['latency 99th percentile'],
                                          result['latency 99.9th percentile'],
                                          result['Total partitions'],
                                          result['Total errors']))

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
        self.log.info(self.str_pattern % ('op-rate', 'partition-rate',
                                          'row-rate', 'latency-mean',
                                          'latency-median', 'l-94th-pct',
                                          'l-99th-pct', 'l-99.9th-pct',
                                          'total-partitions', 'total-err'))

        test_xml = ""
        try:
            for single_result in results:
                self.display_single_result(single_result)
                test_xml += self.get_test_xml(single_result, test_name=test_name)

            with open(os.path.join(self.logdir, 'jenkins_perf_PerfPublisher.xml'), 'w') as f:
                content = """<report name="%s report" categ="none">%s</report>""" % (test_name, test_xml)
                f.write(content)
        except Exception as ex:
            self.log.debug('Failed to display results: {0}'.format(results))
            self.log.debug('Exception: {0}'.format(ex))

    def test_write(self):
        """
        Test steps:

        1. Run a write workload
        """
        # run a write workload
        base_cmd_w = self.params.get('stress_cmd_w')
        self.create_test_stats()
        # run a workload
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd_w, stress_num=2, keyspace_num=1)
        results = self.get_stress_results(queue=stress_queue)

        self.update_test_details()
        self.display_results(results, test_name='test_write')
        self.check_regression()

    def test_read(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a read workload
        """

        base_cmd_w = self.params.get('prepare_write_cmd')
        base_cmd_r = self.params.get('stress_cmd_r')

        self.create_test_stats()
        # run a write workload
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd_w, stress_num=2, prefix='preload-')
        self.get_stress_results(queue=stress_queue)

        stress_queue = self.run_stress_thread(stress_cmd=base_cmd_r, stress_num=2)
        results = self.get_stress_results(queue=stress_queue)

        self.update_test_details()
        self.display_results(results, test_name='test_read')
        self.check_regression()

    def test_mixed(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a mixed workload
        """

        base_cmd_w = self.params.get('prepare_write_cmd')
        base_cmd_m = self.params.get('stress_cmd_m')

        self.create_test_stats()
        # run a write workload as a preparation
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd_w, stress_num=2, prefix='preload-')
        self.get_stress_results(queue=stress_queue)

        # run a mixed workload
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd_m, stress_num=2)
        results = self.get_stress_results(queue=stress_queue)

        self.update_test_details()
        self.display_results(results, test_name='test_mixed')
        self.check_regression()

    def test_uniform_counter_update_bench(self):
        """
        Test steps:

        1. Run workload: -workload uniform -mode counter_update -duration 30m
        """
        base_cmd_r = ("scylla-bench -workload uniform -mode counter_update -duration 30m "
                      "-partition-count 50000000 -clustering-row-count 1 -connection-count "
                      "32 -concurrency 512 -replication-factor 3")

        self.create_test_stats()
        stress_queue = self.run_stress_thread_bench(stress_cmd=base_cmd_r)
        results = self.get_stress_results_bench(queue=stress_queue)

        self.update_test_details()
        self.display_results(results, test_name='test_read_bench')
        self.check_regression()


if __name__ == '__main__':
    main()
