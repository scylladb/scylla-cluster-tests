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


import datetime
import json
import os
import re

import requests

from avocado import main
from sdcm.tester import ClusterTester


class PerformanceRegressionTest(ClusterTester):

    """
    Test Scylla performance regression with cassandra-stress.

    :avocado: enable
    """

    str_pattern = '%8s%16s%10s%14s%16s%12s%12s%14s%16s%16s'

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

    def get_test_xml(self, result, test_name='simple_regression_test'):
        test_content = """
  <test name="%s-stress_modes: (%s) Loader%s CPU%s Keyspace%s" executed="yes">
    <description>"simple regression test, ami_id: %s, scylla version:
    %s", stress_mode: %s, hardware: %s</description>
    <targets>
      <target threaded="yes">target-ami_id-%s</target>
      <target threaded="yes">target-version-%s</target>
      <target threaded="yes">stress_modes-%s</target>
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
""" % (test_name,
            self.params.get('stress_modes'),
            result['loader_idx'],
            result['cpu_idx'],
            result['keyspace_idx'],
            self.params.get('ami_id_db_scylla'),
            self.params.get('ami_id_db_scylla_desc'),
            self.params.get('stress_modes'),
            self.params.get('instance_type_db'),
            self.params.get('ami_id_db_scylla'),
            self.params.get('ami_id_db_scylla_desc'),
            self.params.get('stress_modes'),
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

    def generate_stats_json(self, results, cmds=[]):
        metrics = {}

        for p in self.params.iteritems():
            metrics[p[1]] = p[2]

        # we use cmds
        del metrics['stress_cmd']
        for cmd in cmds:
            metrics = self.add_stress_cmd_params(metrics, cmd)
            metrics = self.add_stress_cmd_params(metrics, cmd)

        metrics['test_name'] = self.params.id.name
        metrics['stats'] = results

        if 'es_password' in metrics:
            # hide ES password
            metrics['es_password'] = '******'

        metrics['time_completed'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

        test_name_file = metrics['test_name'].replace(':', '__').replace('.', '_')

        with open('jenkins_%s.json' % test_name_file, 'w') as fp:
            json.dump(metrics, fp, indent=4)

        self.upload_stats_es(metrics, test_name=test_name_file)

    def display_results(self, results, test_name='simple_regression_test'):
        self.log.info(self.str_pattern % ('op-rate', 'partition-rate',
                                          'row-rate', 'latency-mean',
                                          'latency-median', 'l-94th-pct',
                                          'l-99th-pct', 'l-99.9th-pct',
                                          'total-partitions', 'total-err'))

        test_xml = ""
        for single_result in results:
            self.display_single_result(single_result)
            test_xml += self.get_test_xml(single_result, test_name=test_name)

        f = open(os.path.join(self.logdir,
                              'jenkins_perf_PerfPublisher.xml'), 'w')
        content = """<report name="%s report" categ="none">
%s
</report>""" % (test_name, test_xml)

        f.write(content)
        f.close()

    def add_stress_cmd_params(self, result, cmd):
        cmd = cmd.strip()
        cmd = cmd.strip().split('cassandra-stress')[1].strip()
        if cmd.split(' ')[0] in ['read', 'write', 'mixed']:
            section = 'cassandra-stress ' + cmd.split(' ')[0]
            result[section] = {}
            # cmd = ' '.join(cmd.split(' ')[1:]).strip()
            # result[section]['no-warmup'] = False
            if cmd.startswith('no-warmup'):
                result[section]['no-warmup'] = True
                # cmd = cmd.strip('no-warmup')[1:].strip()

            match = re.search('(cl\s?=\s?\w+)', cmd)
            if match:
                result[section]['cl'] = match.group(0).split('=')[1].strip()

            match = re.search('(duration\s?=\s?\W+)', cmd)
            if match:
                result[section]['duration'] = match.group(0).split('=')[1].strip()

            match = re.search('( n\s?=\s?\W+)', cmd)
            if match:
                result[section]['n'] = match.group(0).split('=')[1].strip()

            for temp in cmd.split(' -')[1:]:
                k = temp.split()[0]
                match = re.search('(-' + k + '\s+([^-| ]+))', cmd)
                if match:
                    result[section][k] = match.group(2).strip()
        return result

    def upload_stats_es(self, metrics, test_name):
        """
        custom date format is used in ES
        curl -XPUT 'https://USER:PASSWORD@HOST_NAME:9243/performanceregressiontest' -H 'Content-Type:application/json' -d'{
           "mappings":{
              "performanceregressiontest":{
                 "properties":{
                    "time_completed":{
                       "type":"date",
                       "format":"yyyy-MM-dd HH:mm"
                    }
                 }
              }
           }
        }
        """
        stats_to_add = {}

        for key in metrics['stats'][0].keys():
            summary = 0
            for stat in metrics['stats']:
                try:
                    summary += float(stat[key])
                except:
                    stats_to_add[key] = stat[key]
            if summary != summary:
                stats_to_add[key] = None
            elif key not in stats_to_add:
                stats_to_add[key] = round(summary / len(metrics['stats']), 1)

        result = {}
        for k, v in metrics.iteritems():
            if k == 'stats':
                continue
            value = v
            try:
                value = int(v)
            except:
                try:
                    value = float(v)
                except:
                    pass

            result[k] = value

        result.update(stats_to_add)

        with open('jenkins_%s_summary.json' % test_name, 'w') as fp:
            json.dump(result, fp, indent=4)

        self.log.info(json.dumps(result, indent=4))
        if self.params.get('es_url'):
            url = '%s/performanceregressiontest/%s/%s_%s?pretty' % \
                  (self.params.get('es_url'), result['test_name'],
                   result['ami_id_db_scylla_desc'], result['time_completed'])

            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
            r = requests.post(url, data=open('jenkins_%s_summary.json' % test_name, 'rb'), headers=headers,
                              auth=(self.params.get('es_user'), self.params.get('es_password')))
            self.log.info(r.content)

    def test_simple_regression(self):
        """
        Test steps:

        1. Run a write workload
        2. Run a read workload (after the write - cache will contain some data)
        3. Restart node, run a read workload (cache will be empty)
        4. Run a mixed read write workload
        """
        # run a write workload
        base_cmd = ("cassandra-stress %s no-warmup cl=QUORUM duration=60m "
                    "-schema 'replication(factor=3)' -port jmx=6868 "
                    "-mode cql3 native -rate threads=100 -errors ignore "
                    "-pop seq=1..10000000")

        stress_modes = self.params.get(key='stress_modes', default='write')
        for mode in stress_modes.split():
            if mode == 'restart':
                # restart all the nodes
                for loader in self.db_cluster.nodes:
                    loader.restart()
            else:
                # run a workload
                stress_queue = self.run_stress_thread(stress_cmd=base_cmd % mode, stress_num=2, keyspace_num=100)
                results = self.get_stress_results(queue=stress_queue, stress_num=2, keyspace_num=100)

        try:
            self.display_results(results)
        except:
            pass
        self.generate_stats_json(results, [base_cmd])

    def test_read(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a read workload
        """
        base_cmd_w = ("cassandra-stress write no-warmup cl=QUORUM n=30000000 "
                      "-schema 'replication(factor=3)' -port jmx=6868 "
                      "-mode cql3 native -rate threads=100 -errors ignore "
                      "-pop seq=1..30000000")
        base_cmd_r = ("cassandra-stress read no-warmup cl=QUORUM duration=50m "
                      "-schema 'replication(factor=3)' -port jmx=6868 "
                      "-mode cql3 native -rate threads=100 -errors ignore "
                      "-pop 'dist=gauss(1..30000000,15000000,1500000)' ")

        # run a write workload
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd_w, stress_num=2)
        self.get_stress_results(queue=stress_queue, stress_num=2)

        stress_queue = self.run_stress_thread(stress_cmd=base_cmd_r, stress_num=2)
        results = self.get_stress_results(queue=stress_queue, stress_num=2)

        try:
            self.display_results(results)
        except:
            pass
        self.generate_stats_json(results, [base_cmd_w, base_cmd_r])

    def test_mixed(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a mixed workload
        """
        base_cmd_w = ("cassandra-stress write no-warmup cl=QUORUM n=30000000 "
                      "-schema 'replication(factor=3)' -port jmx=6868 "
                      "-mode cql3 native -rate threads=100 -errors ignore "
                      "-pop seq=1..30000000")
        base_cmd_m = ("cassandra-stress mixed no-warmup cl=QUORUM duration=50m "
                      "-schema 'replication(factor=3)' -port jmx=6868 "
                      "-mode cql3 native -rate threads=100 -errors ignore "
                      "-pop 'dist=gauss(1..30000000,15000000,1500000)' ")

        # run a write workload as a preparation
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd_w, stress_num=2)
        self.get_stress_results(queue=stress_queue, stress_num=2)

        # run a mixed workload
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd_m, stress_num=2)
        results = self.get_stress_results(queue=stress_queue, stress_num=2)

        try:
            self.display_results(results)
        except:
            pass
        self.generate_stats_json(results, [base_cmd_w, base_cmd_m])

if __name__ == '__main__':
    main()
