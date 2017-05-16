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

import platform
import requests
import subprocess

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

    def generate_stats_json(self, results, cmds=[]):
        metrics = {'test_details': {}, 'setup_details': {}, 'versions': {}, 'results': {}}

        is_gce = False
        for p in self.params.iteritems():
            if ('/run/backends/gce', 'cluster_backend', 'gce') == p:
                is_gce = True
        for p in self.params.iteritems():
            if p[1] in ['test_duration']:
                metrics['test_details'][p[1]] = p[2]
            elif p[1] in ['stress_cmd', 'stress_modes']:
                continue
            else:
                if is_gce and (p[0], p[1]) in \
                        [('/run', 'instance_type_loader'),
                         ('/run', 'instance_type_monitor'),
                         ('/run/databases/scylla', 'instance_type_db')]:
                    # exclude these params from gce run
                    continue
                metrics['setup_details'][p[1]] = p[2]

        versions_output = self.db_cluster.nodes[0].remoter.run('rpm -qa | grep scylla')\
            .stdout.split('\n')
        versions = {}
        for line in versions_output:
            for package in ['scylla-jmx', 'scylla-server', 'scylla-tools']:
                match = re.search('(%s-(\S+)-(0.)?([0-9]{8,8}).(\w+).)' % package, line)
                if match:
                    versions[package] = {'version': match.group(2), 'date': match.group(4), 'commit_id': match.group(5)}

        metrics['versions'] = versions

        # we use cmds. the last on is a stress, others are presetup
        metrics = self.add_stress_cmd_params(metrics, cmds[-1])
        for i in xrange(len(cmds) - 1):
            # we can have multiples preloads
            metrics = self.add_stress_cmd_params(metrics, cmds[i], prefix='preload%s-' % i)

        metrics['test_details']['test_name'] = self.params.id.name
        metrics['test_details']['sct_git_commit'] = \
            subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip()

        if os.environ['JOB_NAME']:
            metrics['test_details']['job_name'] = os.environ['JOB_NAME']
            metrics['test_details']['job_url'] = os.environ['BUILD_URL']

        metrics['results']['stats'] = results

        if 'es_password' in metrics['setup_details']:
            # hide ES password
            metrics['setup_details']['es_password'] = '******'

        new_scylla_packages = self.params.get('update_db_packages')
        if new_scylla_packages and os.listdir(new_scylla_packages):
            metrics['setup_details']['packages_updated'] = True
        else:
            metrics['setup_details']['packages_updated'] = False

        metrics['test_details']['time_completed'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        metrics['test_details']['start_host'] = platform.node()

        test_name_file = metrics['test_details']['test_name'].replace(':', '__').replace('.', '_')

        self.upload_stats_es(metrics, test_name=test_name_file)

    def display_results(self, results, test_name=''):
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

    def add_stress_cmd_params(self, result, cmd, prefix=''):
        # parsing stress command and return dict with params
        cmd = cmd.strip().split('cassandra-stress')[1].strip()
        if cmd.split(' ')[0] in ['read', 'write', 'mixed']:
            section = '{0}cassandra-stress'.format(prefix)
            result['test_details'][section] = {}
            result['test_details'][section]['command'] = cmd.split(' ')[0]
            if 'no-warmup' in cmd:
                result['test_details'][section]['no-warmup'] = True

            match = re.search('(cl\s?=\s?\w+)', cmd)
            if match:
                result['test_details'][section]['cl'] = match.group(0).split('=')[1].strip()

            match = re.search('(duration\s?=\s?\W+)', cmd)
            if match:
                result['test_details'][section]['duration'] = match.group(0).split('=')[1].strip()

            match = re.search('( n\s?=\s?\W+)', cmd)
            if match:
                result['test_details'][section]['n'] = match.group(0).split('=')[1].strip()

            for temp in cmd.split(' -')[1:]:
                k = temp.split()[0]
                match = re.search('(-' + k + '\s+([^-| ]+))', cmd)
                if match:
                    result['test_details'][section][k] = match.group(2).strip()
            if 'rate' in result['test_details'][section]:
                # split rate section on separate items
                if 'threads' in result['test_details'][section]['rate']:
                    result['test_details'][section]['rate threads'] = \
                        re.search('(threads\s?=\s?(\w+))', result['test_details'][section]['rate']).group(2)
                if 'throttle' in result['test_details'][section]['rate']:
                    result['test_details'][section]['throttle threads'] =\
                        re.search('(throttle\s?=\s?(\w+))', result['test_details'][section]['rate']).group(2)
                if 'fixed' in result['test_details'][section]['rate']:
                    result['test_details'][section]['fixed threads'] =\
                        re.search('(fixed\s?=\s?(\w+))', result['test_details'][section]['rate']).group(2)
                del result['test_details'][section]['rate']

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
        average_stats = {}

        # calculate average stats
        for key in metrics['results']['stats'][0].keys():
            summary = 0
            for stat in metrics['results']['stats']:
                try:
                    summary += float(stat[key])
                except:
                    average_stats[key] = stat[key]
            if summary != summary:
                average_stats[key] = None
            elif key not in average_stats:
                average_stats[key] = round(summary / len(metrics['results']['stats']), 1)
        metrics['results']['stats_average'] = average_stats

        for section_k, section_v in metrics.iteritems():
            # trying to convert str values into int/float
            for k, v in section_v.iteritems():
                value = v
                try:
                    value = int(v)
                except:
                    try:
                        value = float(v)
                    except:
                        pass
                    metrics[section_k][k] = value

        with open('jenkins_%s_summary.json' % test_name, 'w') as fp:
            json.dump(metrics, fp, indent=4)

        self.log.info(json.dumps(metrics, indent=4))
        if self.params.get('es_url'):
            url = '%s/performanceregression/%s/%s_%s?pretty' % \
                  (self.params.get('es_url'), metrics['test_details']['test_name'],
                   metrics['setup_details']['ami_id_db_scylla_desc'], metrics['test_details']['time_completed'])

            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
            r = requests.post(url, data=open('jenkins_%s_summary.json' % test_name, 'rb'), headers=headers,
                              auth=(self.params.get('es_user'), self.params.get('es_password')))
            self.log.info(r.content)

    def test_write(self):
        """
        Test steps:

        1. Run a write workload
        """
        # run a write workload
        base_cmd_w = ("cassandra-stress write no-warmup cl=QUORUM duration=60m "
                      "-schema 'replication(factor=3)' -port jmx=6868 "
                      "-mode cql3 native -rate threads=100 -errors ignore "
                      "-pop seq=1..10000000")

        # run a workload
        stress_queue = self.run_stress_thread(stress_cmd=base_cmd_w, stress_num=2, keyspace_num=100)
        results = self.get_stress_results(queue=stress_queue, stress_num=2, keyspace_num=100)

        try:
            self.display_results(results, test_name='test_write')
        except:
            pass
        self.generate_stats_json(results, [base_cmd_w])

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
            self.display_results(results, test_name='test_read')
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
            self.display_results(results, test_name='test_mixed')
        except:
            pass
        self.generate_stats_json(results, [base_cmd_w, base_cmd_m])

if __name__ == '__main__':
    main()
