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

import os
import re
import uuid
import time
import logging
import concurrent.futures

import yaml

from sdcm.loader import CassandraStressExporter
from sdcm.prometheus import nemesis_metrics_obj
from sdcm.cluster import BaseLoaderSet
from sdcm.sct_events import CassandraStressEvent
from sdcm.utils.common import FileFollowerThread
from sdcm.sct_events import CassandraStressLogEvent, Severity

LOGGER = logging.getLogger(__name__)


class CassandraStressEventsPublisher(FileFollowerThread):
    def __init__(self, node, cs_log_filename):
        super(CassandraStressEventsPublisher, self).__init__()
        self.cs_log_filename = cs_log_filename
        self.node = str(node)
        self.cs_events = [CassandraStressLogEvent(type='IOException', regex=r'java\.io\.IOException', severity=Severity.CRITICAL),
                          CassandraStressLogEvent(type='ConsistencyError', regex=r'Cannot achieve consistency level', severity=Severity.ERROR)]

    def run(self):
        patterns = [(event, re.compile(event.regex)) for event in self.cs_events]

        while not self.stopped():
            exists = os.path.isfile(self.cs_log_filename)
            if not exists:
                time.sleep(0.5)
                continue

            for line_number, line in enumerate(self.follow_file(self.cs_log_filename)):
                if self.stopped():
                    break

                for event, pattern in patterns:
                    m = pattern.search(line)
                    if m:
                        event.add_info_and_publish(node=self.node, line=line, line_number=line_number)


class CassandraStressThread(object):
    def __init__(self, loader_set, stress_cmd, timeout, output_dir, stress_num=1, keyspace_num=1, keyspace_name='',
                 profile=None, node_list=[], round_robin=False):
        self.loader_set = loader_set
        self.stress_cmd = stress_cmd
        self.timeout = timeout
        self.output_dir = output_dir
        self.stress_num = stress_num
        self.keyspace_num = keyspace_num
        self.keyspace_name = keyspace_name
        self.profile = profile
        self.node_list = node_list
        self.round_robin = round_robin

        self.executor = None
        self.results_futures = []
        self.max_workers = 0

    def create_stress_cmd(self, node, loader_idx, keyspace_idx):
        stress_cmd = self.stress_cmd
        if node.cassandra_stress_version == "unknown":  # Prior to 3.11, cassandra-stress didn't have version argument
            stress_cmd = stress_cmd.replace("throttle", "limit")  # after 3.11 limit was renamed to throttle

        # When using cassandra-stress with "user profile" the profile yaml should be provided
        if 'profile' in stress_cmd and not self.profile:
            cs_profile = re.search('profile=(.*)yaml', stress_cmd).group(1) + 'yaml'
            cs_profile = os.path.join(os.path.dirname(__file__), '../', 'data_dir', os.path.basename(cs_profile))
            with open(cs_profile, 'r') as yaml_stream:
                profile = yaml.safe_load(yaml_stream)
                keyspace_name = profile['keyspace']
            self.profile = cs_profile
            self.keyspace_name = keyspace_name

        if self.keyspace_name:
            stress_cmd = stress_cmd.replace(" -schema ", " -schema keyspace={} ".format(self.keyspace_name))
        elif 'keyspace=' not in stress_cmd:  # if keyspace is defined in the command respect that
            stress_cmd = stress_cmd.replace(" -schema ", " -schema keyspace=keyspace{} ".format(keyspace_idx))

        credentials = self.loader_set.get_db_auth()
        if credentials and 'user=' not in stress_cmd:
            # put the credentials into the right place into -mode section
            stress_cmd = re.sub(r'(-mode.*?)-', r'\1 user={} password={} -'.format(*credentials), stress_cmd)

        if self.node_list and '-node' not in stress_cmd:
            first_node = [n for n in self.node_list if n.dc_idx == loader_idx % 3]  # make sure each loader is targeting on datacenter/region
            first_node = first_node[0] if first_node else self.node_list[0]
            stress_cmd += " -node {}".format(first_node.ip_address)

        return stress_cmd

    def _run_stress(self, node, loader_idx, cpu_idx, keyspace_idx):
        stress_cmd = self.create_stress_cmd(node, loader_idx, keyspace_idx)

        if self.profile:
            with open(self.profile) as fp:
                LOGGER.info('Profile content:\n%s' % fp.read())
            node.remoter.send_files(self.profile, os.path.join('/tmp', os.path.basename(self.profile)))

        stress_cmd_opt = stress_cmd.split()[1]

        LOGGER.info('Stress command:\n%s' % stress_cmd)

        log_file_name = os.path.join(node.logdir, 'cassandra-stress-l%s-c%s-k%s-%s.log' % (loader_idx, cpu_idx, keyspace_idx, uuid.uuid4()))

        LOGGER.debug('cassandra-stress local log: %s', log_file_name)

        # This tag will be output in the header of c-stress result,
        # we parse it to know the loader & cpu info in _parse_cs_summary().
        tag = 'TAG: loader_idx:%s-cpu_idx:%s-keyspace_idx:%s' % (loader_idx, cpu_idx, keyspace_idx)

        if self.stress_num > 1:
            node_cmd = 'taskset -c %s bash -c "%s"' % (cpu_idx, stress_cmd)
        else:
            node_cmd = stress_cmd

        node_cmd = 'echo %s; %s' % (tag, node_cmd)

        CassandraStressEvent(type='start', node=str(node), stress_cmd=stress_cmd)

        with CassandraStressExporter(instance_name=node.ip_address,
                                     metrics=nemesis_metrics_obj(),
                                     cs_operation=stress_cmd_opt,
                                     cs_log_filename=log_file_name,
                                     loader_idx=loader_idx, cpu_idx=cpu_idx), \
                CassandraStressEventsPublisher(node=node, cs_log_filename=log_file_name):

            result = node.remoter.run(cmd=node_cmd,
                                      timeout=self.timeout,
                                      ignore_status=True,
                                      log_file=log_file_name)

        CassandraStressEvent(type='finish', node=str(node), stress_cmd=stress_cmd, log_file_name=log_file_name)

        return node, result

    def run(self):
        if self.round_robin:
            # cancel stress_num
            self.stress_num = 1
            loaders = [self.loader_set.get_loader()]
            LOGGER.debug("Round-Robin through loaders, Selected loader is {} ".format(loaders))
        else:
            loaders = self.loader_set.nodes

        self.max_workers = len(loaders) * self.stress_num
        LOGGER.debug("Starting %d c-s Worker threads", self.max_workers)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)

        for loader_idx, loader in enumerate(loaders):
            for cpu_idx in range(self.stress_num):
                for ks_idx in range(1, self.keyspace_num + 1):
                    self.results_futures += [self.executor.submit(self._run_stress, *(loader, loader_idx, cpu_idx, ks_idx))]
                    if loader_idx == 0 and cpu_idx == 0 and self.max_workers > 1:
                        # Wait for first stress thread to create the schema, before spawning new stress threads
                        time.sleep(30)

        return self

    def get_results(self):
        ret = []
        results = []

        LOGGER.debug('Wait for %s stress threads results', self.max_workers)
        for future in concurrent.futures.as_completed(self.results_futures, timeout=self.timeout):
            results.append(future.result())

        for node, result in results:
            output = result.stdout + result.stderr
            lines = output.splitlines()
            node_cs_res = BaseLoaderSet._parse_cs_summary(lines)
            if node_cs_res:
                ret.append(node_cs_res)

        return ret

    def verify_results(self):
        results = []
        cs_summary = []
        errors = []

        LOGGER.debug('Wait for %s stress threads to verify', self.max_workers)
        for future in concurrent.futures.as_completed(self.results_futures, timeout=self.timeout):
            results.append(future.result())

        for node, result in results:
            output = result.stdout + result.stderr
            lines = output.splitlines()
            node_cs_res = BaseLoaderSet._parse_cs_summary(lines)
            if node_cs_res:
                cs_summary.append(node_cs_res)
            for line in lines:
                if 'java.io.IOException' in line:
                    errors += ['%s: %s' % (node, line.strip())]

        return cs_summary, errors


if __name__ == '__main__':
    import tempfile
    from sdcm.remote import RemoteCmdRunner
    import unittest
    from sdcm.sct_events import start_events_device, stop_events_device

    logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)

    class Node():
        ssh_login_info = {'hostname': '34.253.205.91',
                          'user': 'centos',
                          'key_file': '~/.ssh/scylla-qa-ec2'}

        remoter = RemoteCmdRunner(**ssh_login_info)
        ip_address = '34.253.205.91'
        cassandra_stress_version = '3.11'

    class DbNode():
        ip_address = "34.244.157.61"
        dc_idx = 1

    class LoaderSetDummy(object):
        def get_db_auth(self):
            return None

        name = 'LoaderSetDummy'
        nodes = [Node()]

    class TestStressThread(unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            cls.temp_dir = tempfile.mkdtemp()
            start_events_device(cls.temp_dir)
            time.sleep(10)

        @classmethod
        def tearDownClass(cls):
            stop_events_device()

        def test_01(self):
            from sdcm.prometheus import start_metrics_server
            start_metrics_server()
            cs = CassandraStressThread(LoaderSetDummy(), output_dir=self.temp_dir, timeout=60, node_list=[DbNode()], stress_num=1,
                                       stress_cmd="cassandra-stress write cl=ONE duration=3m -schema 'replication(factor=3) compaction(strategy=SizeTieredCompactionStrategy)' -port jmx=6868 -mode cql3 native -rate threads=1000 -pop seq=1..10000000 -log interval=5")

            cs1 = CassandraStressThread(LoaderSetDummy(), output_dir=self.temp_dir, timeout=60, node_list=[DbNode()],
                                        stress_num=1,
                                        stress_cmd="cassandra-stress write cl=ONE duration=3m -schema 'replication(factor=3) compaction(strategy=SizeTieredCompactionStrategy)' -port jmx=6868 -mode cql3 native -rate threads=1000 -pop seq=1..10000000 -log interval=5")

            fs = cs.run()
            time.sleep(5)
            fs1 = cs1.run()
            time.sleep(60)
            print "killing"
            Node().remoter.run(cmd='pgrep -f cassandra-stress | xargs -I{}  kill -TERM -{}', ignore_status=True)

            print(cs.verify_results())

            print(cs1.verify_results())

        def test_02(self):

            tmp_file = tempfile.NamedTemporaryFile(mode='w+')
            tailer = CassandraStressEventsPublisher(node=Node(), cs_log_filename=tmp_file.name)

            res = tailer.start()
            bad_line = "Cannot achieve consistency level"
            line = '[34.241.184.166] [stdout] total,      83086089,   70178,   70178,   70178,    14.2,    11.9,    33.2,    53.6,    77.7,   105.4, 1220.0,  0.00868,      0,      0,       0,       0,       0,       0'

            tmp_file.file.write(line + '\n')
            tmp_file.file.flush()

            tmp_file.file.write(bad_line + '\n')
            tmp_file.file.flush()
            time.sleep(2)
            tailer.stop()

            res.result()

    unittest.main(verbosity=2, defaultTest="TestStressThread.test_01")
