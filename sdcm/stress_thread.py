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

from __future__ import print_function

import os
import re
import uuid
import time
import logging
import concurrent.futures

from sdcm.loader import CassandraStressExporter
from sdcm.prometheus import nemesis_metrics_obj
from sdcm.cluster import BaseLoaderSet
from sdcm.sct_events import CassandraStressEvent
from sdcm.utils.common import FileFollowerThread, makedirs, generate_random_string, get_profile_content
from sdcm.sct_events import CassandraStressLogEvent, Severity

LOGGER = logging.getLogger(__name__)


def format_stress_cmd_error(exc):
    """
    format nicely the excpetion we get from stress command failures

    :param exc: the exception
    :return: string to add to the event
    """
    if hasattr(exc, 'result') and exc.result.failed:
        stderr = exc.result.stderr
        if len(stderr) > 100:
            stderr = stderr[:100]
        errors_str = f'Stress command completed with bad status {exc.result.exited}: {stderr}'
    else:
        errors_str = f'Stress command execution failed with: {str(exc)}'

    return errors_str


class CassandraStressEventsPublisher(FileFollowerThread):
    def __init__(self, node, cs_log_filename):
        super(CassandraStressEventsPublisher, self).__init__()
        self.cs_log_filename = cs_log_filename
        self.node = str(node)
        self.cs_events = [CassandraStressLogEvent(type='IOException', regex=r'java\.io\.IOException', severity=Severity.ERROR),
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
                    match = pattern.search(line)
                    if match:
                        event.add_info_and_publish(node=self.node, line=line, line_number=line_number)


class CassandraStressThread():  # pylint: disable=too-many-instance-attributes
    def __init__(self, loader_set, stress_cmd, timeout, stress_num=1, keyspace_num=1, keyspace_name='',  # pylint: disable=too-many-arguments
                 profile=None, node_list=None, round_robin=False, client_encrypt=False):
        if not node_list:
            node_list = []
        self.loader_set = loader_set
        self.stress_cmd = stress_cmd
        self.timeout = timeout
        self.stress_num = stress_num
        self.keyspace_num = keyspace_num
        self.keyspace_name = keyspace_name
        self.profile = profile
        self.node_list = node_list
        self.round_robin = round_robin
        self.client_encrypt = client_encrypt

        self.executor = None
        self.results_futures = []
        self.shell_marker = generate_random_string(20)
        #  This marker is used to mark shell commands, in order to be able to kill them later
        self.max_workers = 0

    def create_stress_cmd(self, node, loader_idx, keyspace_idx):
        stress_cmd = self.stress_cmd
        if node.cassandra_stress_version == "unknown":  # Prior to 3.11, cassandra-stress didn't have version argument
            stress_cmd = stress_cmd.replace("throttle", "limit")  # after 3.11 limit was renamed to throttle

        # When using cassandra-stress with "user profile" the profile yaml should be provided
        if 'profile' in stress_cmd and not self.profile:
            # support of using -profile in sct test-case yaml, assumes they exists data_dir
            # TODO: move those profile to their own directory
            cs_profile, profile = get_profile_content(stress_cmd)
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
        if self.client_encrypt and 'transport' not in stress_cmd:
            stress_cmd += ' -transport "truststore=/etc/scylla/ssl_conf/client/cacerts.jks truststore-password=cassandra"'

        if self.node_list and '-node' not in stress_cmd:
            first_node = [n for n in self.node_list if n.dc_idx == loader_idx %
                          3]  # make sure each loader is targeting on datacenter/region
            first_node = first_node[0] if first_node else self.node_list[0]
            stress_cmd += " -node {}".format(first_node.ip_address)

        return stress_cmd

    def _run_stress(self, node, loader_idx, cpu_idx, keyspace_idx):
        stress_cmd = self.create_stress_cmd(node, loader_idx, keyspace_idx)

        if self.profile:
            with open(self.profile) as profile_file:
                LOGGER.info('Profile content:\n%s', profile_file.read())
            node.remoter.send_files(self.profile, os.path.join('/tmp', os.path.basename(self.profile)), delete_dst=True)

        stress_cmd_opt = stress_cmd.split()[1]

        LOGGER.info('Stress command:\n%s', stress_cmd)

        if not os.path.exists(node.logdir):
            makedirs(node.logdir)
        log_file_name = os.path.join(node.logdir,
                                     f'cassandra-stress-l{loader_idx}-c{cpu_idx}-k{keyspace_idx}-{uuid.uuid4()}.log')

        LOGGER.debug('cassandra-stress local log: %s', log_file_name)

        # This tag will be output in the header of c-stress result,
        # we parse it to know the loader & cpu info in _parse_cs_summary().
        tag = f'TAG: loader_idx:{loader_idx}-cpu_idx:{cpu_idx}-keyspace_idx:{keyspace_idx}'

        if self.stress_num > 1:
            node_cmd = f'STRESS_TEST_MARKER={self.shell_marker}; taskset -c {cpu_idx} {stress_cmd}'
        else:
            node_cmd = f'STRESS_TEST_MARKER={self.shell_marker}; {stress_cmd}'

        node_cmd = f'echo {tag}; {node_cmd}'

        CassandraStressEvent(type='start', node=str(node), stress_cmd=stress_cmd)

        with CassandraStressExporter(instance_name=node.ip_address,
                                     metrics=nemesis_metrics_obj(),
                                     cs_operation=stress_cmd_opt,
                                     cs_log_filename=log_file_name,
                                     loader_idx=loader_idx, cpu_idx=cpu_idx), \
                CassandraStressEventsPublisher(node=node, cs_log_filename=log_file_name):
            result = None
            try:
                result = node.remoter.run(cmd=node_cmd,
                                          timeout=self.timeout,
                                          log_file=log_file_name)
            except Exception as exc:  # pylint: disable=broad-except
                errors_str = format_stress_cmd_error(exc)
                CassandraStressEvent(type='failure', node=str(node), stress_cmd=stress_cmd,
                                     log_file_name=log_file_name, severity=Severity.CRITICAL,
                                     errors=[errors_str])

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
                    self.results_futures += [self.executor.submit(self._run_stress,
                                                                  *(loader, loader_idx, cpu_idx, ks_idx))]
                    if loader_idx == 0 and cpu_idx == 0 and self.max_workers > 1:
                        # Wait for first stress thread to create the schema, before spawning new stress threads
                        time.sleep(30)

        return self

    def kill(self):
        if self.round_robin:
            self.stress_num = 1
            loaders = [self.loader_set.get_loader()]
        else:
            loaders = self.loader_set.nodes
        for loader in loaders:
            loader.remoter.run(cmd=f"pkill -9 -f {self.shell_marker}",
                               timeout=self.timeout,
                               ignore_status=True)

    def get_results(self):
        ret = []
        results = []

        LOGGER.debug('Wait for %s stress threads results', self.max_workers)
        for future in concurrent.futures.as_completed(self.results_futures, timeout=self.timeout):
            results.append(future.result())

        for _, result in results:
            if not result:
                # Silently skip if stress command throwed error, since it was already reported in _run_stress
                continue
            output = result.stdout + result.stderr
            try:
                lines = output.splitlines()
                node_cs_res = BaseLoaderSet._parse_cs_summary(lines)  # pylint: disable=protected-access
                if node_cs_res:
                    ret.append(node_cs_res)
            except Exception as exc:  # pylint: disable=broad-except
                CassandraStressEvent(
                    type='failure', node='', severity=Severity.CRITICAL,
                    errors=[f'Failed to proccess stress summary due to {exc}'])

        return ret

    def verify_results(self):
        results = []
        cs_summary = []
        errors = []

        LOGGER.debug('Wait for %s stress threads to verify', self.max_workers)
        for future in concurrent.futures.as_completed(self.results_futures, timeout=self.timeout):
            results.append(future.result())

        for node, result in results:
            if not result:
                # Silently skip if stress command throwed error, since it was already reported in _run_stress
                continue
            output = result.stdout + result.stderr
            lines = output.splitlines()
            node_cs_res = BaseLoaderSet._parse_cs_summary(lines)  # pylint: disable=protected-access
            if node_cs_res:
                cs_summary.append(node_cs_res)
            for line in lines:
                if 'java.io.IOException' in line:
                    errors += ['%s: %s' % (node, line.strip())]

        return cs_summary, errors
