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
import time
import uuid
import random
import logging
import concurrent.futures
from typing import Any
from itertools import chain

from sdcm.loader import CassandraStressExporter
from sdcm.cluster import BaseLoaderSet
from sdcm.prometheus import nemesis_metrics_obj
from sdcm.sct_events import Severity
from sdcm.utils.common import FileFollowerThread, generate_random_string, get_profile_content
from sdcm.sct_events.loaders import CassandraStressEvent, CS_ERROR_EVENTS_PATTERNS, CS_NORMAL_EVENTS_PATTERNS
from sdcm.utils.docker_remote import RemoteDocker

LOGGER = logging.getLogger(__name__)


def format_stress_cmd_error(exc: Exception) -> str:
    """Format nicely the exception from a stress command failure."""

    if hasattr(exc, "result") and exc.result.failed:
        return f"Stress command completed with bad status {exc.result.exited}: {exc.result.stderr:.100}"
    return f"Stress command execution failed with: {exc}"


class CassandraStressEventsPublisher(FileFollowerThread):
    def __init__(self, node: Any, cs_log_filename: str, event_id: str = None):
        super().__init__()

        self.node = str(node)
        self.cs_log_filename = cs_log_filename
        self.event_id = event_id

    def run(self) -> None:
        while not self.stopped():
            if not os.path.isfile(self.cs_log_filename):
                time.sleep(0.5)
                continue

            for line_number, line in enumerate(self.follow_file(self.cs_log_filename)):
                if self.stopped():
                    break

                for pattern, event in chain(CS_NORMAL_EVENTS_PATTERNS, CS_ERROR_EVENTS_PATTERNS):
                    if self.event_id:
                        # Connect the event to the stress load
                        event.event_id = self.event_id

                    if pattern.search(line):
                        event.add_info(node=self.node, line=line, line_number=line_number).publish()
                        break  # Stop iterating patterns to avoid creating two events for one line of the log


class CassandraStressThread:  # pylint: disable=too-many-instance-attributes
    def __init__(self, loader_set, stress_cmd, timeout, stress_num=1, keyspace_num=1, keyspace_name='', compaction_strategy='',  # pylint: disable=too-many-arguments
                 profile=None, node_list=None, round_robin=False, client_encrypt=False, stop_test_on_failure=True):
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
        self.stop_test_on_failure = stop_test_on_failure
        self.compaction_strategy = compaction_strategy

        self.executor = None
        self.results_futures = []
        self.shell_marker = generate_random_string(20)
        #  This marker is used to mark shell commands, in order to be able to kill them later
        self.max_workers = 0

    def create_stress_cmd(self, node, keyspace_idx):
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

        if self.compaction_strategy and "compaction(" not in stress_cmd:
            stress_cmd = stress_cmd.replace(" -schema ", f" -schema 'compaction(strategy={self.compaction_strategy})' ")

        credentials = self.loader_set.get_db_auth()
        if credentials and 'user=' not in stress_cmd:
            # put the credentials into the right place into -mode section
            stress_cmd = re.sub(r'(-mode.*?)-', r'\1 user={} password={} -'.format(*credentials), stress_cmd)
        if self.client_encrypt and 'transport' not in stress_cmd:
            stress_cmd += \
                ' -transport "truststore=/etc/scylla/ssl_conf/client/cacerts.jks truststore-password=cassandra"'

        if self.node_list and '-node' not in stress_cmd:
            node_ip_list = [n.cql_ip_address for n in self.node_list]
            stress_cmd += " -node {}".format(",".join(node_ip_list))
        if 'skip-unsupported-columns' in self._get_available_suboptions(node, '-errors'):
            stress_cmd = self._add_errors_option(stress_cmd, ['skip-unsupported-columns'])
        return stress_cmd

    @staticmethod
    def _add_errors_option(stress_cmd: str, to_add: list) -> str:
        """
        Add suboption to -errors option, if such suboption is there, does not add or change it
        """
        to_add = list(to_add)
        current_error_option = next((option for option in stress_cmd.split(' -') if option.startswith('errors ')), None)
        if current_error_option is None:
            return f"{stress_cmd} -errors {' '.join(to_add)}"
        current_error_suboptions = current_error_option.split()[1:]
        new_error_suboptions = \
            list({suboption.split('=', 1)[0]: suboption for suboption in to_add + current_error_suboptions}.values())
        if len(new_error_suboptions) == len(current_error_suboptions):
            return stress_cmd
        return stress_cmd.replace(current_error_option, 'errors ' + ' '.join(new_error_suboptions))

    def _get_available_suboptions(self, node, option, _cache={}):  # pylint: disable=dangerous-default-value
        if cached_value := _cache.get(node.name):
            return cached_value
        try:
            result = node.remoter.run(
                cmd=f'cassandra-stress help {option} | grep "^Usage:"',
                timeout=self.timeout,
                ignore_status=True).stdout
        except Exception:  # pylint: disable=broad-except
            return []
        findings = re.findall(r' *\[([\w-]+?)[=?]*] *', result)
        _cache[node.name] = findings
        return findings

    @staticmethod
    def _disable_logging_for_cs(node, _cache={}):  # pylint: disable=dangerous-default-value
        if not (node.is_kubernetes() or node.name in _cache):
            node.remoter.run("cp /etc/scylla/cassandra/logback-tools.xml .", ignore_status=True)
            _cache[node.name] = 'done'

    def _run_stress(self, node, loader_idx, cpu_idx, keyspace_idx):  # pylint: disable=too-many-locals
        stress_cmd = self.create_stress_cmd(node, keyspace_idx)

        if self.profile:
            with open(self.profile, encoding="utf-8") as profile_file:
                LOGGER.info('Profile content:\n%s', profile_file.read())
            node.remoter.send_files(self.profile, os.path.join('/tmp', os.path.basename(self.profile)), delete_dst=True)

        # Get next word after `cassandra-stress' in stress_cmd.
        # Do it this way because stress_cmd can contain env variables before `cassandra-stress'.
        stress_cmd_opt = stress_cmd.split("cassandra-stress", 1)[1].split(None, 1)[0]

        LOGGER.info('Stress command:\n%s', stress_cmd)

        os.makedirs(node.logdir, exist_ok=True)
        log_file_name = \
            os.path.join(node.logdir, f'cassandra-stress-l{loader_idx}-c{cpu_idx}-k{keyspace_idx}-{uuid.uuid4()}.log')

        LOGGER.debug('cassandra-stress local log: %s', log_file_name)

        # This tag will be output in the header of c-stress result,
        # we parse it to know the loader & cpu info in _parse_cs_summary().
        tag = f'TAG: loader_idx:{loader_idx}-cpu_idx:{cpu_idx}-keyspace_idx:{keyspace_idx}'

        if self.stress_num > 1:
            node_cmd = f'STRESS_TEST_MARKER={self.shell_marker}; taskset -c {cpu_idx} {stress_cmd}'
        else:
            node_cmd = f'STRESS_TEST_MARKER={self.shell_marker}; {stress_cmd}'
        node_cmd = f'echo {tag}; {node_cmd}'

        result = None
        self._disable_logging_for_cs(node)

        with CassandraStressExporter(instance_name=node.cql_ip_address,
                                     metrics=nemesis_metrics_obj(),
                                     stress_operation=stress_cmd_opt,
                                     stress_log_filename=log_file_name,
                                     loader_idx=loader_idx, cpu_idx=cpu_idx), \
                CassandraStressEventsPublisher(node=node, cs_log_filename=log_file_name) as publisher, \
                CassandraStressEvent(node=node, stress_cmd=self.stress_cmd,
                                     log_file_name=log_file_name) as cs_stress_event:
            publisher.event_id = cs_stress_event.event_id
            try:
                result = node.remoter.run(cmd=node_cmd, timeout=self.timeout, log_file=log_file_name)
            except Exception as exc:  # pylint: disable=broad-except
                cs_stress_event.severity = Severity.CRITICAL if self.stop_test_on_failure else Severity.ERROR
                cs_stress_event.add_error(errors=[format_stress_cmd_error(exc)])

        return node, result, cs_stress_event

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
        self.executor = concurrent.futures.ThreadPoolExecutor(  # pylint: disable=consider-using-with
            max_workers=self.max_workers)

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

    def get_results(self) -> list[dict | None]:
        ret = []
        results = []

        LOGGER.debug('Wait for %s stress threads results', self.max_workers)
        for future in concurrent.futures.as_completed(self.results_futures, timeout=self.timeout):
            results.append(future.result())

        for _, result, event in results:
            if not result:
                # Silently skip if stress command threw error, since it was already reported in _run_stress
                continue
            output = result.stdout + result.stderr
            try:
                lines = output.splitlines()
                node_cs_res = BaseLoaderSet._parse_cs_summary(lines)  # pylint: disable=protected-access
                if node_cs_res:
                    ret.append(node_cs_res)
            except Exception as exc:  # pylint: disable=broad-except
                event.add_error([f"Failed to process stress summary due to {exc}"])
                event.severity = Severity.CRITICAL
                event.event_error()

        return ret

    def verify_results(self):
        results = []
        cs_summary = []
        errors = []

        LOGGER.debug('Wait for %s stress threads to verify', self.max_workers)
        for future in concurrent.futures.as_completed(self.results_futures, timeout=self.timeout):
            results.append(future.result())

        for node, result, _ in results:
            if not result:
                # Silently skip if stress command threw error, since it was already reported in _run_stress
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


class DockerBasedStressThread:
    # pylint: disable=too-many-instance-attributes
    DOCKER_IMAGE_PARAM_NAME = ""  # test yaml param that stores image

    def __init__(self, loader_set, stress_cmd, timeout, stress_num=1, node_list=None,  # pylint: disable=too-many-arguments
                 round_robin=False, params=None, stop_test_on_failure=True):
        self.loader_set: BaseLoaderSet = loader_set
        self.stress_cmd = stress_cmd
        self.timeout = timeout
        self.stress_num = stress_num
        self.node_list = node_list or []
        self.round_robin = round_robin
        self.params = params or {}
        self.loaders = []

        self.executor = None
        self.results_futures = []
        self.max_workers = 0
        self.shell_marker = generate_random_string(20)
        self.shutdown_timeout = 180  # extra 3 minutes
        self.stop_test_on_failure = stop_test_on_failure
        self.docker_image_name = self.params.get(self.DOCKER_IMAGE_PARAM_NAME)
        if "k8s" not in self.params.get("cluster_backend"):
            for loader in self.loader_set.nodes:
                RemoteDocker.pull_image(loader, self.docker_image_name)

    def run(self):
        if self.round_robin:
            self.stress_num = 1
            loaders = [self.loader_set.get_loader()]
            LOGGER.debug("Round-Robin through loaders, Selected loader is {} ".format(loaders))
        else:
            loaders = self.loader_set.nodes
        self.loaders = loaders

        self.max_workers = len(loaders) * self.stress_num
        LOGGER.debug("Starting %d %s Worker threads", self.max_workers, self.__class__.__name__)
        self.executor = concurrent.futures.ThreadPoolExecutor(  # pylint: disable=consider-using-with
            max_workers=self.max_workers)

        for loader_idx, loader in enumerate(loaders):
            for cpu_idx in range(self.stress_num):
                self.results_futures += [self.executor.submit(self._run_stress, *(loader, loader_idx, cpu_idx))]

        return self

    def _run_stress(self, loader, loader_idx, cpu_idx):
        raise NotImplementedError()

    def get_results(self):
        results = []
        timeout = self.timeout + 120
        LOGGER.debug('Wait for %s stress threads results', self.max_workers)
        for future in concurrent.futures.as_completed(self.results_futures, timeout=timeout):
            results.append(future.result())

        return results

    def verify_results(self):
        results = []
        errors = []
        timeout = self.timeout + 120
        LOGGER.debug('Wait for %s stress threads to verify', self.max_workers)
        for future in concurrent.futures.as_completed(self.results_futures, timeout=timeout):
            results.append(future.result())

        return results, errors

    def kill(self):
        for loader in self.loaders:
            loader.remoter.run(cmd=f"docker rm -f `docker ps -a -q --filter label=shell_marker={self.shell_marker}`",
                               timeout=60,
                               ignore_status=True)

    def db_node_to_query(self, loader):
        """Select DB node in the same region as loader node to query"""
        if self.params.get("region_aware_loader"):
            nodes_in_region = self.loader_set.nodes_by_region(self.node_list).get(loader.region)
            assert nodes_in_region, f"No DB nodes found in {loader.region}"
            db_nodes = [db_node for db_node in nodes_in_region if not db_node.running_nemesis]
            assert db_nodes, "No node to query, nemesis runs on all DB nodes!"
            node_to_query = random.choice(db_nodes)
            LOGGER.debug("Selected '%s' to query for local nodes", node_to_query)
            return node_to_query.cql_ip_address
        return self.node_list[0].cql_ip_address


duration_pattern = re.compile(r'(?P<hours>[\d]*)h|(?P<minutes>[\d]*)m|(?P<seconds>[\d]*)s')
stress_cmd_get_duration_pattern = re.compile(r' [-]{0,2}duration[\s=]+([\d]+[hms]+)')
stress_cmd_get_warmup_pattern = re.compile(r' [-]{0,2}warmup[\s=]+([\d]+[hms]+)')


def time_period_str_to_seconds(time_str: str) -> int:
    """Transforms duration string into seconds int. e.g. 1h -> 3600, 1h22m->4920 or 10m->600"""
    return sum([int(g[0] or 0) * 3600 + int(g[1] or 0) * 60 + int(g[2] or 0) for g in duration_pattern.findall(time_str)])


def get_timeout_from_stress_cmd(stress_cmd: str) -> int | None:
    """Gets timeout in seconds based on duration and warmup arguments from stress command."""
    timeout = 0
    if duration_match := stress_cmd_get_duration_pattern.search(stress_cmd):
        timeout += time_period_str_to_seconds(duration_match.group(0))
    if warmup_match := stress_cmd_get_warmup_pattern.search(stress_cmd):
        timeout += time_period_str_to_seconds(warmup_match.group(0))
    if timeout == 0:
        return None
    else:
        # adding 10 minutes to timeout for general all others delays
        return timeout + 600
