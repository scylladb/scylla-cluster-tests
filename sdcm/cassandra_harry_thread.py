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
# Copyright (c) 2021 ScyllaDB

import os
import uuid
import time
import logging

from sdcm.loader import CassandraHarryStressExporter
from sdcm.prometheus import nemesis_metrics_obj
from sdcm.sct_events.loaders import CassandraHarryEvent, CASSANDRA_HARRY_ERROR_EVENTS_PATTERNS
from sdcm.utils.docker_remote import RemoteDocker
from sdcm.stress_thread import DockerBasedStressThread
from sdcm.stress.base import format_stress_cmd_error
from sdcm.utils.common import FileFollowerThread


LOGGER = logging.getLogger(__name__)


class CassandraHarryStressEventsPublisher(FileFollowerThread):
    def __init__(self, node, harry_log_filename):
        super().__init__()
        self.harry_log_filename = harry_log_filename
        self.node = str(node)

    def run(self):
        while not self.stopped():
            exists = os.path.isfile(self.harry_log_filename)
            if not exists:
                time.sleep(0.5)
                continue

            for line_number, line in enumerate(self.follow_file(self.harry_log_filename)):
                if self.stopped():
                    break

                for pattern, event in CASSANDRA_HARRY_ERROR_EVENTS_PATTERNS:
                    if pattern.search(line):
                        event.add_info(node=self.node, line=line, line_number=line_number).publish()


class CassandraHarryThread(DockerBasedStressThread):

    DOCKER_IMAGE_PARAM_NAME = "stress_image.harry"

    def __init__(self, *args, **kwargs):
        credentials = kwargs.pop('credentials', None)

        super().__init__(*args, **kwargs)

        if credentials and "username=" not in self.stress_cmd:
            self.stress_cmd += " -username {} -password {}".format(*credentials)

    def _run_stress(self, loader, loader_idx, cpu_idx):
        # Select first seed node to send the scylla-harry cmds
        ip = self.node_list[0].cql_address

        if not os.path.exists(loader.logdir):
            os.makedirs(loader.logdir, exist_ok=True)
        log_file_name = os.path.join(loader.logdir, f'cassandra-harry-l{loader_idx}-{uuid.uuid4()}.log')
        LOGGER.debug('cassandra-harry local log: %s', log_file_name)

        LOGGER.debug("running: %s", self.stress_cmd)

        if self.stress_num > 1:
            node_cmd = f'taskset -c {cpu_idx} bash -c "{self.stress_cmd}"'
        else:
            node_cmd = self.stress_cmd

        docker = cleanup_context = RemoteDocker(loader, self.docker_image_name,
                                                extra_docker_opts=f' --label shell_marker={self.shell_marker}')

        node_cmd = f'STRESS_TEST_MARKER={self.shell_marker}; {node_cmd}'

        CassandraHarryEvent.start(node=loader, stress_cmd=self.stress_cmd).publish()

        result = {}
        harry_failure_event = harry_finish_event = None
        with CassandraHarryStressExporter(instance_name=loader.ip_address,
                                          metrics=nemesis_metrics_obj(),
                                          stress_operation='write',
                                          stress_log_filename=log_file_name,
                                          loader_idx=loader_idx), \
                CassandraHarryStressEventsPublisher(node=loader, harry_log_filename=log_file_name), \
                cleanup_context:
            result = None
            try:
                docker_run_result = docker.run(cmd=f"{node_cmd} -node {ip}",
                                               timeout=self.timeout + self.shutdown_timeout,
                                               log_file=log_file_name,
                                               verbose=True,
                                               retry=0,
                                               )
                result = self._parse_harry_summary(docker_run_result.stdout.splitlines())
            except Exception as exc:  # noqa: BLE001
                errors_str = format_stress_cmd_error(exc)
                if "timeout" in errors_str:
                    harry_failure_event = CassandraHarryEvent.timeout
                elif self.stop_test_on_failure:
                    harry_failure_event = CassandraHarryEvent.failure
                else:
                    harry_failure_event = CassandraHarryEvent.error
                harry_failure_event(
                    node=loader,
                    stress_cmd=self.stress_cmd,
                    log_file_name=log_file_name,
                    errors=[errors_str, ],
                ).publish()
            else:
                harry_finish_event = CassandraHarryEvent.finish(node=loader, stress_cmd=self.stress_cmd,
                                                                log_file_name=log_file_name)
                harry_finish_event.publish()

        return loader, result, harry_failure_event or harry_finish_event

    @staticmethod
    def _parse_harry_summary(lines):
        """
        Currently we only check if it succeeds or not.
        succeed sample:

        INFO  [pool-6-thread-1] instance_id_IS_UNDEFINED 2021-01-19 13:46:49,835 Runner.java:255 - Completed
        """
        results = {}
        if any(['Runner.java:255 - Completed' in line for line in lines]):
            results['status'] = 'completed'
        else:
            results['status'] = 'failed'
        return results
