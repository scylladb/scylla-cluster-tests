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

import os
import re
import logging
import time
import uuid
from typing import Any

from sdcm.prometheus import nemesis_metrics_obj
from sdcm.sct_events.loaders import NdBenchStressEvent, NDBENCH_ERROR_EVENTS_PATTERNS
from sdcm.utils.common import FileFollowerThread
from sdcm.utils.docker_remote import RemoteDocker
from sdcm.stress_thread import DockerBasedStressThread
from sdcm.stress.base import format_stress_cmd_error

LOGGER = logging.getLogger(__name__)


class NdBenchStressEventsPublisher(FileFollowerThread):
    def __init__(self, node: Any, ndbench_log_filename: str, event_id: str = None):
        super().__init__()

        self.node = str(node)
        self.ndbench_log_filename = ndbench_log_filename
        self.event_id = event_id

    def run(self) -> None:
        while not self.stopped():
            if not os.path.isfile(self.ndbench_log_filename):
                time.sleep(0.5)
                continue

            for line_number, line in enumerate(self.follow_file(self.ndbench_log_filename)):
                if self.stopped():
                    break

                for pattern, event in NDBENCH_ERROR_EVENTS_PATTERNS:
                    if self.event_id:
                        # Connect the event to the stress load
                        event.event_id = self.event_id

                    if pattern.search(line):
                        event.add_info(node=self.node, line=line, line_number=line_number).publish()
                        break  # Stop iterating patterns to avoid creating two events for one line of the log


class NdBenchStatsPublisher(FileFollowerThread):
    METRICS = {}
    collectible_ops = ['read', 'write']

    def __init__(self, loader_node, loader_idx, ndbench_log_filename):
        super().__init__()
        self.loader_node = loader_node
        self.loader_idx = loader_idx
        self.ndbench_log_filename = ndbench_log_filename

        for operation in self.collectible_ops:
            gauge_name = self.gauge_name(operation)
            if gauge_name not in self.METRICS:
                metrics = nemesis_metrics_obj()
                self.METRICS[gauge_name] = metrics.create_gauge(gauge_name,
                                                                'Gauge for ndbench metrics',
                                                                ['instance', 'loader_idx', 'type'])

    @staticmethod
    def gauge_name(operation):
        return 'sct_ndbench_%s_gauge' % operation.replace('-', '_')

    def set_metric(self, operation, name, value):
        metric = self.METRICS[self.gauge_name(operation)]
        metric.labels(self.loader_node.ip_address, self.loader_idx, name).set(value)

    def run(self):
        # INFO RPSCount:78 - Read avg: 0.314ms, Read RPS: 7246, Write avg: 0.39ms, Write RPS: 1802, total RPS: 9048, Success Ratio: 100%
        stat_regex = re.compile(
            r'Read avg: (?P<read_lat_avg>.*?)ms.*?'
            r'Read RPS: (?P<read_ops>.*?),.*?'
            r'Write avg: (?P<write_lat_avg>.*?)ms.*?'
            r'Write RPS: (?P<write_ops>.*?),', re.IGNORECASE)

        while not self.stopped():
            exists = os.path.isfile(self.ndbench_log_filename)
            if not exists:
                time.sleep(0.5)
                continue

            for _, line in enumerate(self.follow_file(self.ndbench_log_filename)):
                if self.stopped():
                    break
                try:
                    match = stat_regex.search(line)
                    if match:
                        for key, value in match.groupdict().items():
                            operation, name = key.split('_', 1)
                            self.set_metric(operation, name, float(value))

                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("Failed to send metric. Failed with exception {exc}".format(exc=exc))


class NdBenchStressThread(DockerBasedStressThread):

    DOCKER_IMAGE_PARAM_NAME = "stress_image.ndbench"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # remove the ndbench command, and parse the rest of the ; separated values
        stress_cmd = re.sub(r'^ndbench', '', self.stress_cmd)
        if credentials := self.loader_set.get_db_auth():
            stress_cmd += f'; cass.username={credentials[0]} ; cass.password={credentials[1]}'

        self.stress_cmd = ' '.join([f'-Dndbench.config.{param.strip()}' for param in stress_cmd.split(';')])
        timeout = '' if 'cli.timeoutMillis' in self.stress_cmd else f'-Dndbench.config.cli.timeoutMillis={self.timeout * 1000}'
        self.stress_cmd = f'./gradlew {timeout}' \
            f' -Dndbench.config.cass.host={self.node_list[0].cql_address} {self.stress_cmd} run'

    def _run_stress(self, loader, loader_idx, cpu_idx):
        if not os.path.exists(loader.logdir):
            os.makedirs(loader.logdir, exist_ok=True)
        log_file_name = os.path.join(loader.logdir, f'ndbench-l{loader_idx}-c{cpu_idx}-{uuid.uuid4()}.log')
        LOGGER.debug('ndbench local log: %s', log_file_name)

        LOGGER.debug("running: %s", self.stress_cmd)

        if self.stress_num > 1:
            node_cmd = f'taskset -c {cpu_idx} bash -c "{self.stress_cmd}"'
        else:
            node_cmd = self.stress_cmd

        docker = cleanup_context = RemoteDocker(loader, self.docker_image_name,
                                                extra_docker_opts='--network=host '
                                                                  '--security-opt seccomp=unconfined '
                                                                  f'--label shell_marker={self.shell_marker}')

        node_cmd = f'STRESS_TEST_MARKER={self.shell_marker}; {node_cmd}'

        NdBenchStressEvent.start(node=loader, stress_cmd=self.stress_cmd).publish()

        result = {}
        ndbench_failure_event = ndbench_finish_event = None
        with NdBenchStatsPublisher(loader, loader_idx, ndbench_log_filename=log_file_name), \
                NdBenchStressEventsPublisher(node=loader, ndbench_log_filename=log_file_name), \
                cleanup_context:
            try:
                result = docker.run(cmd=node_cmd,
                                    timeout=self.timeout + self.shutdown_timeout,
                                    ignore_status=True,
                                    log_file=log_file_name,
                                    verbose=True,
                                    retry=0,
                                    timestamp_logs=True)
            except Exception as exc:  # noqa: BLE001
                ndbench_failure_event = NdBenchStressEvent.failure(
                    node=str(loader),
                    stress_cmd=self.stress_cmd,
                    log_file_name=log_file_name,
                    errors=[format_stress_cmd_error(exc), ])
                ndbench_failure_event.publish()
            finally:
                ndbench_finish_event = NdBenchStressEvent.finish(
                    node=loader,
                    stress_cmd=self.stress_cmd,
                    log_file_name=log_file_name)
                ndbench_finish_event.publish()

        return loader, result, ndbench_failure_event or ndbench_finish_event
