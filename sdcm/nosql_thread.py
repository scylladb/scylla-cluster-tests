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
import logging
import time
import uuid
import threading

from sdcm.cluster import BaseNode
from sdcm.stress.base import DockerBasedStressThread
from sdcm.sct_events.loaders import NoSQLBenchStressEvent, NOSQLBENCH_EVENT_PATTERNS
from sdcm.utils.common import FileFollowerThread

LOGGER = logging.getLogger(__name__)


class NoSQLBenchEventsPublisher(FileFollowerThread):
    def __init__(self, node: BaseNode, log_filename: str):
        super().__init__()
        self.nb_log_filename = log_filename
        self.node = node

    def run(self) -> None:
        while not self.stopped():
            if not os.path.isfile(self.nb_log_filename):
                time.sleep(0.5)
                continue

            for line_number, line in enumerate(self.follow_file(self.nb_log_filename)):
                if self.stopped():
                    break

                for pattern, event in NOSQLBENCH_EVENT_PATTERNS:
                    if pattern.search(line):
                        event.clone().add_info(node=self.node, line=line, line_number=line_number).publish()


class NoSQLBenchStressThread(DockerBasedStressThread):
    """
    A stress thread that is running NoSQLBench docker on remote loader and getting results back
    If you have questions regarding NoSQLBench command line please checkout following documentation:
      https://github.com/scylladb/scylla-cluster-tests/blob/master/docs/sct-events.md
    """

    DOCKER_IMAGE_PARAM_NAME = "stress_image.nosqlbench"
    GRAPHITE_EXPORTER_CONFIG_SRC_PATH = "docker/graphite-exporter/graphite_mapping.conf"
    GRAPHITE_EXPORTER_CONFIG_DST_PATH = "/tmp/"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._per_loader_count = {}
        self._per_loader_count_lock = threading.Semaphore()

    def build_stress_cmd(self, loader_idx: int):
        if hasattr(self.node_list[0], 'parent_cluster'):
            target_address = self.node_list[0].parent_cluster.get_node().cql_address
        else:
            target_address = self.node_list[0].cql_address

        dc = self.node_list[0].datacenter

        with self._per_loader_count_lock:
            threads_on_loader = self._per_loader_count.get(loader_idx, 0)
            threads_on_loader += 1
            self._per_loader_count[threads_on_loader] = threads_on_loader

        return f"{self.stress_cmd} localdc={dc} hosts={target_address} table=nosqlbench_table_{loader_idx + 1}_threads_on_loader"

    def _run_stress(self, loader, loader_idx, cpu_idx):
        stress_cmd = self.build_stress_cmd(loader_idx=loader_idx)

        if not os.path.exists(loader.logdir):
            os.makedirs(loader.logdir, exist_ok=True)
        log_file_name = os.path.join(loader.logdir, 'nosql-bench-l%s-c%s-%s.log' %
                                     (loader_idx, cpu_idx, uuid.uuid4()))
        LOGGER.debug('nosql-bench-stress local log: %s', log_file_name)
        LOGGER.debug("'running: %s", stress_cmd)
        with NoSQLBenchStressEvent(node=loader, stress_cmd=stress_cmd, log_file_name=log_file_name) as stress_event, \
                NoSQLBenchEventsPublisher(node=loader, log_filename=log_file_name):
            try:
                # copy graphite-exporter config file to loader
                loader.remoter.send_files(src=self.GRAPHITE_EXPORTER_CONFIG_SRC_PATH,
                                          dst=self.GRAPHITE_EXPORTER_CONFIG_DST_PATH,
                                          verbose=False)

                # create shared network for the containers
                create_network_cmd = "docker network create --driver bridge nosql"
                graphite_run_cmd = "docker run -d -p 9108:9108 -p 9109:9109 -p 9109:9109/udp " \
                                   "-v /tmp/graphite_mapping.conf:/tmp/graphite_mapping.conf " \
                                   "--name=graphite-exporter " \
                                   "--network=nosql " \
                                   "prom/graphite-exporter --graphite.mapping-config=/tmp/graphite_mapping.conf"
                loader.remoter.run(cmd=create_network_cmd)
                loader.remoter.run(cmd=graphite_run_cmd,
                                   timeout=self.timeout + self.shutdown_timeout,
                                   log_file=log_file_name,
                                   ignore_status=True)

                return loader.remoter.run(cmd=f'docker run '
                                          '--name=nb '
                                          '--network=nosql '
                                          f'{self.docker_image_name} '
                                          f'{stress_cmd} --report-graphite-to graphite-exporter:9109',
                                          timeout=self.timeout + self.shutdown_timeout, log_file=log_file_name)
            except Exception as exc:  # noqa: BLE001
                self.configure_event_on_failure(stress_event=stress_event, exc=exc)
            return None
