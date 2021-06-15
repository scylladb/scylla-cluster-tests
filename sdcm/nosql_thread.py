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
import uuid
import threading

from sdcm.sct_events import Severity
from sdcm.stress_thread import format_stress_cmd_error, DockerBasedStressThread
from sdcm.sct_events.loaders import NoSQLBenchStressEvent


LOGGER = logging.getLogger(__name__)


class NoSQLBenchStressThread(DockerBasedStressThread):  # pylint: disable=too-many-instance-attributes
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._per_loader_count = {}
        self._per_loader_count_lock = threading.Semaphore()

    def build_stress_cmd(self, loader_idx: int):
        if hasattr(self.node_list[0], 'parent_cluster'):
            target_address = self.node_list[0].parent_cluster.get_node().ip_address
        else:
            target_address = self.node_list[0].ip_address
        with self._per_loader_count_lock:
            threads_on_loader = self._per_loader_count.get(loader_idx, 0)
            threads_on_loader += 1
            self._per_loader_count[threads_on_loader] = threads_on_loader
        stress_cmd = self.stress_cmd.replace('nosqlbench', '') + f" hosts={target_address}"
        stress_cmd += ' table=nosqlbench_table_' + str(loader_idx + 1) + '_' + str(threads_on_loader)
        return stress_cmd

    def _run_stress(self, loader, loader_idx, cpu_idx):
        stress_cmd = self.build_stress_cmd(loader_idx=loader_idx)

        if not os.path.exists(loader.logdir):
            os.makedirs(loader.logdir, exist_ok=True)
        log_file_name = os.path.join(loader.logdir, 'nosql-bench-l%s-c%s-%s.log' %
                                     (loader_idx, cpu_idx, uuid.uuid4()))
        LOGGER.debug('nosql-bench-stress local log: %s', log_file_name)
        LOGGER.debug("'running: %s", stress_cmd)
        with NoSQLBenchStressEvent(node=loader, stress_cmd=stress_cmd, log_file_name=log_file_name) as stress_event:
            try:
                return loader.remoter.run(cmd=f'docker run scylla0dkropachev/nosqlbench:latest {stress_cmd}',
                                          timeout=self.timeout + self.shutdown_timeout, log_file=log_file_name)
            except Exception as exc:  # pylint: disable=broad-except
                stress_event.severity = Severity.CRITICAL if self.stop_test_on_failure else Severity.ERROR
                stress_event.add_error(errors=[format_stress_cmd_error(exc)])
            return None
