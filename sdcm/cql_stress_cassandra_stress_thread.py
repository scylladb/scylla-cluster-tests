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
# Copyright (c) 2023 ScyllaDB

import logging
import re
import os
import time
import contextlib
from typing import Any
from sdcm.loader import CqlStressCassandraStressExporter, CqlStressHDRExporter
from sdcm.prometheus import nemesis_metrics_obj
from sdcm.reporting.tooling_reporter import CqlStressCassandraStressVersionReporter
from sdcm.sct_events.loaders import CQL_STRESS_CS_ERROR_EVENTS_PATTERNS, CqlStressCassandraStressEvent
from sdcm.stress_thread import CassandraStressThread
from sdcm.utils.common import FileFollowerThread, SoftTimeoutContext
from sdcm.utils.docker_remote import RemoteDocker
from sdcm.utils.remote_logger import HDRHistogramFileLogger

LOGGER = logging.getLogger(__name__)


class CqlStressCassandraStressEventsPublisher(FileFollowerThread):
    def __init__(self, node: Any, log_filename: str, event_id: str = None):
        super().__init__()

        self.node = str(node)
        self.log_filename = log_filename
        self.event_id = event_id

    def run(self) -> None:
        while not self.stopped():
            if not os.path.isfile(self.log_filename):
                time.sleep(0.5)
                continue

            for line_number, line in enumerate(self.follow_file(self.log_filename)):
                if self.stopped():
                    break

                for pattern, event in CQL_STRESS_CS_ERROR_EVENTS_PATTERNS:
                    if self.event_id:
                        event.event_id = self.event_id

                    if pattern.search(line):
                        event.add_info(node=self.node, line=line, line_number=line_number).publish()


class CqlStressCassandraStressThread(CassandraStressThread):
    DOCKER_IMAGE_PARAM_NAME = 'stress_image.cql-stress-cassandra-stress'

    def __init__(self, loader_set, stress_cmd, timeout, stress_num=1, keyspace_num=1, keyspace_name='', compaction_strategy='',  # pylint: disable=too-many-arguments  # noqa: PLR0913
                 profile=None, node_list=None, round_robin=False, client_encrypt=False, stop_test_on_failure=True,
                 params=None):
        super().__init__(loader_set=loader_set, stress_cmd=stress_cmd, timeout=timeout,
                         stress_num=stress_num, node_list=node_list,  # pylint: disable=too-many-arguments
                         round_robin=round_robin, stop_test_on_failure=stop_test_on_failure, params=params,
                         keyspace_num=keyspace_num, keyspace_name=keyspace_name, profile=profile,
                         client_encrypt=client_encrypt, compaction_strategy=compaction_strategy)

    def create_stress_cmd(self, cmd_runner, keyspace_idx, loader):  # pylint: disable=too-many-branches
        stress_cmd = self.stress_cmd

        stress_cmd = self.append_no_warmup_to_cmd(stress_cmd)
        stress_cmd = self.adjust_cmd_keyspace_name(stress_cmd, keyspace_idx)
        stress_cmd = self.adjust_cmd_compaction_strategy(stress_cmd)

        if '-col' in stress_cmd:
            # In cassandra-stress, '-col n=' parameter defines number of columns called (C0, C1, ..., Cn) respectively.
            # It accepts a distribution as value, however, only 'FIXED' distribution is accepted for the cql mode.
            # In cql-stress, we decided to accept a number instead.
            # To support the c-s syntax in test-cases files, we replace occurrences
            # of n=FIXED(k) to n=k.
            stress_cmd = re.sub(r' (\'?)n=[\s]*fixed\(([0-9]+)\)(\'?)', r' \1n=\2\3',
                                stress_cmd, flags=re.IGNORECASE)

        if '-pop' in stress_cmd and "seq=" in stress_cmd:
            # '-pop seq=x..y' syntax is not YET supported by cql-stress.
            # TODO remove when seq=x..y syntax is supported.
            LOGGER.warning(
                "-pop seq=x..y syntax is not YET supported by cql-stress-cassandra-stress. Replacing seq=x..y with dist=SEQ(x..y).")
            stress_cmd = re.sub(r' seq=[\s]*([\d]+\.\.[\d]+)',
                                r" 'dist=SEQ(\1)'", stress_cmd)

        credentials = self.loader_set.get_db_auth()
        if credentials and 'user=' not in stress_cmd:
            if '-mode' in stress_cmd:
                # put the credentials into the right place into -mode section
                stress_cmd = re.sub(r'(-mode.*?)(-)?', r'\1 user={} password={} \2'.format(*credentials), stress_cmd)
            else:
                stress_cmd += ' -mode user={} password={} '.format(*credentials)
        stress_cmd = self.adjust_cmd_node_option(stress_cmd, loader, cmd_runner)
        return stress_cmd

    def _run_cs_stress(self, loader, loader_idx, cpu_idx, keyspace_idx):  # pylint: disable=too-many-locals,too-many-branches,too-many-statements  # noqa: PLR0914
        # TODO:
        # - Add support for profile yaml once cql-stress supports 'user' command.
        # - Adjust metrics collection once cql-stress displays the metrics grouped by operation (mixed workload).

        cleanup_context = contextlib.nullcontext()
        os.makedirs(loader.logdir, exist_ok=True)

        stress_cmd_opt = self.stress_cmd.split(
            "cql-stress-cassandra-stress", 1)[1].split(None, 1)[0]

        log_id = self._build_log_file_id(loader_idx, cpu_idx, keyspace_idx)
        log_file_name = os.path.join(
            loader.logdir, f'cql-stress-cassandra-stress-{stress_cmd_opt}-{log_id}.log')

        LOGGER.debug('cql-stress-cassandra-stress local log: %s', log_file_name)
        remote_hdr_file_name = f"hdrh-cscs-{stress_cmd_opt}-{log_id}.hdr"
        LOGGER.debug("cql-stress-cassandra-stress remote HDR histogram log file: %s", remote_hdr_file_name)
        local_hdr_file_name = os.path.join(loader.logdir, remote_hdr_file_name)
        LOGGER.debug("cql-stress-cassandra-stress HDR local file %s", local_hdr_file_name)
        loader.remoter.run(f"touch $HOME/{remote_hdr_file_name}", ignore_status=False, verbose=False)
        remote_hdr_file_name_full_path = loader.remoter.run(
            f"realpath $HOME/{remote_hdr_file_name}", ignore_status=False, verbose=False).stdout.strip()
        cmd_runner_name = loader.ip_address

        cpu_options = ""
        if self.stress_num > 1:
            cpu_options = f'--cpuset-cpus="{cpu_idx}"'

        cmd_runner = cleanup_context = RemoteDocker(loader, self.docker_image_name,
                                                    command_line="-c 'tail -f /dev/null'",
                                                    extra_docker_opts=f'{cpu_options} '
                                                    '--network=host '
                                                    '--security-opt seccomp=unconfined '
                                                    f'--label shell_marker={self.shell_marker}'
                                                    f' --entrypoint /bin/bash'
                                                    f' -w /'
                                                    f' -v {remote_hdr_file_name_full_path}:/{remote_hdr_file_name}'
                                                    )
        stress_cmd = self.create_stress_cmd(cmd_runner, keyspace_idx, loader)

        if self.params.get("use_hdrhistogram"):
            stress_cmd = self._add_hdr_log_option(stress_cmd, remote_hdr_file_name)
            hdrh_logger_context = HDRHistogramFileLogger(
                node=loader,
                remote_log_file=remote_hdr_file_name_full_path,
                target_log_file=os.path.join(loader.logdir, remote_hdr_file_name),
            )
        else:
            hdrh_logger_context = contextlib.nullcontext()
        LOGGER.info('Stress command:\n%s', stress_cmd)

        tag = f'TAG: loader_idx:{loader_idx}-cpu_idx:{cpu_idx}-keyspace_idx:{keyspace_idx}'

        if self.stress_num > 1:
            node_cmd = f'STRESS_TEST_MARKER={self.shell_marker}; taskset -c {cpu_idx} {stress_cmd}'
        else:
            node_cmd = f'STRESS_TEST_MARKER={self.shell_marker}; {stress_cmd}'
        node_cmd = f'echo {tag}; {node_cmd}'

        try:
            prefix,  *_ = stress_cmd.split("cql-stress-cassandra-stress", maxsplit=1)
            reporter = CqlStressCassandraStressVersionReporter(
                cmd_runner, prefix, loader.parent_cluster.test_config.argus_client())
            reporter.report()
        except Exception:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.info("Failed to collect cql-stress-cassandra-stress version information", exc_info=True)
        result = None
        with cleanup_context, \
                CqlStressCassandraStressExporter(instance_name=cmd_runner_name,
                                                 metrics=nemesis_metrics_obj(),
                                                 stress_operation=stress_cmd_opt,
                                                 stress_log_filename=log_file_name,
                                                 loader_idx=loader_idx, cpu_idx=cpu_idx), \
                CqlStressCassandraStressEventsPublisher(node=loader, log_filename=log_file_name) as publisher, \
                CqlStressCassandraStressEvent(node=loader, stress_cmd=self.stress_cmd,
                                              log_file_name=log_file_name) as cs_stress_event, \
                CqlStressHDRExporter(instance_name=cmd_runner_name,
                                     hdr_tags=self.hdr_tags,
                                     metrics=nemesis_metrics_obj(),
                                     stress_operation=stress_cmd_opt,
                                     stress_log_filename=local_hdr_file_name,
                                     loader_idx=loader_idx, cpu_idx=cpu_idx), \
                hdrh_logger_context:
            publisher.event_id = cs_stress_event.event_id
            try:
                # prolong timeout by 5% to avoid killing cassandra-stress process
                hard_timeout = self.timeout + int(self.timeout * 0.05)
                with SoftTimeoutContext(timeout=self.timeout, operation="cql-stress-cassandra-stress"):
                    result = cmd_runner.run(
                        cmd=node_cmd, timeout=hard_timeout, log_file=log_file_name, retry=0)
            except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                self.configure_event_on_failure(
                    stress_event=cs_stress_event, exc=exc)

        return loader, result, cs_stress_event
