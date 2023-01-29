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
import time
import random
import logging
import uuid
import threading

from functools import cached_property
from typing import Dict

from sdcm.stress_thread import format_stress_cmd_error, DockerBasedStressThread
from sdcm.utils.docker_remote import RemoteDocker
from sdcm.sct_events.system import InfoEvent
from sdcm.sct_events.loaders import KclStressEvent
from sdcm.cluster import BaseNode


LOGGER = logging.getLogger(__name__)


class KclStressThread(DockerBasedStressThread):  # pylint: disable=too-many-instance-attributes

    DOCKER_IMAGE_PARAM_NAME = "stress_image.kcl"

    def run(self):
        _self = super().run()
        # wait for the KCL thread to create the tables, so the YCSB thread beat this one, and start failing
        time.sleep(120)
        return _self

    def build_stress_cmd(self):
        if hasattr(self.node_list[0], 'parent_cluster'):
            target_address = self.node_list[0].parent_cluster.get_node().ip_address
        else:
            target_address = self.node_list[0].ip_address
        stream_args = f'-e http://{target_address}:{self.params.get("alternator_port")} ' \
            f'{self.stress_cmd.replace("hydra-kcl", "")}'
        return stream_args

    def _run_stress(self, loader, loader_idx, cpu_idx):
        # KCS Dockers list:
        # scylladb/hydra-loaders:kcl-jdk8-20210310-extra-debug
        # scylladb/hydra-loaders:kcl-jdk8-20210310-ShardSyncStrategyType-PERIODIC
        # scylladb/hydra-loaders:kcl-jdk8-20210215
        # scylladb/hydra-loaders:kcl-jdk8-20210526-ShardSyncStrategyType-PERIODIC
        # yarongilor/alternator:kcl-jdk8-20211026
        # TODO: i have no permissions to upload the docker image to scylla QA repository. should be fixed by maintainers
        docker = cleanup_context = RemoteDocker(loader, "yarongilor/alternator:kcl-jdk8-20230129",
                                                extra_docker_opts=f'--label shell_marker={self.shell_marker}')
        stream_args = self.build_stress_cmd()

        if not os.path.exists(loader.logdir):
            os.makedirs(loader.logdir, exist_ok=True)
        log_file_name = os.path.join(loader.logdir, 'kcl-l%s-c%s-%s.log' %
                                     (loader_idx, cpu_idx, uuid.uuid4()))
        LOGGER.debug('kcl-stress local log: %s', log_file_name)

        LOGGER.debug("'running: %s", stream_args)

        java_cmd = 'java -jar kraken-dynamo-syncer-0.0.1-SNAPSHOT-jar-with-dependencies.jar'
        node_cmd = f'cd /kraken-dynamo/syncer/target && {java_cmd} {stream_args}'

        KclStressEvent.start(node=loader, stress_cmd=node_cmd).publish()

        try:
            with cleanup_context:
                result = docker.run(cmd=node_cmd,
                                    timeout=self.timeout + self.shutdown_timeout,
                                    log_file=log_file_name,
                                    retry=0,
                                    )

                return result

        except Exception as exc:  # pylint: disable=broad-except
            errors_str = format_stress_cmd_error(exc)
            KclStressEvent.failure(
                node=loader,
                stress_cmd=self.stress_cmd,
                log_file_name=log_file_name,
                errors=[errors_str, ],
            ).publish()
            raise
        finally:
            KclStressEvent.finish(node=loader, stress_cmd=node_cmd, log_file_name=log_file_name).publish()


class CompareTablesSizesThread(DockerBasedStressThread):  # pylint: disable=too-many-instance-attributes
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stop_event = threading.Event()

    def kill(self):
        self._stop_event.set()

    def db_node_to_query(self, loader):
        """Select DB node in the same region as loader node to query"""
        db_nodes = [db_node for db_node in self.node_list if not db_node.running_nemesis]
        assert db_nodes, "No node to query, nemesis runs on all DB nodes!"
        node_to_query = random.choice(db_nodes)
        LOGGER.debug("Selected '%s' to query for local nodes", node_to_query)
        return node_to_query

    @cached_property
    def _options(self) -> Dict[str, str]:
        return dict(item.strip().split("=") for item in self.stress_cmd.replace('table_compare', '').strip().split(";"))

    @property
    def _interval(self) -> int:
        return int(self._options.get('interval', 20))

    @property
    def _timeout(self) -> int:
        return int(self._options.get('timeout', 28800))

    def _run_stress(self, loader, loader_idx, cpu_idx):
        KclStressEvent.start(node=loader, stress_cmd=self.stress_cmd).publish()
        try:
            src_table = self._options.get('src_table')
            dst_table = self._options.get('dst_table')
            end_time = time.time() + self._timeout

            while not self._stop_event.is_set():
                node: BaseNode = self.db_node_to_query(loader)
                node.running_nemesis = "Compare tables size by cf-stats"
                node.run_nodetool('flush')

                dst_size = node.get_cfstats(dst_table)['Number of partitions (estimate)']
                src_size = node.get_cfstats(src_table)['Number of partitions (estimate)']

                node.running_nemesis = None
                status = f"== CompareTablesSizesThread: dst table/src table number of partitions: {dst_size}/{src_size} =="
                LOGGER.info(status)
                InfoEvent(f'[{time.time()}/{end_time}] {status}').publish()

                if src_size == 0:
                    continue
                if time.time() > end_time:
                    InfoEvent(f"== CompareTablesSizesThread: exiting on timeout of {self._timeout}").publish()
                    break
                time.sleep(self._interval)
            return None

        except Exception as exc:  # pylint: disable=broad-except
            KclStressEvent.failure(
                node=loader,
                stress_cmd=self.stress_cmd,
                errors=[format_stress_cmd_error(exc), ]).publish()
            raise
        finally:
            KclStressEvent.finish(node=loader).publish()
