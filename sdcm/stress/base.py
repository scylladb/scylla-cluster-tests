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

import logging
import random
import concurrent.futures
from pathlib import Path
from functools import cached_property
import uuid

from sdcm.cluster import BaseLoaderSet
from sdcm.utils.common import generate_random_string
from sdcm.utils.docker_remote import RemoteDocker
from sdcm.sct_events import Severity
from sdcm.sct_events.stress_events import StressEvent
from sdcm.remote.libssh2_client.exceptions import Failure
LOGGER = logging.getLogger(__name__)


class DockerBasedStressThread:  # pylint: disable=too-many-instance-attributes
    DOCKER_IMAGE_PARAM_NAME = ""  # test yaml param that stores image

    def __init__(self, loader_set, stress_cmd, timeout, stress_num=1, node_list=None,  # pylint: disable=too-many-arguments
                 round_robin=False, params=None, stop_test_on_failure=True):
        self.loader_set: BaseLoaderSet = loader_set
        self.stress_cmd = stress_cmd
        self.timeout = timeout
        # prolong timeout by 10% to avoid killing stress process
        self.hard_timeout = self.timeout + int(self.timeout * 0.1)
        # prolong soft timeout by 5%
        self.soft_timeout = self.timeout + int(self.timeout * 0.05)

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
        self.hdr_tags = []

        if "k8s" not in self.params.get("cluster_backend") and self.docker_image_name:
            for loader in self.loader_set.nodes:
                RemoteDocker.pull_image(loader, self.docker_image_name)

    @cached_property
    def docker_image_name(self):
        return self.params.get(self.DOCKER_IMAGE_PARAM_NAME)

    def configure_executer(self):

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

    def run(self):
        self.configure_executer()
        for loader in self.loaders:
            for cpu_idx in range(self.stress_num):
                self.results_futures += [self.executor.submit(self._run_stress, *(loader, loader.node_index, cpu_idx))]

        return self

    def _run_stress(self, loader, loader_idx, cpu_idx):
        raise NotImplementedError()

    def get_results(self):
        results = []
        timeout = self.hard_timeout + 120
        LOGGER.debug('Wait for %s stress threads results', self.max_workers)
        for future in concurrent.futures.as_completed(self.results_futures, timeout=timeout):
            results.append(future.result())

        return results

    def verify_results(self):
        results = []
        errors = []
        timeout = self.hard_timeout + 120
        LOGGER.debug('Wait for %s stress threads to verify', self.max_workers)
        for future in concurrent.futures.as_completed(self.results_futures, timeout=timeout):
            results.append(future.result())

        return results, errors

    def kill(self):
        if self.loaders and self.loaders[0].is_kubernetes():
            for loader in self.loaders:
                loader.remoter.stop()
        else:
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
            return node_to_query.cql_address
        return self.node_list[0].cql_address

    @property
    def connection_bundle_file(self) -> Path:
        return self.node_list[0].parent_cluster.connection_bundle_file

    @property
    def target_connection_bundle_file(self) -> str:
        return str(Path('/tmp/') / self.connection_bundle_file.name)

    def configure_event_on_failure(self, stress_event: StressEvent, exc: Exception | Failure):
        error_msg = format_stress_cmd_error(exc)
        if (hasattr(exc, "result") and exc.result.failed) and exc.result.exited == 137:
            error_msg = f"Stress killed by test/teardown\n{error_msg}"
            stress_event.severity = Severity.WARNING
        elif self.stop_test_on_failure:
            stress_event.severity = Severity.CRITICAL
        else:
            stress_event.severity = Severity.ERROR
        stress_event.add_error(errors=[error_msg])

    @staticmethod
    def _build_log_file_id(loader_idx, cpu_idx, keyspace_idx):
        keyspace_suffix = f"-k{keyspace_idx}" if keyspace_idx else ""
        return f"l{loader_idx}-c{cpu_idx}{keyspace_suffix}-{uuid.uuid4()}"


def format_stress_cmd_error(exc: Exception) -> str:
    """Format nicely the exception from a stress command failure."""

    if hasattr(exc, "result") and exc.result.failed:
        # Report only first two lines
        message = "\n".join(exc.result.stderr.splitlines()[:2])
        return f"Stress command completed with bad status {exc.result.exited}: {message}"
    return f"Stress command execution failed with: {exc}"
