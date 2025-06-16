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


class DockerBasedStressThread:
    DOCKER_IMAGE_PARAM_NAME = ""  # test yaml param that stores image

    def __init__(self, loader_set, stress_cmd, timeout, stress_num=1, node_list=None,
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
        self.stress_operation = self.set_stress_operation(stress_cmd)
        self.stress_tool_name = ""
        self.datacenter_option_name = "datacenter"
        self.rack_option_name = "rack"

        if "k8s" not in self.params.get("cluster_backend") and self.docker_image_name:
            for loader in self.loader_set.nodes:
                RemoteDocker.pull_image(loader, self.docker_image_name)

    def set_stress_operation(self, stress_cmd):
        return ""

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
        self.executor = concurrent.futures.ThreadPoolExecutor(
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

    def parse_results(self) -> tuple[list, dict]:
        """
        Parses the raw results from the finished stress threads

        :returns: tuple of (results, errors) lists, where:
            - results: list of results from the stress threads that finished successfully
            - errors: dict of errors, where the key is the loader name and the value is a list of errors
                occurred on that loader
        """
        results = []
        errors = {}

        stress_results = self.get_results()
        for loader, result, event in stress_results:
            if result:
                if hasattr(self, '_parse_stress_summary'):
                    output = result.stdout + result.stderr
                    if stress_summary := self._parse_stress_summary(output.splitlines()):
                        results.append(stress_summary)
                else:
                    results.append(result)

            if event and getattr(event, 'errors', None):
                errors.setdefault(loader.name, []).extend(event.errors)

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

    def get_stress_usage_text(self, loader, option):
        result = loader.run(
            cmd=f'{self.stress_tool_name} help {option} | grep "^Usage:"',
            timeout=self.timeout,
            ignore_status=True).stdout
        LOGGER.debug("Available suboptions for '%s', '%s': %s", self.stress_tool_name, option, result)
        return result

    @staticmethod
    def _parse_help_text(result):
        """
            Parses the help text output from the stress tool to extract available suboptions.

            Args:
                result (str): The help text output as a string.

            Returns:
                list: A list of suboption names found in the help text.
        """
        raise NotImplementedError("Subclasses must implement _parse_help_text method to parse help text output.")

    def _get_available_suboptions(self, loader, option, _cache=None):
        """
        Returns a list of available suboptions for the stress tool.
        This is a placeholder method and should be overridden in subclasses.
        """
        if _cache is None:
            _cache = {}

        if cached_value := _cache.get(option):
            return cached_value

        try:
            result = self.get_stress_usage_text(loader, option)
        except Exception:  # noqa: BLE001
            return []

        findings = self._parse_help_text(result)
        LOGGER.debug("Parsed suboptions for '%s': %s", option, findings)
        _cache[option] = findings
        LOGGER.debug("Cached suboptions for '%s': %s", option, _cache[option])
        return findings

    def build_rack_option(self, loader_rack, loader=None):
        rack_option_cmd = f"{self.rack_option_name}={loader_rack} "
        return rack_option_cmd

    def get_datacenter_name_for_loader(self, loader):
        datacenter_name_per_region = self.loader_set.get_datacenter_name_per_region(db_nodes=self.node_list)
        datacenter_cmd = ""
        if loader_dc := datacenter_name_per_region.get(loader.region):
            datacenter_cmd = f" {self.datacenter_option_name}={loader_dc} "
        else:
            LOGGER.error("Not found datacenter for loader region '%s'. Datacenter per loader dict: %s",
                         loader.region, datacenter_name_per_region)
        return datacenter_cmd

    def adjust_cmd_node_option(self, stress_cmd, loader, cmd_runner, stress_node_option_name="-node", help_option="-node"):
        """
            Adjusts the stress command to include node and rack options based on the current configuration.

            This method modifies the provided `stress_cmd` string to specify which database nodes and racks
            should be targeted by the stress tool. It appends node IPs and, if applicable, rack and datacenter
            information to the command. The adjustments depend on the loader's region, rack, and the test
            configuration parameters.

            Args:
                stress_cmd (str): The initial stress command to be adjusted.
                loader: The loader node object for which the command is being constructed.
                cmd_runner: The object used to run commands on the loader.
                stress_node_option_name (str, optional): The name of the node option to use in the stress command. Defaults to "-node".
                help_option (str, optional): The help option to query available suboptions. Defaults to "-node".

            Returns:
                str: The adjusted stress command string with node and rack options appended as needed.
        """
        LOGGER.debug("Start adjusting stress command to use nodes: %s", stress_cmd)
        datacenter_cmd, rack_option_cmd = "", ""
        if self.node_list and stress_node_option_name not in stress_cmd:
            stress_cmd += f" {stress_node_option_name} "
            LOGGER.debug("Adjusting stress command to use nodes: %s", stress_cmd)

            node_list = self.node_list
            LOGGER.debug("Node list to use in stress command: %s", node_list)
            help_text = self._get_available_suboptions(cmd_runner, help_option)
            self.datacenter_option_name = next(
                (opt for opt in help_text if self.datacenter_option_name in opt), self.datacenter_option_name)
            LOGGER.debug("Datacenter option name: %s", self.datacenter_option_name)
            if self.params.get("rack_aware_loader"):
                self.rack_option_name = next(
                    (opt for opt in help_text if self.rack_option_name in opt), self.rack_option_name)
                LOGGER.debug("Rack option name: %s", self.rack_option_name)
                LOGGER.debug("Rack-aware loader is enabled, trying to pin stress command to rack")
                # if there are multiple rack/AZs configured, we'll try to configue c-s to pin to them
                rack_names = self.loader_set.get_rack_names_per_datacenter_and_rack_idx(db_nodes=self.node_list)
                by_region_rack_names = self.loader_set.get_rack_names_per_datacenter_from_rack_mapping(rack_names)
                if any(len(racks) > 1 for racks in by_region_rack_names.values()) and self.rack_option_name in help_text:
                    LOGGER.debug("Loader rack: %s", loader.rack)
                    LOGGER.debug("Rack names for loader region: %s",
                                 rack_names.get((str(loader.region), str(loader.rack))))
                    if loader_rack := rack_names.get((str(loader.region), str(loader.rack))):
                        rack_option_cmd = self.build_rack_option(loader_rack, loader)
                        LOGGER.debug("Rack value for stress command:  %s", rack_option_cmd)
                        node_list = self.loader_set.get_nodes_per_datacenter_and_rack_idx(
                            db_nodes=self.node_list).get((str(loader.region), str(loader.rack)))
                        LOGGER.debug("Nodes list: %s", node_list)

            if self.loader_set.test_config.MULTI_REGION and f"{self.datacenter_option_name}=" not in rack_option_cmd:
                # The datacenter name can be received from "nodetool status" output. It's possible for DB nodes only,
                # not for loader nodes. So call next function for DB nodes
                datacenter_cmd = self.get_datacenter_name_for_loader(loader)

            node_ip_list = [n.cql_address for n in node_list]
            LOGGER.debug("Node IPs to use in stress command: %s", node_ip_list)

            stress_cmd += ",".join(node_ip_list)
            if datacenter_cmd:
                stress_cmd += f"{datacenter_cmd}"
            if rack_option_cmd:
                stress_cmd += f" {rack_option_cmd}"
        LOGGER.debug("Final stress command: %s", stress_cmd)
        return stress_cmd


def format_stress_cmd_error(exc: Exception) -> str:
    """Format nicely the exception from a stress command failure."""

    if hasattr(exc, "result") and exc.result.failed:
        # Report only first two lines
        message = "\n".join(exc.result.stderr.splitlines()[:2])
        return f"Stress command completed with bad status {exc.result.exited}: {message}"
    return f"Stress command execution failed with: {exc}"
