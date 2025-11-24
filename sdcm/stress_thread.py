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
import logging
import contextlib
from typing import Any
from itertools import chain

from sdcm.db_stats import get_stress_cmd_params
from sdcm.loader import CassandraStressExporter, CassandraStressHDRExporter
from sdcm.prometheus import nemesis_metrics_obj
from sdcm.provision.helpers.certificate import SCYLLA_SSL_CONF_DIR, c_s_transport_str
from sdcm.reporting.tooling_reporter import CassandraStressVersionReporter
from sdcm.sct_events import Severity
from sdcm.utils.common import FileFollowerThread, get_data_dir_path, time_period_str_to_seconds, SoftTimeoutContext
from sdcm.utils.user_profile import get_profile_content, replace_scylla_qa_internal_path
from sdcm.sct_events.loaders import CassandraStressEvent, CS_ERROR_EVENTS_PATTERNS, CS_NORMAL_EVENTS_PATTERNS
from sdcm.stress.base import DockerBasedStressThread
from sdcm.utils.docker_remote import RemoteDocker
from sdcm.utils.remote_logger import HDRHistogramFileLogger


LOGGER = logging.getLogger(__name__)


class CassandraStressEventsPublisher(FileFollowerThread):
    def __init__(self, node: Any, cs_log_filename: str, event_id: str = None, stop_test_on_failure: bool = True):
        super().__init__()

        self.node = str(node)
        self.cs_log_filename = cs_log_filename
        self.event_id = event_id
        self.stop_test_on_failure = stop_test_on_failure

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
                        if event.severity == Severity.CRITICAL and not self.stop_test_on_failure:
                            event = event.clone()  # so we don't change the severity to other stress threads  # noqa: PLW2901
                            event.severity = Severity.ERROR
                        event.add_info(node=self.node, line=line, line_number=line_number).publish()
                        break  # Stop iterating patterns to avoid creating two events for one line of the log


class CassandraStressThread(DockerBasedStressThread):
    DOCKER_IMAGE_PARAM_NAME = 'stress_image.cassandra-stress'

    def __init__(self, loader_set, stress_cmd, timeout, stress_num=1, keyspace_num=1, keyspace_name='', compaction_strategy='',  # noqa: PLR0913
                 profile=None, node_list=None, round_robin=False, client_encrypt=False, stop_test_on_failure=True,
                 params=None):
        super().__init__(loader_set=loader_set, stress_cmd=stress_cmd, timeout=timeout,
                         stress_num=stress_num, node_list=node_list,
                         round_robin=round_robin, stop_test_on_failure=stop_test_on_failure, params=params)
        self.keyspace_num = keyspace_num
        self.keyspace_name = keyspace_name
        self.profile = profile
        self.client_encrypt = client_encrypt
        self.stop_test_on_failure = stop_test_on_failure
        self.compaction_strategy = compaction_strategy
        self.set_hdr_tags(stress_cmd)

    def set_stress_operation(self, stress_cmd):
        if " mixed " in stress_cmd:
            self.stress_operation = "mixed"
        elif " read " in stress_cmd:
            self.stress_operation = "read"
        elif " write " in stress_cmd:
            self.stress_operation = "write"
        elif " counter_read " in stress_cmd:
            self.stress_operation = "counter_read"
        elif " counter_write " in stress_cmd:
            self.stress_operation = "counter_write"
        elif " user " in stress_cmd:
            self.stress_operation = "user"
        else:
            raise ValueError(
                "Cannot detect supported stress operation type from the stress command: %s" % stress_cmd)
        return self.stress_operation

    def set_hdr_tags(self, stress_cmd):
        # TODO: add support for the "counter_write", "counter_read" and "user" modes?
        params = get_stress_cmd_params(stress_cmd)
        tag_suffix = "rt" if "fixed threads" in params else "st"
        if "user profile=" in stress_cmd:
            # Examples:
            # Write: ops(insert=1)
            # Read: ops(read=2)
            # Mixed: ops(insert=1,read=2)
            # Only standard operations (read and insert) are supported per user profile stress command in this implementation
            if "insert=" in stress_cmd:
                self.hdr_tags.append(f"WRITE-{tag_suffix}")
            elif "read=" in stress_cmd:
                self.hdr_tags.append(f"READ-{tag_suffix}")
            else:
                raise ValueError(
                    "Cannot detect supported stress operation type from the stress command with user profile: %s", stress_cmd)
        elif " mixed " in stress_cmd:
            self.hdr_tags = [f"WRITE-{tag_suffix}", f"READ-{tag_suffix}"]
        elif " read " in stress_cmd:
            self.hdr_tags = [f"READ-{tag_suffix}"]
        else:
            self.hdr_tags = [f"WRITE-{tag_suffix}"]

    @staticmethod
    def append_no_warmup_to_cmd(stress_cmd):
        if "no-warmup" not in stress_cmd:
            # add no-warmup to stress_cmd if it's not there. See issue #5767
            stress_cmd = re.sub(r'(cassandra-stress [\w]+)', r'\1 no-warmup', stress_cmd)
        return stress_cmd

    def adjust_cmd_keyspace_name(self, stress_cmd, keyspace_idx):
        if self.keyspace_name:
            stress_cmd = stress_cmd.replace(" -schema ", " -schema keyspace={} ".format(self.keyspace_name))
        elif 'keyspace=' not in stress_cmd:  # if keyspace is defined in the command respect that
            stress_cmd = stress_cmd.replace(" -schema ", " -schema keyspace=keyspace{} ".format(keyspace_idx))
        return stress_cmd

    def adjust_cmd_compaction_strategy(self, stress_cmd):
        if self.compaction_strategy and "compaction(" not in stress_cmd:
            stress_cmd = stress_cmd.replace(" -schema ", f" -schema 'compaction(strategy={self.compaction_strategy})' ")
        return stress_cmd

    def adjust_cmd_node_option(self, stress_cmd, loader, cmd_runner):
        if self.node_list and '-node' not in stress_cmd:
            stress_cmd += " -node "
            if self.loader_set.test_config.MULTI_REGION:
                # The datacenter name can be received from "nodetool status" output. It's possible for DB nodes only,
                # not for loader nodes. So call next function for DB nodes
                datacenter_name_per_region = self.loader_set.get_datacenter_name_per_region(db_nodes=self.node_list)
                if loader_dc := datacenter_name_per_region.get(loader.region):
                    stress_cmd += f"datacenter={loader_dc} "
                else:
                    LOGGER.error("Not found datacenter for loader region '%s'. Datacenter per loader dict: %s",
                                 loader.region, datacenter_name_per_region)

            node_list = self.node_list
            if self.params.get("rack_aware_loader"):
                # if there are multiple rack/AZs configured, we'll try to configue c-s to pin to them
                rack_names = self.loader_set.get_rack_names_per_datacenter_and_rack_idx(db_nodes=self.node_list)
                by_region_rack_names = self.loader_set.get_rack_names_per_datacenter_from_rack_mapping(rack_names)
                if any(len(racks) > 1 for racks in by_region_rack_names.values()) and 'rack' in self._get_available_suboptions(cmd_runner, '-node'):
                    if loader_rack := rack_names.get((str(loader.region), str(loader.rack))):
                        stress_cmd += f"rack={loader_rack} "
                        node_list = self.loader_set.get_nodes_per_datacenter_and_rack_idx(
                            db_nodes=self.node_list).get((str(loader.region), str(loader.rack)))

            node_ip_list = [n.cql_address for n in node_list]

            stress_cmd += ",".join(node_ip_list)
        return stress_cmd

    def adjust_cmd_connection_options(self, stress_cmd: str, loader, cmd_runner) -> str:
        stress_cmd = self.adjust_cmd_node_option(stress_cmd, loader, cmd_runner)
        return stress_cmd

    def create_stress_cmd(self, cmd_runner, keyspace_idx, loader):
        stress_cmd = self.stress_cmd

        stress_cmd = self.append_no_warmup_to_cmd(stress_cmd)

        # When using cassandra-stress with "user profile" the profile yaml should be provided
        if 'profile' in stress_cmd and not self.profile:
            # support of using -profile in sct test-case yaml, assumes they exists data_dir
            # TODO: move those profile to their own directory
            cs_profile, profile = get_profile_content(stress_cmd)
            keyspace_name = profile['keyspace']
            self.profile = cs_profile
            self.keyspace_name = keyspace_name

        stress_cmd = self.adjust_cmd_keyspace_name(stress_cmd, keyspace_idx)
        stress_cmd = self.adjust_cmd_compaction_strategy(stress_cmd)

        credentials = self.loader_set.get_db_auth()
        if credentials and 'user=' not in stress_cmd:
            # put the credentials into the right place into -mode section
            stress_cmd = re.sub(r'(-mode.*?)-', r'\1 user={} password={} -'.format(*credentials), stress_cmd)
        if self.client_encrypt and 'transport' not in stress_cmd:
            transport_str = c_s_transport_str(
                self.params.get('peer_verification'), self.params.get('client_encrypt_mtls'))
            stress_cmd += f" -transport '{transport_str}'"

        stress_cmd = self.adjust_cmd_connection_options(stress_cmd, loader, cmd_runner)

        if 'skip-unsupported-columns' in self._get_available_suboptions(cmd_runner, '-errors'):
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

    def _get_available_suboptions(self, loader, option, _cache={}):  # noqa: B006
        if cached_value := _cache.get(option):
            return cached_value
        try:
            result = loader.run(
                cmd=f'cassandra-stress help {option} | grep "^Usage:"',
                timeout=self.timeout,
                ignore_status=True).stdout
        except Exception:  # noqa: BLE001
            return []
        findings = re.findall(r' *\[([\w-]+?)[=?]*] *', result)
        _cache[option] = findings
        return findings

    @staticmethod
    def _disable_logging_for_cs(node, cmd_runner, _cache={}):  # noqa: B006
        if not (node.is_kubernetes() or node.name in _cache):
            cmd_runner.run("cp /etc/scylla/cassandra/logback-tools.xml .", ignore_status=True)
            _cache[node.name] = 'done'

    @staticmethod
    def _add_hdr_log_option(stress_cmd: str, hdr_log_name: str) -> str:
        if "-log" in stress_cmd:
            if match := re.search(r"\s-log ([^-]*)-?", stress_cmd):
                cs_log_option = match.group(1)
                if "hdrfile" not in cs_log_option:
                    stress_cmd = stress_cmd.replace("-log", f"-log hdrfile={hdr_log_name}")
                else:  # noqa: PLR5501
                    if replacing_hdr_file := re.search(r"hdrfile=(.*?)\s", cs_log_option):
                        stress_cmd = stress_cmd.replace(
                            f"hdrfile={replacing_hdr_file.group(1)}", f"hdrfile={hdr_log_name}")
        else:
            stress_cmd += f" -log hdrfile={hdr_log_name} interval=10s"

        return stress_cmd

    def _run_stress(self, loader, loader_idx, cpu_idx):
        pass

    def _run_cs_stress(self, loader, loader_idx, cpu_idx, keyspace_idx):  # noqa: PLR0914
        cleanup_context = contextlib.nullcontext()
        os.makedirs(loader.logdir, exist_ok=True)

        # This tag will be output in the header of c-stress result,
        # we parse it to know the loader & cpu info in _parse_stress_summary().
        stress_cmd_opt = self.stress_cmd.split("cassandra-stress", 1)[1].split(None, 1)[0]

        log_id = self._build_log_file_id(loader_idx, cpu_idx, keyspace_idx)
        log_file_name = \
            os.path.join(loader.logdir, f'cassandra-stress-{stress_cmd_opt}-{log_id}.log')
        LOGGER.debug('cassandra-stress local log: %s', log_file_name)
        remote_hdr_file_name = f"hdrh-cs-{stress_cmd_opt}-{log_id}.hdr"
        LOGGER.debug("cassandra-stress remote HDR histogram log file: %s", remote_hdr_file_name)
        local_hdr_file_name = os.path.join(loader.logdir, remote_hdr_file_name)
        LOGGER.debug("cassandra-stress HDR local file %s", local_hdr_file_name)

        remote_hdr_file_name_full_path = remote_hdr_file_name
        if "k8s" in self.params.get("cluster_backend"):
            cmd_runner = loader.remoter
            cmd_runner_name = loader.remoter.pod_name
        elif self.params.get("use_prepared_loaders"):
            cmd_runner = loader.remoter
            cmd_runner_name = loader.ip_address
        else:
            loader.remoter.run(f"touch $HOME/{remote_hdr_file_name}", ignore_status=False, verbose=False)
            remote_hdr_file_name_full_path = loader.remoter.run(
                f"realpath $HOME/{remote_hdr_file_name}", ignore_status=False, verbose=False).stdout.strip()
            cmd_runner_name = loader.ip_address

            cpu_options = ""
            cmd_runner = cleanup_context = RemoteDocker(loader, self.docker_image_name,
                                                        command_line="-c 'tail -f /dev/null'",
                                                        extra_docker_opts=f'{cpu_options} '
                                                        '--network=host '
                                                        '--security-opt seccomp=unconfined '
                                                        f'--label shell_marker={self.shell_marker}'
                                                        f' --entrypoint /bin/bash'
                                                        f' -w /'
                                                        f' -v {remote_hdr_file_name_full_path}:/{remote_hdr_file_name}')

        stress_cmd = self.create_stress_cmd(cmd_runner, keyspace_idx, loader)
        if self.params.get('cs_debug'):
            cmd_runner.send_files(get_data_dir_path('logback-tools-debug.xml'),
                                  '/etc/scylla/cassandra/logback-tools.xml', delete_dst=True)
        if self.profile:
            loader_profile_path = os.path.join('/tmp', os.path.basename(self.profile))
            with open(self.profile, encoding="utf-8") as profile_file:
                LOGGER.info('Profile content:\n%s', profile_file.read())
            cmd_runner.send_files(self.profile, loader_profile_path, delete_dst=True)
            if 'scylla-qa-internal' in self.profile:
                LOGGER.info("Replace profile path %s in c-s command with actual %s",
                            self.profile, loader_profile_path)
                stress_cmd = replace_scylla_qa_internal_path(stress_cmd, loader_profile_path)

        if self.client_encrypt:
            for ssl_file in loader.ssl_conf_dir.iterdir():
                if ssl_file.is_file():
                    cmd_runner.send_files(str(ssl_file),
                                          str(SCYLLA_SSL_CONF_DIR / ssl_file.name),
                                          verbose=True)

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

        result = None
        self._disable_logging_for_cs(loader, cmd_runner)
        try:
            prefix,  *_ = stress_cmd.split("cassandra-stress", maxsplit=1)
            reporter = CassandraStressVersionReporter(
                cmd_runner, prefix, loader.parent_cluster.test_config.argus_client())
            reporter.report()
        except Exception:  # noqa: BLE001
            LOGGER.info("Failed to collect cassandra-stress version information", exc_info=True)
        with cleanup_context, \
                CassandraStressExporter(instance_name=cmd_runner_name,
                                        metrics=nemesis_metrics_obj(),
                                        stress_operation=stress_cmd_opt,
                                        stress_log_filename=log_file_name,
                                        loader_idx=loader_idx, cpu_idx=cpu_idx), \
                CassandraStressEventsPublisher(node=loader, cs_log_filename=log_file_name,
                                               stop_test_on_failure=self.stop_test_on_failure) as publisher, \
                CassandraStressEvent(node=loader, stress_cmd=self.stress_cmd,
                                     log_file_name=log_file_name) as cs_stress_event, \
                CassandraStressHDRExporter(instance_name=cmd_runner_name,
                                           hdr_tags=self.hdr_tags,
                                           metrics=nemesis_metrics_obj(),
                                           stress_operation=stress_cmd_opt,
                                           stress_log_filename=local_hdr_file_name,
                                           loader_idx=loader_idx, cpu_idx=cpu_idx), \
                hdrh_logger_context:
            publisher.event_id = cs_stress_event.event_id
            try:
                with SoftTimeoutContext(timeout=self.soft_timeout, operation="cassandra-stress"):
                    result = cmd_runner.run(
                        cmd=node_cmd, timeout=self.hard_timeout, log_file=log_file_name, retry=0, timestamp_logs=True)
            except Exception as exc:  # noqa: BLE001
                self.configure_event_on_failure(stress_event=cs_stress_event, exc=exc)

        return loader, result, cs_stress_event

    def run(self):
        self.configure_executer()
        for loader in self.loaders:
            loader_idx = loader.node_index
            for cpu_idx in range(self.stress_num):
                for ks_idx in range(1, self.keyspace_num + 1):
                    self.results_futures += [self.executor.submit(self._run_cs_stress,
                                                                  *(loader, loader_idx, cpu_idx, ks_idx))]
                    if loader_idx == 0 and cpu_idx == 0 and self.max_workers > 1:
                        # Wait for first stress thread to create the schema, before spawning new stress threads
                        time.sleep(30)

        return self

    def _parse_stress_summary(self, lines: list) -> dict:
        """
        Parsing c-s results, only parse the summary results.
        Collect results of all nodes and return a dictionaries' list,
        the new structure data will be easy to parse, compare, display or save.
        """
        results = {}
        enable_parse = False

        for line in lines:
            line = line.strip()  # noqa: PLW2901
            if not line:
                continue
            # Parse loader & cpu info
            if line.startswith('TAG:'):
                # TAG: loader_idx:1-cpu_idx:0-keyspace_idx:1
                ret = re.findall(r"TAG: loader_idx:(\d+)-cpu_idx:(\d+)-keyspace_idx:(\d+)", line)
                results['loader_idx'] = ret[0][0]
                results['cpu_idx'] = ret[0][1]
                results['keyspace_idx'] = ret[0][2]
                continue
            if line.startswith('Username:'):
                # Mode:
                # ...
                #   Username: null
                #   Password: null
                results['username'] = line.split('Username:')[1].strip()
            if line.startswith('Results:'):
                # Results:
                # Op rate                   :    9,999 op/s  [WRITE: 9,999 op/s]
                # Partition rate            :    9,999 pk/s  [WRITE: 9,999 pk/s]
                # Row rate                  :    9,999 row/s [WRITE: 9,999 row/s]
                # ....
                enable_parse = True
                continue
            if line == '':
                continue
            if line == 'END':
                break
            if not enable_parse:
                continue
            split_idx = line.find(':')
            if split_idx < 0:
                continue
            # Op rate                   :    9,999 op/s  [WRITE: 9,999 op/s]
            # Partition rate            :    9,999 pk/s  [WRITE: 9,999 pk/s]
            # Row rate                  :    9,999 row/s [WRITE: 9,999 row/s]
            # Latency mean              :    1.1 ms [WRITE: 1.1 ms]
            # Latency median            :    0.6 ms [WRITE: 0.6 ms]
            # Latency 95th percentile   :    2.3 ms [WRITE: 2.3 ms]
            # Latency 99th percentile   :    5.4 ms [WRITE: 5.4 ms]
            # Latency 99.9th percentile :   23.7 ms [WRITE: 23.7 ms]
            # Latency max               : 15787.4 ms [WRITE: 15,787.4 ms]
            # Total partitions          : 108,000,096 [WRITE: 108,000,096]
            # Total errors              :          0 [WRITE: 0]
            # Total GC count            : 0
            # Total GC memory           : 0.000 KiB
            # Total GC time             :    0.0 seconds
            key = line[:split_idx].strip().lower()
            value = line[split_idx + 1:].split()[0].replace(",", "")
            results[key] = value
            match = re.findall(r'\[READ:\s([\d,]+\.\d+)\sms,\sWRITE:\s([\d,]+\.\d)\sms\]', line)
            if match:  # parse results for mixed workload
                results['%s read' % key] = match[0][0]
                results['%s write' % key] = match[0][1]

        if not enable_parse:
            LOGGER.warning('Cannot find summary in c-stress results: %s', lines[-10:])
            return {}
        return results


stress_cmd_get_duration_pattern = re.compile(r'(?:^|[ \n])[-]{0,2}duration[\s=]+([\d]+[hms]+)')
stress_cmd_get_warmup_pattern = re.compile(r'(?:^|[ \n])[-]{0,2}warmup[\s=]+([\d]+[hms]+)')


def get_timeout_from_stress_cmd(stress_cmd: str) -> int | None:
    """Gets timeout in seconds based on duration and warmup arguments from stress command."""
    timeout = 0
    if duration_match := stress_cmd_get_duration_pattern.search(stress_cmd):
        timeout += time_period_str_to_seconds(duration_match.group(1))
    if warmup_match := stress_cmd_get_warmup_pattern.search(stress_cmd):
        timeout += time_period_str_to_seconds(warmup_match.group(1))
    if timeout == 0:
        return None
    else:
        # adding 15 minutes to timeout for general all others delays
        return timeout + 900
