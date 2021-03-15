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
import uuid
import time
import logging
import concurrent.futures

from sdcm.loader import ScyllaBenchStressExporter
from sdcm.prometheus import nemesis_metrics_obj
from sdcm.sct_events.loaders import ScyllaBenchEvent, SCYLLA_BENCH_ERROR_EVENTS_PATTERNS
from sdcm.utils.common import FileFollowerThread, generate_random_string, convert_metric_to_ms
from sdcm.stress_thread import format_stress_cmd_error


LOGGER = logging.getLogger(__name__)


class ScyllaBenchStressEventsPublisher(FileFollowerThread):
    def __init__(self, node, sb_log_filename):
        super().__init__()
        self.sb_log_filename = sb_log_filename
        self.node = str(node)

    def run(self):
        while not self.stopped():
            exists = os.path.isfile(self.sb_log_filename)
            if not exists:
                time.sleep(0.5)
                continue

            for line_number, line in enumerate(self.follow_file(self.sb_log_filename)):
                if self.stopped():
                    break

                for pattern, event in SCYLLA_BENCH_ERROR_EVENTS_PATTERNS:
                    if pattern.search(line):
                        event.add_info(node=self.node, line=line, line_number=line_number).publish()


class ScyllaBenchThread:
    def __init__(self, stress_cmd, loader_set, timeout, node_list=None, round_robin=False, use_single_loader=False,
                 stop_test_on_failure=False, stress_num=1, credentials=None):
        if not node_list:
            node_list = []
        self.loader_set = loader_set
        self.stress_cmd = stress_cmd
        self.timeout = timeout
        self.use_single_loader = use_single_loader
        self.round_robin = round_robin
        self.node_list = node_list
        self.stress_num = stress_num
        if credentials and 'username=' not in self.stress_cmd:
            self.stress_cmd += " -username {} -password {}".format(*credentials)
        self.stop_test_on_failure = stop_test_on_failure

        self.executor = None
        self.results_futures = []
        self.shell_marker = generate_random_string(20)
        self.max_workers = 0

    def get_stress_results_bench(self):
        ret = []
        results = []

        LOGGER.debug('Wait for stress threads results')
        for future in concurrent.futures.as_completed(self.results_futures, timeout=self.timeout):
            results.append(future.result())

        for _, result in results:
            if not result:
                # Silently skip if stress command throwed error, since it was already reported in _run_stress
                continue
            output = result.stdout + result.stderr
            try:
                lines = output.splitlines()
                node_cs_res = self._parse_bench_summary(lines)  # pylint: disable=protected-access
                if node_cs_res:
                    ret.append(node_cs_res)
            except Exception as exc:
                ScyllaBenchEvent.error(node="",
                                       stress_cmd=self.stress_cmd,
                                       errors=[f"Failed to proccess stress summary due to {exc}"]).publish()

        return ret

    def verify_results(self):
        sb_summary = []
        results = []
        errors = []

        LOGGER.debug('Wait for stress threads results')
        for future in concurrent.futures.as_completed(self.results_futures, timeout=self.timeout):
            results.append(future.result())

        for node, result in results:
            if not result:
                # Silently skip if stress command throwed error, since it was already reported in _run_stress
                continue
            output = result.stdout + result.stderr

            lines = output.splitlines()
            node_cs_res = self._parse_bench_summary(lines)  # pylint: disable=protected-access

            if node_cs_res:
                sb_summary.append(node_cs_res)

            for line in lines:
                if 'java.io.IOException' in line:
                    errors += ['%s: %s' % (node, line.strip())]

        return sb_summary, errors

    def _run_stress_bench(self, node, loader_idx, stress_cmd, node_list):
        read_gap = 480  # reads starts after write, read can look before start read time to current time using several sstables
        stress_cmd = re.sub(r"SCT_TIME", f"{int(time.time()) - read_gap}", stress_cmd)
        LOGGER.debug(f"replaced stress command {stress_cmd}")

        ScyllaBenchEvent.start(node=node, stress_cmd=stress_cmd).publish()
        os.makedirs(node.logdir, exist_ok=True)

        log_file_name = os.path.join(node.logdir, f'scylla-bench-l{loader_idx}-{uuid.uuid4()}.log')
        # Select first seed node to send the scylla-bench cmds
        ips = node_list[0].ip_address

        # Find stress mode:
        #    "scylla-bench -workload=sequential -mode=write -replication-factor=3 -partition-count=100"
        #    "scylla-bench -workload=uniform -mode=read -replication-factor=3 -partition-count=100"
        found = re.search(r"-mode=(.+?) ", stress_cmd)
        stress_cmd_opt = found.group(1)

        with ScyllaBenchStressExporter(instance_name=node.ip_address,
                                       metrics=nemesis_metrics_obj(),
                                       stress_operation=stress_cmd_opt,
                                       stress_log_filename=log_file_name,
                                       loader_idx=loader_idx), \
                ScyllaBenchStressEventsPublisher(node=node, sb_log_filename=log_file_name):
            result = None
            try:
                result = node.remoter.run(
                    cmd="/$HOME/go/bin/{name} -nodes {ips}".format(name=stress_cmd.strip(), ips=ips),
                    timeout=self.timeout,
                    log_file=log_file_name)
            except Exception as exc:  # pylint: disable=broad-except
                errors_str = format_stress_cmd_error(exc)
                if "truncate: seastar::rpc::timeout_error" in errors_str:
                    event_type = ScyllaBenchEvent.timeout
                elif self.stop_test_on_failure:
                    event_type = ScyllaBenchEvent.failure
                else:
                    event_type = ScyllaBenchEvent.error
                event_type(
                    node=node,
                    stress_cmd=stress_cmd,
                    log_file_name=log_file_name,
                    errors=[errors_str, ],
                ).publish()
            else:
                ScyllaBenchEvent.finish(node=node, stress_cmd=stress_cmd, log_file_name=log_file_name).publish()

        return node, result

    def run(self):
        if self.round_robin:
            loaders = [self.loader_set.get_loader()]
        else:
            loaders = self.loader_set.nodes if not self.use_single_loader else [self.loader_set.nodes[0]]
        LOGGER.debug("Round-Robin through loaders, Selected loader is {} ".format(loaders))

        self.max_workers = (os.cpu_count() or 1) * 5
        LOGGER.debug("Starting %d scylla-bench Worker threads", self.max_workers)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)

        for loader_idx, loader in enumerate(loaders):
            self.results_futures += [self.executor.submit(self._run_stress_bench,
                                                          *(loader, loader_idx, self.stress_cmd, self.node_list))]
            time.sleep(60)

        return self

    @staticmethod
    def _parse_bench_summary(lines):  # pylint: disable=too-many-branches
        """
        Parsing bench results, only parse the summary results.
        Collect results of all nodes and return a dictionaries' list,
        the new structure data will be easy to parse, compare, display or save.
        """
        results = {'keyspace_idx': None, 'stdev gc time(ms)': None, 'Total errors': None,
                   'total gc count': None, 'loader_idx': None, 'total gc time (s)': None,
                   'total gc mb': 0, 'cpu_idx': None, 'avg gc time(ms)': None, 'latency mean': None}
        enable_parse = False

        for line in lines:
            line.strip()
            # Parse load params
            # pylint: disable=too-many-boolean-expressions
            if line.startswith('Mode:') or line.startswith('Workload:') or line.startswith('Timeout:') or \
                    line.startswith('Consistency level:') or line.startswith('Partition count') or \
                    line.startswith('Clustering rows:') or \
                    line.startswith('Rows per request:') or line.startswith('Page size:') or \
                    line.startswith('Concurrency:') or line.startswith('Connections:') or \
                    line.startswith('Maximum rate:') or line.startswith('Client compression:'):
                split_idx = line.index(':')
                key = line[:split_idx].strip()
                value = line[split_idx + 1:].split()[0]
                results[key] = value
            elif line.startswith('Clustering row size:'):
                split_idx = line.index(':')
                key = line[:split_idx].strip()
                value = ' '.join(line[split_idx + 1:].split())
                results[key] = value

            if line.startswith('Results'):
                enable_parse = True
                continue
            if line.startswith('Latency:') or ':' not in line:
                continue
            if not enable_parse:
                continue

            split_idx = line.index(':')
            key = line[:split_idx].strip()
            value = line[split_idx + 1:].split()[0]

            # the value may be in milliseconds(ms) or microseconds(string containing non-ascii character)
            value = convert_metric_to_ms(value)

            # we try to use the same stats as we have in cassandra
            if key == 'max':
                key = 'latency max'
            elif key == '99.9th':
                key = 'latency 99.9th percentile'
            elif key == '99th':
                key = 'latency 99th percentile'
            elif key == '95th':
                key = 'latency 95th percentile'
            elif key == 'median':
                key = 'latency median'
            elif key == 'mean':
                key = 'latency mean'
            elif key == 'Operations/s':
                key = 'op rate'
            elif key == 'Rows/s':
                key = 'row rate'
                results['partition rate'] = value
            elif key == 'Total ops':  # ==Total rows?
                key = 'Total partitions'
            elif key == 'Time (avg)':
                key = 'Total operation time'
            results[key] = value
        return results
