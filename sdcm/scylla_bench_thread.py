# Copyright 2020 ScyllaDB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
import uuid
import time
import logging
import concurrent.futures

from sdcm.loader import ScyllaBenchStressExporter
from sdcm.prometheus import nemesis_metrics_obj
from sdcm.sct_events import ScyllaBenchLogEvent, ScyllaBenchEvent
from sdcm.utils.common import FileFollowerThread, generate_random_string, convert_metric_to_ms
from sdcm.sct_events import Severity
from sdcm.stress_thread import format_stress_cmd_error


LOGGER = logging.getLogger(__name__)


class ScyllaBenchStressEventsPublisher(FileFollowerThread):
    def __init__(self, node, sb_log_filename):
        super(ScyllaBenchStressEventsPublisher, self).__init__()
        self.sb_log_filename = sb_log_filename
        self.node = str(node)
        self.sb_events = []
        # TODO: decide about error events
        self.sb_events = [ScyllaBenchLogEvent(type='ConsistencyError', regex=r'received only', severity=Severity.ERROR)]

    def run(self):
        patterns = [(event, re.compile(event.regex)) for event in self.sb_events]

        while not self.stopped():
            exists = os.path.isfile(self.sb_log_filename)
            if not exists:
                time.sleep(0.5)
                continue

            for line_number, line in enumerate(self.follow_file(self.sb_log_filename)):
                if self.stopped():
                    break

                for event, pattern in patterns:
                    match = pattern.search(line)
                    if match:
                        event.add_info_and_publish(node=self.node, line=line, line_number=line_number)


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
            except Exception as exc:  # pylint: disable=broad-except
                ScyllaBenchEvent(type='failure', node='', stress_cmd=self.stress_cmd, severity=Severity.ERROR,
                                 errors=[f'Failed to proccess stress summary due to {exc}'])

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

        ScyllaBenchEvent(type='start', node=str(node), stress_cmd=stress_cmd)
        os.makedirs(node.logdir, exist_ok=True)

        log_file_name = os.path.join(node.logdir, f'scylla-bench-l{loader_idx}-{uuid.uuid4()}.log')
        # Select first seed node to send the scylla-bench cmds
        ips = node_list[0].private_ip_address

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
                    event_type, event_severity = 'timeout', Severity.ERROR
                else:
                    event_type = 'failure'
                    event_severity = Severity.CRITICAL if self.stop_test_on_failure else Severity.ERROR

                ScyllaBenchEvent(type=event_type, node=str(node), stress_cmd=stress_cmd,
                                 log_file_name=log_file_name, severity=event_severity,
                                 errors=[errors_str])
            else:
                ScyllaBenchEvent(type='finish', node=str(node), stress_cmd=stress_cmd, log_file_name=log_file_name)

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
