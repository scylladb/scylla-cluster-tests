import os
import re
import logging
import time
import uuid
import concurrent.futures
from distutils.util import strtobool  # pylint: disable=import-error,no-name-in-module
import atexit


from sdcm.prometheus import nemesis_metrics_obj
from sdcm.sct_events import NdbenchStressEvent, Severity
from sdcm.utils.common import FileFollowerThread, generate_random_string
from sdcm.remote import FailuresWatcher

LOGGER = logging.getLogger(__name__)


class NdBenchStatsPublisher(FileFollowerThread):
    METRICS = dict()
    collectible_ops = ['read', 'write']

    def __init__(self, loader_node, loader_idx, ndbench_log_filename):
        super(NdBenchStatsPublisher, self).__init__()
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
        return 'collectd_ndbench_%s_gauge' % operation.replace('-', '_')

    def set_metric(self, operation, name, value):
        metric = self.METRICS[self.gauge_name(operation)]
        metric.labels(self.loader_node.ip_address, self.loader_idx, name).set(value)

    def run(self):
        # INFO RPSCount:78 - Read avg: 0.314ms, Read RPS: 7246, Write avg: 0.39ms, Write RPS: 1802, total RPS: 9048, Success Ratio: 100%
        stat_regex = re.compile(
            r'Read avg: (?P<read_lat_avg>.*?)ms.*?Read RPS: (?P<read_ops>.*?),.*?Write avg: (?P<write_lat_avg>.*?)ms.*?Write RPS: (?P<write_ops>.*?),', re.IGNORECASE)

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

                except Exception:  # pylint: disable=broad-except
                    LOGGER.exception("fail to send metric")


def convert_bool_or_int(value):
    try:
        return int(value)
    except ValueError:
        pass

    try:
        return strtobool(value)
    except ValueError:
        pass

    return value


class RemoteDocker:
    def __init__(self, node, image_name, ports=None, command_line="tail -f /dev/null", extra_docker_opts=""):  # pylint: disable=too-many-arguments
        self.node = node
        self._internal_ip_address = None

        ports = " ".join([f'-p {port}:{port}' for port in ports]) if ports else ""
        res = self.node.remoter.run(
            f'docker run {extra_docker_opts} -d {ports} {image_name} {command_line}', verbose=True)
        self.docker_id = res.stdout.strip()
        atexit.register(self.atexit)

    @property
    def internal_ip_address(self):
        if not self._internal_ip_address:
            self._internal_ip_address = self.node.remoter.run(
                f"docker inspect --format='{{{{ .NetworkSettings.IPAddress }}}}' {self.docker_id}").stdout.strip()
        return self._internal_ip_address

    @property
    def ip_address(self):
        return self.internal_ip_address

    @property
    def external_address(self):
        return self.internal_ip_address

    @property
    def remoter(self):
        return self.run

    def run(self, cmd, *args, **kwargs):
        return self.node.remoter.run(f"docker exec -i {self.docker_id} /bin/bash -c '{cmd}'", *args, **kwargs)

    def atexit(self):
        return self.node.remoter.run(f"docker rm -f {self.docker_id}", verbose=False, ignore_status=True)


class NdBenchStressThread:  # pylint: disable=too-many-instance-attributes

    def __init__(self, loader_set, stress_cmd, timeout, stress_num=1, node_list=None, round_robin=False,  # pylint: disable=too-many-arguments
                 params=None):
        self.loader_set = loader_set
        self.timeout = timeout
        self.stress_num = stress_num
        self.node_list = node_list if node_list else []
        self.round_robin = round_robin
        self.params = params if params else dict()

        # remove the ndbench command, and parse the rest of the ; separated values
        stress_cmd = re.sub(r'^ndbench', '', stress_cmd)
        self.stress_cmd = ' '.join([f'-Dndbench.config.{param.strip()}' for param in stress_cmd.split(';')])
        self.stress_cmd = f'./gradlew -Dndbench.config.cli.timeoutMillis={self.timeout * 1000}' \
                          f' -Dndbench.config.cass.host={self.node_list[0].external_address} {self.stress_cmd} run'

        self.executor = None
        self.results_futures = []
        self.max_workers = 0
        self.shell_marker = generate_random_string(20)

    def run(self):
        if self.round_robin:
            self.stress_num = 1
            loaders = [self.loader_set.get_loader()]
            LOGGER.debug("Round-Robin through loaders, Selected loader is {} ".format(loaders))
        else:
            loaders = self.loader_set.nodes

        self.max_workers = len(loaders) * self.stress_num
        LOGGER.debug("Starting %d ndbench Worker threads", self.max_workers)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)

        for loader_idx, loader in enumerate(loaders):
            for cpu_idx in range(self.stress_num):
                self.results_futures += [self.executor.submit(self._run_stress, *(loader, loader_idx, cpu_idx))]

        return self

    def create_stress_cmd(self, node, loader_idx):
        pass

    def _run_stress(self, loader, loader_idx, cpu_idx):
        log_dir = os.path.join(loader.logdir, self.loader_set.name)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file_name = os.path.join(log_dir, f'ndbench-l{loader_idx}-c{cpu_idx}-{uuid.uuid4()}.log')
        LOGGER.debug('ndbench local log: %s', log_file_name)

        def raise_event_callback(sentinal, line):  # pylint: disable=unused-argument
            if line:
                NdbenchStressEvent(type='error', severity=Severity.CRITICAL,
                                   node=loader, stress_cmd=self.stress_cmd, errors=[str(line)])

        NdbenchStressEvent('start', node=loader, stress_cmd=self.stress_cmd)
        try:

            LOGGER.debug("running: %s", self.stress_cmd)

            if self.stress_num > 1:
                node_cmd = f'taskset -c {cpu_idx} bash -c "{self.stress_cmd}"'
            else:
                node_cmd = self.stress_cmd

            docker = RemoteDocker(loader, 'scylladb/hydra-loaders:ndbench-jdk8-20200206',
                                  extra_docker_opts=f'--network=host --label shell_marker={self.shell_marker}')

            node_cmd = f'STRESS_TEST_MARKER={self.shell_marker}; cd /ndbench && {node_cmd}'

            with NdBenchStatsPublisher(loader, loader_idx, ndbench_log_filename=log_file_name):
                result = docker.run(cmd=node_cmd,
                                    timeout=self.timeout + 60,
                                    ignore_status=True,
                                    log_file=log_file_name,
                                    verbose=True,
                                    watchers=[FailuresWatcher(r'\sERROR|\sFAILURE|\sFAILED', callback=raise_event_callback, raise_exception=False)])

        finally:
            NdbenchStressEvent('finish', node=loader, stress_cmd=self.stress_cmd, log_file_name=log_file_name)

        return result

    def get_results(self):
        ret = []
        results = []
        timeout = self.timeout + 120
        LOGGER.debug('Wait for %s stress threads results', self.max_workers)
        for future in concurrent.futures.as_completed(self.results_futures, timeout=timeout):
            results.append(future.result())

        return ret

    def verify_results(self):
        ret = []
        results = []
        errors = []
        timeout = self.timeout + 120
        LOGGER.debug('Wait for %s stress threads to verify', self.max_workers)
        for future in concurrent.futures.as_completed(self.results_futures, timeout=timeout):
            results.append(future.result())

        return ret, errors

    def kill(self):
        if self.round_robin:
            self.stress_num = 1
            loaders = [self.loader_set.get_loader()]
        else:
            loaders = self.loader_set.nodes
        for loader in loaders:
            loader.remoter.run(cmd=f"docker rm -f `docker ps -a -q --filter label=shell_marker={self.shell_marker}`",
                               timeout=60,
                               ignore_status=True)
