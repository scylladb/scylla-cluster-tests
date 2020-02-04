import os
import re
import logging
import time
import uuid
import tempfile
from textwrap import dedent

from sdcm.prometheus import nemesis_metrics_obj
from sdcm.sct_events import YcsbStressEvent, Severity
from sdcm.remote import FailuresWatcher
from sdcm.utils.common import FileFollowerThread
from sdcm.utils.thread import DockerBasedStressThread
from sdcm.utils.docker import RemoteDocker

LOGGER = logging.getLogger(__name__)


class YcsbStatsPublisher(FileFollowerThread):
    METRICS = dict()
    collectible_ops = ['read', 'insert', 'update', 'cleanup', 'read-failed', 'update-failed', 'verify']

    def __init__(self, loader_node, loader_idx, ycsb_log_filename):
        super().__init__()
        self.loader_node = loader_node
        self.loader_idx = loader_idx
        self.ycsb_log_filename = ycsb_log_filename

        for operation in self.collectible_ops:
            gauge_name = self.gauge_name(operation)
            if gauge_name not in self.METRICS:
                metrics = nemesis_metrics_obj()
                self.METRICS[gauge_name] = metrics.create_gauge(gauge_name,
                                                                'Gauge for ycsb metrics',
                                                                ['instance', 'loader_idx', 'type'])

    @staticmethod
    def gauge_name(operation):
        return 'collectd_ycsb_%s_gauge' % operation.replace('-', '_')

    def set_metric(self, operation, name, value):
        metric = self.METRICS[self.gauge_name(operation)]
        metric.labels(self.loader_node.ip_address, self.loader_idx, name).set(value)

    def handle_verify_metric(self, line):
        verify_status_regex = re.compile(r"Return\((?P<status>.*?)\)=(?P<value>\d*)")
        verify_regex = re.compile(r'\[VERIFY:(.*?)\]')
        verify_content = verify_regex.findall(line)[0]

        for status_match in verify_status_regex.finditer(verify_content):
            stat = status_match.groupdict()
            self.set_metric('verify', stat['status'], float(stat['value']))

    def run(self):
        # pylint: disable=too-many-nested-blocks

        # 729.39 current ops/sec;
        # [READ: Count=510, Max=195327, Min=2011, Avg=4598.69, 90=5743, 99=12583, 99.9=194815, 99.99=195327]
        # [CLEANUP: Count=5, Max=3, Min=0, Avg=0.6, 90=3, 99=3, 99.9=3, 99.99=3]
        # [UPDATE: Count=490, Max=190975, Min=2004, Avg=3866.96, 90=4395, 99=6755, 99.9=190975, 99.99=190975]

        regex_dict = dict()
        for operation in self.collectible_ops:
            regex_dict[operation] = re.compile(
                fr'\[{operation.upper()}:\sCount=(?P<count>\d*?),'
                fr'.*?Max=(?P<max>\d*?),.*?Min=(?P<min>\d*?),'
                fr'.*?Avg=(?P<avg>.*?),.*?90=(?P<p90>\d*?),'
                fr'.*?99=(?P<p99>\d*?),.*?99.9=(?P<p999>\d*?),'
                fr'.*?99.99=(?P<p9999>\d*?)[\],\s]'
            )

        while not self.stopped():
            exists = os.path.isfile(self.ycsb_log_filename)
            if not exists:
                time.sleep(0.5)
                continue

            for _, line in enumerate(self.follow_file(self.ycsb_log_filename)):
                if self.stopped():
                    break
                try:
                    for operation, regex in regex_dict.items():
                        match = regex.search(line)
                        if match:
                            if operation == 'verify':
                                self.handle_verify_metric(line)

                            for key, value in match.groupdict().items():
                                if not key == 'count':
                                    value = float(value) / 1000.0
                                self.set_metric(operation, key, float(value))

                except Exception:  # pylint: disable=broad-except
                    LOGGER.exception("fail to send metric")


class YcsbStressThread(DockerBasedStressThread):  # pylint: disable=too-many-instance-attributes

    def copy_template(self, docker):
        if 'dynamodb' in self.stress_cmd:
            dynamodb_teample = dedent('''
                measurementtype=hdrhistogram
                dynamodb.awsCredentialsFile = /tmp/aws_empty_file
                dynamodb.endpoint = http://{0}:{1}
                requestdistribution = uniform
            '''.format(self.node_list[0].private_ip_address,
                       self.params.get('alternator_port')))  # TODO: hardcode to seed node

            dynamodb_primarykey_type = self.params.get('dynamodb_primarykey_type', 'HASH')

            if dynamodb_primarykey_type == 'HASH_AND_RANGE':
                dynamodb_teample += dedent('''
                    dynamodb.primaryKey = p
                    dynamodb.hashKeyName = c
                    dynamodb.primaryKeyType = HASH_AND_RANGE
                ''')
            elif dynamodb_primarykey_type == 'HASH':
                dynamodb_teample += dedent('''
                    dynamodb.primaryKey = p
                    dynamodb.primaryKeyType = HASH
                ''')

            aws_empty_file = dedent(""""
                accessKey =
                secretKey =
            """)

            with tempfile.NamedTemporaryFile(mode='w+', encoding='utf-8') as tmp_file:
                tmp_file.write(dynamodb_teample)
                tmp_file.flush()
                docker.send_files(tmp_file.name, os.path.join('/tmp', 'dynamodb.properties'))

            with tempfile.NamedTemporaryFile(mode='w+', encoding='utf-8') as tmp_file:
                tmp_file.write(aws_empty_file)
                tmp_file.flush()
                docker.send_files(tmp_file.name, os.path.join('/tmp', 'aws_empty_file'))

            self.stress_cmd += f' -s -P /tmp/dynamodb.properties -p maxexecutiontime={self.timeout}'

    def _run_stress(self, loader, loader_idx, cpu_idx):

        docker = RemoteDocker(loader, "scylladb/hydra-loaders:ycsb-jdk8-20200204",
                              extra_docker_opts=f'--network=host --label shell_marker={self.shell_marker}')
        self.copy_template(docker)

        if not os.path.exists(loader.logdir):
            os.makedirs(loader.logdir)
        log_file_name = os.path.join(loader.logdir, 'ycsb-l%s-c%s-%s.log' %
                                     (loader_idx, cpu_idx, uuid.uuid4()))
        LOGGER.debug('ycsb-stress local log: %s', log_file_name)

        def raise_event_callback(sentinal, line):  # pylint: disable=unused-argument
            if line:
                YcsbStressEvent('error', severity=Severity.CRITICAL, node=loader,
                                stress_cmd=self.stress_cmd, errors=[line])

        YcsbStressEvent('start', node=loader, stress_cmd=self.stress_cmd)
        try:
            LOGGER.debug("running: %s", self.stress_cmd)

            if self.stress_num > 1:
                node_cmd = 'taskset -c %s bash -c "%s"' % (cpu_idx, self.stress_cmd)
            else:
                node_cmd = self.stress_cmd

            node_cmd = 'cd /YCSB && {}'.format(node_cmd)

            with YcsbStatsPublisher(loader, loader_idx, ycsb_log_filename=log_file_name):
                result = docker.run(cmd=node_cmd,
                                    timeout=self.timeout + 60,
                                    log_file=log_file_name,
                                    watchers=[FailuresWatcher(r'ERROR|UNEXPECTED_STATE', callback=raise_event_callback, raise_exception=False)])
        finally:
            YcsbStressEvent('finish', node=loader, stress_cmd=self.stress_cmd, log_file_name=log_file_name)

        return result
