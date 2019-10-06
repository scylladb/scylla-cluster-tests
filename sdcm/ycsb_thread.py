import os
import logging
import time
import uuid
import concurrent.futures
import tempfile
from textwrap import dedent

from sdcm.prometheus import nemesis_metrics_obj
from sdcm.sct_events import YcsbStressEvent
from sdcm.remote import FailuresWatcher

LOGGER = logging.getLogger(__name__)


class YcsbStressThread():  # pylint: disable=too-many-instance-attributes
    METRICS = dict()
    collectible_ops = ['read', 'update', 'cleanup', 'read-failed', 'update-failed']

    def __init__(self, loader_set, stress_cmd, timeout, output_dir, stress_num=1, node_list=None, round_robin=False,  # pylint: disable=too-many-arguments
                 params=None):
        self.loader_set = loader_set
        self.stress_cmd = stress_cmd
        self.timeout = timeout
        self.output_dir = output_dir
        self.stress_num = stress_num
        self.node_list = node_list if node_list else []
        self.round_robin = round_robin
        self.params = params if params else dict()

        self.executor = None
        self.results_futures = []
        self.max_workers = 0

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

    def run(self):
        if self.round_robin:
            self.stress_num = 1
            loaders = [self.loader_set.get_loader()]
            LOGGER.debug("Round-Robin through loaders, Selected loader is {} ".format(loaders))
        else:
            loaders = self.loader_set.nodes

        self.max_workers = len(loaders) * self.stress_num
        LOGGER.debug("Starting %d ycsb Worker threads", self.max_workers)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)

        for loader_idx, loader in enumerate(loaders):
            for cpu_idx in range(self.stress_num):
                self.results_futures += [self.executor.submit(self._run_stress, *(loader, loader_idx, cpu_idx))]

        return self

    def copy_template(self, loader):
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

            with tempfile.NamedTemporaryFile() as tmp_file:
                tmp_file.write(dynamodb_teample)
                tmp_file.flush()
                loader.remoter.send_files(tmp_file.name, os.path.join('/tmp', 'dynamodb.properties'))

            with tempfile.NamedTemporaryFile() as tmp_file:
                tmp_file.write(aws_empty_file)
                tmp_file.flush()
                loader.remoter.send_files(tmp_file.name, os.path.join('/tmp', 'aws_empty_file'))

            self.stress_cmd += ' -P /tmp/dynamodb.properties'

    def create_stress_cmd(self, node, loader_idx):
        pass

    def _run_stress(self, loader, loader_idx, cpu_idx):
        # pylint: disable=too-many-locals, too-many-nested-blocks, too-many-branches
        output = ''

        self.copy_template(loader)

        log_dir = os.path.join(self.output_dir, self.loader_set.name)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file_name = os.path.join(log_dir, 'cassandra-stress-l%s-c%s-%s.log' % (loader_idx, cpu_idx, uuid.uuid4()))
        LOGGER.debug('cassandra-stress local log: %s', log_file_name)

        def set_metric(operation, name, value):
            metric = self.METRICS[self.gauge_name(operation)]
            metric.labels(loader.ip_address, loader_idx, name).set(value)

        def inc_metric(operation, name, value):
            metric = self.METRICS[self.gauge_name(operation)]
            metric.labels(loader.ip_address, loader_idx, name).inc(value)

        def raise_event_callback(sentinal, line):  # pylint: disable=unused-argument
            YcsbStressEvent('error', node=loader, stress_cmd=self.stress_cmd, errors=line)

        YcsbStressEvent('start', node=loader, stress_cmd=self.stress_cmd)
        timeout_start = time.time()
        try:
            while time.time() < timeout_start + self.timeout:

                LOGGER.debug("running: %s", self.stress_cmd)

                if self.stress_num > 1:
                    node_cmd = 'taskset -c %s bash -c "%s"' % (cpu_idx, self.stress_cmd)
                else:
                    node_cmd = self.stress_cmd

                node_cmd = 'cd ~/ycsb-0.15.0 && {}'.format(node_cmd)
                result = loader.remoter.run(cmd=node_cmd,
                                            timeout=self.timeout,
                                            ignore_status=True,
                                            log_file=log_file_name,
                                            watchers=[FailuresWatcher('ERROR', callback=raise_event_callback)])

                assert result.exit_status == 0

                LOGGER.debug(result.stdout)
                output += result.stdout
                run_time = 0
                # TODO: get statistic from -s format
                #  '''2019-08-06 13:03:18:305 10 sec: 5390 operations; 538.95 current ops/sec; est completion in 1 day 1 hour [INSERT: Count=5391, Max=1956863, Min=3196, Avg=204978.3, 90=324863, 99=1863679, 99.9=1902591, 99.99=1927167] '''
                for line in result.stdout.splitlines():
                    if '[OVERALL]' in line:
                        title, _type, measure = line.split(', ')
                        if title == '[OVERALL]' and _type == 'RunTime(ms)':
                            run_time = int(measure)

                    for operation in self.collectible_ops:
                        if '[{}]'.format(operation.upper()) in line:
                            title, _type, measure = line.split(', ')
                            if _type == 'Operations':
                                set_metric(operation, 'ops', float(measure) / (run_time / 1000.0))
                            if _type == 'Return=CLIENT_ERROR':
                                LOGGER.error("%s %s errors were found", _type, measure)
                                inc_metric(operation, 'errors', float(measure))

                            short_name_mapping = {
                                'MaxLatency(us)': 'lat_max',
                                '95thPercentileLatency(us)': 'lat_perc_95',
                                '99thPercentileLatency(us)': 'lat_perc_99',
                                'AverageLatency(us)': 'lat_med',
                            }
                            if _type in short_name_mapping:
                                set_metric(operation, short_name_mapping[_type], float(measure) * 0.001)
        finally:
            YcsbStressEvent('finish', node=loader, stress_cmd=self.stress_cmd, log_file_name=log_file_name)

        return output

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
