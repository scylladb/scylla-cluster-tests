import re
import datetime
import time
import os
import subprocess
import platform
import logging
import requests
import json
import yaml
from textwrap import dedent
from math import sqrt

from requests import ConnectionError

from es import ES
from utils import get_job_name, retrying, remove_comments


logger = logging.getLogger(__name__)


class CassandraStressCmdParseError(Exception):
    def __init__(self, cmd, ex):
        self.command = cmd
        self.exception = repr(ex)

    def __str__(self):
        return dedent("""
            Stress command: '{0.command}'
            Error: {0.exception}""".format(self))

    def __repr__(self):
        return self.__str__()


def stddev(lst):
    mean = float(sum(lst)) / len(lst)
    return sqrt(sum((x - mean)**2 for x in lst) / len(lst))


def get_stress_cmd_params(cmd):
    """
    Parsing cassandra stress command
    :param cmd: stress cmd
    :return: dict with params
    """
    cmd_params = {
        "raw_cmd": cmd
    }
    try:
        cmd = cmd.strip().split('cassandra-stress')[1].strip()
        if cmd.split(' ')[0] in ['read', 'write', 'mixed', 'counter_write', 'user']:
            cmd_params['command'] = cmd.split(' ')[0]
            if 'no-warmup' in cmd:
                cmd_params['no-warmup'] = True

            match = re.search('(cl\s?=\s?\w+)', cmd)
            if match:
                cmd_params['cl'] = match.group(0).split('=')[1].strip()

            match = re.search('(duration\s?=\s?\w+)', cmd)
            if match:
                cmd_params['duration'] = match.group(0).split('=')[1].strip()

            match = re.search('( n\s?=\s?\w+)', cmd)
            if match:
                cmd_params['n'] = match.group(0).split('=')[1].strip()
            match = re.search('profile=(\S+)\s+', cmd)
            if match:
                cmd_params['profile'] = match.group(1).strip()
                match = re.search('ops(\S+)\s+', cmd)
                if match:
                    cmd_params['ops'] = match.group(1).split('=')[0].strip('(')

            for temp in cmd.split(' -')[1:]:
                try:
                    k, v = temp.split(" ", 1)
                except ValueError as e:
                    logger.warning("%s:%s" % (temp, e))
                else:
                    cmd_params[k] = v.strip().replace("'", "")
            if 'rate' in cmd_params:
                # split rate section on separate items
                if 'threads' in cmd_params['rate']:
                    cmd_params['rate threads'] = \
                        re.search('(threads\s?=\s?(\w+))', cmd_params['rate']).group(2)
                if 'throttle' in cmd_params['rate']:
                    cmd_params['throttle threads'] =\
                        re.search('(throttle\s?=\s?(\w+))', cmd_params['rate']).group(2)
                if 'fixed' in cmd_params['rate']:
                    cmd_params['fixed threads'] =\
                        re.search('(fixed\s?=\s?(\w+))', cmd_params['rate']).group(2)
                del cmd_params['rate']

        return cmd_params
    except Exception as e:
        raise CassandraStressCmdParseError(cmd=cmd, ex=e)


def get_stress_bench_cmd_params(cmd):
    """
    Parsing bench stress command
    :param cmd: stress cmd
    :return: dict with params
    """
    cmd = cmd.strip().split('scylla-bench')[1].strip()
    cmd_params = {}
    for key in ['partition-count', 'clustering-row-count', 'clustering-row-size', 'mode',
                'workload', 'concurrency', 'max-rate', 'connection-count', 'replication-factor',
                'timeout', 'client-compression', 'duration']:
        match = re.search('(-' + key + '\s+([^-| ]+))', cmd)
        if match:
            cmd_params[key] = match.group(2).strip()
    return cmd_params


def get_gemini_cmd_params(cmd):
    cmd = cmd.strip()
    cmd_params = {
        'raw_cmd': cmd
    }
    return cmd_params


class PrometheusDBStats(object):
    def __init__(self, host, port=9090):
        self.host = host
        self.port = port
        self.range_query_url = "http://{0.host}:{0.port}/api/v1/query_range?query=".format(self)
        self.config = self.get_configuration()

    @property
    def scylla_scrape_interval(self):
        return int(self.config["scrape_configs"]["scylla"]["scrape_interval"][:-1])

    @retrying(n=5, sleep_time=7, allowed_exceptions=(ConnectionError,))
    def request(self, url, post=False):
        if post:
            response = requests.post(url)
        else:
            response = requests.get(url)
        result = json.loads(response.content)
        logger.debug("Response from Prometheus server: %s", str(result)[:200])
        if result["status"] == "success":
            return result
        else:
            logger.error("Prometheus returned error: %s", result)

    def get_configuration(self):
        result = self.request(url="http://{0.host}:{0.port}/api/v1/status/config".format(self))
        configs = yaml.safe_load(result["data"]["yaml"])
        logger.debug("Parsed Prometheus configs: %s", configs)
        new_scrape_configs = {}
        for conf in configs["scrape_configs"]:
            new_scrape_configs[conf["job_name"]] = conf
        configs["scrape_configs"] = new_scrape_configs
        return configs

    def query(self, query, start, end):
        """
        :param start_time=<rfc3339 | unix_timestamp>: Start timestamp.
        :param end_time=<rfc3339 | unix_timestamp>: End timestamp.

        :param query:
        :return: {
                  metric: { },
                  values: [[linux_timestamp1, value1], [linux_timestamp2, value2]...[linux_timestampN, valueN]]
                 }
        """
        url = "http://{0.host}:{0.port}/api/v1/query_range?query=".format(self)
        step = self.scylla_scrape_interval
        _query = "{url}{query}&start={start}&end={end}&step={step}".format(**locals())
        logger.debug("Query to PrometheusDB: %s" % _query)
        result = self.request(url=_query)
        if result:
            return result["data"]["result"]
        else:
            logger.error("Prometheus query unsuccessful!")
            return []

    @staticmethod
    def _check_start_end_time(start_time, end_time):
        if end_time - start_time < 120:
            logger.warning("Time difference too low to make a query [start_time: %s, end_time: %s" % (start_time,
                                                                                                      end_time))
            return False
        return True

    def _get_query_values(self, q, start_time, end_time):
        results = self.query(query=q, start=start_time, end=end_time)
        if results:
            return results[0]["values"]
        else:
            return []

    def get_throughput(self, start_time, end_time):
        """
        Get Scylla throughput (ops/second) from PrometheusDB

        :return: list of tuples (unix time, op/s)
        """
        if not self._check_start_end_time(start_time, end_time):
            return []
        # the query is taken from the Grafana Dashborad definition
        q = "sum(irate(scylla_transport_requests_served{}[30s]))%20%2B%20sum(irate(scylla_thrift_served{}[30s]))"
        return self._get_query_values(q, start_time, end_time)

    def get_latency(self, start_time, end_time, latency_type):
        """latency values are returned in microseconds"""
        assert latency_type in ["read", "write"]
        if not self._check_start_end_time(start_time, end_time):
            return []
        q = "histogram_quantile(0.99, sum(rate(scylla_storage_proxy_" \
            "coordinator_%s_latency_bucket{}[30s])) by (le))" % latency_type
        return self._get_query_values(q, start_time, end_time)

    def get_latency_read_99(self, start_time, end_time):
        return self.get_latency(start_time, end_time, latency_type="read")

    def get_latency_write_99(self, start_time, end_time):
        return self.get_latency(start_time, end_time, latency_type="write")

    def create_snapshot(self):
        url = "http://{0.host}:{0.port}/api/v1/admin/tsdb/snapshot".format(self)
        result = self.request(url, True)
        logger.debug('Request result: {}'.format(result))
        return result


class Stats(object):
    """
    This class is responsible for creating and updating database entry(document in Elasticsearch DB)
    There are two usage options:
    1. without arguments - as a based class of TestStatsMixin - for saving test statistics
    2. with arguments - as a separate object to update an existing document
    """

    def __init__(self, *args, **kwargs):
        self._test_index = kwargs.get('test_index', None)
        self._test_id = kwargs.get('test_id', None)
        self._es_doc_type = "test_stats"
        self.es = ES()
        self._stats = {}
        if not self._test_id:
            super(Stats, self).__init__(*args, **kwargs)

    def get_doc_id(self):
        return self._test_id

    def create(self):
        self.es.create_doc(index=self._test_index, doc_type=self._es_doc_type, doc_id=self._test_id, body=self._stats)

    def update(self, data):
        """
        Update document
        :param data: data dictionary
        """
        try:
            self.es.update_doc(index=self._test_index, doc_type=self._es_doc_type, doc_id=self._test_id, body=data)
        except Exception as ex:
            logger.error('Failed to update test stats: test_id: %s, error: %s', self._test_id, ex)


class TestStatsMixin(Stats):
    """
    This mixin is responsible for saving test details and statistics in database.
    """
    KEYS = ['test_details', 'setup_details', 'versions', 'results', 'nemesis', 'errors', 'coredumps']
    PROMETHEUS_STATS = ('throughput', 'latency_read_99', 'latency_write_99')
    PROMETHEUS_STATS_UNITS = {'throughput': "op/s", 'latency_read_99': "us", 'latency_write_99': "us"}
    STRESS_STATS = ('op rate', 'latency mean', 'latency 99th percentile')
    STRESS_STATS_TOTAL = ('op rate', 'Total errors')

    def __init__(self, *args, **kwargs):
        super(TestStatsMixin, self).__init__(*args, **kwargs)

    @staticmethod
    def _create_test_id():
        return datetime.datetime.now().strftime("%Y%m%d-%H%M%S-%f")

    def _init_stats(self):
        return {k: {} for k in self.KEYS}

    def get_scylla_versions(self):
        versions = {}
        try:
            versions_output = self.db_cluster.nodes[0].remoter.run('rpm -qa | grep scylla').stdout.splitlines()
            for line in versions_output:
                for package in ['scylla-jmx', 'scylla-server', 'scylla-tools', 'scylla-enterprise-jmx',
                                'scylla-enterprise-server', 'scylla-enterprise-tools']:
                    match = re.search('(%s-(\S+)-(0.)?([0-9]{8,8}).(\w+).)' % package, line)
                    if match:
                        versions[package.replace('-enterprise', '')] = {'version': match.group(2),
                                                                        'date': match.group(4),
                                                                        'commit_id': match.group(5)}
        except Exception as ex:
            logger.error('Failed getting scylla versions: %s', ex)

        return versions

    def get_setup_details(self):
        exclude_details = ['send_email', 'email_recipients', 'es_url', 'es_password']
        setup_details = {}
        is_gce = self.params.get('cluster_backend') == 'gce'

        if os.environ.get('SCT_NEW_CONFIG', False):
            test_params = self.params.items()
        else:
            # take only values, don't care about paths
            test_params = [(k, v) for _, k, v in self.avocado_params.iteritems()]

        for k, v in test_params:
            if k in exclude_details or (isinstance(k, str) and k.startswith('stress_cmd')):
                continue
            else:
                if is_gce and k in \
                        ['instance_type_loader',
                         'instance_type_monitor',
                         'instance_type_db']:
                    # exclude these params from gce run
                    continue
                elif k == 'n_db_nodes' and isinstance(v, str) and re.search(r'\s', v):  # multidc
                    setup_details['n_db_nodes'] = sum([int(i) for i in v.split()])
                else:
                    setup_details[k] = v

        new_scylla_packages = self.params.get('update_db_packages')
        setup_details['packages_updated'] = True if new_scylla_packages and os.listdir(new_scylla_packages) else False
        setup_details['cpu_platform'] = 'UNKNOWN'
        if is_gce and self.db_cluster:
            setup_details['cpu_platform'] = self.db_cluster.nodes[0]._instance.extra.get('cpuPlatform', 'UNKNOWN')

        setup_details['db_cluster_details'] = {}
        return setup_details

    def get_test_details(self):
        # avoid cyclic-decencies between cluster and db_stats
        from sdcm.cluster import Setup

        test_details = {}
        test_details['sct_git_commit'] = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip()
        test_details['job_name'] = get_job_name()
        test_details['job_url'] = os.environ.get('BUILD_URL', '')
        test_details['start_host'] = platform.node()
        test_details['test_duration'] = self.params.get(key='test_duration', default=60)
        test_details['start_time'] = time.time()
        test_details['grafana_snapshots'] = []
        test_details['grafana_screenshots'] = []
        test_details['grafana_annotations'] = []
        test_details['prometheus_data'] = ""
        test_details['test_id'] = Setup.test_id()
        test_details['log_files'] = {}
        return test_details

    def get_db_cluster_details(self):

        db_cluster_details = {}
        for node in self.db_cluster.nodes:
            node_system_info = {}
            node_system_info[node.name] = {
                'cpu_model': node.get_cpumodel(),
                'sys_info': node.get_system_info(),
            }
            db_cluster_details.update(node_system_info)

        return db_cluster_details

    def create_test_stats(self, sub_type=None):
        self._test_index = self.__class__.__name__.lower()
        self._test_id = self._create_test_id()
        self._stats = self._init_stats()
        self._stats['setup_details'] = self.get_setup_details()
        self._stats['versions'] = self.get_scylla_versions()
        self._stats['test_details'] = self.get_test_details()
        if sub_type:
            self._stats['test_details']['test_name'] = '{}_{}'.format(self.avocado_params.id.name, sub_type)
        else:
            self._stats['test_details']['test_name'] = self.avocado_params.id.name
        for stat in self.PROMETHEUS_STATS:
            self._stats['results'][stat] = {}
        self.create()

    def update_stress_cmd_details(self, cmd, prefix='', stresser="cassandra-stress", aggregate=True):
        section = '{prefix}{stresser}'.format(**locals())
        if section not in self._stats['test_details']:
            self._stats['test_details'][section] = [] if aggregate else {}
        if stresser == "cassandra-stress":
            cmd_params = get_stress_cmd_params(cmd)
        elif stresser == "scylla-bench":
            cmd_params = get_stress_bench_cmd_params(cmd)
        elif stresser == 'gemini':
            cmd_params = get_gemini_cmd_params(cmd)
        else:
            cmd_params = None
            self.log.warning("Unknown stresser: %s" % stresser)
        if cmd_params:
            self._stats['test_details'][section].append(cmd_params) if aggregate else\
                self._stats['test_details'][section].update(cmd_params)
            self.update(dict(test_details=self._stats['test_details']))

    def _calc_stats(self, ps_results):
        try:
            if not ps_results or len(ps_results) <= 3:
                self.log.error("Not enough data from Prometheus: %s" % ps_results)
                return {}
            stat = {}
            ops_per_sec = [float(val) for _, val in ps_results if val.lower() != "nan"]  # float("nan") is not a number
            stat["max"] = max(ops_per_sec)
            # filter all values that are less than 1% of max
            ops_filtered = filter(lambda x: x >= stat["max"] * 0.01, ops_per_sec)
            stat["min"] = min(ops_filtered)
            stat["avg"] = float(sum(ops_filtered)) / len(ops_filtered)
            stat["stdev"] = stddev(ops_filtered)
            self.log.debug("Stats: %s", stat)
            return stat
        except Exception as e:
            self.log.error("Exception when calculating PrometheusDB stats: %s" % e)
            return {}

    def get_prometheus_stats(self):
        self.log.info("Calculating throughput stats from PrometheusDB...")
        ps = PrometheusDBStats(host=self.monitors.nodes[0].public_ip_address)
        offset = 120  # 2 minutes offset
        start = int(self._stats["test_details"]["start_time"] + offset)
        end = int(time.time() - offset)
        prometheus_stats = {}
        for stat in self.PROMETHEUS_STATS:
            stat_calc_func = getattr(ps, "get_" + stat)
            prometheus_stats[stat] = self._calc_stats(ps_results=stat_calc_func(start_time=start, end_time=end))
        self._stats['results'].update(prometheus_stats)
        return prometheus_stats

    def update_stress_results(self, results, calculate_stats=True):
        if 'stats' not in self._stats['results']:
            self._stats['results']['stats'] = results
        else:
            self._stats['results']['stats'].extend(results)
        if calculate_stats:
            self.calculate_stats_average()
            self.calculate_stats_total()
        self.update(dict(results=self._stats['results']))

    def _convert_stat(self, stat, stress_result):
        if stat not in stress_result or stress_result[stat] == 'NaN':
            self.log.warning("Stress stat not found: '%s'", stat)
            return 0
        try:
            return float(stress_result[stat])
        except Exception as details:
            self.log.warning("Error in conversion of '%s' for stat '%s': '%s'"
                             "Discarding stat." % (stress_result[stat], stat, details))
        return 0

    def _calc_stat_total(self, stat):
        total = 0
        for stress_result in self._stats['results']['stats']:
            stat_val = self._convert_stat(stat=stat, stress_result=stress_result)
            if not stat_val:
                return 0  # discarding all stat results completely if one of the results is bad
            total += stat_val
        return total

    def calculate_stats_average(self):
        # calculate average stats
        average_stats = {}
        for stat in self.STRESS_STATS:
            average_stats[stat] = ''  # default
            total = self._calc_stat_total(stat=stat)
            if total:
                average_stats[stat] = round(total / len(self._stats['results']['stats']), 1)
        self._stats['results']['stats_average'] = average_stats

    def calculate_stats_total(self):
        total_stats = {}
        for stat in self.STRESS_STATS_TOTAL:
            total_stats[stat] = ''  # default
            total = self._calc_stat_total(stat=stat)
            if total:
                total_stats[stat] = total
        self._stats['results']['stats_total'] = total_stats

    def update_test_details(self, errors=None, coredumps=None, scylla_conf=False):
        if self.create_stats:
            update_data = {}
            self._stats['test_details']['time_completed'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            if self.monitors and self.monitors.nodes:
                test_start_time = self._stats['test_details']['start_time']
                update_data['results'] = self.get_prometheus_stats()
                grafana_dataset = self.monitors.get_grafana_screenshot_and_snapshot(test_start_time)
                self._stats['test_details']['grafana_screenshots'] = grafana_dataset.get('screenshots', [])
                self._stats['test_details']['grafana_snapshots'] = grafana_dataset.get('snapshots', [])
                self._stats['test_details']['grafana_annotations'] = self.monitors.upload_annotations_to_s3()
                self._stats['test_details']['prometheus_data'] = self.monitors.download_monitor_data()

            if self.db_cluster:
                self._stats['setup_details']['db_cluster_details'] = self.get_db_cluster_details()

            if self.db_cluster and scylla_conf and 'scylla_args' not in self._stats['setup_details'].keys():
                node = self.db_cluster.nodes[0]
                res = node.remoter.run('grep ^SCYLLA_ARGS /etc/sysconfig/scylla-server', verbose=True)
                self._stats['setup_details']['scylla_args'] = res.stdout.strip()
                res = node.remoter.run('cat /etc/scylla.d/io.conf', verbose=True)
                self._stats['setup_details']['io_conf'] = remove_comments(res.stdout.strip())
                res = node.remoter.run('cat /etc/scylla.d/cpuset.conf', verbose=True)
                self._stats['setup_details']['cpuset_conf'] = remove_comments(res.stdout.strip())

            self._stats['status'] = self.status
            update_data.update(
                {'status': self._stats['status'],
                 'setup_details': self._stats['setup_details'],
                 'test_details': self._stats['test_details']
                 })
            if errors:
                update_data.update({'errors': errors})
            if coredumps:
                update_data.update({'coredumps': coredumps})

            self.update(update_data)

    def get_doc_data(self, key):
        if self.create_stats:
            result = self.es.get_doc(self._test_index, self.get_doc_id(), doc_type=self._es_doc_type)

            return result['_source'].get(key, None)
