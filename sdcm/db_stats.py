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
from utils import get_job_name, retrying

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
    def request(self, url):
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
        is_gce = False
        for p in self.params.iteritems():
            if ('/run/backends/gce', 'cluster_backend', 'gce') == p:
                is_gce = True
        for p in self.params.iteritems():
            if p[1] in exclude_details or p[1].startswith('stress_cmd'):
                continue
            else:
                if is_gce and (p[0], p[1]) in \
                        [('/run', 'instance_type_loader'),
                         ('/run', 'instance_type_monitor'),
                         ('/run/databases/scylla', 'instance_type_db')]:
                    # exclude these params from gce run
                    continue
                elif p[1] == 'n_db_nodes' and isinstance(p[2], str) and re.search('\s', p[2]):  # multidc
                    setup_details['n_db_nodes'] = sum([int(i) for i in p[2].split()])
                else:
                    setup_details[p[1]] = p[2]

        new_scylla_packages = self.params.get('update_db_packages')
        setup_details['packages_updated'] = True if new_scylla_packages and os.listdir(new_scylla_packages) else False
        setup_details['cpu_platform'] = 'UNKNOWN'
        if is_gce and self.db_cluster:
            setup_details['cpu_platform'] = self.db_cluster.nodes[0]._instance.extra.get('cpuPlatform', 'UNKNOWN')

        return setup_details

    def get_test_details(self):
        test_details = {}
        test_details['sct_git_commit'] = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip()
        test_details['job_name'] = get_job_name()
        test_details['job_url'] = os.environ.get('BUILD_URL', '')
        test_details['start_host'] = platform.node()
        test_details['test_duration'] = self.params.get(key='test_duration', default=60)
        test_details['start_time'] = time.time()
        test_details['grafana_snapshot'] = ""
        test_details['prometheus_report'] = ""
        return test_details

    def create_test_stats(self, sub_type=None):
        self._test_index = self.__class__.__name__.lower()
        self._test_id = self._create_test_id()
        self._stats = self._init_stats()
        self._stats['setup_details'] = self.get_setup_details()
        self._stats['versions'] = self.get_scylla_versions()
        self._stats['test_details'] = self.get_test_details()
        if sub_type:
            self._stats['test_details']['test_name'] = '{}_{}'.format(self.params.id.name, sub_type)
        else:
            self._stats['test_details']['test_name'] = self.params.id.name
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

    def update_stress_results(self, results):
        if 'stats' not in self._stats['results']:
            self._stats['results']['stats'] = results
        else:
            self._stats['results']['stats'].extend(results)
        self.calculate_stats_average()
        self.update(dict(results=self._stats['results']))

    def calculate_stats_average(self):
        average_stats = {}
        total_stats = {}

        for key in self._stats['results']['stats'][0].keys():
            # exclude loader info from statistics
            if key in ['loader_idx', 'cpu_idx', 'keyspace_idx']:
                continue
            summary = 0
            for stat in self._stats['results']['stats']:
                if key not in stat or stat[key] == 'NaN':
                    continue
                try:
                    summary += float(stat[key])
                except:
                    average_stats[key] = stat[key]
            if key not in average_stats:
                average_stats[key] = round(summary / len(self._stats['results']['stats']), 1)
                if key in ['op rate', 'Total errors']:
                    total_stats.update({key: summary})
        if average_stats:
            self._stats['results']['stats_average'] = average_stats
        if total_stats:
            self._stats['results']['stats_total'] = total_stats

    def update_test_details(self, errors=None, coredumps=None, scylla_conf=False):
        if self.create_stats:
            update_data = {}
            self._stats['test_details']['time_completed'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            if self.monitors:
                test_start_time = self._stats['test_details']['start_time']
                update_data['results'] = self.get_prometheus_stats()
                self._stats['test_details']['grafana_snapshot'] = self.monitors.get_grafana_screenshot(test_start_time)
                self._stats['test_details']['prometheus_report'] = self.monitors.download_monitor_data()

            if self.db_cluster and scylla_conf and 'scylla_args' not in self._stats['test_details'].keys():
                node = self.db_cluster.nodes[0]
                res = node.remoter.run('grep ^SCYLLA_ARGS /etc/sysconfig/scylla-server', verbose=True)
                self._stats['test_details']['scylla_args'] = res.stdout.strip()
                res = node.remoter.run('cat /etc/scylla.d/io.conf', verbose=True)
                self._stats['test_details']['io_conf'] = res.stdout.strip()
                res = node.remoter.run('cat /etc/scylla.d/cpuset.conf', verbose=True)
                self._stats['test_details']['cpuset_conf'] = res.stdout.strip()

            self._stats['status'] = self.status
            update_data.update({'status': self._stats['status'], 'test_details': self._stats['test_details']})
            if errors:
                update_data.update({'errors': errors})
            if coredumps:
                update_data.update({'coredumps': coredumps})
            self.update(update_data)

