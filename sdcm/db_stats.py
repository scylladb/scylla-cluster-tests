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
from collections import defaultdict

from requests import ConnectionError

import es
from results_analyze import PerformanceResultsAnalyzer
from utils import get_job_name, retrying

logger = logging.getLogger(__name__)

ES_DOC_TYPE = "test_stats"


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
    cmd_params = {}
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

    def get_scylladb_throughput(self, start_time, end_time):
        """
        Get Scylla throughput (ops/second) from PrometheusDB

        :return: list of tuples (unix time, op/s)
        """
        if end_time - start_time < 120:
            logger.warning("Time difference too low to make a query [start_time: %s, end_time: %s" % (start_time,
                                                                                                      end_time))
            return []
        # the query is taken from the Grafana Dashborad definition
        q = "sum(irate(scylla_transport_requests_served{}[30s]))%20%2B%20sum(irate(scylla_thrift_served{}[30s]))"
        results = self.query(query=q, start=start_time, end=end_time)
        if results:
            return results[0]["values"]
        else:
            return []


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
        self._stats = {}
        if not self._test_id:
            super(Stats, self).__init__(*args, **kwargs)

    def create(self):
        es.ES().create(self._test_index, ES_DOC_TYPE, self._test_id, self._stats)

    def update(self, data):
        """
        Update document
        :param data: data dictionary
        """
        try:
            es.ES().update(self._test_index, ES_DOC_TYPE, self._test_id, data)
        except Exception as ex:
            logger.error('Failed to update test stats: test_id: %s, error: %s', self._test_id, ex)


class TestStatsMixin(Stats):
    """
    This mixin is responsible for saving test details and statistics in database.
    """
    KEYS = ['test_details', 'setup_details', 'versions', 'results', 'nemesis', 'errors', 'coredumps']

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
        if is_gce:
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
        self._stats['results']['throughput'] = defaultdict(dict)
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

    def get_scylla_throughput(self):
        if self.monitors:
            try:
                self.log.info("Calculating throughput stats from PrometheusDB...")
                ps = PrometheusDBStats(host=self.monitors.nodes[0].public_ip_address)
                offset = 120  # 2 minutes offset
                ps_results = ps.get_scylladb_throughput(start_time=int(self._stats["test_details"]["start_time"] + offset),
                                                        end_time=int(time.time() - offset))  # op/s
                if len(ps_results) <= 3:
                    self.log.error("Not enough data from Prometheus: %s" % ps_results)
                    return
                throughput = self._stats["results"]["throughput"]
                ops_per_sec = [float(val) for _, val in ps_results]
                throughput["max"] = max(ops_per_sec)
                # filter all values that is less than 1% of max at he beginning and at the end
                ops_filtered = filter(lambda x: x >= throughput["max"] * 0.01, ops_per_sec)
                throughput["min"] = min(ops_filtered)
                throughput["avg"] = float(sum(ops_filtered)) / len(ops_filtered)
                throughput["stdev"] = stddev(ops_filtered)
                self.log.debug("Throughput stats: %s", self._stats["results"]["throughput"])
            except Exception as e:
                self.log.error("Exception when calculating PrometheusDB stats: %s" % e)
                return
        else:
            self.log.warning("Unable to get stats from Prometheus. "
                             "Probably scylla monitoring nodes were not provisioned.")

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
            self._stats['test_details']['time_completed'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            if self.monitors:
                test_start_time = self._stats['test_details']['start_time']
                self.get_scylla_throughput()
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
            update_data = {'status': self._stats['status'], 'test_details': self._stats['test_details'],
                           'results': {'throughput': self._stats['results']['throughput']}}
            if errors:
                update_data.update({'errors': errors})
            if coredumps:
                update_data.update({'coredumps': coredumps})
            self.update(update_data)

    def check_regression(self):
        ra = PerformanceResultsAnalyzer(es_index=self._test_index, es_doc_type=ES_DOC_TYPE,
                             send_email=self.params.get('send_email', default=True),
                             email_recipients=self.params.get('email_recipients', default=None))
        is_gce = True if self.params.get('cluster_backend') == 'gce' else False
        try:
            ra.check_regression(self._test_id, is_gce)
        except Exception as ex:
            logger.exception('Failed to check regression: %s', ex)
