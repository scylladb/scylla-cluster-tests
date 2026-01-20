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

import re
import time
import logging
import json
import urllib.parse

from textwrap import dedent
from math import sqrt
from collections import defaultdict

import yaml
import requests

from sdcm.utils.common import normalize_ipv6_url
from sdcm.utils.decorators import retrying

LOGGER = logging.getLogger(__name__)

FS_SIZE_METRIC = "node_filesystem_size_bytes"
FS_SIZE_METRIC_OLD = "node_filesystem_size"
AVAIL_SIZE_METRIC = "node_filesystem_avail_bytes"
AVAIL_SIZE_METRIC_OLD = "node_filesystem_avail"
KB_SIZE = 2**10
MB_SIZE = KB_SIZE * 1024
GB_SIZE = MB_SIZE * 1024
SCYLLA_DIR = "/var/lib/scylla"


class CassandraStressCmdParseError(Exception):
    def __init__(self, cmd, ex):
        super().__init__()
        self.command = cmd
        self.exception = repr(ex)

    def __str__(self):
        return dedent(
            """
            Stress command: '{0.command}'
            Error: {0.exception}""".format(self)
        )

    def __repr__(self):
        return self.__str__()


def stddev(lst):
    mean = float(sum(lst)) / len(lst)
    return sqrt(sum((x - mean) ** 2 for x in lst) / len(lst))


def get_stress_cmd_params(cmd):
    """
    Parsing cassandra stress command
    :param cmd: stress cmd
    :return: dict with params
    """

    cmd_params = {"raw_cmd": cmd}
    try:
        cmd = cmd.strip().split("cassandra-stress")[1].strip()
        if cmd.split(" ")[0] in ["read", "write", "mixed", "counter_write", "user"]:
            cmd_params["command"] = cmd.split(" ")[0]
            if "no-warmup" in cmd:
                cmd_params["no-warmup"] = True

            match = re.search(r"(cl\s?=\s?\w+)", cmd)
            if match:
                cmd_params["cl"] = match.group(0).split("=")[1].strip()

            match = re.search(r"(duration\s?=\s?\w+)", cmd)
            if match:
                cmd_params["duration"] = match.group(0).split("=")[1].strip()

            match = re.search(r"( n\s?=\s?\w+)", cmd)
            if match:
                cmd_params["n"] = match.group(0).split("=")[1].strip()
            match = re.search(r"profile=(\S+)\s+", cmd)
            if match:
                cmd_params["profile"] = match.group(1).strip()
                match = re.search(r"ops(\S+)\s+", cmd)
                if match:
                    cmd_params["ops"] = match.group(1).split("=")[0].strip("(")

            for temp in cmd.split(" -")[1:]:
                try:
                    key, value = temp.split(" ", 1)
                except ValueError as ex:
                    LOGGER.warning("%s:%s", temp, ex)
                else:
                    cmd_params[key] = value.strip().replace("'", "")
            if "rate" in cmd_params:
                # split rate section on separate items
                if "threads" in cmd_params["rate"]:
                    cmd_params["rate threads"] = re.search(r"(threads\s?=\s?(\w+))", cmd_params["rate"]).group(2)
                if "throttle" in cmd_params["rate"]:
                    cmd_params["throttle threads"] = re.search(r"(throttle\s?=\s?(\w+))", cmd_params["rate"]).group(2)
                if "fixed" in cmd_params["rate"]:
                    cmd_params["fixed threads"] = re.search(r"(fixed\s?=\s?(\w+))", cmd_params["rate"]).group(2)
                del cmd_params["rate"]

        return cmd_params
    except Exception as ex:  # noqa: BLE001
        raise CassandraStressCmdParseError(cmd=cmd, ex=ex) from None


def get_stress_bench_cmd_params(cmd):
    """
    Parsing bench stress command
    :param cmd: stress cmd
    :return: dict with params
    """
    cmd = cmd.strip().split("scylla-bench")[1].strip()
    cmd_params = {}
    for key in [
        "partition-count",
        "clustering-row-count",
        "clustering-row-size",
        "mode",
        "workload",
        "concurrency",
        "max-rate",
        "connection-count",
        "replication-factor",
        "timeout",
        "client-compression",
        "duration",
    ]:
        match = re.search(r"(-" + key + r"\s+([^-| ]+))", cmd)
        if match:
            cmd_params[key] = match.group(2).strip()
    return cmd_params


def get_stress_harry_cmd_params(cmd):
    """
    Parsing cassandra-harry stress command
    :param cmd: stress cmd
    :return: dict with params
    """
    cmd = cmd.split("cassandra-harry")[1].strip()
    cmd_params = {}
    for key in ["run-time", "run-time-unit"]:
        match = re.search(rf"(-{key}\s+([^-| ]+))", cmd)
        if match:
            cmd_params[key] = match.group(2).strip()
    return cmd_params


def get_ycsb_cmd_params(cmd):
    """
    Parsing ycsb command
    :param cmd: stress cmd
    :return: dict with params
    """

    cmd_params = {"raw_cmd": cmd}

    threads_regex = re.compile(r"-threads\s*(.*?)[\s$]")
    key_value_regex = re.compile(r"-p\s.*?(?P<key>.*?)=(?P<value>.*?)(\s|$)")
    for match in key_value_regex.finditer(cmd):
        match_dict = match.groupdict()
        cmd_params[match_dict["key"]] = match_dict["value"]
    match = threads_regex.search(cmd)
    if match:
        cmd_params["threads"] = match.group(1)
    return cmd_params


def get_raw_cmd_params(cmd):
    cmd = cmd.strip()
    cmd_params = {"raw_cmd": cmd}
    return cmd_params


def get_gemini_cmd_params(cmd):
    return get_raw_cmd_params(cmd)


def get_cdcreader_cmd_params(cmd):
    return get_raw_cmd_params(cmd)


class PrometheusDBStats:
    def __init__(self, host, port=9090, protocol="http", alternator=None):
        self.host = host
        self.port = port
        self.protocol = protocol
        self.range_query_url = "{}://{}:{}/api/v1/query_range?query=".format(protocol, normalize_ipv6_url(host), port)
        self.config = self.get_configuration()
        self.alternator = alternator

    @property
    def scylla_scrape_interval(self):
        return int(self.config["scrape_configs"]["scylla"]["scrape_interval"][:-1])

    @retrying(n=5, sleep_time=7, allowed_exceptions=(requests.ConnectionError, requests.HTTPError))
    def request(self, url, post=False):
        kwargs = {}
        if self.protocol == "https":
            kwargs["verify"] = False
        if post:
            response = requests.post(url, **kwargs)
        else:
            response = requests.get(url, **kwargs)
        response.raise_for_status()

        result = json.loads(response.content)
        LOGGER.debug("Response from Prometheus server: %s", str(result)[:200])
        if result["status"] == "success":
            return result
        else:
            LOGGER.error("Prometheus returned error: %s", result)
        return None

    def get_configuration(self):
        result = self.request(
            url="{}://{}:{}/api/v1/status/config".format(self.protocol, normalize_ipv6_url(self.host), self.port)
        )
        configs = yaml.safe_load(result["data"]["yaml"])
        LOGGER.debug("Parsed Prometheus configs: %s", configs)
        new_scrape_configs = {}
        for conf in configs["scrape_configs"]:
            new_scrape_configs[conf["job_name"]] = conf
        configs["scrape_configs"] = new_scrape_configs
        return configs

    def query(self, query, start, end, scrap_metrics_step=None):
        """
        :param start: time=<rfc3339 | unix_timestamp>: Start timestamp.
        :param end: time=<rfc3339 | unix_timestamp>: End timestamp.
        :param scrap_metrics_step: is the granularity of data requested from Prometheus DB
        :param query:
        :return: {
                  metric: { },
                  values: [[linux_timestamp1, value1], [linux_timestamp2, value2]...[linux_timestampN, valueN]]
                 }
        """
        url = "{}://{}:{}/api/v1/query_range?query=".format(self.protocol, normalize_ipv6_url(self.host), self.port)
        if not scrap_metrics_step:
            scrap_metrics_step = self.scylla_scrape_interval
        _query = "{url}{query}&start={start}&end={end}&step={scrap_metrics_step}".format(
            url=url, query=query, start=start, end=end, scrap_metrics_step=scrap_metrics_step
        )
        LOGGER.debug("Query to PrometheusDB: %s", _query)
        result = self.request(url=_query)
        if result:
            return result["data"]["result"]
        else:
            LOGGER.error("Prometheus query unsuccessful!")
            return []

    @staticmethod
    def _check_start_end_time(start_time, end_time):
        if end_time - start_time < 120:
            LOGGER.warning(
                "Time difference too low to make a query [start_time: %s, end_time: %s", start_time, end_time
            )
            return False
        return True

    def _get_query_values(self, query, start_time, end_time, scrap_metrics_step=None):
        results = self.query(query=query, start=start_time, end=end_time, scrap_metrics_step=scrap_metrics_step)
        if results:
            return results[0]["values"]
        else:
            return []

    def get_throughput(self, start_time, end_time, scrap_metrics_step=None):
        """
        Get Scylla throughput (ops/second) from PrometheusDB

        :return: list of tuples (unix time, op/s)
        """
        if not self._check_start_end_time(start_time, end_time):
            return []
        # the query is taken from the Grafana Dashborad definition
        if self.alternator:
            query = "sum(irate(scylla_alternator_operation{}[30s]))"
        else:
            query = "sum(irate(scylla_transport_requests_served{}[30s]))"
        return self._get_query_values(query, start_time, end_time, scrap_metrics_step=scrap_metrics_step)

    def get_scylla_reactor_utilization(self, start_time, end_time, scrap_metrics_step=None, instance=None):
        """
        Get Scylla CPU (avg) from PrometheusDB

        :return: list of tuples (unix time, op/s)
        """
        if not self._check_start_end_time(start_time, end_time):
            return []

        instance_filter = f'instance="{instance}"' if instance else ""
        query = "avg(scylla_reactor_utilization{%s})" % instance_filter
        res = self._get_query_values(query, start_time, end_time, scrap_metrics_step=scrap_metrics_step)
        if res:
            res = [float(value[1]) for value in res]
            return sum(res) / len(res)
        else:
            return res

    def get_scylla_io_queue_total_operations(self, start_time, end_time, node_ip, irate_sample_sec="30s"):
        """
        Get Scylla CPU scheduler runtime from PrometheusDB

        :return: list of tuples (unix time, op/s)
        """
        if not self._check_start_end_time(start_time, end_time):
            return {}
        # the query is taken from the Grafana Dashborad definition
        query = f'avg(irate(scylla_io_queue_total_operations{{class=~"sl:.*", instance="{node_ip}"}}  [{irate_sample_sec}] )) by (class, instance)'
        results = self.query(query=query, start=start_time, end=end_time)
        # result example:
        #  {'status': 'success', 'data': {'resultType': 'matrix', 'result': [{'metric': {'class': 'sl:default', 'instance': '10.4.3.205'}, 'values': [[1731495365.897, '1000'], [1731495385.897, '1000'], .....
        res = defaultdict(dict)
        for item in results:
            res[item["metric"]["instance"]].update(
                {item["metric"]["class"]: [float(runtime[1]) for runtime in item["values"]]}
            )
        return res

    def get_scylla_storage_proxy_replica_cross_shard_ops(self, start_time, end_time):
        query = """sum(irate(scylla_storage_proxy_replica_cross_shard_ops{instance=~".+[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.[0-9]{1,3}.+",shard=~"[0-9]+"}[1m])) by (dc)"""
        query = urllib.parse.quote(query)
        results = self.query(query, start_time, end_time)
        cross_shards_ops_per_node_shard_by_dc = []
        for res in results:
            cross_shards_ops_per_node_shard_by_dc.extend([float(v) for _, v in res["values"]])

        return cross_shards_ops_per_node_shard_by_dc

    def get_latency(self, start_time, end_time, latency_type, scrap_metrics_step=None):
        """latency values are returned in microseconds"""
        assert latency_type in ["read", "write"]
        if not self._check_start_end_time(start_time, end_time):
            return []
        query = (
            "histogram_quantile(0.99, sum(rate(scylla_storage_proxy_"
            "coordinator_%s_latency_bucket{}[30s])) by (le))" % latency_type
        )
        return self._get_query_values(query, start_time, end_time, scrap_metrics_step=scrap_metrics_step)

    def get_latency_read_99(self, start_time, end_time, scrap_metrics_step=None):
        return self.get_latency(start_time, end_time, latency_type="read", scrap_metrics_step=scrap_metrics_step)

    def get_latency_write_99(self, start_time, end_time, scrap_metrics_step=None):
        return self.get_latency(start_time, end_time, latency_type="write", scrap_metrics_step=scrap_metrics_step)

    def create_snapshot(self):
        url = "http://{}:{}/api/v1/admin/tsdb/snapshot".format(normalize_ipv6_url(self.host), self.port)
        result = self.request(url, True)
        LOGGER.debug("Request result: {}".format(result))
        return result

    @staticmethod
    def generate_node_capacity_query_postfix(node):
        return f'{{mountpoint="{SCYLLA_DIR}", instance=~".*?{node.private_ip_address}.*?"}}'

    def get_used_capacity_gb(self, node):
        #  example: node_filesystem_size_bytes{mountpoint="/var/lib/scylla",
        #  instance=~".*?10.0.79.46.*?"}-node_filesystem_avail_bytes{mountpoint="/var/lib/scylla",
        #  instance=~".*?10.0.79.46.*?"}
        node_capacity_query_postfix = self.generate_node_capacity_query_postfix(node)
        filesystem_capacity_query = f"{FS_SIZE_METRIC}{node_capacity_query_postfix}"
        used_capacity_query = f"{filesystem_capacity_query}-{AVAIL_SIZE_METRIC}{node_capacity_query_postfix}"
        LOGGER.debug("filesystem_capacity_query: %s", filesystem_capacity_query)

        fs_size_res = self.query(query=filesystem_capacity_query, start=int(time.time()) - 5, end=int(time.time()))
        if not fs_size_res:
            LOGGER.warning("No results from Prometheus query: %s", filesystem_capacity_query)
            return 0
            # assert fs_size_res, "No results from Prometheus"
        if not fs_size_res[0]:  # if no returned values - try the old metric names.
            filesystem_capacity_query = f"{FS_SIZE_METRIC_OLD}{node_capacity_query_postfix}"
            used_capacity_query = f"{filesystem_capacity_query}-{AVAIL_SIZE_METRIC_OLD}{node_capacity_query_postfix}"
            LOGGER.debug("filesystem_capacity_query: %s", filesystem_capacity_query)
            fs_size_res = self.query(query=filesystem_capacity_query, start=int(time.time()) - 5, end=int(time.time()))

        assert fs_size_res[0], "Could not resolve capacity query result."
        LOGGER.debug("used_capacity_query: %s", used_capacity_query)
        used_cap_res = self.query(query=used_capacity_query, start=int(time.time()) - 5, end=int(time.time()))
        assert used_cap_res, "No results from Prometheus"
        used_size_mb = float(used_cap_res[0]["values"][0][1]) / float(MB_SIZE)
        used_size_gb = round(float(used_size_mb / 1024), 2)
        LOGGER.debug(
            "The used filesystem capacity on node %s is: %s MB/ %s GB",
            node.public_ip_address,
            used_size_mb,
            used_size_gb,
        )
        return used_size_gb

    def get_filesystem_total_size_gb(self, node) -> float:
        """
        :param node:
        :return: FS size in GB
        """
        node_capacity_query_postfix = self.generate_node_capacity_query_postfix(node)
        filesystem_capacity_query = f"{FS_SIZE_METRIC}{node_capacity_query_postfix}"
        fs_size_res = self.query(query=filesystem_capacity_query, start=int(time.time()) - 5, end=int(time.time()))
        assert fs_size_res, "No results from Prometheus"
        if not fs_size_res[0]:  # if no returned values - try the old metric names.
            filesystem_capacity_query = f"{FS_SIZE_METRIC_OLD}{node_capacity_query_postfix}"
            LOGGER.debug("filesystem_capacity_query: %s", filesystem_capacity_query)
            fs_size_res = self.query(query=filesystem_capacity_query, start=int(time.time()) - 5, end=int(time.time()))
        assert fs_size_res[0], "Could not resolve capacity query result."
        LOGGER.debug("fs_size_res: %s", fs_size_res)
        fs_size_gb = float(fs_size_res[0]["values"][0][1]) / float(GB_SIZE)
        return fs_size_gb
