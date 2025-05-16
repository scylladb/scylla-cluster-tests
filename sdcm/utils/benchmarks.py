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
# Copyright (c) 2021 ScyllaDB
import json
import logging
import re
from dataclasses import dataclass, field, asdict
from typing import NamedTuple

from sdcm.es import ES
from sdcm.remote import RemoteCmdRunnerBase, shell_script_cmd
from sdcm.test_config import TestConfig
from sdcm.utils.common import ParallelObject
from sdcm.utils.git import clone_repo
from sdcm.utils.metaclasses import Singleton
from sdcm.utils.decorators import retrying

LOGGER = logging.getLogger(__name__)
ES_INDEX = "node_benchmarks"


class ComparableResult(NamedTuple):
    sysbench_eps: float = 0.0
    cassandra_fio_read_bw: float = 0.0
    cassandra_fio_write_bw: float = 0.0

    def __getitem__(self, item):
        return self.__getattribute__(item)


class Margins(ComparableResult):
    ...


class Averages(ComparableResult):
    ...


@dataclass
class ScyllaNodeBenchmarkSysbenchResult:
    cmd_output: str = field(repr=False)
    sysbench_events_per_second: float = field(init=False)
    sysbench_latency_min: float = field(init=False)
    sysbench_latency_max: float = field(init=False)
    sysbench_latency_avg: float = field(init=False)
    sysbench_latency_p95: float = field(init=False)
    sysbench_thread_fairness_avg: float = field(init=False)
    sysbench_thread_fairness_stdev: float = field(init=False)
    sysbench_thread_fairness_time_avg: float = field(init=False)
    sysbench_thread_fairness_time_stdev: float = field(init=False)

    def __post_init__(self):
        eps_regex = re.compile(
            r"events per second:\s+(?P<eps>[\d.]+)([\s\w\d.:/()]+)"
            r"min:\s+(?P<latency_min>[\d.]+)([\s\w\d.:/()]+)"
            r"avg:\s+(?P<latency_avg>[\d.]+)([\s\w\d.:/()]+)"
            r"max:\s+(?P<latency_max>[\d.]+)([\s\w\d.:/()]+)"
            r"95th percentile:\s+(?P<latency_p95>[\d.]+)([\s\w\d.:/()]+)"
            r"events\s+\(avg/stddev\):\s+(?P<thread_avg>[\d.]+)/(?P<thread_stdev>[\d.]+)([\s\w\d.:/()]+)"
            r"execution time \(avg/stddev\):\s+(?P<time_avg>[\d.]+)/(?P<time_stdev>[\d.]+)"
        )
        search_result = eps_regex.search(self.cmd_output)
        self.sysbench_events_per_second = float(search_result.groupdict()["eps"])
        self.sysbench_latency_min = float(search_result.groupdict()["latency_min"])
        self.sysbench_latency_max = float(search_result.groupdict()["latency_max"])
        self.sysbench_latency_avg = float(search_result.groupdict()["latency_avg"])
        self.sysbench_latency_p95 = float(search_result.groupdict()["latency_p95"])
        self.sysbench_thread_fairness_avg = float(search_result.groupdict()["thread_avg"])
        self.sysbench_thread_fairness_stdev = float(search_result.groupdict()["thread_stdev"])
        self.sysbench_thread_fairness_time_avg = float(search_result.groupdict()["time_avg"])
        self.sysbench_thread_fairness_time_stdev = float(search_result.groupdict()["time_stdev"])


class ScyllaClusterBenchmarkManager(metaclass=Singleton):
    """
    ScyllaClusterBenchmarkManager gathers the benchmark results
    of all the relevant db nodes in the cluster and presents
    them in unified fashion.
    ElasticSearch is used to store the results.
    """

    def __init__(self, global_compare: bool = False):
        self._nodes: list["BaseNode"] = []  # noqa: F821
        self._benchmark_runners: list[ScyllaNodeBenchmarkRunner] = []
        self._es = ES()
        self._comparison = {}
        self._global_compare = global_compare
        self._test_id = TestConfig().test_id()

    @property
    def comparison(self):
        return self._comparison

    def add_node(self, new_node: "BaseNode"):  # noqa: F821
        if new_node.distro.is_debian_like:
            self._benchmark_runners.append(ScyllaNodeBenchmarkRunner(new_node))
        else:
            LOGGER.debug("Skipped installing benchmarking tools on a non-debian-like distro.")

    def add_nodes(self, nodes: list["BaseNode"]):  # noqa: F821
        for node in nodes:
            self.add_node(node)

    def install_benchmark_tools(self):
        try:
            parallel = ParallelObject(self._benchmark_runners, timeout=600)
            parallel.run(lambda x: x.install_benchmark_tools(), ignore_exceptions=False)
        except TimeoutError as exc:
            LOGGER.warning("Ran into TimeoutError while installing benchmark tools: Exception:\n%s", exc)

    def run_benchmarks(self):
        try:
            parallel = ParallelObject(self._benchmark_runners, timeout=1200)
            parallel.run(lambda x: x.run_benchmarks(), ignore_exceptions=False)
        except TimeoutError as exc:
            LOGGER.warning("Run into TimeoutError during running benchmarks. Exception:\n%s", exc)
        self._collect_benchmark_output()
        self._compare_results()

    def _collect_benchmark_output(self):
        """
        Collect the results from ScyllaClusterBenchmarkRunner
        instances and post them to Elasticsearch.
        """

        for runner in self._benchmark_runners:
            if runner.benchmark_results:
                results = {
                    "test_id": self._test_id,
                    "node_instance_type": runner.node_instance_type,
                    "node_name": runner.node_name,
                    **runner.benchmark_results
                }
                doc_id = f"{self._test_id}-{runner.node_name.split('-')[-1]}"
                self._es.create_doc(index=ES_INDEX, doc_type=None, doc_id=doc_id, body=results)
            else:
                LOGGER.debug("No benchmarks results for node: %s", runner.node_name)

    def _get_benchmark_results(self, instance_type: str = "") -> list[dict]:
        filter_path = ['hits.hits._id',
                       'hits.hits._source.test_id',
                       'hits.hits._source.node_instance_type',
                       'hits.hits._source',
                       ]
        query = f"test_id: \"{self._test_id}\""

        if self._global_compare:
            query = f"NOT {query}"

        if instance_type:
            query += f" AND node_instance_type: \"{instance_type}\""

        LOGGER.debug("QUERY ES: %s", query)

        @retrying(n=3, sleep_time=1, message="Get hw performance nodes result...", raise_on_exceeded=False)
        def search_results():
            results = self._es.search(index=ES_INDEX,
                                      q=query,
                                      filter_path=filter_path,
                                      size=1000)

            LOGGER.debug("Found results: %s", results)
            if not results:
                raise Exception("No results found")

            return [doc["_source"] for doc in results["hits"]["hits"]]

        result = search_results()
        return result or []

    def _compare_results(self):
        if not self._benchmark_runners:
            return

        # assume that cluster nodes have same instance type
        instance_type = self._benchmark_runners[0].node_instance_type
        averages = self._get_average_results(es_docs=self._get_benchmark_results(instance_type))

        for runner in self._benchmark_runners:
            if not runner.benchmark_results:
                continue
            try:
                result = ComparableResult(
                    sysbench_eps=runner.benchmark_results["sysbench_events_per_second"],
                    cassandra_fio_read_bw=runner.benchmark_results["cassandra_fio_lcs_64k_read"]["read"]["bw"],
                    cassandra_fio_write_bw=runner.benchmark_results["cassandra_fio_lcs_64k_write"]["write"]["bw"]
                )
                self._comparison.update(
                    self._check_results(node_name=runner.node_name,
                                        averages=averages,
                                        result=result,
                                        margins=Margins(sysbench_eps=0.03,
                                                        cassandra_fio_read_bw=0.01,
                                                        cassandra_fio_write_bw=0.01)))
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning(
                    "Failed to generate comparable result for the following item:\n%s"
                    "\nException:%s", runner.benchmark_results, exc)
                continue

    @staticmethod
    def _check_results(node_name: str, averages: Averages, result: ComparableResult, margins: Margins) -> dict:
        results = {node_name: {}}

        for item in result._fields:
            avg_ratio = result[item] / averages[item] if averages[item] > 0 else 1.0
            results[node_name][item] = {
                "value": result[item],
                "average": averages[item],
                "average_ratio": avg_ratio,
                "is_within_margin": avg_ratio > (1 - margins[item])
            }
        return results

    @staticmethod
    def _get_average_results(es_docs: list):
        results = []
        if not es_docs:
            LOGGER.warning("Results were not found for averages calculation")
            return Averages()

        for item in es_docs:
            try:
                results.append(ComparableResult(
                    sysbench_eps=item["sysbench_events_per_second"],
                    cassandra_fio_read_bw=item["cassandra_fio_lcs_64k_read"]["read"]["bw"],
                    cassandra_fio_write_bw=item["cassandra_fio_lcs_64k_write"]["write"]["bw"]
                ))
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning(
                    "Failed to generate comparable result for the following item:\n%s"
                    "\nException:%s", item, exc)
        eps = [item.sysbench_eps for item in results] or [0.0]
        read_bw = [item.cassandra_fio_read_bw for item in results] or [0.0]
        write_bw = [item.cassandra_fio_write_bw for item in results] or [0.0]

        return Averages(sysbench_eps=sum(eps) / len(eps),
                        cassandra_fio_read_bw=sum(read_bw) / len(read_bw),
                        cassandra_fio_write_bw=sum(write_bw) / len(write_bw))


class ScyllaNodeBenchmarkRunner:

    """
    ScyllaNodeBenchmarkRunner installs and runs benchmarking
    tools on given cluster nodes and collects the output.
    """

    def __init__(self, node: "BaseNode"):  # noqa: F821
        self._node = node
        self._remoter: RemoteCmdRunnerBase = node.remoter
        self.node_instance_type = self._get_db_node_instance_type()
        self._benchmark_results = {}

    @property
    def node_name(self):
        return self._node.name

    @property
    def benchmark_results(self):
        return self._benchmark_results

    def install_benchmark_tools(self):
        self._node.install_package("git")
        clone_repo(self._remoter, "https://github.com/akopytov/sysbench.git")
        # upstream repo: https://github.com/ibspoof/cassandra-fio
        clone_repo(self._remoter, "https://github.com/KnifeyMoloko/cassandra-fio.git")
        self._install_ubuntu_prerequisites()
        self._build_and_install_sysbench()

    def _get_db_node_instance_type(self) -> str:
        backend = self._node.parent_cluster.params.get("cluster_backend")
        if backend in ("aws", "aws-siren"):
            return self._node.parent_cluster.params.get("instance_type_db")
        elif backend in ("gce", "gce-siren"):
            return self._node.parent_cluster.params.get("gce_instance_type_db")
        else:
            LOGGER.warning("Unrecognized backend type, defaulting to 'Unknown' for"
                           "db instance type.")
            return ""

    def _install_ubuntu_prerequisites(self):
        package_list = ["make", "automake", "libtool", "pkg-config", "libaio-dev", "fio"]
        try:
            LOGGER.info("Installing Ubuntu prerequisites for the node benchmarks...")
            for pkg in package_list:
                self._node.install_package(pkg)
            LOGGER.info("Ubuntu prerequisites for the node benchmarks installed.")
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to install Ubuntu prerequisites for the node benchmarking tools. "
                           "Exception:\n%s", exc)

    def _build_and_install_sysbench(self):
        build_and_install_script = """\
            cd ./sysbench
            ./autogen.sh
            ./configure --without-mysql
            make -j
            make install
        """
        self._remoter.sudo(shell_script_cmd(build_and_install_script), ignore_status=True)

    def run_sysbench(self, thread_num: int = None, test_time: int = 120):
        thread_num = thread_num or self._node.cpu_cores
        run_cmd = f"sysbench cpu --threads={thread_num} --time={test_time} run"
        sysbench_run_result = self._remoter.run(run_cmd, ignore_status=True)
        results_dict = asdict(ScyllaNodeBenchmarkSysbenchResult(sysbench_run_result.stdout))
        results_dict.pop("cmd_output")
        self.benchmark_results.update(results_dict)

    def run_fio(self):
        run_cmd = """\
            cd cassandra-fio
            ./fio_runner.sh lcs
        """
        self._remoter.sudo(shell_script_cmd(run_cmd), new_session=True, ignore_status=True)
        self._get_fio_results()

    def _get_fio_results(self):
        fio_reports_path = "/home/ubuntu/cassandra-fio/reports/"
        cat_cmd = f"cat {fio_reports_path}{'lcs.64k.fio.json'}"
        try:
            cat_out = self._remoter.run(cat_cmd, ignore_status=True)
            jsoned_output = {f"cassandra_fio_{key}": value for key, value in json.loads(cat_out.stdout).items()}

            if cat_out.stderr:
                LOGGER.info("Cat error out: %s", cat_out.stderr)

            cassandra_fio_jobs = jsoned_output.pop("cassandra_fio_jobs")
            cassandra_fio_jobs = [job for job in cassandra_fio_jobs if "setup" not in job["jobname"]]

            for job in cassandra_fio_jobs:
                jsoned_output.update({f'cassandra_fio_{job["jobname"]}': job})
            self.benchmark_results.update(jsoned_output)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to get cassandra-fio result for node %s with exception:\n%s", self.node_name, exc)

    def run_benchmarks(self):
        self.run_sysbench()
        self.run_fio()
