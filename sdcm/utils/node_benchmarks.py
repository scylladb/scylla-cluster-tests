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
import logging

from sdcm.cluster import BaseNode
from sdcm.utils.metaclasses import Singleton


LOGGER = logging.getLogger(__name__)


class NodeBenchmarksManager(metaclass=Singleton):
    def __init__(self, nodes: list[BaseNode]):
        self._nodes = nodes
        self._node_runners: list[NodeBenchmarksRunner] = [
            NodeBenchmarksRunner(node) for node in self._nodes
        ]

    def install_benchmark_tools_on_nodes(self):
        """
        Install the tools needed for running the benchmark test
        on all nodes.
        """
        for runner in self._node_runners:
            runner.install_benchmark_tools()


class NodeBenchmarksRunner:
    def __init__(self, node: BaseNode):
        self._node = node
        self.remoter = node.remoter

    def install_benchmark_tools(self):
        try:
            LOGGER.info("Installing node benchmarking tools...")
            self._install_sysbench()
            self._install_fio()
            self._install_cassandra_fio()
        except Exception as exc:
            raise exc

    def _install_sysbench(self):
        sysbench_install_cmd = "sudo apt-get install -y sysbench"
        self.remoter.run(sysbench_install_cmd)

    def _install_fio(self):
        fio_install_cmd = "sudo apr-get install -y fio"
        self.remoter.run(fio_install_cmd)

    def _install_cassandra_fio(self):
        git_cmd = "git clone git@github.com:ibspoof/cassandra-fio.git"
        self.remoter.run(git_cmd)

    def run_sysbench_cpu_test(self, threads: int = 8, time: int = 120):
        sysbench_run_cmd = f"sysbench cpu --threads={threads} --time={time}"
        sysbench_result = self.remoter.run()
        LOGGER.info("Sysbnench CPU test result:\n%s\nerrors:\n%s", sysbench_result.stdout, sysbench_result.stderr)
