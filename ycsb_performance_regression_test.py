#!/usr/bin/env python

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
# Copyright (c) 2016 ScyllaDB
from pprint import pformat
from typing import Dict

from performance_regression_test import BasePerformanceRegression
from sdcm.sct_events.system import InfoEvent


class BaseYCSBPerformanceRegressionTest(BasePerformanceRegression):
    ycsb_workloads: Dict[str, str] = {
        "a": "Update Heavy (50/50 read/write ratio)",
        "b": "Read Mostly (95/5 read/write ratio)",
        "c": "Read Only (100/0 read/write ratio)",
        "d": "Read Latest (95/0/5 read/update/insert ratio)",
        "e": "Short Range (95/5 scan/insert ratio)",
        "f": "Read-Modify-Write (50/50 read/read-modify-write ratio)",
    }
    scylla_connection: int = 280  # number of connections per node
    target_size: int = 120_000  # 120K operation per seconds
    stress_cmd: str = "stress_latency_workload{}"
    records_size: int = 1_000_000

    def __init__(self, *args):
        super().__init__(*args)
        self._create_ycsb_commands()

    def _create_ycsb_commands(self):
        threads_size = self.params.get("n_db_nodes") * self.scylla_connection
        self.params["prepare_write_cmd"] = [
            f"{cmd}"
            f" -target {self.target_size}"
            f" -threads {threads_size}"
            f" -p recordcount={self.records_size}"
            f" -p insertcount={self.records_size}"
            f" -p scylla.coreconnections={self.scylla_connection}"
            f" -p scylla.maxconnections={self.scylla_connection}"
            for cmd in self.params['prepare_write_cmd']
        ]

        for workload_type in self.ycsb_workloads:
            self.params[self.stress_cmd.format(workload_type)] = \
                f"bin/ycsb run scylla -s -P workloads/workload{workload_type}" \
                f" -target {self.target_size}" \
                f" -threads {threads_size}" \
                f" -p recordcount={self.records_size}" \
                f" -p scylla.coreconnections={self.scylla_connection}" \
                f" -p scylla.maxconnections={self.scylla_connection}" \
                f" {self.params['stress_cmd_m']}"

        InfoEvent(message="All params:\n(%s)" % pformat(self.params)).publish()

    def test_latency(self):
        """
        Test steps:

        1. Prepare cluster with data (reach steady_stet of compactions and ~x10 capacity than RAM.
        with round_robin and list of stress_cmd - the data will load several times faster.
        2. Run WRITE workload with gauss population.
        """
        self.run_fstrim_on_all_db_nodes()
        self.preload_data()

        for workload_type, workload_details in self.ycsb_workloads.items():
            InfoEvent(message="Starting YCSB workload%s (%s)" % (workload_type, workload_details)).publish()
            self.run_workload(stress_cmd=self.stress_cmd.format(workload_type), sub_type=workload_details)

    def test_latency_workload_a_with_nemesis(self):
        self.preload_data()
        # self._latency_read_with_nemesis(
        #     stress_cmd=self.stress_cmd.format("a"), sub_type=self.ycsb_workloads["a"])

    def test_latency_workload_a_without_nemesis(self):
        self.preload_data()
        # self.run_workload(
        #     stress_cmd=self.stress_cmd.format("a"), sub_type=self.ycsb_workloads["a"], is_preload_data=True)

    def test_latency_workload_b_with_nemesis(self):
        self._latency_read_with_nemesis(
            stress_cmd=self.stress_cmd.format("b"), sub_type=self.ycsb_workloads["b"])

    def test_latency_workload_b_without_nemesis(self):
        self.run_workload(
            stress_cmd=self.stress_cmd.format("b"), sub_type=self.ycsb_workloads["b"], is_preload_data=True)

    def test_latency_workload_c_with_nemesis(self):
        self._latency_read_with_nemesis(
            stress_cmd=self.stress_cmd.format("c"), sub_type=self.ycsb_workloads["c"])

    def test_latency_workload_c_without_nemesis(self):
        self.run_workload(
            stress_cmd=self.stress_cmd.format("c"), sub_type=self.ycsb_workloads["c"], is_preload_data=True)

    def test_latency_workload_d_with_nemesis(self):
        self._latency_read_with_nemesis(
            stress_cmd=self.stress_cmd.format("d"), sub_type=self.ycsb_workloads["d"])

    def test_latency_workload_d_without_nemesis(self):
        self.run_workload(
            stress_cmd=self.stress_cmd.format("d"), sub_type=self.ycsb_workloads["d"], is_preload_data=True)

    def test_latency_workload_e_with_nemesis(self):
        self._latency_read_with_nemesis(
            stress_cmd=self.stress_cmd.format("e"), sub_type=self.ycsb_workloads["e"])

    def test_latency_workload_e_without_nemesis(self):
        self.run_workload(
            stress_cmd=self.stress_cmd.format("e"), sub_type=self.ycsb_workloads["e"], is_preload_data=True)

    def test_latency_workload_f_with_nemesis(self):
        self._latency_read_with_nemesis(
            stress_cmd=self.stress_cmd.format("f"), sub_type=self.ycsb_workloads["f"])

    def test_latency_workload_f_without_nemesis(self):
        self.run_workload(
            stress_cmd=self.stress_cmd.format("f"), sub_type=self.ycsb_workloads["f"], is_preload_data=True)


class YCSBPerformanceRegression1MRecordsTest(BaseYCSBPerformanceRegressionTest):
    records_size: int = 10 ** 6  # create database with 1M records


class YCSBPerformanceRegression10MRecordsTest(BaseYCSBPerformanceRegressionTest):
    records_size: int = 10 ** 7  # create database with 10M records


class YCSBPerformanceRegression100MRecordsTest(BaseYCSBPerformanceRegressionTest):
    records_size: int = 10 ** 8  # create database with 100M records


class YCSBPerformanceRegression1BRecordsTest(BaseYCSBPerformanceRegressionTest):
    records_size: int = 10 ** 9  # create database with 1 billion records
