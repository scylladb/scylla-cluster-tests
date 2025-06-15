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
# Copyright (c) 2022 ScyllaDB

import logging
import time
from dataclasses import dataclass

from performance_regression_test import PerformanceRegressionTest
from sdcm.sct_events.system import InfoEvent


LOGGER = logging.getLogger(__name__)


@dataclass
class YcsbWorkload:
    name: str
    detailed_name: str
    sub_type: str


class BaseYCSBPerformanceRegressionTest(PerformanceRegressionTest):
    ycsb_workloads: list[YcsbWorkload] = [
        YcsbWorkload(name="workloada", detailed_name="Update Heavy (50/50 read/write ratio)",
                     sub_type="mixed"),
        YcsbWorkload(name="workloadb", detailed_name="Read Mostly (95/5 read/write ratio)",
                     sub_type="mostly_read"),
        YcsbWorkload(name="workloadc", detailed_name="Read Only (100/0 read/write ratio)",
                     sub_type="read"),
        YcsbWorkload(name="workloadd", detailed_name="Read Latest (95/0/5 read/update/insert ratio)",
                     sub_type="read_latest"),
        YcsbWorkload(name="workloade", detailed_name="Short Range (95/5 scan/insert ratio)",
                     sub_type="short_range"),
        YcsbWorkload(name="workloadf", detailed_name="Read-Modify-Write (50/50 read/read-modify-write ratio)",
                     sub_type="read_write_modify"),
    ]
    records_size: int = 1_000_000

    def setUp(self):
        super().setUp()
        self._create_prepare_cmds(self.ycsb_workloads[0])

    def _create_prepare_cmds(self, workload: YcsbWorkload):
        self.params['prepare_write_cmd'] = [
            "bin/ycsb -jvm-args='-Dorg.slf4j.simpleLogger.defaultLogLevel=OFF' load scylla -s"
            f" -P workloads/{workload.name}"
            f" -p recordcount={self.records_size}"
            f" -p insertcount={self.records_size}"
            f" {cmd}"
            for cmd in self.params['prepare_write_cmd']
        ]

    def _create_stress_cmd(self, workload: YcsbWorkload):
        cmd = f"bin/ycsb -jvm-args='-Dorg.slf4j.simpleLogger.defaultLogLevel=OFF' run scylla -s -P workloads/{workload.name}" \
            f" -p recordcount={self.records_size}" \
            f" -p operationcount={self.records_size}" \
            f" {self.params['stress_cmd']}"
        return cmd

    def run_pre_create_keyspace(self):
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            for cmd in self.params.get('pre_create_keyspace'):
                session.execute(cmd)

    def run_workload(self, stress_cmd, nemesis=False, sub_type=None):
        self.create_test_stats(sub_type=sub_type, doc_id_with_timestamp=True)
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stats_aggregate_cmds=False)
        if nemesis:
            interval = self.params.get('nemesis_interval')
            time.sleep(interval * 60)  # Sleeping one interval (in minutes) before starting the nemesis
            self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(), tester_obj=self)
            self.db_cluster.start_nemesis(interval=interval)
        results = self.get_stress_results(queue=stress_queue)
        LOGGER.info("Stress results: %s", str(results))
        self.update_test_details(scrap_metrics_step=60, scylla_conf=True)
        if not nemesis:
            self.check_regression()
        else:
            self.check_latency_during_ops(hdr_tags=stress_queue.hdr_tags)

    def test_latency(self):
        """
        Test steps:

        1. Prepare cluster with data (reach steady_stet of compactions and ~x10 capacity than RAM.
        with round_robin and list of stress_cmd - the data will load several times faster.
        2. Run WRITE workload with gauss population.
        """
        self.run_pre_create_keyspace()
        self.run_fstrim_on_all_db_nodes()
        self.preload_data()

        for workload in self.ycsb_workloads:
            self.wait_no_compactions_running()
            self.run_fstrim_on_all_db_nodes()
            InfoEvent(message="Starting YCSB %s (%s)" % (workload.name, workload.detailed_name)).publish()
            self.run_workload(stress_cmd=self._create_stress_cmd(workload), sub_type=workload.sub_type)


class YCSBPerformanceRegression1MRecordsTest(BaseYCSBPerformanceRegressionTest):
    records_size: int = 10 ** 6  # create database with 1M records


class YCSBPerformanceRegression10MRecordsTest(BaseYCSBPerformanceRegressionTest):
    records_size: int = 10 ** 7  # create database with 10M records


class YCSBPerformanceRegression100MRecordsTest(BaseYCSBPerformanceRegressionTest):
    records_size: int = 10 ** 8  # create database with 100M records


class YCSBPerformanceRegression1BRecordsTest(BaseYCSBPerformanceRegressionTest):
    records_size: int = 10 ** 9  # create database with 1 billion records
