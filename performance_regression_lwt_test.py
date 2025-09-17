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

import pprint

from invoke.exceptions import UnexpectedExit, Failure

from performance_regression_test import PerformanceRegressionTest
from sdcm.utils.decorators import log_run_info, retrying
from sdcm.sct_events.group_common_events import ignore_operation_errors


PP = pprint.PrettyPrinter(indent=2)


class PerformanceRegressionLWTTest(PerformanceRegressionTest):
    latency_report_metrics = [
        "metrics.cs_metrics.latency_99",
        "metrics.cs_metrics.latency_mean",
    ]

    throughput_report_metrics = [
        "metrics.cs_metrics.throughput",
    ]

    lwt_subtests = [
        {
            "name": "INSERT baseline",
            "sct_param": "stress_cmd_lwt_i",
            "subtest_name": "lwt-insert-std",
            "groups": ["INSERT"],
        },
        {
            "name": "UPDATE baseline",
            "sct_param": "stress_cmd_lwt_u",
            "subtest_name": "lwt-update-std",
            "groups": ["UPDATE"],
        },
        {
            "name": "DELETE baseline",
            "sct_param": "stress_cmd_lwt_d",
            "subtest_name": "lwt-delete-std",
            "groups": ["DELETE"],
        },
        {
            "name": "INSERT IF NOT EXISTS",
            "sct_param": "stress_cmd_lwt_ine",
            "subtest_name": "lwt-insert-not-exists",
            "baseline": "lwt-insert-std",
            "groups": ["INSERT"],
        },
        {
            "name": "UPDATE IF <cond>",
            "sct_param": "stress_cmd_lwt_uc",
            "subtest_name": "lwt-update-conditional",
            "baseline": "lwt-update-std",
            "groups": ["UPDATE"],
        },
        {
            "name": "UPDATE IF EXISTS",
            "sct_param": "stress_cmd_lwt_ue",
            "subtest_name": "lwt-update-exists",
            "baseline": "lwt-update-std",
            "groups": ["UPDATE"],
        },
        {
            "name": "DELETE IF <cond>",
            "sct_param": "stress_cmd_lwt_dc",
            "subtest_name": "lwt-delete-conditional",
            "baseline": "lwt-delete-std",
            "groups": ["DELETE"],
        },
        {
            "name": "DELETE IF EXISTS",
            "sct_param": "stress_cmd_lwt_de",
            "subtest_name": "lwt-delete-exists",
            "baseline": "lwt-delete-std",
            "groups": ["DELETE"],
        },
        {
            "name": "MIXED WORKLOAD",
            "sct_param": "stress_cmd_lwt_mixed",
            "subtest_name": "lwt-mixed",
            "baseline": "lwt-mixed-baseline",
            "groups": ["MIXED"],
        },
        {
            "name": "MIXED baseline",
            "sct_param": "stress_cmd_lwt_mixed_baseline",
            "subtest_name": "lwt-mixed-baseline",
            "groups": ["MIXED"],
        },
    ]

    @log_run_info
    @retrying(n=3, sleep_time=15, allowed_exceptions=(UnexpectedExit, Failure))  # retrying since SSH can fail with 255
    def run_compaction_on_all_nodes(self):
        for node in self.db_cluster.nodes:
            node.run_nodetool(
                "compact", warning_event_on_exception=(UnexpectedExit, Failure), error_message="Expected exception. "
            )

    def _run_workload(self, param_name, subtype, keyspace_num=1):
        cmd = self.params.get(param_name)
        self.wait_no_compactions_running()
        self.run_fstrim_on_all_db_nodes()
        self.create_test_stats(sub_type=subtype, doc_id_with_timestamp=True)
        stress_queue = self.run_stress_thread(
            stress_cmd=cmd, stress_num=1, keyspace_num=keyspace_num, stats_aggregate_cmds=False
        )
        results = self.get_stress_results(queue=stress_queue, store_results=True)
        self.update_test_details(scylla_conf=True)
        stat_results = PP.pformat(self._stats["results"])
        self.log.debug("Results %s: \n%s", subtype, stat_results)
        self.display_results(results, test_name=subtype)
        return self._test_id

    def test_latency(self):
        with ignore_operation_errors():
            self.preload_data()
            self.run_compaction_on_all_nodes()
            for subtest in self.lwt_subtests:
                self._run_workload(subtest["sct_param"], subtest["subtest_name"])
            self.check_regression_multi_baseline(subtests_info=self.lwt_subtests, metrics=self.latency_report_metrics)

    def test_throughput(self):
        with ignore_operation_errors():
            self.preload_data()
            self.run_compaction_on_all_nodes()
            for subtest in self.lwt_subtests:
                self._run_workload(subtest["sct_param"], subtest["subtest_name"])

            self.check_regression_multi_baseline(
                subtests_info=self.lwt_subtests, metrics=self.throughput_report_metrics
            )
