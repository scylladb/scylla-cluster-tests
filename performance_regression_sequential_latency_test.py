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
# Copyright (c) 2026 ScyllaDB

import re

from performance_regression_test import PerformanceRegressionTest
from sdcm.utils.decorators import latency_calculator_decorator
from sdcm.utils.common import skip_optional_stage


class PerformanceRegressionSequentialLatencyTest(PerformanceRegressionTest):
    """Performance test that measures each stress command as a separate latency cycle.

    Unlike the concurrent workloads in PerformanceRegressionTest, this runs stress
    commands one at a time, treating each as an independent latency measurement
    cycle reported to Argus via latency_calculator_decorator.

    Useful for any workload whose individual query patterns must be measured in
    isolation, since running them concurrently would make the per-pattern latency
    and throughput numbers indistinguishable from each other.
    """

    def test_sequential_latency(self):
        """Run every stress_cmd entry sequentially as an independent measurement cycle.

        Each command is wrapped in latency_calculator_decorator so per-cycle latency
        and throughput data (HDR histograms, Grafana screenshots, reactor-stall stats)
        is collected and sent to Argus before the next command starts.

        Test steps:
        1. Add nemesis as configured by nemesis_class_name.
        2. Preload data via preload_data() (honours pre_create_keyspace).
        3. Wait for space threshold, start nemesis, wait for compactions, fstrim.
        4. Run each stress_cmd sequentially; report per-cycle results to Argus.
        """
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(), tester_obj=self)

        self.preload_data()

        self.db_cluster.wait_total_space_used_per_node(keyspace=None)
        self.db_cluster.start_nemesis()

        self.wait_no_compactions_running()
        self.run_fstrim_on_all_db_nodes()

        stress_cmds = self.params.get("stress_cmd")
        if isinstance(stress_cmds, str):
            stress_cmds = [stress_cmds]

        if skip_optional_stage("main_load"):
            return

        for stress_cmd in stress_cmds:
            match = re.search(r"--function[= ](\S+)", stress_cmd)
            cycle_name = match.group(1) if match else "stress_cmd"

            @latency_calculator_decorator(workload_type="read", cycle_name=cycle_name, legend=cycle_name)
            def _run_single_cmd(tester, cmd):
                stress_thread = tester.run_stress_thread(
                    stress_cmd=cmd, stress_num=1, round_robin=True, stats_aggregate_cmds=False
                )
                tester.verify_stress_thread(stress_thread)
                tester.get_stress_results(queue=stress_thread, store_results=True)
                # NOTE: 'hdr_tags' will be used by the 'latency_calculator_decorator' decorator
                return {"hdr_tags": stress_thread.hdr_tags}

            _run_single_cmd(self, stress_cmd)
