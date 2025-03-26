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
# Copyright (c) 2023 ScyllaDB
import json

from sdcm.argus_results import send_perf_simple_query_result_to_argus
from sdcm.tester import ClusterTester, teardown_on_exception, log_run_info


class PerfSimpleQueryTest(ClusterTester):
    @teardown_on_exception
    @log_run_info
    def setUp(self):
        super().setUp()

        if es_index := self.params.get("custom_es_index"):
            self._test_index = es_index

    def test_perf_simple_query(self):
        perf_simple_query_extra_command = self.params.get('perf_simple_query_extra_command') or ""
        result = self.db_cluster.nodes[0].remoter.run(
            f"scylla perf-simple-query --json-result=perf-simple-query-result.txt --smp 1 -m 1G {perf_simple_query_extra_command}")
        if result.ok:
            output = self.db_cluster.nodes[0].remoter.run("cat perf-simple-query-result.txt").stdout
            results = json.loads(output)

        send_perf_simple_query_result_to_argus(self.test_config.argus_client(), results)
