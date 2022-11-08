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
from dataclasses import dataclass

from sdcm.stress_thread import CassandraStressThread
from sdcm.tester import ClusterTester
LOGGER = logging.getLogger(__name__)


@dataclass
class PerfResult:
    latency_99: float
    ops: int


class PerPartitionLimitTest(ClusterTester):

    def test_per_partition_limit(self):
        """
        assumptions:
        no need to preload data
        no need to add nemesis (even there's might be impact to limit precision,
        nemesis than can affect this like add/remove/restart nodes are rare)
        Test steps:
        1. measure latency/ops baseline for read/write/mixed with and without additional heavy several-partitions load
        2. measure latency/ops with per-partition limits set with and without additional heavy several-partitions load
        3. compare results: there should be no much degradation in performance (and improvement when additional load added)
        """
        table = "scylla_bench.test"
        stress_write_cmd = self.params.get("stress_cmd_w")
        stress_read_cmd = self.params.get("stress_cmd_r")
        stress_write_partition_cmd = self.params.get("stress_cmd")[0]
        stress_read_partition_cmd = self.params.get("stress_cmd")[1]

        LOGGER.info("measure base latency and ops/s - without setting limits")
        base_write_result = self._measure_latency_and_ops(stress_cmd=stress_write_cmd)
        base_read_result = self._measure_latency_and_ops(stress_cmd=stress_read_cmd)

        LOGGER.info("measure base latency and ops/s with stressing several partitions - without setting limits")
        self._truncate_table(table=table)
        self.run_stress_thread(stress_cmd=stress_write_partition_cmd)
        stressed_base_write_result = self._measure_latency_and_ops(stress_cmd=stress_write_cmd)
        self.run_stress_thread(stress_cmd=stress_read_partition_cmd)
        stressed_base_read_result = self._measure_latency_and_ops(stress_cmd=stress_read_cmd)

        LOGGER.info("measure latency and ops/s - with setting limits: should not affect base results much")
        self._set_per_partition_limits(table=table, reads_limit=100, writes_limit=100)
        self._truncate_table(table=table)
        limited_write_result = self._measure_latency_and_ops(stress_cmd=stress_write_cmd)
        limited_read_result = self._measure_latency_and_ops(stress_cmd=stress_read_cmd)

        LOGGER.info("measure latency and ops/s with stressing several partitions - with setting limits")
        self._truncate_table(table=table)
        self.run_stress_thread(stress_cmd=stress_write_partition_cmd)
        stressed_limited_write_result = self._measure_latency_and_ops(stress_cmd=stress_write_cmd)
        self.run_stress_thread(stress_cmd=stress_read_partition_cmd)
        stressed_limited_read_result = self._measure_latency_and_ops(stress_cmd=stress_read_cmd)

        assert limited_write_result.latency_99 < base_write_result.latency_99 * 1.05, \
            f"Per partition limit caused more than 5% write latency degradation " \
            f"{limited_write_result.latency_99}<{base_write_result.latency_99 * 1.05}"
        assert limited_read_result.latency_99 < base_read_result.latency_99 * 1.05, \
            f"Per partition limit caused more than 5% read latency degradation" \
            f"{limited_read_result.latency_99}<{base_read_result.latency_99 * 1.05}"
        assert limited_write_result.ops > base_write_result.ops*0.95, \
            f"Per partition limit caused more than 5% write op/s degradation" \
            f"{limited_write_result.ops}>{base_write_result.ops*0.95}"
        assert limited_read_result.ops > base_read_result.ops*0.95, \
            f"Per partition limit caused more than 5% read op/s degradation" \
            f"{limited_read_result.ops}>{base_read_result.ops*0.95}"

        assert stressed_limited_write_result.latency_99 < stressed_base_write_result.latency_99 * 0.95, \
            f"Per partition limit didn't bring write latency improvement when several partitions were additionally stressed" \
            f"{stressed_limited_write_result.latency_99} < {stressed_base_write_result.latency_99 * 0.95}"
        assert stressed_limited_read_result.latency_99 < stressed_base_read_result.latency_99 * 0.95, \
            "Per partition limit didn't bring read latency improvement when several partitions were additionally stressed" \
            f"{stressed_limited_read_result.latency_99} < {stressed_base_read_result.latency_99 * 0.95}"
        assert stressed_limited_write_result.ops > stressed_base_write_result.ops * 1.05, \
            "Per partition limit didn't bring write op/s improvement when several partitions were additionally stressed" \
            f"{stressed_limited_write_result.ops} > {stressed_base_write_result.ops * 1.05}"
        assert stressed_limited_read_result.ops > stressed_base_read_result.ops * 1.05, \
            "Per partition limit didn't bring read op/s improvement when several partitions were additionally stressed" \
            f"{stressed_limited_read_result.ops} > {stressed_base_read_result.ops * 1.05}"

    def _measure_latency_and_ops(self, stress_cmd: str):
        # investigate if we shouldn't aggregate results param or round robin when more loaders is added
        stress_thread: CassandraStressThread = self.run_stress_thread(stress_cmd=stress_cmd)
        cs_summary, errors = stress_thread.verify_results()
        LOGGER.debug(cs_summary)
        result = PerfResult(latency_99=cs_summary[0]["latency 99th percentile"],
                            ops=cs_summary[0]["op rate"])
        LOGGER.info(result)
        assert not errors, f"Errors during running stress_cmd: {stress_cmd}: {errors}"
        return result

    def _set_per_partition_limits(self, table: str, reads_limit: int, writes_limit: int):
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(f"""
             ALTER TABLE {table}
             WITH per_partition_rate_limit = {{
             'max_reads_per_second': {reads_limit},
             'max_writes_per_second': {writes_limit}
             }}
            """)

    def _truncate_table(self, table: str):
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(f"TRUNCATE TABLE {table}")
