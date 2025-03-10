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
import time
import yaml

from sdcm.cluster import BaseNode
from sdcm.sct_events import Severity
from sdcm.sct_events.system import InfoEvent
from sdcm.stress_thread import CassandraStressThread
from sdcm.tester import ClusterTester


LOGGER = logging.getLogger(__name__)


@dataclass
class PerfResult:
    latency_99: float
    ops: int


class ThroughputLimitFunctionalTest(ClusterTester):

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
        base_write_result = self._measure_latency_and_ops(stress_cmd=stress_write_cmd, conditions="base write")
        base_read_result = self._measure_latency_and_ops(stress_cmd=stress_read_cmd, conditions="base read")

        LOGGER.info("measure base latency and ops/s with stressing several partitions - without setting limits")
        self._truncate_table(table=table)
        self.run_stress_thread(stress_cmd=stress_write_partition_cmd)
        stressed_base_write_result = self._measure_latency_and_ops(
            stress_cmd=stress_write_cmd, conditions="base write stressed")
        self.run_stress_thread(stress_cmd=stress_read_partition_cmd)
        stressed_base_read_result = self._measure_latency_and_ops(
            stress_cmd=stress_read_cmd, conditions="base read stressed")

        LOGGER.info("measure latency and ops/s - with setting limits: should not affect base results much")
        self._set_per_partition_limits(table=table, reads_limit=100, writes_limit=100)
        self._truncate_table(table=table)
        limited_write_result = self._measure_latency_and_ops(stress_cmd=stress_write_cmd, conditions="limited write")
        limited_read_result = self._measure_latency_and_ops(stress_cmd=stress_read_cmd, conditions="limited read")

        LOGGER.info("measure latency and ops/s with stressing several partitions - with setting limits")
        self._truncate_table(table=table)
        self.run_stress_thread(stress_cmd=stress_write_partition_cmd)
        stressed_limited_write_result = self._measure_latency_and_ops(
            stress_cmd=stress_write_cmd, conditions="limited write stressed")
        self.run_stress_thread(stress_cmd=stress_read_partition_cmd)
        stressed_limited_read_result = self._measure_latency_and_ops(
            stress_cmd=stress_read_cmd, conditions="limited read stressed")

        assert limited_write_result.latency_99 < base_write_result.latency_99 * 1.05, \
            f"Per partition limit caused more than 5% write latency degradation " \
            f"{limited_write_result.latency_99}<{base_write_result.latency_99 * 1.05}"
        assert limited_read_result.latency_99 < base_read_result.latency_99 * 1.05, \
            f"Per partition limit caused more than 5% read latency degradation" \
            f"{limited_read_result.latency_99}<{base_read_result.latency_99 * 1.05}"
        assert limited_write_result.ops > base_write_result.ops * 0.95, \
            f"Per partition limit caused more than 5% write op/s degradation" \
            f"{limited_write_result.ops}>{base_write_result.ops * 0.95}"
        assert limited_read_result.ops > base_read_result.ops * 0.95, \
            f"Per partition limit caused more than 5% read op/s degradation" \
            f"{limited_read_result.ops}>{base_read_result.ops * 0.95}"

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

    def test_compaction_throughput_limit(self):
        """
        Test how compaction thoughput limit affects Scylla latency/throughput.

        Test steps:
        1. measure latency/ops baseline for write load
        2. measure latency/ops with compaction throughput limits
        3. compare results: there should be some latency/throughput improvements when limit is on
        4. see monitor dashboards to see compaction and write throughput effects for different settings
        """
        table = "keyspace1.standard1"
        disk_write_throughput = self._get_disk_write_max_throughput(self.db_cluster.nodes[0])
        stress_write_cmd = self.params.get("stress_cmd_w")

        LOGGER.info("measure base latency and ops/s - without setting limits")
        base_write_result = self._measure_latency_and_ops(stress_cmd=stress_write_cmd, conditions="base write")
        for limit in [0.45, 0.2, 0.01]:
            LOGGER.info("measure latency and ops/s - with setting compaction limit to %s of disk write bandwidth", limit)
            self._truncate_table(table=table)
            time.sleep(60)  # to make scylla complete truncate and make clear gap between load
            self._set_compaction_limit(limit_mb=round(disk_write_throughput * limit))

            limited_write_result = self._measure_latency_and_ops(
                stress_cmd=stress_write_cmd, conditions=f"limited {str(limit)} write")
            if limited_write_result.latency_99 > base_write_result.latency_99:
                InfoEvent(f"Compaction throughput limit didn't bring write latency improvements when "
                          f"limiting compaction to {limit} of disk write bandwidth "
                          f"{limited_write_result.latency_99}<{base_write_result.latency_99}",
                          severity=Severity.ERROR).publish()
            if limited_write_result.ops < base_write_result.ops:
                InfoEvent(f"Compaction throughput limit didn't bring write op/s improvements when "
                          f"limiting compaction to {limit} of disk write bandwidth "
                          f"{limited_write_result.ops}>{base_write_result.ops}",
                          severity=Severity.ERROR).publish()

    def _measure_latency_and_ops(self, stress_cmd: str, conditions: str = ""):
        # investigate if we shouldn't aggregate results param or round robin when more loaders is added
        stress_thread: CassandraStressThread = self.run_stress_thread(stress_cmd=stress_cmd)
        cs_summary, errors = stress_thread.parse_results()
        LOGGER.debug(cs_summary)
        result = PerfResult(latency_99=float(cs_summary[0]["latency 99th percentile"]),
                            ops=float(cs_summary[0]["op rate"]))
        LOGGER.info("%s: %s", conditions, result)
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
            session.execute(f"TRUNCATE TABLE {table}", timeout=300)

    @staticmethod
    def _get_disk_write_max_throughput(node: BaseNode):
        result = node.remoter.run("cat /etc/scylla.d/io_properties.yaml", ignore_status=True)
        if result.ok:
            io_properties = result.stdout
        else:
            LOGGER.warning("Failed to read io_properties.yaml file. Taking defaults.")
            io_properties = """
            disks:
              - mountpoint: /var/lib/scylla
                read_iops: 540317
                read_bandwidth: 1914761472
                write_iops: 300319
                write_bandwidth: 116202240
         """
        return yaml.safe_load(io_properties)["disks"][0]["write_bandwidth"] / 1024 / 1024

    def _set_compaction_limit(self, limit_mb: int = 0):
        for node in self.db_cluster.nodes:
            with node.remote_scylla_yaml() as scylla_yml:
                scylla_yml.compaction_throughput_mb_per_sec = limit_mb
            mark = node.mark_log()
            node.reload_config()
            node.follow_system_log(patterns=[f"Set compaction bandwidth to {limit_mb}MB/s"], start_from_beginning=True)
            self._wait_for_compaction_limit_set(node, mark=mark, limit_mb=limit_mb)
        time.sleep(60)  # wait for scylla to stabilse after config change

    @staticmethod
    def _wait_for_compaction_limit_set(node, mark, limit_mb, timeout=60):
        """Waits for compaction limit set by reading node log and waiting for proper log line."""
        start_time = time.time()
        watch_for = f"Set compaction bandwidth to {limit_mb}MB/s"
        with open(node.system_log, encoding="utf-8") as log_file:
            log_file.seek(mark)
            while time.time() - start_time < timeout:
                line = log_file.readline()
                if watch_for in line:
                    LOGGER.info("set compaction limit to %s on node %s", limit_mb, node.name)
                    return
        raise TimeoutError(f"Timeout during setting compaction limit on {node.name}")
