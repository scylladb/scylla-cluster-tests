import pprint
import time

from sdcm.cluster import BaseNode, UnexpectedExit, Failure
from performance_regression_test import PerformanceRegressionTest


PP = pprint.PrettyPrinter(indent=2)


class PerformanceRegressionCDCTest(PerformanceRegressionTest):
    keyspace = None
    table = None
    cdclog_reader_cmd = None
    enable_batching = False
    update_es = False

    def test_write_with_cdc(self):
        write_cmd = self.params.get("stress_cmd_w")

        self._workload_cdc(write_cmd,
                           stress_num=2,
                           test_name="test_write",
                           sub_type="cdc_disabled")

        node1: BaseNode = self.db_cluster.nodes[0]

        self.truncate_base_table(node1)
        node1.run_cqlsh(f"ALTER TABLE {self.keyspace}.{self.table} WITH cdc = {{'enabled': true}}")

        self.wait_no_compactions_running()
        self.run_fstrim_on_all_db_nodes()

        self._workload_cdc(write_cmd,
                           stress_num=2,
                           test_name="test_write",
                           sub_type="cdc_enabled")

        self.wait_no_compactions_running()
        self.check_regression_with_baseline(subtest_baseline="cdc_disabled")

    def test_write_with_cdc_preimage(self):
        write_cmd = self.params.get("stress_cmd_w")

        self._workload_cdc(write_cmd,
                           stress_num=2,
                           test_name="test_write",
                           sub_type="cdc_disabled")
        node1: BaseNode = self.db_cluster.nodes[0]

        self.truncate_base_table(node1, cdclog_table=True)
        node1.run_cqlsh(f"ALTER TABLE {self.keyspace}.{self.table} WITH cdc = {{'enabled': true, 'preimage': true}}")

        self._workload_cdc(write_cmd,
                           stress_num=2,
                           test_name="test_write",
                           sub_type="cdc_preimage_enabled")

        self.check_regression_with_baseline(subtest_baseline="cdc_disabled")

    def test_write_with_cdc_postimage(self):
        write_cmd = self.params.get("stress_cmd_w")

        self._workload_cdc(write_cmd,
                           stress_num=2,
                           test_name="test_write",
                           sub_type="cdc_disabled")
        node1: BaseNode = self.db_cluster.nodes[0]

        self.truncate_base_table(node1, cdclog_table=True)
        node1.run_cqlsh(f"ALTER TABLE {self.keyspace}.{self.table} WITH cdc = {{'enabled': true, 'postimage': true}}")

        self._workload_cdc(write_cmd,
                           stress_num=2,
                           test_name="test_write",
                           sub_type="cdc_preimage_enabled")

        self.check_regression_with_baseline(subtest_baseline="cdc_disabled")

    def test_write_throughput(self):
        self.cdc_workflow()

    def test_write_latency(self):
        self.cdc_workflow()
        self.update_test_details()

    def test_mixed_throughput(self):
        self.cdc_workflow(use_cdclog_reader=True)

    def test_mixed_latency(self):
        self.cdc_workflow(use_cdclog_reader=True)

    def cdc_workflow(self, use_cdclog_reader=False):
        self.keyspace = "keyspace1"
        self.table = "standard1"
        write_cmd = self.params.get("stress_cmd_w")

        if use_cdclog_reader:
            self.cdclog_reader_cmd = self.params.get('stress_cdclog_reader_cmd')
            self.update_es = self.params.get('store_cdclog_reader_stats_in_es')
            self.enable_batching = self.params.get('stress_cdc_log_reader_batching_enable')

        self._workload_cdc(write_cmd,
                           stress_num=2,
                           test_name="test_write",
                           sub_type="cdc_disabled")

        node1: BaseNode = self.db_cluster.nodes[0]
        self.wait_no_compactions_running()
        self.run_fstrim_on_all_db_nodes()
        self.truncate_base_table(node1)

        node1.run_cqlsh(f"ALTER TABLE {self.keyspace}.{self.table} WITH cdc = {{'enabled': true}}")

        self._workload_cdc(write_cmd,
                           stress_num=2,
                           test_name="test_write",
                           sub_type="cdc_enabled",
                           read_cdclog_cmd=self.cdclog_reader_cmd,
                           update_cdclog_stats=self.update_es,
                           enable_batching=self.enable_batching)

        self.wait_no_compactions_running()
        self.run_fstrim_on_all_db_nodes()
        self.truncate_base_table(node1, cdclog_table=True)

        node1.run_cqlsh(f"ALTER TABLE {self.keyspace}.{self.table} WITH cdc = {{'enabled': true, 'preimage': true}}")

        self._workload_cdc(write_cmd,
                           stress_num=2,
                           test_name="test_write",
                           sub_type="cdc_preimage_enabled",
                           read_cdclog_cmd=self.cdclog_reader_cmd,
                           update_cdclog_stats=self.update_es,
                           enable_batching=self.enable_batching)

        self.wait_no_compactions_running()
        self.run_fstrim_on_all_db_nodes()
        self.truncate_base_table(node1, cdclog_table=True)

        node1.run_cqlsh(f"ALTER TABLE {self.keyspace}.{self.table} WITH cdc = {{'enabled': true, 'postimage': true}}")

        self._workload_cdc(write_cmd,
                           stress_num=2,
                           test_name="test_write",
                           sub_type="cdc_postimage_enabled",
                           read_cdclog_cmd=self.cdclog_reader_cmd,
                           update_cdclog_stats=self.update_es,
                           enable_batching=self.enable_batching)

        self.wait_no_compactions_running()

        self.check_regression_with_baseline(subtest_baseline="cdc_disabled")

    def _workload_cdc(self, stress_cmd, stress_num, test_name, sub_type=None,
                      save_stats=True, read_cdclog_cmd=None, update_cdclog_stats=False, enable_batching=True):
        cdc_stress_queue = None

        if save_stats:
            self.create_test_stats(sub_type=sub_type,
                                   doc_id_with_timestamp=True,
                                   append_sub_test_to_name=False)

        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stress_num=stress_num,
                                              stats_aggregate_cmds=False)

        if read_cdclog_cmd:
            cdc_stress_queue = self.run_cdclog_reader_thread(stress_cmd=read_cdclog_cmd,
                                                             stress_num=1,
                                                             keyspace_name=self.keyspace,
                                                             base_table_name=self.table,
                                                             enable_batching=enable_batching)

        results = self.get_stress_results(queue=stress_queue, store_results=True)
        if cdc_stress_queue:
            cdc_stress_results = self.verify_cdclog_reader_results(cdc_stress_queue, update_cdclog_stats)
            self.log.debug(cdc_stress_results)

        if save_stats:
            self.update_test_details(scylla_conf=True)
            stat_results = PP.pformat(self._stats["results"])
            self.log.debug('Results %s: \n%s', test_name, stat_results)
            self.display_results(results, test_name=test_name)

    def truncate_base_table(self, node, cdclog_table=False):
        try:
            node.run_cqlsh(f"TRUNCATE TABLE {self.keyspace}.{self.table}", timeout=300)
        except (UnexpectedExit, Failure) as details:
            self.log.warning("Truncate error %s. Sleep and continue", details)
            time.sleep(60)
        if cdclog_table:
            try:
                node.run_cqlsh(f"TRUNCATE TABLE {self.keyspace}.{self.table}_scylla_cdc_log", timeout=300)
            except (UnexpectedExit, Failure) as details:
                self.log.warning("Truncate error %s. Sleep and continue", details)
                time.sleep(60)
