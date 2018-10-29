from avocado.utils.process import CmdError

from performance_regression_test import PerformanceRegressionTest
from sdcm.utils import log_run_info, retrying


class InMemoryPerformanceRegressionTest(PerformanceRegressionTest):

    """
    Test Scylla performance regression with cassandra-stress.

    :avocado: enable
    """

    def __init__(self, *args, **kwargs):
        super(InMemoryPerformanceRegressionTest, self).__init__(*args, **kwargs)

    @log_run_info
    @retrying(n=3, sleep_time=15, allowed_exceptions=(CmdError,))  # retrying since SSH can fail with 255
    def run_compaction_on_all_nodes(self):
        for node in self.db_cluster.nodes:
            node.run_nodetool("compact")

    def test_latency(self):
        self.preload_data()
        self.alter_table_to_in_memory()
        # restart needed to load data to in-memory store
        self.run_compaction_on_all_nodes()
        self.db_cluster.restart_scylla()
        self.run_read_workload()
