import logging

from sdcm.tester import ClusterTester

LOGGER = logging.getLogger(__name__)


class PerformanceRegressionNosqlBenchTest(ClusterTester):
    #  pylint: disable=useless-super-delegation
    def __init__(self, *args):
        super().__init__(*args)

    def test_mixed(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a mixed workload
        """
        stress_cmd = self.params.get("stress_cmd_m")
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stress_num=1, stats_aggregate_cmds=False)
        results = self.get_stress_results(queue=stress_queue)
        LOGGER.info("Raw nosqlbench run result: %s", results)
