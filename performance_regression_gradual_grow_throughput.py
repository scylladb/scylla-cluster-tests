import os
from enum import Enum
from collections import defaultdict

import json
from performance_regression_test import PerformanceRegressionTest
from sdcm.results_analyze import ThroughputLatencyGradualGrowPayloadPerformanceAnalyzer


class CSPopulateDistribution(Enum):
    GAUSS = "gauss"
    UNIFORM = "uniform"


class PerformanceRegressionGradualGrowThroughutTest(PerformanceRegressionTest):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # all parameters were taken from scylla-stress-orch repo
        # Planned data size is 3 TB in total: 1tb per node
        self.NUM_THREADS = 500
        self.CLUSTER_SIZE = self.params.get("n_db_nodes")
        self.REPLICATION_FACTOR = 3
        # Planned data size is 3 TB in total: 1tb per node
        self.DATASET_SIZE = 3000 * 1024 * 1024 * 1024
        # expected row size (was calculated by dev team ~313 bytes)
        self.ROW_SIZE_BYTES = 210 * 1024 * 1024 * 1024 / 720_000_000
        self.ROW_COUNT = int(self.DATASET_SIZE / self.ROW_SIZE_BYTES /
                             self.REPLICATION_FACTOR)
        # How many standard deviations should span the cluster's memory
        self.CONFIDENCE = 6
        # instance i3.4xlarge
        self.INSTANCE_MEMORY_GB = 122
        self.GAUSS_CENTER = self.ROW_COUNT // 2
        self.GAUSS_SIGMA = int(self.INSTANCE_MEMORY_GB * self.CLUSTER_SIZE * 1024 * 1024 *
                               1024 // (self.CONFIDENCE * self.ROW_SIZE_BYTES * self.REPLICATION_FACTOR))

        self.MAX_95TH_LATENCY = 20.0  # pylint: disable=invalid-name
        self.MAX_99TH_LATENCY = 400.0  # pylint: disable=invalid-name

    def test_mixed_gradual_increase_load(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a mixed workload with gradual increase load
        """
        self._base_test_workflow(cs_cmd_tmpl=self.params.get('stress_cmd_m'),
                                 test_name="Test 'mixed: read:50%,write:50%'")

    def test_write_gradual_increase_load(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a write workload with gradual increase load
        """
        self._base_test_workflow(cs_cmd_tmpl=self.params.get('stress_cmd_w'),
                                 test_name="Test 'write 100%'")

    def test_read_gradual_increase_load(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a read workload with gradual increase load
        """
        self._base_test_workflow(cs_cmd_tmpl=self.params.get('stress_cmd_r'),
                                 test_name="Test 'read 100%'")

    def _base_test_workflow(self, cs_cmd_tmpl, test_name):
        stress_num = 1
        num_loaders = len(self.loaders.nodes)
        self.run_fstrim_on_all_db_nodes()
        # run a write workload as a preparation
        compaction_strategy = self.params.get('compaction_strategy')
        self.preload_data(compaction_strategy=compaction_strategy)
        self.wait_no_compactions_running(n=400, sleep_time=120)

        self.run_fstrim_on_all_db_nodes()
        # self.disable_autocompaction_on_all_nodes()
        self.run_gradual_increase_load(stress_cmd_templ=cs_cmd_tmpl,
                                       start_ops=self.start_ops,
                                       throttle_step=self.throttle_step,
                                       max_ops=self.max_ops,
                                       stress_num=stress_num,
                                       num_loaders=num_loaders,
                                       compaction_strategy=compaction_strategy,
                                       test_name=test_name)

    def preload_data(self, compaction_strategy=None):
        prepare_write_tmpl: str = self.params.get("prepare_write_cmd")
        num_of_loaders: int = self.params.get("n_loaders")
        row_count_per_loader: int = self.ROW_COUNT // num_of_loaders
        range_points = [1]

        for i in range(num_of_loaders):
            range_points.append(range_points[-1] + row_count_per_loader)
        range_points[-1] = self.ROW_COUNT

        population_commands = []
        for i in range(len(range_points) - 1):
            cmd = prepare_write_tmpl.replace(
                "$ROW_NUMBER", f"{range_points[i + 1] - range_points[i] + 1}").replace(
                "$SEQUENCE", f"{range_points[i]}..{range_points[i + 1]}")
            population_commands.append(cmd)

        self.log.info("Population c-s commands: %s", population_commands)
        # Check if it should be round_robin across loaders
        params = {}
        stress_queue = []
        if self.params.get('round_robin'):
            self.log.debug('Populating data using round_robin')
            params.update({'stress_num': 1, 'round_robin': True})
        if compaction_strategy:
            self.log.debug('Next compaction strategy will be used %s', compaction_strategy)
            params['compaction_strategy'] = compaction_strategy

        for stress_cmd in population_commands:
            params.update({'stress_cmd': stress_cmd})
            # Run all stress commands
            params.update(dict(stats_aggregate_cmds=False))
            self.log.debug('RUNNING stress cmd: {}'.format(stress_cmd))
            stress_queue.append(self.run_stress_thread(**params))

        for stress in stress_queue:
            self.get_stress_results(queue=stress, store_results=False)

        self.log.info("Dataset has been populated")

    def run_gradual_increase_load(self, stress_cmd_templ,
                                  start_ops, max_ops, throttle_step,
                                  stress_num, num_loaders, compaction_strategy, test_name):
        self.warmup_cache(compaction_strategy)
        total_summary = []
        cs_popuplation_distribution = self.get_cs_distribution()
        base_stress_cmd = stress_cmd_templ[0].replace("$DIST_PARAMS", cs_popuplation_distribution)

        for current_ops in range(start_ops, max_ops + throttle_step, throttle_step):
            self.log.info("Run cs command with rate: %s Kops", current_ops)
            current_throttle = current_ops // (num_loaders * stress_num)
            stress_cmd = base_stress_cmd.replace("$threads", f"{self.NUM_THREADS}").replace(
                "$throttle", f"{current_throttle}")

            stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stress_num=stress_num,
                                                  compaction_strategy=compaction_strategy, stats_aggregate_cmds=False)
            results = self.get_stress_results(queue=stress_queue, store_results=False)
            self.log.debug("All c-s results: %s", results)
            summary_result = self._calculate_average_latency(results)
            summary_result["ops"] = current_ops
            self.log.debug("C-S results for ops: %s. \n Results: \n %s", current_ops, summary_result)
            total_summary.append(summary_result)
            if (summary_result["latency 95th percentile"] > self.MAX_95TH_LATENCY or
                    summary_result["latency 99th percentile"] > self.MAX_99TH_LATENCY):

                self.log.warning("Latency 95th percentile is large that %d", self.MAX_95TH_LATENCY)
                break

        total_summary_json = json.dumps(total_summary, indent=4, separators=(", ", ": "))
        self.log.debug("---------------------------------")
        self.log.debug("Final table with results: \n %s", total_summary_json)
        self.log.debug("---------------------------------")

        filename = f"{self.logdir}/result_gradual_increase.log"
        with open(filename, "w", encoding="utf-8") as res_file:
            res_file.write(total_summary_json)

        screenshots = self.monitors.get_grafana_screenshots(self.monitors.nodes[0], self.start_time)

        setup_details = {
            "test_id": self.test_id,
            "scylla_version": self.db_cluster.nodes[0].scylla_version_detailed,
            "num_loaders": len(self.loaders.nodes),
            "cluster_size": len(self.db_cluster.nodes),
            "db_instance_type": self.params.get("instance_type_db"),
            "loader_instance_type": self.params.get("instance_type_loader"),
            "scylladb_ami": self.params.get("ami_id_db_scylla"),
            "loader_ami": self.params.get("ami_id_loader"),
            "start_time": self.start_time,
            "screenshots": screenshots,
            "job_url": os.environ.get("BUILD_URL"),
            "shard_aware_driver": self.is_shard_awareness_driver,
        }

        perf_analyzer = ThroughputLatencyGradualGrowPayloadPerformanceAnalyzer(
            es_index=self._test_index,
            es_doc_type=self._es_doc_type,
            email_recipients=self.params.get('email_recipients'))
        perf_analyzer.check_regression(test_name=test_name, test_results=total_summary, test_details=setup_details)

    def get_cs_distribution(self):
        popuplation_distribution = CSPopulateDistribution(self.params.get("cs_populating_distribution"))
        if popuplation_distribution == CSPopulateDistribution.GAUSS:
            return f"GAUSSIAN(1..{self.ROW_COUNT},{self.GAUSS_CENTER},{self.GAUSS_SIGMA})"
        elif popuplation_distribution == CSPopulateDistribution.UNIFORM:
            return f"UNIFORM(1..{self.ROW_COUNT})"
        else:
            self.log.error("Unsupported cs population distribution")
            return ""

    @staticmethod
    def _calculate_average_latency(results):
        status = defaultdict(float).fromkeys(results[0].keys(), 0.0)
        for result in results:
            for key in status:
                try:
                    status[key] += float(result.get(key, 0.0))
                except ValueError:
                    continue

        for key in status:
            status[key] = round(status[key] / len(results), 2)

        return status

    def warmup_cache(self, compaction_strategy):
        cmd = f"cassandra-stress read no-warmup cl=QUORUM duration=180m -pop 'dist={self.get_cs_distribution()}' -mode native cql3 -rate 'threads=500 throttle=35000/s'"  # pylint: disable=line-too-long
        stress_queue = self.run_stress_cassandra_thread(
            stress_cmd=cmd,
            stress_num=1,
            compaction_strategy=compaction_strategy,
            stats_aggregate_cmds=False,
            round_robin=True
        )
        self.get_stress_results(stress_queue, store_results=False)
