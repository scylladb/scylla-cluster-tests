import pathlib
import time
from enum import Enum
from collections import defaultdict

import json
from typing import NamedTuple

from performance_regression_test import PerformanceRegressionTest
from sdcm.utils.common import skip_optional_stage
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.results_analyze import PredefinedStepsTestPerformanceAnalyzer
from sdcm.utils.decorators import latency_calculator_decorator
from sdcm.utils.latency import calculate_latency, analyze_hdr_percentiles


class CSPopulateDistribution(Enum):
    GAUSS = "gauss"
    UNIFORM = "uniform"


class Workload(NamedTuple):
    workload_type: str
    cs_cmd_tmpl: list
    cs_cmd_warm_up: list | None
    num_threads: int
    throttle_steps: list
    preload_data: bool
    drop_keyspace: bool
    wait_no_compactions: bool


class PerformanceRegressionPredefinedStepsTest(PerformanceRegressionTest):  # pylint: disable=too-many-instance-attributes
    """
    This class presents new performance test that run gradual increased throughput steps.
    The test run steps with different throughput.
    Throughput of every step is fixed and defined hardcoded according to the load type (write, read
    and mixed). Last step is unthrottled.
    Latency for every step is received from cassandra-stress HDR file and reported in Argus and email.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.CLUSTER_SIZE = self.params.get("n_db_nodes")  # pylint: disable=invalid-name
        self.REPLICATION_FACTOR = 3  # pylint: disable=invalid-name

    def throttle_steps(self, workload_type):
        throttle_steps = self.params["perf_gradual_throttle_steps"]
        if workload_type not in throttle_steps:
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"Throttle steps for '{workload_type}' test is not defined in "
                                       f"'perf_gradual_throttle_steps' parameter",
                               severity=Severity.CRITICAL).publish()
        return throttle_steps[workload_type]

    def test_mixed_gradual_increase_load(self):  # pylint: disable=too-many-locals
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a mixed workload with gradual increase load
        """
        workload_type = "mixed"
        workload = Workload(workload_type=workload_type,
                            cs_cmd_tmpl=self.params.get('stress_cmd_m'),
                            cs_cmd_warm_up=self.params.get('stress_cmd_cache_warmup'),
                            num_threads=self.params["perf_gradual_threads"][workload_type],
                            throttle_steps=self.throttle_steps(workload_type),
                            preload_data=True,
                            drop_keyspace=False,
                            wait_no_compactions=True)
        self._base_test_workflow(workload=workload,
                                 test_name="test_mixed_gradual_increase_load (read:50%,write:50%)")

    def test_write_gradual_increase_load(self):  # pylint: disable=too-many-locals
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a write workload with gradual increase load
        """
        workload_type = "write"
        workload = Workload(workload_type=workload_type,
                            cs_cmd_tmpl=self.params.get('stress_cmd_w'),
                            cs_cmd_warm_up=None,
                            num_threads=self.params["perf_gradual_threads"][workload_type],
                            throttle_steps=self.throttle_steps(workload_type),
                            preload_data=False,
                            drop_keyspace=True,
                            wait_no_compactions=False)
        self._base_test_workflow(workload=workload,
                                 test_name="test_write_gradual_increase_load (100% writes)")

    def test_read_gradual_increase_load(self):  # pylint: disable=too-many-locals
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a read workload with gradual increase load
        """
        workload_type = "read"
        workload = Workload(workload_type=workload_type,
                            cs_cmd_tmpl=self.params.get('stress_cmd_r'),
                            cs_cmd_warm_up=self.params.get('stress_cmd_cache_warmup'),
                            num_threads=self.params["perf_gradual_threads"][workload_type],
                            throttle_steps=self.throttle_steps(workload_type),
                            preload_data=True,
                            drop_keyspace=False,
                            wait_no_compactions=True)
        self._base_test_workflow(workload=workload,
                                 test_name="test_read_gradual_increase_load (100% reads)")

    def _base_test_workflow(self, workload: Workload, test_name):
        stress_num = 1
        num_loaders = len(self.loaders.nodes)
        self.run_fstrim_on_all_db_nodes()
        # run a write workload as a preparation
        if workload.preload_data and not skip_optional_stage('perf_preload_data'):
            self.preload_data()
            self.wait_no_compactions_running(n=400, sleep_time=120)
            self.run_fstrim_on_all_db_nodes()

            if post_prepare_cql_cmds := self.params.get('post_prepare_cql_cmds'):
                self.log.debug("Execute post prepare queries: %s", post_prepare_cql_cmds)
                self._run_cql_commands(post_prepare_cql_cmds)

        self.run_gradual_increase_load(workload=workload,
                                       stress_num=stress_num,
                                       num_loaders=num_loaders,
                                       test_name=test_name)

    def preload_data(self, compaction_strategy=None):
        population_commands: list = self.params.get("prepare_write_cmd")

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

    def check_latency_during_steps(self, step):
        with open(self.latency_results_file, encoding="utf-8") as file:
            latency_results = json.load(file)
        self.log.debug('Step %s: latency_results were loaded from file %s and its result is %s',
                       step, self.latency_results_file, latency_results)
        if latency_results and self.create_stats:
            latency_results[step]["step"] = step
            latency_results[step] = calculate_latency(latency_results[step])
            latency_results = analyze_hdr_percentiles(latency_results)
            pathlib.Path(self.latency_results_file).unlink()
            self.log.debug('collected latency values are: %s', latency_results)
            self.update({"latency_during_ops": latency_results})
            return latency_results

    def run_step(self, stress_cmds, current_throttle, num_threads):
        results = []
        stress_queue = []
        for stress_cmd in stress_cmds:
            params = {"round_robin": True, "stats_aggregate_cmds": False}
            stress_cmd_to_run = stress_cmd.replace(
                "$threads", f"{num_threads}").replace("$throttle", f"{current_throttle}")
            params.update({'stress_cmd': stress_cmd_to_run})
            # Run all stress commands
            self.log.debug('RUNNING stress cmd: %s', stress_cmd_to_run)
            stress_queue.append(self.run_stress_thread(**params))

        for stress in stress_queue:
            results.extend(self.get_stress_results(queue=stress, store_results=False))
            self.log.debug("One c-s command results: %s", results[-1])
        return results

    def drop_keyspace(self):
        self.log.debug(f'Drop keyspace {"keyspace1"}')
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(f'DROP KEYSPACE IF EXISTS {"keyspace1"};')

    # pylint: disable=too-many-arguments,too-many-locals
    def run_gradual_increase_load(self, workload: Workload, stress_num, num_loaders, test_name):  # noqa: PLR0914
        if workload.cs_cmd_warm_up is not None:
            self.warmup_cache(workload.cs_cmd_warm_up, workload.num_threads)

        if not self.exists():
            self.log.debug("Create test statistics in ES")
            self.create_test_stats(sub_type=workload.workload_type, doc_id_with_timestamp=True)
        total_summary = {}

        for throttle_step in workload.throttle_steps:
            self.log.info("Run cs command with rate: %s Kops", throttle_step)
            current_throttle = f"fixed={int(int(throttle_step) // (num_loaders * stress_num))}/s" if throttle_step != "unthrottled" else ""
            run_step = ((latency_calculator_decorator(legend=f"Gradual test step {throttle_step} op/s",
                                                      cycle_name=throttle_step))(self.run_step))
            results = run_step(stress_cmds=workload.cs_cmd_tmpl, current_throttle=current_throttle,
                               num_threads=workload.num_threads)

            calculate_result = self._calculate_average_max_latency(results)
            self.update_test_details(scylla_conf=True)
            summary_result = self.check_latency_during_steps(step=throttle_step)
            summary_result[throttle_step].update({"ops_rate": calculate_result["op rate"] * num_loaders})
            total_summary.update(summary_result)
            if workload.drop_keyspace:
                self.drop_keyspace()
            # We want 3 minutes (180 sec) wait between steps.
            # In case of "mixed" workflow - wait for compactions finished.
            # In case of "read" workflow -  it just will wait for 3 minutes
            if workload.wait_no_compactions and (wait_time := self.wait_no_compactions_running()[0]) < 180:
                time.sleep(180 - wait_time)

        self.save_total_summary_in_file(total_summary)
        self.run_performance_analyzer(total_summary=total_summary)

    def save_total_summary_in_file(self, total_summary):
        total_summary_json = json.dumps(total_summary, indent=4, separators=(", ", ": "))
        self.log.debug("---------------------------------")
        self.log.debug("Final table with results: \n %s", total_summary_json)
        self.log.debug("---------------------------------")

        filename = f"{self.logdir}/result_gradual_increase.log"
        with open(filename, "w", encoding="utf-8") as res_file:
            res_file.write(total_summary_json)

    def run_performance_analyzer(self, total_summary):
        perf_analyzer = PredefinedStepsTestPerformanceAnalyzer(
            es_index=self._test_index,
            es_doc_type=self._es_doc_type,
            email_recipients=self.params.get('email_recipients'))
        # Keep next 2 lines for debug purpose
        self.log.debug("es_index: %s", self._test_index)
        self.log.debug("total_summary: %s", total_summary)
        is_gce = bool(self.params.get('cluster_backend') == 'gce')
        try:
            perf_analyzer.check_regression(test_id=self.test_id,
                                           data=total_summary,
                                           is_gce=is_gce,
                                           email_subject_postfix=self.params.get('email_subject_postfix'))
        except Exception as exc:  # noqa: BLE001
            TestFrameworkEvent(
                message='Failed to check regression',
                source=self.__class__.__name__,
                source_method='check_regression',
                exception=exc
            ).publish_or_dump()

    @staticmethod
    def _calculate_average_max_latency(results):
        status = defaultdict(float).fromkeys(results[0].keys(), 0.0)
        max_latency = defaultdict(list)

        for result in results:
            for key in status:
                try:
                    status[key] += float(result.get(key, 0.0))
                    if key in ["latency 95th percentile", "latency 99th percentile"]:
                        max_latency[f"{key} max"].append(float(result.get(key, 0.0)))
                except ValueError:
                    continue

        for key in status:
            status[key] = round(status[key] / len(results), 2)

        for key, latency in max_latency.items():
            status[key] = max(latency)

        return status

    def warmup_cache(self, stress_cmd_templ, num_threads):
        stress_queue = []
        for stress_cmd in stress_cmd_templ:
            params = {"round_robin": True, "stats_aggregate_cmds": False}
            stress_cmd_to_run = stress_cmd.replace("$threads", str(num_threads))
            params.update({'stress_cmd': stress_cmd_to_run})
            # Run all stress commands
            self.log.debug('RUNNING warm up stress cmd: %s', stress_cmd_to_run)
            stress_queue.append(self.run_stress_thread(**params))

        for stress in stress_queue:
            self.get_stress_results(queue=stress, store_results=False)
