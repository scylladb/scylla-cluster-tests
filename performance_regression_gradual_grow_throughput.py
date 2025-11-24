import pathlib
import time
from enum import Enum
from collections import defaultdict, Counter

import json
from dataclasses import dataclass, replace
from typing import List, Union

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


@dataclass
class Workload:
    workload_type: str
    cs_cmd_tmpl: list
    cs_cmd_warm_up: list | None
    num_threads: Union[List[int], int]
    throttle_steps: list
    preload_data: bool
    drop_keyspace: bool
    wait_no_compactions: bool
    step_duration: str

    def __post_init__(self):
        if isinstance(self.num_threads, int):
            # If only one thread count is provided, convert it to a list
            self.num_threads = [self.num_threads]


def is_latte_command(stress_cmd: str) -> bool:
    return "latte " in stress_cmd and " run " in stress_cmd


class PerformanceRegressionPredefinedStepsTest(PerformanceRegressionTest):
    """
    This class presents new performance test that run gradual increased throughput steps.
    The test run steps with different throughput.
    Throughput of every step is fixed and defined hardcoded according to the load type (write, read
    and mixed). Last step is unthrottled.
    Latency for every step is received from cassandra-stress HDR file and reported in Argus and email.
    """

    def setUp(self):
        super().setUp()
        self.CLUSTER_SIZE = self.params.get("n_db_nodes")
        self.REPLICATION_FACTOR = 3

    def throttle_steps(self, workload_type):
        throttle_steps = self.params["perf_gradual_throttle_steps"]
        if workload_type not in throttle_steps:
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"Throttle steps for '{workload_type}' test is not defined in "
                               f"'perf_gradual_throttle_steps' parameter",
                               severity=Severity.CRITICAL).publish()
        return throttle_steps[workload_type]

    def step_duration(self, workload_type):
        step_duration = self.params["perf_gradual_step_duration"]
        if workload_type not in step_duration:
            TestFrameworkEvent(source=self.__class__.__name__,
                               message=f"Step duration for '{workload_type}' test is not defined in "
                               f"'perf_gradual_step_duration' parameter",
                               severity=Severity.CRITICAL).publish()
        return step_duration[workload_type]

    def test_mixed_gradual_increase_load(self):
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
                            wait_no_compactions=True,
                            step_duration=self.step_duration(workload_type))
        self._base_test_workflow(workload=workload,
                                 test_name="test_mixed_gradual_increase_load (read:50%,write:50%)")

    def test_write_gradual_increase_load(self):
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
                            wait_no_compactions=False,
                            step_duration=self.step_duration(workload_type))
        self._base_test_workflow(workload=workload,
                                 test_name="test_write_gradual_increase_load (100% writes)")

    def test_read_gradual_increase_load(self):
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
                            wait_no_compactions=True,
                            step_duration=self.step_duration(workload_type))
        self._base_test_workflow(workload=workload,
                                 test_name="test_read_gradual_increase_load (100% reads)")

    def _base_test_workflow(self, workload: Workload, test_name):
        stress_num = 1  # TODO: fix it to support multiple stress cmds per loader node (useful for latte)
        num_loaders = len(self.loaders.nodes)
        self.run_fstrim_on_all_db_nodes()
        # run a write workload as a preparation
        if workload.preload_data and not skip_optional_stage('perf_preload_data'):
            self.preload_data()
            if post_prepare_cql_cmds := self.params.get('post_prepare_cql_cmds'):
                self.log.debug("Execute post prepare queries: %s", post_prepare_cql_cmds)
                self._run_cql_commands(post_prepare_cql_cmds)

            self.wait_no_compactions_running(n=400, sleep_time=120)
            # In the test_read performance test, we observed that even without any write operations, compactions were occurring.
            # These compactions are a result of tablet splits and can happen several minutes after the wait_no_compactions function
            # has finished.
            # To address this, we will now verify that no tablet splits or merges are active by checking the system.tablets table.
            # The new condition for system idleness requires the resize_type column to be 'none' for all relevant tablets for a
            # continuous period of three minutes.
            self.wait_for_no_tablets_splits()
            self.run_fstrim_on_all_db_nodes()

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
            params.update({
                'stress_cmd': stress_cmd,
                'duration': self.params.get('prepare_stress_duration'),
            })
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
        return {step: {"step": step, "legend": "", "cycles": []}}

    def run_step(self, stress_cmds, current_throttle, num_threads, step_duration):
        results = []
        stress_queue = []
        for stress_cmd in stress_cmds:
            params = {"round_robin": True, "stats_aggregate_cmds": False}
            stress_cmd_to_run = stress_cmd.replace(
                "$threads", f"{num_threads}").replace("$throttle", f"{current_throttle}")
            if step_duration is not None:
                stress_cmd_to_run = stress_cmd_to_run.replace("$duration", step_duration)
            params.update({'stress_cmd': stress_cmd_to_run})
            # Run all stress commands
            self.log.debug('RUNNING stress cmd: %s', stress_cmd_to_run)
            stress_queue.append(self.run_stress_thread(**params))

        for stress in stress_queue:
            results.extend(self.get_stress_results(queue=stress, store_results=False))
            self.log.debug("One c-s command results: %s", results[-1])
        # NOTE: 'stress_queue' will be used by the 'latency_calculator_decorator' decorator
        return results, stress_queue

    def drop_keyspace(self):
        self.log.debug(f'Drop keyspace {"keyspace1"}')
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(f'DROP KEYSPACE IF EXISTS {"keyspace1"};')

    @staticmethod
    def _step_names(step_names, total_counts):
        """
        Helper function to generate names based on throttle_steps and num_threads.
        Example:
            step_names = ["100", "unthrottled", "unthrottled"]
            total_counts = {"unthrottled": 2, "100": 1}
            Result: ["100", "unthrottled_1", "unthrottled_2"]
        """
        step_seen = {}
        result = []
        for name in step_names:
            step_seen[name] = step_seen.get(name, 0) + 1
            if total_counts[name] > 1:
                result.append(f"{name}_{step_seen[name]}")
            else:
                result.append(name)
        return result

    def get_sequential_throttle_steps(self, workload: Workload):
        """
        Returns a list of throttle step names based on throttle_steps and num_threads.
        - If all num_threads are the same, use throttle_step (with count if repeated).
        - If num_threads are unique per step, use '<throttle_step>_<num_threads>_threads'.
          If this combination repeats, append a count.
        """
        throttle_steps = workload.throttle_steps
        num_threads = workload.num_threads

        if len(set(num_threads)) == 1:
            # All thread counts are the same, only add count for repeated steps
            step_names = throttle_steps
        else:
            # Each step has a unique thread count, use <throttle_step>_<num_threads>_threads
            step_names = [f"{step}_{threads}_threads" for step, threads in zip(throttle_steps, num_threads)]

        total_counts = Counter(step_names)

        return self._step_names(step_names, total_counts)

    @staticmethod
    def update_num_threads_for_steps(workload: Workload):
        """
        Ensures that the `num_threads` list in the workload matches the length of `throttle_steps`.
        If only one thread count is provided but multiple throttle steps exist, the single value is repeated
        to match the number of steps.

        Args:
            workload (Workload): The workload namedtuple containing `num_threads` and `throttle_steps`.

        Returns:
            Workload: A new Workload instance with an updated `num_threads` list if needed.
        """
        if len(workload.num_threads) == 1 and len(workload.throttle_steps) > 1:
            workload = replace(workload, num_threads=[workload.num_threads[0]] * len(workload.throttle_steps))
        return workload

    # pylint: disable=too-many-arguments,too-many-locals
    def run_gradual_increase_load(self, workload: Workload, stress_num, num_loaders, test_name):  # noqa: PLR0914
        workload = self.update_num_threads_for_steps(workload=workload)

        if workload.cs_cmd_warm_up is not None:
            # Use the maximum thread count for warmup to ensure the cache is warmed up with the highest level of concurrency
            self.warmup_cache(workload.cs_cmd_warm_up, max(workload.num_threads))
            # Wait for 4 minutes after warmup to let for all background processes to finish
            time.sleep(240)

        if not self.exists():
            self.log.debug("Create test statistics in ES")
            self.create_test_stats(sub_type=workload.workload_type, doc_id_with_timestamp=False)
        total_summary = {}

        sequential_steps = self.get_sequential_throttle_steps(workload)
        for throttle_step, num_threads, current_throttle_step in zip(workload.throttle_steps, workload.num_threads, sequential_steps):
            self.log.info("Run cs command with rate: %s Kops; threads: %s; step name: %s", throttle_step, num_threads,
                          current_throttle_step)
            if throttle_step == "unthrottled":
                current_throttle = ""
            else:
                throttle_value = int(int(throttle_step) // (num_loaders * stress_num))
                if is_latte_command(workload.cs_cmd_tmpl[0]):
                    current_throttle = f"--rate={throttle_value}"
                else:
                    current_throttle = f"fixed={throttle_value}/s"
            run_step = ((latency_calculator_decorator(legend=f"Gradual test step {current_throttle_step} op/s",
                                                      cycle_name=current_throttle_step))(self.run_step))
            results, _ = run_step(stress_cmds=workload.cs_cmd_tmpl, current_throttle=current_throttle,
                                  num_threads=num_threads, step_duration=workload.step_duration)
            self.log.debug("All c-s commands results collected and saved in Argus")

            calculate_result = self._calculate_average_max_latency(results)
            self.update_test_details()
            summary_result = self.check_latency_during_steps(step=current_throttle_step)
            summary_result[current_throttle_step].update({"ops_rate": calculate_result["op rate"] * num_loaders})
            total_summary.update(summary_result)
            if workload.drop_keyspace:
                self.drop_keyspace()
            # We want 3 minutes (180 sec) wait between steps.
            # In case of "mixed" workflow - wait for compactions finished.
            # In case of "read" workflow -  it just will wait for 3 minutes
            if workload.wait_no_compactions:
                if (wait_time := self.wait_no_compactions_running()[0]) < 180:
                    time.sleep(180 - wait_time)
                self.log.info("All compactions are finished")

                # In the test_read performance test, we observed that even without any write operations, compactions were occurring.
                # These compactions are a result of tablet splits and can happen several minutes after the wait_no_compactions function
                # has finished.
                # To address this, we will now verify that no tablet splits or merges are active by checking the system.tablets table.
                # The new condition for system idleness requires the resize_type column to be 'none' for all relevant tablets for a
                # continuous period of three minutes.
                self.wait_for_no_tablets_splits()

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
            email_recipients=self.params.get('email_recipients'))
        # Keep next 2 lines for debug purpose
        self.log.debug("es_index: %s", self._test_index)
        self.log.debug("total_summary: %s", total_summary)
        is_gce = bool(self.params.get('cluster_backend') == 'gce')
        try:
            perf_analyzer.check_regression(test_id=self._test_id,
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
