import pathlib
import time
from contextlib import contextmanager, nullcontext
from enum import Enum
from collections import defaultdict, Counter

import json
from dataclasses import dataclass, replace
from typing import List, Union

from performance_regression_test import PerformanceRegressionTest
from sdcm.rest.task_profiler_client import TaskProfilerClient
from sdcm.utils.common import skip_optional_stage
from sdcm.utils.parallel_object import ParallelObject
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
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
    prepare_schema: bool
    test_keyspace: str = ""
    test_table: str = ""
    task_profiler: bool = False

    def __post_init__(self):
        if isinstance(self.num_threads, int):
            # If only one thread count is provided, convert it to a list
            self.num_threads = [self.num_threads]

        # Normalize throttle_steps to dict format for internal use
        # Convert string/int steps to dict: '100000' -> {rate: '100000'}
        normalized_steps = []
        for step in self.throttle_steps:
            if isinstance(step, dict):
                normalized_steps.append(step)
            elif isinstance(step, (str, int)):
                # String or int throttle value - convert to dict with rate only
                normalized_steps.append({"rate": str(step) if isinstance(step, int) else step})
            else:
                # Should not happen due to validation, but handle gracefully
                normalized_steps.append({"rate": str(step)})
        self.throttle_steps = normalized_steps


def is_latte_command(stress_cmd: Union[str, list]) -> bool:
    """Check if stress command(s) use latte tool.

    Args:
        stress_cmd: Single command string or list of command strings

    Returns:
        True if any command contains 'latte ' and ' run ', False otherwise
    """
    if isinstance(stress_cmd, str):
        stress_cmd = [stress_cmd]
    return any("latte " in cmd and " run " in cmd for cmd in stress_cmd)


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
        self.CLUSTER_SIZE = sum(self.params.get("n_db_nodes"))
        self.REPLICATION_FACTOR = 3

    def throttle_steps(self, workload_type):
        throttle_steps = self.params["perf_gradual_throttle_steps"]
        if workload_type not in throttle_steps:
            TestFrameworkEvent(
                source=self.__class__.__name__,
                message=f"Throttle steps for '{workload_type}' test is not defined in "
                f"'perf_gradual_throttle_steps' parameter",
                severity=Severity.CRITICAL,
            ).publish()
        return throttle_steps[workload_type]

    def get_num_threads_for_workload(self, workload_type):
        """
        Get num_threads for a workload type.

        First tries to extract threads from perf_gradual_throttle_steps dict entries.
        Falls back to perf_gradual_threads if throttle steps contain only rate values.

        Args:
            workload_type: The workload type (read, write, mixed, etc.)

        Returns:
            List of thread counts or single thread count
        """
        throttle_steps = self.throttle_steps(workload_type)

        # Check if any throttle step has threads defined (dict format)
        threads_from_steps = []
        has_threads_in_steps = False

        for step in throttle_steps:
            if isinstance(step, dict) and "threads" in step:
                threads_from_steps.append(step["threads"])
                has_threads_in_steps = True
            else:
                # Step is string/int (rate only) - will need fallback
                threads_from_steps.append(None)

        if has_threads_in_steps:
            # At least some steps have threads defined
            # For steps without threads, use perf_gradual_threads as fallback
            perf_gradual_threads = self.params.get("perf_gradual_threads")
            fallback_threads = perf_gradual_threads[workload_type] if perf_gradual_threads else None

            result = [
                thread_count if thread_count is not None else fallback_threads for thread_count in threads_from_steps
            ]
            if any(t is None for t in result):
                raise ValueError(
                    f"Some steps for '{workload_type}' lack 'threads' and no perf_gradual_threads fallback is defined"
                )
            return result
        else:
            # No steps have threads - use perf_gradual_threads parameter
            return self.params["perf_gradual_threads"][workload_type]

    def step_duration(self, workload_type):
        step_duration = self.params["perf_gradual_step_duration"]
        if workload_type not in step_duration:
            TestFrameworkEvent(
                source=self.__class__.__name__,
                message=f"Step duration for '{workload_type}' test is not defined in "
                f"'perf_gradual_step_duration' parameter",
                severity=Severity.CRITICAL,
            ).publish()
        return step_duration[workload_type]

    def get_test_table_name(self, stress_cmds) -> (str, str):
        """Resolve keyspace and table name for a stress command.

        For all stress tools (cassandra-stress, scylla-bench, cql-stress-cassandra-stress)
        the values are read from ``perf_stress_keyspace`` / ``perf_stress_table`` SCT config options.

        For latte commands the values are first looked up in ``perf_stress_keyspace`` /
        ``perf_stress_table``, then in ``latte_schema_parameters`` (``keyspace`` / ``table`` keys).
        """
        stress_cmd = stress_cmds[0]
        stress_tool = stress_cmd.split(" ")[0]

        keyspace = self.params.get("perf_stress_keyspace") or ""
        table = self.params.get("perf_stress_table") or ""

        if is_latte_command(stress_cmd) and (not keyspace or not table):
            if latte_schema_parameters := self.params.get("latte_schema_parameters"):
                if not keyspace:
                    keyspace = latte_schema_parameters.get("keyspace", "")
                if not table:
                    table = latte_schema_parameters.get("table", "")

        if not keyspace:
            raise ValueError(
                f"'perf_stress_keyspace' (or 'latte_schema_parameters.keyspace' for latte) is required for "
                f"'{stress_tool}' gradual performance tests. Please add it to your test YAML configuration."
            )
        if not table:
            raise ValueError(
                f"'perf_stress_table' (or 'latte_schema_parameters.table' for latte) is required for "
                f"'{stress_tool}' gradual performance tests. Please add it to your test YAML configuration."
            )
        return keyspace, table

    def test_mixed_gradual_increase_load(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a mixed workload with gradual increase load
        """
        workload_type = "mixed"
        keyspace, table = self.get_test_table_name(self.params.get("stress_cmd_m"))
        workload = Workload(
            workload_type=workload_type,
            cs_cmd_tmpl=self.params.get("stress_cmd_m"),
            cs_cmd_warm_up=self.params.get("stress_cmd_cache_warmup"),
            num_threads=self.get_num_threads_for_workload(workload_type),
            throttle_steps=self.throttle_steps(workload_type),
            preload_data=True,
            drop_keyspace=False,
            wait_no_compactions=True,
            step_duration=self.step_duration(workload_type),
            test_keyspace=keyspace,
            test_table=table,
            prepare_schema=False,
        )
        self._base_test_workflow(workload=workload, test_name="test_mixed_gradual_increase_load (read:50%,write:50%)")

    def test_write_gradual_increase_load(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a write workload with gradual increase load
        """
        workload_type = "write"
        keyspace, table = self.get_test_table_name(self.params.get("stress_cmd_w"))
        workload = Workload(
            workload_type=workload_type,
            cs_cmd_tmpl=self.params.get("stress_cmd_w"),
            cs_cmd_warm_up=None,
            num_threads=self.get_num_threads_for_workload(workload_type),
            throttle_steps=self.throttle_steps(workload_type),
            preload_data=False,
            drop_keyspace=True,
            wait_no_compactions=False,
            step_duration=self.step_duration(workload_type),
            test_keyspace=keyspace,
            test_table=table,
            prepare_schema=True,
        )
        self._base_test_workflow(workload=workload, test_name="test_write_gradual_increase_load (100% writes)")

    def test_write_gradual_increase_load_with_task_profiler(self):
        """Run write workload with task profiler enabled.

        Identical to test_write_gradual_increase_load but brackets each
        throttle step with task profiler start/stop calls on all DB
        nodes. The stop call also dumps per-shard profiling data.
        Requires a Scylla build from
        https://github.com/Jadw1/scylla/tree/sc_perf_with_profile
        """
        workload_type = "write"
        keyspace, table = self.get_test_table_name(self.params.get("stress_cmd_w"))
        workload = Workload(
            workload_type=workload_type,
            cs_cmd_tmpl=self.params.get("stress_cmd_w"),
            cs_cmd_warm_up=None,
            num_threads=self.get_num_threads_for_workload(workload_type),
            throttle_steps=self.throttle_steps(workload_type),
            preload_data=False,
            drop_keyspace=True,
            wait_no_compactions=False,
            step_duration=self.step_duration(workload_type),
            test_keyspace=keyspace,
            test_table=table,
            prepare_schema=True,
            task_profiler=True,
        )
        self._base_test_workflow(
            workload=workload,
            test_name="test_write_gradual_increase_load_with_task_profiler (100% writes)",
        )

    def test_read_gradual_increase_load(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a read workload with gradual increase load
        """
        workload_type = "read"
        keyspace, table = self.get_test_table_name(self.params.get("stress_cmd_r"))
        workload = Workload(
            workload_type=workload_type,
            cs_cmd_tmpl=self.params.get("stress_cmd_r"),
            cs_cmd_warm_up=self.params.get("stress_cmd_cache_warmup"),
            num_threads=self.get_num_threads_for_workload(workload_type),
            throttle_steps=self.throttle_steps(workload_type),
            preload_data=True,
            drop_keyspace=False,
            wait_no_compactions=True,
            step_duration=self.step_duration(workload_type),
            test_keyspace=keyspace,
            test_table=table,
            prepare_schema=False,
        )
        self._base_test_workflow(workload=workload, test_name="test_read_gradual_increase_load (100% reads)")

    def test_read_disk_only_gradual_increase_load(self):
        """
        Test steps:

        1. Run a write workload as a preparation
        2. Run a read workload with gradual increase load that reads data only from disk (no cache hits)
        """
        workload_type = "read_disk_only"
        keyspace, table = self.get_test_table_name(self.params.get("stress_cmd_read_disk"))
        workload = Workload(
            workload_type=workload_type,
            cs_cmd_tmpl=self.params.get("stress_cmd_read_disk"),
            cs_cmd_warm_up=None,
            num_threads=self.get_num_threads_for_workload(workload_type),
            throttle_steps=self.throttle_steps(workload_type),
            preload_data=True,
            drop_keyspace=False,
            wait_no_compactions=True,
            step_duration=self.step_duration(workload_type),
            test_keyspace=keyspace,
            test_table=table,
            prepare_schema=False,
        )
        self._base_test_workflow(
            workload=workload, test_name="test_read_disk_only_gradual_increase_load (100% reads from disk)"
        )

    def _base_test_workflow(self, workload: Workload, test_name):
        stress_num = 1  # TODO: fix it to support multiple stress cmds per loader node (useful for latte)
        num_loaders = len(self.loaders.nodes)
        self.run_fstrim_on_all_db_nodes()
        # run a write workload as a preparation
        if workload.preload_data and not skip_optional_stage("perf_preload_data"):
            self.preload_data()
            self.run_post_prepare_cql(workload=workload)
            self.wait_no_compactions_running(n=400, sleep_time=120)
            # In the test_read performance test, we observed that even without any write operations, compactions were occurring.
            # These compactions are a result of tablet splits and can happen several minutes after the wait_no_compactions function
            # has finished.
            # To address this, we will now verify that no tablet splits or merges are active by checking the system.tablets table.
            # The new condition for system idleness requires the resize_type column to be 'none' for all relevant tablets for a
            # continuous period of three minutes.
            self.wait_for_no_tablets_splits()
            self.run_fstrim_on_all_db_nodes()

        self.run_gradual_increase_load(
            workload=workload, stress_num=stress_num, num_loaders=num_loaders, test_name=test_name
        )

    def run_post_prepare_cql(self, workload):
        if post_prepare_cql_cmds := self.params.get("post_prepare_cql_cmds"):
            test_table = f"{workload.test_keyspace}.{workload.test_table}"
            matching_cmds = []
            for cmd in post_prepare_cql_cmds:
                if test_table not in cmd.lower():
                    self.log.error(
                        "Post prepare cql command '%s' does not match test table '%s', skipping it.",
                        cmd.lower(),
                        test_table,
                    )
                else:
                    matching_cmds.append(cmd)
            if matching_cmds:
                self.log.debug("Execute post prepare queries: %s", matching_cmds)
                self._run_cql_commands(matching_cmds)

    def preload_data(self, compaction_strategy=None):
        if self.params.get("pre_create_keyspace"):
            self._pre_create_keyspace()

        population_commands: list = self.params.get("prepare_write_cmd")

        self.log.info("Population c-s commands: %s", population_commands)
        # Check if it should be round_robin across loaders
        params = {}
        stress_queue = []
        if self.params.get("round_robin"):
            self.log.debug("Populating data using round_robin")
            params.update({"stress_num": 1, "round_robin": True})
        if compaction_strategy:
            self.log.debug("Next compaction strategy will be used %s", compaction_strategy)
            params["compaction_strategy"] = compaction_strategy

        for stress_cmd in population_commands:
            params.update(
                {
                    "stress_cmd": stress_cmd,
                    "duration": self.params.get("prepare_stress_duration"),
                }
            )
            # Run all stress commands
            params.update(dict(stats_aggregate_cmds=False))
            self.log.debug(f"RUNNING stress cmd: {stress_cmd}")
            stress_queue.append(self.run_stress_thread(**params))

        for stress in stress_queue:
            self.get_stress_results(queue=stress, store_results=False)

        self.log.info("Dataset has been populated")

    def prepare_schema(self, workload: Workload):
        if workload.prepare_schema and self.params.get("pre_create_keyspace"):
            self._pre_create_keyspace()
        elif workload.prepare_schema and (prepare_stress_cmds := self.params.get("prepare_stress_cmd")):
            stress_queue = []
            for stress_cmd in prepare_stress_cmds:
                self.log.info("Preparing schema using command: %s", stress_cmd)
                params = {"stress_cmd": stress_cmd, "round_robin": True, "stats_aggregate_cmds": False}
                stress_queue.append(self.run_stress_thread(**params))

            for stress in stress_queue:
                self.get_stress_results(queue=stress, store_results=False)

            self.log.info("Schema has been prepared")
            self.run_post_prepare_cql(workload=workload)

    def check_latency_during_steps(self, step):
        with open(self.latency_results_file, encoding="utf-8") as file:
            latency_results = json.load(file)
        self.log.debug(
            "Step %s: latency_results were loaded from file %s and its result is %s",
            step,
            self.latency_results_file,
            latency_results,
        )
        if latency_results:
            latency_results[step]["step"] = step
            latency_results[step] = calculate_latency(latency_results[step])
            latency_results = analyze_hdr_percentiles(latency_results)
            pathlib.Path(self.latency_results_file).unlink()
            self.log.debug("collected latency values are: %s", latency_results)
            return latency_results
        return {step: {"step": step, "legend": "", "cycles": []}}

    def run_step(self, stress_cmds, step_params, step_duration):
        """
        Run a single stress step with parameters from step_params dict.

        Args:
            stress_cmds: List of stress command templates
            step_params: Dict with step parameters (threads, concurrency, rate, throttle)
            step_duration: Duration for this step
        """
        results = []
        stress_queue = []
        for stress_cmd in stress_cmds:
            params = {"round_robin": True, "stats_aggregate_cmds": False}
            stress_cmd_to_run = stress_cmd

            # Replace placeholders from step_params dict
            if "threads" in step_params:
                stress_cmd_to_run = stress_cmd_to_run.replace("$threads", str(step_params["threads"]))
            if "concurrency" in step_params:
                stress_cmd_to_run = stress_cmd_to_run.replace("$concurrency", str(step_params["concurrency"]))
            if "throttle" in step_params:
                stress_cmd_to_run = stress_cmd_to_run.replace("$throttle", step_params["throttle"])
            if step_duration is not None:
                # For latte, --duration accepts an integer iteration count (number of cycles to run)
                # rather than a time string like cassandra-stress
                stress_cmd_to_run = stress_cmd_to_run.replace("$duration", step_duration)

            params.update({"stress_cmd": stress_cmd_to_run})
            # Run all stress commands
            self.log.debug("RUNNING stress cmd: %s", stress_cmd_to_run)
            stress_queue.append(self.run_stress_thread(**params))

        for stress in stress_queue:
            results.extend(self.get_stress_results(queue=stress, store_results=False))
            self.log.debug("One c-s command results: %s", results[-1])
        # NOTE: 'stress_queue' will be used by the 'latency_calculator_decorator' decorator
        return results, stress_queue

    def drop_keyspace(self, keyspace_name):
        self.log.debug(f"Drop keyspace {keyspace_name}")
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_name};")
        self.log.debug("Keyspace '%s' has been dropped", keyspace_name)

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

        throttle_steps are now dicts, so we extract the 'rate' value for naming.
        """
        throttle_steps = workload.throttle_steps
        num_threads = workload.num_threads

        # Extract rate values for step names (throttle_steps are now dicts)
        step_rate_values = [step.get("rate", "unthrottled") for step in throttle_steps]

        if len(set(num_threads)) == 1:
            # All thread counts are the same, only add count for repeated steps
            step_names = step_rate_values
        else:
            # Each step has a unique thread count, use <throttle_step>_<num_threads>_threads
            step_names = [f"{rate}_{threads}_threads" for rate, threads in zip(step_rate_values, num_threads)]

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

    @staticmethod
    def current_throttle(throttle_step_dict, num_loaders, stress_num, stress_cmd):
        """
        Generate throttle parameter from step dict.

        Args:
            throttle_step_dict: Dict with step parameters (must have 'rate' key)
            num_loaders: Number of loader nodes
            stress_num: Number of stress commands per loader
            stress_cmd: Stress command to determine format

        Returns:
            str: Formatted throttle parameter for the stress command
        """
        rate = throttle_step_dict.get("rate", "unthrottled")
        if rate == "unthrottled":
            return ""

        throttle_value = int(int(rate) // (num_loaders * stress_num))
        if is_latte_command(stress_cmd):
            current_throttle = f"--rate={throttle_value}"
        elif stress_cmd.startswith("scylla-bench"):
            current_throttle = f"-max-rate={throttle_value}"
        else:
            # cassandra-stress and cql-cassandra-stress
            current_throttle = f"fixed={throttle_value}/s"

        return current_throttle

    # pylint: disable=too-many-arguments,too-many-locals
    def run_gradual_increase_load(self, workload: Workload, stress_num, num_loaders, test_name):  # noqa: PLR0914
        workload = self.update_num_threads_for_steps(workload=workload)

        if workload.cs_cmd_warm_up is not None:
            # Use the maximum thread count for warmup to ensure the cache is warmed up with the highest level of concurrency
            # Build warmup params dict
            max_threads = max(workload.num_threads)
            warmup_params = {"threads": max_threads}
            # If any throttle step has concurrency, use max for warmup
            concurrency_values = [step.get("concurrency") for step in workload.throttle_steps if "concurrency" in step]
            if concurrency_values:
                warmup_params["concurrency"] = max(concurrency_values)
            self.warmup_cache(workload.cs_cmd_warm_up, warmup_params)
            # Wait for 4 minutes after warmup to let for all background processes to finish
            time.sleep(240)

        total_summary = {}

        sequential_steps = self.get_sequential_throttle_steps(workload)
        for throttle_step_dict, num_threads, current_throttle_step in zip(
            workload.throttle_steps, workload.num_threads, sequential_steps
        ):
            self.prepare_schema(workload=workload)

            # Build step_params dict from throttle_step_dict and num_threads
            step_params = dict(throttle_step_dict)  # Copy the dict

            # Add threads from num_threads if not already in step dict
            if "threads" not in step_params:
                step_params["threads"] = num_threads

            # Generate throttle parameter from rate
            step_params["throttle"] = self.current_throttle(
                throttle_step_dict, num_loaders, stress_num, workload.cs_cmd_tmpl[0]
            )

            self.log.info(
                "Run cs command with rate: %s Kops; threads: %s; step name: %s",
                throttle_step_dict.get("rate", "unthrottled"),
                step_params["threads"],
                current_throttle_step,
            )

            with self._task_profiler_step(current_throttle_step) if workload.task_profiler else nullcontext():
                run_step = (
                    latency_calculator_decorator(
                        legend=f"Gradual test step {current_throttle_step} op/s", cycle_name=current_throttle_step
                    )
                )(self.run_step)
                results, _ = run_step(
                    stress_cmds=workload.cs_cmd_tmpl,
                    step_params=step_params,
                    step_duration=workload.step_duration,
                )

            self.log.debug("All c-s commands results collected and saved in Argus")

            calculate_result = self._calculate_average_max_latency(results)
            summary_result = self.check_latency_during_steps(step=current_throttle_step)
            summary_result[current_throttle_step].update({"ops_rate": calculate_result["op rate"] * num_loaders})
            total_summary.update(summary_result)
            if workload.drop_keyspace:
                self.drop_keyspace(keyspace_name=workload.test_keyspace)
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

    @contextmanager
    def _task_profiler_step(self, step_name: str):
        """Bracket a single throttle step with task profiler start/stop+collect.

        Starts the profiler on all DB nodes and always runs stop+collect in the
        ``finally`` block, so any profiler that was started (even when a later
        node's startup fails) is stopped and cannot skew subsequent steps.
        Startup is inside the protected boundary and startup failures are
        tolerated per node: nodes that started successfully are stopped, and a
        failure to start on some nodes does not abort the whole step.
        """
        try:
            self._task_profiler_start_all_nodes()
            yield
        finally:
            try:
                self._task_profiler_stop_and_collect(step_name=step_name)
            except Exception:  # noqa: BLE001
                self.log.error("Task profiler stop/collect failed for step %s", step_name, exc_info=True)

    def _task_profiler_start_all_nodes(self, sampling_interval_ms: int = 10):
        """Start task profiler on all DB nodes in parallel.

        Startup failures are tolerated so a single node that fails to start does
        not leave already-started profilers running on the other nodes; the
        ``finally`` in :meth:`_task_profiler_step` still stops every node.
        """

        def start_on_node(node):
            self.log.info("Starting task profiler on %s", node.name)
            TaskProfilerClient(node).start(sampling_interval_ms=sampling_interval_ms)

        results = ParallelObject(objects=self.db_cluster.nodes, timeout=120).run(start_on_node, ignore_exceptions=True)
        for result in results:
            if result.exc is not None:
                self.log.warning("Failed to start task profiler on %s: %s", result.obj.name, result.exc)

    def _task_profiler_stop_and_collect(self, step_name: str):
        """Stop profiler, write dump files, verify, archive, download, and clean up for one step.

        For each node in parallel:
        1. Create the dump directory on the remote node.
        2. Call the REST stop endpoint which stops profiling AND writes per-shard
           folded-stack files to the given path.
        3. Verify at least one dump file exists and is non-empty.
        4. Create a tar.gz archive on the remote node.
        5. Download the archive into node.logdir/ via receive_files.
        6. Clean up remote files.
        """

        def stop_and_collect_on_node(node):
            run_id = self.test_id  # unique per test run, prevents collisions on reused nodes
            base_dir = f"/tmp/async_profile_{run_id}"  # noqa: S108 - remote scratch dir on ephemeral test node
            dir_name = f"{step_name}_{node.name}"
            dump_dir = f"{base_dir}/{dir_name}"
            dump_filename = f"{dump_dir}/async_profile_{step_name}"
            archive_name = f"async_profiles_{run_id}_{step_name}_{node.name}.tar.gz"
            remote_archive = f"/tmp/{archive_name}"  # noqa: S108 - remote scratch dir on ephemeral test node

            # mkdir must happen first (stop writes files into this dir).
            # If mkdir fails, stop profiler with a fallback path so it
            # doesn't keep running and skew subsequent steps.
            try:
                node.remoter.run(f"mkdir -p {dump_dir} && chmod 777 {dump_dir}")
            except Exception:
                self.log.warning("mkdir failed on %s, stopping profiler with fallback path", node.name)
                try:
                    TaskProfilerClient(node).stop(filename=f"{base_dir}/fallback_{step_name}")
                except Exception:  # noqa: BLE001
                    self.log.error("Fallback stop also failed on %s, profiler may still be running", node.name)
                raise

            # Stop profiler + write dump files into the prepared directory
            self.log.info("Stopping task profiler on %s, dumping to %s", node.name, dump_filename)
            TaskProfilerClient(node).stop(filename=dump_filename)

            # Verify dumps exist
            result = node.remoter.run(
                f"ls {dump_dir}/async_profile_* 2>/dev/null | head -1",
                ignore_status=True,
            )
            if result.failed or not result.stdout.strip():
                raise FileNotFoundError(f"No task profiler dump files found on {node.name} under {dump_dir}/")

            # Verify at least the first file is non-empty
            first_file = result.stdout.strip()
            size_result = node.remoter.run(f"stat -c %s {first_file}")
            file_size = int(size_result.stdout.strip())
            if file_size == 0:
                raise ValueError(f"Task profiler dump file {first_file} on {node.name} is empty")

            # Count total dump files
            count_result = node.remoter.run(f"ls -1 {dump_dir}/async_profile_* | wc -l")
            file_count = int(count_result.stdout.strip())
            self.log.info(
                "Found %d task profiler dump files on %s for step %s",
                file_count,
                node.name,
                step_name,
            )

            # Archive
            node.remoter.run(f"tar -czf {remote_archive} -C {base_dir} {dir_name}/")

            # Download into node.logdir (where FileLog search_locally finds it)
            node.remoter.receive_files(
                src=remote_archive,
                dst=node.logdir,
            )
            self.log.info(
                "Downloaded %s to %s/%s",
                archive_name,
                node.logdir,
                archive_name,
            )

            # Cleanup remote files
            node.remoter.run(f"rm -rf {dump_dir} {remote_archive}")

        results = ParallelObject(objects=self.db_cluster.nodes, timeout=600).run(
            stop_and_collect_on_node, ignore_exceptions=True
        )
        for result in results:
            if result.exc is not None:
                self.log.warning("Failed to stop/collect task profiler on %s: %s", result.obj.name, result.exc)

    def save_total_summary_in_file(self, total_summary):
        total_summary_json = json.dumps(total_summary, indent=4, separators=(", ", ": "))
        self.log.debug("---------------------------------")
        self.log.debug("Final table with results: \n %s", total_summary_json)
        self.log.debug("---------------------------------")

        filename = f"{self.logdir}/result_gradual_increase.log"
        with open(filename, "w", encoding="utf-8") as res_file:
            res_file.write(total_summary_json)

    def _calculate_average_max_latency(self, results):
        status = defaultdict(float).fromkeys(results[0].keys(), 0.0)
        max_latency = defaultdict(list)

        for result in results:
            for key in status:
                try:
                    status[key] += float(result.get(key, 0.0)) if result.get(key) else 0.0
                    if key in ["latency 95th percentile", "latency 99th percentile"]:
                        max_latency[f"{key} max"].append(float(result.get(key, 0.0)))
                except ValueError:
                    continue
                except TypeError as error:
                    self.log.info("TypeError for key %s with value %s: %s", key, result.get(key), error)
                    continue

        for key in status:
            status[key] = round(status[key] / len(results), 2)

        for key, latency in max_latency.items():
            status[key] = max(latency)

        return status

    def warmup_cache(self, stress_cmd_templ, params_dict):
        """
        Warm up cache with stress commands.

        Args:
            stress_cmd_templ: List of stress command templates
            params_dict: Dict with parameters (threads, concurrency, etc.)
        """
        stress_queue = []
        for stress_cmd in stress_cmd_templ:
            params = {"round_robin": True, "stats_aggregate_cmds": False}
            stress_cmd_to_run = stress_cmd

            # Replace placeholders from params_dict
            if "threads" in params_dict:
                stress_cmd_to_run = stress_cmd_to_run.replace("$threads", str(params_dict["threads"]))
            if "concurrency" in params_dict:
                stress_cmd_to_run = stress_cmd_to_run.replace("$concurrency", str(params_dict["concurrency"]))

            params.update({"stress_cmd": stress_cmd_to_run})
            # Run all stress commands
            self.log.debug("RUNNING warm up stress cmd: %s", stress_cmd_to_run)
            stress_queue.append(self.run_stress_thread(**params))

        for stress in stress_queue:
            self.get_stress_results(queue=stress, store_results=False)
