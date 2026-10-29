import pathlib
import time
from enum import Enum
from collections import defaultdict, Counter

import json
from dataclasses import asdict, dataclass, replace
from typing import List, Union

import requests
from argus.client.base import ArgusClientError

from performance_regression_test import PerformanceRegressionTest
from sdcm.argus_results import (
    LATENCY_ERROR_THRESHOLDS,
    send_best_throughput_result_to_argus,
    send_goodput_results_to_argus,
)
from sdcm.cluster import SCYLLA_DIR
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.stress_thread import CassandraStressThread
from sdcm.utils.common import skip_optional_stage
from sdcm.utils.dataset_sizing import (
    GB,
    GIB,
    DatasetScale,
    DatasetSizingConfig,
    dataset_scale,
    scale_dataset_params,
)
from sdcm.utils.decorators import latency_calculator_decorator
from sdcm.utils.latency import (
    BestThroughputStep,
    analyze_hdr_percentiles,
    calculate_latency,
    find_best_throughput,
    p99_violations,
    step_latency,
)
from sdcm.utils.overload import GoodputResult, OverloadConfig, goodput_result
from sdcm.utils.thread_search import ThreadSearch, ThreadSearchConfig


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
        if sizing := DatasetSizingConfig.from_params(self.params.get("perf_dataset_sizing")):
            self.size_dataset(sizing)

    def size_dataset(self, sizing: DatasetSizingConfig) -> DatasetScale:
        """Scale the stress commands' datasets to the smallest DB node's RAM and data disk."""
        nodes = self.db_cluster.data_nodes
        ram_bytes = min(self._node_memory_bytes(node) for node in nodes)
        disk_bytes = min(self._node_data_disk_bytes(node) for node in nodes)
        scale = dataset_scale(ram_bytes=ram_bytes, disk_bytes=disk_bytes, config=sizing)
        self.log.info(
            "Dataset sized for %.1f GiB RAM and %.0f GB data disk (reference %s GiB, %s GB): scale x%s for RAM, "
            "x%s for disk, x%s for the preload and disk reads",
            ram_bytes / GIB,
            disk_bytes / GB,
            sizing.reference_ram_gib,
            sizing.reference_disk_gb,
            float(scale.ram),
            float(scale.disk),
            float(scale.preload),
        )
        if scale.preload < scale.ram:
            self.log.warning(
                "The data disk limits the preload to x%s while RAM is x%s: disk-only reads cover less data per GiB "
                "of RAM than on the reference node, so more of them may hit the cache",
                float(scale.preload),
                float(scale.ram),
            )
        for param, commands in scale_dataset_params(self.params, scale).items():
            self.log.debug("Sized %s: %s", param, commands)
            self.params[param] = commands
        return scale

    @staticmethod
    def _node_memory_bytes(node) -> int:
        return int(node.remoter.run("awk '/^MemTotal:/ {print $2}' /proc/meminfo").stdout.strip()) * 1024

    @staticmethod
    def _node_data_disk_bytes(node) -> int:
        return int(node.remoter.run(f"df -B1 --output=size {SCYLLA_DIR} | tail -1").stdout.strip())

    def throttle_steps(self, workload_type):
        if self.thread_search_config(workload_type):
            # The thread search picks every step at run time.
            return []
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
        if self.thread_search_config(workload_type):
            # The thread search picks every step's thread count at run time.
            return []
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
            preload_data=bool(self.params.get("perf_gradual_write_preload_data")),
            drop_keyspace=True,
            wait_no_compactions=False,
            step_duration=self.step_duration(workload_type),
            test_keyspace=keyspace,
            test_table=table,
            prepare_schema=True,
        )
        self._base_test_workflow(workload=workload, test_name="test_write_gradual_increase_load (100% writes)")

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
        if workload.prepare_schema and (prepare_stress_cmds := self.params.get("prepare_stress_cmd")):
            stress_queue = []
            for stress_cmd in prepare_stress_cmds:
                self.log.info("Preparing schema using command: %s", stress_cmd)
                params = {"stress_cmd": stress_cmd, "round_robin": True, "stats_aggregate_cmds": False}
                stress_queue.append(self.run_stress_thread(**params))

            for stress in stress_queue:
                self.get_stress_results(queue=stress, store_results=False)

            self.log.info("Schema has been prepared")
            self.run_post_prepare_cql(workload=workload)

    @staticmethod
    def _aggregate_ops_rate(results: list, num_loaders: int, num_commands: int) -> float:
        # round-robin: 1 cmd/loader, so sum == avg×loaders; additive: distinct cmds, so multiply by loaders.
        if not results:
            return 0.0

        def _op_rate(result):
            try:
                return float(result.get("op rate", 0) or 0)
            except TypeError, ValueError:
                return 0.0

        total = sum(_op_rate(r) for r in results)
        return total if num_commands == num_loaders else total * num_loaders

    @staticmethod
    def _failed_ops(results: list) -> int | None:
        """Failed operations of a step, from the stress tools' 'total errors'; None when no tool reported it."""
        errors = []
        for result in results or []:
            try:
                errors.append(int(float(result["total errors"])))
            except KeyError, TypeError, ValueError:
                continue
        return sum(errors) if errors else None

    def test_dual_engine_mixed_gradual_increase_load(self):
        """Run logstor (60%) and LSM (40%) Latte workloads together as one mixed workload.

        All four commands (logstor_write, logstor_read, lsm_write, lsm_read) run
        concurrently within the same gradual step, so what is measured is the combined
        60/40 distribution across both storage engines.  Argus reports one latency row
        per HDR tag (fn--logstor_write, fn--logstor_read, fn--lsm_write, fn--lsm_read)
        plus a summary row for the distribution as a whole.

        Test flow:
        1. Populate both tables (500M rows each, CL=ALL).  Both tables are created by the
           auto-schema call inside LatteStressThread.build_stress_cmd, which passes
           latte_schema_parameters (both engines' keyspace/table names) to 'latte schema'.
        2. Wait for compactions to quiesce.
        3. Run the mixed (write:30/read:70) gradual-throughput steps; per-engine rates are
           baked into each command via $logstor_write_rate / $lsm_read_rate / ... .

        Required config keys (provided by logstor_lsm_dual_60_40.yaml):
          stress_cmd_m - all four commands,
          perf_gradual_throttle_steps.dual_engine_mixed,
          perf_gradual_step_duration.dual_engine_mixed
        """
        workload_type = "dual_engine_mixed"
        # Each engine has its own keyspace/table, so there is no single "test table" here.
        # test_keyspace/test_table stay empty: they only feed drop_keyspace (disabled) and
        # run_post_prepare_cql (no post_prepare_cql_cmds in this config).
        workload = Workload(
            workload_type=workload_type,
            cs_cmd_tmpl=self.params.get("stress_cmd_m"),
            cs_cmd_warm_up=None,
            num_threads=self.get_num_threads_for_workload(workload_type),
            throttle_steps=self.throttle_steps(workload_type),
            preload_data=True,
            drop_keyspace=False,
            wait_no_compactions=True,
            step_duration=self.step_duration(workload_type),
            prepare_schema=False,
        )
        self._base_test_workflow(
            workload=workload, test_name="test_dual_engine_mixed_gradual_increase_load (logstor 60% / lsm 40%)"
        )

    def check_latency_during_steps(self, step):
        # TestConfig.latency_results_file() creates the file empty and only
        # latency_calculator_decorator ever fills it in, so it is still empty here whenever the
        # decorator failed to collect the results of the step. An empty file is not valid JSON:
        # read it the way the decorator itself does, to report the step without latencies rather
        # than fail the whole test on a JSONDecodeError that hides the original failure.
        # A step after one with results finds no file at all: processing those results removed it, and
        # TestConfig creates it only once. Read that as an empty file too.
        results_file = pathlib.Path(self.latency_results_file)
        latency_results = json.loads(
            (results_file.read_text(encoding="utf-8") if results_file.exists() else "").strip() or "{}"
        )
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
            stress_cmds:   List of stress command templates
            step_params:   Dict with step parameters (threads, concurrency, rate, throttle)
            step_duration: Duration for this step

        Returns the step's results and, for latency_calculator_decorator, the HDR tags to build
        the histograms of the step from.
        """
        results = []
        stress_queue = []
        for stress_cmd in stress_cmds:
            params = {"round_robin": True, "stats_aggregate_cmds": False}
            stress_cmd_to_run = stress_cmd

            # Replace placeholders from step_params dict
            for param_name, param_value in sorted(step_params.items(), key=lambda item: len(item[0]), reverse=True):
                stress_cmd_to_run = stress_cmd_to_run.replace(f"${param_name}", str(param_value))
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
        # NOTE: the returned 'hdr_tags' will be used by the 'latency_calculator_decorator' decorator.
        # Every stress thread derives its own tags from the command it actually runs, which is the
        # only place the tags of a step can come from: they are tool specific (cassandra-stress
        # WRITE/READ, scylla-bench co-fixed, latte fn--<name>) and, for cassandra-stress, they also
        # depend on the step itself, which tags a throttled run 'WRITE-rt' and an unthrottled one
        # 'WRITE-st'. Merge every queue's, or a step running a write and a read command reports the
        # writes and drops the reads.
        hdr_tags = list(
            dict.fromkeys(tag for queue in stress_queue for tag in (getattr(queue, "hdr_tags", None) or []))
        )
        self.log.debug("HDR tags of the step, collected from %s stress queue(s): %s", len(stress_queue), hdr_tags)
        return results, {"hdr_tags": hdr_tags}

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
    def run_gradual_increase_load(self, workload: Workload, stress_num, num_loaders, test_name):
        workload = self.update_num_threads_for_steps(workload=workload)
        search_config = self.thread_search_config(workload.workload_type)
        search_start_threads = (
            search_config.start_threads(
                total_shards=sum(node.scylla_shards for node in self.db_cluster.data_nodes),
                num_stress_commands=len(workload.cs_cmd_tmpl),
            )
            if search_config
            else None
        )

        if workload.cs_cmd_warm_up is not None:
            # Use the maximum thread count for warmup to ensure the cache is warmed up with the highest level of concurrency
            # Build warmup params dict
            max_threads = search_start_threads or max(workload.num_threads)
            warmup_params = {"threads": max_threads}
            # If any throttle step has concurrency, use max for warmup
            concurrency_values = [step.get("concurrency") for step in workload.throttle_steps if "concurrency" in step]
            if concurrency_values:
                warmup_params["concurrency"] = max(concurrency_values)
            self.warmup_cache(workload.cs_cmd_warm_up, warmup_params)
            # Wait for 4 minutes after warmup to let for all background processes to finish
            time.sleep(240)

        total_summary = {}
        if search_config:
            self.run_thread_search(
                workload=workload,
                search=ThreadSearch(start_threads=search_start_threads, config=search_config),
                total_summary=total_summary,
                stress_num=stress_num,
                num_loaders=num_loaders,
            )
        else:
            sequential_steps = self.get_sequential_throttle_steps(workload)
            for throttle_step_dict, num_threads, current_throttle_step in zip(
                workload.throttle_steps, workload.num_threads, sequential_steps
            ):
                total_summary.update(
                    self.run_gradual_step(
                        workload=workload,
                        throttle_step_dict=throttle_step_dict,
                        num_threads=num_threads,
                        step_name=current_throttle_step,
                        stress_num=stress_num,
                        num_loaders=num_loaders,
                    )
                )

        best_step = self.find_best_step(workload=workload, total_summary=total_summary)
        # The results log is written before each Argus send, so the run's results survive a failed send.
        self.save_total_summary_in_file(total_summary, best_step=best_step)
        self.report_best_throughput(workload=workload, best_step=best_step)
        if overload_config := self.overload_config(workload.workload_type):
            if overload_results := self.run_overload_steps(
                workload=workload,
                overload_config=overload_config,
                best_step=best_step,
                stress_num=stress_num,
                num_loaders=num_loaders,
            ):
                self.save_total_summary_in_file(total_summary, best_step=best_step, overload_results=overload_results)
                self.report_goodput(
                    workload=workload, overload_config=overload_config, best_step=best_step, results=overload_results
                )

    def overload_config(self, workload_type: str) -> OverloadConfig | None:
        """The workload's goodput-under-overload settings, or None when it runs no overload steps."""
        return OverloadConfig.from_params(self.params.get("perf_overload_steps"), workload_type)

    def run_overload_steps(
        self,
        workload: Workload,
        overload_config: OverloadConfig,
        best_step: BestThroughputStep | None,
        stress_num,
        num_loaders,
    ) -> list[GoodputResult] | None:
        """Offer factor x the best throughput and send the goodput of each factor to Argus."""
        if best_step is None or not best_step.threads:
            self.log.warning(
                "No overload steps for '%s': no best step with a known thread count to overload", workload.workload_type
            )
            return None

        # Under overload requests time out; the stress tool has to keep going and count them, not stop the step.
        stress_cmds = []
        for stress_cmd in workload.cs_cmd_tmpl:
            if stress_cmd.startswith("cassandra-stress"):
                stress_cmd = CassandraStressThread.add_errors_option(stress_cmd, ["ignore"])  # noqa: PLW2901
            else:
                self.log.warning("Overload step: '%s' may stop on errors under overload", stress_cmd.split()[0])
            stress_cmds.append(stress_cmd)
        overload_workload = replace(workload, cs_cmd_tmpl=stress_cmds)

        results = []
        for step in overload_config.steps(best_threads=best_step.threads, best_throughput=best_step.throughput):
            self.log.info(
                "Overload step %s: offering %s op/s (%s x best %s op/s) with %s threads",
                step.name,
                step.rate,
                step.factor,
                best_step.throughput,
                step.threads,
            )
            step_summary = self.run_gradual_step(
                workload=overload_workload,
                throttle_step_dict={"rate": str(step.rate)},
                num_threads=step.threads,
                step_name=step.name,
                stress_num=stress_num,
                num_loaders=num_loaders,
            )[step.name]
            result = goodput_result(
                step, step_summary, best_throughput=best_step.throughput, failed_ops=step_summary.get("failed_ops")
            )
            self.log.info(
                "Overload step %s: goodput %s op/s, %s of the best throughput, %s failed operations",
                step.name,
                result.goodput,
                result.goodput_ratio,
                result.failed_ops,
            )
            results.append(result)
        return results

    def report_goodput(
        self, workload: Workload, overload_config: OverloadConfig, best_step: BestThroughputStep, results
    ) -> None:
        """Send the goodput of every overload step to Argus."""
        self.send_to_argus(
            f"the '{workload.workload_type}' goodput under overload",
            send_goodput_results_to_argus,
            workload=workload.workload_type,
            results=results,
            best_throughput=best_step.throughput,
            min_goodput_ratio=overload_config.min_goodput_ratio,
        )

    def send_to_argus(self, what: str, send, **kwargs) -> None:
        """Send a result table to Argus. An Argus outage must not fail the test: the results log keeps the values."""
        try:
            send(argus_client=self.test_config.argus_client(), **kwargs)
        except ArgusClientError, requests.RequestException:
            self.log.warning("Could not send %s to Argus", what, exc_info=True)

    def thread_search_config(self, workload_type: str) -> ThreadSearchConfig | None:
        """The workload's adaptive thread search settings, or None when it runs its fixed steps."""
        return ThreadSearchConfig.from_params(self.params.get("perf_best_throughput_search"), workload_type)

    def run_thread_search(self, workload: Workload, search: ThreadSearch, total_summary: dict, stress_num, num_loaders):
        """Run unthrottled steps at the thread counts the search picks, until it stops."""
        p99_limit_ms = self.p99_limit_ms(workload)
        while (threads := search.next_threads()) is not None:
            # A rerun of a step that collected no results gets its own name, so its results stand alone.
            step_name = f"unthrottled_{threads}_threads{'_retry' if search.is_retry else ''}"
            step_summary = self.run_gradual_step(
                workload=workload,
                throttle_step_dict={"rate": "unthrottled"},
                num_threads=threads,
                step_name=step_name,
                stress_num=stress_num,
                num_loaders=num_loaders,
            )
            total_summary.update(step_summary)
            if (latency := step_latency(step_name, step_summary[step_name])) is None:
                passed, throughput = False, None
            else:
                p99, throughput = latency
                passed = not p99_violations(step_name, p99, p99_limit_ms)  # no HDR key over the limit
            self.log.info(
                "Thread search step %s: %s op/s, P99 within %s ms: %s", step_name, throughput, p99_limit_ms, passed
            )
            search.record(threads=threads, passed=passed, throughput=throughput)
        self.log.info(
            "Thread search for '%s' ran %s steps and stopped: %s",
            workload.workload_type,
            len(search.steps),
            search.stop_reason,
        )

    def run_gradual_step(  # noqa: PLR0913
        self, workload: Workload, throttle_step_dict: dict, num_threads, step_name: str, stress_num, num_loaders
    ) -> dict:
        """Run one step and return its summary, keyed by the step name."""
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
        step_duration = throttle_step_dict.get("duration", workload.step_duration)

        self.log.info(
            "Run cs command with rate: %s Kops; threads: %s; step name: %s; duration: %s",
            throttle_step_dict.get("rate", "unthrottled"),
            step_params["threads"],
            step_name,
            step_duration,
        )
        run_step = (latency_calculator_decorator(legend=f"Gradual test step {step_name} op/s", cycle_name=step_name))(
            self.run_step
        )
        results, _ = run_step(
            stress_cmds=workload.cs_cmd_tmpl,
            step_params=step_params,
            step_duration=step_duration,
        )
        self.log.debug("All c-s commands results collected and saved in Argus")

        summary_result = self.check_latency_during_steps(step=step_name)
        summary_result[step_name].update(
            {
                "ops_rate": self._aggregate_ops_rate(results, num_loaders, len(workload.cs_cmd_tmpl)),
                "threads": step_params["threads"],
                "failed_ops": self._failed_ops(results),
            }
        )
        if workload.drop_keyspace:
            self.drop_keyspace(keyspace_name=workload.test_keyspace)
        # We want 3 minutes (180 sec) wait between steps.
        # In case of "mixed" workflow - wait for compactions finished.
        # In case of "read" workflow -  it just will wait for 3 minutes
        if throttle_step_dict.get("wait_no_compactions", workload.wait_no_compactions):
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
        return summary_result

    def p99_limit_ms(self, workload: Workload) -> float:
        """The workload's P99 limit from perf_gradual_latency_limit, else its 'default' entry, else 10 ms."""
        latency_limit = self.params.get("perf_gradual_latency_limit") or {}
        return latency_limit.get(
            workload.workload_type, latency_limit.get("default", LATENCY_ERROR_THRESHOLDS["default"])
        )["percentile_99"]

    def find_best_step(self, workload: Workload, total_summary: dict) -> BestThroughputStep | None:
        """Find the step with the highest throughput within the P99 limit."""
        p99_limit_ms = self.p99_limit_ms(workload)
        best_step = find_best_throughput(total_summary=total_summary, p99_limit_ms=p99_limit_ms)
        if best_step:
            self.log.info(
                "Best throughput for '%s' with P99 within %s ms: %s op/s at step %s (threads: %s, P99: %s)",
                workload.workload_type,
                p99_limit_ms,
                best_step.throughput,
                best_step.name,
                best_step.threads,
                best_step.p99,
            )
        else:
            self.log.warning("No '%s' step kept P99 within %s ms", workload.workload_type, p99_limit_ms)
        return best_step

    def report_best_throughput(self, workload: Workload, best_step: BestThroughputStep | None) -> None:
        """Send the best step, or "none passed", to Argus."""
        self.send_to_argus(
            f"the '{workload.workload_type}' best throughput",
            send_best_throughput_result_to_argus,
            workload=workload.workload_type,
            best_step=best_step,
            p99_limit_ms=self.p99_limit_ms(workload),
        )

    def save_total_summary_in_file(
        self,
        total_summary,
        best_step: BestThroughputStep | None = None,
        overload_results: list[GoodputResult] | None = None,
    ):
        total_summary = total_summary | {"best_throughput": asdict(best_step) if best_step else None}
        if overload_results:
            total_summary["goodput_under_overload"] = [asdict(result) for result in overload_results]
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
