import pathlib
import time
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from collections import defaultdict, Counter

import json
from dataclasses import dataclass, replace
from typing import List, Union

from performance_regression_test import PerformanceRegressionTest
from sdcm.stress.latte_thread import find_latte_fn_names
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.common import skip_optional_stage
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
            except (TypeError, ValueError):
                return 0.0

        total = sum(_op_rate(r) for r in results)
        return total if num_commands == num_loaders else total * num_loaders

    # ─── Dual-engine orchestration (LSM + logstor concurrent, separate metrics) ───

    def _dispatch_stress_cmds(self, stress_cmds, step_params, step_duration):
        """Dispatch stress commands non-blocking and return the live stress queues.

        Does NOT wait for results — only launches stress threads.  Callers dispatch
        both engines' commands before waiting on any, ensuring true concurrency.

        Args:
            stress_cmds:   List of stress command templates with $-placeholders.
            step_params:   Dict of placeholder values (threads, rates, duration …).
            step_duration: Duration string to substitute for $duration.

        Returns:
            List of stress queue objects (one per command).
        """
        stress_queue = []
        for stress_cmd in stress_cmds:
            params = {"round_robin": True, "stats_aggregate_cmds": False}
            stress_cmd_to_run = stress_cmd

            for param_name, param_value in sorted(step_params.items(), key=lambda item: len(item[0]), reverse=True):
                stress_cmd_to_run = stress_cmd_to_run.replace(f"${param_name}", str(param_value))
            if step_duration is not None:
                stress_cmd_to_run = stress_cmd_to_run.replace("$duration", step_duration)

            params.update({"stress_cmd": stress_cmd_to_run})
            self.log.debug("DISPATCHING stress cmd: %s", stress_cmd_to_run)
            stress_queue.append(self.run_stress_thread(**params))
        return stress_queue

    def _await_stress_queues(self, stress_queue: list) -> list:
        """Collect results from already-running stress queues.

        Shared by run_step (single-engine) and _await_engine_results (dual-engine).
        """
        results = []
        for stress in stress_queue:
            results.extend(self.get_stress_results(queue=stress, store_results=False))
        return results

    def _await_engine_results(
        self,
        engine_name: str,
        queues: list,
        hdr_tags: list[str],
    ) -> tuple[list, list]:
        """Await results from already-running stress queues for one engine.

        This method is designed to be wrapped with latency_calculator_decorator
        *after* both engines' stress threads have been dispatched non-blocking.
        Because the threads are already running when this is called, the decorator's
        start..end interval encloses the actual result-collection wait (which spans
        the live ~60-minute workload), not a near-zero no-op.

        Both engines' threads are dispatched before either _await_engine_results
        call, so the workloads execute concurrently on the cluster.  The two
        decorated await calls are themselves run concurrently (in separate threads)
        so each decorator's start..end window spans that engine's real wait instead
        of one call's window being squeezed to near-zero by waiting for the other
        first.  latency_calculator_decorator guards the shared latency_results_file's
        read-modify-write with its own lock, so running both concurrently is safe.

        Pass the engine's flattened hdr_tags as the 'hdr_tags' kwarg when calling
        the decorated version; _find_hdr_tags checks kwargs first (decorators.py:295)
        and returns the list immediately, so both write and read tags are reported.

        Args:
            engine_name: Human-readable label used in log messages ("logstor"/"lsm").
            queues:      Live stress queue objects returned by _dispatch_stress_cmds.
            hdr_tags:    Explicit HDR tag list consumed by latency_calculator_decorator.

        Returns:
            Tuple (results, queues).
        """
        self.log.debug("Engine '%s': awaiting %d queue(s)", engine_name, len(queues))
        results = self._await_stress_queues(queues)
        self.log.debug("Engine '%s': collected %d result(s)", engine_name, len(results))
        return results, queues

    @staticmethod
    def _extract_step_latency(latency_results: dict, step_key: str) -> dict:
        """Build the enriched summary dict for one step key from a raw latency_results dict.

        Shared by check_latency_during_steps and check_latency_during_steps_dual_engine.
        analyze_hdr_percentiles requires at least one cycle with 'hdr_summary', so steps
        with no cycles (e.g. an engine that never got a result) are returned as-is instead.
        """
        if not latency_results or step_key not in latency_results:
            return {step_key: {"step": step_key, "legend": "", "cycles": []}}
        latency_results[step_key]["step"] = step_key
        entry = calculate_latency(latency_results[step_key])
        if entry.get("cycles"):
            return analyze_hdr_percentiles({step_key: entry})
        return {step_key: entry}

    def check_latency_during_steps_dual_engine(self, logstor_step, lsm_step):
        """Read latency results for both engine steps and remove the results file.

        Unlike check_latency_during_steps (which reads one step then deletes the
        file), this variant reads both steps in a single pass before deleting,
        so neither engine's results are lost.

        Args:
            logstor_step: Step name key written by the logstor decorator call.
            lsm_step:     Step name key written by the LSM decorator call.

        Returns:
            Tuple of (logstor_summary_dict, lsm_summary_dict).
        """
        with open(self.latency_results_file, encoding="utf-8") as file:
            latency_results = json.load(file)

        logstor_summary = self._extract_step_latency(latency_results, logstor_step)
        lsm_summary = self._extract_step_latency(latency_results, lsm_step)
        pathlib.Path(self.latency_results_file).unlink()
        return logstor_summary, lsm_summary

    def run_dual_engine_gradual_increase_load(  # noqa: PLR0914
        self,
        logstor_workload: Workload,
        lsm_workload: Workload,
        num_loaders: int,
        test_name: str,
    ):
        """Run LSM and logstor workloads concurrently with per-engine latency reporting.

        Concurrency model:
          1. Dispatch ALL stress threads for both engines non-blocking in one pass,
             so both workloads start on the cluster simultaneously.
          2. Await and decorate each engine's results *concurrently* (one thread per
             engine).  Awaiting sequentially would make the second engine's decorated
             start..end window collapse to near-zero (it would start only once the
             first engine's ~step_duration-long wait already returned), missing that
             engine's real HDR data.  Running both awaits in parallel threads keeps
             each decorator's window aligned with that engine's actual run.
             latency_calculator_decorator serializes the shared latency_results_file's
             read-modify-write internally, so this is race-free.

        Latency reporting:
          latency_calculator_decorator wraps _await_engine_results per engine, each
          with its own cycle_name, workload_type, and explicit hdr_tags kwarg.
          This produces two Argus latency rows per step:
            "logstor_<step>" — HDR tags fn--logstor_write, fn--logstor_read
            "lsm_<step>"     — HDR tags fn--lsm_write, fn--lsm_read

        Throughput:
          Each engine has 2 additive commands (write + read) on a single loader,
          so _aggregate_ops_rate with num_commands != num_loaders uses the sum path.

        Args:
            logstor_workload: Workload for the logstor engine (60% ops).
            lsm_workload:     Workload for the LSM engine (40% ops).
            num_loaders:      Number of loader nodes.
            test_name:        Human-readable test name for logging.
        """
        logstor_workload = self.update_num_threads_for_steps(workload=logstor_workload)
        lsm_workload = self.update_num_threads_for_steps(workload=lsm_workload)

        logstor_steps = self.get_sequential_throttle_steps(logstor_workload)

        total_logstor_summary = {}
        total_lsm_summary = {}

        for throttle_step_dict, num_threads, current_step in zip(
            logstor_workload.throttle_steps, logstor_workload.num_threads, logstor_steps
        ):
            logstor_step_key = f"logstor_{current_step}"
            lsm_step_key = f"lsm_{current_step}"

            step_params = dict(throttle_step_dict)
            if "threads" not in step_params:
                step_params["threads"] = num_threads
            step_params.setdefault("throttle", "")

            step_duration = throttle_step_dict.get("duration", logstor_workload.step_duration)

            self.log.info(
                "Dual-engine step '%s': logstor %s/%s op/s, lsm %s/%s op/s, threads=%s, duration=%s",
                current_step,
                throttle_step_dict.get("logstor_write_rate", "?"),
                throttle_step_dict.get("logstor_read_rate", "?"),
                throttle_step_dict.get("lsm_write_rate", "?"),
                throttle_step_dict.get("lsm_read_rate", "?"),
                num_threads,
                step_duration,
            )

            logstor_hdr_tags = ["fn--logstor_write", "fn--logstor_read"]
            lsm_hdr_tags = ["fn--lsm_write", "fn--lsm_read"]

            # ── Step 1: dispatch all threads for both engines non-blocking ────
            # Both workloads start on the cluster before either result is collected.
            logstor_queues = self._dispatch_stress_cmds(logstor_workload.cs_cmd_tmpl, step_params, step_duration)
            lsm_queues = self._dispatch_stress_cmds(lsm_workload.cs_cmd_tmpl, step_params, step_duration)

            # ── Step 2: await + decorate each engine concurrently ─────────────
            # Both engines' threads are already running for the same step_duration, so
            # awaiting them one after another would make the decorator measure the
            # *second* engine's start..end window as a near-zero-length slice right after
            # the first engine's (already ~step_duration long) wait returns -- that window
            # would miss the actual HDR data recorded throughout the real run, leaving that
            # engine's Argus latency table empty. Awaiting both in parallel threads lets each
            # decorator's own start..end genuinely span the concurrent workload. The shared
            # latency_results_file read-modify-write is serialized via a lock inside
            # latency_calculator_decorator itself, so this doesn't race.
            await_logstor = latency_calculator_decorator(
                legend=f"Logstor step {current_step} op/s (60%)",
                cycle_name=logstor_step_key,
                workload_type="mixed",
            )(self._await_engine_results)

            await_lsm = latency_calculator_decorator(
                legend=f"LSM step {current_step} op/s (40%)",
                cycle_name=lsm_step_key,
                workload_type="mixed",
            )(self._await_engine_results)

            with ThreadPoolExecutor(max_workers=2) as executor:
                logstor_future = executor.submit(
                    await_logstor, engine_name="logstor", queues=logstor_queues, hdr_tags=logstor_hdr_tags
                )
                lsm_future = executor.submit(await_lsm, engine_name="lsm", queues=lsm_queues, hdr_tags=lsm_hdr_tags)
                logstor_results, _ = logstor_future.result()
                lsm_results, _ = lsm_future.result()

            self.log.debug("Dual-engine step '%s' complete; collecting latency summaries", current_step)

            # Each engine has 2 additive commands (write + read) on 1 loader →
            # num_commands(2) != num_loaders(1) → _aggregate_ops_rate uses sum path.
            logstor_ops = self._aggregate_ops_rate(logstor_results, num_loaders, len(logstor_workload.cs_cmd_tmpl))
            lsm_ops = self._aggregate_ops_rate(lsm_results, num_loaders, len(lsm_workload.cs_cmd_tmpl))

            logstor_summary, lsm_summary = self.check_latency_during_steps_dual_engine(
                logstor_step=logstor_step_key, lsm_step=lsm_step_key
            )

            if logstor_ops:
                logstor_summary[logstor_step_key].update({"ops_rate": logstor_ops})
            if lsm_ops:
                lsm_summary[lsm_step_key].update({"ops_rate": lsm_ops})

            total_logstor_summary.update(logstor_summary)
            total_lsm_summary.update(lsm_summary)

            if throttle_step_dict.get("wait_no_compactions", logstor_workload.wait_no_compactions):
                if (wait_time := self.wait_no_compactions_running()[0]) < 180:
                    time.sleep(180 - wait_time)
                self.log.info("All compactions finished after dual-engine step '%s'", current_step)
                self.wait_for_no_tablets_splits()

        combined = {"logstor": total_logstor_summary, "lsm": total_lsm_summary}
        self.save_total_summary_in_file(combined)

    @staticmethod
    def _partition_dual_engine_cmds(stress_cmds: list) -> tuple[list, list]:
        """Partition a stress_cmd_m list into logstor and LSM command sets.

        Uses the Latte function-name prefix as the discriminator:
          logstor_* → logstor engine commands
          lsm_*     → LSM engine commands

        Args:
            stress_cmds: Full stress_cmd_m list containing commands for both engines.

        Returns:
            Tuple (logstor_cmds, lsm_cmds).

        Raises:
            ValueError: If either engine's command set is empty after partitioning.
        """
        logstor_cmds = [c for c in stress_cmds if any(fn.startswith("logstor_") for fn in find_latte_fn_names(c))]
        lsm_cmds = [c for c in stress_cmds if any(fn.startswith("lsm_") for fn in find_latte_fn_names(c))]

        if not logstor_cmds:
            raise ValueError(
                "_partition_dual_engine_cmds: no logstor_* commands found in stress_cmd_m. "
                "Ensure commands using --function logstor_write / logstor_read are present."
            )
        if not lsm_cmds:
            raise ValueError(
                "_partition_dual_engine_cmds: no lsm_* commands found in stress_cmd_m. "
                "Ensure commands using --function lsm_write / lsm_read are present."
            )
        return logstor_cmds, lsm_cmds

    def test_dual_engine_mixed_gradual_increase_load(self):
        """Run logstor (60%) and LSM (40%) Latte workloads concurrently with separate metrics.

        Test flow:
        1. Populate both tables (500M rows each, CL=ALL).
        2. Wait for compactions to quiesce.
        3. Run a series of mixed (write:30/read:70) gradual-throughput steps with:
           - Logstor table at 60% of target ops/s (fn--logstor_write, fn--logstor_read HDR tags).
           - LSM table at 40% of target ops/s (fn--lsm_write, fn--lsm_read HDR tags).
        4. Report independent per-engine latency series to Argus.

        Required config keys (provided by logstor_lsm_dual_60_40.yaml):
          stress_cmd_m  — all four commands (logstor_write, logstor_read, lsm_write, lsm_read);
                          partitioned by function-name prefix (logstor_* vs lsm_*).
          perf_gradual_throttle_steps.dual_engine_mixed,
          perf_gradual_step_duration.dual_engine_mixed
        """
        workload_type = "dual_engine_mixed"
        num_loaders = len(self.loaders.nodes)
        self.run_fstrim_on_all_db_nodes()

        # Preload both tables before entering the measured loop.
        if not skip_optional_stage("perf_preload_data"):
            # Create both tables before populating: run the prepare_stress_cmd
            # (a single 'latte schema' call that creates both ks_lsm and ks_logstor).
            # Neither Workload sets prepare_schema=True (they share a single schema
            # command), so we dispatch it directly here rather than via prepare_schema().
            if prepare_cmds := self.params.get("prepare_stress_cmd"):
                if isinstance(prepare_cmds, str):
                    prepare_cmds = [prepare_cmds]
                schema_queues = [
                    self.run_stress_thread(stress_cmd=cmd, round_robin=True, stats_aggregate_cmds=False)
                    for cmd in prepare_cmds
                ]
                for q in schema_queues:
                    self.get_stress_results(queue=q, store_results=False)
                self.log.info("Dual-engine schema created successfully")
            self.preload_data()
            self.wait_no_compactions_running(n=400, sleep_time=120)
            self.wait_for_no_tablets_splits()
            self.run_fstrim_on_all_db_nodes()

        throttle_steps_for_type = self.throttle_steps(workload_type)
        step_duration = self.step_duration(workload_type)
        num_threads = self.get_num_threads_for_workload(workload_type)

        all_stress_cmds = self.params.get("stress_cmd_m") or []
        logstor_cmds, lsm_cmds = self._partition_dual_engine_cmds(all_stress_cmds)

        # Read latte_schema_parameters once via the single-arg SCTConfiguration.get(),
        # then use plain dict.get(key, default) on the returned dict.
        schema_params = self.params.get("latte_schema_parameters") or {}

        logstor_workload = Workload(
            workload_type=workload_type,
            cs_cmd_tmpl=logstor_cmds,
            cs_cmd_warm_up=None,
            num_threads=num_threads,
            throttle_steps=throttle_steps_for_type,
            preload_data=False,
            drop_keyspace=False,
            wait_no_compactions=True,
            step_duration=step_duration,
            prepare_schema=False,
            test_keyspace=schema_params.get("logstor_keyspace", "ks_logstor"),
            test_table=schema_params.get("logstor_table", "t_logstor"),
        )

        lsm_workload = Workload(
            workload_type=workload_type,
            cs_cmd_tmpl=lsm_cmds,
            cs_cmd_warm_up=None,
            num_threads=num_threads,
            throttle_steps=throttle_steps_for_type,
            preload_data=False,
            drop_keyspace=False,
            wait_no_compactions=True,
            step_duration=step_duration,
            prepare_schema=False,
            test_keyspace=schema_params.get("lsm_keyspace", "ks_lsm"),
            test_table=schema_params.get("lsm_table", "t_lsm"),
        )

        self.run_dual_engine_gradual_increase_load(
            logstor_workload=logstor_workload,
            lsm_workload=lsm_workload,
            num_loaders=num_loaders,
            test_name="test_dual_engine_mixed_gradual_increase_load (logstor 60% / lsm 40%)",
        )

    def check_latency_during_steps(self, step):
        with open(self.latency_results_file, encoding="utf-8") as file:
            latency_results = json.load(file)
        self.log.debug(
            "Step %s: latency_results were loaded from file %s and its result is %s",
            step,
            self.latency_results_file,
            latency_results,
        )
        summary = self._extract_step_latency(latency_results, step)
        if latency_results:
            pathlib.Path(self.latency_results_file).unlink()
        self.log.debug("collected latency values are: %s", summary)
        return summary

    def run_step(self, stress_cmds, step_params, step_duration, hdr_tags=None):
        """
        Run a single stress step with parameters from step_params dict.

        Args:
            stress_cmds:   List of stress command templates
            step_params:   Dict with step parameters (threads, concurrency, rate, throttle)
            step_duration: Duration for this step
            hdr_tags:      Optional explicit list of HDR tag strings.  When provided,
                           latency_calculator_decorator uses it directly so that all
                           function tags (write + read) are reported rather than only
                           the first queue's tags.  Pass None to use auto-detection.
        """
        stress_queue = self._dispatch_stress_cmds(stress_cmds, step_params, step_duration)
        results = self._await_stress_queues(stress_queue)
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
            step_duration = throttle_step_dict.get("duration", workload.step_duration)

            self.log.info(
                "Run cs command with rate: %s Kops; threads: %s; step name: %s; duration: %s",
                throttle_step_dict.get("rate", "unthrottled"),
                step_params["threads"],
                current_throttle_step,
                step_duration,
            )
            # Pre-compute flattened HDR tags from command templates so both write
            # and read tags are passed explicitly to the decorator.  This prevents
            # _find_hdr_tags from stopping at the first queue and omitting the read tag.
            step_hdr_tags = [f"fn--{fn}" for cmd in workload.cs_cmd_tmpl for fn in find_latte_fn_names(cmd)]

            run_step = (
                latency_calculator_decorator(
                    legend=f"Gradual test step {current_throttle_step} op/s", cycle_name=current_throttle_step
                )
            )(self.run_step)
            results, _ = run_step(
                stress_cmds=workload.cs_cmd_tmpl,
                step_params=step_params,
                step_duration=step_duration,
                hdr_tags=step_hdr_tags or None,
            )
            self.log.debug("All c-s commands results collected and saved in Argus")

            summary_result = self.check_latency_during_steps(step=current_throttle_step)
            summary_result[current_throttle_step].update(
                {"ops_rate": self._aggregate_ops_rate(results, num_loaders, len(workload.cs_cmd_tmpl))}
            )
            total_summary.update(summary_result)
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

        self.save_total_summary_in_file(total_summary)

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
