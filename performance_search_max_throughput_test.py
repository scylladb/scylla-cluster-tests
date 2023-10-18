import re
import json

from collections import defaultdict
from typing import Any

# from sdcm.send_email import Email
from performance_regression_test import PerformanceRegressionTest
from sdcm.results_analyze import SearchBestThroughputConfigPerformanceAnalyzer
from sdcm.utils.common import format_timestamp


class MaximumPerformanceSearchTest(PerformanceRegressionTest):

    def test_search_best_read_throughput(self):  # pylint: disable=too-many-locals
        stress_params = {"stress_cmd_tmpl":  self.params.get('stress_cmd_r'),
                         "test_name": "Test read"}
        stress_params.update(**self._get_stress_parameters())

        self.run_fstrim_on_all_db_nodes()
        # run a write workload as a preparation
        self.preload_data()
        self.wait_no_compactions_running(n=400, sleep_time=120)
        self.run_fstrim_on_all_db_nodes()

        self.run_search_best_performance(**stress_params)

    def test_search_best_write_throughput(self):  # pylint: disable=too-many-locals
        stress_params = {"stress_cmd_tmpl":  self.params.get('stress_cmd_w'),
                         "test_name": "Test write"}
        stress_params.update(**self._get_stress_parameters())
        # run a write workload as a preparation
        self.preload_data()
        self.wait_no_compactions_running(n=400, sleep_time=120)
        self.run_fstrim_on_all_db_nodes()

        self.run_search_best_performance(**stress_params)

    def test_search_best_mixed_throughput(self):  # pylint: disable=too-many-locals
        stress_params = {"stress_cmd_tmpl":  self.params.get('stress_cmd_m'),
                         "test_name": "Test mixed"}
        stress_params.update(**self._get_stress_parameters())
        # run a write workload as a preparation
        self.preload_data()
        self.wait_no_compactions_running(n=400, sleep_time=120)
        self.run_fstrim_on_all_db_nodes()

        self.run_search_best_performance(**stress_params)

    # pylint: disable=too-many-arguments,too-many-locals,too-many-statements,too-many-branches
    def run_search_best_performance(self, stress_cmd_tmpl: str,
                                    stress_num: int,
                                    stress_num_step: int,
                                    stress_step_duration: str,
                                    start_threads: int,
                                    threads_step: int,
                                    num_loaders_step: int,
                                    max_deviation: float,
                                    test_name: str):
        """Run search best performance result

           Test starts with predefined number of loaders,
           processes per loader and threads of c-s tool.

           after first run we got some throughput which
           is set as best result. After that on each
           step the number of threads of cs tool increased
           on thread_num_step threads. The total threads become bigger.
           1. if result on current step is lager than best result + deviation (configured),
           then new best throughput result is written and continue
           2. if current result is in (best - deviation, best + deviation),
           then rerun step with decreased number of cs tool processes per
           loader with last number of threads. the total threads in this case
           will be in interval ( total threads in previous step, total threads in current step).
           if current step with less number of process and same number of threads
           give new best results, we back to condition 1.
           the number of processes continue to decrease while reach 0.
           3. if test configured with loader_num_step = 0, this means
           that number of loaders won't be decreased, then test will be stopped.
           if test configured with loader_num_step > 1, then, number of loaders
           will decreased, number of processes will be set to preconfigured one,
           and number of threads per process will be calculated by dividing total
           threads of best result on new number of loaders and processes

        """
        all_results = []
        best_result = {}
        original_nodes_list = self.loaders.nodes[:]
        origin_stress_num = stress_num
        decrease_loaders_num = False

        threads = start_threads
        self.log.info("Start search best performance")
        self.log.debug("Use c-s cmd template %s", stress_cmd_tmpl)
        while len(self.loaders.nodes) > 0:
            stress_cmd = self._build_stress_command(cmd_tmpl=stress_cmd_tmpl,
                                                    threads=threads,
                                                    duration=stress_step_duration)
            self.log.debug("Run c-s command %s", stress_cmd)
            stress_threads = []
            base_cmd_w = self._get_write_c_s_commands_spread_evenly(stress_cmd, len(self.loaders.nodes),
                                                                    stress_num)
            for stress_cmd in base_cmd_w:
                stress_threads.append(self.run_stress_thread(stress_cmd=stress_cmd, stress_num=stress_num,
                                                             round_robin=True, stats_aggregate_cmds=False))
            results = []
            for stress in stress_threads:
                results += self.get_stress_results(queue=stress)
            self.log.debug("Current results %s", results)
            current_result = self._calculate_average_stats(results)
            current_result.update({
                "threads": threads,
                "n_loaders": len(self.loaders.nodes),
                "n_process": stress_num,
                "total_threads": threads * stress_num * len(self.loaders.nodes),
                "stress_cmd": stress_cmd})

            self.log.debug("Summary results for current step: %s", current_result)
            all_results.append(current_result)
            if not best_result:
                best_result = current_result
                self.log.debug("Increase threads on %s", threads_step)
                threads += threads_step
                continue

            deviation = best_result["op rate"] * max_deviation / 100

            if current_result["op rate"] > best_result["op rate"] + deviation:
                self.log.info("New best throughtput %s", current_result["op rate"])
                best_result = current_result
                self.log.info("Current throughput: %s larger than best: %s. Increase threads",
                              current_result["op rate"],
                              best_result["op rate"])
                threads += threads_step
            elif (current_result["op rate"] >= best_result["op rate"] - deviation and
                    current_result["op rate"] <= best_result["op rate"] + deviation):
                stress_num -= stress_num_step
                if stress_num < 1:
                    decrease_loaders_num = True
            else:
                decrease_loaders_num = True

            if decrease_loaders_num:
                if not num_loaders_step:
                    self.log.info("Decrease number of stress processes")
                    stress_num -= stress_num_step
                    if stress_num < 1:
                        self.log.info("No processes left for run. Stop test")
                        break
                else:
                    self.log.info("Decrease number of loaders and back to start num of process")
                    new_num_of_nodes = len(self.loaders.nodes) - num_loaders_step
                    if new_num_of_nodes < 1:
                        self.log.info("No loaders left for run. Stop test")
                        break
                    self.loaders.nodes = original_nodes_list[:new_num_of_nodes]
                    stress_num = origin_stress_num

                threads = (best_result["total_threads"] // (len(self.loaders.nodes) * stress_num)) - threads_step
                threads = 10 * round(threads / 10) or start_threads
                decrease_loaders_num = False

            if current_result["op rate"] < best_result["op rate"] / 2:
                self.log.warning("Current result less in 2 times than best result. Stop the test")
                break

        self.loaders.nodes = original_nodes_list[:]

        self.log.info("Found best configuration with results %s",  best_result)

        self.log.info("Write data to files")
        filename = f"{self.logdir}/all_stats_result.json"
        with open(filename, "w", encoding="utf-8") as fp_json:
            json.dump(all_results, fp_json, indent=4)
        filename = f"{self.logdir}/best_stat_result.json"
        with open(filename, "w", encoding="utf-8") as fp_json:
            json.dump(best_result, fp_json, indent=4)

        raw_results = {
            "best_stat": best_result,
            "all_stats": all_results
        }

        setup_details = {
            "scylla_version": self.db_cluster.nodes[0].scylla_version_detailed,
            "start_time": format_timestamp(self.start_time),
            "instance_type_db": self.params.get("instance_type_db"),
            "instance_type_loader": self.params.get("instance_type_loader"),
            "prepare_cs_cmd": self.params.get("prepare_write_cmd")
        }

        analyzer = SearchBestThroughputConfigPerformanceAnalyzer(es_index=self._test_index,
                                                                 es_doc_type=self._es_doc_type,
                                                                 email_recipients=self.params.get("email_recipients"))
        analyzer.check_regression(test_name, setup_details=setup_details, test_results=raw_results)

    def _get_stress_parameters(self) -> dict[Any, Any]:
        return {
            "stress_num":  int(self.params.get("n_stress_process")),
            "stress_num_step": int(self.params.get("stress_process_step")),
            "num_loaders_step": int(self.params.get("num_loaders_step")),
            "start_threads":  int(self.params.get("stress_threads_start_num")),
            "threads_step":  int(self.params.get("num_threads_step")),
            "max_deviation": float(self.params.get("max_deviation")),
            "stress_step_duration":  self.params.get("stress_step_duration"),
        }

    def _calculate_average_stats(self, results):
        status = defaultdict(list)
        # calculate total status for each c-s
        for result in results:
            for key in result.keys():
                try:
                    status[key].append(float(result.get(key)))
                except ValueError as exc:
                    self.log.warning("Failed to convert value %s. Error: %s", result.get(key), exc)
                    continue

        # get average for latencies
        final_result = {}
        for key in status:
            if key == "op rate":
                final_result[key] = sum(status[key])
                continue
            if not status[key] or key == "errors":
                final_result[key] = 0
                continue
            final_result[key] = round(sum(status[key]) / len(status[key]), 2) or 0

        return final_result

    @staticmethod
    def _calculate_difference(current: float, best: float) -> float:
        diff = (current - best) / current * 100
        diff = diff if diff > 0 else -1 * diff
        return diff

    @staticmethod
    def _build_stress_command(cmd_tmpl: str, threads: int, duration: str) -> str:
        if isinstance(cmd_tmpl, list):
            cmd_tmpl = cmd_tmpl[0]
        cmd_tmpl = re.sub(r'\sthreads=\d+\s', f' threads={threads} ', cmd_tmpl)
        cmd_tmpl = re.sub(r'\sduration=\d+[mhd]\s', f' duration={duration} ', cmd_tmpl)
        return cmd_tmpl
