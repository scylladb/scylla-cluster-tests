# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB
import logging
import statistics
from dataclasses import dataclass
from typing import Any

from sdcm.argus_results import LATENCY_ERROR_THRESHOLDS
from sdcm.db_stats import PrometheusDBStats

LOGGER = logging.getLogger(__name__)


def avg(values):
    return sum(values) / len(values)


def collect_latency(monitor_node, start, end, load_type, cluster, nodes_list):  # noqa: PLR0914
    res = {}
    prometheus = PrometheusDBStats(host=monitor_node.external_address)
    duration = int(end - start)
    cassandra_stress_precision = ["99", "95"]  # in the future should include also 'max'
    scylla_precision = ["99"]  # in the future should include also '95', '5'
    threshold = 10  # ms

    for precision in cassandra_stress_precision:
        metric = f"c-s {precision}" if precision == "max" else f"c-s P{precision}"
        if not precision == "max":
            precision = f"perc_{precision}"  # noqa: PLW2901
        query = f'sct_cassandra_stress_{load_type}_gauge{{type="lat_{precision}"}}'
        query_res = prometheus.query(query, start, end)
        latency_values_lst = []
        max_latency_values_lst = []
        for entry in query_res:
            if not entry["values"]:
                continue
            sequence = [float(val[-1]) for val in entry["values"] if not val[-1].lower() == "nan"]
            if not sequence or all(val == sequence[0] for val in sequence):
                continue
            latency_values_lst.extend(sequence)
            max_latency_values_lst.extend(sequence)

        if latency_values_lst:
            res[metric] = float(format(avg(latency_values_lst), ".2f"))
            res[f"{metric}_stdev"] = float(format(statistics.stdev(latency_values_lst), ".2f"))
            res[f"{metric}_points_above_threshold"] = len([v for v in latency_values_lst if v > threshold])
        if max_latency_values_lst:
            res[f"{metric} max"] = float(format(max(max_latency_values_lst), ".2f"))

    if load_type == "mixed":
        load_type = ["read", "write"]
    elif load_type == "read_disk_only":
        load_type = ["read"]
    else:
        load_type = [load_type]

    for load in load_type:
        for precision in scylla_precision:
            query = (
                f"histogram_quantile(0.{precision},sum(rate(scylla_storage_proxy_coordinator_{load}_"
                f"latency_bucket{{}}[{duration}s])) by (instance, le))"
            )
            query_res = prometheus.query(query, start, end)
            for entry in query_res:
                node_ip = entry["metric"]["instance"].replace("[", "").replace("]", "")
                node = cluster.get_node_by_ip(node_ip)
                if not node:
                    for db_node in nodes_list:
                        if db_node.ip_address == node_ip:
                            node = db_node
                if node:
                    node_idx = node.name.split("-")[-1]
                else:
                    continue
                node_name = f"node-{node_idx}"
                metric = f"Scylla P{precision}_{load} - {node_name}"
                if not entry["values"]:
                    continue
                sequence = [float(val[-1]) for val in entry["values"] if not val[-1].lower() == "nan"]
                if sequence:
                    res[metric] = float(format(avg(sequence) / 1000, ".2f"))

    return res


NON_METRIC_FIELDS = ["screenshots", "hdr", "hdr_summary", "duration", "duration_in_sec", "reactor_stalls_stats"]


def calculate_latency(latency_results):
    result_dict = {}
    all_keys = list(latency_results.keys())
    steady_key = ""
    if all_keys:
        steady_key = [key for key in all_keys if "steady" in key.lower()]
    if not steady_key or not all_keys:
        return latency_results
    else:
        steady_key = all_keys.pop(all_keys.index(steady_key[0]))
    result_dict[steady_key] = latency_results[steady_key].copy()
    for key in all_keys:
        if key == "summary":
            result_dict[key] = latency_results[key].copy()
            continue
        result_dict[key] = latency_results[key].copy()
        temp_dict = {}
        for cycle in latency_results[key]["cycles"]:
            for metric, value in cycle.items():
                if metric in NON_METRIC_FIELDS or "stdev" in metric or "threshold" in metric:
                    continue
                if metric not in temp_dict:
                    temp_dict[metric] = []
                temp_dict[metric].append(value)
        for temp_key, temp_val in temp_dict.items():
            if "Cycles Average" not in result_dict[key]:
                result_dict[key]["Cycles Average"] = {}
            average = float(format(avg([float(val) for val in temp_val]), ".2f"))
            result_dict[key]["Cycles Average"][temp_key] = average
            if "Relative to Steady" not in result_dict[key]:
                result_dict[key]["Relative to Steady"] = {}
            if temp_key in latency_results[steady_key]:
                steady_val = float(latency_results[steady_key][temp_key])
                if steady_val != 0:
                    result_dict[key]["Relative to Steady"][temp_key] = float(format((average - steady_val), ".2f"))
                if "color" not in result_dict[key]:
                    result_dict[key]["color"] = {}
                if average - steady_val >= 10:
                    result_dict[key]["color"][temp_key] = "red"
                elif average - steady_val >= 5:
                    result_dict[key]["color"][temp_key] = "yellow"
                else:
                    result_dict[key]["color"][temp_key] = "blue"
    return result_dict


def analyze_hdr_percentiles(result_stats: dict[str, Any]) -> dict[str, Any]:
    for operation, stats_data in result_stats.items():
        top_limit_operation = operation if operation in LATENCY_ERROR_THRESHOLDS else "default"
        stats = stats_data.get("cycles") or [stats_data]
        for cycle in stats:
            for workload, results in cycle["hdr_summary"].items():
                cycle["hdr_summary"][workload]["color"] = {}
                if results["percentile_90"] > LATENCY_ERROR_THRESHOLDS[top_limit_operation]["percentile_90"]:
                    cycle["hdr_summary"][workload]["color"].update({"percentile_90": "red"})
                else:
                    cycle["hdr_summary"][workload]["color"].update({"percentile_90": ""})
                if results["percentile_99"] > LATENCY_ERROR_THRESHOLDS[top_limit_operation]["percentile_99"]:
                    cycle["hdr_summary"][workload]["color"].update({"percentile_99": "red"})
                else:
                    cycle["hdr_summary"][workload]["color"].update({"percentile_99": ""})

            for interval in cycle["hdr"]:
                for workload, results in interval.items():
                    interval[workload]["color"] = {}
                    if results["percentile_90"] > LATENCY_ERROR_THRESHOLDS[top_limit_operation]["percentile_90"]:
                        results["color"].update({"percentile_90": "red"})
                    else:
                        results["color"].update({"percentile_90": ""})
                    if results["percentile_99"] > LATENCY_ERROR_THRESHOLDS[top_limit_operation]["percentile_99"]:
                        results["color"].update({"percentile_99": "red"})
                    else:
                        results["color"].update({"percentile_99": ""})

    return result_stats


@dataclass(frozen=True)
class BestThroughputStep:
    name: str
    throughput: int
    p99: dict[str, float]
    threads: int | None = None


def step_latency(name: str, step: dict[str, Any]) -> tuple[dict[str, float], int] | None:
    """Return a gradual step's worst P99 per HDR key and its average HDR-measured throughput.

    Keys are '<operation>--<HDR tag>'. Every key any cycle reported is included, each with the worst P99 of
    the cycles that reported it, so an operation that shows up only in a later cycle is not let through
    unchecked. Returns None when the step has no usable HDR results.
    """
    if not (cycles := [cycle for cycle in step.get("cycles") or [] if cycle.get("hdr_summary")]):
        LOGGER.debug("Step %s: no HDR results", name)
        return None
    try:
        hdr_keys = dict.fromkeys(hdr_key for cycle in cycles for hdr_key in cycle["hdr_summary"])
        p99 = {
            hdr_key: max(
                cycle["hdr_summary"][hdr_key]["percentile_99"] for cycle in cycles if hdr_key in cycle["hdr_summary"]
            )
            for hdr_key in hdr_keys
        }
        throughput = round(avg([cycle["cycle_hdr_throughput"] for cycle in cycles]))
    except (KeyError, TypeError) as exc:
        LOGGER.debug("Step %s: incomplete HDR results (%r)", name, exc)
        return None
    return p99, throughput


def p99_violations(name: str, p99: dict[str, float], p99_limit_ms: float) -> dict[str, float]:
    """Return the HDR keys of a step whose P99 is over the limit; an empty dict means the step passed."""
    if over_limit := {hdr_key: value for hdr_key, value in p99.items() if value > p99_limit_ms}:
        LOGGER.debug("Step %s: P99 over the %s ms limit: %s", name, p99_limit_ms, over_limit)
    return over_limit


def find_best_throughput(total_summary: dict[str, Any], p99_limit_ms: float) -> BestThroughputStep | None:
    """Pick the gradual step with the highest measured throughput whose P99 is within the limit.

    `total_summary` is the per-step summary of the gradual test, keyed by step name. The best-throughput
    search runs unthrottled steps that differ by thread count, so a step passes on latency alone: the HDR
    P99 of every operation, in every cycle that reported it, must be within `p99_limit_ms` (for mixed, both
    read and write). Steps without HDR results are skipped.

    Passing steps are ranked by HDR-measured throughput, not by thread count: past saturation, more threads
    add latency without adding throughput. The step's 'threads' entry, when present, is reported as is.
    Returns None when no step passed.
    """
    passed = []
    for name, step in total_summary.items():
        if (latency := step_latency(name, step)) is None:
            continue
        p99, throughput = latency
        if p99_violations(name, p99, p99_limit_ms):
            continue
        passed.append(BestThroughputStep(name=name, throughput=throughput, p99=p99, threads=step.get("threads")))

    return max(passed, key=lambda step: step.throughput, default=None)
