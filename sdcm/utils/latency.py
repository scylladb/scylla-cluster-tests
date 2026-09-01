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
import re
import statistics
from typing import Any

from sdcm.argus_results import LATENCY_ERROR_THRESHOLDS
from sdcm.db_stats import PrometheusDBStats

LOGGER = logging.getLogger(__name__)

# node_exporter scrapes the loaders in the same job as the DB nodes, on this port
NODE_EXPORTER_PORT = 9100
# range vector for the loader rates: long enough to cover at least two scrapes of any interval SCT
# uses. It smooths a sub-second stall away on purpose - these figures answer "were the loaders hot
# during this step", the loader-cpu.log sampler answers "what happened in that second".
LOADER_LOAD_RATE_WINDOW = "1m"


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


def _loader_instances_filter(loader_nodes) -> str:
    """A Prometheus regex matching the node_exporter `instance` labels of the given loaders.

    Every address a loader may have been registered under is included: the monitor builds the scrape
    targets from one of them, and which one depends on the backend and on ip_ssh_connections.
    """
    instances = []
    for node in loader_nodes:
        for attr in ("ip_address", "private_ip_address", "public_ip_address"):
            try:
                address = getattr(node, attr, None)
            except Exception:  # noqa: BLE001  # a cloud lookup of an already terminated node
                continue
            if not address:
                continue
            # IPv6 targets are registered in their bracketed form
            targets = [f"{address}:{NODE_EXPORTER_PORT}"]
            if ":" in address:
                targets.append(f"[{address}]:{NODE_EXPORTER_PORT}")
            for target in targets:
                if target not in instances:
                    instances.append(target)
    return "|".join(re.escape(instance) for instance in instances)


def collect_loader_load(monitor_node, start, end, loader_nodes) -> dict[str, float]:
    """Peak loader-side CPU figures over a latency step window (SCT-601).

    A client-observed P99 spike with flat server-side metrics is not necessarily a ScyllaDB problem -
    the loaders themselves can be CPU starved. Reported per step next to the latencies so that two
    runs can be compared directly instead of having to dig through Prometheus afterwards:

    - **busy**: the hottest loader's CPU utilization.
    - **steal**: CPU the hypervisor took away - a noisy neighbour or a credit-limited instance.
    - **pressure**: PSI, the share of time at least one task was stalled waiting for a CPU. The
      clearest "this loader was starved" signal, and it is high exactly when steal is not.
    - **load1**: the coarse signal that used to be the only one available, kept for comparison
      against past runs.

    A metric whose query returned nothing is left out rather than reported as 0 - no data must not
    read as an idle loader.
    """
    if not loader_nodes:
        return {}
    instances = _loader_instances_filter(loader_nodes)
    if not instances:
        LOGGER.warning("None of the loaders has an address to match node_exporter metrics with")
        return {}

    labels = f'instance=~"{instances}"'
    queries = {
        "Loader CPU busy max": f"100 - (avg by (instance) "
        f'(rate(node_cpu_seconds_total{{mode="idle",{labels}}}[{LOADER_LOAD_RATE_WINDOW}])) * 100)',
        "Loader CPU steal max": f"avg by (instance) "
        f'(rate(node_cpu_seconds_total{{mode="steal",{labels}}}[{LOADER_LOAD_RATE_WINDOW}])) * 100',
        "Loader CPU pressure max": f"rate(node_pressure_cpu_waiting_seconds_total{{{labels}}}"
        f"[{LOADER_LOAD_RATE_WINDOW}]) * 100",
        "Loader load1 max": f"node_load1{{{labels}}}",
    }

    prometheus = PrometheusDBStats(host=monitor_node.external_address)
    res = {}
    for metric, query in queries.items():
        try:
            query_res = prometheus.query(query, start, end)
        except Exception:  # noqa: BLE001  # diagnostics must not fail the latency reporting
            LOGGER.warning("Failed to query '%s' for %s", query, metric, exc_info=True)
            continue
        values = [
            float(value[-1])
            for entry in query_res
            for value in entry.get("values") or []
            if value[-1].lower() not in ("nan", "+inf", "-inf")
        ]
        if values:
            res[metric] = float(format(max(values), ".2f"))
    LOGGER.debug("Loader load during the step: %s", res)
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
