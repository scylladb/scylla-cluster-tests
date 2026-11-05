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
# Copyright (c) 2026 ScyllaDB

"""Performance regression tests configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, DictOrStr, SctField, String


class PerformanceConfigMixin(BaseModel):
    """Performance regression tests.

    Throughput/latency measurement runs, including gradual-throughput steps and HDR histogram
    settings.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Performance regression tests"

    max_deviation: float = SctField(
        description="Max relative difference between best and current throughput, if current throughput larger then best on max_rel_diff, it become new best one",
    )
    n_stress_process: int = SctField(
        description="Number of stress processes per loader",
    )
    num_loaders_step: int = SctField(
        description="Number of loaders which should be added per step",
    )
    num_threads_step: int = SctField(
        description="Number of threads which should be added on per step",
    )
    perf_best_throughput_search: DictOrStr = SctField(
        description="Rate search of the gradual performance test, which looks for the best throughput within "
        "perf_gradual_latency_limit. When set, the sub-test runs fixed-rate steps whose rate the search picks, "
        "instead of perf_gradual_throttle_steps/perf_gradual_threads. Each step gets rate x the P99 limit "
        "requests in flight: latte keeps the threads of the sub-test's first perf_gradual_throttle_steps entry "
        "and gets them as its concurrency, other tools as threads. Keyed by sub-test, with 'default' used for "
        "sub-tests not listed; a sub-test's entry overrides 'default' key by key. Keys: "
        "min_rate_per_shard and max_rate_per_shard (the bounds, times the DB shards; the minimum runs first and "
        "must pass, the maximum runs only when the search narrows up to it and must fail there, else the run "
        "reports an error), "
        "precision_pct (stop when the passing and failing rates are this close), "
        "max_steps (step budget per sub-test, the bounds included), "
        "min_sent_ratio (a step passes only when it measured at least this share of its rate). "
        "The search bisects the rate on a log scale. A step that collected no results is run once more before "
        "it counts as failed. "
        "Example: {'default': {'min_rate_per_shard': 1000, 'max_rate_per_shard': 60000, 'precision_pct': 5, "
        "'max_steps': 10, 'min_sent_ratio': 0.95}}",
    )
    perf_dataset_sizing: DictOrStr = SctField(
        description="Size the gradual performance test's dataset from the DB nodes' RAM and data disk. The "
        "test-case's stress commands define the dataset for a reference node; on other nodes their row counts "
        "(n=) and key ranges (-pop seq=) are scaled by the smallest node's RAM and disk relative to the "
        "reference, each rounded to scale_step: read/mixed hot ranges and cache warmup by RAM, the write "
        "dataset by disk, the preload and disk-only reads by the smaller of the two. The preload's throttle and "
        "threads scale with its rows, so it takes as long as on the reference node. Keys: reference_ram_gib, "
        "reference_disk_gb, scale_step. Not set: the commands run as written. "
        "Example: {'reference_ram_gib': 128, 'reference_disk_gb': 3750, 'scale_step': 0.25}",
    )
    perf_gradual_latency_limit: DictOrStr = SctField(
        description="P99 latency limit of the best-throughput search, in ms under the 'percentile_99' key: a "
        "gradual performance test step must stay within it to count as passing when looking for the best "
        "throughput. Keyed by sub-test, with 'default' used for sub-tests not listed. Only 'percentile_99' is "
        "supported. It is the search's limit only: the best-throughput overlay sets the P99 fixed_limit of "
        "latency_decorator_error_thresholds to null, because a search always has steps over the limit and those "
        "must not fail the run. "
        "Example: {'default': {'percentile_99': 10}, 'read_disk_only': {'percentile_99': 15}}",
    )
    perf_gradual_step_duration: DictOrStr = SctField(
        description="Step duration of c-s load for gradual performance test per sub-test. "
        "Example: {'read': '30m', 'write': None, 'mixed': '30m'}",
    )
    perf_gradual_threads: DictOrStr = SctField(
        description="Threads amount of stress load for gradual performance test per sub-test. "
        "Example: {'read': 100, 'write': [200, 300], 'mixed': 300}",
    )
    perf_gradual_throttle_steps: DictOrStr = SctField(
        description="Used for gradual performance test. Define throttle for load step in ops. "
        "Supports three formats: "
        "1) String/int list (cassandra-stress): {'read': ['100000', '150000'], 'mixed': [100, 200]} "
        "2) Dict list (latte/multi-param): {'read': [{'threads': 10, 'concurrency': 128, 'rate': '100000'}, ...]} "
        "Dict format allows specifying threads, concurrency, and rate per step. "
        "Integers are automatically converted to strings for backward compatibility.",
    )
    perf_gradual_write_preload_data: Boolean = SctField(
        description="If true, preload data (via prepare_write_cmd) before "
        "test_write_gradual_increase_load. Needed for LWT conditional-update "
        "workloads (e.g. UPDATE ... IF <cond>) that require existing rows to "
        "have a chance of applying; not needed for INSERT-based write "
        "workloads on a fresh table.",
    )
    perf_overload_steps: DictOrStr = SctField(
        description="Goodput under overload, run after the gradual performance test found its best throughput "
        "within perf_gradual_latency_limit. One throttled step per factor offers factor x the best throughput, "
        "with the best step's requests in flight times the factor (its concurrency for latte, else its threads); the "
        "goodput (throughput of successful operations) is reported as a ratio of the best throughput, and a "
        "ratio under min_goodput_ratio fails the run. Keyed by sub-test, with 'default' used for sub-tests not "
        "listed; a sub-test's entry overrides 'default' key by key. Not set: no overload steps. "
        "Example: {'default': {'factors': [1.3, 2], 'min_goodput_ratio': 0.9}}",
    )
    perf_simple_query_extra_command: String = SctField(
        description="Extra command line options to pass to perf_simple_query",
    )
    perf_stress_keyspace: String = SctField(
        description="""Keyspace name used in performance gradual throughput tests.
        Required for all stress tools (cassandra-stress, scylla-bench, cql-stress-cassandra-stress, latte).
        For latte, if not set, falls back to the 'keyspace' key in latte_schema_parameters.""",
    )
    perf_stress_table: String = SctField(
        description="""Table name used in performance gradual throughput tests.
        Required for all stress tools (cassandra-stress, scylla-bench, cql-stress-cassandra-stress, latte).
        For latte, if not set, falls back to the 'table' key in latte_schema_parameters.""",
    )
    run_db_node_benchmarks: Boolean = SctField(
        description="Flag for running db node benchmarks before the tests",
    )
    search_test_config: String = SctField(
        description="""Search test definition (datasets, shards and their runtime settings).
        Accepts an absolute path, or one relative to the SCT root, e.g. data_dir/latte/fts_search/plan.yaml.
        Required by a search test: it is the definition of what to run, so there is nothing to fall back on.
        Per-dataset load and index-wait limits live inside this file, not in SCT params.""",
    )
    stop_on_hw_perf_failure: Boolean = SctField(
        description="""Stop sct performance test if hardware performance test failed

    Hardware performance tests runs on each node with sysbench and cassandra-fio tools.
    Results stored in ES. HW perf tests run during cluster setups and not affect
    SCT Performance tests. Results calculated as average among all results for certain
    instance type or among all nodes during single run.
    if results for a single node is not in margin 0.01 of
    average result for all nodes, hw test considered as Failed.
    If stop_on_hw_perf_failure is True, then sct performance test will be terminated
       after hw perf tests detect node with hw results not in margin with average
    If stop_on_hw_perf_failure is False, then sct performance test will be run
       even after hw perf tests detect node with hw results not in margin with average""",
    )
    stress_process_step: int = SctField(
        description="add/remove num of process on each round",
    )
    stress_step_duration: String = SctField(
        description="Duration of time for stress round",
    )
    stress_threads_start_num: int = SctField(
        description="Number of threads for c-s command",
    )
    use_hdrhistogram: Boolean = SctField(
        description="Enable hdr histogram logging for cs",
    )
    workload_name: String = SctField(
        description="Workload name, can be: write|read|mixed|unset. "
        "Used for e.g. latency_calculator_decorator (use with 'use_hdrhistogram' set to true). "
        "If unset, workload is taken from test name.",
    )
