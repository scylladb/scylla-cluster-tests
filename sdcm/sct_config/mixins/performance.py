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
