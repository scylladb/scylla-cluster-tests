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
from pydantic.types import confloat

from sdcm.sct_config.types import Boolean, DictOrStr, SctField, String, StringOrList


class PerformanceConfigMixin(BaseModel):
    """Performance regression tests configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Performance regression tests"

    perf_simple_query_extra_command: String = SctField(
        description="Extra command line options to pass to perf_simple_query",
    )
    stress_cmd_w: StringOrList = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    stress_cmd_r: StringOrList = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    stress_cmd_m: StringOrList = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    stress_cmd_read_disk: StringOrList = SctField(
        description="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure.
                multiple commands can passed as a list""",
    )
    stress_cmd_cache_warmup: StringOrList = SctField(
        description="""cassandra-stress commands for warm-up before read workload.
            You can specify everything but the -node parameter, which is going to
            be provided by the test suite infrastructure.
            multiple commands can passed as a list""",
    )
    effective_compression_ratio: confloat(gt=0, le=1.0) = SctField(
        description=(
            "Effective compression ratio used for Jinja stress command templating. "
            "Defined as on_disk_bytes / logical_uncompressed_bytes. "
            "This estimates how much disk space Scylla uses after compression relative to the logical "
            "uncompressed dataset size. For example, 1.0 means no effective compression and 0.68 means the "
            "data is expected to occupy about 68% of its logical uncompressed size on disk. Used together "
            "with the effective_disk_size_bytes template variable to calculate row counts that fill a target "
            "fraction of available disk capacity. You can estimate this ratio from Grafana in Keyspace -> "
            "Compression metrics; a compression value of 0% corresponds to effective_compression_ratio=1.0. "
            "Must be in range (0, 1.0]."
        ),
    )
    stress_template_context: DictOrStr = SctField(
        description=(
            "Shared runtime-only Jinja variables for stress command templating. "
            "Entries are resolved in declaration order and may reference earlier context entries as well as "
            "built-in stress template variables such as effective_disk_size_bytes and db_node_count_per_dc. "
            "These values are available to stress commands rendered by SCT, but are not evaluated during config "
            "load or validation."
        ),
    )
    prepare_write_cmd: StringOrList = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    stress_before_migration: String = SctField(
        description="Stress command to write data for post-migration validation",
    )
    verify_stress_after_migration: String = SctField(
        description="Stress command to verify data after migration",
    )
    stress_cmd_no_mv: StringOrList = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    stress_cmd_no_mv_profile: StringOrList = SctField(
        description="",
    )
    cs_user_profiles: StringOrList = SctField(
        description="cassandra-stress user-profiles list. Executed in test step",
    )
    prepare_cs_user_profiles: StringOrList = SctField(
        description="cassandra-stress user-profiles list. Executed in prepare step",
    )
    cs_duration: String = SctField(
        description="",
    )
    cs_debug: Boolean = SctField(
        description="enable debug for cassandra-stress",
    )
    cs_extra_jvm_opts: String = SctField(
        description="Extra JVM options passed to cassandra-stress via JVM_OPTS environment variable. "
        "Recommended for low-latency: '-XX:+UseZGC -XX:+ZGenerational -Xms8g -Xmx8g -XX:+AlwaysPreTouch' "
        "(requires Java 21+, which cassandra-stress 3.20.6+ ships with).",
    )
    cs_safepoint_logging: Boolean = SctField(
        description="Enable JVM safepoint logging (-Xlog:safepoint) for the cassandra-stress loaders. "
        "The log is written on the loader host, pulled into the loader log directory and collected into the "
        "run log archive. Use it to tell a loader JVM pause (including non-GC safepoints) apart from a "
        "server-side or network stall behind a latency-step failure. Not supported for k8s backends and "
        "prepared loaders.",
    )
    stress_cmd_mv: StringOrList = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    prepare_stress_cmd: StringOrList = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
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
    perf_gradual_step_duration: DictOrStr = SctField(
        description="Step duration of c-s load for gradual performance test per sub-test. "
        "Example: {'read': '30m', 'write': None, 'mixed': '30m'}",
    )
    perf_gradual_write_preload_data: Boolean = SctField(
        description="If true, preload data (via prepare_write_cmd) before "
        "test_write_gradual_increase_load. Needed for LWT conditional-update "
        "workloads (e.g. UPDATE ... IF <cond>) that require existing rows to "
        "have a chance of applying; not needed for INSERT-based write "
        "workloads on a fresh table.",
    )
    # PerformanceRegressionLWTTest
    stress_cmd_lwt_i: StringOrList = SctField(
        description="Stress command for LWT performance test for INSERT baseline",
    )
    stress_cmd_lwt_d: StringOrList = SctField(
        description="Stress command for LWT performance test for DELETE baseline",
    )
    stress_cmd_lwt_u: StringOrList = SctField(
        description="Stress command for LWT performance test for UPDATE baseline",
    )
    stress_cmd_lwt_ine: StringOrList = SctField(
        description="Stress command for LWT performance test for INSERT with IF NOT EXISTS",
    )
    stress_cmd_lwt_uc: StringOrList = SctField(
        description="Stress command for LWT performance test for UPDATE with IF <condition>",
    )
    stress_cmd_lwt_ue: StringOrList = SctField(
        description="Stress command for LWT performance test for UPDATE with IF EXISTS",
    )
    stress_cmd_lwt_de: StringOrList = SctField(
        description="Stress command for LWT performance test for DELETE with IF EXISTS",
    )
    stress_cmd_lwt_dc: StringOrList = SctField(
        description="Stress command for LWT performance test for DELETE with IF <condition>",
    )
    stress_cmd_lwt_mixed: StringOrList = SctField(
        description="Stress command for LWT performance test for mixed lwt load",
    )
    stress_cmd_lwt_mixed_baseline: StringOrList = SctField(
        description="Stress command for LWT performance test for mixed lwt load baseline",
    )
    run_db_node_benchmarks: Boolean = SctField(
        description="Flag for running db node benchmarks before the tests",
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
    num_loaders_step: int = SctField(
        description="Number of loaders which should be added per step",
    )
    stress_threads_start_num: int = SctField(
        description="Number of threads for c-s command",
    )
    num_threads_step: int = SctField(
        description="Number of threads which should be added on per step",
    )
    stress_step_duration: String = SctField(
        description="Duration of time for stress round",
    )
    max_deviation: float = SctField(
        description="Max relative difference between best and current throughput, if current throughput larger then best on max_rel_diff, it become new best one",
    )
    n_stress_process: int = SctField(
        description="Number of stress processes per loader",
    )
    stress_process_step: int = SctField(
        description="add/remove num of process on each round",
    )
    use_hdrhistogram: Boolean = SctField(
        description="Enable hdr histogram logging for cs",
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
