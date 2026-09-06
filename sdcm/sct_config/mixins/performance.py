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

from sdcm.sct_config.types import Boolean, DictOrStr, MultitenantValue, SctField, String, StringOrList


class PerformanceConfigMixin(BaseModel):
    """Performance regression tests configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Performance regression tests"

    perf_simple_query_extra_command: String = SctField(
        description="Extra command line options to pass to perf_simple_query",
    )
    stress_cmd_w: MultitenantValue(StringOrList) = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    stress_cmd_r: MultitenantValue(StringOrList) = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    stress_cmd_m: MultitenantValue(StringOrList) = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. Multiple commands can be passed as a list",
    )
    stress_cmd_read_disk: MultitenantValue(StringOrList) = SctField(
        description="""cassandra-stress commands.
                You can specify everything but the -node parameter, which is going to
                be provided by the test suite infrastructure.
                multiple commands can passed as a list""",
    )
    stress_cmd_cache_warmup: MultitenantValue(StringOrList) = SctField(
        description="""cassandra-stress commands for warm-up before read workload.
            You can specify everything but the -node parameter, which is going to
            be provided by the test suite infrastructure.
            multiple commands can passed as a list""",
    )
    prepare_write_cmd: MultitenantValue(StringOrList) = SctField(
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
        "Example: {'read': ['100000', '150000'], 'mixed': ['300']}",
    )
    perf_gradual_step_duration: DictOrStr = SctField(
        description="Step duration of c-s load for gradual performance test per sub-test. "
        "Example: {'read': '30m', 'write': None, 'mixed': '30m'}",
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
