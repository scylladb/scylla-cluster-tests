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

"""Stress commands and load generation configuration options."""

from typing import ClassVar, Literal

from pydantic import BaseModel
from pydantic.types import confloat

from sdcm.sct_config.types import Boolean, DictOrStr, SctField, String, StringOrList


class StressConfigMixin(BaseModel):
    """Stress commands and load generation.

    The load applied to the cluster: stress tool command lines, loader-side settings and stress
    duration.

    **Which option belongs to which tool.** Options fall into two kinds, and the difference is not
    obvious from the names.

    *Tool-agnostic* -- every `stress_cmd*`, `prepare_*_cmd` and `stress_read_cmd` option is a
    command line, and **the tool is whatever the command's first word is**. SCT reads it straight
    off the string (see `SCTConfiguration.list_of_stress_tools`), so the same option runs
    cassandra-stress, scylla-bench, gemini, latte, ycsb, cql-stress, nosqlbench, ndbench,
    cassandra-harry or hydra-kcl depending only on what you put there:

    ```yaml
    stress_cmd: "cassandra-stress write cl=QUORUM n=1000000 ..."     # cassandra-stress
    stress_cmd: "scylla-bench -workload=sequential -mode=write ..."  # scylla-bench
    stress_cmd: "latte run --duration 30m ..."                       # latte
    ```

    Their names describe the *role* in the test (write, read, mixed, prepare, verify), not the tool.

    *Tool-specific* -- these apply only when that tool is in use, and are ignored otherwise:

    | Tool | Options |
    |---|---|
    | cassandra-stress | cs_user_profiles, prepare_cs_user_profiles, cs_duration, cs_debug, cs_extra_jvm_opts, cs_safepoint_logging, cs_populating_distribution, c_s_driver_version, user_profile_table_count, add_cs_user_profiles_extra_tables, stress_cmd_no_mv_profile |
    | gemini | gemini_cmd, gemini_seed, gemini_schema_url, gemini_table_options, gemini_log_cql_statements |
    | latte | latte_schema_parameters |
    | cdc log reader | stress_cdclog_reader_cmd, stress_cdc_log_reader_batching_enable, store_cdclog_reader_stats_in_es |

    Everything else here is loader-side and applies whatever the tool: stress_image, bare_loaders,
    use_prepared_loaders, loader_swap_size, round_robin, region_aware_loader, rack_aware_loader,
    the stress_multiplier options, stress_duration, prepare_stress_duration and
    stop_test_on_stress_failure.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Stress commands and load generation"

    add_cs_user_profiles_extra_tables: Boolean = SctField(
        description="extra tables to create for template user c-s, in addition to pre-created tables",
    )
    alternator_stress_rate: int = SctField(
        description="""
           Number of operations per second to achieve in stress commands for alternator testing.
      """,
    )
    alternator_write_always_lwt_stress_rate: int = SctField(
        description="""
              Number of operations per second to achieve in stress commands for alternator testing, in write test with isolation set to always LWT. If non-zero, overwrites alternator_stress_rate.
         """,
    )
    bare_loaders: Boolean = SctField(
        description="Don't install anything but node_exporter to the loaders during cluster setup",
    )
    batch_size: int = SctField(
        description="Number of rows per batch for the stress commands that write in batches.",
    )
    c_s_driver_version: Literal["3", "4", "random"] = SctField(
        description="cassandra-stress driver version to use: 3|4|random",
    )
    cs_debug: Boolean = SctField(
        description="enable debug for cassandra-stress",
    )
    cs_duration: String = SctField(
        description="Duration passed to cassandra-stress, e.g. '50m'. Overrides any duration in the command itself.",
    )
    cs_extra_jvm_opts: String = SctField(
        description="Extra JVM options passed to cassandra-stress via JVM_OPTS environment variable. "
        "Recommended for low-latency: '-XX:+UseZGC -XX:+ZGenerational -Xms8g -Xmx8g -XX:+AlwaysPreTouch' "
        "(requires Java 21+, which cassandra-stress 3.20.6+ ships with).",
    )
    cs_populating_distribution: String = SctField(
        description="set c-s parameter '-pop' with gauss/uniform distribution for performance gradual throughput grow tests",
    )
    cs_safepoint_logging: Boolean = SctField(
        description="Enable JVM safepoint logging (-Xlog:safepoint) for the cassandra-stress loaders. "
        "The log is written on the loader host, pulled into the loader log directory and collected into the "
        "run log archive. Use it to tell a loader JVM pause (including non-GC safepoints) apart from a "
        "server-side or network stall behind a latency-step failure. Not supported for k8s backends and "
        "prepared loaders.",
    )
    cs_user_profiles: StringOrList = SctField(
        description="cassandra-stress user-profiles list. Executed in test step",
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
    gemini_cmd: String = SctField(
        description="gemini command to run (for now used only in GeminiTest)",
    )
    gemini_log_cql_statements: Boolean = SctField(
        description="Log CQL statements to file",
    )
    gemini_schema_url: String = SctField(
        description="""Path to a local schema JSON file or a remote URL (http/https) that Gemini will use.
                    Local files are uploaded to the loader via send_files and mounted into the Gemini Docker
                    container via --schema.
                    Remote URLs are downloaded on the loader node with curl and then mounted the same way.""",
    )
    gemini_seed: int = SctField(
        description="Seed number for gemini command",
    )
    gemini_table_options: list = SctField(
        description="table options for created table. example: ['cdc={'enabled': true}'], ['cdc={'enabled': true}', 'compaction={'class': 'IncrementalCompactionStrategy'}']",
    )
    keyspace_num: int = SctField(
        description="Number of keyspaces to use in the test",
    )
    latte_schema_parameters: DictOrStr = SctField(
        description="""Optional. Allows to pass through custom rune script parameters to the 'latte schema' command.
        Also used as a fallback source for keyspace/table in gradual performance tests when
        perf_stress_keyspace/perf_stress_table are not set.
        For example, {'keyspace': 'test_keyspace', 'table': 'test_table'}""",
    )
    loader_swap_size: int = SctField(
        description="The size of the swap file for the loaders. Its size in bytes calculated by x * 1MB",
    )
    prepare_cs_user_profiles: StringOrList = SctField(
        description="cassandra-stress user-profiles list. Executed in prepare step",
    )
    prepare_stress_cmd: StringOrList = SctField(
        description="Stress command(s) run in the prepare phase, alongside 'prepare_write_cmd'. See 'stress_cmd' for the format.",
    )
    prepare_stress_duration: int = SctField(
        description="""
              Time in minutes, which is required to run prepare stress commands
              defined in prepare_*_cmd for dataset generation, and is used in
              test duration calculation
         """,
    )
    prepare_verify_cmd: StringOrList = SctField(
        description="Stress command(s) that verify the pre-loaded dataset before the test proper. See 'stress_cmd' for the format.",
    )
    prepare_wait_no_compactions_timeout: int = SctField(
        description="Time to wait for compaction to finish at the end of prepare stage. Use only when compaction affects the test or load",
    )
    prepare_write_cmd: StringOrList = SctField(
        description="Stress command(s) that pre-load the dataset before the test's own load starts. See 'stress_cmd' for the format.",
    )
    prepare_write_stress: StringOrList = SctField(
        description="Stress command to prepare write operations.",
    )
    rack_aware_loader: Boolean = SctField(
        description="When enabled, loaders will look for nodes on the same rack.",
    )
    region_aware_loader: Boolean = SctField(
        description="When in multi region mode, run stress on loader that is located in the same region as db node",
    )
    round_robin: Boolean = SctField(
        description="Enable or disable round robin selection of nodes for operations",
    )
    stop_test_on_stress_failure: Boolean = SctField(
        description="""If set to True the test will be stopped immediately when stress command failed.
                       When set to False the test will continue to run even when there are errors in the
                       stress process""",
    )
    store_cdclog_reader_stats_in_es: Boolean = SctField(
        description="Add cdclog reader stats to ES for future performance result calculating",
    )
    stress_before_migration: String = SctField(
        description="Stress command to write data for post-migration validation",
    )
    stress_cdc_log_reader_batching_enable: Boolean = SctField(
        description="""retrieving data from multiple streams in one poll""",
    )
    stress_cdclog_reader_cmd: String = SctField(
        description="""cdc-stressor command to read cdc_log table.
                       You can specify everything but the -node, -keyspace, -table parameter, which is going to
                       be provided by the test suite infrastructure.
                       Multiple commands can be passed as a list.""",
    )
    stress_cmd: StringOrList = SctField(
        description="The test's main stress command(s). Everything except '-node' can be set; SCT fills in the node list. Accepts a single command or a list, one per loader thread.",
    )
    stress_cmd_1: StringOrList = SctField(
        description="Primary stress command to be executed.",
    )
    stress_cmd_cache_warmup: StringOrList = SctField(
        description="""cassandra-stress commands for warm-up before read workload.
            You can specify everything but the -node parameter, which is going to
            be provided by the test suite infrastructure.
            multiple commands can passed as a list""",
    )
    stress_cmd_complex_prepare: StringOrList = SctField(
        description="Stress command for complex preparation steps.",
    )
    stress_cmd_complex_verify_delete: StringOrList = SctField(
        description="Stress command(s) that delete rows in the complex-schema data validation flow. See 'stress_cmd' for the format.",
    )
    stress_cmd_complex_verify_more: StringOrList = SctField(
        description="Additional stress command to verify complex operations.",
    )
    stress_cmd_complex_verify_read: StringOrList = SctField(
        description="Stress command to verify complex read operations.",
    )
    stress_cmd_lwt_d: StringOrList = SctField(
        description="Stress command for LWT performance test for DELETE baseline",
    )
    stress_cmd_lwt_dc: StringOrList = SctField(
        description="Stress command for LWT performance test for DELETE with IF <condition>",
    )
    stress_cmd_lwt_de: StringOrList = SctField(
        description="Stress command for LWT performance test for DELETE with IF EXISTS",
    )
    # PerformanceRegressionLWTTest
    stress_cmd_lwt_i: StringOrList = SctField(
        description="Stress command for LWT performance test for INSERT baseline",
    )
    stress_cmd_lwt_ine: StringOrList = SctField(
        description="Stress command for LWT performance test for INSERT with IF NOT EXISTS",
    )
    stress_cmd_lwt_mixed: StringOrList = SctField(
        description="Stress command for LWT performance test for mixed lwt load",
    )
    stress_cmd_lwt_mixed_baseline: StringOrList = SctField(
        description="Stress command for LWT performance test for mixed lwt load baseline",
    )
    stress_cmd_lwt_u: StringOrList = SctField(
        description="Stress command for LWT performance test for UPDATE baseline",
    )
    stress_cmd_lwt_uc: StringOrList = SctField(
        description="Stress command for LWT performance test for UPDATE with IF <condition>",
    )
    stress_cmd_lwt_ue: StringOrList = SctField(
        description="Stress command for LWT performance test for UPDATE with IF EXISTS",
    )
    stress_cmd_m: StringOrList = SctField(
        description="Mixed read/write stress command(s). See 'stress_cmd' for the accepted format.",
    )
    stress_cmd_mv: StringOrList = SctField(
        description="Stress command(s) for the leg of the test that runs with materialized views. See 'stress_cmd' for the format.",
    )
    stress_cmd_no_mv: StringOrList = SctField(
        description="Stress command(s) for the leg of the test that runs without materialized views, so the MV overhead can be compared. See 'stress_cmd' for the format.",
    )
    stress_cmd_no_mv_profile: StringOrList = SctField(
        description="cassandra-stress user profile (YAML) for the no-materialized-view leg of the test.",
    )
    stress_cmd_r: StringOrList = SctField(
        description="Read-only stress command(s). See 'stress_cmd' for the accepted format.",
    )
    stress_cmd_read_10m: StringOrList = SctField(
        description="Stress command to perform read operations for 10 minutes.",
    )
    stress_cmd_read_60m: StringOrList = SctField(
        description="Stress command to perform read operations for 60 minutes.",
    )
    stress_cmd_read_cl_one: StringOrList = SctField(
        description="Stress command to perform read operations with consistency level ONE.",
    )
    stress_cmd_read_cl_quorum: StringOrList = SctField(
        description="Stress command to perform read operations with consistency level QUORUM.",
    )
    stress_cmd_read_disk: StringOrList = SctField(
        description="Read stress command(s) sized to miss the cache and read from disk. See 'stress_cmd' for the format.",
    )
    stress_cmd_w: StringOrList = SctField(
        description="Write-only stress command(s). See 'stress_cmd' for the accepted format.",
    )
    stress_duration: int = SctField(
        description="""
              Time in minutes, Time of execution for stress commands from stress_cmd parameters
              and is used in test duration calculation
        """,
    )
    stress_image: DictOrStr = SctField(
        description="Dict of the images to use for the stress tools",
    )
    stress_multiplier: int = SctField(
        description="Multiplier for stress command intensity",
    )
    stress_multiplier_m: int = SctField(
        description="Mixed operations stress command intensity multiplier",
    )
    stress_multiplier_r: int = SctField(
        description="Multiplies the thread count of every read stress command, to scale read load without editing each command.",
    )
    stress_multiplier_w: int = SctField(
        description="Multiplies the thread count of every write stress command, to scale write load without editing each command.",
    )
    stress_read_cmd: StringOrList = SctField(
        description="Read stress command(s) run in the verification phase. See 'stress_cmd' for the format.",
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
    use_prepared_loaders: Boolean = SctField(
        description="If True, we use prepared VMs for loader (instead of using docker images)",
    )
    user_profile_table_count: int = SctField(
        description="Number of user profile tables to create for the test",
    )
