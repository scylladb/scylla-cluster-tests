# Stress commands and load generation

[← All configuration options](../configuration_options.md)

The load applied to the cluster: stress tool command lines, loader-side settings and stress
duration.

**Which option belongs to which tool.** Options fall into two kinds, and the difference is not
obvious from the names.

*Tool-agnostic* -- every `stress_cmd*`, `prepare_*_cmd` and [`stress_read_cmd`](#stress_read_cmd) option is a
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

Everything else here is loader-side and applies whatever the tool: [`stress_image`](#stress_image), [`bare_loaders`](#bare_loaders),
[`use_prepared_loaders`](#use_prepared_loaders), [`loader_swap_size`](#loader_swap_size), [`round_robin`](#round_robin), [`region_aware_loader`](#region_aware_loader), [`rack_aware_loader`](#rack_aware_loader),
the [`stress_multiplier`](#stress_multiplier) options, [`stress_duration`](#stress_duration), [`prepare_stress_duration`](#prepare_stress_duration) and
[`stop_test_on_stress_failure`](#stop_test_on_stress_failure).

**74 options.**


<a id="add_cs_user_profiles_extra_tables"></a>

## **add_cs_user_profiles_extra_tables** / SCT_ADD_CS_USER_PROFILES_EXTRA_TABLES

extra tables to create for template user c-s, in addition to pre-created tables

**default:** False

**type:** bool


<a id="alternator_stress_rate"></a>

## **alternator_stress_rate** / SCT_ALTERNATOR_STRESS_RATE

Number of operations per second to achieve in stress commands for alternator testing.

**default:** N/A

**type:** int


<a id="alternator_write_always_lwt_stress_rate"></a>

## **alternator_write_always_lwt_stress_rate** / SCT_ALTERNATOR_WRITE_ALWAYS_LWT_STRESS_RATE

Number of operations per second to achieve in stress commands for alternator testing, in write test with isolation set to always LWT. If non-zero, overwrites [`alternator_stress_rate`](#alternator_stress_rate).

**default:** N/A

**type:** int


<a id="bare_loaders"></a>

## **bare_loaders** / SCT_BARE_LOADERS

Don't install anything but node_exporter to the loaders during cluster setup

**default:** False

**type:** bool


<a id="batch_size"></a>

## **batch_size** / SCT_BATCH_SIZE

Number of rows per batch for the stress commands that write in batches.

**default:** 1

**type:** int


<a id="c_s_driver_version"></a>

## **c_s_driver_version** / SCT_C_S_DRIVER_VERSION

cassandra-stress driver version to use: 3|4|random

**default:** 3

**type:** Literal['3', '4', 'random']


<a id="cs_debug"></a>

## **cs_debug** / SCT_CS_DEBUG

enable debug for cassandra-stress

**default:** N/A

**type:** bool


<a id="cs_duration"></a>

## **cs_duration** / SCT_CS_DURATION

Duration passed to cassandra-stress, e.g. '50m'. Overrides any duration in the command itself.

**default:** 50m

**type:** str (appendable)


<a id="cs_extra_jvm_opts"></a>

## **cs_extra_jvm_opts** / SCT_CS_EXTRA_JVM_OPTS

Extra JVM options passed to cassandra-stress via JVM_OPTS environment variable. Recommended for low-latency: '-XX:+UseZGC -XX:+ZGenerational -Xms8g -Xmx8g -XX:+AlwaysPreTouch' (requires Java 21+, which cassandra-stress 3.20.6+ ships with).

**default:** N/A

**type:** str (appendable)


<a id="cs_populating_distribution"></a>

## **cs_populating_distribution** / SCT_CS_POPULATING_DISTRIBUTION

set c-s parameter '-pop' with gauss/uniform distribution for performance gradual throughput grow tests

**default:** N/A

**type:** str (appendable)


<a id="cs_safepoint_logging"></a>

## **cs_safepoint_logging** / SCT_CS_SAFEPOINT_LOGGING

Enable JVM safepoint logging (-Xlog:safepoint) for the cassandra-stress loaders. The log is written on the loader host, pulled into the loader log directory and collected into the run log archive. Use it to tell a loader JVM pause (including non-GC safepoints) apart from a server-side or network stall behind a latency-step failure. Not supported for k8s backends and prepared loaders.

**default:** False

**type:** bool


<a id="cs_user_profiles"></a>

## **cs_user_profiles** / SCT_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in test step

**default:** []

**type:** str | list[str] → list[str] (appendable)


<a id="effective_compression_ratio"></a>

## **effective_compression_ratio** / SCT_EFFECTIVE_COMPRESSION_RATIO

Effective compression ratio used for Jinja stress command templating. Defined as on_disk_bytes / logical_uncompressed_bytes. This estimates how much disk space Scylla uses after compression relative to the logical uncompressed dataset size. For example, 1.0 means no effective compression and 0.68 means the data is expected to occupy about 68% of its logical uncompressed size on disk. Used together with the effective_disk_size_bytes template variable to calculate row counts that fill a target fraction of available disk capacity. You can estimate this ratio from Grafana in Keyspace -> Compression metrics; a compression value of 0% corresponds to [`effective_compression_ratio`](#effective_compression_ratio)=1.0. Must be in range (0, 1.0].

**default:** 1.0

**type:** float


<a id="gemini_cmd"></a>

## **gemini_cmd** / SCT_GEMINI_CMD

gemini command to run (for now used only in GeminiTest)

**default:** N/A

**type:** str (appendable)


<a id="gemini_log_cql_statements"></a>

## **gemini_log_cql_statements** / SCT_GEMINI_LOG_CQL_STATEMENTS

Log CQL statements to file

**default:** N/A

**type:** bool


<a id="gemini_schema_url"></a>

## **gemini_schema_url** / SCT_GEMINI_SCHEMA_URL

Path to a local schema JSON file or a remote URL (http/https) that Gemini will use.<br>Local files are uploaded to the loader via send_files and mounted into the Gemini Docker<br>container via --schema.<br>Remote URLs are downloaded on the loader node with curl and then mounted the same way.

**default:** N/A

**type:** str (appendable)


<a id="gemini_seed"></a>

## **gemini_seed** / SCT_GEMINI_SEED

Seed number for gemini command

**default:** N/A

**type:** int


<a id="gemini_table_options"></a>

## **gemini_table_options** / SCT_GEMINI_TABLE_OPTIONS

table options for created table. example: ['cdc={'enabled': true}'], ['cdc={'enabled': true}', 'compaction={'class': 'IncrementalCompactionStrategy'}']

**default:** N/A

**type:** list


<a id="keyspace_num"></a>

## **keyspace_num** / SCT_KEYSPACE_NUM

Number of keyspaces to use in the test

**default:** 1

**type:** int


<a id="latte_schema_parameters"></a>

## **latte_schema_parameters** / SCT_LATTE_SCHEMA_PARAMETERS

Optional. Allows to pass through custom rune script parameters to the 'latte schema' command.<br>Also used as a fallback source for keyspace/table in gradual performance tests when<br>perf_stress_keyspace/perf_stress_table are not set.<br>For example, {'keyspace': 'test_keyspace', 'table': 'test_table'}

**default:** {}

**type:** dict | YAML/JSON string → dict


<a id="loader_swap_size"></a>

## **loader_swap_size** / SCT_LOADER_SWAP_SIZE

The size of the swap file for the loaders. Its size in bytes calculated by x * 1MB

**default:** N/A

**type:** int


<a id="prepare_cs_user_profiles"></a>

## **prepare_cs_user_profiles** / SCT_PREPARE_CS_USER_PROFILES

cassandra-stress user-profiles list. Executed in prepare step

**default:** []

**type:** str | list[str] → list[str] (appendable)


<a id="prepare_stress_cmd"></a>

## **prepare_stress_cmd** / SCT_PREPARE_STRESS_CMD

Stress command(s) run in the prepare phase, alongside [`prepare_write_cmd`](#prepare_write_cmd). See [`stress_cmd`](#stress_cmd) for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="prepare_stress_duration"></a>

## **prepare_stress_duration** / SCT_PREPARE_STRESS_DURATION

Time in minutes, which is required to run prepare stress commands<br>defined in prepare_*_cmd for dataset generation, and is used in<br>test duration calculation

**default:** 300

**type:** int


<a id="prepare_verify_cmd"></a>

## **prepare_verify_cmd** / SCT_PREPARE_VERIFY_CMD

Stress command(s) that verify the pre-loaded dataset before the test proper. See [`stress_cmd`](#stress_cmd) for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="prepare_wait_no_compactions_timeout"></a>

## **prepare_wait_no_compactions_timeout** / SCT_PREPARE_WAIT_NO_COMPACTIONS_TIMEOUT

Time to wait for compaction to finish at the end of prepare stage. Use only when compaction affects the test or load

**default:** N/A

**type:** int


<a id="prepare_write_cmd"></a>

## **prepare_write_cmd** / SCT_PREPARE_WRITE_CMD

Stress command(s) that pre-load the dataset before the test's own load starts. See [`stress_cmd`](#stress_cmd) for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="prepare_write_stress"></a>

## **prepare_write_stress** / SCT_PREPARE_WRITE_STRESS

Stress command to prepare write operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="rack_aware_loader"></a>

## **rack_aware_loader** / SCT_RACK_AWARE_LOADER

When enabled, loaders will look for nodes on the same rack.

**default:** False

**type:** bool


<a id="region_aware_loader"></a>

## **region_aware_loader** / SCT_REGION_AWARE_LOADER

When in multi region mode, run stress on loader that is located in the same region as db node

**default:** False

**type:** bool


<a id="round_robin"></a>

## **round_robin** / SCT_ROUND_ROBIN

Enable or disable round robin selection of nodes for operations

**default:** False

**type:** bool


<a id="stop_test_on_stress_failure"></a>

## **stop_test_on_stress_failure** / SCT_STOP_TEST_ON_STRESS_FAILURE

If set to True the test will be stopped immediately when stress command failed.<br>When set to False the test will continue to run even when there are errors in the<br>stress process

**default:** True

**type:** bool


<a id="store_cdclog_reader_stats_in_es"></a>

## **store_cdclog_reader_stats_in_es** / SCT_STORE_CDCLOG_READER_STATS_IN_ES

Add cdclog reader stats to ES for future performance result calculating

**default:** False

**type:** bool


<a id="stress_before_migration"></a>

## **stress_before_migration** / SCT_STRESS_BEFORE_MIGRATION

Stress command to write data for post-migration validation

**default:** N/A

**type:** str (appendable)


<a id="stress_cdc_log_reader_batching_enable"></a>

## **stress_cdc_log_reader_batching_enable** / SCT_STRESS_CDC_LOG_READER_BATCHING_ENABLE

retrieving data from multiple streams in one poll

**default:** True

**type:** bool


<a id="stress_cdclog_reader_cmd"></a>

## **stress_cdclog_reader_cmd** / SCT_STRESS_CDCLOG_READER_CMD

cdc-stressor command to read cdc_log table.<br>You can specify everything but the -node, -keyspace, -table parameter, which is going to<br>be provided by the test suite infrastructure.<br>Multiple commands can be passed as a list.

**default:** cdc-stressor -stream-query-round-duration 30s

**type:** str (appendable)


<a id="stress_cmd"></a>

## **stress_cmd** / SCT_STRESS_CMD

The test's main stress command(s). Everything except '-node' can be set; SCT fills in the node list. Accepts a single command or a list, one per loader thread.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_1"></a>

## **stress_cmd_1** / SCT_STRESS_CMD_1

Primary stress command to be executed.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_cache_warmup"></a>

## **stress_cmd_cache_warmup** / SCT_STRESS_CMD_CACHE_WARMUP

cassandra-stress commands for warm-up before read workload.<br>You can specify everything but the -node parameter, which is going to<br>be provided by the test suite infrastructure.<br>multiple commands can passed as a list

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_complex_prepare"></a>

## **stress_cmd_complex_prepare** / SCT_STRESS_CMD_COMPLEX_PREPARE

Stress command for complex preparation steps.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_complex_verify_delete"></a>

## **stress_cmd_complex_verify_delete** / SCT_STRESS_CMD_COMPLEX_VERIFY_DELETE

Stress command(s) that delete rows in the complex-schema data validation flow. See [`stress_cmd`](#stress_cmd) for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_complex_verify_more"></a>

## **stress_cmd_complex_verify_more** / SCT_STRESS_CMD_COMPLEX_VERIFY_MORE

Additional stress command to verify complex operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_complex_verify_read"></a>

## **stress_cmd_complex_verify_read** / SCT_STRESS_CMD_COMPLEX_VERIFY_READ

Stress command to verify complex read operations.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_lwt_d"></a>

## **stress_cmd_lwt_d** / SCT_STRESS_CMD_LWT_D

Stress command for LWT performance test for DELETE baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_lwt_dc"></a>

## **stress_cmd_lwt_dc** / SCT_STRESS_CMD_LWT_DC

Stress command for LWT performance test for DELETE with IF <condition>

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_lwt_de"></a>

## **stress_cmd_lwt_de** / SCT_STRESS_CMD_LWT_DE

Stress command for LWT performance test for DELETE with IF EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_lwt_i"></a>

## **stress_cmd_lwt_i** / SCT_STRESS_CMD_LWT_I

Stress command for LWT performance test for INSERT baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_lwt_ine"></a>

## **stress_cmd_lwt_ine** / SCT_STRESS_CMD_LWT_INE

Stress command for LWT performance test for INSERT with IF NOT EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_lwt_mixed"></a>

## **stress_cmd_lwt_mixed** / SCT_STRESS_CMD_LWT_MIXED

Stress command for LWT performance test for mixed lwt load

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_lwt_mixed_baseline"></a>

## **stress_cmd_lwt_mixed_baseline** / SCT_STRESS_CMD_LWT_MIXED_BASELINE

Stress command for LWT performance test for mixed lwt load baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_lwt_u"></a>

## **stress_cmd_lwt_u** / SCT_STRESS_CMD_LWT_U

Stress command for LWT performance test for UPDATE baseline

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_lwt_uc"></a>

## **stress_cmd_lwt_uc** / SCT_STRESS_CMD_LWT_UC

Stress command for LWT performance test for UPDATE with IF <condition>

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_lwt_ue"></a>

## **stress_cmd_lwt_ue** / SCT_STRESS_CMD_LWT_UE

Stress command for LWT performance test for UPDATE with IF EXISTS

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_m"></a>

## **stress_cmd_m** / SCT_STRESS_CMD_M

Mixed read/write stress command(s). See [`stress_cmd`](#stress_cmd) for the accepted format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_mv"></a>

## **stress_cmd_mv** / SCT_STRESS_CMD_MV

Stress command(s) for the leg of the test that runs with materialized views. See [`stress_cmd`](#stress_cmd) for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_no_mv"></a>

## **stress_cmd_no_mv** / SCT_STRESS_CMD_NO_MV

Stress command(s) for the leg of the test that runs without materialized views, so the MV overhead can be compared. See [`stress_cmd`](#stress_cmd) for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_no_mv_profile"></a>

## **stress_cmd_no_mv_profile** / SCT_STRESS_CMD_NO_MV_PROFILE

cassandra-stress user profile (YAML) for the no-materialized-view leg of the test.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_r"></a>

## **stress_cmd_r** / SCT_STRESS_CMD_R

Read-only stress command(s). See [`stress_cmd`](#stress_cmd) for the accepted format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_read_10m"></a>

## **stress_cmd_read_10m** / SCT_STRESS_CMD_READ_10M

Stress command to perform read operations for 10 minutes.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_read_60m"></a>

## **stress_cmd_read_60m** / SCT_STRESS_CMD_READ_60M

Stress command to perform read operations for 60 minutes.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_read_cl_one"></a>

## **stress_cmd_read_cl_one** / SCT_STRESS_CMD_READ_CL_ONE

Stress command to perform read operations with consistency level ONE.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_read_cl_quorum"></a>

## **stress_cmd_read_cl_quorum** / SCT_STRESS_CMD_READ_CL_QUORUM

Stress command to perform read operations with consistency level QUORUM.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_read_disk"></a>

## **stress_cmd_read_disk** / SCT_STRESS_CMD_READ_DISK

Read stress command(s) sized to miss the cache and read from disk. See [`stress_cmd`](#stress_cmd) for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_cmd_w"></a>

## **stress_cmd_w** / SCT_STRESS_CMD_W

Write-only stress command(s). See [`stress_cmd`](#stress_cmd) for the accepted format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_duration"></a>

## **stress_duration** / SCT_STRESS_DURATION

Time in minutes, Time of execution for stress commands from [`stress_cmd`](#stress_cmd) parameters<br>and is used in test duration calculation

**default:** 0

**type:** int


<a id="stress_image"></a>

## **stress_image** / SCT_STRESS_IMAGE

Dict of the images to use for the stress tools

**default:** {}

**type:** dict | YAML/JSON string → dict


<a id="stress_multiplier"></a>

## **stress_multiplier** / SCT_STRESS_MULTIPLIER

Multiplier for stress command intensity

**default:** 1

**type:** int


<a id="stress_multiplier_m"></a>

## **stress_multiplier_m** / SCT_STRESS_MULTIPLIER_M

Mixed operations stress command intensity multiplier

**default:** 1

**type:** int


<a id="stress_multiplier_r"></a>

## **stress_multiplier_r** / SCT_STRESS_MULTIPLIER_R

Multiplies the thread count of every read stress command, to scale read load without editing each command.

**default:** 1

**type:** int


<a id="stress_multiplier_w"></a>

## **stress_multiplier_w** / SCT_STRESS_MULTIPLIER_W

Multiplies the thread count of every write stress command, to scale write load without editing each command.

**default:** 1

**type:** int


<a id="stress_read_cmd"></a>

## **stress_read_cmd** / SCT_STRESS_READ_CMD

Read stress command(s) run in the verification phase. See [`stress_cmd`](#stress_cmd) for the format.

**default:** N/A

**type:** str | list[str] → list[str] (appendable)


<a id="stress_template_context"></a>

## **stress_template_context** / SCT_STRESS_TEMPLATE_CONTEXT

Shared runtime-only Jinja variables for stress command templating. Entries are resolved in declaration order and may reference earlier context entries as well as built-in stress template variables such as effective_disk_size_bytes and db_node_count_per_dc. These values are available to stress commands rendered by SCT, but are not evaluated during config load or validation.

**default:** {}

**type:** dict | YAML/JSON string → dict


<a id="use_prepared_loaders"></a>

## **use_prepared_loaders** / SCT_USE_PREPARED_LOADERS

If True, we use prepared VMs for loader (instead of using docker images)

**default:** N/A

**type:** bool


<a id="user_profile_table_count"></a>

## **user_profile_table_count** / SCT_USER_PROFILE_TABLE_COUNT

Number of user profile tables to create for the test

**default:** 1

**type:** int
