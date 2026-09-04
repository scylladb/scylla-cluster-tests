# Performance Test Catalog

Every performance test in SCT: what it measures, where it lives, and what drives it.

Inventory as of this document: **13 driver modules**, **~25 test classes**,
**39** test-case YAMLs under `test-cases/performance/`, **40** overlay configs under
`configurations/performance/`, **49** production Jenkinsfiles under
`jenkins-pipelines/performance/branch-perf-v17/` and **57** under
`jenkins-pipelines/performance_staging/`.

---

## 1. Test families

Performance tests fall into seven families. Everything below is one of these.

| # | Family | Question it answers | Driver |
|---|---|---|---|
| 1 | **Throughput staircase** | Can the cluster hold rate X with P99 under Y, sustained? | `PerformanceRegressionPredefinedStepsTest` |
| 2 | **Latency under load** | What is steady-state latency at a fixed load? | `PerformanceRegressionTest.test_latency*` |
| 3 | **Performance under operations** | How much does a nemesis/upgrade hurt, and for how long? | `PerformanceRegressionTest.test_latency_*_with_nemesis`, `PerformanceRegressionUpgradeTest` |
| 4 | **Max-throughput discovery** | What is the ceiling on this hardware? | `MaximumPerformanceSearchTest` |
| 5 | **Feature-cost tests** | What does feature Z cost? (CDC, MV, LWT, tablets, compression) | `PerformanceRegressionCDCTest`, `...LWTTest`, MV tests |
| 6 | **Microbenchmarks** | Per-operation CPU cost, no cluster involved | `PerfSimpleQueryTest`, `PerfCqlRawTest` |
| 7 | **Operational-time tests** | How long does an operation take? (repair, rebuild, backup) | `PerformanceRegressionRowLevelRepairTest`, `ScaleUpTest`, `PerformanceRegressionManagerBackupTest` |

---

## 2. Driver modules

### `performance_regression_test.py` — the core driver

Everything else inherits from this. `PerformanceRegressionTest(ClusterTester, LoaderUtilsMixin)`.

| Test method | Family | What it does |
|---|---|---|
| `test_write` / `test_read` / `test_mixed` | 2 | Classic throughput runs from `stress_cmd_w`/`_r`/`_m`. Read and mixed preload first. |
| `test_latency` | 2 | Preload to steady-state compactions (~10x RAM), then latency workload with gauss population. |
| `test_latency_steady_state` | 2 | Multiple stress commands, possibly different op types. Groups HDR tags per stress operation — this is the latte path, where rune function names become HDR tags. |
| `test_latency_read_with_nemesis` | 3 | fstrim -> preload -> wait for no compactions -> fstrim -> `run_workload(nemesis=True)`. |
| `test_latency_write_with_nemesis` | 3 | as above, write workload |
| `test_latency_mixed_with_nemesis` | 3 | as above, mixed workload |
| `test_mv_write` | 5 | Write on base table, then with MV on partition key, drop MV, then MV on clustering key. |
| `test_mv_{write,read,mixed}_{populated,not_populated}` | 5 | MV cost matrix (6 methods). |
| `test_uniform_counter_update_bench` | 5 | scylla-bench `-workload uniform -mode counter_update -duration 30m`. |
| `test_timeseries_bench` | 5 | scylla-bench timeseries write/read. |

`PerformanceRegressionUpgradeTest(PerformanceRegressionTest, UpgradeTest)` — latency during
rolling upgrade. `upgrade_node` is wrapped in
`@latency_calculator_decorator(legend="Upgrade Node")`, so each node upgrade is one cycle.

| Test method | Family |
|---|---|
| `test_latency_read_with_upgrade` | 3 |
| `test_latency_write_with_upgrade` | 3 |
| `test_latency_mixed_with_upgrade` | 3 |

`PerformanceRegressionMaterializedViewLatencyTest(PerformanceRegressionTest)` — worst-case
MV scenario: modifying a base-table regular column that is an MV primary-key column.

| Test method | Family |
|---|---|
| `test_read_mv_latency` | 5 |

### `performance_regression_gradual_grow_throughput.py` — the staircase

`PerformanceRegressionPredefinedStepsTest(PerformanceRegressionTest)`.

Each step runs at a fixed throttle from `perf_gradual_throttle_steps`; the last step is
unthrottled. Per-step latency is read from the stress HDR file and reported to Argus.

| Test method | Family | Workload key |
|---|---|---|
| `test_mixed_gradual_increase_load` | 1 | `mixed` |
| `test_write_gradual_increase_load` | 1 | `write` |
| `test_read_gradual_increase_load` | 1 | `read` |
| `test_read_disk_only_gradual_increase_load` | 1 | `read_disk_only` (no cache hits) |

Supporting types in the same module: `CSPopulateDistribution` (gauss/uniform) and the
`Workload` dataclass (`cmd` template, warm-up cmd, `num_threads`, `throttle_steps`,
`preload_data`, `drop_keyspace`).

**Step naming** (`get_sequential_throttle_steps`, `_step_names`) — this determines the key
your threshold YAML must use:

- All steps share one thread count -> step name is the rate string (`"300000"`, `unthrottled`).
- Thread counts vary per step -> step name is `<rate>_<threads>_threads`.
- A name repeats -> suffixed with an occurrence counter (`unthrottled_1`, `unthrottled_2`).

### `performance_search_max_throughput_test.py`

`MaximumPerformanceSearchTest(PerformanceRegressionTest)` — iterative search for maximum
sustainable throughput. Preload, wait for compactions, fstrim, then search using
`stress_cmd_r`/`_w`/`_m` as a template.

| Test method | Family |
|---|---|
| `test_search_best_read_throughput` | 4 |
| `test_search_best_write_throughput` | 4 |
| `test_search_best_mixed_throughput` | 4 |

> **Status:** flagged in [strategy.md](../performance-testing-strategy.md) as not having proven useful; slated for
> a rebuild on latte.

### `performance_regression_cdc_test.py`

`PerformanceRegressionCDCTest(PerformanceRegressionTest)` — measures CDC overhead by running
the same workload with CDC disabled, then enabled.

| Test method | Family | Notes |
|---|---|---|
| `test_write_with_cdc` | 5 | write w/o CDC, truncate, enable CDC, write again |
| `test_write_with_cdc_preimage` | 5 | `cdc = {enabled: true, preimage: true}` |
| `test_write_with_cdc_postimage` | 5 | `cdc = {enabled: true, postimage: true}` |
| `test_write_throughput` / `test_write_latency` | 5 | `cdc_workflow()` |
| `test_mixed_throughput` / `test_mixed_latency` | 5 | `cdc_workflow(use_cdclog_reader=True)` — adds the CDC log reader stress thread |

### `performance_regression_lwt_test.py`

`PerformanceRegressionLWTTest(PerformanceRegressionTest)` — LWT (Paxos) cost. Defines its own
`latency_report_metrics` / `throughput_report_metrics` and an `lwt_subtests` list.

| Test method | Family | Notes |
|---|---|---|
| `test_latency` | 5 | preload, compact all nodes, loop over `lwt_subtests` under `ignore_operation_errors()` |
| `test_throughput` | 5 | same shape |

### `performance_regression_alternator_test.py`

`PerformanceRegressionAlternatorTest(PerformanceRegressionTest)` — DynamoDB API via YCSB.
All methods delegate to `run_test_suite_by_configuration_name(mode)`.

| Test method | Mode |
|---|---|
| `test_full` | `full` |
| `test_latency` / `test_latency_read` | `basic-read` |
| `test_latency_write` | `basic-write` |
| `test_latency_mixed` | `basic-mixed` |
| `test_throughput` / `test_throughput_read` / `test_throughput_write` | `basic-throughput` |

### `ycsb_performance_regression_test.py`

`BaseYCSBPerformanceRegressionTest(PerformanceRegressionTest)` — iterates YCSB workloads a-f
(update-heavy, read-mostly, read-only, read-latest, short-range scan, read-modify-write).
Builds `bin/ycsb load|run scylla` with `recordcount = records_size`.

`test_latency` pre-creates the keyspace, fstrims, preloads, then runs each of the six
workloads with a compaction wait + fstrim between them.

Sized subclasses (inherit `test_latency` only): `YCSBPerformanceRegression1MRecordsTest`
(10^6), `...10MRecordsTest` (10^7), `...100MRecordsTest` (10^8), `...1BRecordsTest` (10^9).

### `performance_regression_row_level_repair_test.py`

`PerformanceRegressionRowLevelRepairTest(ClusterTester)`. All methods require
`SCT_HINTED_HANDOFF_DISABLED=true`, 3 nodes, RF=3.

| Test method | Family | Notes |
|---|---|---|
| `test_row_level_repair_single_node_diff` | 7 | repair time when only one node diverges |
| `test_row_level_repair_3_nodes_small_diff` | 7 | small distinct writes on all 3 nodes |
| `test_row_level_repair_large_partitions` | 7 | scylla-bench only (asserts `stress_cmd` starts with `scylla-bench`) |
| `test_row_level_repair_during_load` | 7 | repair while background c-s write load runs |

### `performance_regression_manager_backup_test.py`

`PerformanceRegressionManagerBackupTest(PerformanceRegressionTest, ManagerTestFunctionsMixIn)`
— latency impact of a Scylla Manager backup, driven by the Manager-backup nemesis.

| Method | Family | Notes |
|---|---|---|
| `test_stress_steady_state(stress_cmd)` | — | helper: run stress, sleep 60s, measure 30min steady state |
| `test_manager_backup` | 3 | preload, align cluster data state, steady state, then mixed workload with backup nemesis |

### `performance_regression_user_profiles_test.py`

`PerformanceRegressionUserProfilesTest(ClusterTester)`. `test_user_profiles` — for each file
in `cs_user_profiles`, extracts the embedded commented-out `cassandra-stress` command lines,
runs each with `cs_duration`, then drops the profile's keyspace.

### `performance_scale_up_test.py`

`ScaleUpTest(ClusterTester)`. `test_write_and_rebuild_time` — measure total ingest time (one
c-s command per loader, round-robin), wait for compactions, wipe `keyspace1/standard1` data
files on node 1, run `nodetool rebuild`, record `ingest_time` and `rebuild_duration`.

### `throughput_limit_test.py`

`ThroughputLimitFunctionalTest(ClusterTester)`. Uses `scylla_bench.test`. Helper dataclass
`PerfResult(latency_99, ops)`.

| Test method | Notes |
|---|---|
| `test_per_partition_limit` | Baseline read/write latency+ops with and without an extra heavy few-partition load, then repeat with per-partition rate limits. Asserts limited runs are not much worse, and better under hot-partition load. |
| `test_compaction_throughput_limit` | Write latency/ops baseline, then with compaction throughput capped at 0.45 / 0.2 / 0.01 of the node's max disk write throughput. |

### `microbenchmarking_test.py`

`MicrobenchmarkTest(ClusterTester)` — base. Runs one `scylla perf-*` tool on a single DB
node, parses its JSON, calls `send_microbenchmark_result_to_argus`.

> Options must be passed as `--name value`, never `--name=value`.

| Class | Test methods | Notes |
|---|---|---|
| `PerfSimpleQueryTest` | `test_perf_simple_query` | `scylla perf-simple-query --json-result ... --smp 1 -m 1G`. In-process query processor; no ports or config collide with the live scylla-server. `perf_simple_query_extra_command: --write` selects the write workload. |
| `PerfCqlRawTest` | `test_read`, `test_write` | `scylla perf-cql-raw` — full networking + CQL frame parsing. Boots a real scylla in-process, writes its own `SCYLLA_CONFIG` to `--workdir /tmp/scylla-perf-cql-raw-workdir/conf/scylla.yaml`, **stops the node's scylla-server** for the run and restarts it in `finally`. |

`PerfCqlRawTest.RESOURCE_OPTIONS = "--smp 2 --cpus 0,1 -m 2G"` and `DURATION = 60` are
constants **because changing them invalidates the Argus baseline**.

---

## 3. Configuration layout

A perf run's config is a merge of a test-case YAML plus one or more overlay configs, listed
in the Jenkinsfile's `test_config` parameter. Merging is a recursive dict merge
(`anyconfig.merge(..., ac_merge=MS_DICTS)` via `merge_dicts_append_strings` in
`sdcm/sct_config.py`), so nested keys accumulate rather than replace.

### `test-cases/performance/` — 39 YAMLs

| Family | Count | Examples |
|---|---|---|
| latency-650gb | 4 | `-elasticity`, `-grow-shrink`, `-upgrade`, `-with-nemesis` |
| other latency sizes / elasticity | 5 | `latency-125gb`, `latency-1TB`, `latency-2.5tb-elasticity`, `latency-250gb-with-nemesis`, `latency-500gb-30min` |
| predefined-throughput-steps (c-s) | 4 | base, `-custom-d3-w1`, `-lwt-heavy`, `-lwt-light` |
| predefined-throughput-steps (latte) | 2 | `latte-perf-regression-predefined-throughput-steps-{tablets,vnodes}` |
| CDC | 4 | `latency-cdc-mixed-poll-batching`, `throughput-cdc-mixed-poll-batching`, `write-latency-cdc`, `write-throughput-cdc` |
| Alternator / DynamoDB | 3 | `alternator-basic`, `alternator-full`, `alternator.100threads.30M-keys` |
| LWT (non-steps) | 4 | `latency-lwt-{big,small}`, `throughput-lwt-{big,small}` |
| Materialized views | 2 | `perf-regression-2mv`, `latency-mv-read-concurrency` |
| classic read/write/mixed (c-s 100 threads) | 4 | `.30M-keys`, `.30M-keys-i4i`, `.30M-keys-i4i-enterprise`, `.100M-keys-z3-enterprise` |
| plain throughput | 2 | `throughput-125gb`, `throughput-baremetal-5gb` |
| user profiles | 1 | `perf-regression-user-profiles` |
| Manager backup nemesis | 1 | `perf-regression-latency-backup-nemesis` |
| row-level repair | 1 | `perf-row-level-repair-1TB` |
| max-throughput search | 1 | `perf-search-best-throughput-config` |
| YCSB | 1 | `ycsb/perf-base.yaml` |

### `configurations/performance/` — 40 overlay YAMLs

| Group | Count | Notes |
|---|---|---|
| `latency-decorator-error-thresholds-*` | 12 | See [results-and-regression.md](results-and-regression.md) |
| cassandra-stress gradual load steps | 5 | `cassandra_stress_gradual_load_steps{,_enterprise,_i8g,_z4d,_reduced_steps_number}` |
| latte | 4 | `latte_gradual_load_steps_enterprise`, `latte-perf-gradual-steps-custom-d3-w1`, `latte-perf-regression-latency-650gb-upgrade`, `latte-perf-regression-latency-steady-state-custom-d1-workload1` |
| cql-stress | 4 | 650gb ent/oss, 6gb mini-test, `cql_stress_gradual_load_reduced_steps_number` |
| cassandra-stress 650gb 80%-throughput | 2 | ent / oss |
| logstor | 3 | `enable_logstor`, `logstor_hotspot_queries`, `logstor_uniform_queries` |
| driver-specific stress | 2 | `gocql-predefined-throughput-steps-stress`, `rust-predefined-throughput-steps-cql-stress` |
| scylla-bench | 1 | `scylla_bench_gradual_load_reduced_steps_number` |
| elasticity | 1 | `elasticity-2.5Tb` |
| `perf_cql_raw/` | 2 | microbenchmark thresholds, arm64 / x86_64 |
| `perf_simple/` | 3 | microbenchmark thresholds + `perf_simple_write_option` |
| `xcloud/` | 1 | `overrides.yaml` |

### Perf configs living outside those two directories

| Path | Contents |
|---|---|
| `configurations/operator/` | `perf-regression-latency.yaml`, `perf-regression-throughput.yaml`, `perf-serverless-thrpt-{1,2}vcpu.yaml` |
| `configurations/perf-loaders-{shard-aware,non-shard-aware}-config.yaml` | loader driver config |
| `configurations/triggers/perf-regression.yaml` | the trigger matrix — see [pipelines-and-scheduling.md](pipelines-and-scheduling.md) |
| `configurations/stress_images/` | pinned Docker tags per stress tool |
| `test-cases/microbenchmarking/` | `amazon_perf_cql_raw_{ARM,x86}.yaml`, `amazon_perf_simple_query_{ARM,x86}.yaml` |
| `test-cases/features/compaction-throughput-limit.yaml` | drives `throughput_limit_test.py` |
| `test-cases/features/elasticity/` | `elasticity-90-percent-perf-i4i-*` |
| `test-cases/upgrades/rolling-upgrade-latency-regression*.yaml` | 3 files |
| `test-cases/longevity/longevity-ycsb-a-{1M,10M,100M,1B}.yaml` | YCSB longevity |

---

## 4. Stress tool integrations

All stress threads subclass `DockerBasedStressThread` (`sdcm/stress/base.py:33`).

| Tool | Module | Thread class |
|---|---|---|
| cassandra-stress | `sdcm/stress_thread.py` | `CassandraStressThread` |
| cql-stress-cassandra-stress | `sdcm/cql_stress_cassandra_stress_thread.py` | `CqlStressCassandraStressThread` |
| scylla-bench | `sdcm/scylla_bench_thread.py` | `ScyllaBenchThread` |
| latte | `sdcm/stress/latte_thread.py` | `LatteStressThread` |
| YCSB | `sdcm/ycsb_thread.py` | `YcsbStressThread` |
| ndbench | `sdcm/ndbench_thread.py` | `NdBenchStressThread` |
| nosqlbench | `sdcm/nosql_thread.py` | `NoSQLBenchStressThread` |
| cassandra-harry | `sdcm/cassandra_harry_thread.py` | `CassandraHarryThread` |
| gemini | `sdcm/gemini_thread.py` | `GeminiStressThread` |
| hydra-kcl (Alternator streams) | `sdcm/kcl_thread.py` | `KclStressThread`, `CompareTablesSizesThread` |
| CDC log reader | `sdcm/cdclog_reader_thread.py` | `CDCLogReaderThread` |

### Dispatch

`ClusterTester.run_stress_thread` (`sdcm/tester.py:3017`) selects the thread by inspecting
the command string, **in this order**:

| Match | Handler |
|---|---|
| `"cql-stress-cassandra-stress" in cmd` | `run_cql_stress_cassandra_thread` |
| `"cassandra-stress" in cmd` | `run_stress_cassandra_thread` |
| `cmd.startswith("scylla-bench")` | `run_stress_thread_bench` |
| `"cassandra-harry" in cmd` | `run_stress_thread_harry` |
| `cmd.startswith("bin/ycsb")` | `run_ycsb_thread` |
| `cmd.startswith("latte")` | `run_latte_thread` |
| `cmd.startswith("ndbench")` | `run_ndbench_thread` |
| `cmd.startswith("hydra-kcl")` | `run_hydra_kcl_thread` |
| `cmd.startswith("nosqlbench")` | `run_nosqlbench_thread` |
| `cmd.startswith("table_compare")` | `run_table_compare_thread` |
| `cmd.startswith("python_thread")` | `run_python_thread` |
| otherwise | `ValueError("Unsupported stress command: ...")` |

> **Ordering matters.** The `cql-stress-cassandra-stress` check must precede the
> `cassandra-stress` substring check, because the former contains the latter.

Gemini is **not** in this dispatch — it has a separate `run_gemini` (`sdcm/tester.py:3363`).
`run_cdclog_reader_thread` (`sdcm/tester.py:3335`) is called directly by the CDC test.

---

## 5. Which driver each Jenkins job uses

Counted across all perf pipeline directories.

| `test_name` | Jenkinsfiles |
|---|---|
| `performance_regression_test.PerformanceRegressionTest` | 46 (+2 operator EKS) |
| `performance_regression_gradual_grow_throughput.PerformanceRegressionPredefinedStepsTest` | 29 |
| `performance_regression_test.PerformanceRegressionUpgradeTest` | 5 |
| `performance_regression_manager_backup_test...test_manager_backup` | 4 |
| `performance_regression_cdc_test.PerformanceRegressionCDCTest` | 4 |
| `performance_regression_lwt_test.PerformanceRegressionLWTTest` | 4 |
| `microbenchmarking_test.PerfSimpleQueryTest.test_perf_simple_query` | 4 |
| `microbenchmarking_test.PerfCqlRawTest.test_read` / `.test_write` | 2 + 2 |
| `performance_search_max_throughput_test.MaximumPerformanceSearchTest` | 2 |
| `performance_regression_alternator_test.PerformanceRegressionAlternatorTest` | 2 |
| `ycsb_performance_regression_test.YCSBPerformanceRegression{1M,10M,100M,1B}RecordsTest` | 1 each |
| `performance_regression_test.PerformanceRegressionMaterializedViewLatencyTest` | 1 |
| `performance_regression_test.PerformanceRegressionTest.test_write` | 1 |
