# Performance Test Catalog

Every performance test in SCT: what it measures, where it lives, and whether it is still run.

Terminology in this folder: a **test module** is a `performance_*_test.py` file, a **test class**
is the class inside it, and a **test method** is the `test_*` function a Jenkins job selects
through `sub_tests`. "Driver" is deliberately avoided — in SCT it means a CQL driver.

---

## 1. What is actually maintained

`configurations/triggers/perf-regression.yaml` is the authoritative answer to "does this test
still run". Everything scheduled there falls into one of four families:

| Family | Question it answers | Test class / methods |
|---|---|---|
| **Throughput staircase** | Can the cluster hold rate X with P99 under Y, sustained? | `PerformanceRegressionPredefinedStepsTest.test_{read,write,mixed,read_disk_only}_gradual_increase_load` |
| **Performance under operations** | How much does a nemesis or a rolling upgrade hurt, and for how long? | `PerformanceRegressionTest.test_latency_{read,write,mixed}_with_nemesis`, `PerformanceRegressionUpgradeTest.test_latency_*_with_upgrade` |
| **Customer steady state** | What is latency for a specific customer workload, run on latte? | `PerformanceRegressionTest.test_latency_steady_state` |
| **Microbenchmarks** | Per-operation CPU cost, no cluster involved | `PerfSimpleQueryTest`, `PerfCqlRawTest` |

Plus one weekly Alternator job (`PerformanceRegressionAlternatorTest.test_full`), owned outside
the perf team.

Cadence and version routing for all of these: [pipelines-and-scheduling.md](pipelines-and-scheduling.md).

## 2. What is not maintained

These modules and methods still exist in the tree and still have test-case YAMLs, but they are
**not in any trigger matrix and have not been run for years**. Treat them as unsupported: there
is no current baseline in Argus to compare against, and nobody has verified they still pass.
Do not build new work on them without reviving and revalidating them first.

| Module / class | What it was for |
|---|---|
| `PerformanceRegressionTest.test_{write,read,mixed}`, `test_latency` | Classic fixed-load throughput and latency runs. Superseded by the throughput staircase and by the nemesis latency tests. |
| `PerformanceRegressionTest.test_mv_*`, `PerformanceRegressionMaterializedViewLatencyTest` | Materialized-view cost matrix. |
| `performance_regression_cdc_test.py` — `PerformanceRegressionCDCTest` | CDC overhead: same workload with CDC off, then on, incl. preimage/postimage and the CDC log reader. |
| `performance_regression_lwt_test.py` — `PerformanceRegressionLWTTest` | LWT (Paxos) cost, with its own metric lists and an `lwt_subtests` loop. |
| `performance_search_max_throughput_test.py` — `MaximumPerformanceSearchTest` | Iterative search for maximum sustainable throughput. Flagged in [strategy.md](../performance-testing-strategy.md) as never having proven useful; slated for a rebuild on latte. |
| `ycsb_performance_regression_test.py` — `BaseYCSBPerformanceRegressionTest` and its `{1M,10M,100M,1B}RecordsTest` subclasses | YCSB workloads a-f at four dataset sizes. |
| `performance_regression_row_level_repair_test.py` | Repair duration under various divergence patterns. Requires `SCT_HINTED_HANDOFF_DISABLED=true`, 3 nodes, RF=3. |
| `performance_scale_up_test.py` — `ScaleUpTest` | Ingest time plus `nodetool rebuild` duration. |
| `performance_regression_user_profiles_test.py` | Runs the commented-out `cassandra-stress` commands embedded in `cs_user_profiles` files. |
| `performance_regression_manager_backup_test.py` | Latency impact of a Scylla Manager backup, driven by the Manager-backup nemesis. Jenkinsfiles exist but no trigger entry; if it runs, it runs on the Manager team's schedule. |

---

## 3. The maintained modules in detail

### `performance_regression_test.py` — the base module

`PerformanceRegressionTest(ClusterTester, LoaderUtilsMixin)`. Everything else inherits from
this: preload, fstrim, compaction waits, `run_workload`, and the latency-decorator wiring.
See [anatomy.md](anatomy.md).

| Test method | What it does |
|---|---|
| `test_latency_{read,write,mixed}_with_nemesis` | fstrim -> preload -> wait for no compactions -> fstrim -> `run_workload(nemesis=True)`. One cycle per disruption. |
| `test_latency_steady_state` | Multiple stress commands, possibly different op types. Groups HDR tags per stress operation — this is the latte path, where Rune function names become HDR tags. |

`PerformanceRegressionUpgradeTest(PerformanceRegressionTest, UpgradeTest)` — latency during a
rolling upgrade, via `test_latency_{read,write,mixed}_with_upgrade`. `upgrade_node` is wrapped
in `@latency_calculator_decorator(legend="Upgrade Node")`, so each node's upgrade is one cycle.

### `performance_regression_gradual_grow_throughput.py` — the staircase

`PerformanceRegressionPredefinedStepsTest(PerformanceRegressionTest)`.

Each step runs at a fixed throttle from `perf_gradual_throttle_steps`; the last step is
unthrottled. Per-step latency is read from the stress HDR file and reported to Argus.
Test methods: `test_{mixed,write,read,read_disk_only}_gradual_increase_load`, one per workload
key (`read_disk_only` is sized so nothing is served from cache).

Supporting types in the same module: `CSPopulateDistribution` (gauss/uniform) and the
`Workload` dataclass (`cmd` template, warm-up cmd, `num_threads`, `throttle_steps`,
`preload_data`, `drop_keyspace`).

**Step naming** (`get_sequential_throttle_steps`, `_step_names`) — this determines the key your
threshold YAML must use:

- All steps share one thread count -> step name is the rate string (`"300000"`, `unthrottled`).
- Thread counts vary per step -> step name is `<rate>_<threads>_threads`.
- A name repeats -> suffixed with an occurrence counter (`unthrottled_1`, `unthrottled_2`).

### `performance_regression_alternator_test.py`

`PerformanceRegressionAlternatorTest(PerformanceRegressionTest)` — DynamoDB API via YCSB. All
methods delegate to `run_test_suite_by_configuration_name(mode)`; only `test_full` is
scheduled. The other methods (`test_latency*`, `test_throughput*`) select `basic-read`,
`basic-write`, `basic-mixed` and `basic-throughput` modes.

### `microbenchmarking_test.py`

`MicrobenchmarkTest(ClusterTester)` — base. Runs one `scylla perf-*` tool on a single DB node,
parses its JSON, calls `send_microbenchmark_result_to_argus`.

> Options must be passed as `--name value`, never `--name=value`.

| Class | Test methods | Notes |
|---|---|---|
| `PerfSimpleQueryTest` | `test_perf_simple_query` | `scylla perf-simple-query --json-result ... --smp 1 -m 1G`. In-process query processor; no ports or config collide with the live scylla-server. `perf_simple_query_extra_command: --write` selects the write workload. |
| `PerfCqlRawTest` | `test_read`, `test_write` | `scylla perf-cql-raw` — full networking and CQL frame parsing. Boots a real scylla in-process, writes its own `SCYLLA_CONFIG` under `--workdir /tmp/scylla-perf-cql-raw-workdir/`, **stops the node's scylla-server** for the run and restarts it in `finally`. |

`PerfCqlRawTest.RESOURCE_OPTIONS = "--smp 2 --cpus 0,1 -m 2G"` and `DURATION = 60` are
constants **because changing them invalidates the Argus baseline**.

---

## 4. Configuration layout

A perf run's config is a merge of one test-case YAML plus one or more overlay configs, listed in
the Jenkinsfile's `test_config` parameter. Later entries win. Merging is a recursive dict merge
(`anyconfig.merge(..., ac_merge=MS_DICTS)` via `merge_dicts_append_strings` in
`sdcm/sct_config.py`), so nested keys accumulate rather than replace — which is why the
`default` threshold block from `defaults/test_default.yaml` survives an overlay that only
defines per-step keys.

| Directory | Holds |
|---|---|
| `test-cases/performance/` | The test case: cluster shape, dataset size, stress commands, duration |
| `configurations/performance/` | Overlays: gradual load steps per (tool, hardware), `latency-decorator-error-thresholds-*` threshold files, microbenchmark thresholds under `perf_simple/` and `perf_cql_raw/` |
| `configurations/triggers/perf-regression.yaml` | The trigger matrix — see [pipelines-and-scheduling.md](pipelines-and-scheduling.md) |
| `configurations/stress_images/` | Pinned Docker tags per stress tool |
| `test-cases/microbenchmarking/` | `amazon_perf_{cql_raw,simple_query}_{ARM,x86}.yaml` |
| `test-cases/upgrades/rolling-upgrade-latency-regression*.yaml` | The rolling-upgrade latency test cases |

Naming conventions, which matter because the pipeline picks files by name:

- Load steps: `<tool>_gradual_load_steps_<hardware>.yaml`
- Thresholds: `latency-decorator-error-thresholds-{steps,nemesis}-<variant>-<topology>.yaml`
- Test cases within a family keep the family prefix (`perf-regression-latency-650gb-*`,
  `perf-regression-predefined-throughput-steps-*`, `latte-perf-regression-*`)

Tablets and vnodes are always separate job families with separate step and threshold files;
their numbers are never compared to each other.

For the full current inventory, list the directories — it changes too often to mirror here.
