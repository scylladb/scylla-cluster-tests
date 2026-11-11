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

**Best-throughput search** (SCT-580). When `perf_best_throughput_search` is set for a sub-test, the
sub-test runs no step of `perf_gradual_throttle_steps`/`perf_gradual_threads`. It runs fixed-rate steps whose
rate a search picks (`sdcm/utils/rate_search.py`):

1. Run the middle, on a log scale, of `min_rate_per_shard` and `max_rate_per_shard` x the DB nodes' shard count
   (default 1000 and 60000 per shard): `sqrt(min x max)`. With a baseline in
   `perf_best_throughput_baselines` for the DB instance family (`i8g` for `i8g.4xlarge`) and the sub-test, the
   first step runs at the baseline x the shards instead, and the bounds are `baseline_range_pct` (25%) below and
   above it, within the minimum and maximum rates.
2. Bisect between the highest passing and the lowest failing rate until they are within `precision_pct` (5), or `max_steps` (10, the bounds included)
   is used up: 7 steps with the default bounds, about 4 from a baseline. Between the minimum and maximum rates,
   which are about 60 times apart, the middle is on a log scale, `sqrt(low x high)`, so the first steps don't go
   to the top. Around a baseline, it is the plain middle, `(low + high) / 2`. Until a step passes, the minimum rate,
   or the baseline's lower bound, counts as the highest passing rate without being run, since almost every run
   passes it. Until a step fails, the maximum rate, or the baseline's upper bound, counts as the lowest failing
   rate without being run: a step at it overloads the cluster and leaves a compaction backlog for every later
   step.
3. Run the minimum or maximum rate only when the search narrows to it, or the minimum as the last step of the
   budget when no step passed. The minimum must pass there, else the
   search stops with `min rate fails` and the run reports an error. The maximum must fail there, which ends the
   search as `narrowed`; else the search stops with `max rate passes`: the best throughput is only a lower bound,
   and the run reports an error. A baseline bound runs the same way when the search narrows to it; when it is crossed,
   it moves out by another `baseline_range_pct`.

A step passes when the P99 of every operation is within `perf_gradual_latency_limit` (default 10 ms) and its
HDR-measured throughput is at least `min_sent_ratio` (0.98) of its rate. A step that measured less fails,
whatever its P99: latte measures the latency from each request's scheduled start, so a loader that lags goes
over the limit too. A loader over `max_loader_cpu_pct` (90%) CPU during a step, read from its node_exporter on
the monitor, stops the search with an error and no best-throughput row: the step measured the loaders, not the
cluster. Raise `n_loaders`. Without a monitor, the check is skipped with a warning, as is a loader the monitor has
no CPU samples for. Each step gets `rate x P99 limit` requests in flight (Little's law), split over the commands:
latte keeps the threads of the sub-test's first `perf_gradual_throttle_steps` entry and gets the requests split
over them as its concurrency, which is per thread; other tools get them as threads. The cache warmup uses the threads
and concurrency of that first entry.

A step that collected no results (e.g. a failed latency collection) runs once more at the same rate, named
`rate_<rate>_retry`; the rerun doesn't use the step budget. If that has no results either, the search stops
with `no results`, the run reports an error, and no best-throughput row goes to Argus.

Steps are named `rate_<rate>`; the best passing one is reported as `BestThroughputResult`.
`configurations/performance/best-throughput/best_throughput.yaml` turns the search on for every sub-test with
no per-step P99 limits (steps over the limit are expected), and sets 30-minute steps for write and mixed and
10-minute steps for read and read_disk_only. On the
latte test-case, `latte_time_bound_write.yaml` makes the write steps time-bound too, so a step writes
rate x duration rows instead of the whole 1.6B-row key range.

**Goodput under overload** (`perf_overload_steps`, `sdcm/utils/overload.py`, SCYLLADB-3267). After the
best step, one throttled step per factor (`overload_1.3x`, `overload_2x`) offers that multiple of the best
throughput; the goodput (successful operations) is reported as a ratio of the best throughput, and a ratio
under `min_goodput_ratio` (0.9) fails the run. Each step also reports the rate it really attempted; a
closed-loop tool can fall short of the target, and a step under 0.95 of it logs a warning. See
[results-and-regression.md](results-and-regression.md#goodput-under-overload-table).

**Dataset sizing** (`perf_dataset_sizing`, `sdcm/utils/dataset_sizing.py`). The predefined-steps
test-case's stress commands are the dataset of a reference node: 128 GiB RAM and a 3750 GB data disk
(i4i.4xlarge, i8g.4xlarge). With `perf_dataset_sizing` set, `setUp` reads the smallest DB node's
`MemTotal` and `/var/lib/scylla` size and scales each command's dataset by them, rounded to `scale_step`
(0.25), so the reference instance types keep today's dataset exactly. For cassandra-stress that is `n=` and
the `-pop seq=` range; for latte, the `--start-cycle`/`--end-cycle` range, `-P row_count`, `-P offset` and a
`--duration` given as a cycle count:

| Commands | Scale | Why |
|---|---|---|
| `stress_cmd_r`, `stress_cmd_m`, `stress_cmd_cache_warmup` | RAM | The hot range must keep fitting in cache |
| `prepare_write_cmd`, `stress_cmd_read_disk` | smaller of RAM and disk | Several times RAM so disk reads miss the cache, but never outgrowing the disk |
| `stress_cmd_w` | disk | Leaves the same share of the disk free for compaction |

The preload's literal `throttle=` and `threads=` (latte: `--rate` and `--threads`) scale with its rows, so it takes as long as on the
reference node (~72 minutes) and fits `prepare_stress_duration` and the test's setup allowance on any
node size; per DB shard it writes at about the reference rate, as i4i and i8g have the same RAM per vCPU.
When the disk limits the preload below the RAM scale, the run logs a warning: disk-only reads then
cover less data per GiB of RAM than on the reference node.
`configurations/performance/best-throughput/sized_dataset.yaml` turns sizing on.

**One command per loader** (`split_stress_commands`). The test-case's 4 commands of a workload split one
key range for 4 loaders, and round-robin runs one command per loader. When the rate search runs on
another `n_loaders`, the workload's commands and its cache warmup are split again over the same range, one
command per loader, so every loader generates load and a step's rate, divided by the number of loaders, adds
up to what it asks. A split latte command gets the offset and the cycle-count duration of its own range.
Commands that differ in more than their range run as written, with a warning.

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
