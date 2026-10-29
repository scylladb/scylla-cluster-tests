---
status: draft
domain: stress-tools
created: 2026-10-19
last_updated: 2026-10-29
owner: juliayakovlev
---

# Best Throughput Under a Latency Limit, On Demand

Jira: [SCT-580](https://scylladb.atlassian.net/browse/SCT-580)
Related: [SCYLLADB-3267](https://scylladb.atlassian.net/browse/SCYLLADB-3267) (goodput under overload)

## Problem Statement

The question the gradual performance tests should answer for the `read`, `write`, `mixed` and
`read_disk_only` workloads is: **what is the highest throughput the cluster sustains while P99 latency
stays under 10 ms?** It is asked for a specific setup: a DB instance type, a node count, a Scylla version.

Today nobody computes that number, and it cannot be asked for a new setup:

- The predefined-steps jobs run fixed throttled rates followed by one unthrottled step. A person reads one
  Argus table per step and picks the last step whose P99 is under 10 ms
  ([example](https://argus.scylladb.com/tests/scylla-cluster-tests/1dfde12a-3e8c-4d6e-8a8e-0c1d88b5a939)).
- A throttled rate only tells whether the cluster can serve *that* rate. The throttle grid is calibrated per
  instance type by hand: i8g.4xlarge serves about 2x the rates of i4i.4xlarge on the same dataset and thread
  counts, so the two have separate step lists (`cassandra_stress_gradual_load_steps_enterprise.yaml` vs
  `cassandra_stress_gradual_load_steps_i8g.yaml`). The same holds for any fixed thread-count grid.
- Every instance type needs its own pre-created Jenkins job and calibrated config. Answering the question for
  an instance type nobody prepared means hand-writing both.
- There is no single "max throughput under 10 ms" value per run, so Argus cannot track it across Scylla
  versions or apply a validation rule to it.
- The dataset is a fixed set of key ranges sized for 128 GiB RAM / 3750 GB disk nodes. On an instance with
  different memory or disk, the read workloads no longer test what they are named for (cache vs disk).

## Current State

- `performance_regression_gradual_grow_throughput.py:PerformanceRegressionPredefinedStepsTest`
  - `run_gradual_increase_load` runs every step from `perf_gradual_throttle_steps` through `run_step`
    wrapped in `sdcm/utils/decorators.py:latency_calculator_decorator`, which sends one Argus latency table
    per step.
  - Steps can already be **unthrottled with different thread counts**: a step with rate `unthrottled` (or a
    dict step without `rate`) gets no throttle, and `perf_gradual_threads` may be a list with one thread
    count per step. `get_sequential_throttle_steps` then names steps `unthrottled_<threads>_threads`. The
    list is fixed before the run starts.
  - `check_latency_during_steps` collects each step into `total_summary`: the HDR P99 of every operation and
    the HDR-measured throughput (`cycle_hdr_throughput`). `save_total_summary_in_file` writes it to
    `<logdir>/result_gradual_increase.log`.
  - The `Workload` dataclass controls what carries over between steps: `drop_keyspace` (write recreates the
    schema per step), `preload_data`, `wait_no_compactions`, and `cs_cmd_warm_up` (read and
    mixed; read_disk_only has none).
- Dataset: `test-cases/performance/perf-regression-predefined-throughput-steps.yaml` defines it as fixed
  cassandra-stress `-pop seq=` ranges, the same for i4i and i8g (1 KB rows, RF=3 on 3 nodes, so every node
  holds a full copy):

  | Workload | Key range | Per node | vs 128 GiB RAM | vs 3750 GB disk |
  |---|---|---|---|---|
  | preload (read, mixed, read_disk_only) | 650M rows | ~650 GB | ~5x | ~17% |
  | read, mixed (hot range) | 20M rows | ~20 GB | ~16% | |
  | write (no preload, runs to completion) | 1.61B rows | up to ~1.6 TB | | ~43% |

  i4i.4xlarge and i8g.4xlarge both have 16 vCPU, 128 GiB RAM and 3750 GB local NVMe, so the ratios hold on
  both. Other instance types (e.g. z4d-highmem-16) reuse the same ranges.
- Unthrottled behaviour at today's thread counts, from the predefined-steps jobs (median of the latest
  3-5 runs, 2026-10), shows that one thread count does not fit both instance types:

  | Workload (threads per loader) | i4i.4xlarge | i8g.4xlarge |
  |---|---|---|
  | read (620) | 869k op/s, P99 42 ms | 1.67M op/s, P99 5.4 ms |
  | read_disk_only (620) | 191k op/s, P99 208 ms | 574k op/s, P99 49 ms |
  | mixed (1900) | 537k op/s, P99 read 477 ms | 875k op/s, P99 read 200 ms |
  | write (400) | no recent results | 708k op/s, P99 6.5 ms |

- Node facts available at run time: `sdcm/cluster.py:BaseNode.scylla_shards` and `BaseNode.cpu_cores`.
  On AWS, `SCTConfiguration` resolves the image architecture from `instance_type_db`
  (`get_arch_from_instance_type`), so an ARM instance type needs no extra config.
- Jobs: `vars/perfRegressionParallelPipeline.groovy` runs the sub-tests in parallel and already takes
  `test_config`, `sub_tests` and `extra_environment_variables` as build parameters.
  `utils/build_system/create_test_release_jobs.py:JenkinsPipelines` creates folders and pipeline jobs that
  point at a jenkinsfile in the SCT repo (used by `sct.py create-performance-jobs`). Argus keeps result
  history per Jenkins job.
- `performance_search_max_throughput_test.py:MaximumPerformanceSearchTest` (jobs
  `jenkins-pipelines/qa/perf-search-throughput-best-config{,-gce}.jenkinsfile`) already searches for the best
  throughput: it raises the cassandra-stress threads by a fixed step, then lowers the processes and loaders once
  throughput stops growing. It applies no latency limit and reports nothing to Argus, only
  `best_stat_result.json` files. This plan doesn't change it; see the follow-up below.
- `sdcm/sct_config/mixins/performance.py` defines the `perf_gradual_*` parameters;
  `sdcm/sct_config/config.py:SCTConfiguration._validate_perf_gradual_throttle_steps` validates them.
- `docs/performance/results-and-regression.md` and `docs/performance/runbook-recalibrate-steps.md` describe
  how step results are read and how steps are chosen today.

## Goals

1. A user asks for the best throughput of a setup by starting **one launcher job** with a short list of
   parameters (DB instance type, Scylla version, ...). No job or config is prepared per instance type.
2. The launcher creates a job for that hardware configuration the first time it is requested and reuses it
   afterwards, so each configuration builds its own Argus history across Scylla versions.
3. The run needs no calibrated grid: it searches **unthrottled steps that differ only by thread count**,
   starting from the instance's CPU, and narrows the result to within 5% of the thread count at the limit.
4. Every run reports one number per workload: the highest HDR-measured throughput (op/s) of a step whose P99
   of every operation is within the configured limit (default 10 ms), plus the thread count that reached it.
5. The dataset is sized from the DB nodes' memory and disk; i4i.4xlarge and i8g.4xlarge keep today's sizes.
6. Existing predefined-steps (throttled) jobs keep their steps, step names and Argus history. They gain one
   informational table (Phase 2) that carries no validation rules, so it never changes their status.
7. Optionally, run the overload steps of SCYLLADB-3267 on top of the measured best throughput: goodput at
   1.3x and 2x the best throughput, reported as a ratio to it (expected >= 0.9).

## Relation to SCYLLADB-3267

SCYLLADB-3267 asks for an SCT-level goodput suite: measure the maximum throughput under 10 ms, then the
goodput at 1.3x and 2x that throughput, with goodput >= 0.9 x maximum expected. It also asks to cover CQL
and Alternator, INSERT/UPDATE/DELETE/SELECT and LWT variants, and to count thrown exceptions per failed op.

| SCYLLADB-3267 part | Where |
|---|---|
| Max throughput under 10 ms | This plan, Phases 1-5 |
| Goodput at 1.3x and 2x of it, ratio >= 0.9 | This plan, Phase 6 |
| Bounded timeouts and concurrency under overload | This plan, Phase 6 |
| Exceptions per failed op | This plan, Phase 6 (**Needs Investigation** per stress tool) |
| CQL INSERT/SELECT (cassandra-stress write/read/mixed) | Covered by the existing workloads |
| CQL UPDATE/DELETE and LWT variants | Follow-up: new workloads on the same mechanism; the LWT predefined-steps test-cases are the starting point |
| Alternator | Follow-up: needs an Alternator stress tool wired into the gradual test |

The follow-ups reuse Phases 1-6 unchanged; they only add workloads, so they belong in their own plan.

A further follow-up, also in its own plan: retire `MaximumPerformanceSearchTest`, or rebuild it on this
search. Its question (the highest throughput with no latency limit) is this search with the limit off, which
stops on saturation or the thread cap.

## How a Request Flows

```
best-throughput-request            launcher job; the user fills in the request parameters
  1. validate the request          instance type exists in the region, architecture, required fields
  2. derive the job name           from the hardware parameters, e.g. aws-i8g.4xlarge-3n-tablets
  3. create the job if missing     from the generic jenkinsfile; reused by later requests
  4. wait for its Argus test       Argus imports new Jenkins jobs every 5 minutes
  5. start it                      without waiting
        |
        v
<generated job>                    perfRegressionParallelPipeline, one sub-test per workload in parallel
  size the dataset from node RAM/disk -> preload -> adaptive thread search -> BestThroughputResult
```

## Running It

The run procedure, the launcher's parameters and how to read the results are in
`docs/performance/runbook-best-throughput.md` (Phase 7). In short: after the implementation PR (#16567)
merges and is backported to `branch-perf-v17`, `create-performance-jobs` creates the launcher; a request is
the launcher's build with at least `instance_type_db` and a Scylla version. The first runs on i4i and i8g
calibrate the starting threads per shard (Phase 3).

## Implementation Phases

### Overview

1. **Latency limit and best-step selection**
   - Add `perf_gradual_latency_limit`, set per workload, defaulting to P99 of 10 ms, and validate it.
   - Add `find_best_throughput()`: a step passes when every operation's P99 is within the limit; it returns
     the passing step with the highest measured throughput.
2. **Report the best step**
   - Record each step's thread count.
   - After all steps, pick the best step and send it to the new `BestThroughputResult` Argus table. If
     nothing passed, send throughput 0.
   - Write the best step to `result_gradual_increase.log`.
3. **Adaptive thread search**
   - Add `perf_best_throughput_search`, with starting threads per shard, a step limit and a precision setting.
   - Start from the DB nodes' shard count.
   - Double the threads until P99 goes over the limit, throughput stops growing or a thread cap is reached;
     the step limit never stops this part.
   - Bisect between the last pass and the first fail until within 5% or the step limit (default 10).
4. **Dataset sized from instance memory and disk**
   - Read RAM and data-disk size from the DB nodes.
   - Scale the test-case's key ranges by them relative to a 128 GiB / 3750 GB reference node: the hot range
     by RAM, the preload and disk reads by the smaller of RAM and disk, the writes by disk.
   - Rounding the scale to 0.25 keeps today's dataset exactly on i4i and i8g.
5. **On-demand launcher and generated jobs**
   - Add a `scylla-enterprise/perf-regression/best-throughput/` folder on `branch-perf-v17` with the launcher and
     the generic jenkinsfiles.
   - Add `sct.py create-best-throughput-job`. It validates the request, builds the job name, assembles
     `test_config` and the environment variables, and creates the job if it's missing.
   - The launcher waits until Argus has a test for the generated job, then starts it.
6. **Goodput under overload** (SCYLLADB-3267, optional)
   - After the best throughput is found, run throttled steps at 1.3x and 2x of it, with timeouts and threads
     capped.
   - Report goodput (the rate of successful operations) as a ratio of the best throughput, plus failed
     operations. Argus fails the run if the ratio drops below 0.9.
7. **Documentation**
   - Document the result table and pass rules, the launcher parameters, and how to request a run.

### Phase 1: Latency limit config and best-step selection

**Importance:** Critical
**Dependencies:** none

- New config `perf_gradual_latency_limit`, per workload, default `{"default": {"percentile_99": 10}}`.
  A workload key overrides `default`. Validated next to `_validate_perf_gradual_throttle_steps`.
- New pure function `find_best_throughput(total_summary, p99_limit_ms)`:
  - A step **passes** when the HDR P99 of every operation, in every cycle that reported it, is within the
    limit. For mixed, read and write must both pass.
  - Passing steps are **ranked by measured throughput** (`cycle_hdr_throughput`). Past saturation, more
    threads add latency without adding throughput, so the best step is not necessarily the one with the
    most threads.
  - Steps with empty or failed latency results are skipped.
  - Returns the best step (name, thread count when the step records it, throughput, P99 per operation) or
    "none passed".

**Definition of Done:**
- [ ] `perf_gradual_latency_limit` defined, defaulted and validated; config docs regenerated
- [ ] `find_best_throughput` covered by the unit tests listed in Testing Requirements

### Phase 2: Report the best step

**Importance:** Critical
**Dependencies:** Phase 1

- `run_gradual_increase_load` records each step's thread count in its summary entry and calls
  `find_best_throughput` after the steps.
- New Argus table `BestThroughputResult` in `sdcm/argus_results.py`: best throughput (op/s,
  `higher_is_better=True`), threads, the step name, P99 read/write at that step, the limit used, and, for a
  search run (Phase 3), why the search stopped. One row per workload. "None passed" is an explicit row with
  throughput 0, not an empty table.
- The result is also written to `result_gradual_increase.log`, before the Argus send.
- An Argus error in the send (a 5xx, an unreachable server) is logged as a warning and doesn't fail the test:
  the send runs at the end of every gradual job, existing ones included, and the Argus client doesn't retry
  5xx responses. A validation failure still fails the run, as for every result table.
- Email: Argus renders the email reports (`argus_email_report_template`), so the table reaches them there;
  no SCT change.
- Reporting also runs for the existing throttled jobs, where it reads as "best step on the throttle grid".
  The table carries no validation rules and every cell is sent as `UNSET`, so a "none passed" row (throughput
  0) can't turn an existing job red. Rules, if any, belong on the generated jobs only.

**Definition of Done:**
- [ ] One `BestThroughputResult` row per workload in a real run
- [ ] `result_gradual_increase.log` contains the best step
- [ ] An Argus error in the send leaves the test passing, with the results log written
- [ ] Minicloud run of a gradual-steps test shows the table (latency values there are meaningless)

### Phase 3: Adaptive thread search

**Importance:** Critical
**Dependencies:** Phase 2

Replaces a fixed `perf_gradual_threads` list for best-throughput runs. Existing jobs keep their fixed steps;
the search runs only when its config is set.

- New config `perf_best_throughput_search`, per workload with a `default` entry: starting threads per Scylla
  shard, maximum number of steps, the precision to stop at (default 5%), and a cap on threads per stress
  command (default 4096).
- The run reads the shard count of the DB nodes and starts at `threads per shard x total shards / stress
  commands` threads per stress command. Today's test-case runs one command per loader, so the two counts
  match with 4 loaders, but not when `n_loaders` differs.
- **Expand:** double the thread count each step, until a step fails the latency limit, throughput grows by
  less than 5% over the previous passing step (saturated: more threads only queue), or the threads reach the
  cap. The cap is about 2x the highest thread count of today's fixed steps (1900 per command, mixed); it bounds
  doubling when loaders far outpace the cluster. The step budget does not stop this phase: stopping before
  the limit is found would report a thread count that isn't the best.
- **Narrow:** bisect the thread count between the last passing and the first failing step. A pass moves the
  lower bound up, a fail moves the upper bound down. Stop when the gap is under the precision or the step
  budget (default 10 steps per workload, counting the expand steps) runs out.
- After a saturation or cap stop no thread count has failed, so there is nothing to bisect: the search ends,
  and the best passing step is the result.
- The `BestThroughputResult` row says why the search stopped: `narrowed`, `saturated`, `thread cap`, `budget`
  or `none passed`. A cap stop means every step up to the cap stayed within the limit, so its throughput is only
  a lower bound, and the row must not read as the limit. On larger clusters, raise the cap to find the limit.
- Every step is unthrottled and named `unthrottled_<threads>_threads`, the name a fixed list would give it;
  only a rerun step (below) adds `_retry`.
  `find_best_throughput` then picks the best step from all of them (Phase 1).
- When the first step already fails, halve instead of doubling, down to one thread per stress command.
- **A step with no results counts as failed**, like a step over the limit. Since one transient collection
  error would then end doubling early and report a low best throughput, such a step first runs once more at
  the same thread count, named `unthrottled_<threads>_threads_retry`. It counts as failed only if the rerun has
  no results either. A step over the latency limit is never rerun, and the rerun doesn't use the step budget.
- Each step is time-bound (`perf_gradual_step_duration`), including write: an n-bound write step at a low
  thread count would take hours. Per-step P99 validation rules are off for these runs, since steps over the
  limit are expected.
- Workload state between probes (a lower thread count can run after a higher one):
  - write: keyspace dropped and schema recreated per step, each probe starts clean.
  - read: preloaded data, compaction wait between steps.
  - read_disk_only: same as read, with no warmup and no cache drop, as in today's steps. The cache state
    between probes doesn't matter: the dataset is ~5x RAM, so the cache holds at most ~20% of the rows, and
    most reads miss it whatever the previous probe left behind.
  - mixed: dataset grows with every step. Keep the compaction and tablet-split waits and note it in the
    report.
- **Needs Investigation:** the default starting threads per shard. From the table in Current State, i8g read
  passes at ~59 threads per shard (620 x 4 loaders over ~42 shards), and read_disk_only and mixed fail at
  today's counts on both instance types. Calibrate on i4i and i8g in the first runs.
- **Run length and cost.** A workload runs at most `max(10, steps through the first failing step)` steps,
  plus one rerun per step without results; a saturation or cap stop ends earlier. Doubling is bounded by the
  thread cap: on i8g.4xlarge (~42 shards, 4 commands) the first step is 168 threads per command and the cap
  is reached in 6 steps, so the default budget of 10 is the bound there. Each 30-minute step is followed by
  waits for compactions (at least 3 minutes) and tablet splits (up to 6), so a default run takes ~7-8 h per
  workload with the preload. The four workloads run in parallel on their own clusters, so a request is up to
  ~30 cluster-hours: roughly $300 at on-demand prices for 3 i8g.4xlarge DB nodes and 4 c7i.8xlarge loaders
  per cluster.
- The run must not hit its test timeout before it reports. The predefined-steps test-case's
  `test_duration` (500 minutes) is too short for a search, so the launcher sizes it (Phase 5).
- **Needs Investigation:** noise. Near the limit, options are to rerun the final passing step once, or to
  require P99 <= 0.9 x limit. The saturation check also compares single samples against a 5% threshold, inside
  the noise of repeated runs (6-9% on i8g writes), so it can stop doubling early. Decide both from the first
  runs.

**Definition of Done:**
- [ ] The search is covered by unit tests against fake latency curves, including a rerun step without
      results and the thread cap
- [ ] On i4i.4xlarge and i8g.4xlarge, every workload ends with a passing and a failing step within 5% of
      each other, or a documented reason (saturated, budget, thread cap)
- [ ] A cap stop is visible as `thread cap` in the `BestThroughputResult` row

### Phase 4: Dataset sized from instance memory and disk

**Importance:** Critical
**Dependencies:** Phase 2 (can run in parallel with Phase 3)

Required for on-demand runs: the instance type is not known when the test-case is written.

- Measure on the DB nodes after provisioning: RAM (`MemTotal`) and the Scylla data-disk size, and size from
  the smallest node. This works on every backend, including baremetal and minicloud.
- The test-case's stress commands are the dataset of a **reference node** (128 GiB RAM, 3750 GB disk:
  i4i.4xlarge and i8g.4xlarge). Their `n=` row counts and `-pop seq=` ranges are scaled by the node's RAM and
  disk relative to the reference:
  - read and mixed hot range, cache warmup: by RAM, so the hot range keeps fitting in cache (~16% of RAM).
  - preload and read_disk_only: by the smaller of RAM and disk, so reads miss the cache (~5x RAM) without the
    dataset outgrowing the disk.
  - write: by disk, leaving the same share of the disk free for compaction (~43% used).
- Each scale is rounded to a step (0.25). The measured `MemTotal` of a 128 GiB node is ~125 GiB, so the
  reference instance types round to 1 and keep exactly today's ranges. Ratios applied to measured values
  could not reproduce today's numbers exactly, which is why the reference commands are scaled instead.
- A scale never drops below one step (0.25): without the floor, a node under ~16 GiB RAM would round to 0 and
  run no dataset. Such a node gets the dataset of a 32 GiB node, so its hot range takes a larger share of RAM
  than on the reference node.
- Scaled ranges stay contiguous across the per-loader commands; commands with neither a range nor a row
  count (e.g. latte) are left as they are.
- New config `perf_dataset_sizing` (`reference_ram_gib`, `reference_disk_gb`, `scale_step`); unset keeps
  the commands as written. When the disk caps the preload below the RAM scale, the run logs a warning.

**Definition of Done:**
- [ ] On i4i.4xlarge and i8g.4xlarge every sized command equals today's
- [ ] On an instance with different RAM/disk, the dataset scales by RAM and disk as above

### Phase 5: On-demand launcher and generated jobs

**Importance:** Critical
**Dependencies:** Phases 3 and 4

- **Location:** a new
  `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/best-throughput/` directory
  holds two jenkinsfiles, the launcher and the generic job. `sct.py create-performance-jobs` already maps
  directories under `branch-perf-v17` to Jenkins folders, so it creates both in the
  `scylla-enterprise/perf-regression/best-throughput/` Jenkins folder, and the generated jobs are created in
  that folder too. All of them run from the `branch-perf-v17` branch.
- **Argus tests:** a run reports results only to a job Argus knows as a test, and Argus creates tests by
  scanning the Jenkins folders in its `JENKINS_MONITORED_RELEASES` every 5 minutes. The folder sits under
  `scylla-enterprise`, which Argus scans with all its subfolders. A top-level `best-throughput/` folder would
  never be scanned. A launcher in a staging folder works the same way, since `scylla-staging` is scanned too.

  ```
  scylla-enterprise/perf-regression/best-throughput/   (Jenkins folder)
    best-throughput-request                      launcher, created by create-performance-jobs
    best-throughput-generic                      generic job, created by create-performance-jobs
    aws-i8g.4xlarge-3n-tablets                   generated on the first request for this configuration
    aws-i4i.4xlarge-3n-tablets                   ...
  ```

- **Launcher job** `best-throughput-request`: a pipeline job whose parameters are the request, in four groups.
  The runbook lists each parameter with its default.
  - **Job identity** (`backend`, `instance_type_db`, `n_db_nodes`, `tablets`, `job_name_suffix`): forms the job
    name, so it decides which runs share an Argus history.
  - **Setup** (`test_config`, `extra_environment_variables`, `region`, loaders, `sub_tests`,
    `provision_type`): passed to every build, so rebuilding the job's last build repeats the setup. Loader
    parameters are passed only when set, so a stress-tool file that picks its own loader type keeps it. The
    region defaults to us-east-1, where the i4i and i8g predefined-steps jobs run, so results compare with
    theirs.
  - **Search and result** (`p99_limit_ms`, `step_duration`, `start_threads_per_shard`, `max_search_steps`,
    `search_precision_pct`, `overload_factors`): `p99_limit_ms`, `step_duration` and `overload_factors` each
    become one `SCT_*` variable; the three search knobs together become the `SCT_PERF_BEST_THROUGHPUT_SEARCH`
    JSON. They are named parameters,
    rather than examples for `extra_environment_variables`, so a request needs no knowledge of the SCT
    variables and the launcher validates the values. Empty means the best-throughput overlay's default.
  - **Per run only** (`scylla_version` or an image parameter, `requested_by_user`, `email_recipients`): not
    part of the job name, so one job's history spans Scylla versions.

- **`test_config` and `extra_environment_variables`** carry everything the structured parameters don't, such
  as `configurations/c-s-driver-version-4.yaml` or `SCT_C_S_DRIVER_VERSION=4`. The launcher builds the final
  values from them:
  - `test_config`: the request's list, then `configurations/tablets_disabled.yaml` when `tablets` is false,
    then the best-throughput overlay (below). The overlay always comes last, so no file in the request can
    turn the search or its validation settings off.
  - `extra_environment_variables`: the request's text, then the `SCT_*` variables of the structured
    parameters above. The structured parameter wins when both set the same variable.
  - The job name has to describe what runs, so the launcher rejects a request whose
    `extra_environment_variables` sets an identity variable (`SCT_INSTANCE_TYPE_DB`, `SCT_N_DB_NODES`) to a
    different value than the structured parameter.
  - Free-form text cannot name a job reliably, so a different `test_config` or extra variable on the same
    hardware lands in the same job. Use `job_name_suffix` when the change should get its own history.

- **Default `test_config`**: the `test_config` of the i8g tablets predefined-steps job, without its per-step
  thresholds and instance-type files. Its step grid is inert here, since the search ignores it, and the
  launcher's environment variables take precedence over the instance types in its files.
  `extra_environment_variables` is empty by default: the instance type and node count have their own
  parameters. Variables left empty in it are ignored.

- **Best-throughput overlay**, appended by the launcher to every request:
  `configurations/performance/best-throughput/best_throughput.yaml`. It holds the search config, 30-minute
  steps for every workload and per-step P99 limits off. Overlays that rewrite stress commands fit only the
  default cassandra-stress test-case, so the launcher adds them only when the request uses it; other
  test-cases keep their own commands and datasets:
  - `cassandra_stress_time_bound_write.yaml` (Phase 3): the write commands with `duration=$duration` instead of
    `n=`, since an n-bound write step at a low thread count would take hours;
  - the sized dataset (Phase 4).

  Backend defaults beyond AWS get their own overlay when GCE and Azure are added.

- The launcher runs a new command, `sct.py create-best-throughput-job`, which:
  - validates the request (required fields, instance type known in the region, architecture, conflicting
    identity variables);
  - derives the job name from the identity parameters, e.g. `aws-i8g.4xlarge-3n-tablets`;
  - builds the final `test_config` and `extra_environment_variables`;
  - creates the job in the launcher's folder if it does not exist, through `JenkinsPipelines` with
    `branch-perf-v17` as the SCT branch, pointing at the generic job's jenkinsfile, and waits for its build #1,
    which only loads the job's parameters and aborts itself;
  - waits until Argus has a test for the job, checked through the Argus test search. The first run of a new
    configuration would otherwise start before the next scan: the search would run in full (the decorator
    keeps each step's latencies, below), but every result submit, including the final `BestThroughputResult`,
    would fail, so the workload would end after hours of stress with nothing in Argus. The wait is up to 30
    minutes; past it the launcher fails without starting the job, and a rerun reuses the created job. A failed
    lookup counts as another poll. The check needs no Argus admin rights.
- The launcher then starts the generated job with all of the request's parameters and does not wait for it.
  `perfRegressionParallelPipeline` already takes `test_config`, `sub_tests` and `extra_environment_variables`,
  so it is unchanged.
- `latency_calculator_decorator` writes a step's latencies to the results file before sending them to Argus.
  Otherwise a failed submit would drop the step, and the search would count it as failed. A step whose results
  file is missing (the decorator failed to collect it) counts as a step without results, so it is rerun.
- The launcher sets `SCT_TEST_DURATION` to the longest run the settings allow: 180 minutes for setup, preload
  and teardown, plus the step duration and 10 minutes of waits for each of up to max(`max_steps`, the steps
  from 1 thread to the thread cap) search steps, 2 reruns and one step per overload factor; 780 minutes at
  the defaults. The budget doesn't cut doubling, so the step count is bounded by the cap, not the budget. A
  request that sets `SCT_TEST_DURATION` keeps it. The perf pipeline computes its Jenkins timeouts after loading
  these variables, so they follow.
- Generated jobs keep no per-job parameter defaults: a pipeline's `parameters` block resets them on every
  build. Every request passes its full setup instead, and repeating a run means rebuilding the job's last
  build. A request with a different setup on the same hardware runs in the same job; `job_name_suffix`
  separates it when it should get its own history.
- The generic job can also be run directly with the same parameters, for a one-off run that should not get
  its own job and history.
- Since the jobs run from `branch-perf-v17`, the implementation PR (#16567, all phases) is backported there
  (`backport/perf-v17`) before the launcher is usable.
- Jenkins credentials: job creation goes through `JenkinsPipelines`, which reads only `jenkins.json` from the
  KeyStore; starting the job goes through the trigger matrix's client, which also accepts the `JENKINS_*`
  variables. The launcher's Groovy writes the request to a JSON file for the command, so user text never goes
  through shell quoting.
- **Needs Investigation:** those credentials can create and reconfigure jobs anywhere in Jenkins, so anyone
  who can build the launcher gets job creation through it. Options: a Jenkins user whose rights are scoped to
  the best-throughput folder, or restricting who can build the launcher.
- Backends: AWS first. GCE and Azure need their instance-type variables and default regions and loaders.

**Definition of Done:**
- [ ] `create-performance-jobs` creates the `scylla-enterprise/perf-regression/best-throughput/` folder with the
      launcher and the generic job
- [ ] A request for a new instance type creates its job in that folder, waits for its Argus test and starts it;
      a second request reuses the job
- [ ] The first run of a new configuration reports its results to Argus
- [ ] Generated jobs for i4i.4xlarge and i8g.4xlarge report `BestThroughputResult` for all four workloads
- [ ] An invalid request fails in the launcher, before anything is provisioned
- [ ] A run at the defaults, and one with overload factors, ends before its test timeout

### Phase 6: Goodput under overload (SCYLLADB-3267)

**Importance:** Medium
**Dependencies:** Phase 3

- New config `perf_overload_steps`, off by default, per workload with a `default` entry: `factors`
  (`[1.3, 2]`), `max_threads_multiplier` (4) and `min_goodput_ratio` (0.9). The launcher's `overload_factors`
  parameter sets the factors.
- After the best throughput is found, run one step per factor at a **rate derived from the measured best
  throughput** (factor x best), throttled through the existing rate path. The rates follow the run's own
  best throughput, so nothing is calibrated per instance type.
- Thread count for overload steps: the best step's thread count times the factor, capped at
  `max_threads_multiplier` times it, so concurrency does not explode while the cluster sheds load.
- The stress tool keeps running on timeouts and errors instead of aborting the step: cassandra-stress commands
  get `-errors ignore` for overload steps. cassandra-stress still logs every operation that fails all its
  retries as a CRITICAL `OperationOnKey` event, which would stop the test at the first one, so the overload
  steps report a failing stress command as an error instead (the run then ends in ERROR, with its goodput sent).
  **Needs Investigation:** the same for latte, cql-stress and
  scylla-bench (until then the run warns that they may stop on errors), and client-side request timeouts
  (today requests are bounded by Scylla's server-side timeouts).
- Goodput is the HDR-measured throughput of the step, which counts successful operations only. Report per
  factor: offered rate, goodput, goodput as a ratio of the best throughput, P99, and failed operations (the
  sum of cassandra-stress `Total errors`, recorded for every step). **Needs Investigation:** exceptions per
  failed op, which cassandra-stress does not break down.
- Results go into a separate Argus table, one row per factor, with a validation rule of ratio >= 0.9. As in
  Phase 2, the results log is written before the send, and an Argus error in it doesn't fail the test.
- Overload steps are named `overload_<factor>x` and run after the search, so they don't affect the best
  throughput. For write and mixed, the dataset grows further; keep the compaction and tablet-split waits.

**Definition of Done:**
- [ ] With overload enabled, each workload reports goodput at 1.3x and 2x of its best throughput
- [ ] The stress tools keep running through timeouts, and failed operations are counted; failed operations
      don't stop the test
- [ ] The ratio >= 0.9 rule fails the run in Argus when goodput drops below it

### Phase 7: Documentation

**Importance:** High
**Dependencies:** delivered with Phases 2-6

- `docs/performance/results-and-regression.md`: `BestThroughputResult`, the pass rules, the goodput table.
- `docs/performance/test-catalog.md` and `docs/performance/runbook-best-throughput.md`: the one-time setup,
  how to request a run, every launcher parameter with its default, and where the results land.

**Definition of Done:**
- [ ] Docs updated in the same PRs as the behaviour they describe

## Testing Requirements

**Unit tests** (`unit_tests/unit/`):
- `find_best_throughput`: mixed with only one operation over the limit; ranking by measured throughput when
  more threads reduce throughput; limit is inclusive; operations reported only by some cycles; empty or
  failed latency results skipped; "none passed" and empty summary.
- Config: `perf_gradual_latency_limit` and `perf_best_throughput_search` defaults, per-workload override,
  invalid values rejected (extend `test_perf_gradual_config.py`).
- Search, against fake latency curves: starting point from shard count; doubling until the limit, until
  saturation, never stopped by the budget; halving when the first step fails; bisection stops at the
  precision or the budget; unique `unthrottled_<threads>_threads` names; a step without results rerun once,
  outside the budget, and counted as failed only the second time; doubling stopped at the thread cap.
- Dataset sizing: derived ranges for i4i/i8g equal today's; scaling for other RAM/disk values.
- Launcher command: job name per request, with and without `job_name_suffix`; same hardware with another
  Scylla version maps to the same job; final `test_config` (base list, tablets and backend overlays, request
  overlays last) and `extra_environment_variables` (request text plus each structured parameter's `SCT_*`
  variable, loader variables only when set, structured parameters winning); a conflicting
  identity variable rejected; invalid requests rejected; existing job reused (Jenkins client mocked); the
  launcher's default `test_config` matches the Python default and every file in it exists.
- Overload: rates derived from the best throughput per factor; thread count bounded by the cap; goodput
  ratio computed from successful operations only; no overload steps when nothing passed.

**Integration / manual:**
- Minicloud run of a gradual-steps test with the search enabled, to check wiring and the Argus tables.
  Needs a perf-steps overlay in `configurations/minicloud/`, which does not exist yet.
- Launcher runs on AWS for i4i.4xlarge and i8g.4xlarge, then one instance type that has never been run.

## Success Criteria

1. A best-throughput result for an instance type that has never been run is obtained by starting the
   launcher, with no SCT change.
2. On the same Scylla version, each generated job's best throughput is at least 0.95x the
   `BestThroughputResult` of the matching predefined-steps job (i4i and i8g): its best grid step under the
   same P99 limit, not a step that passes the grid's own per-step thresholds. Single runs differ by 6-9%,
   so a miss is rechecked on the medians of 3 runs of each.
3. Each generated job builds its own Argus history; Phases 1-5 DoD met.
4. Existing predefined-steps jobs report the same per-step results as before.

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Unthrottled steps are noisy near the limit, so the best value jumps between runs | High | Medium | Confirm step or stricter margin (Phase 3); Argus history makes the jitter visible before relying on it |
| The search starts far from the limit and spends many steps doubling | Medium | Medium | Start from threads per shard calibrated on i4i/i8g; doubling reaches the limit in a few steps, and the budget only limits the bisection after it |
| Loaders saturate before the cluster, so the best value measures loaders | Medium | High | Search stops on throughput saturation and reports it; check loader CPU at the last step; launcher can raise `n_loaders` |
| Mixed probes run on a growing dataset, so later steps see more data | Medium | Medium | Keep compaction and tablet-split waits; note it in the report |
| Derived dataset changes results of existing jobs | Low | High | Phase 4 defaults reproduce today's ranges on i4i/i8g; unit-tested |
| Generated jobs pile up in Jenkins | Medium | Low | One job per hardware configuration, reused across versions; Scylla version is not in the name |
| The launcher's job-creation credentials are too broad | Low | Medium | A folder-scoped Jenkins user, or restricting who can build the launcher (Phase 5 Needs Investigation) |
| A generated job has no Argus test on its first run, so every result submit fails and the workload ends after the full search with nothing in Argus | High (every new configuration) | High: hours of cluster time and no result | The jobs live under `scylla-enterprise`, which Argus scans; the launcher starts a job only once Argus has its test; the decorator keeps a step's latencies when a submit fails (Phase 5) |
| Under 2x overload the stress tool aborts on timeouts or its concurrency grows without bound | High | High | Phase 6 bounds timeouts and threads and requires the tool to continue on errors; checked per tool before enabling |
| The test timeout ends a search run before it sends its result | High with the test-case's 500 minutes | High: hours of cluster time and no result | The launcher sizes `SCT_TEST_DURATION` from the search and overload settings (Phase 5) |
| An Argus outage at the end of a run fails the test and loses its results | Low | Medium: affects every gradual job | The sends log a warning instead, and the results log is written first (Phases 2 and 6) |
| A search that reaches the thread cap reports a lower bound as the limit | Medium on large clusters | Medium | The row's stop reason says `thread cap` (Phase 3) |

Rollback: reporting only adds a table; the search, the sized dataset and the overload steps run only when
their configs are set; the launcher and generated jobs are separate from the existing perf jobs.
