---
status: draft
domain: stress-tools
created: 2026-10-19
last_updated: 2026-10-22
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
    read_disk_only).
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
6. Existing predefined-steps (throttled) jobs, their step names and Argus history are unchanged.
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

## How a Request Flows

```
best-throughput-request            launcher job; the user fills in the request parameters
  1. validate the request          instance type exists in the region, architecture, required fields
  2. derive the job name           from the hardware parameters, e.g. aws-i8g.4xlarge-3n-tablets
  3. create the job if missing     generic jenkinsfile; request stored as the job's parameter defaults
  4. start it                      without waiting
        |
        v
<generated job>                    perfRegressionParallelPipeline, one sub-test per workload in parallel
  size the dataset from node RAM/disk -> preload -> adaptive thread search -> BestThroughputResult
```

## Implementation Phases

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
  `higher_is_better=True`), threads, the step name, P99 read/write at that step, and the limit used. One row
  per workload. "None passed" is an explicit row with throughput 0, not an empty table.
- The result is also written to `result_gradual_increase.log`.
- Email: Argus renders the email reports (`argus_email_report_template`), so the table reaches them there;
  no SCT change.
- Reporting also runs for the existing throttled jobs, where it reads as "best step on the throttle grid".

**Definition of Done:**
- [ ] One `BestThroughputResult` row per workload in a real run
- [ ] `result_gradual_increase.log` contains the best step
- [ ] Minicloud run of a gradual-steps test shows the table (latency values there are meaningless)

### Phase 3: Adaptive thread search

**Importance:** Critical
**Dependencies:** Phase 2

Replaces a fixed `perf_gradual_threads` list for best-throughput runs. Existing jobs keep their fixed steps;
the search runs only when its config is set.

- New config `perf_best_throughput_search`, per workload with a `default` entry: starting threads per Scylla
  shard, maximum number of steps, and the precision to stop at (default 5%).
- The run reads the shard count of the DB nodes and starts at `threads per shard x total shards / loaders`
  threads per loader.
- **Expand:** double the thread count each step, until a step fails the latency limit, or throughput grows by
  less than 5% over the previous passing step (saturated: more threads only queue), or the step budget runs
  out.
- **Narrow:** bisect the thread count between the last passing and the first failing step. A pass moves the
  lower bound up, a fail moves the upper bound down. Stop when the gap is under the precision or the step
  budget runs out.
- Every step is unthrottled and named `unthrottled_<threads>_threads`, the name a fixed list would give it.
  `find_best_throughput` then picks the best step from all of them (Phase 1).
- When the first step already fails, halve instead of doubling, down to one thread per loader.
- Each step is time-bound (`perf_gradual_step_duration`), including write: an n-bound write step at a low
  thread count would take hours. Per-step P99 validation rules are off for these runs, since steps over the
  limit are expected.
- Workload state between probes (a lower thread count can run after a higher one):
  - write: keyspace dropped and schema recreated per step, each probe starts clean.
  - read: preloaded data, compaction wait between steps.
  - read_disk_only: same as read, provided warmup/cache-drop runs before each probe.
  - mixed: dataset grows with every step. Keep the compaction and tablet-split waits and note it in the
    report.
- **Needs Investigation:** the default starting threads per shard. From the table in Current State, i8g read
  passes at ~59 threads per shard (620 x 4 loaders over ~42 shards), and read_disk_only and mixed fail at
  today's counts on both instance types. Calibrate on i4i and i8g in the first runs, and keep the step budget
  so a run is at most ~8 steps per workload.
- **Needs Investigation:** noise near the limit. Options: rerun the final passing step once, or require
  P99 <= 0.9 x limit. Decide from the first runs.

**Definition of Done:**
- [ ] The search is covered by unit tests against fake latency curves
- [ ] On i4i.4xlarge and i8g.4xlarge, every workload ends with a passing and a failing step within 5% of
      each other, or a documented reason (saturated, budget)

### Phase 4: Dataset sized from instance memory and disk

**Importance:** Critical
**Dependencies:** Phase 2 (can run in parallel with Phase 3)

Required for on-demand runs: the instance type is not known when the test-case is written.

- Measure on the DB nodes after provisioning: RAM and Scylla data-disk size. This works on every backend,
  including baremetal and minicloud.
- Derive the key ranges per workload from those values instead of the fixed `-pop seq=` ranges:
  - read and mixed (hot range): a fraction of RAM, today ~16%.
  - preload and read_disk_only: a multiple of RAM, today ~5x, so reads miss the cache.
  - write: capped by free disk, today ~43%, leaving room for compaction.
- The ratios are configurable; their defaults reproduce today's ranges on i4i.4xlarge and i8g.4xlarge, so
  results on those stay comparable.
- **Needs Investigation:** how to substitute the derived ranges into the stress commands (template
  placeholders vs generated commands).

**Definition of Done:**
- [ ] On i4i.4xlarge and i8g.4xlarge the derived ranges equal today's ranges
- [ ] On an instance with different RAM/disk, the dataset scales by the configured ratios

### Phase 5: On-demand launcher and generated jobs

**Importance:** Critical
**Dependencies:** Phases 3 and 4

- **Location:** a new `jenkins-pipelines/performance/branch-perf-v17/best-throughput/` directory holds two
  jenkinsfiles, the launcher and the generic job. `sct.py create-performance-jobs` already maps directories
  under `branch-perf-v17` to Jenkins folders, so it creates both in a `best-throughput/` Jenkins folder, and
  the generated jobs are created in that folder too. All of them run from the `branch-perf-v17` branch.

  ```
  best-throughput/                               (Jenkins folder)
    best-throughput-request                      launcher, created by create-performance-jobs
    best-throughput-generic                      generic job, created by create-performance-jobs
    aws-i8g.4xlarge-3n-tablets                   generated on the first request for this configuration
    aws-i4i.4xlarge-3n-tablets                   ...
  ```

- **Launcher job** `best-throughput-request`: a pipeline job whose parameters are the request. Proposed
  parameters (**to be decided**):

  | Parameter | Required | Default | Part of the job name |
  |---|---|---|---|
  | `backend` | yes | aws | yes |
  | `instance_type_db` | yes | | yes |
  | `scylla_version` (or image / repo) | yes | | no: history spans versions |
  | `n_db_nodes` | no | 3 | yes |
  | `tablets` | no | true | yes (`tablets` / `vnodes`) |
  | `region` | no | per backend | no |
  | `instance_type_loader`, `n_loaders` | no | per backend, today c7i.8xlarge x 4 on AWS | no |
  | `workloads` | no | all four | no |
  | `p99_limit_ms` | no | 10 | no |
  | `provision_type`, `email_recipients`, `requested_by_user` | no | as in perf jobs | no |

- The launcher runs a new command, `sct.py create-best-throughput-job`, which:
  - validates the request (required fields, instance type known in the region, architecture);
  - derives the job name from the hardware parameters, e.g.
    `best-throughput/aws-i8g.4xlarge-3n-tablets`;
  - creates the job in the `best-throughput/` folder if it does not exist, through `JenkinsPipelines` with
    `branch-perf-v17` as the SCT branch. Every generated job points at the generic job's jenkinsfile; the
    request is stored as the job's parameter defaults, so a plain rebuild repeats the same configuration.
- The launcher then starts the generated job with the request's parameters and does not wait for it. The
  instance types reach SCT through `extra_environment_variables` (`SCT_INSTANCE_TYPE_DB`, ...), which the
  perf pipeline already supports, so `perfRegressionParallelPipeline` is unchanged.
- The generic jenkinsfile runs the predefined-steps test with a best-throughput overlay: the search (Phase 3),
  sized dataset (Phase 4), time-bound steps, and no per-step P99 validation.
- The generic job can also be run directly with the same parameters, for a one-off run that should not get
  its own job and history.
- Since the jobs run from `branch-perf-v17`, every PR of Phases 1-5 is backported there
  (`backport/perf-v17`) before the launcher is usable.
- **Needs Investigation:** the Jenkins credentials the launcher uses to create jobs, limited to the
  `best-throughput/` folder.
- Backends: AWS first. GCE and Azure need their instance-type variables and default regions and loaders.

**Definition of Done:**
- [ ] `create-performance-jobs` creates the `best-throughput/` folder with the launcher and the generic job
- [ ] A request for a new instance type creates its job in that folder and starts it; a second request
      reuses the job
- [ ] Generated jobs for i4i.4xlarge and i8g.4xlarge report `BestThroughputResult` for all four workloads
- [ ] An invalid request fails in the launcher, before anything is provisioned

### Phase 6: Goodput under overload (SCYLLADB-3267)

**Importance:** Medium
**Dependencies:** Phase 3

- New config, off by default, listing the overload factors per workload, e.g. `[1.3, 2]`, and a launcher
  parameter to enable it.
- After the best throughput is found, run one step per factor at a **rate derived from the measured best
  throughput** (factor x best), throttled through the existing rate path. The rates follow the run's own
  best throughput, so nothing is calibrated per instance type.
- Thread count for overload steps: enough to offer the target rate, but bounded, so concurrency does not
  explode while the cluster sheds load. **Needs Investigation:** start from the best step's thread count
  scaled by the factor, capped by a configured maximum.
- Client request timeouts are bounded and the stress tool keeps running on timeouts and errors instead of
  aborting the step. **Needs Investigation** per tool (cassandra-stress `-errors`, latte `--request-timeout`
  and retry options).
- Goodput is the throughput of successful operations. Report per factor: offered rate, goodput, goodput as
  a ratio of the best throughput, P99, and failed operations. Exceptions per failed op where the stress
  tool exposes them (**Needs Investigation**).
- Results go into a separate Argus table, one row per factor, with a validation rule of ratio >= 0.9.
- Overload steps are named `overload_<factor>x` and run after the search, so they don't affect the best
  throughput. For write and mixed, the dataset grows further; keep the compaction and tablet-split waits.

**Definition of Done:**
- [ ] With overload enabled, each workload reports goodput at 1.3x and 2x of its best throughput
- [ ] The stress tools keep running through timeouts, and failed operations are counted
- [ ] The ratio >= 0.9 rule fails the run in Argus when goodput drops below it

### Phase 7: Documentation

**Importance:** High
**Dependencies:** delivered with Phases 2-6

- `docs/performance/results-and-regression.md`: `BestThroughputResult`, the pass rules, the goodput table.
- `docs/performance/test-catalog.md` and a short runbook: how to request a best-throughput run, what each
  launcher parameter means, and where the results land.

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
  saturation, or until the budget; halving when the first step fails; bisection stops at the precision or
  the budget; unique `unthrottled_<threads>_threads` names.
- Dataset sizing: derived ranges for i4i/i8g equal today's; scaling for other RAM/disk values.
- Launcher command: job name per request; same hardware with another Scylla version maps to the same job;
  invalid requests rejected; existing job reused (Jenkins client mocked).
- Overload: rates derived from the best throughput per factor; thread count bounded by the cap; goodput
  ratio computed from successful operations only; no overload steps when nothing passed.

**Integration / manual:**
- Minicloud run of a gradual-steps test with the search enabled, to check wiring and the Argus tables.
  Needs a perf-steps overlay in `configurations/minicloud/`, which does not exist yet.
- Launcher runs on AWS for i4i.4xlarge and i8g.4xlarge, then one instance type that has never been run.

## Success Criteria

1. A best-throughput result for an instance type that has never been run is obtained by starting the
   launcher, with no SCT change.
2. On the same Scylla version, each generated job's best throughput is at least the highest passing rate on
   the throttled grid of the matching predefined-steps job (i4i and i8g).
3. Each generated job builds its own Argus history; Phases 1-5 DoD met.
4. Existing predefined-steps jobs report the same per-step results as before.

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Unthrottled steps are noisy near the limit, so the best value jumps between runs | High | Medium | Confirm step or stricter margin (Phase 3); Argus history makes the jitter visible before relying on it |
| The search starts far from the limit and uses its step budget before narrowing | Medium | Medium | Start from threads per shard calibrated on i4i/i8g; doubling reaches the limit in a few steps; budget is configurable |
| Loaders saturate before the cluster, so the best value measures loaders | Medium | High | Search stops on throughput saturation and reports it; check loader CPU at the last step; launcher can raise `n_loaders` |
| Mixed probes run on a growing dataset, so later steps see more data | Medium | Medium | Keep compaction and tablet-split waits; note it in the report |
| Derived dataset changes results of existing jobs | Low | High | Phase 4 defaults reproduce today's ranges on i4i/i8g; unit-tested |
| Generated jobs pile up in Jenkins | Medium | Low | One job per hardware configuration, reused across versions; Scylla version is not in the name |
| The launcher's job-creation credentials are too broad | Low | Medium | Restrict them to the `best-throughput/` folder (Phase 5 Needs Investigation) |
| Under 2x overload the stress tool aborts on timeouts or its concurrency grows without bound | High | High | Phase 6 bounds timeouts and threads and requires the tool to continue on errors; checked per tool before enabling |

Rollback: reporting only adds a table; the search, the sized dataset and the overload steps run only when
their configs are set; the launcher and generated jobs are separate from the existing perf jobs.
