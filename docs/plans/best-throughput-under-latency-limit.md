---
status: draft
domain: stress-tools
created: 2026-10-19
last_updated: 2026-10-20
owner: juliayakovlev
---

# Best Throughput Under a Latency Limit for Gradual-Steps Performance Tests

Jira: [SCT-580](https://scylladb.atlassian.net/browse/SCT-580)
Related: [SCYLLADB-3267](https://scylladb.atlassian.net/browse/SCYLLADB-3267) (goodput under overload)

## Problem Statement

The question the gradual performance tests should answer for the `read`, `write`, `mixed` and
`read_disk_only` workloads is: **what is the highest throughput the cluster sustains while P99 latency
stays under 10 ms?**

Today nobody computes that number:

- The predefined-steps jobs run fixed throttled rates followed by one unthrottled step. A person reads one
  Argus table per step and picks the last step whose P99 is under 10 ms
  ([example](https://argus.scylladb.com/tests/scylla-cluster-tests/1dfde12a-3e8c-4d6e-8a8e-0c1d88b5a939)).
- A throttled rate only tells whether the cluster can serve *that* rate. The throttle grid has to be
  recalibrated per instance type by hand: i8g.4xlarge serves about 2x the rates of i4i.4xlarge on the same
  dataset and thread counts, so the two have separate step lists
  (`cassandra_stress_gradual_load_steps_enterprise.yaml` vs `cassandra_stress_gradual_load_steps_i8g.yaml`).
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
    count per step. `get_sequential_throttle_steps` then names steps `unthrottled_<threads>_threads`.
  - `check_latency_during_steps` collects each step into `total_summary`. Per step it holds the HDR P99 of
    every operation (`cycles[0]["hdr_summary"][<op>]["percentile_99"]`) and the HDR-measured throughput
    (`cycle_hdr_throughput`). `save_total_summary_in_file` writes it to
    `<logdir>/result_gradual_increase.log`; nothing selects a best step.
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
- `sdcm/sct_config/mixins/performance.py` defines `perf_gradual_throttle_steps`, `perf_gradual_threads` and
  `perf_gradual_step_duration`; `sdcm/sct_config/config.py:SCTConfiguration._validate_perf_gradual_throttle_steps`
  validates them.
- `sdcm/argus_results.py` has the per-step tables (`LatencyCalculatorMixedResult`, `...WriteResult`,
  `...ReadResult`, `...ReadDiskOnlyResult`).
- `docs/performance/results-and-regression.md` and `docs/performance/runbook-recalibrate-steps.md` describe
  how step results are read and how steps are chosen today.

## Goals

1. A best-throughput job runs only **unthrottled steps that differ by thread count** per workload. No
   throttle grid to recalibrate per instance type.
2. Every run reports one number per workload: the highest HDR-measured throughput (op/s) of a step whose P99
   of every operation is within the configured limit (default 10 ms), plus the thread count that reached it.
3. That number is stored in a new Argus table with history, so a drop between Scylla versions is visible
   without opening per-step tables.
4. The dataset is sized from the DB nodes' memory and disk, so cache-read and disk-read workloads keep
   their meaning on any instance type; i4i.4xlarge and i8g.4xlarge keep today's sizes.
5. Optionally (off by default), narrow the result by running extra steps at thread counts between the last
   passing and the first failing step, adding at most `N x step_duration` per workload.
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

## Implementation Phases

### Phase 1: Latency limit config and best-step selection

**Importance:** Critical
**Dependencies:** none

- New config `perf_gradual_latency_limit`, per workload, default `{"default": {"percentile_99": 10}}`.
  A workload key overrides `default`. Validated next to `_validate_perf_gradual_throttle_steps`.
- New pure function `find_best_throughput(total_summary, p99_limit_ms)`:
  - A step **passes** when the HDR P99 of every operation is within the limit. For mixed, read and write
    must both pass.
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
  `find_best_throughput` after the steps loop.
- New Argus table `BestThroughputResult` in `sdcm/argus_results.py`: best throughput (op/s,
  `higher_is_better=True`), threads, the step name, P99 read/write at that step, and the limit used. One row
  per workload. "None passed" is an explicit row, not an empty table.
- The result is also written to `result_gradual_increase.log`.
- Email report: **Needs Investigation** — find which reporter renders the gradual-steps email and add the
  best-step line there.
- Reporting also runs for the existing throttled jobs, where it reads as "best step on the throttle grid".

**Definition of Done:**
- [ ] One `BestThroughputResult` row per workload in a real run
- [ ] `result_gradual_increase.log` contains the best step
- [ ] Minicloud run of a gradual-steps test shows the table (latency values there are meaningless)

### Phase 3: Best-throughput thread-step configs and jobs

**Importance:** High
**Dependencies:** Phase 2

- New steps configs for i4i.4xlarge and i8g.4xlarge: every step `unthrottled`, `perf_gradual_threads` a
  list of increasing thread counts per workload. They reuse the existing test class, test-case yaml and
  dataset.
- Thread grid: **Needs Investigation.** Start from today's thread counts (read 620, write 400, mixed 1900,
  read_disk_only 620), which reach the unthrottled maximum, and step down from there until P99 is under
  the limit. Both instance types have 16 vCPU, but i8g needs more concurrency for its ~2x throughput, so the
  grids differ per instance type.
- Whether to express the grid per Scylla shard (scaled by the cluster's shard count) instead of absolute
  thread counts: **Needs Investigation**, decided after the first i4i vs i8g runs.
- New Jenkins jobs next to the existing predefined-steps jobs (tablets first).

**Definition of Done:**
- [ ] i4i and i8g best-throughput jobs run all four workloads and report `BestThroughputResult`
- [ ] Each grid has at least one passing and one failing step per workload

### Phase 4: Dataset sized from instance memory and disk

**Importance:** High
**Dependencies:** Phase 3 (can run in parallel with Phase 5)

- Measure on the DB nodes after provisioning: RAM, Scylla data-disk size and shard count. This works on
  every backend, including baremetal and minicloud.
- Derive the key ranges per workload from those values instead of the fixed `-pop seq=` ranges:
  - read and mixed (hot range): a fraction of RAM, today ~16%.
  - preload and read_disk_only: a multiple of RAM, today ~5x, so reads miss the cache.
  - write: capped by free disk, today ~43%, leaving room for compaction.
- The ratios are configurable; their defaults reproduce today's ranges on i4i.4xlarge and i8g.4xlarge, so
  results on those stay comparable.
- **Needs Investigation:** how to substitute the derived ranges into the stress commands (template
  placeholders vs generated commands), and whether this should move to its own plan since it also benefits
  the throttled jobs.

**Definition of Done:**
- [ ] On i4i.4xlarge and i8g.4xlarge the derived ranges equal today's ranges
- [ ] On an instance with different RAM/disk, the dataset scales by the configured ratios

### Phase 5: Refine between the last passing and first failing step

**Importance:** Medium
**Dependencies:** Phase 3, plus a few runs of `BestThroughputResult` history

- New config `perf_gradual_refine_steps: N`, default `0` (off).
- Behaviour when `N > 0`:
  1. Run the configured thread steps unchanged.
  2. Find `lo` (thread count of the last passing step) and `hi` (first failing step) with the Phase 1 rules.
  3. Run an unthrottled step at `(lo + hi) / 2` threads through the same step path. Pass moves `lo` up,
     fail moves `hi` down. Repeat up to N times, or stop once the thread gap is under 5%.
  4. Refined steps are named `refine_<threads>_threads` so they get their own Argus rows.
  5. Run `find_best_throughput` over configured and refined steps together.
- No refinement when nothing passed or everything passed; the report says why.
- Optional flag: stop the configured steps after the first failing step, since more threads only add
  latency from there.
- Workload state per probe (a lower thread count can run after a higher one):
  - write: keyspace dropped and schema recreated per step, each probe starts clean.
  - read: preloaded data, compaction wait between steps.
  - read_disk_only: same as read, provided warmup/cache-drop runs before each probe.
  - mixed: dataset grows with every step. Keep the compaction and tablet-split waits and flag refined mixed
    results in the report.
- Noise: rerun the final `lo` once as a confirm step, or require P99 <= 0.9 x limit. **Needs
  Investigation**, decided from the first refined runs.

**Definition of Done:**
- [ ] Refinement and early stop run only when enabled
- [ ] Bisection loop covered by unit tests against a fake latency curve

### Phase 6: Goodput under overload (SCYLLADB-3267)

**Importance:** Medium
**Dependencies:** Phase 3; Phase 5 makes the best throughput, and so the overload rates, more precise

- New config, off by default, listing the overload factors per workload, e.g. `[1.3, 2]`.
- After the best throughput is found, run one step per factor at a **rate derived from the measured best
  throughput** (factor x best), throttled through the existing rate path. This is not a fixed throttle
  grid: the rates follow the run's own best throughput, so nothing is recalibrated per instance type.
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
- Overload steps are named `overload_<factor>x` and run after all other steps, so they don't affect the
  best-throughput search. For write and mixed, the dataset grows further; keep the compaction and
  tablet-split waits between steps.

**Definition of Done:**
- [ ] With the config set, each workload reports goodput at 1.3x and 2x of its best throughput
- [ ] The stress tools keep running through timeouts, and failed operations are counted
- [ ] The ratio >= 0.9 rule fails the run in Argus when goodput drops below it

### Phase 7: Documentation

**Importance:** High
**Dependencies:** delivered with Phases 2-6

- `docs/performance/results-and-regression.md`: `BestThroughputResult` and the pass rules.
- `docs/performance/runbook-recalibrate-steps.md`: choosing thread grids for a new instance type.
- `docs/performance/test-catalog.md`: the new best-throughput jobs.

**Definition of Done:**
- [ ] Docs updated in the same PRs as the behaviour they describe

## Testing Requirements

**Unit tests** (`unit_tests/unit/`):
- `find_best_throughput`: mixed with only one operation over the limit; ranking by measured throughput when
  more threads reduce throughput; limit is inclusive; empty or failed latency results skipped; multiple
  cycles; "none passed" and empty summary.
- Config: `perf_gradual_latency_limit` default, per-workload override, invalid values rejected (extend
  `test_perf_gradual_config.py`).
- Dataset sizing: derived ranges for i4i/i8g equal today's; scaling for other RAM/disk values.
- Overload: rates derived from the best throughput per factor; thread count bounded by the cap; goodput
  ratio computed from successful operations only; no overload steps when nothing passed.
- Bisection: against a fake latency curve it converges, stops after N steps, stops early at the 5% gap,
  handles "nothing passed" and "everything passed", names steps `refine_<threads>_threads`.

**Integration / manual:**
- Minicloud run of a gradual-steps test to check wiring and the Argus table. Needs a perf-steps overlay in
  `configurations/minicloud/`, which does not exist yet.
- Real AWS runs of the i4i and i8g best-throughput jobs per phase.

## Success Criteria

1. Phases 1-3 DoD met: i4i and i8g best-throughput jobs report a best throughput and thread count per
   workload in Argus.
2. On the same Scylla version, each job's best throughput is at least the highest passing rate on the
   throttled grid of the matching predefined-steps job.
3. Phase 4 DoD met without changing results of the existing i4i and i8g jobs.

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Unthrottled steps are noisier than throttled ones, so the best value jumps between runs | High | Medium | Phase 5 confirm step or stricter margin; Argus history makes the jitter visible before relying on it |
| Thread grid too coarse or entirely above/below the limit on a new instance type | Medium | Medium | Phase 3 DoD requires a pass and a fail per workload; Phase 5 refinement narrows the gap |
| Loaders saturate before the cluster, so the best value measures loaders | Medium | High | Check loader CPU at the highest thread step during Phase 3; add loaders if they are the limit |
| Mixed probes run on a growing dataset, so later steps see more data | Medium | Medium | Keep compaction and tablet-split waits; mark refined mixed results in the report |
| Derived dataset changes results of existing jobs | Low | High | Phase 4 defaults reproduce today's ranges on i4i/i8g; unit-tested |
| Under 2x overload the stress tool aborts on timeouts or its concurrency grows without bound | High | High | Phase 6 bounds timeouts and threads and requires the tool to continue on errors; checked per tool before enabling |
| Extra jobs and refinement extend run time on baremetal, where sub-tests run one at a time | Medium | Medium | Refinement off by default and bounded by N and the 5% stop |

Rollback: reporting only adds a table; the new jobs and configs are separate files; refinement is behind
`perf_gradual_refine_steps` (default 0), overload steps are off by default, and dataset sizing defaults to
today's ranges.
