---
status: draft
domain: stress-tools
created: 2026-10-19
last_updated: 2026-10-19
owner: juliayakovlev
---

# Best Throughput Under a Latency Limit for Gradual-Steps Performance Tests

Jira: [SCT-580](https://scylladb.atlassian.net/browse/SCT-580)

## Problem Statement

The predefined-steps performance tests run a fixed list of throttled steps followed by one unthrottled step,
for the `read`, `write`, `mixed` and `read_disk_only` workloads. The question these tests are meant to
answer is: **what is the highest throughput the cluster sustains while P99 latency stays under 10 ms?**

Today nobody computes that number:

- A person reads one Argus table per step and picks the last step whose P99 is under 10 ms
  ([example](https://argus.scylladb.com/tests/scylla-cluster-tests/1dfde12a-3e8c-4d6e-8a8e-0c1d88b5a939)).
- There is no single "max throughput under 10 ms" value per run, so Argus cannot track it across Scylla
  versions or apply a validation rule to it.
- The answer is only as precise as the step grid. With steps at 150k and 200k op/s, "best = 150k" really
  means "somewhere in 150k to 200k". Closing that gap today means editing the steps and rerunning the test.
- Reading by eye is error-prone: a throttled step that fell short of its target rate shows low latency only
  because the loaders or cluster were saturated, yet it looks like a passing step.

## Current State

- `performance_regression_gradual_grow_throughput.py:PerformanceRegressionPredefinedStepsTest`
  - `run_gradual_increase_load` runs every step from `perf_gradual_throttle_steps`. Each step goes through
    `run_step` wrapped in `sdcm/utils/decorators.py:latency_calculator_decorator`, which sends one Argus
    latency table per step.
  - `check_latency_during_steps` collects each step's results into `total_summary`, keyed by the step name
    from `get_sequential_throttle_steps`. Per step it holds the HDR P99 of every operation
    (`cycles[0]["hdr_summary"][<op>]["percentile_99"]`), the HDR-measured throughput
    (`cycle_hdr_throughput`, set by the decorator) and the stress-tool rate (`ops_rate`).
  - `save_total_summary_in_file` writes `total_summary` to `<logdir>/result_gradual_increase.log`.
    Nothing else reads `total_summary`; no step is selected as "best".
  - The `Workload` dataclass controls what carries over between steps: `drop_keyspace` (write recreates
    the schema per step), `preload_data`, `wait_no_compactions`, and `cs_cmd_warm_up` (read_disk_only).
- `sdcm/sct_config/mixins/performance.py` defines `perf_gradual_throttle_steps`, `perf_gradual_threads`
  and `perf_gradual_step_duration`, all per workload.
  `sdcm/sct_config/config.py:SCTConfiguration._validate_perf_gradual_throttle_steps` validates them.
- `sdcm/argus_results.py` has the per-step tables (`LatencyCalculatorMixedResult`, `...WriteResult`,
  `...ReadResult`, `...ReadDiskOnlyResult`) and `LATENCY_ERROR_THRESHOLDS`
  (`default: percentile_99 = 10`), which already encodes the 10 ms limit for red marking.
- `unit_tests/unit/test_perf_gradual_config.py` covers config validation of the gradual parameters.
- `docs/performance/results-and-regression.md` and `docs/performance/runbook-recalibrate-steps.md`
  describe how step results are read and how steps are chosen today.

## Goals

1. Every gradual-steps run reports one number per workload: the highest measured throughput (op/s) of a
   throttled step whose P99 of every operation is within the configured limit (default 10 ms).
2. That number is stored in a new Argus table with history, so a drop between Scylla versions is visible
   without opening per-step tables.
3. Steps that fell short of their target rate (below 95% of target) and the unthrottled step never count
   as "best".
4. Optionally (off by default), narrow the result to within 5% of the true limit by running extra steps
   between the last passing and the first failing predefined step, adding at most
   `N × step_duration` per workload.
5. Existing step names, Argus per-step tables and their history are unchanged.

## Implementation Phases

Phases 1-2 are "Layer 1" (no extra run time). Phases 3-4 are "Layer 2" (opt-in refinement) and start only
after Layer 1's Argus table has a few runs of history.

### Phase 1: Latency limit config and best-step selection

**Importance:** Critical
**Dependencies:** none

- New config `perf_gradual_latency_limit`, per workload, default `{"default": {"percentile_99": 10}}`.
  A workload key overrides `default` (read_disk_only may need its own limit later). Validated next to
  `_validate_perf_gradual_throttle_steps`.
- New pure function `find_best_throughput(total_summary, p99_limit_ms, min_achieved_ratio=0.95)`:
  - A step **passes** when the P99 of every operation is within the limit. For mixed, read and write
    must both pass.
  - Steps are **ranked by measured throughput** (`cycle_hdr_throughput`), not by the target rate.
  - A step whose measured throughput is below `min_achieved_ratio × target rate` does not count, even with
    low latency. The unthrottled step has no target and is always excluded.
  - Steps with empty or failed latency results are skipped.
  - Returns the best step (name, throughput, P99 per operation) or "none passed".

**Definition of Done:**
- [ ] `perf_gradual_latency_limit` defined, defaulted and validated; config docs regenerated
- [ ] `find_best_throughput` covered by the unit tests listed in Testing Requirements

### Phase 2: Report the best step

**Importance:** Critical
**Dependencies:** Phase 1

- `run_gradual_increase_load` calls `find_best_throughput` after the steps loop.
- New Argus table `BestThroughputResult` in `sdcm/argus_results.py`: best throughput (op/s,
  `higher_is_better=True`), the step name, P99 read/write at that step, and the limit used. One row per
  workload.
- The result is also written to `result_gradual_increase.log`.
- Email report: **Needs Investigation** — find which reporter renders the gradual-steps email and add the
  best-step line there.
- "None passed" is reported as an explicit row, not an empty table, so a regression to zero passing steps
  is visible.

**Definition of Done:**
- [ ] One `BestThroughputResult` row per workload in a real run
- [ ] `result_gradual_increase.log` contains the best step
- [ ] Minicloud run of a gradual-steps test shows the table (latency values there are meaningless)

### Phase 3: Refine between the last passing and first failing step

**Importance:** Medium
**Dependencies:** Phase 2, plus a few runs of `BestThroughputResult` history

- New config `perf_gradual_refine_steps: N`, default `0` (off).
- Behaviour when `N > 0`:
  1. Run the predefined steps unchanged (coarse search; Argus history stays comparable).
  2. Find `lo` (last passing step) and `hi` (first failing step) with the Phase 1 rules.
  3. Run a step at `(lo + hi) / 2` through the same step path as predefined steps. Pass moves `lo` up,
     fail moves `hi` down. Repeat up to N times, or stop once `(hi - lo) / lo < 5%`. Three refine steps
     shrink the gap to 1/8.
  4. Refined steps are named `refine_<rate>` so they get their own Argus rows and never collide with
     predefined step names.
  5. Run `find_best_throughput` over predefined and refined steps together.
- No refinement when nothing passed or everything passed; the report says why.
- Workload state per probe (a lower rate can run after a higher one):
  - write: keyspace dropped and schema recreated per step, each probe starts clean.
  - read: preloaded data, compaction wait between steps.
  - read_disk_only: same as read, provided warmup/cache-drop runs before each probe.
  - mixed: dataset grows with every step, so later probes run on more data. Keep the compaction and
    tablet-split waits and flag refined mixed results as such in the report.

**Definition of Done:**
- [ ] Refinement runs only when `perf_gradual_refine_steps > 0`
- [ ] Bisection loop covered by unit tests against a fake latency curve
- [ ] One real run per workload with `N = 3` reports a refined best within 5% of the coarse gap

### Phase 4: Noise handling and run-time saver

**Importance:** Low
**Dependencies:** Phase 3

- Confirm step: rerun the final `lo` once at full step duration; report the confirmed value.
  **Needs Investigation:** whether a confirm rerun or a stricter pass margin (P99 ≤ 0.9 × limit) gives
  more stable history; decide from the Phase 3 runs.
- Optional flag: after the first failing throttled step, skip the remaining throttled steps and keep the
  unthrottled step as the max-throughput datapoint.

**Definition of Done:**
- [ ] Chosen noise strategy implemented and documented
- [ ] Skip-after-fail flag off by default; unit-tested

### Phase 5: Documentation

**Importance:** High
**Dependencies:** delivered with Phases 2 and 3

- `docs/performance/results-and-regression.md`: describe `BestThroughputResult` and the pass rules.
- `docs/performance/runbook-recalibrate-steps.md`: how to use the best step and refinement when choosing
  new steps.

**Definition of Done:**
- [ ] Both docs updated in the same PRs as the behaviour they describe

## Testing Requirements

**Unit tests** (`unit_tests/unit/`):
- `find_best_throughput`: mixed with only one operation over the limit fails; a step below 95% of its
  target is excluded; unthrottled is excluded; empty or failed latency results are skipped; "none passed"
  and "all passed" cases.
- Config: `perf_gradual_latency_limit` default, per-workload override, invalid values rejected
  (extend `test_perf_gradual_config.py`).
- Bisection: against a fake latency curve it converges, stops after N steps, stops early at the 5%
  gap, handles "nothing passed" and "everything passed", and names steps `refine_<rate>`.

**Integration / manual:**
- Minicloud run of a gradual-steps test to check wiring and the Argus table. Needs a perf-steps overlay in
  `configurations/minicloud/`, which does not exist yet.
- One real AWS run of the enterprise gradual-steps job per phase to confirm the reported best step matches
  what a person reads from the per-step tables.

## Success Criteria

1. Phase 1 and 2 DoD met: every gradual-steps run reports a best throughput per workload in Argus.
2. For the last 3 runs of the enterprise gradual-steps job, the reported best step matches the step a
   person picks by reading the per-step tables.
3. If Phase 3 is enabled, refined runs add no more than `N × step_duration` per workload.

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Run-to-run noise near the limit flips pass/fail, making the best value jump between steps | High | Medium | Phase 4 confirm step or stricter margin; Argus history makes the jitter visible before relying on it |
| Mixed probes run on a growing dataset, so refined results are not comparable to the predefined grid | Medium | Medium | Keep compaction and tablet-split waits; mark refined mixed results in the report |
| Refinement extends run time on baremetal, where sub-tests run one at a time | Medium | Medium | Off by default; bounded by N and the 5% stop |
| The 95% achieved-rate threshold is wrong for some stress tool (rate accounting differs) | Medium | Low | Threshold is a parameter; verify against existing cassandra-stress and latte runs before enabling |
| New Argus table needs a validation rule set up on the Argus side | Low | Low | Table works without rules; add rules after a few runs of history |

Rollback: Layer 1 only adds a report and a table; reverting the PR removes them without affecting step
results. Layer 2 is behind `perf_gradual_refine_steps` (default 0).
