---
status: draft
domain: testing
created: 2026-09-23
last_updated: 2026-09-23
owner: fruch
---

# Cost Estimation Rollout and Test Plan

Tracked as [SCT-852](https://scylladb.atlassian.net/browse/SCT-852) under epic
[SCT-851](https://scylladb.atlassian.net/browse/SCT-851).

## 1. Problem Statement

Cost estimation exists but reaches almost nobody. The estimate runs in exactly one of the
thirteen shared pipeline definitions, so twelve pipeline families — performance, artifacts,
rolling upgrade, manager, jepsen, operator, release gating and the rest — provision hardware
with no idea what it will cost. The epic's later phases want to alert on a threshold and
gate an expensive run behind approval, and neither is possible on a pipeline that never
produces a figure.

Coverage is not the only gap. The estimate is a number nobody has yet checked against a
bill, and it is computed from four independent price sources that drift on their own
schedules. A wrong estimate is worse than none: it gets acted on.

Measurable pain points:

- **1 of 13** shared pipelines runs the estimate today; the remaining 12 cover the majority
  of provisioned hours.
- **0** automated checks compare an estimate against what a run actually cost.
- **4** price sources (AWS live, GCE and Azure catalog, OCI derived) with no test that fails
  when one silently starts returning nothing.
- **2 of 4** clouds price spot at all today; Azure and OCI report unknown.

## 2. Current State

**The estimate itself.** `sdcm/utils/cloud_catalog/cost.py:estimate_run_cost` produces a
per-role breakdown and a total from an already-resolved configuration, exposed as
`sct.py:estimate-cost`. `sdcm/utils/cloud_catalog/pricing.py` holds the per-cloud rate
lookups, backed by the checked-in catalog under `data/instance_catalog/` and, for AWS spot,
a live batched query. Unit coverage lives in `unit_tests/unit/test_cost.py`.

**The pipeline surface.** `vars/estimateTestCost.groovy` renders the estimate, and
`vars/longevityPipeline.groovy` is the only caller. The other twelve entry points —
`artifactsPipeline`, `perfRegressionParallelPipeline`, `perfRegressionParallelPipelinebyRegion`,
`perfSearchBestConfigParallelPipeline`, `rollingUpgradePipeline`,
`rollingOperatorUpgradePipeline`, `managerPipeline`, `jepsenPipeline`,
`byoLongevityPipeline`, `sdReleaseGatingPipeline`, `createTestJobPipeline` and
`triggerMatrixPipeline` — have no cost stage. Because every one of the ~1,125 jenkinsfiles
delegates to a shared definition, the rollout is thirteen files, not a thousand.

**What is not built.** Reporting the estimate to Argus needs its merged cost API, which no
client release contains yet. Fallback visibility and spot support for Azure and OCI each
have their own mini-plan.

## 3. Goals

1. Every shared pipeline definition produces a cost estimate before it provisions anything
   — 13 of 13, verified by a lint check rather than by inspection.
2. No pipeline can fail because of cost estimation. A cost stage that errors, times out or
   finds no price leaves the build result untouched.
3. The estimate is accurate within a stated band: for a completed run, the estimate is
   within **±25%** of the same run's measured instance-hour cost, on AWS and GCE.
4. A price source that silently stops returning data is caught within one catalog-refresh
   cycle, by a test that fails rather than by a human noticing a zero.
5. All four clouds price both on-demand and spot, or report unknown explicitly — no cloud
   reports a confident zero.
6. The estimate is reachable by later phases: a machine-readable figure, including the
   on-demand ceiling, is available to a threshold alert or an approval gate without
   re-deriving it.

## 4. Implementation Phases

### Phase 1 — Make the cost stage safe to add everywhere

**Importance: critical.** Everything else depends on a stage that cannot hurt a build.

Extract the stage into a form each pipeline can call identically, and make its failure modes
explicit: an exception, a missing price, a missing credential and a slow API all end with the
build unaffected and a line in the log. Give it a bounded timeout, because the one path that
touches a cloud API is the one that can hang.

**Definition of Done:** a single call renders the estimate; injected failures of each kind
leave the stage green; the stage has a timeout; unit tests cover each failure mode.

### Phase 2 — Roll out to the remaining twelve pipelines

**Importance: critical.** This is the coverage goal.

Add the stage to each remaining shared definition, placed as it is in longevity: after the
test duration is known and before anything is provisioned, so aborting is free. Some
pipelines will not fit that shape — the parallel performance pipelines provision per region,
and the trigger matrix does not provision at all — so each needs its placement decided rather
than copied.

**Definition of Done:** every shared pipeline that provisions produces an estimate; pipelines
that do not provision are explicitly listed as out of scope with a reason; a lint check fails
when a new pipeline is added without one.

### Phase 3 — Close the per-cloud pricing gaps

**Importance: high.** Two clouds cannot be estimated at all today.

Azure spot needs only that the rows already present in a response the generator fetches stop
being discarded. OCI needs its flat preemptible ratio and a real on-demand price, since it
currently reports zero unconditionally. Both are small and independent of the rollout.

**Definition of Done:** all four clouds return a non-zero on-demand and spot rate for the
instance types SCT actually runs, or an explicit unknown.

### Phase 4 — Compare estimates against reality

**Importance: high.** Until this exists, accuracy is an assumption.

Take completed runs and compare the estimate against the same run's measured instance-hour
cost, reporting the distribution rather than a single number. This is what turns goal 3 from
an aspiration into a measurement, and it is the check that catches a price source that has
quietly gone stale.

**Definition of Done:** a repeatable comparison over a sample of recent runs per backend; the
±25% band is either met or the deviation is explained; the comparison runs often enough to
catch drift.

### Phase 5 — Guard the price sources

**Importance: medium.** Prevents silent rot.

An integration test per cloud that asks for a price the way the product does and fails if the
answer is missing or zero. These must be skippable without credentials and must not run in
the ordinary unit suite.

**Definition of Done:** one guarded test per cloud; each fails loudly when its source stops
answering; all skip cleanly without credentials.

## 5. Testing Requirements

### Unit

Offline, no credentials, no network. The properties worth pinning are the ones a refactor
would quietly break:

- **Call count.** Pricing a whole run costs one AWS call per region. Node count and
  instance-type count must not change it. An `on_demand` run makes none; no backend other
  than AWS makes any.
- **Unknown stays unknown.** A missing price is never zero, and a missing *spot* price never
  falls back to the on-demand rate — that would quote a figure several times too high under
  a spot label.
- **Failure is contained.** An unreachable API, absent credentials and a malformed response
  each yield an unknown rate and never raise.
- **Arithmetic.** Fractional hours, node counts multiplied correctly, multi-DC counts summed,
  a partial estimate flagged rather than silently low.

### Integration

Credential-gated, skipped cleanly without them, never in the unit suite: one per cloud,
asking for a price the way the product does, failing when the source returns nothing. These
are the tests that catch a provider changing an API shape.

### Pipeline

- A lint check that every shared pipeline definition which provisions also estimates. This is
  what keeps coverage from regressing the next time a pipeline is added.
- A staging run per pipeline family showing the stage renders and does not affect the result.
- A deliberately broken estimate — no credentials, bad config — confirming the build still
  passes.

### Manual and measurement

- `utils/spot_pricing_report.py` run against live AWS, GCE and Azure, with its reported call
  counts matching what the unit tests assert.
- The estimate-versus-actual comparison from Phase 4, per backend.
- A spot-heavy run and an on-demand run cross-checked against the cloud-monitor cost site for
  the same test id.

## 6. Success Criteria

Phase DoD items above, plus two that only make sense across the whole effort:

- No build has failed because of cost estimation during the rollout. This is the criterion
  that matters most: a cost feature that breaks tests will be reverted, and deservedly.
- The accuracy band from goal 3 is met, or the gap is documented with its cause — a spot
  estimate priced before a fallback to on-demand, for instance, is expected to under-read.

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| A cost stage breaks unrelated builds | Medium | High | Phase 1 before any rollout; failures contained and timed out; staging run per family |
| Estimates are confidently wrong and get acted on | Medium | High | Phase 4 measures before later phases gate on the number; unknown reported as unknown, never zero |
| A price source silently returns nothing | High | Medium | Phase 5 guards per cloud; the catalog refresh cadence is short enough to catch drift |
| Prices drift between refreshes | High | Low | AWS is live; GCE and Azure move at most monthly; the accuracy band accounts for it |
| Spot estimate under-reads because a run fell back to on-demand | High | Medium | The on-demand ceiling is reported alongside; fallback visibility is its own mini-plan |
| Rollout stalls on pipelines that do not fit the placement | Medium | Low | Placement decided per pipeline in Phase 2; non-provisioning pipelines excluded explicitly |
| Argus reporting blocks the rollout | Medium | Low | Kept out of scope — the estimate is useful in the build log alone |

## Needs Investigation

- Where the estimate belongs in the parallel performance pipelines, which provision per
  region rather than once.
- Whether `triggerMatrixPipeline` should estimate the aggregate cost of what it triggers,
  rather than being excluded as non-provisioning.
- What "measured instance-hour cost" should be derived from for Phase 4 — the cloud-monitor
  site, cloud billing, or SCT's own per-node reporting once it exists.
