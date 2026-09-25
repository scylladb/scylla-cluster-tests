# Mini-Plan: Roll Cost Estimation Out to Every Pipeline

**Date:** 2026-09-23
**Owner:** fruch
**Estimated LOC:** ~400
**Related Jira:** [SCT-852](https://scylladb.atlassian.net/browse/SCT-852) (epic [SCT-851](https://scylladb.atlassian.net/browse/SCT-851))
**Related mini-plans:** spot-aware cost estimates, on-demand fallback visibility

## Problem

Cost estimation runs in **1 of 13** shared pipeline definitions. Twelve pipeline families —
performance, artifacts, rolling upgrade, manager, jepsen, operator, release gating and the
rest — provision hardware with no idea what it will cost. The epic's later phases want to
alert above a threshold and gate an expensive run behind approval, and neither can work on a
pipeline that never produces a figure.

Coverage is not the only gap. The estimate has never been checked against an actual bill, and
it draws on four price sources that drift on their own schedules. A wrong estimate is worse
than none, because it gets acted on. Today there are **zero** automated checks comparing an
estimate to what a run really cost, and nothing fails when a price source quietly starts
returning nothing.

Because all ~1,125 jenkinsfiles delegate to a shared definition, the rollout is thirteen
files rather than eleven hundred.

## Approach

### 1. Make the stage safe before putting it anywhere

Nothing else matters if a cost stage can break a build — a cost feature that fails tests gets
reverted, and deservedly. Before any rollout, the stage must survive an exception, a missing
price, an absent credential and a slow API with the build result untouched and a line in the
log, under a bounded timeout. The one path that touches a cloud API is the one that can hang.

### 2. Roll out to the remaining twelve

Place it as in longevity: after the test duration is known, before anything is provisioned,
so aborting is free. Not every pipeline fits that shape — the parallel performance pipelines
provision per region, and the trigger matrix provisions nothing — so each placement is a
decision, not a copy. Pipelines that genuinely should not estimate are listed with a reason.

A lint check keeps this from regressing: adding a provisioning pipeline without a cost stage
should fail, rather than being noticed a year later.

### 3. Measure the estimate against reality

Compare completed runs against their measured instance-hour cost and report the
distribution, not a single number. Target: within **±25%** on AWS and GCE. This is what turns
accuracy from an assumption into a measurement, and it is the check that catches a price
source that has gone stale. Expect a spot estimate to under-read where a run fell back to
on-demand — that is the fallback-visibility mini-plan's job to surface, not a defect here.

### 4. Guard the price sources

One credential-gated integration test per cloud, asking for a price the way the product does
and failing when the answer is missing or zero. Skippable without credentials, never in the
unit suite. These catch a provider changing an API shape, which is the failure mode nobody
notices until a bill arrives.

### 5. The SCT runner is missing from every estimate

The runner is a real cost that no estimate includes, and it runs *longer* than the test: its
timeout covers startup, the test, teardown, log collection, cleanup and email, so for a
four-hour test it is budgeted for closer to seven.

The good news is that it is entirely predictable, contrary to the worry that it might be an
autoscaling group of mixed types. It is a single instance type chosen by one rule — long-term
above seven hours, regular below — with two fixed types per cloud, and it is never spot. Both
inputs to that rule, backend and test duration, are already in hand when the estimate runs.

The actual obstacle is smaller and duller: the runner families are **not in the catalog**,
which only carries what clusters use, so `m7i-flex.*` on AWS and `Standard_E2s_v3` on Azure
price as unknown today. Adding them is a catalog-config change.

Include it as its own line in the estimate rather than folded into a cluster role, and price
it over the runner's own window rather than `test_duration`, which would under-count. It is a
few percent of a large run and a much larger share of a small one, which is exactly where a
cost gate would otherwise be wrong.

### 6. Out of scope

Reporting cost to Argus, which needs a client release that does not exist yet. Spot pricing
itself and fallback visibility each have their own mini-plan.

## Needs Investigation

- Where the estimate belongs in the parallel performance pipelines, which provision per
  region rather than once.
- Whether the trigger matrix should estimate the aggregate cost of what it triggers, rather
  than being excluded as non-provisioning.
- What "actual cost" is measured against — the cloud-monitor site, cloud billing, or SCT's
  own per-node reporting once it exists.

## Files to Modify

- `vars/estimateTestCost.groovy` -- contain every failure mode and bound the runtime, so the
  stage cannot affect a build result
- `vars/artifactsPipeline.groovy`, `vars/perfRegressionParallelPipeline.groovy`,
  `vars/perfRegressionParallelPipelinebyRegion.groovy`,
  `vars/perfSearchBestConfigParallelPipeline.groovy`, `vars/rollingUpgradePipeline.groovy`,
  `vars/rollingOperatorUpgradePipeline.groovy`, `vars/managerPipeline.groovy`,
  `vars/jepsenPipeline.groovy`, `vars/byoLongevityPipeline.groovy`,
  `vars/sdReleaseGatingPipeline.groovy` -- add the stage, placed per pipeline
- `vars/createTestJobPipeline.groovy`, `vars/triggerMatrixPipeline.groovy` -- resolve whether
  these estimate at all, and record the decision
- `utils/lint_pipelines.sh` -- fail when a provisioning pipeline has no cost stage
- `data/instance_catalog/sizing_config.yaml` -- add the SCT runner families so the runner can
  be priced at all
- `unit_tests/unit/test_cost.py` -- failure containment for each mode the stage must survive
- `unit_tests/integration/test_cloud_pricing.py` -- **(new file)** one credential-gated check
  per cloud that its price source still answers

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_cost.py -v`
- [ ] Every shared pipeline that provisions renders an estimate; the lint check fails when one
      does not
- [ ] An injected failure of each kind -- exception, missing price, absent credential, slow
      API -- leaves the stage green and the build result untouched
- [ ] The stage has a timeout, verified by making the pricing call hang
- [ ] A staging run per pipeline family shows the estimate and does not change the result
- [ ] Integration checks pass with credentials and skip cleanly without them
- [ ] Estimate versus measured cost is within ±25% on AWS and GCE over a sample of recent
      runs, or the deviation is explained
- [ ] `uv run sct.py unit-tests`
- [ ] `uv run sct.py pre-commit` passes
