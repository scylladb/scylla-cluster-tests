# Mini-Plan: Cheaper Artifact Tests: Skip Logs on Pass, Faster Checks, and Spot with Retry

**Date:** 2026-10-18 (updated 2026-10-21)
**Estimated LOC:** ~900 across the PRs below
**Related ticket:** [SCT-707](https://scylladb.atlassian.net/browse/SCT-707)
**Implementation:** #16546 (part B, the retry loop for artifact tests)

## Problem

A spot reclaim on any backend publishes `SpotTerminationEvent` as CRITICAL, `EventsAnalyzer` fails the
test, and Argus records `test_error`. SCT-707 collects 7 longevity occurrences on AWS and Azure, plus 3 GCE
artifact runs between 2026-10-13 and 2026-10-16, all on `n2d-standard-2` in us-east1 and preempted within
minutes of boot. Nothing re-runs the test, so someone has to retrigger it by hand. Because of that, the
scylla-pkg pipelines that start release QA jobs force `provision_type=on_demand` (scylla-pkg#4684).

Release artifact builds are where this matters. A release branch runs each artifact job for a list of
instance types, one parallel branch and one builder per type: `scylla-2026.3/artifacts/artifacts-ami-test #36`
ran 20 types on 20 builders. Stage timings of that build, per branch, are about the same for every type:

| Stage | Time | DB node alive |
|---|---|---|
| Run SCT Test | ~6.7 min | yes |
| Collect log data | ~8.3 min | yes |
| Clean resources | ~5.8 min | until `clean-resources --post-behavior` destroys it |

Artifact configs leave `execute_post_behavior` at its default (false), so the DB node outlives the test and
is only destroyed in "Clean resources". The test itself is about a third of the node's billed time, and log
collection, which only matters when the test failed, is the largest part.

The release lists are growing. scylladb/scylla-pkg#6900 adds 16 `c4` and `c4d` `-lssd` types to the GCE
release list, taking a release GCE image build from 8 to 24 instance types, and scylladb/scylla-pkg#6890 adds
the arm64 GCE image test on `c4a-highmem-4-lssd`. Every extra type is another branch that can be reclaimed in
the same build: with a 5% chance of a reclaim per branch, 34% of 8-type builds and 71% of 24-type builds
would have at least one reclaimed branch. On spot, those builds stay reliable only with a retry.

The GCE instance type also changed silently on 2026-08-23, when the artifact configs moved from
`gce_instance_type_db: n2-standard-2` to `sizing_db: {vcpu: 2, memory: '>=8'}`. The GCE `db` preferred
families in `data/instance_catalog/sizing_config.yaml` list `n2d` before `n2`, so every GCE artifact job now
gets `n2d-standard-2`. Sizing never considers how often a type is reclaimed.

## Approach

The parts are independent. Part A pays off on its own and should land regardless of spot.

### Part A: shorter artifact runs, regardless of spot (review suggestion by soyacz)

- **Skip log collection when the test passed.** In `vars/artifactsPipeline.groovy`, run "Collect log data"
  only when the test failed. That removes ~8.3 minutes of node and builder time from every passing branch.
- **Speed up the test itself.** Run the independent checks in `artifacts_test.py` in parallel and drop
  waits that are not needed. **Needs Investigation:** how much of the ~6.7 minutes is provisioning and
  Scylla setup, which parallel checks cannot shorten.

### Part B: per-test retry after a spot reclaim, artifact tests only (#16546)

- **Configuration decides which tests retry.** New SCT option `spot_termination_retry`: a list of
  provision types, one per retry. It defaults to `[]` (off). The validator allows at most 2 entries and
  requires the last one to be `on_demand`, so a retrying test always ends with an attempt that cannot be
  reclaimed. Artifact tests use `[spot, on_demand]`: spot → spot → on-demand.
- **The decision is Python, the loop is Groovy.** `sct.py spot-retry-decision --attempt <n>` reads the test
  config and the finished run's CRITICAL and ERROR events from Argus, and prints
  `SPOT_RETRY_DECISION=<stop|spot|on_demand>`. A reclaim is a `SpotTerminationEvent`, which all 4 clouds
  publish, or an `OperationPreemptedError` from a provisioning-time preemption that had no on-demand
  fallback. The provisioner publishes no event for the latter on purpose: `provision_instances_with_fallback`
  catches it to downgrade to on-demand, and a CRITICAL event would fail runs the fallback saved. An
  unreachable Argus or a missing event prints `stop`, so nothing retries blindly.
- **The loop.** `vars/runWithSpotRetry.groovy` wraps one attempt: create Argus run → run test → collect
  logs → clean resources → finish Argus run → `spot-retry-decision`. On a retry it sets a new
  `SCT_TEST_ID` and `SCT_INSTANCE_PROVISION` and goes round again. A real abort is never retried and is
  recorded at once. A stage timeout is retried like any failure, because on GCE a reclaimed node can leave
  the test retrying SSH until the timeout fires (artifacts-ubuntu2404-test #696, #697).
- **Where the counter lives.** It is a local loop variable inside one build, bounded by a hard
  `attempt <= 3` in Groovy whatever the config says. No new build is triggered and nothing is persisted, so
  no stage, rebuild, description edit or Zeus can reset it. The build description
  (`i4i.large: spot-retried 2/2, passed`) is written for people and never read back.
- **Build result.** The test step no longer marks the build failed for each attempt, because a build
  result can only get worse and a passing retry would stay red. Only the last attempt's error is recorded,
  which keeps today's build result.
- **The instance-type loop.** Each `instance_type` branch runs on its own builder (every builder has 1
  executor), so the retry loop runs inside each branch with its own counter and Argus runs. Retries keep a
  type the job named explicitly; only a type chosen by sizing is swapped out (part C).

  Example: `instance_type="t3.large m6i.large i4i.large"` with `spot_termination_retry: [spot, on_demand]`:

  | Branch (own builder) | Attempt 1 | Attempt 2 | Attempt 3 | Branch result |
  |---|---|---|---|---|
  | `t3.large` (builder A) | spot, passed | — | — | passed; builder A freed after 1 run |
  | `m6i.large` (builder B) | spot, reclaimed | spot, same type, passed | — | passed |
  | `i4i.large` (builder C) | spot, reclaimed | spot, same type, reclaimed | on-demand, same type, passed | passed; builder C held for 3 runs |

  The build passes with 6 Argus runs (1 + 2 + 3). A reclaimed attempt keeps its `test_error`, so
  preemptions stay countable. The result email covers the last attempt.

### Part C: rank spot candidates by interruption rate

- **Offline data, no runtime network calls.** `sct_sizing.py update-catalog` writes an
  `interruption_bucket` per region into the instance catalog, next to the spot prices it already stores
  (0 = below 5%, 1 = 5–10%, 2 = 10–15%, 3 = 15–20%, 4 = above 20%).

  | Cloud | Source | Result |
  |---|---|---|
  | AWS | Spot Instance Advisor data (`spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json`), keyed by region, OS and type | bucket 0–4 |
  | Azure | Resource Graph `SpotResources`, type `microsoft.compute/skuspotevictionrate/location` (`azure-mgmt-resourcegraph` is already a dependency) | bucket 0–4 |
  | GCE | No per-type signal. Capacity Advisor (`sdcm/provision/gce/capacity_advisor.py`) returned 0.9 obtainability and 3600s estimated uptime, the API maximum, for `n2d-standard-2`, `n2-standard-2`, `n2-highmem-2` and `e2-standard-2` in us-east1 on 2026-10-18 | none |
  | OCI | No interruption data, and preemptible capacity has a flat price | none |

- **Selection rule.** For spot, `select_instance` in `sdcm/utils/cloud_catalog/instance_matcher.py` sorts
  by `interruption_bucket` first, then by the existing order. A missing bucket sorts after the known ones,
  so GCE and OCI keep today's order. A new list option `sizing_exclude_instance_types` (default empty)
  removes the reclaimed type on a spot retry.
- **GCE default, now.** Put `n2` ahead of `n2d` for the GCE `db` role, which restores the type artifact jobs
  used before 2026-08-23. **Needs Investigation:** a GCE ranking from our own Argus data (preempted runs
  divided by spot runs, per type and region) once 30 days of `SpotProvisionOutcomeEvent` data exist.

### Part D: opt in, then release loops

1. Set `spot_termination_retry: [spot, on_demand]` in `test-cases/artifacts/*.yaml` (spot backends only),
   including `gce-image.yaml` and the arm64 GCE image config that carry the new `c4`, `c4d` and `c4a` types.
2. After two weeks of master runs, propose in scylla-pkg that release artifact jobs drop the forced
   `on_demand`. This is a cross-repo change that releng owns.

### Not in scope: longevity

A longevity run keeps 6 DB nodes and 2 loaders for hours, so its chance of a reclaim is high, and a spot
retry would most likely be reclaimed again. Re-running a whole longevity test does not fit it. soyacz's
alternative is to recover inside the test instead: treat a reclaim like a nemesis, pause or abort the
running nemesis, ignore the expected errors for a while, and replace the node with another type or AZ.
That is a separate design. Short longevity can be measured first: a few spot runs of `longevity-10gb-3h`
with reclaims counted, before deciding on either approach.

### Cost

One `scylla-2026.3` `artifacts-ami-test` build (20 types). AWS us-east-1 live on-demand and spot prices on
2026-10-18, node time per branch from the stage timings above, and builders at ~$0.04/h (2 vCPU, 4–8 GiB,
on-demand):

| Scenario | Per build | vs today |
|---|---|---|
| Today: on-demand, logs always collected | $24.74 | — |
| Part A: on-demand, skip logs on pass | $15.13 | −39% |
| Part A, with the test 2× faster (assumed) | $11.04 | −55% |
| Spot, today's flow | $7.96 | −68% |
| Part A + spot | $4.89 | −80% |
| Part A (2× faster) + spot | $3.59 | −85% |
| Part A + spot, the 4 most expensive types reclaimed once | $8.59 | −65% |
| Part A + spot, the 4 most expensive types reclaimed twice | $19.17 | −23% |
| Part A + spot, all 20 types reclaimed twice | $28.45 | +15% |

Builders are $0.22–0.37 of each build; a retried branch adds about $0.02 of builder time. A reclaimed attempt
still collects logs and cleans up, which is why the worst case costs more than the node time alone.

## Files to Modify

Part A:
- `vars/artifactsPipeline.groovy` -- run "Collect log data" only when the test failed
- `artifacts_test.py` -- run independent checks in parallel (scope after the investigation above)

Part B (#16546):
- `sdcm/sct_config/mixins/common.py`, `sdcm/sct_config/config.py`, `defaults/test_default.yaml` --
  `spot_termination_retry` and its validator
- `sdcm/utils/spot_retry.py` (new file), `sct.py` -- the decision and the `spot-retry-decision` command
- `sdcm/sct_provision/instances_provider.py` -- keep a preemption out of a failed on-demand fallback's
  traceback, so the decision does not read it as a reclaim
- `vars/runWithSpotRetry.groovy`, `vars/spotRetryDecision.groovy` (new files), `vars/sctScript.groovy`,
  `vars/artifactsPipeline.groovy` -- the loop
- `unit_tests/unit/test_spot_retry.py` (new file)

Part C:
- `sdcm/utils/cloud_catalog/catalog_generator.py` -- AWS advisor and Azure eviction buckets
- `sdcm/utils/cloud_catalog/instance_matcher.py` -- spot sort order, `sizing_exclude_instance_types`
- `data/instance_catalog/sizing_config.yaml` -- `n2` before `n2d` for the GCE `db` role
- `data/instance_catalog/aws.yaml`, `data/instance_catalog/azure.yaml` -- regenerated
- `docs/cross-cloud-sizing.md` -- interruption ranking and its data sources

Part D:
- `test-cases/artifacts/*.yaml` (spot backends)

## Verification

### Unit tests

In `unit_tests/`, with no cloud access:
- `spot-retry-decision`, table-driven over attempt 1/2/3, policy `[]`, `[on_demand]` and
  `[spot, on_demand]`, reclaim present or absent, `OperationPreemptedError`, Argus unreachable, and a
  replay-only Argus client.
- Config validator: rejects 3 entries, and rejects a list whose last entry is not `on_demand`.
- Matcher: for spot, a bucket-0 candidate beats a cheaper bucket-3 one; on-demand order is unchanged; an
  excluded type is never chosen.
- Every changed `vars/*.groovy` compiles in a groovy container, because lint-pipelines never compiles them.

### Staging

Reclaims are injected with each cloud's own tool: AWS FIS `aws:ec2:send-spot-instance-interruptions`
(**Needs Investigation:** FIS IAM role in the QA account), `az vm simulate-eviction`, and
`gcloud compute instances simulate-maintenance-event` on a GCE spot VM (**Needs Investigation:** confirm it
logs `compute.instances.preempted`). Runs go through `utils/staging_trigger` into `scylla-staging/fruch/`:

| # | Job | Injection | Expected |
|---|---|---|---|
| 1 | artifacts-ubuntu2604 (GCE) | attempt 1 | attempt 2 passes, description `spot-retried 1/2, passed` |
| 2 | artifacts-ami (AWS) | attempts 1 and 2 | attempt 3 provisioned `on_demand` passes |
| 3 | artifacts-azure-image | attempt 1 | attempt 2 passes |
| 4 | artifacts-ubuntu2604 | none; bad `scylla_repo` | exactly one attempt, build failed, logs collected |
| 5 | artifacts-ubuntu2604 with policy `[]` | attempt 1 | exactly one attempt, `test_error` as today |
| 6 | artifacts-ami with `instance_type="t3.large m6i.large"` | only the `t3.large` branch | only that branch retries, with the same type |
| 7 | artifacts-ubuntu2604, passing | none | no "Collect log data" stage, node destroyed in "Clean resources" |
| 8 | artifacts-ubuntu2604 | abort the build mid-test | no retry, Argus `aborted` |

### Checklist

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_spot_retry.py -v`
- [ ] Staging runs 1–8 each match their expected outcome, with Argus and Jenkins links in the PRs
- [ ] `uv run sct.py sizing preview test-cases/artifacts/ubuntu2604.yaml` resolves the GCE db type to
      `n2-standard-2`
- [ ] Two weeks after part D: no master artifact build ends in `test_error` caused by a spot reclaim
- [ ] `uv run sct.py pre-commit` passes
