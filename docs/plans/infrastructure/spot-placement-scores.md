---
status: implemented
domain: infrastructure
created: 2026-08-23
last_updated: 2026-08-23
owner: fruch
jira: SCT-850
---
# Spot Utilization Driven by Spot Placement Scores

## Problem Statement

[SCT-850](https://scylladb.atlassian.net/browse/SCT-850) asks to "switch back to using spot instances for tests
shorter than 12 hours by utilizing spot placement scores and selecting optimal regions and availability zones".

SCT already defaults to spot on every cloud backend (`defaults/aws_config.yaml`, `defaults/gce_config.yaml`,
`defaults/azure_config.yaml`, `vars/longevityPipeline.groovy`). The real problems were:

1. **Silent downgrade.** `ProvisionPlan.provision_instances()` walks `[spot, on_demand]` and quietly succeeds
   on-demand after logging one ERROR line. Nothing recorded the realized provision type, so the actual
   spot-vs-on-demand spend split was unmeasured — and any claim about savings unverifiable.
2. **Placement blind to spot capacity.** AZ ordering was alphabetical; `region: random` was a
   `Collections.shuffle`. Measured with our own credentials, `eu-west-1` (the default region) scored **1/10**
   for `i4i.2xlarge` while `eu-west-3`, `eu-north-1`, `us-east-2` and `eu-central-1` scored **3**.
3. **Request shape did not match the scoring assumption.** Spot fleet requests set no `AllocationStrategy`,
   while the API documents that a high per-AZ score "assumes ... the capacity-optimized allocation strategy".
   Ranking by a score we then didn't honour would have measured the wrong thing.
4. **No duration policy.** 100+ jenkinsfiles hand-pinned `provision_type` with no rule behind it.

## Key API Constraints

From the [GetSpotPlacementScores docs](https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_GetSpotPlacementScores.html),
all encoded in `sdcm/provision/aws/spot_placement_score.py`:

| Constraint | Consequence |
|---|---|
| "We recommend ... at least three instance types. If you specify one or two ... the returned placement score will always be low" | Scores are a **relative ranking only**, never an absolute verdict. `spot_placement_score_min` defaults to `0` (drop nothing) and a warning is logged below 3 types. |
| "a recommendation only. No score guarantees that your Spot request will be fully or partially fulfilled" | The existing AZ/region capacity fallback stays the safety net. |
| A high per-AZ score assumes a single AZ + `capacity-optimized` allocation strategy | `SPOT_FLEET_ALLOCATION_STRATEGY` is now set on fleet requests. |
| `RegionName.N` max 10 entries | Region lists are chunked (`_chunk_regions`). |
| Response is the top 10 placements | Unscored AZs/regions keep their order and go **last** — absent ≠ bad. |
| Response reports `AvailabilityZoneId` (`euw1-az3`), not names | Translated per-account via `describe_availability_zones`; the mapping is shuffled per account and must never be hardcoded. |
| `ec2:GetSpotPlacementScores` is a distinct IAM action | Every failure path returns `[]` so callers keep their previous ordering. Runners can lag a policy rollout. |

Measured effect of type diversification, target capacity 6: `i4i.2xlarge` alone tops out at **3**; with
`i4i.4xlarge`/`i7i.2xlarge`/`i8g.2xlarge` added, `us-west-2` scores **9** in all four AZs. Type
diversification matters more than region choice — which is why this integrates with
`instance_type_db_alternatives` from the EC2 Fleet work rather than duplicating it.

## What Was Implemented

**Observability (measure first).** `SpotProvisionOutcomeEvent` (`sdcm/sct_events/system.py`) is published once
per provisioning step from `ProvisionPlan.provision_instances()`, recording requested vs realized provision
type, region, AZ, instance type and count, with a `downgraded` flag. This is the acceptance criterion for
everything else: without it, no saving is provable.

**Scoring module.** `sdcm/provision/aws/spot_placement_score.py` — `get_scores()` (TTL-cached, paginated,
region-chunked, fail-soft), plus `rank_az_letters()` and `rank_regions()`.

**Request shape.** `AllocationStrategy: capacity-optimized` on spot fleet requests
(`sdcm/provision/aws/utils.py`), making the scores predictive of our own fulfillment.

**Placement ordering.** `AZResolver` (`sdcm/provision/aws/az_resolver.py`) now ranks by score in
`resolve()`, `get_fallback_candidates()`, `get_region_fallback_candidates()` and
`get_dc_fallback_candidates()`. The offerings intersection remains the hard filter; scores only reorder what
survives it. Configured AZ letters keep priority unless `spot_score_overrides_configured_az` is set. Multi-region
configs are skipped — an AZ letter must be valid in every region, so no single ranking is meaningful.

**Optional upfront relocation.** `spot_score_region_relocation_margin` (default `0` = off) relocates to a
better-scoring, VPC-peered region *before* the first attempt, instead of waiting for the configured region to
fail. Useful for `region: random` jobs that land badly.

**Duration policy.** `SCTConfiguration._apply_duration_based_provision_policy()` sets spot at or below
`spot_max_test_duration` (default 720 min = 12h) and on_demand above it — but **only when
`instance_provision` was not set explicitly**. Provenance is captured during `__init__` from the user config
files and the `SCT_*` environment, before the merge flattens the layers; values inherited from `defaults/` are
not explicit. This is what keeps the 36 `perf-v17` jobs on-demand.

**Debug/CI CLI.** `hydra spot-placement-scores` for inspection and `hydra pick-spot-region` (prints
`SPOT_REGION=<region>`) for CI use.

## Known Limitation: Jenkins Builder Region

`vars/getJenkinsLabels.groovy` resolves `region: random` and selects a region-specific builder ASG label, and
`initAwsRegionParam.groovy` then forces the test region to equal the builder region. That function runs **on the
Jenkins controller before any agent is allocated**, so `sh()` is unavailable and `hydra` cannot be called there.
The builder-region shuffle is therefore unchanged, and `getJenkinsLabels.groovy` now only filters candidates to
those that actually have a builder label. Score-driven region choice happens inside SCT instead, via the
region-fallback ordering and the optional upfront relocation.

Note also that three region lists disagree: `AWS_SUPPORTED_REGIONS` (8, includes `eu-west-1`/`us-east-1`, omits
`ca-central-1`), `getJenkinsLabels.groovy` (7, the inverse), and the `jenkins_labels` map. Reconciling them is
follow-up work.

## GCE Equivalent (not implemented)

GCE has `advice.capacity`, verified against the live Compute discovery documents:

| | `compute/beta` | `compute/v1` |
|---|---|---|
| `advice.capacity` — `scores.obtainability` (0.0-1.0), `scores.estimatedUptime` | present | **absent** |
| `advice.capacityHistory` — `preemptionHistory[].preemptionRate`, `priceHistory[]` | present | **absent** |

`POST projects/{project}/regions/{region}/advice/capacity`, scopes `cloud-platform` or `compute`, IAM
`compute.advice.capacityHistory` (in `roles/compute.viewer`). Status: **Preview**.

### How to reach it — no dependency upgrade needed

The typed SDK is a dead end here: `compute_v1.AdviceClient` exposes only `calendar_mode`, in both the pinned
`google-cloud-compute` 1.48.0 **and** the latest 1.50.0. Upgrading that package does *not* unlock
`advice.capacity`.

The discovery-based client does, and it is **already a dependency** — `google-api-python-client>=2.93.0`
(2.197.0 installed), which SCT already uses for other GCE gaps:

```python
# sdcm/utils/gce_region.py:76 and gce_utils.py:744 already do exactly this for iam/v1 and logging/v2
from googleapiclient.discovery import build
service = build("compute", "beta", credentials=credentials, cache_discovery=False)
service.advice().capacity(project=project, region=region, body={...}).execute()
```

So a GCE implementation needs no new or upgraded dependency — only the beta API version string. The remaining
reasons it is deferred rather than blocked are that the endpoint is **Preview** (Pre-GA terms, limited support,
subject to change) and that the payoff is smaller than on AWS: GCE already defaults to spot, so its real gap is
fallback breadth (zone fallback only handles
`count == 1` in `cluster_gce.py`, and on-demand fallback triggers only on `OperationPreemptedError` in
`instances_provider.py`).

## Follow-ups

- Graceful spot interruption handling — [SCT-707](https://scylladb.atlassian.net/browse/SCT-707). Reclamation is
  a CRITICAL `SpotTerminationEvent` → `TestFailure` → Argus `test_error`. More spot means more of these; this
  work only *measures* them.
- Jenkins builder ASGs on spot — [SCT-844](https://scylladb.atlassian.net/browse/SCT-844).
- Reconcile the three AWS region lists, ideally enforced by `lint-pipelines`.
- Pass `instance_type_db_alternatives` into the score query once the EC2 Fleet work lands (the 3 → 9 lever).
- GCE `advice.capacity` — implementable now via the already-present `google-api-python-client` discovery client
  (`build("compute", "beta", ...)`), no dependency change; gated on accepting a Preview endpoint. Waiting for
  `v1` is the conservative alternative, not a technical requirement.
