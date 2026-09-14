# Mini-Plan: Spot-Aware Cost Estimates for AWS and GCE

**Date:** 2026-09-14
**Owner:** fruch
**Estimated LOC:** ~400
**Related Jira:** [SCT-852](https://scylladb.atlassian.net/browse/SCT-852) (epic [SCT-851](https://scylladb.atlassian.net/browse/SCT-851))
**Depends on:** PR #15987 (pre-provisioning cost estimate) -- introduces `cost.py`, which this
plan modifies. This work cannot start until that merges.

## Problem

The pre-provisioning estimate always prices on-demand from the checked-in catalog. Most SCT
runs are spot, so every estimate overstates the real cost, typically by two to four times. An
estimate that is wrong in a consistent direction teaches people to ignore it — and the epic
wants to gate runs on this number, so a systematic overstatement would block runs that are
actually cheap.

The spot paths that exist today cannot fix this:

- **AWS** queries the live spot price history once per region and instance type. That needs
  EC2 credentials in the target region and returns a three-hour trailing average.
- **GCE** has a hardcoded table covering only older n1/n2 families. Every machine family SCT
  currently runs misses it and reports unknown.

Neither is usable from the estimate, which runs on the Jenkins builder before provisioning,
is not guaranteed cloud credentials, and must not make per-instance API calls.

## Approach

The governing principle: **spot prices belong in the checked-in catalog, resolved offline by
the catalog generator. The estimate itself makes zero API calls** — the same contract the
on-demand estimate already honours via its catalog-only lookup.

End to end: `sct sizing update-catalog` → per-cloud spot source → normalise to absolute
USD/hour per region → `data/instance_catalog/<cloud>.yaml` → catalog-only rate lookup →
estimate and (later) the per-resource Argus report.

### 1. AWS — one unauthenticated request covers every region

AWS publishes the Spot Instance Advisor dataset as a public JSON file requiring no
credentials. A single request returns, for 34 regions and roughly 1,150 Linux instance types,
a savings percentage against on-demand and an interruption-rate bucket. One request replaces
the entire per-instance spot-history path.

Store the **derived absolute price**, not the percentage, so the runtime lookup has one shape
across clouds and the cloud-specific arithmetic stays in the generator.

Store the **interruption-rate bucket** too. It costs nothing extra in the same payload, and it
is exactly the input a later "is spot safe for this run" decision needs — an estimate that is
cheap but has a high chance of the run dying is not actually cheap.

**Prerequisite — regenerate the AWS catalog with per-region on-demand prices.** The AWS
catalog currently holds one price per instance type with no region dimension, unlike GCE which
is already per-region. Deriving spot from a region-specific savings percentage applied to a
single-region on-demand price mixes two bases and produces a number that is wrong in a way
nobody can audit. `catalog_generator.py:generate_aws_catalog` already accepts a region list and
emits a per-region mapping, so this is a data refresh rather than new code. It also corrects a
pre-existing inaccuracy in the on-demand estimate for non-default regions.

### 2. GCE — one more page on a scrape that already runs

The generator already scrapes Google's general-purpose and storage-optimized pricing pages for
on-demand prices. Spot prices are not on those pages; they live on the dedicated Spot VMs
pricing page, which carries per-machine-type spot prices for the families SCT uses, z3
included. Extending the existing scrape to that one page adds a single request at generation
time and none at estimate time, and reuses the parsing already written for the other two.

GCE spot prices are administratively set rather than auction-driven, so a checked-in GCE value
stays accurate far longer than an AWS one.

### 3. Catalog format

Spot sits beside the existing on-demand price, same per-region shape, so an absent key means
unknown rather than free:

```yaml
- instance_type: i4i.4xlarge
  price_per_hour:            # on-demand, per region
    us-east-1: 1.371
  spot_price_per_hour:       # absent when unknown
    us-east-1: 0.617
  spot_interruption: "5-10%" # AWS only; advisory, not used in arithmetic
```

This roughly doubles catalog file size. If that is unwelcome, the lever is restricting
generation to the regions SCT actually uses rather than every region the provider offers —
the GCE catalog currently carries 43 regions for 89 machine types, far more than SCT runs in.

### 4. Lookup behaviour

`cost.py:get_hourly_rate` answers spot from the catalog like it answers on-demand. Unknown
stays unknown and never becomes zero — the existing rule, unchanged.

Remove the GCE hardcoded spot table outright; it returns unknown for everything SCT runs, so
it has no users to break. Recommend also removing the AWS live spot-history path rather than
keeping it as an opt-in: leaving two sources for the same number invites them to disagree, and
nothing would call the slow one.

### 5. Teach the estimate which lifecycle the run will use

The estimate currently hardcodes on-demand. It should read the configured provision type and
price accordingly, treating every spot variant as spot.

One wrinkle worth deciding rather than defaulting: SCT falls back from spot to on-demand when
capacity is short, so a spot estimate is a **lower** bound, not a prediction. Recommend
reporting the spot figure as the estimate and the on-demand figure as the ceiling, and having
the future approval gate read the ceiling — underestimating is the dangerous direction for a
gate, which matches the ceiling-versus-truth split already recorded for the reporting plan.

### 6. Out of scope

Azure and OCI spot. Refresh cadence is a real gap — nothing currently regenerates the catalog
on a schedule, so any checked-in price drifts — but automating it is a separable concern and
should not be bundled here. AWS savings percentages move slowly and GCE spot barely moves, so
a manual refresh is tolerable in the short term; it should not stay that way.

## Files to Modify

- `sdcm/utils/cloud_catalog/catalog_generator.py` -- fetch the AWS spot advisor dataset and
  derive absolute spot prices; scrape the GCE spot pricing page; emit the new catalog fields
- `sdcm/utils/cloud_catalog/instance_catalog.py` -- carry spot price and interruption rate on
  the instance record, with a per-region accessor mirroring the on-demand one
- `sdcm/utils/cloud_catalog/cost.py` -- **introduced by PR #15987, not yet on master** --
  resolve spot rates from the catalog; derive the lifecycle from config instead of assuming
  on-demand; report estimate and ceiling
- `sdcm/utils/cloud_catalog/pricing.py` -- drop the GCE hardcoded spot table and the AWS live
  spot-history path
- `data/instance_catalog/aws.yaml` -- regenerated with per-region on-demand prices plus spot
- `data/instance_catalog/gce.yaml` -- regenerated with spot prices
- `unit_tests/unit/test_cost.py` -- **introduced by PR #15987** -- spot resolution,
  unknown-stays-unknown, ceiling reporting
- `unit_tests/unit/test_catalog_generator.py` -- advisor parsing and GCE spot scrape, on
  recorded fixtures rather than live endpoints

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_cost.py unit_tests/unit/test_catalog_generator.py -v`
- [ ] Estimating a spot longevity config makes no network calls and no cloud API calls -- the
      property the pipeline stage depends on
- [ ] A spot estimate is materially below the on-demand estimate for the same config, and both
      are reported
- [ ] An instance type with no spot entry reports unknown, never zero and never a silent
      fall-through to the on-demand price
- [ ] Spot prices for a sample of AWS and GCE types SCT actually runs are within a sane margin
      of the provider consoles; sanity-check one finished spot run against the cloud-monitor
      cost site
- [ ] Catalog regeneration is reproducible: a second `sct sizing update-catalog` run with no
      upstream price change produces no diff
- [ ] Full unit test suite passes: `uv run sct.py unit-tests`
- [ ] `uv run sct.py pre-commit` passes
