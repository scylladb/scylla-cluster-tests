# Mini-Plan: Spot-Aware Cost Estimates for AWS and GCE

**Date:** 2026-09-14
**Owner:** fruch
**Estimated LOC:** ~400
**Related Jira:** [SCT-852](https://scylladb.atlassian.net/browse/SCT-852) (epic [SCT-851](https://scylladb.atlassian.net/browse/SCT-851)), [SCT-1005](https://scylladb.atlassian.net/browse/SCT-1005) (fallback default)
**Depends on:** PR #15987 (pre-provisioning cost estimate) -- introduces `cost.py`, which this
plan modifies. This work cannot start until that merges.

## Problem

The pre-provisioning estimate always prices on-demand from the checked-in catalog. Most SCT runs
are spot, so every estimate overstates the real cost by roughly two to four times. An estimate
wrong in a consistent direction teaches people to ignore it — and the epic wants to gate runs on
this number, where a systematic overstatement blocks runs that are actually cheap.

The existing spot paths do not help: GCE has a hardcoded n1/n2 table that reports unknown for
every family SCT runs, and the AWS path was written per instance type, which made it look far
more expensive than it is.

### What the numbers actually say

Both halves of the design were measured rather than assumed.

**AWS spot is genuinely volatile.** Over 90 days, five instance types SCT runs, us-east-1 and
eu-west-1: prices change 2-4 times per AZ per day, 21%-132% min-to-max spread. A cached value
drifts 8% after a week, 12% after a month (worst seen: 40%). But the spread *across AZs at any
instant* is 22%-38% — so no single number per region-and-type is ever precise, however fresh.

**But the AWS API is far cheaper than assumed.** `DescribeSpotPriceHistory` takes a *list* of
instance types, so the unit of work is a region, not an instance type: 3 types cost ~190 ms and 15
cost ~200 ms, both unpaginated. **Caching AWS spot prices buys nothing and costs 12% accuracy.**

## Approach

The two clouds get opposite answers, because their pricing behaves differently. Neither needs new
shared infrastructure.

### 1. AWS — query live, one call per region

A run uses three distinct instance types (db, loader, monitor), all covered by one call. Expected
call counts:

| Moment | Calls |
|---|---|
| Pre-provisioning estimate | 1 per region |
| Provisioning a cluster | 1 per region, regardless of node count |
| Nodes added mid-test (grow/shrink nemesis) | at most 1 per region per cache window |
| Whole 3-day longevity run | order of tens, at ~200 ms each |

Memoise per region and instance type with a short TTL, so a burst of node creations shares one
lookup and a long test still refreshes as prices move. Take the mean across AZs and record the
spread — the estimate cannot know which AZ it will land in, and pretending otherwise is false
precision.

Credentials are available where this runs: the estimate stage sits on the Jenkins builder, which
already runs hydra to create the SCT runner and hard-fails without AWS credentials. Where they are
absent (local development), fall back to the on-demand catalog price and say so. **The estimate
must never fail its caller.**

### 2. GCE — keep it in the catalog, sourced from the Billing Catalog API

GCE stays catalog-only at estimate time, verified rather than assumed. Its pricing API is shaped
differently from the AWS one: the Cloud Billing Catalog API only lists *every* Compute Engine SKU,
with no server-side filter by family, region or usage type. Measured on the SCT project:

| Property | Measured |
|---|---|
| API calls for a full listing | 7 (pages of 5,000) |
| Wall time | ~13 s median (8.8 s - 18.7 s over 3 runs) |
| Payload | 28.3 MB, 32,873 SKUs |
| Of which Spot | 3,816, of which 1,332 in families SCT uses, across 48 regions |

Pulling 28 MB to price three machine types is the wrong shape for a per-run call, and buys nothing
anyway: GCP sets spot administratively, so the value barely moves between catalog refreshes.

For the *generator* it is clearly better than today's scraping, which fetches an 18.4 MB spot page
and a 35.7 MB on-demand page and matches them with regexes that break whenever Google restyles.
One structured source replaces both, and needs no API key — the existing keystore service account
authenticates with the `cloud-platform` scope.

**Validated end to end.** GCE prices are resource-based: a machine type's rate is
`vcpus x Core rate + memory_gb x Ram rate`, per family per region, with spot carried as the
`Preemptible` usage type. Recomputing *on-demand* this way reproduces the checked-in catalog
exactly for N2 and E2, standard and highmem — which is what confirms the SKU selection is right,
and therefore that the spot figures derived the same way are trustworthy.

**The spot discount is strongly region-dependent** and must be stored per region, not as one
global rate: for the same N2 core it ranges from 40% off in us-east1 to 71-78% off in
europe-west1, asia-northeast1 and southamerica-east1.

**Cost of using it:** no published per-call charge — the Catalog API serves *"publicly available
Google Cloud pricing information"*. The binding limit is quota, 300 calls per minute per project
(975 per organisation). Seven calls per monthly regeneration is roughly 2% of a single minute's
allowance, so quota is not a constraint at any plausible cadence. The API must be enabled on the
project (`cloudbilling.googleapis.com`); it was disabled until this plan was written.

### 3. Refresh cadence

- **AWS prices:** no cadence — always live.
- **GCE prices:** refreshed with the catalog. Monthly is sufficient and matches how often GCP
  moves them; quarterly is not.
- **AWS interruption rates:** monthly, with the catalog (below).

### 4. Keep the interruption rate, drop the rest of the advisor

AWS publishes a Spot Instance Advisor dataset as public JSON needing no credentials — one request
(1.2 MB, ~1.8 s) covers 34 regions and ~1,150 instance types. Its savings percentages are redundant
against live pricing, but its **interruption-rate bucket is not available from the pricing API at
all**, and is what a later "is spot safe for this run" decision needs — a run that is cheap but
likely to be killed is not cheap. Store that one field; monthly refresh suits it.

### 5. Why not a shared S3 cache

Considered and rejected on the measurements above. For AWS it would add a writer, a bucket,
credentials, a staleness policy and new failure modes to save ~200 ms per provisioning event,
while making every price up to a cache-window old. For GCE the checked-in catalog already *is*
the cache. Revisit only if throttling appears, which a few dozen calls a day will not cause.

### 6. Fallback to on-demand must be visible in estimate and bill

When spot capacity is short SCT falls back to on-demand, so a spot run can silently cost several
times its estimate:

- **Estimate:** when the resolved config enables fallback, warn and report the on-demand ceiling
  beside the spot figure. The gate should read the ceiling — underestimating is the dangerous
  direction.
- **Billing:** report each node's *actual* lifecycle, which SCT already knows per node, and
  summarise how many nodes ended up on-demand. A run that quietly fell back is the most useful
  thing this feature can report.

Fallback is currently the AWS and Azure backend default rather than artifact-only, so this warning
would fire on every AWS run until [SCT-1005](https://scylladb.atlassian.net/browse/SCT-1005) flips
it. That ticket is independent of this work and should land first.

### 7. Out of scope

Azure and OCI spot. AZ-level price selection: the spread is reported, not modelled away.

## Files to Modify

- `sdcm/utils/cloud_catalog/pricing.py` -- rework the AWS spot lookup to batch instance types per
  region and memoise with a TTL; drop the GCE hardcoded spot table
- `sdcm/utils/cloud_catalog/cost.py` -- **introduced by PR #15987** -- resolve AWS spot live and
  GCE spot from the catalog; derive lifecycle from config; report spot estimate, on-demand ceiling,
  AZ spread and the fallback warning; degrade to catalog pricing without credentials
- `sdcm/utils/cloud_catalog/catalog_generator.py` -- source GCE spot (and ideally on-demand)
  rates from the Cloud Billing Catalog API instead of scraping, keeping the scrape as a fallback;
  fetch AWS interruption-rate buckets from the advisor dataset
- `sdcm/utils/cloud_catalog/instance_catalog.py` -- carry GCE spot price and AWS interruption
  bucket, with per-region accessors mirroring the on-demand one
- `data/instance_catalog/gce.yaml` -- regenerated with spot prices
- `data/instance_catalog/aws.yaml` -- regenerated with interruption buckets
- `unit_tests/unit/test_cost.py` -- **introduced by PR #15987** -- live-vs-catalog resolution,
  unknown-stays-unknown, ceiling and fallback warning, credential-less degradation
- `unit_tests/unit/test_catalog_generator.py` -- GCE spot scrape and advisor parsing, against
  recorded fixtures rather than live endpoints

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_cost.py unit_tests/unit/test_catalog_generator.py -v`
- [ ] Pricing a whole run costs **one** AWS call per region, asserted by counting calls against a
      mocked client -- node count and instance-type count must not change it
- [ ] With AWS credentials unavailable the estimate still returns, marked as on-demand-based, and
      never raises
- [ ] Estimating a GCE config makes no network calls at all
- [ ] GCE catalog generation via the Billing API produces the same prices as the scrape it
      replaces, for a sample of machine types SCT runs
- [ ] A spot estimate is materially below the on-demand estimate for the same config, and both are
      reported
- [ ] A config with fallback enabled produces a visible warning and shows the on-demand ceiling
- [ ] An instance type with no spot price reports unknown -- never zero, never a silent
      fall-through to the on-demand price
- [ ] Cross-check one finished spot run against the cloud-monitor cost site for the same test id
- [ ] Catalog regeneration is reproducible: a second `sct sizing update-catalog` run with no
      upstream price change produces no diff
- [ ] Full unit test suite passes: `uv run sct.py unit-tests`
- [ ] `uv run sct.py pre-commit` passes
