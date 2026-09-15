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

**Spot is genuinely volatile.** Over 90 days, for five instance types SCT runs, in us-east-1 and
eu-west-1: prices change 2-4 times per AZ per day, with a 21%-132% min-to-max spread. A cached
value drifts 8% after a week and 12% after a month (worst case seen: 40%). For comparison, the
spread *across AZs at any single instant* is 22%-38% — so no single number per region-and-type is
ever precise, however fresh.

**But the AWS API is far cheaper than assumed.** `DescribeSpotPriceHistory` takes a *list* of
instance types, so the unit of work is a region, not an instance type:

| Query | Latency | Result |
|---|---|---|
| 3 instance types (a whole run), current prices | ~190 ms | 16 rows, no pagination |
| 15 instance types | ~200 ms | 80 rows, no pagination |

Call cost is essentially independent of how many instance types are asked for. **Caching AWS spot
prices buys nothing and costs 12% accuracy.**

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
genuinely absent (local development), fall back to the on-demand catalog price and mark the result
as such. **The estimate must never fail its caller.**

### 2. GCE — keep it in the catalog, but source it from the Billing API

GCE stays catalog-only at estimate time, and this was checked rather than assumed. Its pricing
API is shaped differently from the AWS one: the Cloud Billing Catalog API is a bulk listing of
every Compute Engine SKU, not a targeted "price these three machine types now" query. Listing
thousands of SKUs to price three machine types is the wrong shape for a per-run call, and buys
nothing anyway — GCP sets spot prices administratively and moves them at most monthly, so a
checked-in value is accurate here in a way an AWS one never is.

For the *generator*, though, that API is clearly the better source than today's scraping. The
pages the generator parses are 18 MB (spot) and 36 MB (on-demand), take seconds to fetch, and are
matched with regexes that break whenever Google restyles a page. The Catalog API returns
structured per-region SKU rates for the same data, including spot, and needs no API key — the
existing keystore service account authenticates fine.

**Prerequisite:** the Cloud Billing API is currently disabled on the SCT project, so the call
fails with an explicit "has not been used in project ... or it is disabled" error. Enabling it is
a one-line project change and there is no published per-call charge for the API; the practical
limit is a 300-calls-per-minute-per-project quota, against which a monthly catalog regeneration
is nothing. Page count and latency should be measured once it is enabled — that number is not yet
known, and the scrape remains the fallback until it is.

### 3. Refresh cadence

- **AWS prices:** no cadence — always live.
- **GCE prices:** refreshed with the catalog. Monthly is sufficient and matches how often GCP
  moves them; quarterly is not.
- **AWS interruption rates:** monthly, with the catalog (below).

### 4. Keep the interruption rate, drop the rest of the advisor

AWS publishes a Spot Instance Advisor dataset as public JSON needing no credentials — one request
covers 34 regions and ~1,150 instance types. Its savings percentages are now redundant against
live pricing, but its **interruption-rate bucket is not available from the pricing API at all**,
and is the input a later "is spot safe for this run" decision needs. A run that is cheap but
likely to be killed is not cheap. Store that one field in the catalog; it is a slow-moving
aggregate, so monthly refresh suits it.

### 5. Why not a shared S3 cache

Considered and rejected on the measurements above. For AWS it would add a writer, a bucket,
credentials, a staleness policy and new failure modes in order to save roughly 200 ms per
provisioning event, while making every price up to a cache-window old. For GCE the checked-in
catalog already *is* the cache, refreshed on the same cadence the prices move. Revisit only if
API throttling ever shows up, which a few dozen describe calls a day will not cause.

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
