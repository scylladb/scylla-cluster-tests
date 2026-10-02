# Mini-Plan: Spot-Aware Cost Estimates

**Date:** 2026-09-14 (updated 2026-09-23)
**Owner:** fruch
**Estimated LOC:** ~450
**Related Jira:** [SCT-852](https://scylladb.atlassian.net/browse/SCT-852) (epic [SCT-851](https://scylladb.atlassian.net/browse/SCT-851))
**Depends on:** PR #15987 -- introduces `cost.py`, which this plan modifies
**Split from this plan:** on-demand fallback visibility, and catalog refresh automation —
each now its own mini-plan

## Problem

The pre-provisioning estimate always prices on-demand from the checked-in catalog. Most SCT runs
are spot, so every estimate overstates the real cost by roughly two to four times. An estimate
wrong in a consistent direction teaches people to ignore it — and the epic wants to gate runs on
this number, where a systematic overstatement blocks runs that are actually cheap.

The existing spot paths do not help: GCE has a hardcoded n1/n2 table that reports unknown for
every family SCT runs, OCI reports zero unconditionally, Azure never asks for spot at all, and the
AWS path was written per instance type, which made it look far more expensive than it is.

## Approach

Each cloud was measured rather than assumed, and they need different answers. None needs new
shared infrastructure — a shared S3 price cache was rejected: it would add a writer, bucket,
credentials and staleness policy to save ~200 ms per provisioning event while making every price
a cache-window old.

| Cloud | Source | Cost to obtain | When |
|---|---|---|---|
| AWS | `DescribeSpotPriceHistory` | 1 call per region, ~190 ms | live, per run |
| GCE | Cloud Billing Catalog API | 7 calls, ~13 s, 28 MB | generator, monthly |
| Azure | Retail Prices API | **free** — already in a response we fetch | generator |
| OCI | none needed — flat 50% off | 0 calls | derived |

### 1. AWS — live, one call per region

`DescribeSpotPriceHistory` takes a *list* of instance types, so the unit of work is a region, not
an instance type: three types (db, loader, monitor) cost ~190 ms and fifteen cost ~200 ms, both
unpaginated. So a run is one call per region, **regardless of node count**, and a whole 3-day
longevity with a grow/shrink nemesis is order of tens of calls.

AWS spot is genuinely volatile — 2-4 price changes per AZ per day, and a cached value drifts 12%
in a month — so it is queried live and never cached to disk. Take the mean across AZs and report
the spread: at any instant that spread is 22%-38%, wider than a month of staleness, so no single
number per region-and-type is ever precise and pretending otherwise is false precision.

**The call happens only when it is needed.** Cheap is not free:

- **Gate on lifecycle first.** An `on_demand` run must make **zero** spot calls, decided from the
  resolved provision type before touching the API.
- **Resolve lazily** — price a region when something asks for a rate in it, never warming every
  region or role up front.
- **Memoise per region with a short TTL**, process-wide, so a burst of node creations costs one
  lookup, the estimate and the per-node report reuse one answer, and a long test still refreshes.
- **Never per node.** Node count must not multiply calls; a test should pin this.

Credentials are available where this runs: the estimate stage sits on the Jenkins builder, which
already runs hydra to create the SCT runner and hard-fails without AWS credentials. Where they are
absent (local development), fall back to the on-demand catalog price and say so. **The estimate
must never fail its caller.**

### 2. GCE — catalog, sourced from the Billing Catalog API

The Cloud Billing Catalog API lists *every* Compute Engine SKU with no server-side filter, so a
full listing is 7 calls, ~13 s and 28.3 MB for 32,873 SKUs. Pulling 28 MB to price three machine
types is the wrong shape for a per-run call, and the price is stable enough not to need it (see
Cadence). It stays in the checked-in catalog.

For the *generator* it beats today's scraping of an 18.4 MB spot page and a 35.7 MB on-demand page
with regexes that break whenever Google restyles. It needs no API key — the keystore service
account authenticates with the `cloud-platform` scope.

**Validated end to end.** GCE prices are resource-based: `vcpus x Core rate + memory_gb x Ram
rate`, per family per region, spot carried as the `Preemptible` usage type. Recomputing
*on-demand* this way reproduces the checked-in catalog exactly for N2 and E2 — which confirms the
SKU selection, and so the spot figures from the same query.

**The discount is strongly region-dependent** — 40% off in us-east1 against 71-78% in
europe-west1, asia-northeast1 and southamerica-east1 — so store it per region, never as one rate.

Cost is quota, not money: no published per-call charge, 300 calls/min/project. The API must be
enabled on the project (`cloudbilling.googleapis.com`).

### 3. Azure — already paid for, currently discarded

The generator already queries the Azure Retail Prices API per SKU prefix and region, and that
response **already contains the Spot rows** — it explicitly skips anything whose SKU name contains
`Spot` or `Low Priority`. Measured for `Standard_L` in eastus: one call, ~400 ms, 147 items of
which 83 are Spot, unpaginated, showing a consistent 79-80% saving.

So Azure spot costs nothing extra to obtain: stop discarding those rows and record them beside
the on-demand price.

### 4. OCI — a flat discount, no API

OCI has no spot market. Preemptible instances are documented as a flat **50% off** the on-demand
rate, deliberately predictable rather than demand-driven, and the public OCI pricing API exposes
no preemptible products at all — 0 of 650 mention it. So derive the preemptible rate from the
on-demand price rather than fetching anything.

This is also the smallest fix available: `OCIPricing` currently returns zero unconditionally, so
OCI is unpriced today even on-demand, despite `oci.yaml` already carrying `price_per_hour` values.

### 5. Cadence — measured, not assumed

Spot prices on the catalogued clouds *do* move, so "it's administered" is not on its own a
reason to cache. What licenses caching is the observed rate, which was measured per cloud:

| Cloud | Provider's stated limit | Observed |
|---|---|---|
| GCE | "can change up to once every day" | **0 of 3,707** (type, region) prices changed over 8 days |
| Azure | not stated | effective dates land on month boundaries; ~24 of 58 SKUs changed over 3 months |

Two things follow, and both correct an earlier assumption in this plan.

**Google's guarantee is daily, not monthly.** The "changes at most monthly" figure came from a
blog post; the documentation says *"Spot prices can change up to once every day."* The
observed stability is what justifies caching here — not a guarantee that does not exist.

**Azure spot moves roughly five times as often as Azure on-demand.** Its spot rows carry a
median age of 114 days against 540 days for on-demand, so the two cannot share a refresh
rationale even though they share a source and a file.

Cadence, then:

- **AWS prices:** none — always live.
- **GCE and Azure prices:** monthly. That matches Azure's observed month-boundary rate and is
  comfortably inside the estimate's accuracy band for GCE.
- **OCI:** none — the 50% ratio is a constant, revisited only if Oracle changes the programme.
- **AWS interruption rates:** monthly, with the catalog (below).

A cadence nothing enforces is a wish, and today nothing regenerates the catalog on a schedule
at all. Automating that — and guarding it against a partial regeneration that silently shrinks
a catalog — is its own mini-plan, `2026-09-23-catalog-refresh-automation.md`.

### 6. Keep the interruption rate, drop the rest of the advisor

AWS publishes a Spot Instance Advisor dataset as public JSON, no credentials, one request (1.2 MB,
~1.8 s) for 34 regions and ~1,150 instance types. Its savings percentages are redundant against
live pricing, but its **interruption-rate bucket exists nowhere else**, and is what a later "is
spot safe for this run" decision needs — a run that is cheap but likely to be killed is not cheap.
Store that one field; monthly refresh suits it.

### 7. Out of scope

AZ-level price selection: the spread is reported, not modelled away. On-demand fallback visibility
is its own mini-plan. Interruption-rate *use* (gating or warning on a risky run) is a follow-up —
this plan only makes the data available.

## Files to Modify

- `sdcm/utils/cloud_catalog/pricing.py` -- batch the AWS spot lookup per region and memoise with a
  TTL; drop the GCE hardcoded spot table; stop discarding Azure Spot rows; give OCI its flat
  preemptible ratio and a real on-demand price
- `sdcm/utils/cloud_catalog/cost.py` -- **introduced by PR #15987** -- resolve AWS spot live and
  the rest from the catalog, gated on lifecycle and memoised per region; report spot estimate, AZ
  spread and savings; degrade to catalog pricing without credentials
- `sdcm/utils/cloud_catalog/catalog_generator.py` -- source GCE spot from the Billing Catalog API
  (keeping the scrape as fallback); keep Azure Spot rows; fetch AWS interruption buckets
- `sdcm/utils/cloud_catalog/instance_catalog.py` -- carry spot price and interruption bucket, with
  per-region accessors mirroring the on-demand one
- `data/instance_catalog/{gce,azure,aws,oci}.yaml` -- regenerated
- `unit_tests/unit/test_cost.py` -- **introduced by PR #15987** -- per-cloud spot resolution, call
  counts, unknown-stays-unknown, credential-less degradation
- `unit_tests/unit/test_catalog_generator.py` -- GCE SKU parsing, Azure Spot retention, advisor
  parsing, against recorded fixtures rather than live endpoints
- `utils/spot_pricing_report.py` -- **(new file, delivered with the implementation)** one entry
  point per backend, exercising the real code path and printing resolved rates, AZ spread,
  savings, **API calls made** and wall time. It shows the implementation works against live
  providers, re-measures when a provider changes something, and makes a regression obvious — a
  call count that grows with node count, or a lookup on an `on_demand` run, should be visible at a
  glance rather than inferred from a bill.

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_cost.py unit_tests/unit/test_catalog_generator.py -v`
- [ ] Pricing a whole run costs **one** AWS call per region, asserted by counting calls against a
      mocked client -- node count and instance-type count must not change it
- [ ] An `on_demand` run makes **zero** spot calls, asserted the same way
- [ ] Estimating a GCE, Azure or OCI config makes no network calls at all
- [ ] A spot estimate is materially below the on-demand estimate on every backend, and both are
      reported
- [ ] An instance type with no spot price reports unknown -- never zero, never a silent
      fall-through to the on-demand price
- [ ] With AWS credentials unavailable the estimate still returns, marked on-demand-based, and
      never raises
- [ ] GCE catalog generation via the Billing API reproduces the on-demand prices it replaces
- [ ] `utils/spot_pricing_report.py` runs green against live AWS, GCE and Azure, and its reported
      call counts match what the unit tests assert
- [ ] Cross-check one finished spot run against the cloud-monitor cost site for the same test id
- [ ] Full unit test suite passes: `uv run sct.py unit-tests`
- [ ] `uv run sct.py pre-commit` passes
