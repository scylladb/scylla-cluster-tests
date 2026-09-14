# Mini-Plan: Spot-Aware Cost Estimates for AWS and GCE

**Date:** 2026-09-14
**Owner:** fruch
**Estimated LOC:** ~450
**Related Jira:** [SCT-852](https://scylladb.atlassian.net/browse/SCT-852) (epic [SCT-851](https://scylladb.atlassian.net/browse/SCT-851))
**Depends on:** PR #15987 (pre-provisioning cost estimate) -- introduces `cost.py`, which this
plan modifies. This work cannot start until that merges.

## Problem

The pre-provisioning estimate always prices on-demand from the checked-in catalog. Most SCT
runs are spot, so every estimate overstates the real cost by roughly two to four times. An
estimate wrong in a consistent direction teaches people to ignore it — and the epic wants to
gate runs on this number, where a systematic overstatement blocks runs that are actually cheap.

Neither existing spot path can fix it. AWS queries live spot price history per region *and*
instance type, needing EC2 credentials in the target region. GCE has a hardcoded n1/n2 table
that reports unknown for every family SCT currently runs. The estimate runs on the Jenkins
builder before provisioning, is not guaranteed cloud credentials, and must not make
per-instance API calls.

### How volatile is spot, really

This decides whether a checked-in price is honest, so it was measured rather than assumed —
90 days of real history, five instance types SCT actually runs, us-east-1 and eu-west-1:

| Property | Measured |
|---|---|
| Price changes per AZ | 2-4 per day (roughly 200-330 over 90 days) |
| 90-day min-to-max spread | 21% - 132% |
| Mean error from a **7-day-old** cached value | 8% |
| Mean error from a **30-day-old** cached value | 12% (worst case seen: 40%) |
| Spread **across AZs at this instant** | 22% - 38% |

The last row is the one that matters: the irreducible spread between AZs *right now* is wider
than the error a month-old cache introduces. Spot moves constantly, but mostly as oscillation
around a slowly-drifting mean, and any single number per region-and-type is approximate no
matter how fresh. Caching is therefore defensible for an *estimate* — but only with a stated
accuracy and a refresh cadence, not as a precise quote.

## Approach

Spot prices are resolved **offline by the catalog generator** into the checked-in catalog, so
the estimate stays a pure catalog lookup with zero API calls — the contract the on-demand
estimate already honours.

End to end: `sct sizing update-catalog` → per-cloud spot source → catalog fields →
catalog-only rate lookup → estimate, with the on-demand figure carried alongside as a ceiling.

### 1. AWS — cache the savings rate, not the price

AWS publishes a Spot Instance Advisor dataset as public JSON needing no credentials. One
request returns, for 34 regions and roughly 1,150 Linux instance types, a savings percentage
against on-demand plus an interruption-rate bucket.

Store the **savings percentage**, not a derived absolute price. The percentage is a long-run
aggregate that ages far better than a spot quote, which is exactly the property a checked-in
value needs; the lookup applies it to the region's on-demand price and stays catalog-only.

Store the **interruption-rate bucket** too — free in the same payload, and the input a later
"is spot safe for this run" decision needs. A run that is cheap but likely to be killed is not
cheap.

**Prerequisite — regenerate the AWS catalog with per-region on-demand prices.** The AWS catalog
holds one price per instance type with no region dimension, unlike GCE. Applying a
region-specific savings percentage to a single-region on-demand price mixes two bases.
`catalog_generator.py:generate_aws_catalog` already accepts a region list, so this is a data
refresh, not new code. It also corrects a pre-existing inaccuracy in the on-demand estimate.

### 2. GCE — one more page on a scrape that already runs

The generator already scrapes Google's general-purpose and storage-optimized pricing pages.
Spot prices are not on those; they live on the dedicated Spot VMs pricing page, which carries
per-machine-type spot prices for the families SCT uses, z3 included. One extra request at
generation time, none at estimate time, reusing the existing parsing.

GCE spot prices are administratively set rather than auction-driven, so absolute per-region
values are appropriate here and age well — the opposite of the AWS case above.

### 3. Refresh cadence is required, not optional

The measurements above only hold if the catalog is actually refreshed. Nothing regenerates it
on a schedule today. A monthly refresh keeps mean error near 12%; letting it drift for a
quarter does not. This plan does not build the automation, but it must not ship without an
agreed owner and cadence — treating that as someone else's problem is what makes the numbers
dishonest.

### 4. Fallback to on-demand must be visible in both the estimate and the bill

When spot capacity is short, SCT falls back to on-demand, so a spot run can silently cost
several times its estimate. That has to be surfaced, not buried:

- **Estimate:** when the resolved config enables fallback, warn and report the on-demand
  ceiling next to the spot figure. The gate should read the ceiling — underestimating is the
  dangerous direction.
- **Billing:** report each node's *actual* lifecycle, which SCT already knows per node, and
  summarise how many nodes ended up on-demand. A run that quietly fell back is the single most
  useful thing this feature can tell someone.

**Finding that needs a decision first.** The intent is that fallback is on only for artifact
tests, and 20 artifact test-cases do set it explicitly. But `defaults/aws_config.yaml` and
`defaults/azure_config.yaml` set it `true` as the backend default, and the Jenkins parameter
defaults to empty so it never overrides. A plain AWS longevity config therefore resolves to
`instance_provision: spot` with `instance_provision_fallback_on_demand: true`. As configured,
the warning would fire on every AWS run.

Either the default flips to `false` — the artifact test-cases already set it themselves, so
they keep working — or the warning is noise from day one. Recommend flipping the default as a
separate, self-contained change, and treating this plan's warning as the thing that would have
caught it.

### 5. Out of scope

Azure and OCI spot. AZ-level pricing: the estimate does not know which AZ it will land in, and
the spread is documented above rather than modelled away.

## Files to Modify

- `sdcm/utils/cloud_catalog/catalog_generator.py` -- fetch the AWS advisor dataset; scrape the
  GCE spot pricing page; emit the new catalog fields
- `sdcm/utils/cloud_catalog/instance_catalog.py` -- carry spot savings rate, spot price and
  interruption bucket, with per-region accessors mirroring the on-demand one
- `sdcm/utils/cloud_catalog/cost.py` -- **introduced by PR #15987** -- resolve spot rates from
  the catalog; derive lifecycle from config; report spot estimate plus on-demand ceiling and
  the fallback warning
- `sdcm/utils/cloud_catalog/pricing.py` -- drop the GCE hardcoded spot table and the AWS live
  spot-history path
- `data/instance_catalog/aws.yaml` -- regenerated per-region, with spot savings rates
- `data/instance_catalog/gce.yaml` -- regenerated with spot prices
- `unit_tests/unit/test_cost.py` -- **introduced by PR #15987** -- spot resolution,
  unknown-stays-unknown, ceiling and fallback warning
- `unit_tests/unit/test_catalog_generator.py` -- advisor parsing and GCE spot scrape, against
  recorded fixtures rather than live endpoints

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_cost.py unit_tests/unit/test_catalog_generator.py -v`
- [ ] Estimating a spot longevity config makes no network and no cloud API calls -- the property
      the pipeline stage depends on
- [ ] A spot estimate is materially below the on-demand estimate for the same config, and both
      are reported
- [ ] A config with fallback enabled produces a visible warning and shows the on-demand ceiling
- [ ] An instance type with no spot entry reports unknown -- never zero, never a silent
      fall-through to the on-demand price
- [ ] Spot figures for AWS and GCE types SCT actually runs land within the documented accuracy
      band against the provider consoles; cross-check one finished spot run against the
      cloud-monitor cost site
- [ ] Catalog regeneration is reproducible: a second `sct sizing update-catalog` run with no
      upstream price change produces no diff
- [ ] Full unit test suite passes: `uv run sct.py unit-tests`
- [ ] `uv run sct.py pre-commit` passes
