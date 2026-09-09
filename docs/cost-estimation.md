# Test cost estimation

SCT can tell you roughly what a test run will cost **before** it provisions anything.
The `Estimate Test Cost` pipeline stage prints it, and you can ask for it yourself:

```bash
hydra estimate-cost -b aws test-cases/longevity/longevity-10gb-3h.yaml
```

## Reading the output

```
Estimated cost of this run: $21.02 (on-demand rates)

  role       nodes  instance type              $/hour   hours      cost
  db             6  i7i.2xlarge                0.7550    4.25    $19.25
  loader         2  t3.xlarge                  0.1664    4.25     $1.41
  monitor        1  t3.large                   0.0832    4.25     $0.35
```

The headline is the cost of the **whole run**: every node, for the full configured test
duration. Each row is one cluster role — node count times hourly rate times duration.

`hours` is `test_duration` from the configuration. A run that finishes early costs less; one
that overruns costs more.

The instance types shown are the *resolved* ones. If a config uses sizing constraints rather
than literal types, what you see here is what those constraints resolved to.

## What it does not include

Instance hours only. Not counted: storage and volumes, network egress, S3, managed-service
overhead (EKS/GKE control planes), or anything the test creates itself.

Treat it as an order-of-magnitude figure for deciding whether a run is reasonable — not as a
bill.

## "PARTIAL" and "unknown"

```
Estimated cost of this run: $1.77 (on-demand rates) -- PARTIAL, no price for: db
  db             6  m5.24xlarge                     -    4.25   unknown
```

A role shows `unknown` when there is no price for its instance type. The total then covers
only the roles that *could* be priced, and is marked `PARTIAL` — it is a floor, not the
answer. This is deliberate: a missing price is never reported as zero, because a silent `0`
reads as "free" and is worse than admitting ignorance.

Causes:

- **OCI** has no usable public pricing, so OCI runs are always unknown.
- **Instance types outside the catalog.** The catalog covers what SCT actually uses; an
  unusual type will not be there.
- **Backends with no cloud instances** — `docker`, `k8s-local-kind*`, `baremetal` — resolve no
  priceable roles at all.

## Where the prices come from

The checked-in instance catalog under `data/instance_catalog/`, refreshed on a schedule by
`sct.py sizing update-catalog`.

The estimate deliberately makes **no cloud API calls**. It runs on a Jenkins builder before
anything exists, and a number telling you what a run will cost should not depend on a cloud
pricing endpoint being reachable. A type missing from the catalog is reported unknown rather
than looked up live.

Prices are list prices and do not reflect any negotiated discount.

## Spot runs

Estimates are currently **on-demand rates only**, even when the run is configured for spot.
Spot typically costs 35-60% of on-demand, so for a spot run the figure is an over-estimate —
useful as a ceiling, misleading as a prediction.

Spot rates are not in the catalog yet. Adding them is tracked separately.

## Accuracy

Expect the same order of magnitude as the cloud-monitor cost site, not an exact match. That
site rounds up to whole hours and prices spot differently; this estimate uses fractional hours
and list on-demand rates.
