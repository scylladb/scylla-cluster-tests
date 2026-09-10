---
name: capacity-failure-analysis
description: >-
  Analyze AWS capacity and provisioning failures in SCT test runs using Argus and
  Jenkins. Use when asked how many runs failed with CapacityReservationError,
  ProvisioningCapacityExhausted or InsufficientInstanceCapacity, when counting
  provisioning failures per week, when grouping capacity failures by AWS region,
  availability zone, instance type or job, when deciding which region to move a
  perf job to, or when investigating why a job cannot get i8g or i4i capacity.
  Triggers include "capacity reservation failures", "how many runs failed to
  provision", "capacity failures per week", "group failures by region", "which
  region has capacity", "why can't we provision". Covers Argus event signatures,
  the region-provenance problem (Argus records no region for these runs), Jenkins
  build-parameter lookup, and generating an HTML report.
---

# Capacity Failure Analysis

Measure and explain AWS capacity failures that kill SCT runs before provisioning finishes.

## Essential Principles

### Rank Regions by Rate, Never by Count

**A region's failure count is dominated by how many runs it hosts; only the rate says whether it is unhealthy.**

In a real 12-week sweep `us-east-1` had the second-highest raw failure count (25) purely because it carried
256 of 471 runs — its rate was 9.8%, the *lowest* of any affected region, while `eu-north-1` failed 40.7% of
27 runs. Reporting counts alone points remediation at the wrong region. Always print runs, failures and rate
together, and sort the remediation discussion by rate.

### State Region Provenance Every Time

**Argus records no region for these runs, so every region figure is second-hand — say where it came from.**

The test aborts before a single instance is allocated, so `region_name` is empty on 100% of capacity-failed
runs. Region has to be recovered from the Jenkins `region` build parameter, and Jenkins rotates builds out of
history within weeks. A region breakdown that does not disclose how many values were measured versus inferred
invites decisions built on guesses. See [region-resolution.md](references/region-resolution.md).

### Count Builds Alongside Runs

**SCT retries provisioning, so one blocked Jenkins build can appear as several failed Argus runs.**

Build 79 of `latency-650gb-with-nemesis-i8g-tablets` produced two runs, both capacity failures. "77 failed
runs" and "50 blocked builds" describe the same window; the first measures wasted attempts, the second
measures how often a scheduled job actually did not deliver a result. Quoting only runs overstates impact.

### Separate the Two CapacityReservationError Raise Sites

**The same exception type means two different problems with two different fixes.**

`capacity_reservation.py:201` means no AZ in the region had the requested instances — a real capacity
shortage, fixed by changing region or instance type. `capacity_reservation.py:156` means the placement group
was missing or unavailable — a configuration or cleanup bug, not a capacity shortage. Lumping them together
sends people hunting for capacity that was never the problem.

### A Retried Failure Is Not a Failed Run

**Capacity errors that the AZ or region fallback recovered from must not be counted as failures.**

`sdcm/sct_provision/aws/layout.py:137` catches `CapacityReservationError` and `ProvisioningCapacityExhausted`
and tries the next AZ, then the next region. Only a run whose *final* status is `test_error` with a CRITICAL
event was actually killed. Match on severity and final status, not on the presence of the string anywhere in
the log, or the numbers will be inflated by failures the framework handled correctly.

## When to Use

- Counting how many runs or builds failed with a capacity or provisioning error over a time window
- Breaking capacity failures down per week, per AWS region, per job or per instance family
- Deciding which region or instance type a perf job should move to
- Investigating why a specific job repeatedly cannot get capacity
- Producing an HTML or email report of provisioning-failure statistics for stakeholders
- Checking whether an AZ or region fallback change actually reduced failures

## When NOT to Use

- Comparing performance results between runs — use `test-run-comparison-reports`
- Producing the regular weekly perf status email — use `perf-weekly-status-report`
- Debugging a single run's non-capacity failure (nemesis error, latency regression, test assertion)
- Changing the capacity reservation code itself — this skill measures behaviour, it does not modify `sdcm/provision/aws/`
- Any non-AWS backend; capacity reservation is AWS-only

## Error Signatures

Detection is on Argus events, not raw logs. Full detail in [error-signatures.md](references/error-signatures.md).

| Signature | Raised at | Fatal? | Means |
|-----------|-----------|--------|-------|
| `CapacityReservationError` "in any availability zone" | `sdcm/provision/aws/capacity_reservation.py:201` | Yes | No AZ in the region had the instances |
| `CapacityReservationError` "placement group" | `sdcm/provision/aws/capacity_reservation.py:156` | Yes | Placement group missing or unavailable |
| `ProvisioningCapacityExhausted` | `sdcm/sct_provision/aws/layout.py:137` | Usually no | Caught by AZ/region fallback |
| `InsufficientInstanceCapacity` | AWS API, surfaced by boto | Usually no | Raw EC2 capacity error |

A fatal capacity failure looks like this in Argus: run `status == "test_error"` plus a CRITICAL
`TestFrameworkEvent` reading `Failed to provision aws resources: CapacityReservationError: ...`.

## Helper Script

`capacity_failure_stats.py` does collection, classification, region resolution and aggregation. Run it from
the repository root so `sdcm` imports resolve:

```bash
PYTHONPATH=. .venv/bin/python skills/capacity-failure-analysis/capacity_failure_stats.py \
    --registry tests.tsv --weeks 12 --json-out capacity.json
```

| Flag | Purpose |
|------|---------|
| `--test-id UUID` | One Argus test UUID; repeatable |
| `--registry FILE` | TSV of `name<TAB>test_id[<TAB>category]` — preferred for multi-job sweeps |
| `--weeks N` | Look-back window, default 12 |
| `--no-jenkins` | Skip Jenkins lookup; regions come from Argus and inference only (much less accurate) |
| `--json-out FILE` | Full per-run dataset, the input for the HTML report |

The Argus CLI cannot enumerate the tests in a group, so test UUIDs must be supplied. The 20 enterprise perf
tests are tabulated in `skills/perf-weekly-status-report/SKILL.md` under "Test Registry".

## Choosing a Remediation

| Finding | Action |
|---------|--------|
| One region's rate far above the others, same instance type | Move the job to a region with a low rate and enough run history to trust |
| All regions bad for one instance family (e.g. every i8g job) | Instance-family shortage — raise with AWS or switch family |
| Failures cluster in specific weeks across every region | External AWS capacity event — check dates before changing config |
| Placement-group variant dominates | Configuration or leaked-resource bug, not capacity — inspect cleanup |
| High run count but low rate | Healthy; do not "fix" it |

## Reference Index

| File | Content |
|------|---------|
| [error-signatures.md](references/error-signatures.md) | Every capacity signature, its raise site, fallback behaviour, and how it appears in Argus |
| [region-resolution.md](references/region-resolution.md) | Why Argus has no region for these runs, the Jenkins lookup, and the inference rules |
| [report-format.md](references/report-format.md) | Structure, palette and caveat conventions for the HTML report |

| Workflow | Purpose |
|----------|---------|
| [analyze-capacity-failures.md](workflows/analyze-capacity-failures.md) | 5-phase process from question to published report |

## Success Criteria

- [ ] Every failure count is paired with the run total and the rate
- [ ] Builds hit is reported alongside failed runs
- [ ] Region provenance is stated: how many measured from Jenkins, how many inferred, with inference accuracy
- [ ] Signature variants are distinguished, not merged into one "capacity" bucket
- [ ] Only runs whose final status is `test_error` with a CRITICAL event are counted
- [ ] Regions with too few runs to be meaningful are flagged rather than ranked
- [ ] Remediation advice names a specific region or instance type, with the rate that justifies it
