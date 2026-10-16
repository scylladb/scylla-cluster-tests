# Mini-Plan: Automate the Instance Catalog Refresh

**Date:** 2026-09-23
**Owner:** fruch
**Estimated LOC:** ~200
**Related Jira:** [SCT-852](https://scylladb.atlassian.net/browse/SCT-852) (epic [SCT-851](https://scylladb.atlassian.net/browse/SCT-851))
**Order:** should land after the spot-pricing PR, so a refresh regenerates spot rates too

## Problem

`data/instance_catalog/` holds the prices every cost estimate is built on, and **nothing
refreshes it on a schedule**. It moves only when someone remembers to run
`sct sizing update-catalog` by hand, which in practice means when something else goes wrong.

Prices do move. Measured on the catalogued clouds: Azure spot rows carry effective dates on
month boundaries, with roughly a third of SKUs changing over three months; GCE spot showed no
change across 3,707 (type, region) prices over eight days, but Google's documentation permits
a change *every day*. On-demand drifts far more slowly — Azure's median on-demand row is 540
days old — so spot is what makes a stale catalog wrong.

Precision is not the goal: this is an estimate, and a two-month refresh is accepted as
accurate enough. What matters is that the refresh happens without anyone having to remember,
and that a bad refresh cannot slip through.

## Approach

### 1. A scheduled job that opens a pull request

A job on a two-month schedule runs `sct sizing update-catalog --cloud all`, and if anything
changed, pushes a branch and opens a PR. A human reviews and merges — the only manual step,
and the one worth keeping, because a price diff is exactly the kind of change that should be
looked at before it lands.

The PR body should say what moved: how many instance types changed per cloud, the largest
swings, and anything that disappeared. A diff of several thousand YAML lines is unreviewable
on its own, and an unreviewable PR gets rubber-stamped, which defeats the point of the
review step.

Manual triggering stays available for when someone needs a refresh sooner.

### 2. Guard against a partial regeneration — the real risk

`update_catalog` skips the file write only when a generator returns *nothing*. The generators
swallow errors in eight places and return whatever they managed to collect, so a rate-limited
pricing API or a restyled pricing page yields a **shrunken** catalog rather than an empty one,
and that writes normally.

Unattended, this is how the catalog quietly loses instance types: a scheduled job proposes a
diff that deletes half of AWS, and it reads like any other price update. So the refresh must
refuse to write a catalog that lost a material share of its instance types against the
committed baseline (159 AWS, 89 GCE, 121 Azure, 54 OCI), and fail loudly instead. Genuine
removals — a retired instance family — then need an explicit override, which is the right
amount of friction for deleting pricing data.

### 3. Where it runs is decided by one credential

The generators need less than it looks. Azure and OCI pricing are public HTTP APIs needing no
credentials at all. GCE needs a GCP service account, but reads it through the keystore, which
is itself unlocked by AWS. AWS needs its own credentials for the pricing and EC2 APIs. So
**one AWS credential unlocks all four catalogs**.

That makes the hosting decision concrete rather than architectural:

- **Jenkins** already has AWS credentials and runs hydra, so it needs nothing new for the
  refresh — but it would need a GitHub token to open the PR.
- **GitHub Actions** already has the tooling to open a PR, and the repo's other automation
  lives there — but no workflow currently holds cloud credentials, so this would be the first,
  and that is a security decision for whoever owns the secrets.

**Needs a decision before implementation.** The recommendation is GitHub Actions, because
opening a PR is the part that has to work reliably and the repo's other bots already live
there, but it depends on whether an AWS credential can be granted to a workflow. If that is
unwelcome, Jenkins is the fallback and the PR is opened with a token instead.

### 4. Out of scope

Changing what the catalog contains, or how any price is derived — this plan only changes how
often the existing generator runs and who notices. Alerting on price changes is a separate
idea and probably a worse one than reviewing the PR.

## Files to Modify

- `.github/workflows/refresh-instance-catalog.yaml` -- **(new file)** scheduled and manually
  triggerable; runs the refresh, opens a PR only when something changed, and summarises what
  moved rather than leaving a reviewer to read thousands of YAML lines
- `sdcm/utils/cloud_catalog/catalog_generator.py` -- refuse to write a catalog that lost a
  material share of its instance types, with an explicit override for genuine removals
- `sct_sizing.py` -- surface the override and a dry-run that reports what would change without
  writing
- `unit_tests/unit/test_catalog_generator.py` -- the shrink guard trips on a truncated
  generator result, passes on a normal one, and is bypassable only deliberately

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_catalog_generator.py -v`
- [ ] A generator returning a truncated list fails the refresh instead of writing a shrunken
      catalog; the override lets a genuine removal through
- [ ] `sct sizing update-catalog --dry-run` reports what would change and writes nothing
- [ ] A manual run of the workflow against an unchanged catalog opens **no** PR
- [ ] A manual run with a seeded price change opens a PR whose body names the affected clouds
      and the largest movers
- [ ] The opened PR passes CI on its own, so a refresh is never blocked on an unrelated failure
- [ ] Re-running the refresh with no upstream change produces no diff -- the generator is
      reproducible, so PRs only appear when prices actually moved
- [ ] `uv run sct.py pre-commit` passes
