# Mini-Plan: Report Test Cost to Argus (SCT-852)

**Date:** 2026-09-06 (revised 2026-10-08)
**Owner:** fruch
**Estimated LOC:** ~250
**Related Jira:** [SCT-852](https://scylladb.atlassian.net/browse/SCT-852) (epic [SCT-851](https://scylladb.atlassian.net/browse/SCT-851))
**Argus counterpart:** [ARGUS-205](https://scylladb.atlassian.net/browse/ARGUS-205), merged in [argus#1101](https://github.com/scylladb/argus/pull/1101)
**Status:** implemented in #16136, on top of the argus-alm 0.16.5 client (#16301)

## Problem

Engineers cannot see what a run costs. The data exists only on the cloud-monitor cost site,
which is awkward to reach and effectively nobody checks, so expensive mistakes go unnoticed
until someone reviews the bill.

#16136 computes the numbers: per-instance rates on all four clouds and a pre-provisioning
estimate. Argus is where people already look, so that is where they belong.

All the cost arithmetic happens in SCT. Argus stores what it is told, sums the per-resource
items, and has no pricing of its own.

Scope is instance-hours only: hourly rate times running time. Network egress, storage,
managed-service overhead, and reconciliation against real billed cost are out.

## Approach

### What Argus accepts

The client offers two calls. `set_estimated_cost` stores one run-level estimate and
replaces it on repeat. `submit_cost_items` takes per-resource items, each with a name, a
category, a final cost, a pricing tier and a `leaked` flag. Items are keyed by name, so
re-sending one replaces it and a retry cannot double-count.

Argus has no field for an hourly rate, so it cannot show a live cost for a resource that is
still running. A resource's cost appears once it is final, when the resource ends.

**Argus falls back to the estimate.** When a run finishes, Argus recomputes its actual cost
with the estimate as a fallback:

- A run with no cost items shows its **estimate as its actual cost**.
- A run with some items shows their sum, with **nothing marking it as partial** when some
  resources sent no item.

SCT reports a resource with no known price as no item at all, because Argus stores a zero as
a real figure. So a run whose resources could not be priced shows the estimate, and a run
priced only in part shows a total that is too low. Showing either case honestly needs Argus
to store what was left unpriced; that is
[ARGUS-253](https://scylladb.atlassian.net/browse/ARGUS-253), and sending it from SCT is
[SCT-1180](https://scylladb.atlassian.net/browse/SCT-1180).

### 1. Send the estimate before provisioning

The "Estimate Test Cost" pipeline stage runs `sct.py estimate-cost --report-to-argus` before
"Create SCT Runner", so nothing is provisioned yet. The pipeline exports `SCT_TEST_ID`, so the
command knows the run's id.

`ClusterTester.init_argus_run` sends the estimate again, when the test starts. This covers the
pipelines that create the Argus run only after the estimate stage, and local runs, which have
no stage. A repeat replaces the stored value.

A partial estimate (one that could not price every role) is not sent, because Argus would store
it as the whole estimate. SCT-1180 changes that once ARGUS-253 lands.

### 2. Price each instance once, when it comes up

When a node comes up, SCT prices it at the lifecycle it **actually got**: a spot request that
fell back to on-demand is priced on-demand. It writes the price on the cloud instance as two
tags, the hourly price in micro-USD and the pricing tier. The same pair is valid on AWS, GCE,
Azure and OCI.

The price has to be on the instance because the cleanup paths run in other processes, and they
only see the cloud instance.

### 3. Report each resource's cost when it ends

The flow: `node comes up → priced and tagged → node ends → hourly price × running time → item sent`.

`leaked` means the resource outlived the test and only the scheduled sweep caught it. Each
caller sets the flag; the shared reporting function never decides it:

| Path | Price from | Running time | `leaked` |
|---|---|---|---|
| SCT terminates a node during the run (nemesis, resize) | the rate kept in memory | since the node was created | false |
| The job's `clean-resources --post-behavior` stage | the instance's tags | since the cloud launched it | false |
| A manual `clean-resources` | the instance's tags | since the cloud launched it | false |
| The scheduled sweep (`hydra-cleanup-cloud`) | the instance's tags | since the cloud launched it | **true** |

The job's cleanup stage is the normal end of a run's nodes: with the default
`execute_post_behavior: false`, it is where every CI run's nodes are terminated, including
nodes that `post_behavior` `keep` or `keep-on-failure` held on purpose. Those are not leaks, so
only the sweep reports `leaked`.

AWS and OCI keep listing an instance for a while after it is terminated. Cleanup skips the cost
report for those, so the exact figure sent when SCT ended the node stands.

### 4. The SCT runner

The runner is a real instance that outlives the test it serves, and no estimate or cost report
includes it yet. That is its own mini-plan, `2026-10-04-price-the-sct-runner.md`.

### 5. Nothing here may fail a run

Every call is best-effort. A pricing, tagging or reporting failure is logged and the run carries
on, and cleanup's reporting never stops the deletion it runs inside.

## Files to Modify

- `sct.py` -- `estimate-cost --report-to-argus` sends the estimate before provisioning
- `vars/estimateTestCost.groovy` -- the pipeline stage that runs it
- `sdcm/tester.py` -- re-sends the estimate when the test starts
- `sdcm/utils/cost_reporting.py` -- **(new file)** builds and sends the estimate and cost items
- `sdcm/cluster.py` and the four cloud node classes -- price and tag a node when it comes up;
  report its cost when SCT terminates it
- `sdcm/utils/resources_cleanup.py` -- the job's cleanup stage reports from the tags, not leaked
- `utils/cloud_cleanup/` -- the scheduled sweep reports from the tags, marked leaked
- `sdcm/provision/oci/constants.py` -- the two tag keys OCI needs defined
- `unit_tests/unit/test_cost_reporting.py` -- **(new file)** what gets sent, and when nothing is

## Verification

- [x] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_cost_reporting.py -v`
- [x] A resource with no price sends **no item**, rather than an item costing zero
- [x] `pricing_tier` reports the lifecycle the node actually got
- [x] The job's cleanup stage reports `leaked=false`; only the scheduled sweep reports `leaked=true`
- [x] Cleanup does not re-price an instance SCT already terminated
- [x] Re-sending an item replaces it rather than double-counting
- [x] An Argus outage leaves the run unaffected: failures are logged, never raised
- [x] A PR provision run on AWS, GCE, Azure and OCI shows the estimate and per-node costs in
      Argus, with nothing marked leaked
- [ ] A run whose resources could not be priced shows its estimate as its actual cost (the
      Argus fallback above); confirm on a backend with no price, e.g. a k8s run
- [ ] A resource reaped by the scheduled sweep shows `leaked=true` (the sweep runs master code,
      so this is checked on the first sweep after #16136 merged)
- [ ] Run totals are the same order of magnitude as the cloud-monitor cost site
- [x] `uv run sct.py pre-commit` passes
