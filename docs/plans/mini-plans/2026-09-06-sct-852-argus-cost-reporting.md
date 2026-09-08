# Mini-Plan: Report Test Cost to Argus (SCT-852)

**Date:** 2026-09-06 (revised 2026-09-27)
**Owner:** fruch
**Estimated LOC:** ~250
**Related Jira:** [SCT-852](https://scylladb.atlassian.net/browse/SCT-852) (epic [SCT-851](https://scylladb.atlassian.net/browse/SCT-851))
**Argus counterpart:** [ARGUS-205](https://scylladb.atlassian.net/browse/ARGUS-205), merged in [argus#1101](https://github.com/scylladb/argus/pull/1101)
**Depends on:** #16136 (the pricing and estimate this reports), and an Argus client release

## Problem

Engineers cannot see what a run costs. The data exists only on the cloud-monitor cost site,
which is awkward to reach and effectively nobody checks, so expensive mistakes go unnoticed
until someone reviews the bill.

#16136 computes the numbers — per-instance rates on all four clouds, and a pre-provisioning
estimate — but nothing sends them anywhere. They appear in a build log and are gone. Argus is
where people already look, so that is where they belong.

The contract is that **all cost arithmetic happens in SCT**. Argus stores what it is told and
sums it; it has no pricing catalog and derives nothing.

Scope is what #16136 already prices: instance-hours, meaning hourly rate times running time.
Network egress, storage and managed-service overhead are out, as is reconciliation against
real billed cost.

## Approach

### What Argus actually accepts

Worth stating plainly, because an earlier draft of this plan assumed a different and more
ambitious API than the one that shipped. The merged client offers exactly two calls:

```python
set_estimated_cost(run_id, value)          # run-level, USD, replaces on repeat
submit_cost_items(run_id, items)           # per-resource, summed into the run's actual cost

@dataclass
class CostItem:
    name: str            # the resource name SCT already registers
    category: str        # db_node, loader, monitor, sct_runner, ...
    cost: float          # USD, final
    pricing_tier: str | None = None   # "spot" / "on-demand"
    leaked: bool = False              # cleaned up by the reaper, not by the test
```

Three consequences follow, and they shape the rest of this plan:

- **There is no live running cost.** Argus stores no hourly rate, so it cannot extrapolate a
  cost for a still-running node. An earlier draft proposed sending the rate at creation for
  exactly that; the merged API has no field for it. Cost appears when a resource's price is
  final — at teardown. Getting a live figure would need a new Argus field and is not attempted
  here.
- **The runner is not special.** It is a `CostItem` with its own category, not a field on the
  runner record. That removes the earlier blocker about `set_sct_runner` carrying no region or
  instance type: pricing happens in SCT, which has both.
- **Unknown means send nothing.** Argus stores a zero as a real figure, so a resource with no
  price is simply omitted. This matches what #16136 already does internally, where a missing
  price stays `None` rather than collapsing to zero.

Items are keyed by name and replace on repeat, so sending is idempotent and a retry is safe.

### 1. Send the estimate before provisioning

The run-level estimate is already computed at run start. Send it there, with
`set_estimated_cost`. It is the figure the future approval gate reads, and the one the details
page compares actual against.

### 2. Send each resource's cost when its price is final

At node teardown, where running time is settled and the lifecycle is known. `pricing_tier`
carries what the node *actually got* rather than what was asked for, which is the only way a
spot run that fell back to on-demand is visible.

The rate must be captured when the node is **created**, not read at teardown: backend
`destroy()` terminates the cloud instance before the Argus hook runs, so by then there is
nothing left to ask.

### 3. Reaped resources report too, with `leaked` set

Resources cleaned up by the reaper rather than by the test are the expensive surprises, so
they must not be the ones showing blank — and `leaked` exists precisely to mark them. These
paths hold the live cloud instance, so type, region and launch time are all available.

This is load-bearing rather than a nicety: Argus derives nothing, so if SCT does not send the
number, nothing else produces it.

### 4. The SCT runner reports as its own item

It is a real instance that outlives the test it serves, and its cost is only final once it is
reaped. Report it from the cleanup path against the right run id, in its own category so it is
legible rather than folded into a cluster role. Price it over its own lifetime, not the test
duration — its budget covers startup, the test, teardown, log collection and cleanup.

### 5. Nothing here may fail a run

A cost report is advisory. Every call is best-effort: a failure is logged and the run carries
on, exactly as the existing Argus resource calls behave.

### Dependencies and sequencing

The Argus side is merged, but **no client release carries it** — the latest is `v0.16.3` from
August, and SCT vendors that. The vendored client is regenerated wholesale by
`utils/update_argus_client.py`, so a hand-edit would be reverted on the next refresh.

So: Argus cuts a release → a separate PR re-vendors the client → this work lands on top.
Development can happen against a locally patched client, but the merge must carry a genuine
re-vendor.

### Already landed, not repeated here

#16136 carries the pricing and the estimate this plan reports: per-cloud spot and on-demand
rates, the unknown-is-never-zero rule, the pre-provisioning estimate with its on-demand
ceiling, and the estimate stage in nine pipelines. The two call sites below are already marked
there with `TODO(SCT-852)`.

An earlier draft of this plan argued for a scheduled rate refresh instead of live pricing
calls, on the grounds that a per-node call at teardown would be a source of flakiness. That
objection was right about per-node calls and wrong about the conclusion: AWS spot pricing takes
a *list* of instance types, so a whole run costs one call per region regardless of node count.
#16136 prices AWS spot live on that basis and everything else from the catalog. Refreshing the
catalog on a schedule is its own mini-plan (#16138).

## Files to Modify

- `argus/client/` -- re-vendored once a client release carries the cost API
- `sdcm/cluster.py` -- capture the rate when a node is registered; submit its cost item at
  teardown, with the lifecycle it actually got
- `sdcm/tester.py` -- send the run-level estimate at run start
- `sdcm/utils/argus.py` -- accept a cost from the cleanup path and submit it with `leaked` set
- `sdcm/utils/resources_cleanup.py`, `utils/cloud_cleanup/__init__.py` -- compute cost from the
  live cloud instance and pass it through
- `sdcm/sct_runner.py` -- report the runner's cost against its run when it is reaped
- `unit_tests/unit/test_cost_reporting.py` -- **(new file)** what gets sent and when

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_cost_reporting.py -v`
- [ ] A resource with no price sends **no item**, rather than an item costing zero
- [ ] `pricing_tier` reports the lifecycle the node actually got -- a spot node that fell back
      to on-demand reports on-demand
- [ ] A resource reaped by cleanup reports its cost with `leaked` set
- [ ] The runner's cost is attributed to the right run once it is reaped, in its own category
- [ ] Re-sending an item replaces it rather than double-counting, so a retry is safe
- [ ] An Argus outage leaves the run unaffected -- reporting failures are logged, never raised
- [ ] A finished run shows per-resource costs and a run total in Argus, with the estimate
      alongside for comparison
- [ ] Run totals are the same order of magnitude as the cloud-monitor cost site (exact
      agreement is not expected: the two round differently and price spot differently)
- [ ] `uv run sct.py pre-commit` passes
