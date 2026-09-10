# Mini-Plan: Report Test Cost to Argus (SCT-852)

**Date:** 2026-09-06
**Owner:** fruch
**Estimated LOC:** ~600
**Related Jira:** [SCT-852](https://scylladb.atlassian.net/browse/SCT-852) (epic [SCT-851](https://scylladb.atlassian.net/browse/SCT-851))
**Argus counterpart:** [ARGUS-205](https://scylladb.atlassian.net/browse/ARGUS-205), implemented in [argus#1071](https://github.com/scylladb/argus/pull/1071)

## Problem

Engineers running SCT tests cannot see what a run costs. The data exists only on the
cloud-monitor cost site, which is awkward to reach and effectively nobody checks, so
expensive mistakes go unnoticed until someone reviews the bill.

Argus is where people already look, so that is where the number belongs. An estimate from
instance types and runtime is good enough — the goal is awareness, not accounting.

The contract with Argus is that **all cost arithmetic happens in SCT**. Argus stores what it
is told, sums it for display, and never grows a pricing catalog of its own.

Scope is **instance-hours only**: hourly rate multiplied by running time, per instance.
Network egress, storage, S3 and managed-service overhead are out of scope, as is real billed
cost and a per-resource breakdown tab. xcloud is a follow-up.

The approval gate that this estimate eventually feeds — blocking an expensive run until a
team lead approves it — is **a separate mini-plan and PR**. It is mentioned here only because
it constrains one thing: the estimate has to be produced at the point where a gate could act
on it, before anything is provisioned.

## Approach

**1. Every instance reports what it costs.** When SCT registers a node with Argus it also
sends the node's hourly rate and whether it is a spot instance. When the node is torn down it
sends the final cost. Sending the rate up front is what lets Argus show a *live* cost while a
test is still running — the case that actually drives cost awareness. Reporting only at
teardown would show nothing until a node dies.

The rate and the lifecycle are captured when the node is created, not when it is destroyed:
by teardown the cloud instance is already gone, and the goal is that teardown does no lookup
work at all.

**2. Unknown is not free.** Pricing is unavailable for whole classes of resource — OCI has no
usable public rates, newer GCE machine families have no spot entry, and any instance type
missing from the catalog has no price. A missing price must be reported as *unknown* and
never as a zero cost. A run containing unpriced resources shows a partial total rather than a
confidently wrong one.

**3. Prices come from a cached catalog, not from live cloud APIs.** SCT already ships an
instance catalog that answers on-demand lookups offline; that stays the primary source. Live
pricing APIs are the fallback only, and per review feedback the intent is to **refresh rates
on a schedule and read the cached values during a run** rather than querying cloud pricing
APIs repeatedly. Spot prices no longer move fast enough to justify a live lookup per node,
and a per-node network call at teardown is exactly the kind of thing that turns a nicety into
a source of flakiness.

**4. The SCT runner counts too.** It is a real instance that outlives the test, so its cost is
only known once it is cleaned up. It reports its rate when the run starts and its final cost
when it is reaped. Two inputs are missing today — the runner registers itself without a region
or an instance type — and both must be filled in before it can be priced at all.

**5. Reaped and leaked resources report cost too.** Resources cleaned up by the reaper rather
than by the test are the expensive surprises, so they must not be the ones showing blank.
These paths have the instance in hand and can compute the cost. This is load-bearing rather
than a nicety: Argus deliberately does not derive a missing cost from a stored rate, so if SCT
does not send the number, nothing else produces it.

**6. A run's cost is estimable before it starts.** By the time a test is configured, the
instance types, node counts, region and duration are all known, so the cost can be computed
from configuration alone — no provisioning, no runner, no Argus. This is exposed as a
standalone command so that anything can ask "what will this cost?" without starting a run,
and it is what the future approval gate will read.

Two consequences worth stating:

- The estimate prices everything at on-demand rates and reports separately whether the run is
  configured for spot. Spot rates are not knowable ahead of time, and for a number that a gate
  may act on, an upper bound is the safe direction to be wrong in.
- Because it needs no pipeline, the same estimate can be surfaced wherever a test is
  *described* rather than run — per review feedback, publishing a full-run on-demand estimate
  alongside a test case's metadata would let people compare the cost of tests without
  launching any of them. Worth doing once the estimate itself is trusted.

**7. The estimate is also reported to Argus** as a run-level figure, so the details page can
show estimate against actual.

### Dependencies

The Argus side is implemented in argus#1071 and covers everything above: rate and spot flag at
creation, live cost while running, a cost field on the runner record, and a run-level estimate.
Argus rejects a zero cost at every layer, which backs up rule 2 rather than replacing it.

SCT vendors the Argus client and regenerates it wholesale, so this work cannot merge until
argus#1071 lands and a client release is tagged. Development happens against a locally patched
client; the merge must carry a genuine re-vendor.

One question is still open on the Argus side about how the stored type is versioned, which
could still move field names.

## Files to Modify

- `sdcm/utils/cloud_catalog/cost.py` -- rate lookup and cost arithmetic, including the
  unknown-is-not-free rule and the pre-run estimate (**landed** in #15987)
- `sct.py` -- `estimate-cost` command (**landed** in #15987)
- `sdcm/cluster.py` -- capture rate and start time when a node is registered; report cost when
  it is terminated
- `argus/client/sct/client.py` -- re-vendored once the Argus client is released
- `sdcm/utils/argus.py`, `sdcm/utils/resources_cleanup.py`, `utils/cloud_cleanup/__init__.py` --
  report cost from the cleanup and reaper paths
- `sdcm/sct_runner.py` -- report the runner's cost when it is reaped
- `sdcm/tester.py` -- send the runner's rate and the run-level estimate at run start
- `sdcm/utils/cloud_catalog/pricing.py` -- scheduled rate refresh; bound the live pricing calls
- `unit_tests/unit/test_cost.py` -- cost rules and estimate (**landed** in #15987)

## Verification

- [ ] Unit tests cover the cost rules offline: a known instance type is priced, an unpriceable
      one reports unknown rather than zero, spot is flagged, and partial totals stay partial
- [ ] Pricing never fails a test: an unreachable or broken pricing source yields "unknown", not
      an exception and not a hang
- [ ] The estimate can be produced for a test case without provisioning anything
- [ ] A live run shows a rising cost in Argus for resources that are still running
- [ ] A finished run shows a per-instance cost and a run total, with unpriceable resources shown
      as unknown and the total marked partial
- [ ] A resource reaped by cleanup rather than by the test still reports its cost
- [ ] The runner's cost is attributed to the right test run once it is reaped
- [ ] Totals are the same order of magnitude as the cloud-monitor cost site for the same run
      (exact agreement is not expected — the two round differently and price spot differently)
- [ ] `uv run sct.py pre-commit` passes
