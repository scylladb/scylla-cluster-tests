# Mini-Plan: Make On-Demand Fallback Visible in Estimate and Bill

**Date:** 2026-09-23
**Owner:** fruch
**Estimated LOC:** ~150
**Related Jira:** [SCT-852](https://scylladb.atlassian.net/browse/SCT-852) (epic [SCT-851](https://scylladb.atlassian.net/browse/SCT-851)), [SCT-1005](https://scylladb.atlassian.net/browse/SCT-1005)
**Depends on:** PR #15987 -- introduces `cost.py`
**Split from:** the spot-aware cost estimates mini-plan

## Problem

When spot capacity is short, SCT falls back to on-demand. A run priced as spot can therefore cost
several times its estimate, and nothing anywhere says it happened. The engineer sees a cheap
estimate, the bill says otherwise, and the gap is invisible unless someone goes looking at the
cost tracking site.

This is not a corner case. `instance_provision_fallback_on_demand` is set `true` by the AWS and
Azure backend defaults, and the Jenkins parameter defaults to empty so it never overrides — a
plain AWS longevity config resolves to `instance_provision: spot` with fallback enabled.
[SCT-1005](https://scylladb.atlassian.net/browse/SCT-1005) proposes flipping that default, but
even once fallback is rare, a run that silently fell back is exactly the run someone needs told
about.

Two separate gaps: the estimate does not say the number could be much higher, and the cost report
does not say which lifecycle each node actually got.

## Approach

### 1. The estimate reports a ceiling, not just a figure

When the resolved config enables fallback, the estimate warns and reports the on-demand total
beside the spot total. The spot figure is a **lower bound**, not a prediction, and should be
presented that way.

The future approval gate should read the **ceiling**, because underestimating is the dangerous
direction for a gate — blocking a cheap run is an annoyance, waving through an expensive one is
the thing the gate exists to prevent.

### 2. The bill reports what actually happened

SCT already knows each node's real lifecycle, and the per-node cost report is therefore already
correct. What is missing is the summary: how many nodes ended up on-demand when spot was asked
for. Report that at run level, so a run that quietly fell back is legible at a glance rather than
reconstructed from per-node rows.

A run where every node fell back is a different event from one where a single replacement node
did, so report the count, not a boolean.

### 3. Do not warn where it carries no information

If SCT-1005 has not landed, fallback is on for every AWS and Azure run, and an unconditional
warning is noise that trains people to ignore it. Warn where the estimate is genuinely uncertain —
a spot run with fallback enabled — and say nothing on an `on_demand` run, where fallback cannot
apply. Keep the ceiling reporting either way; that is data, not a warning.

### 4. Out of scope

Flipping the default is SCT-1005. Spot pricing itself is its own mini-plan. Alerting or gating on
the ceiling belongs to the approval-gate work in phases 3-4 of the epic.

## Files to Modify

- `sdcm/utils/cloud_catalog/cost.py` -- **introduced by PR #15987** -- carry an on-demand ceiling
  and a fallback-enabled flag on the estimate result; expose both to callers
- `sct.py` -- surface the warning and the ceiling in `estimate-cost` output, in both human and
  JSON forms, so the pipeline stage can read the ceiling
- `sdcm/cluster.py` -- include the node's actual lifecycle when reporting its cost, so the run
  summary can count fallbacks
- `unit_tests/unit/test_cost.py` -- **introduced by PR #15987** -- ceiling present when fallback is
  enabled, absent warning on an `on_demand` run, fallback counting

## Verification

- [ ] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_cost.py -v`
- [ ] A spot config with fallback enabled reports both a spot figure and an on-demand ceiling, and
      warns
- [ ] An `on_demand` config reports no fallback warning
- [ ] A spot config with fallback disabled reports no warning but still exposes the ceiling
- [ ] `estimate-cost --format json` exposes the ceiling as a distinct field the gate can read
- [ ] A run where some nodes fell back reports the count, not just a flag
- [ ] Full unit test suite passes: `uv run sct.py unit-tests`
- [ ] `uv run sct.py pre-commit` passes
