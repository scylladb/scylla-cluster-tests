---
status: draft
domain: framework
created: 2026-09-08
last_updated: 2026-09-08
owner: fruch
---

# Health Check Node Sampling Plan

Split out of `docs/plans/infrastructure/health-check-optimization.md`, which
reduces what each health-check gate costs. This plan reduces how many nodes
each gate covers. It is deliberately separate: it depends on information the
framework does not record today, and its open questions were blocking review of
the cost work.

**Do not start this plan before the parent plan's instrumentation phase has
produced a measured baseline.** Sampling trades coverage for time, and there is
currently no evidence about how much time is left to win once the retry costs
are fixed. If the parent plan's phases bring the gate down to a few minutes,
sampling may not be worth its complexity at all — that decision is the first
deliverable here.

## Problem Statement

The health gate checks every node in the cluster, and each node reports on the
whole cluster. Coverage is therefore quadratic in cluster size while the
information gained is heavily redundant: on a healthy 60-node cluster, the
sixtieth node's report says the same thing as the first.

For very large clusters this is the last remaining source of scale-dependent
cost after the parent plan's work. The tradeoff is real, though: checking fewer
nodes weakens split-brain detection, which is the main thing the gate exists to
catch.

## Current State

`sdcm/cluster.py:BaseScyllaCluster.check_cluster_health` checks every node,
with no notion of topology or of which nodes are more likely to be affected.

The framework does expose the topology needed to sample: nodes carry a
datacenter and rack index, and `sdcm/cluster.py:BaseScyllaCluster` can already
group nodes by rack. `sdcm/nemesis/utils/node_allocator.py:NemesisNodeAllocator`
holds the canonical datacenter/rack filtering used to pick nemesis targets, and
is the natural place to reuse rather than reimplement.

**What is missing.** The earlier design assumed the gate could prioritise "the
nodes that participated in the last disruption". It cannot, for two reasons:

- The gate runs *before* a disruption, not after it. The disruption whose
  damage would justify prioritising a node finished on the previous cycle.
- The allocator's record of which nodes a disruption held is cleared when the
  disruption ends, before the next cycle's gate runs.

Recording participation so the gate can use it is therefore new work, and its
shape is unresolved — see Needs Investigation.

## Goals

1. **Decide whether sampling is warranted at all**, from the parent plan's
   measured baseline, before building it.
2. **Reduce the checked set proportionally** while guaranteeing coverage of
   every datacenter and every rack.
3. **Avoid permanent blind spots** — no node may go unchecked indefinitely
   across a long run.
4. **Keep split-brain detection honest** — document, per sampling mode, what it
   can no longer detect.
5. **Default to no sampling**, so existing tests are unaffected.

## Implementation Phases

### Phase 1: Decision from measurement

**Importance**: Critical
**Dependencies**: parent plan's instrumentation and backoff phases

Take the parent plan's post-fix measurements on the largest available cluster
and decide whether sampling is still worth building. Record the decision and
its numbers here.

**Definition of Done**:
- [ ] Post-fix gate cost measured on a large multi-DC cluster
- [ ] Go/no-go decision recorded with the numbers behind it
- [ ] If no-go, this plan is closed as `complete` with the rationale

---

### Phase 2: Topology-aware sampling

**Importance**: Important
**Dependencies**: Phase 1 (go decision)

Select a subset of nodes to check, guaranteeing at least one node per
datacenter and per rack, and never fewer than two nodes overall so that
disagreement remains detectable. Rotate the selection across gates so that
every node is checked within a bounded number of cycles.

Reuse the existing datacenter/rack filtering rather than adding a second
implementation of it.

**Configuration**:
```yaml
cluster_health_check_sampling_mode: 'all'  # all | cluster_quorum | dc_quorum | rack_quorum | dc_one | rack_one
```

**Definition of Done**:
- [ ] `all` is the default and checks every node, as today
- [ ] Every other mode guarantees at least one node per datacenter and per rack
- [ ] Never fewer than two nodes are checked
- [ ] Selection rotates so no node is skipped indefinitely
- [ ] Interaction with nemesis-node exclusion is defined: exclusion does not
      drop the sample below its guarantees
- [ ] Each mode's detection tradeoff documented
- [ ] Parameter documented, default added, `docs/configuration_options.md` regenerated
- [ ] Unit tests cover the guarantees for multi-DC and multi-rack topologies

## Needs Investigation

- **Prioritising nodes affected by the previous disruption.** Requires
  recording which nodes a disruption held, and keeping that record past the
  disruption's cleanup so the next gate can read it. Whether this is worth the
  coupling between the nemesis runner and the gate is unresolved.
- **Detection rate of each sampling mode.** The acceptable false-negative rate
  for split-brain detection has never been agreed. Without it, the modes cannot
  be recommended for anything.
- **Escalation on disagreement.** Whether a sampled gate that finds
  disagreement should escalate to a full check, and what that costs.

## Testing Requirements

**Unit.** Sampling selection is pure logic over topology and should be tested
directly: per-mode guarantees, minimum node count, rotation across repeated
gates, and behaviour when exclusion removes candidates. Extend
`unit_tests/unit/test_health_check_parallel.py`.

**Integration.** A multi-DC, multi-rack cluster must show the sample respecting
topology, and repeated gates must show rotation covering every node.

**Correctness.** A simulated membership disagreement must still be detected in
every mode that claims to detect it.

## Success Criteria

1. The go/no-go decision is recorded with measurements behind it.
2. If built: `all` remains the default and no existing test changes behaviour.
3. Every non-default mode's coverage guarantees hold on multi-DC, multi-rack
   topologies, and are unit-tested.
4. No node goes unchecked for more than a documented number of gates.
5. Each mode's detection limits are documented.

## Risk Mitigation

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Sampling misses a real problem | High | Medium | Guaranteed per-DC and per-rack coverage; rotation; `all` stays the default |
| Built without being needed | Medium | High | Phase 1 is an explicit go/no-go gated on measurement |
| Sampling and nemesis exclusion interact badly | Medium | Medium | Define exclusion as applied before selection, with guarantees enforced afterwards |
| Rotation state adds cross-gate coupling | Low | Medium | Keep the state on the cluster object and treat loss of it as "check everything" |
