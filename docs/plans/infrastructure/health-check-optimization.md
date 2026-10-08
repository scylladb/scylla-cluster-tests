---
status: in_progress
domain: framework
created: 2026-01-11
last_updated: 2026-09-08
owner: fruch
---

# Health Check Optimization Plan

Tracked as [SCT-523](https://scylladb.atlassian.net/browse/SCT-523). Originally
reported as [#9547](https://github.com/scylladb/scylla-cluster-tests/issues/9547).

## Problem Statement

The cluster health check is the gate that runs between nemesis disruptions to
decide whether the cluster is still fit to continue the test. On a 60-node,
5-DC longevity run it took over two hours per gate — longer than the nemesis
operations it was guarding — making it the dominant cost of the test rather
than a cheap safety check.

Two of the original causes have since been fixed (see Current State), and
measurement has since shown that most of that two hours went with them.

**The remaining cost is waiting, not working — but only when the gate fails.**
A node that disagrees with the cluster is re-checked up to ten times with a
fixed delay between attempts, and the checked state is *cluster-wide*: every
node reports on every other node. So a single condition — one node held down by
a nemesis, one node lagging in gossip — is detected independently by every node
in the cluster, and every node then pays the full retry budget waiting for it to
clear. The cost of one disagreement scales with cluster size even though there
is only one thing wrong.

**A healthy gate was never the problem.** Across 453 measured gates the retry
path did not fire once, and the gate cost seconds: eight on six nodes, twenty-five
on thirty, an extrapolated fifty on sixty. Parallel node checks (Phase 2) already
cut the failing case from just under three hours to roughly thirty-five minutes.
What is left to win is the remaining thirty-five minutes, and it is won by
shrinking the retry budget rather than by making any check faster.

This produces three distinct pain points:

1. **Fixed delays dominate.** A node that will never agree within the gate
   burns its entire retry budget in `time.sleep`. Multiplied across nodes, this
   accounts for essentially all of the reported two hours.
2. **Transient lag is charged at the full rate.** The retry delay is flat, with
   no shorter first attempt. Gossip propagates schema roughly once a second, so
   a disagreement that resolves almost immediately still costs a full delay.
   Measured on a rolling-upgrade run, this consumed roughly half of an entire
   test phase, with every single occurrence resolving on the first retry
   ([SCT-808](https://scylladb.atlassian.net/browse/SCT-808)).
3. **Nodes that are *supposed* to be down still trip the gate.** A node under
   an active nemesis is legitimately unavailable, but every other node reports
   it as down and retries over it. The framework's current answer is to disable
   health checks entirely whenever more than one nemesis runs, which trades the
   cost for a complete loss of the safety gate.

## Current State

### What the gate does

`sdcm/cluster.py:BaseScyllaCluster.check_cluster_health` is invoked from
`sdcm/nemesis/__init__.py:NemesisRunner.execute_nemesis`, at the *start* of each
nemesis cycle. It is the only production caller. The end-to-end lifecycle is:

> nemesis cycle begins -> health gate runs *before* the disruption -> each node
> independently gathers cluster-wide state (nodetool status, `system.peers`,
> gossip info, Raft group0 membership, token ring) -> per-node validators
> compare that state against the rest of the cluster -> any disagreement
> discards the result and re-gathers for that node after a delay -> the gate
> either passes silently or publishes validation events on the final attempt ->
> the disruption runs -> its outcome is recorded on the runner -> the next
> cycle's gate consults that outcome and skips itself if the disruption was
> skipped

Per-node validation lives in `sdcm/utils/health_checker.py`
(`check_nodes_status`, `check_node_status_in_gossip_and_nodetool_status`,
`check_schema_version`, `check_nulls_in_peers`,
`check_group0_tokenring_consistency`). The first of these fails on *any* node
that is not in the `UN` state, from the point of view of the node doing the
reporting — which is what makes one down node cost the whole cluster.

`sdcm/cluster.py:BaseScyllaCluster.wait_for_schema_agreement` is a second,
separate per-node loop that still runs sequentially and shares the same retry
constants (`sdcm/utils/health_checker.py:check_schema_agreement_in_gossip_and_peers`).
It is the path SCT-808 measured.

### Already delivered

- **Skip after a skipped nemesis** — the gate is suppressed when the previous
  disruption on that runner was skipped, since nothing happened that could have
  changed cluster health. Landed in
  [#13522](https://github.com/scylladb/scylla-cluster-tests/pull/13522) after an
  initial attempt was reverted; the skip decision deliberately lives in the
  caller rather than inside the gate.
- **Parallel node checks** — nodes are checked concurrently, with per-node
  failures collected and reported rather than aborting the sweep. Worker count
  is `cluster_health_check_parallel_workers` (default 5). Landed in
  [#13584](https://github.com/scylladb/scylla-cluster-tests/pull/13584).

Parallelism reduced wall-clock by roughly the worker count but did not remove
the underlying waiting — the sleeps are now paid five at a time instead of one
at a time. The original 120-minute figure is therefore no longer a valid
baseline, and no measurement has been taken since.

### Configuration that exists today

Only `cluster_health_check` (on/off) and
`cluster_health_check_parallel_workers`. Earlier revisions of this plan
proposed five further parameters under two inconsistent naming schemes; none of
them were implemented, and the names are superseded by this revision.

### Constraint: the gate must stay synchronous

A background health monitor was prototyped and rejected
([#15209](https://github.com/scylladb/scylla-cluster-tests/pull/15209)): the
check exists to decide whether to *stop the test* before the next disruption
compounds an existing problem. Moving it off the critical path defeats its
purpose and produces cascading failures that are hard to diagnose. Optimizations
must make the gate cheaper, not asynchronous.

### Related work

`docs/plans/nemesis/nemesis-precheck.md` removes gate invocations for nemesis
that can never run in a given configuration. It reduces *how often* the gate
runs; this plan reduces *what each run costs*. The two are complementary and
touch different code.

Topology-aware node sampling — checking a representative subset rather than
every node — is split into `docs/plans/infrastructure/health-check-sampling.md`,
because it depends on information the framework does not record yet and its open
questions were blocking review of the rest.

## Goals

1. **Establish a real baseline.** Produce a measured breakdown of gate cost by
   operation, by retry attempt, and by time-spent-waiting versus
   time-spent-working. No further optimization is sized against estimates.
2. **Make a transient disagreement cheap.** A condition that resolves on the
   first retry must cost on the order of a second, not the full fixed delay.
3. **Make a persistent disagreement cost once, not once per node.** Detecting
   that one node is down must not cost the retry budget of every other node.
4. **Restore the gate under parallel nemesis.** Health checks must run when
   more than one nemesis is active, excluding only the nodes actually under
   disruption, instead of being disabled wholesale.
5. **Allow a cheaper check where full validation is not warranted.** Operators
   of very large clusters must be able to trade validation depth for speed
   through configuration, with `full` remaining the default.
6. **Preserve split-brain detection.** Every reduction must keep the ability to
   detect nodes disagreeing about cluster membership, or state plainly in its
   documentation that it does not.

## Implementation Phases

Phases 1 and 2 are complete and retained for history. Phases 3 onward are the
remaining work, ordered by dependency: measure, then fix what measurement
confirms, then add configurability.

---

### Phase 1: Skip the gate after a skipped nemesis — COMPLETE

**Importance**: Critical

Suppress the gate when the previous disruption was skipped, since no cluster
state changed. Shipped in
[#13522](https://github.com/scylladb/scylla-cluster-tests/pull/13522).

**Definition of Done**:
- [x] Gate is skipped when the preceding disruption reported itself skipped
- [x] Skip and its reason are logged
- [x] Decision lives in the caller, not inside the gate
- [x] Unit tests cover skipped, not-skipped, and no-previous-disruption cases

---

### Phase 2: Parallel node checks — COMPLETE

**Importance**: Critical

Check nodes concurrently instead of one at a time, collecting per-node failures
rather than aborting on the first. Shipped in
[#13584](https://github.com/scylladb/scylla-cluster-tests/pull/13584).

**Definition of Done**:
- [x] Nodes are checked concurrently with a configurable worker count
- [x] `cluster_health_check_parallel_workers` documented, default 5
- [x] A failure on one node does not prevent other nodes being checked
- [x] All per-node failures are reported, not just the first
- [x] Unit tests cover the parallel path, the single-worker fallback, and failures

---

### Phase 3: Instrumentation and baseline

**Importance**: Critical
**Dependencies**: none

Make the gate's cost observable before changing it. Record, per gate
invocation: elapsed time for each of the five state-gathering operations, the
number of retry attempts each node consumed, which validator triggered each
retry, and the split between time spent waiting and time spent working.

Reuse the framework's existing timing helpers rather than introducing another
one. No behaviour changes: this phase only observes.

Then run the measurement against a multi-node longevity job and record the
resulting breakdown in this document, replacing the stale 120-minute estimate
with real numbers. Subsequent phases are sized against that table.

**Deliverables**: timing instrumentation in `sdcm/cluster.py` (the gate and the
per-node retry loop) and `sdcm/utils/health_checker.py` (the validators, and the
timing breakdown they are recorded into); a measured baseline table added to
this plan.

**Definition of Done**:
- [x] Each state-gathering operation logs its own elapsed time
- [x] Each node logs its retry count and the validator that caused each retry
- [x] The gate logs total wall-clock and the waiting-versus-working split
- [x] No change to which checks run or when the gate passes or fails
- [x] Baseline measured on a multi-node cluster and recorded in this plan

**Measured (2026-09-14 to 2026-09-24)**: six runs, 453 gates and 5,298 per-node
samples. The last three vary only node count, holding workers at five.

| nodes | gates | samples | gate wall-clock | per node | validation | gathering |
|-------|-------|---------|-----------------|----------|------------|-----------|
| 6     | 226   | 1,357   | 8.00 s          | 4.171 s  | 2.328 s    | 1.843 s   |
| 20    | 129   | 2,581   | 16.90 s         | 4.221 s  | 2.312 s    | 1.908 s   |
| 30    | 32    | 961     | 25.05 s         | 4.112 s  | 2.244 s    | 1.868 s   |

Gathering, per node per gate: nodetool status 0.697 s, token ring 0.503 s, Raft
group0 0.231 s, gossip 0.206 s, peers 0.206 s. A seventh run at six nodes,
adding a per-validator breakdown, reproduced the same 8.00 s gate and the same
2.3 s validation, so the instrumentation does not change what it measures.

**Waiting was 0.0 s in every gate, and all 5,298 samples passed on their first
attempt.** The retry path did not fire once, including in a run configured to
force it: no settle time between a disruption and the next gate, and a
disruption pool restricted to node-down nemeses. The gate opened a median 1.5 s
after each disruption ended and still found every node up, because each nemesis
already waits for the node it disturbed before returning.

**Per-node cost does not vary with cluster size**, and neither does validation.
Gate wall-clock is `ceil(nodes / workers) x per-node cost`, which fits all three
points to within a few percent: the gate simply runs waves of roughly four
seconds, five nodes at a time. A 60-node healthy gate extrapolates to about
50 seconds.

**Scope.** Every run was single-datacentre with a single nemesis, so the gate
always opened in a quiet window. The reported two-hour case was 60 nodes across
five datacentres; cross-datacentre gossip latency remains untested, and it is
the one condition under which the retry path might plausibly fire.

---

### Phase 8: Name the fixed cost inside validation

**Importance**: Important
**Dependencies**: Phase 3
**Tracked as**: [SCT-1002](https://scylladb.atlassian.net/browse/SCT-1002)

Validation is the larger half of the gate: 2.3 s of the 4.2 s each node spends,
against 1.9 s for all five gathering operations together. Until it was timed,
the gate reported well under half of its own cost.

**The premise this phase started from was wrong.** It assumed each node's
validators walk every other node's entry, so the work would grow with the square
of cluster size and resist parallelism. Measurement refutes that: per-node
validation is 2.328 s at six nodes and 2.244 s at thirty. It does not grow with
node count at all. The earlier "2.2x worse than ideal" figure was computed
against gathering alone and should not be carried forward.

That flatness pointed at fixed overhead rather than per-node work, and a
per-validator breakdown (230 gates, 1,381 samples, six nodes) found it in one
place.

**The group0/token-ring check is 99.9% of validation** — 2.298 s of 2.300 s. The
other four validators cost 0.000 s each, which is what comparing a few dozen
dictionary entries should cost, and nothing is unaccounted for between them.

That check is not a comparison. It delegates to the node's Raft helper, which
opens a **fresh exclusive CQL connection, per node and per gate**, to read
whether the limited-voters feature is enabled — a cluster-wide constant — before
computing its difference. Across the run it did this 1,381 times, once per node
per gate, and the difference was empty every single time. The group0 and
token-ring membership it compares had already been gathered moments earlier in
the same function and handed to it as arguments.

So the larger half of the gate is a repeated connect-and-query for a value that
cannot change during a run. This phase has found what it set out to find; the
fix belongs to a follow-up.

**Definition of Done**:
- [x] Per-operation timings are logged with enough resolution to be averaged
- [x] Validation time is measured per node and reported alongside gathering
- [x] Measurement across node counts shows how validation scales — it does not
- [x] Validation is broken down per validator, naming where the fixed cost sits
- [x] The gate accounts for its wall-clock: gathering, validation and overhead
      sum to the measured total, with no unattributed remainder
- [ ] A follow-up is opened for the fix — this phase measures, it does not
      optimise

**Follow-up, in increasing order of effort.** Cache the feature-flag lookup for
the cluster rather than reading it per node per gate. Compare the membership
already passed in and only take the expensive path when it actually differs.
Reuse a pooled session instead of opening an exclusive connection. Any one of
them removes most of the larger half of the gate; on the measured model a
healthy 60-node gate would fall from about 50 seconds to about 22.

---

### Phase 4: Backoff and cluster-level short-circuit

**Importance**: Critical
**Dependencies**: Phase 3

Measurement changed the ordering here. The retry path never fired in 5,298
samples, so nothing in this phase affects a healthy run at all. It governs only
what a failing gate costs — and there the budget is large. A node that never
agrees spends ten attempts and nine fifteen-second sleeps, about 177 s. Because
the checked state is cluster-wide, every node pays it for the same condition, so
a 60-node gate costs roughly 35 minutes before it gives up. Sequentially, as the
gate ran before Phase 2, the same arithmetic gives just under three hours, which
is where the original report came from.

**Budget.** Reduce the number of attempts. This is the largest single
improvement available, needs no further measurement to justify, and is simpler
than either change below. Five attempts with one-, two-, four- and eight-second
delays leaves a fifteen-second tolerance for transient lag — five times what
SCT-808 ever needed — and cuts the worst case roughly fivefold. The trade-off is
deliberate: a disagreement taking longer than the new ceiling fails the gate
where today it would pass, which is the intended behaviour for a run that is
already in trouble.

**Backoff.** Make the delay start short and grow rather than staying flat, so a
disagreement that resolves immediately — every occurrence SCT-808 observed —
costs about a second instead of the full delay. This applies to both retrying
call paths, which share the same constants, and closes
[SCT-808](https://scylladb.atlassian.net/browse/SCT-808).

**Short-circuit.** Stop every node independently paying the retry budget for
the same cluster-wide condition. When the disagreement is about a node's
membership state rather than about the reporting node, the gate should reach
that conclusion once for the cluster instead of once per node. This is the
largest change of the three and the least urgent once the budget shrinks.

**Definition of Done**:
- [ ] Attempt count reduced; the new worst-case gate cost is documented per
      node count alongside the old one
- [ ] Retry delay starts short and grows; worst-case ceiling is documented
- [ ] Both retrying call paths use the shared backoff
- [ ] A cluster-wide condition costs one retry budget, not one per node
- [ ] Gate still fails for conditions that do not clear, with the same events
- [ ] Unit tests cover the attempt count, backoff timing and the short-circuit
- [ ] SCT-808 closed

---

### Phase 5: Exclude nodes under nemesis; restore the gate under parallel nemesis

**Importance**: Critical
**Dependencies**: Phase 4 — not a technical dependency. Restoring the gate for
parallel-nemesis tests adds gate invocations to exactly the tests that have none
today, so the retries those gates perform should be cheap before that happens.
Reordering is possible if Phase 4 stalls, at the cost of a slower rollout.

A node under an active disruption is expected to be unavailable. Today the gate
neither excludes it nor tolerates it, so it disables itself entirely whenever
more than one nemesis runs — losing the safety gate exactly where chaos is
highest.

Change two behaviours:

- Nodes currently marked as running a nemesis are excluded from the set of
  nodes being checked.
- Those nodes being reported as down *by other nodes* is treated as expected,
  not as a critical health failure.

Together these remove the wholesale bail-out: the gate runs under parallel
nemesis over the nodes that are not under disruption, and the checks that were
being silently skipped along with it — the running-nemesis count check and
partition validation — run again.

**Configuration**:
```yaml
cluster_health_check_exclude_running_nemesis: true  # default: true
```

**Definition of Done**:
- [ ] Nodes marked as running a nemesis are not themselves checked
- [ ] Those nodes reported as down by others does not fail the gate
- [ ] The gate runs with more than one nemesis active
- [ ] Running-nemesis count check and partition validation run under parallel nemesis
- [ ] Setting the parameter to `false` restores checking every node
- [ ] Parameter documented and default added
- [ ] Unit tests cover exclusion, tolerance, and the parallel-nemesis path

---

### Phase 6: Configurable check operations

**Importance**: Important
**Dependencies**: Phase 5

Let operators trade validation depth for speed. `full` keeps today's behaviour
and remains the default; `basic` keeps membership and Raft consistency;
`minimal` keeps only node status. An explicit list may be given instead of a
named mode.

Each mode must document what it stops detecting — `minimal` in particular has
materially weaker split-brain detection and must say so.

**Configuration**:
```yaml
# named mode
cluster_health_check_operations: 'full'   # full | basic | minimal

# or an explicit list
cluster_health_check_operations: ['status', 'raft']
```

**Definition of Done**:
- [ ] Named modes and explicit lists both accepted; invalid values rejected at config load
- [ ] `full` is the default and behaves exactly as before this phase
- [ ] Only the selected operations are gathered — unselected ones are not fetched
- [ ] Each mode's detection tradeoff documented
- [ ] Parameter documented and default added
- [ ] Unit tests assert that unselected operations are not performed

---

### Phase 7: Documentation and rollout

**Importance**: Important
**Dependencies**: Phase 6

**Definition of Done**:
- [ ] `docs/configuration_options.md` regenerated
- [ ] Recommended settings per cluster size documented, with their tradeoffs
- [ ] Migration note for tests that currently set `cluster_health_check: false`
      purely for speed, which may no longer need to
- [ ] Large-cluster test-case configurations updated to the recommended settings
- [ ] This plan's status advanced and its baseline table filled in

## Testing Requirements

**Unit.** Extend the existing suites rather than adding parallel ones:
`unit_tests/unit/test_health_check_parallel.py` for gate behaviour,
`unit_tests/unit/test_utils_health_checker.py` for the validators and their
retry behaviour, and `unit_tests/unit/nemesis/execute_nemesis/` for the
interaction between the gate and the nemesis runner.

Timing-sensitive tests must assert on the delay sequence the code requests, not
on real elapsed time.

**Integration.** For Phase 5, a run with more than one nemesis active must show
the gate executing and excluding only the disrupted nodes. For Phase 6, a run
per mode must show that unselected operations are never issued.

**Performance.** Phase 3 establishes the baseline; Phases 4 and 5 each report
their measured improvement against it on the same cluster shape. Measure at
least: total gate wall-clock, per-operation time, retry counts, and the
waiting-versus-working split.

**Correctness.** A node made genuinely unavailable outside of a nemesis must
still fail the gate at every phase. A simulated disagreement about cluster
membership must still be detected in `full` and `basic` modes.

## Success Criteria

1. A measured baseline exists in this document, and every subsequent phase
   reports its improvement against it.
2. A disagreement that clears immediately costs roughly a second rather than a
   full retry delay.
3. One node being down costs one retry budget for the cluster, not one per node.
4. The gate runs under parallel nemesis, and running-nemesis count and
   partition validation are no longer silently skipped.
5. `full` mode detects everything it detects today; any mode that detects less
   says so in its documentation.
6. New parameters are documented with safe defaults. Tests running a single
   nemesis see no behavioural change. Parallel-nemesis tests do change, by
   design: the gate they silently lost is restored over their non-disrupted
   nodes, and Phase 5's rollout covers that.
7. `uv run sct.py pre-commit` passes; MASTER.md and progress.json reflect the
   plan's status.

## Risk Mitigation

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Shorter retries turn transient lag into false failures | High | Medium | Keep the worst-case ceiling comparable to today's; Phase 3 measurement shows how many attempts real conditions need |
| Short-circuiting hides a genuine per-node problem | High | Low | Short-circuit only where the disagreement is about another node's membership, never where it is about the reporting node |
| Excluding nemesis nodes masks damage the nemesis caused | Medium | Medium | Exclusion is scoped to the disruption's duration; the node is checked on the next gate, once released |
| Restoring the gate under parallel nemesis destabilises those tests | Medium | Medium | Parameter defaults preserve exclusion; roll out on a limited set of parallel-nemesis jobs first |
| `minimal` mode misses split-brain | High | Medium | Not the default; documented limitation; recommended only alongside sampling |
| Configuration surface grows unmanageable | Low | High | Two new parameters only; sampling deliberately deferred to its own plan |
