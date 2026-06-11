---
status: draft
domain: nemesis
created: 2026-06-11
last_updated: 2026-06-15
owner: pehala
---

# Nemesis Pre-Execution Skip Check (`precheck`) Plan

## Problem Statement

Nemesis decide whether they can run by raising `UnsupportedNemesis` (or `MethodVersionNotFound`) **inside `disrupt()`**, which executes on **every cycle**. By the time that exception is raised, the framework has already paid for expensive, repeated setup work that is then thrown away.

### Current Behavior

For every nemesis cycle — including one that will immediately skip — `NemesisRunner.execute_nemesis()` (`sdcm/nemesis/__init__.py:1904`) performs, in order:

1. `self.cluster.check_cluster_health()` (`sdcm/nemesis/__init__.py:1919`). The inline comment at `sdcm/nemesis/__init__.py:1910` states this can cost **2+ hours on large clusters**.
2. `self.set_target_node()` (`sdcm/nemesis/__init__.py:1928`) — acquires a node lock from `NemesisNodeAllocator`.
3. Construction of a `DisruptionEvent`, an Argus submission (`argus_submit`), action-log scopes, and metrics events (`sdcm/nemesis/__init__.py:1929-1943`).
4. Only then `nemesis.disrupt()` runs and raises (`sdcm/nemesis/__init__.py:1948-1951`).

The runtime guard in `NemesisRunner.run()` (`sdcm/nemesis/__init__.py:436-448`) only suppresses the *interval* sleep on a skip and stops the thread after 3 consecutive skips **when a single nemesis remains** (`sdcm/nemesis/__init__.py:440`). It does nothing to avoid the wasted health-check / node-selection / event work, and it does not help when many nemesis are selected.

Skipped nemesis also generate noise in Argus and logs today because the skip is reported after execution starts, so infeasible nemesis still produce repeated skip rows and log messages on every cycle.

### Why this is wasteful

A code analysis of all **130** explicit `raise UnsupportedNemesis(...)` sites (plus the implicit `MethodVersionNotFound` raised by the `@scylla_versions` decorator) shows that the skip decision is **static for the whole test run** in roughly two thirds of cases:

| Category | Decision depends on | Count | Static per run? |
|----------|---------------------|------:|-----------------|
| 1 — Config / backend / edition | `cluster.params.get(...)`, `_is_it_on_kubernetes()`, `node.is_enterprise`, `cluster.extra_network_interface` | 57 | Yes — known before the test starts |
| 2 — Version / feature flag / cluster-uniform node attribute | `is_tablets_feature_enabled()`, `raft.is_consistent_topology_changes_enabled`, `ComparableScyllaVersion` checks, `SkipPerIssues()`, node distro/OS | 28 | Yes — fixed once the cluster is up |
| 3 — Runtime cluster state | keyspaces/tables/sstables/partitions present, live node counts, busy nodes, audit state toggled by the nemesis itself | 45 | No — genuinely dynamic |

So **~65 % (85/130)** of skip decisions never change after the cluster is up, yet they are re-evaluated every cycle behind the expensive setup described above. The remaining **~35 % (45/130)** legitimately depend on live, mutable state and must stay inside `disrupt()`.

The node OS/distro check in `disrupt_memory_stress` (`sdcm/nemesis/__init__.py:4768-4774`) is Category 2, not Category 3: although it reads `self.target_node.distro`, the distro is fixed at provisioning by a single `scylla_linux_distro` config param (`sdcm/sct_config.py:671`) and is uniform across the data-node pool, so `cluster.nodes[0].distro` yields the same answer for any target.

There is no mechanism today to evaluate a static skip **once** and exclude the nemesis from the rotation.

## Current State

### Nemesis class model

- `sdcm/nemesis/__init__.py:238` — `NemesisFlags`: boolean flags used only for selector filtering.
- `sdcm/nemesis/__init__.py:261` — `NemesisBaseClass(NemesisFlags, ABC)`: abstract base for individual disruptions. Holds `self.runner`, declares the abstract `disrupt()` (`sdcm/nemesis/__init__.py:269`). **No feasibility hook exists.**
- `sdcm/nemesis/__init__.py:276` — `NemesisRunner`: orchestrator. Builds and owns `self.disruptions_list` and the execution flow.

### Where the disruption list is built (the proposed gate point)

- `sdcm/nemesis/__init__.py:2005` — `build_disruptions_by_selector()` instantiates each subclass returned by `NemesisRegistry.filter_subclasses(...)` and appends it to `disruptions`. It already swallows construction errors with a broad `except Exception` (`sdcm/nemesis/__init__.py:2023-2024`).
- `sdcm/nemesis/__init__.py:2027` — `build_disruptions_by_name()` routes through `build_disruptions_by_selector()`, so both selection styles share one gate.
- `sdcm/nemesis/monkey/runners.py:16` — `SisyphusMonkey.__init__` calls `build_disruptions_by_selector(self.nemesis_selector)` then `shuffle_list_of_disruptions(...)`. Other runners (`ScyllaCloudLimitedChaosMonkey`, `K8sSetMonkey`, etc.) call `build_disruptions_by_name([...])`.

### Execution path that the precheck will short-circuit

- `sdcm/nemesis/__init__.py:2078` — `infinite_cycle` is `itertools.cycle(self.disruptions_list)`; pruned nemesis simply never enter it.
- `sdcm/nemesis/__init__.py:2083` — `call_next_nemesis()` asserts `self.disruptions_list` is non-empty (`sdcm/nemesis/__init__.py:2085`) and runs `execute_nemesis(next(self.infinite_cycle))`.

### Timing window (verified)

- `sdcm/cluster.py:5780` — `add_nemesis()` instantiates each runner (which immediately builds its disruption list).
- `longevity_test.py:136` calls `add_nemesis(...)` **after** the cluster is initialized (`db_cluster.wait_for_init(...)` in `sdcm/tester.py:965-974`) but **before** data prepare/load and **before** `db_cluster.start_nemesis()` (`longevity_test.py:240`).
- Consequence at build time: the cluster is **up** (Category 2 version/feature probes work against `self.runner.cluster.nodes[0]`), config/backend is **known** (Category 1), but **no data is loaded** and **no target node is selected** (Category 3 is not yet meaningful). This is exactly the right place for a one-time static check.

### Skip reporting today

- `sdcm/sct_events/continuous_event.py:134` — `DisruptionEvent.skip(skip_reason)` sets `is_skipped`/`skip_reason`.
- `sdcm/sct_events/nemesis.py:50-55` — `nemesis_status` maps `is_skipped` → `NemesisStatus.SKIPPED` (`argus.common.enums.NemesisStatus`).
- `sdcm/sct_events/argus.py:66` — the Argus plugin reads `nemesis_status` from the event, so a skipped nemesis currently surfaces as a `SKIPPED` row in Argus **once per skipped cycle**.

### Existing skips to migrate (line references)

- Category 1 examples: `disrupt_ldap_connection_toggle` (`sdcm/nemesis/__init__.py:1117-1123`), the SLA block (`sdcm/nemesis/__init__.py:5084-5232`), `_enable_disable_table_encryption` backend check (`sdcm/nemesis/__init__.py:4474`), network-interface checks (`3557`, `3648`, `4016`).
- Category 2 examples: `disrupt_restart_with_resharding` (`sdcm/nemesis/__init__.py:915-920`), `disrupt_destroy_data_then_rebuild` (`sdcm/nemesis/__init__.py:1090-1094`), raft-coordinator checks (`5652`, `5723`, `5834`), the node OS/distro check in `disrupt_memory_stress` (`4768-4774`), `@scylla_versions`-decorated groups (`2211/2216`, `3445/3451`, `4381`, `4471`).
- Category 3 (must stay in `disrupt()`): `modify_table.py:62`, `_destroy_data_and_restart_scylla` (`sdcm/nemesis/__init__.py:1023/1034`), all "no keyspaces/tables/partitions" checks, `AbortDecommissionMonkey` per-rack count (`sdcm/nemesis/monkey/abort_decommission.py:90`).

### What's Missing

- An overridable, return-based feasibility hook on `NemesisBaseClass`. → **Phase 1**
- A one-time evaluation/pruning step in `build_disruptions_by_selector()`. → **Phase 1**
- Graceful handling (a single CRITICAL event that fails the test) when every selected nemesis is pruned. → **Phase 2**
- A taxonomy-driven migration of the 85 static skips out of the per-cycle `disrupt()` path. → **Phases 3–4**
  - 57 Category 1 (config / backend / edition) guards → **Phase 3**
  - 28 Category 2 (version / feature flag / cluster-uniform node attribute) guards → **Phase 4**
- Documentation of the `precheck()` contract, the category rule, and the pruning/reporting behavior for future nemesis authors. → **Phase 5**

## Goals

1. **Add a return-based `precheck()` hook** to `NemesisBaseClass` that returns `str | None` (no exception-based control flow): `None` = runnable, a string = skip reason. Default implementation returns `None`.
2. **Evaluate `precheck()` exactly once** per nemesis, at disruption-list build time, and permanently exclude any nemesis that returns a reason — so pruned nemesis incur **zero** per-cycle health-check, node-selection, or event cost.
3. **Migrate all 85 static skips** (Category 1 + Category 2) from runner `disrupt_*` methods into the owning nemesis class `precheck()`, removing the now-redundant static guards in the **same PR** as each migration. Category 3 (45) checks remain in `disrupt()`.
4. **Fail the test loudly when misconfigured**: if every selected nemesis is pruned, emit exactly one `Severity.CRITICAL` event and stop the nemesis thread (configured nemesis not running must fail the test).
5. **Report each pruned nemesis as a `SKIPPED` Argus row exactly once** (not per cycle, not zero times), preserving skip visibility while eliminating the repeated work.
6. **No behavioral regression** for nemesis with no static skip (default `precheck()` returns `None`; they run exactly as before).

## Implementation Phases

Phases are ordered by dependency: the framework hook and reporting/empty-list handling land first (foundational, must leave the tree working), then the bulk migration is split into reviewable, domain-grouped PRs of ≤200 LOC each. Migration PRs depend only on Phase 1/2 and are independent of one another.

### Phase 1: Add the `precheck()` hook and one-time pruning

**Importance**: Critical
**Description**: Introduce the return-based hook and wire it into the single build gate. No nemesis overrides it yet, so behavior is unchanged (every `precheck()` returns `None`).

**Deliverables**:
- New method on `NemesisBaseClass` (`sdcm/nemesis/__init__.py:261`):
  ```python
  def precheck(self) -> str | None:
      """
      Static feasibility check, evaluated ONCE before the nemesis is scheduled.

      Return None  -> nemesis is runnable and kept in the rotation.
      Return a str -> human-readable reason; nemesis is permanently excluded.

      Use ONLY for conditions that do not change during the test:
        - test config / backend / product edition         (Category 1)
        - Scylla version / feature flags / cluster-uniform
          node attributes such as OS distro                (Category 2)

      No target node is selected yet — use self.runner.cluster.nodes[0] as a
      representative live node. Do NOT check dynamic state (data presence,
      live node counts, whether a specific target node is busy or alone in
      its rack) — those stay inside disrupt().
      """
      return None
  ```
- `build_disruptions_by_selector()` (`sdcm/nemesis/__init__.py:2005`) calls `precheck()` after instantiation and keeps the nemesis only when it returns `None`. Real errors raised inside `precheck()` continue to be caught by the existing broad `except Exception` (treated as today: log and skip the nemesis). Excluded nemesis are collected with their reasons and logged once.

**Adaptation Notes**: A bare `str | None` contract is used (per design decision) rather than a structured result; the reason string is what flows to logs and the Argus row in Phase 2.

**Definition of Done**:
- [ ] `NemesisBaseClass.precheck()` exists with the documented contract and `None` default.
- [ ] `build_disruptions_by_selector()` invokes `precheck()` exactly once per nemesis and excludes those returning a reason.
- [ ] Exceptions inside `precheck()` are handled by the existing `except Exception` path (no new crash modes).
- [ ] With no overrides, `_disruption_list_names` is identical to pre-change for a representative selector (regression-covered by unit test).
- [ ] Unit tests in `unit_tests/unit/nemesis/` cover: default keeps nemesis; reason prunes nemesis; exception in `precheck()` prunes and logs.
- [ ] `uv run sct.py pre-commit` passes.

---

### Phase 2: Empty-list handling and one-time Argus SKIPPED reporting

**Importance**: Critical
**Description**: Make pruning observable and fail-safe. Emit one `SKIPPED` Argus row per pruned nemesis, and fail the test if pruning empties the rotation.

**Dependencies**: Phase 1

**Deliverables**:
- For each pruned nemesis, publish a single `DisruptionEvent` marked skipped (via `DisruptionEvent.skip(reason)`, `sdcm/sct_events/continuous_event.py:134`) so it appears exactly once as `NemesisStatus.SKIPPED` in Argus (`sdcm/sct_events/argus.py:66`). Emitted at build time, not per cycle.
- Empty-rotation guard: if `disruptions_list` is empty after pruning, publish one `InfoEvent(..., severity=Severity.CRITICAL)` naming the excluded nemesis and their reasons, and stop the nemesis thread cleanly instead of letting `call_next_nemesis()` raise the bare `AssertionError` at `sdcm/nemesis/__init__.py:2085`. This supersedes the "skipped 3× in a row" static path (`sdcm/nemesis/__init__.py:440`) for prune-based cases.

**Adaptation Notes**: "Configured nemesis not running must fail the test" — the CRITICAL event is the failure signal. Decide whether to raise after publishing or rely on the event severity to fail the test summary; align with how existing CRITICAL nemesis events fail runs (`get_event_summary().get("CRITICAL")`, used at `sdcm/nemesis/__init__.py:1953`).

**Definition of Done**:
- [ ] Each pruned nemesis produces exactly one `SKIPPED` Argus row (verified in unit test by asserting one published skipped `DisruptionEvent` per pruned class).
- [ ] No per-cycle skip events are produced for pruned nemesis (they are absent from `infinite_cycle`).
- [ ] Empty rotation after pruning emits one CRITICAL event and stops the thread without an uncaught `AssertionError`.
- [ ] Unit tests cover: single pruned nemesis → one SKIPPED row; all pruned → one CRITICAL event + graceful stop.
- [ ] `uv run sct.py pre-commit` passes.

---

### Phase 3: Migrate Category 1 (config / backend / edition) skips

**Importance**: Important
**Description**: Move the 57 config/backend/edition guards from runner `disrupt_*` methods into the owning class `precheck()`, switching `self.target_node` references to `self.runner.cluster.nodes[0]`. Remove the migrated static guards from the runner methods in the same PR. Split into ≤200 LOC PRs grouped by nemesis family (e.g. SLA group, LDAP/auth group, network-interface group, K8s-only group, manager group).

**Dependencies**: Phase 1, Phase 2

**Deliverables**:
- `precheck()` overrides on the relevant classes in `sdcm/nemesis/monkey/` (and extracted modules), e.g. LDAP nemesis (`sdcm/nemesis/__init__.py:1117-1123`), SLA nemesis (`sdcm/nemesis/__init__.py:5084-5232`), KMS-encryption backend gate (`sdcm/nemesis/__init__.py:4474`), network-interface nemesis (`3557/3648/4016`).
- Removal of the corresponding `raise UnsupportedNemesis(...)` static guards from the runner methods.
- Per-family unit tests asserting the nemesis is pruned under the negative config and kept under the positive config.

**Adaptation Notes**: Some SLA methods mix a Category 1 gate (`sla`/`enterprise`/`authenticator`) with a Category 3 data-presence gate (`get_cassandra_stress_write_cmds()`). Only the Category 1 portion moves to `precheck()`; the data-presence check stays in `disrupt()`.

**Definition of Done**:
- [ ] All 57 Category 1 guards are evaluated via `precheck()` and removed from the per-cycle path.
- [ ] No remaining `target_node`-dependent reference inside any migrated `precheck()`.
- [ ] Unit tests per family (negative prunes, positive keeps).
- [ ] `uv run sct.py pre-commit` passes for each PR.

---

### Phase 4: Migrate Category 2 (version / feature flag / cluster-uniform node attribute) skips

**Importance**: Important
**Description**: Move the 28 version/feature/uniform-attribute guards into `precheck()`, using a representative node (`cluster.nodes[0]`) for cluster-wide probes. Convert the implicit `@scylla_versions` `MethodVersionNotFound` cases into explicit `precheck()` version checks where the method group is owned by a single nemesis. Split into ≤200 LOC PRs (e.g. tablets group, raft-coordinator group, version-compare group, `SkipPerIssues` group).

**Dependencies**: Phase 1, Phase 2

**Deliverables**:
- `precheck()` overrides for tablets-gated nemesis (`915-920`, `1090-1094`, `4242`, `5277`, `5334`, `5837`, `5921`), raft-coordinator nemesis (`5652`, `5723`, `5834`), version-compare nemesis (`1798`, `5409`, `5413`), the node OS/distro check in `disrupt_memory_stress` (`4768-4774`, evaluated via `cluster.nodes[0].distro`), and `SkipPerIssues`-gated nemesis.
- Removal of the corresponding static guards from runner methods; for `@scylla_versions`-decorated single-owner groups, add an explicit version `precheck()` and document that the decorator remains the in-method safety net.

**Adaptation Notes (Needs Investigation)**: `disrupt_create_index`, `disrupt_add_remove_mv`, `disrupt_kill_mv_building_coordinator`, and `disrupt_trigger_split_merge_tablets_with_alter` mix Category 2 feature gates with Category 3 table-existence / node-busy gates. Confirm per nemesis that only the feature/version portion is hoisted to `precheck()` and the dynamic portion remains in `disrupt()`. Verify that `is_views_with_tablets_enabled(session)` (`sdcm/nemesis/__init__.py:5841`) can be evaluated against a representative node session at build time.

**Definition of Done**:
- [ ] All 28 Category 2 guards evaluated via `precheck()`; static guards removed from the per-cycle path.
- [ ] Feature probes use a representative node, not a target node.
- [ ] Unit tests assert pruning under disabled feature/version and retention under enabled.
- [ ] `uv run sct.py pre-commit` passes for each PR.

---

### Phase 5: Documentation update

**Importance**: Critical
**Description**: Document the `precheck()` contract, the Category 1/2/3 rule, and the pruning/reporting behavior so future nemesis authors place static checks correctly.

**Dependencies**: Phase 1 (content can expand as Phases 3–4 land)

**Deliverables**:
- New "Pre-execution skip checks (`precheck`)" section in `docs/nemesis.md` covering the contract, the representative-node rule, the "Category 3 stays in `disrupt()`" rule, the one-time SKIPPED Argus row, and the empty-rotation CRITICAL behavior.
- Update the `writing-nemesis` skill (`skills/writing-nemesis/SKILL.md`) and AGENTS.md nemesis notes to mention `precheck()`.
- Update this plan's PR History table as phases merge.

**Definition of Done**:
- [ ] `docs/nemesis.md` documents `precheck()` with a before/after example.
- [ ] `writing-nemesis` skill references `precheck()` and the category rule.
- [ ] `uv run sct.py pre-commit` passes.

## Testing Requirements

### Unit Tests

- Use the existing nemesis test harness (`unit_tests/unit/nemesis/`, `TestRunner`, `fake_cluster.py`).
- Phase 1: default `precheck()` keeps nemesis; reason-returning `precheck()` is excluded from `build_disruptions_by_selector()`; an exception inside `precheck()` is caught and the nemesis excluded; build output is unchanged when no class overrides `precheck()`.
- Phase 2: a pruned nemesis publishes exactly one skipped `DisruptionEvent` (assert via `fake_events`); all-pruned emits one CRITICAL event and stops the thread without `AssertionError`.
- Phases 3–4: per migrated family, parametrized tests toggling the relevant config/flag and asserting prune-vs-keep, plus an assertion that the representative node (not `target_node`) is used.
- Run with: `uv run sct.py unit-tests -t unit_tests/unit/nemesis/`.

### Integration Tests

- Docker backend smoke run with a selector that is statically infeasible on Docker (e.g. a K8s-only nemesis) to confirm it is pruned once and the run proceeds:
  `uv run sct.py run-test longevity_test.LongevityTest.test_custom_time --backend docker --config test-cases/PR-provision-test.yaml`.
- Docker run with a selector resolving to a single statically-infeasible nemesis to confirm the empty-rotation CRITICAL path fails the test.
- Run with: `uv run sct.py integration-tests`.

### Manual Testing

- Longevity run on a multi-node cluster with `SisyphusMonkey` and a broad selector: confirm previously-skipping nemesis (e.g. tablets-incompatible) are absent from the post-build "List of Nemesis to execute" log (`sdcm/nemesis/__init__.py:2073`) and that no `check_cluster_health()` is invoked on their behalf.
- Confirm in Argus that each pruned nemesis appears exactly once as `SKIPPED`.
- Performance/large-cluster run: confirm elimination of repeated multi-minute health checks attributable to skipped nemesis.

## Success Criteria

All Definition of Done items across phases are met. Additionally:

1. For a representative broad-selector longevity run, the number of `check_cluster_health()` invocations attributable to statically-skipping nemesis drops to zero (the per-cycle wasted work for the 85 static skips is eliminated).
2. No statically-infeasible nemesis remains in `disruptions_list` after build, and each is reported once as `SKIPPED` in Argus.
3. A selector that resolves to only statically-infeasible nemesis fails the test via a single CRITICAL event.

## Risk Mitigation

### Risk: A `precheck()` mis-categorizes a dynamic condition as static
**Likelihood**: Medium
**Impact**: A nemesis is permanently pruned based on state that would have become valid later (e.g. data not yet loaded), reducing coverage silently.
**Mitigation**: The contract explicitly forbids dynamic-state and `target_node` checks in `precheck()`; code review enforces the Category 1/2/3 rule; migration PRs keep Category 3 checks in `disrupt()`. The taxonomy in this plan lists the exact line references for each category.

### Risk: Representative-node probe differs from target-node reality
**Likelihood**: Low
**Impact**: A feature/version probe against `cluster.nodes[0]` yields a different answer than the eventual target node.
**Mitigation**: All Category 2 conditions identified are cluster-wide (tablets, raft, views, version) or cluster-uniform by provisioning config (OS distro, fixed by the single `scylla_linux_distro` param at `sdcm/sct_config.py:671`). Phase 4 includes a DoD item and review gate rejecting any genuinely per-node-specific check in `precheck()`.

### Risk: Empty rotation false-positive fails a legitimately-configured run
**Likelihood**: Low
**Impact**: A valid test fails because all selected nemesis are (correctly) infeasible on that backend/config.
**Mitigation**: This is the desired behavior per the requirement ("configured nemesis not running must fail the test"). The CRITICAL event lists each exclusion reason so the misconfiguration is immediately diagnosable; selectors can be adjusted per backend.

### Risk: Removing the static guard from a runner method while another caller relies on it
**Likelihood**: Medium
**Impact**: A `disrupt_*` method still reachable from a legacy delegate could lose its guard and run where it should not.
**Mitigation**: Migrate and remove in the same PR; before removing a guard, `grep` for all callers of the runner method; keep guards that are shared by multiple nemesis until all owners have a `precheck()`. Phase 3/4 DoD requires per-family tests.

### Risk: Double reporting or missing the one-time SKIPPED row
**Likelihood**: Low
**Impact**: Pruned nemesis appear zero times or many times in Argus, breaking the audit trail.
**Mitigation**: Emit the skipped `DisruptionEvent` from the single build gate and assert "exactly once per pruned class" in unit tests (Phase 2 DoD).

## Related Plans

- [nemesis-rework.md](nemesis-rework.md) — Phase 3 of the rework extracts disrupt logic into self-contained classes; `precheck()` builds on that class-as-carrier model and is most cleanly applied to already-extracted nemesis.
- [nemesis-extraction.md](nemesis-extraction.md) — As methods are extracted, their static guards become natural `precheck()` overrides; sequencing migrations after extraction reduces churn.

## PR History

| Phase | PR | Status |
|-------|-----|--------|
| Phase 1 — `precheck()` hook + pruning | — | Not started |
| Phase 2 — Empty-list + Argus reporting | — | Not started |
| Phase 3 — Migrate Category 1 skips | — | Not started |
| Phase 4 — Migrate Category 2 skips | — | Not started |
| Phase 5 — Documentation | — | Not started |
