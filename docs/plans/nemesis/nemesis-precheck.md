---
status: in_progress
domain: nemesis
created: 2026-06-11
last_updated: 2026-06-25
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

The node OS/distro check in `disrupt_memory_stress` (`sdcm/nemesis/__init__.py:4768-4774`) is Category 2, not Category 3: although it reads `self.target_node.distro`, the distro is fixed at provisioning by a single `scylla_linux_distro` config param (`sdcm/sct_config.py:671`) and is uniform across the data-node pool, so the representative `node.distro` passed to `precheck(node)` yields the same answer for any target.

There is no mechanism today to evaluate a static skip **once** and exclude the nemesis from the rotation.

## Current State

### Nemesis class model

- `sdcm/nemesis/__init__.py:238` — `NemesisFlags`: boolean flags used only for selector filtering.
- `sdcm/nemesis/__init__.py:261` — `NemesisBaseClass(NemesisFlags, ABC)`: abstract base for individual disruptions. Holds `self.runner`, declares the abstract `disrupt()` (`sdcm/nemesis/__init__.py:269`). **No feasibility hook exists.**
- `sdcm/nemesis/__init__.py:276` — `NemesisRunner`: orchestrator. Builds and owns `self.disruptions_list` and the execution flow.

### Where the disruption list is built

- `sdcm/nemesis/__init__.py:2005` — `build_disruptions_by_selector()` instantiates each subclass returned by `NemesisRegistry.filter_subclasses(...)` and appends it to `disruptions`. It already swallows construction errors with a broad `except Exception` (`sdcm/nemesis/__init__.py:2023-2024`).
- `sdcm/nemesis/__init__.py:2027` — `build_disruptions_by_name()` routes through `build_disruptions_by_selector()`, so both selection styles share one gate.
- `sdcm/nemesis/monkey/runners.py:16` — `SisyphusMonkey.__init__` calls `build_disruptions_by_selector(self.nemesis_selector)` then `shuffle_list_of_disruptions(...)`. Other runners (`ScyllaCloudLimitedChaosMonkey`, `K8sSetMonkey`, etc.) call `build_disruptions_by_name([...])`.

### Where the precheck gate runs

- `sdcm/nemesis/__init__.py:443` — `NemesisRunner.run()` calls `precheck_nemesis()` once before the execution loop starts.
- `sdcm/nemesis/__init__.py:2054` — `precheck_nemesis()` passes a representative live node to `nemesis.precheck(node=...)`, removes excluded nemesis from `disruptions_list`, and reports each exclusion once.

### Execution path that the precheck will short-circuit

- `sdcm/nemesis/__init__.py:2078` — `infinite_cycle` is `itertools.cycle(self.disruptions_list)`; pruned nemesis simply never enter it.
- `sdcm/nemesis/__init__.py:2083` — `call_next_nemesis()` asserts `self.disruptions_list` is non-empty (`sdcm/nemesis/__init__.py:2085`) and runs `execute_nemesis(next(self.infinite_cycle))`.

### Timing window (verified)

- `sdcm/cluster.py:5780` — `add_nemesis()` instantiates each runner (which immediately builds its disruption list).
- `longevity_test.py:136` calls `add_nemesis(...)` **after** the cluster is initialized (`db_cluster.wait_for_init(...)` in `sdcm/tester.py:965-974`) but **before** data prepare/load and **before** `db_cluster.start_nemesis()` (`longevity_test.py:240`).
- Consequence at precheck time: the cluster is **up** (Category 2 version/feature probes work against the representative node passed to `precheck(node)`), config/backend is **known** (Category 1), and **no target node is selected** (Category 3 target-node state is not meaningful). This is exactly the right place for a one-time static check.

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
- A one-time evaluation/pruning step in `precheck_nemesis()` before the run loop. → **Phase 1**
- Graceful handling (a single CRITICAL event that fails the test) when every selected nemesis is pruned. → **Phase 2**
- A taxonomy-driven migration of the 85 static skips out of the per-cycle `disrupt()` path. → **Phases 3–4**
  - 57 Category 1 (config / backend / edition) guards → **Phase 3**
  - 28 Category 2 (version / feature flag / cluster-uniform node attribute) guards → **Phase 4**
- Documentation of the `precheck(node)` contract, the category rule, and the pruning/reporting behavior for future nemesis authors. → **Phase 5**

## Goals

1. **Add a return-based `precheck(node)` hook** to `NemesisBaseClass` that returns `str | None` (no exception-based control flow): `None` = runnable, a string = skip reason. Default implementation returns `None`.
2. **Evaluate `precheck(node)` exactly once** per nemesis before the execution loop starts, and permanently exclude any nemesis that returns a reason — so pruned nemesis incur **zero** per-cycle health-check, node-selection, or event cost.
3. **Migrate all 85 static skips** (Category 1 + Category 2) from runner `disrupt_*` methods into the owning nemesis class `precheck(node)`, removing the now-redundant static guards in the **same PR** as each migration. Category 3 (45) checks remain in `disrupt()`.
4. **Fail the test loudly when misconfigured**: if every selected nemesis is pruned, emit exactly one `Severity.CRITICAL` event and stop the nemesis thread (configured nemesis not running must fail the test).
5. **Report each pruned nemesis as a `SKIPPED` Argus row exactly once** (not per cycle, not zero times), preserving skip visibility while eliminating the repeated work.
6. **No behavioral regression** for nemesis with no static skip (default `precheck(node)` returns `None`; they run exactly as before).

## Implementation Phases

Phases are ordered by dependency: the framework hook and reporting/empty-list handling land first (foundational, must leave the tree working), then the bulk migration is split into reviewable, domain-grouped PRs of ≤200 LOC each. Migration PRs depend only on Phase 1/2 and are independent of one another.

### Phase 1: Add the `precheck(node)` hook and one-time pruning — Done

**Importance**: Critical
**Description**: Introduce the return-based hook and wire it into the one-time pre-execution gate. No nemesis overrides it yet, so behavior is unchanged (every `precheck(node)` returns `None`).

**Deliverables**:
- New method on `NemesisBaseClass` (`sdcm/nemesis/__init__.py:261`):
  ```python
  def precheck(self, node: BaseNode) -> str | None:
      """
      Static feasibility check, evaluated ONCE before the nemesis is scheduled.

      Return None  -> nemesis is runnable and kept in the rotation.
      Return a str -> human-readable reason; nemesis is permanently excluded.

      Use ONLY for conditions that do not change during the test:
        - test config / backend / product edition         (Category 1)
        - Scylla version / feature flags / cluster-uniform
          node attributes such as OS distro                (Category 2)

      No target node is selected yet — use the provided representative live node.
      Do NOT check dynamic state (data presence,
      live node counts, whether a specific target node is busy or alone in
      its rack) — those stay inside disrupt().
      """
      return None
  ```
- `NemesisRunner.precheck_nemesis()` (`sdcm/nemesis/__init__.py`) calls `precheck(node)` before the execution loop and keeps the nemesis only when it returns `None`. Real errors raised inside `precheck(node)` are caught and reported as failed prechecks. Excluded nemesis are collected with their reasons and logged once.

**Adaptation Notes**: A bare `str | None` contract is used (per design decision) rather than a structured result; the reason string is what flows to logs and the Argus row in Phase 2.

**Definition of Done**:
- [x] `NemesisBaseClass.precheck(node)` exists with the documented contract and `None` default.
- [x] `precheck_nemesis()` invokes `precheck(node)` before the execution loop and excludes those returning a reason.
- [x] Exceptions inside `precheck(node)` are handled and reported as failed prechecks (no new crash modes).
- [x] With no overrides, `_disruption_list_names` is identical to pre-change for a representative selector (regression-covered by unit test).
- [x] Unit tests in `unit_tests/unit/nemesis/` cover: default keeps nemesis; reason prunes nemesis; exception in `precheck(node)` prunes and logs; the passed node is the representative cluster node, not `target_node`.
- [x] `uv run sct.py pre-commit` passes for the changed files. Full commit-range pre-commit is blocked by unstaged working-tree edits during checkout.

---

### Phase 2: Empty-list handling and one-time Argus SKIPPED reporting

**Importance**: Critical
**Description**: Make pruning observable and fail-safe. Emit one `SKIPPED` Argus row per pruned nemesis, and fail the test if pruning empties the rotation.

**Dependencies**: Phase 1

**Deliverables**:
- For each pruned nemesis, publish a single `DisruptionEvent` marked skipped (via `DisruptionEvent.skip(reason)`, `sdcm/sct_events/continuous_event.py:134`) so it appears exactly once as `NemesisStatus.SKIPPED` in Argus (`sdcm/sct_events/argus.py:66`). Emitted at precheck time, not per cycle.
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
**Description**: Move the 57 config/backend/edition guards from runner `disrupt_*` methods into the owning class `precheck(node)`, switching `self.target_node` references to the provided representative `node`. Remove the migrated static guards from the runner methods in the same PR. Split into ≤200 LOC PRs grouped by nemesis family (e.g. SLA group, LDAP/auth group, network-interface group, K8s-only group, manager group).

**Dependencies**: Phase 1, Phase 2

**Deliverables**:
- `precheck(node)` overrides on the relevant classes in `sdcm/nemesis/monkey/` (and extracted modules), e.g. LDAP nemesis (`sdcm/nemesis/__init__.py:1117-1123`), SLA nemesis (`sdcm/nemesis/__init__.py:5084-5232`), KMS-encryption backend gate (`sdcm/nemesis/__init__.py:4474`), network-interface nemesis (`3557/3648/4016`).
- Removal of the corresponding `raise UnsupportedNemesis(...)` static guards from the runner methods.
- Per-family unit tests asserting the nemesis is pruned under the negative config and kept under the positive config.

**Adaptation Notes**: Some SLA methods mix a Category 1 gate (`sla`/`enterprise`/`authenticator`) with a Category 3 data-presence gate (`get_cassandra_stress_write_cmds()`). Only the Category 1 portion moves to `precheck(node)`; the data-presence check stays in `disrupt()`.

**Definition of Done**:
- [ ] All 57 Category 1 guards are evaluated via `precheck(node)` and removed from the per-cycle path.
- [ ] No remaining `target_node`-dependent reference inside any migrated `precheck(node)`.
- [ ] Unit tests per family (negative prunes, positive keeps).
- [ ] `uv run sct.py pre-commit` passes for each PR.

---

### Phase 4: Migrate Category 2 (version / feature flag / cluster-uniform node attribute) skips

**Importance**: Important
**Description**: Move the 28 version/feature/uniform-attribute guards into `precheck(node)`, using the provided representative node for cluster-wide probes. Convert the implicit `@scylla_versions` `MethodVersionNotFound` cases into explicit `precheck(node)` version checks where the method group is owned by a single nemesis. Split into ≤200 LOC PRs (e.g. tablets group, raft-coordinator group, version-compare group, `SkipPerIssues` group).

**Dependencies**: Phase 1, Phase 2

**Deliverables**:
- `precheck(node)` overrides for tablets-gated nemesis (`915-920`, `1090-1094`, `4242`, `5277`, `5334`, `5837`, `5921`), raft-coordinator nemesis (`5652`, `5723`, `5834`), version-compare nemesis (`1798`, `5409`, `5413`), the node OS/distro check in `disrupt_memory_stress` (`4768-4774`, evaluated via `node.distro`), and `SkipPerIssues`-gated nemesis.
- Removal of the corresponding static guards from runner methods; for `@scylla_versions`-decorated single-owner groups, add an explicit version `precheck(node)` and document that the decorator remains the in-method safety net.

**Adaptation Notes (Needs Investigation)**: `disrupt_create_index`, `disrupt_add_remove_mv`, `disrupt_kill_mv_building_coordinator`, and `disrupt_trigger_split_merge_tablets_with_alter` mix Category 2 feature gates with Category 3 table-existence / node-busy gates. Confirm per nemesis that only the feature/version portion is hoisted to `precheck(node)` and the dynamic portion remains in `disrupt()`. Verify that `is_views_with_tablets_enabled(session)` (`sdcm/nemesis/__init__.py:5841`) can be evaluated against a representative node session before the execution loop.

**Definition of Done**:
- [ ] All 28 Category 2 guards evaluated via `precheck(node)`; static guards removed from the per-cycle path.
- [ ] Feature probes use a representative node, not a target node.
- [ ] Unit tests assert pruning under disabled feature/version and retention under enabled.
- [ ] `uv run sct.py pre-commit` passes for each PR.

---

### Phase 5: Documentation update

**Importance**: Critical
**Description**: Document the `precheck(node)` contract, the Category 1/2/3 rule, and the pruning/reporting behavior so future nemesis authors place static checks correctly.

**Dependencies**: Phase 1 (content can expand as Phases 3–4 land)

**Deliverables**:
- New "Pre-execution skip checks (`precheck`)" section in `docs/nemesis.md` covering the `precheck(node)` contract, the representative-node rule, the "Category 3 stays in `disrupt()`" rule, the one-time SKIPPED Argus row, and the empty-rotation CRITICAL behavior.
- Update the `writing-nemesis` skill (`skills/writing-nemesis/SKILL.md`) and AGENTS.md nemesis notes to mention `precheck(node)`.
- Update this plan's PR History table as phases merge.

**Definition of Done**:
- [ ] `docs/nemesis.md` documents `precheck(node)` with a before/after example.
- [ ] `writing-nemesis` skill references `precheck(node)` and the category rule.
- [ ] `uv run sct.py pre-commit` passes.

## Testing Requirements

### Unit Tests

- Use the existing nemesis test harness (`unit_tests/unit/nemesis/`, `TestRunner`, `fake_cluster.py`).
- Phase 1: default `precheck(node)` keeps nemesis; reason-returning `precheck(node)` is excluded by `precheck_nemesis()`; an exception inside `precheck(node)` is caught and the nemesis excluded; build output is unchanged when no class overrides `precheck(node)`.
- Phase 2: a pruned nemesis publishes exactly one skipped `DisruptionEvent` (assert via `fake_events`); all-pruned emits one CRITICAL event and stops the thread without `AssertionError`.
- Phases 3–4: per migrated family, parametrized tests toggling the relevant config/flag and asserting prune-vs-keep, plus an assertion that the passed representative node (not `target_node`) is used.
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

### Risk: A `precheck(node)` mis-categorizes a dynamic condition as static
**Likelihood**: Medium
**Impact**: A nemesis is permanently pruned based on state that would have become valid later (e.g. data not yet loaded), reducing coverage silently.
**Mitigation**: The contract explicitly forbids dynamic-state and `target_node` checks in `precheck(node)`; code review enforces the Category 1/2/3 rule; migration PRs keep Category 3 checks in `disrupt()`. The taxonomy in this plan lists the exact line references for each category.

### Risk: Representative-node probe differs from target-node reality
**Likelihood**: Low
**Impact**: A feature/version probe against the representative node yields a different answer than the eventual target node.
**Mitigation**: All Category 2 conditions identified are cluster-wide (tablets, raft, views, version) or cluster-uniform by provisioning config (OS distro, fixed by the single `scylla_linux_distro` param at `sdcm/sct_config.py:671`). Phase 4 includes a DoD item and review gate rejecting any genuinely per-node-specific check in `precheck(node)`.

### Risk: Empty rotation false-positive fails a legitimately-configured run
**Likelihood**: Low
**Impact**: A valid test fails because all selected nemesis are (correctly) infeasible on that backend/config.
**Mitigation**: This is the desired behavior per the requirement ("configured nemesis not running must fail the test"). The CRITICAL event lists each exclusion reason so the misconfiguration is immediately diagnosable; selectors can be adjusted per backend.

### Risk: Removing the static guard from a runner method while another caller relies on it
**Likelihood**: Medium
**Impact**: A `disrupt_*` method still reachable from a legacy delegate could lose its guard and run where it should not.
**Mitigation**: Migrate and remove in the same PR; before removing a guard, `grep` for all callers of the runner method; keep guards that are shared by multiple nemesis until all owners have a `precheck(node)`. Phase 3/4 DoD requires per-family tests.

### Risk: Double reporting or missing the one-time SKIPPED row
**Likelihood**: Low
**Impact**: Pruned nemesis appear zero times or many times in Argus, breaking the audit trail.
**Mitigation**: Emit the skipped `DisruptionEvent` from the single build gate and assert "exactly once per pruned class" in unit tests (Phase 2 DoD).

## Related Plans

- [nemesis-rework.md](nemesis-rework.md) — Phase 3 of the rework extracts disrupt logic into self-contained classes; `precheck(node)` builds on that class-as-carrier model and is most cleanly applied to already-extracted nemesis.
- [nemesis-extraction.md](nemesis-extraction.md) — As methods are extracted, their static guards become natural `precheck(node)` overrides; sequencing migrations after extraction reduces churn.

## Commit Plan

The foundation (Phases 1–2 + 5 of this plan — the `precheck(node)` hook, one-time
pruning/reporting, and documentation) ships as **one PR of 3 commits**. Each
commit is self-standing: all tests in `unit_tests/unit/nemesis/` pass after every
individual commit.

Verification after every commit:
```bash
uv run python -m pytest unit_tests/unit/nemesis/
uv run sct.py pre-commit
```

> **Design note.** Pruning is performed by a dedicated `NemesisRunner.precheck_nemesis()`
> method invoked **once at the start of `run()`**, before the execution loop — *not*
> inside `build_disruptions_by_selector()`. This keeps `build_disruptions_by_selector()`
> a pure builder (it still returns a plain list), avoids changing every caller's
> signature, and means runners that legitimately keep an empty `disruptions_list`
> and override `call_next_nemesis()` (`NoOpMonkey`, `ManagerRcloneBackup`,
> `ManagerNativeBackup`, `CategoricalMonkey`) are unaffected.

### Commit 1 — `feat(nemesis): add precheck(node) hook to NemesisBaseClass`

Files: `sdcm/nemesis/__init__.py`

Add `precheck(self, node: BaseNode) -> str | None` to `NemesisBaseClass` with the
full contract docstring and a `return None` default. No other changes — zero
behavioral impact, every existing test passes.

### Commit 2 — `feat(nemesis): prune infeasible nemesis via precheck_nemesis() before the run loop`

Files: `sdcm/nemesis/__init__.py`, `unit_tests/unit/nemesis/__init__.py`,
`unit_tests/unit/nemesis/execute_nemesis/__init__.py`,
`unit_tests/unit/nemesis/execute_nemesis/test_precheck.py` (new)

- Add `NemesisRunner.precheck_nemesis()`. It iterates `self.disruptions_list`,
  evaluates each nemesis's `precheck(node)`, prunes the infeasible ones in place, and
  returns the `(name, reason)` exclusions. Each exclusion is reported once:
  - a returned reason → `DisruptionEvent.skip()` (SKIPPED, NORMAL) +
    Argus `finalize_nemesis(status=SKIPPED)`.
  - a raised exception → `DisruptionEvent.add_simple_error()` (FAILED, ERROR) +
    Argus `finalize_nemesis(status=FAILED)` — a broken `precheck(node)` is a test
    error, not an intentional skip.
- Argus reporting reuses `argus_submit()`, which gains an optional `node`
  parameter so pre-execution callers can reuse the same representative node
  passed to `precheck(node)` (no `target_node` exists yet). Existing call-sites are
  unchanged (default `node=None` → `self.target_node`).
- `run()` calls `precheck_nemesis()` before the loop; if exclusions emptied the
  rotation it publishes one `Severity.CRITICAL` `InfoEvent` naming every excluded
  nemesis and returns.
- Test infrastructure (shipped here to keep the suite green — `run()` now calls
  `precheck(node)` on test nemesis): add a `precheck(node)` stub to `TestBaseClass` and
  `TestExecuteBaseClass`, plus `PrecheckSkipNemesis` / `PrecheckErrorNemesis`.
- `execute_nemesis/test_precheck.py` covers pruning, DisruptionEvent shapes,
  Argus submit/finalize, the representative node argument, and the `run()` empty-
  rotation path. Skip-vs-exception variants are parametrized; full event dicts
  are asserted.

### Commit 3 — `docs(nemesis): document precheck(node) contract and pruning behavior`

Files: `docs/nemesis.md`, `skills/writing-nemesis/SKILL.md`, `AGENTS.md`,
`docs/plans/nemesis/nemesis-precheck.md`, `docs/plans/progress.json`,
`docs/plans/assets/progress-roadmap.svg`

Add a "Pre-execution skip checks (`precheck`)" section to `docs/nemesis.md`
(`precheck(node)` contract, what-belongs-where rule in plain language, representative-node rule,
SKIPPED/FAILED reporting, empty-rotation CRITICAL, before/after example). Update
the `writing-nemesis` skill and the AGENTS.md nemesis section. Advance plan
status to `in_progress`.

### Future work — migrating the 85 static skips (Phases 3–4)

Migrating the 57 config/backend and 28 version/feature guards out of the
`disrupt_*` methods into `precheck(node)` overrides (Implementation Phases 3 and 4
above) is **not** part of this foundation PR. Each migration adds a `precheck(node)`
override to a nemesis class and removes the matching `raise UnsupportedNemesis`
guard, grouped by nemesis family into ≤200 LOC PRs, using the `precheck_nemesis()`
machinery landed here.

## PR History

| Phase | PR | Status |
|-------|-----|--------|
| Phase 1 — `precheck(node)` hook + pruning | current PR | Done |
| Phase 2 — Empty-list + Argus reporting | — | Not started |
| Phase 3 — Migrate Category 1 skips | — | Not started |
| Phase 4 — Migrate Category 2 skips | — | Not started |
| Phase 5 — Documentation | — | Not started |
