---
status: in_progress
domain: ci-cd
created: 2026-09-11
last_updated: 2026-09-11
owner: aleksbykov
---

# Strong Consistency Performance Tests with Topology Operations

Jira: [SCYLLADB-4346](https://scylladb.atlassian.net/browse/SCYLLADB-4346) — *Performance tests with topology operations*

## Problem Statement

Strong Consistency (SC) performance in SCT is measured today only under a **steady, undisturbed cluster**: the
`PerformanceRegressionPredefinedStepsTest` gradual-throughput jobs grow the load in throttle steps against a
3-node cluster that never changes shape. The parent Jira asks for the missing half of the picture — SC latency
**while topology changes** (grow / terminate+replace / shrink) are in flight, using the Rust driver's
leader-aware tablet routing.

Concrete gaps today:

1. **No SC job exercises topology operations.** All 8 SC/EC pipelines under
   `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/` use
   `performance_regression_gradual_grow_throughput.PerformanceRegressionPredefinedStepsTest`, which never starts
   a nemesis (`nemesis_add_node_cnt: 0` in the base test-case).
2. **The existing latency-during-operations job cannot run cql-stress.** `perf-regression-latency-650gb-with-nemesis.yaml`
   is hard-wired to `cassandra-stress`, which has no leader-aware load balancing policy, so SC writes are routed
   to a non-leader replica and the measurement is dominated by the extra hop.
3. **The framework kills cql-stress load as a test failure.** `PerformanceRegressionTest._stop_load_when_nemesis_threads_end()`
   (`performance_regression_test.py:203`) downgrades only `CassandraStressEvent` before calling
   `self.loaders.kill_stress_thread()`. A cql-stress run killed the same way raises a **CRITICAL**
   `CqlStressCassandraStressEvent.failure` and fails the job at the very end of an ~14 h run.
4. **No arm64 (i8g) coverage for SC at all.** SC jobs exist only for the x86 `i4i.4xlarge` shape.

Measurable pain: an SC latency-during-topology-operations number does not exist for either i4i or i8g, and the
first attempt to produce one with cql-stress would fail in step 3 after burning a full test run.

## Current State

### Test classes

| File | Class / method | Role |
|------|----------------|------|
| `performance_regression_test.py:674-693` | `test_latency_read_with_nemesis`, `test_latency_write_with_nemesis`, `test_latency_mixed_with_nemesis` | Sub-tests used by the `-with-nemesis` pipelines |
| `performance_regression_test.py:285-303` | `PerformanceRegressionTest.run_workload()` | Starts load on **all** loaders, sleeps `nemesis_interval`, then runs the nemesis for exactly 1 cycle |
| `performance_regression_test.py:203-212` | `_stop_load_when_nemesis_threads_end()` | Joins nemesis threads, filters `CassandraStressEvent` to NORMAL, kills stress |
| `performance_regression_test.py:214-263` | `preload_data()` | Calls `_pre_create_keyspace()` when `pre_create_keyspace` is set, then runs `prepare_write_cmd` round-robin |
| `sdcm/nemesis/__init__.py:4144-4168` | `disrupt_run_unique_sequence()` | The `NemesisSequence` body: manager repair → `_grow_cluster` → `_terminate_and_replace_node` → `_shrink_cluster` |

### Stress-tool plumbing

| File | Fact |
|------|------|
| `sdcm/tester.py:3001` | `run_stress_thread()` dispatches on the literal `cql-stress-cassandra-stress` **before** the `cassandra-stress` branch, so a cql-stress command in `stress_cmd_w/r/m` is routed correctly with no code change |
| `sdcm/cql_stress_cassandra_stress_thread.py:62` | Image comes from `stress_image.cql-stress-cassandra-stress` |
| `sdcm/cql_stress_cassandra_stress_thread.py:111-117` | `-pop seq=x..y` is rewritten to `dist=SEQ(x..y)`; `n=FIXED(k)` is rewritten to `n=k` |
| `sdcm/cql_stress_cassandra_stress_thread.py:148` | HDR file is `hdrh-cscs-*.hdr`; the collector pattern in `sdcm/utils/hdrhistogram.py:141` is `*/hdrh-*.hdr`, so HDR latency collection already works |
| `sdcm/stress_thread.py:135-150` | `set_hdr_tags()` is inherited by `CqlStressCassandraStressThread`; `fixed=` rate produces the `-rt` tags the nemesis latency decorator expects |
| `sdcm/sct_events/loaders.py:78,87` | `CassandraStressEvent` and `CqlStressCassandraStressEvent` are **siblings**, both derived from `StressEvent` — this is why gap 3 above exists |
| `sdcm/utils/loader_utils.py:203-241` | `_run_all_stress_cmds()` runs each command without `round_robin`, i.e. **each command runs on every loader** |
| `performance_regression_gradual_grow_throughput.py:555` | Confirms the project convention: a throttle step is an **aggregate** rate, divided by `num_loaders` before it reaches the command |

### Existing configuration building blocks (all verified to exist)

| File | Content |
|------|---------|
| `test-cases/performance/perf-regression-latency-650gb-with-nemesis.yaml` | Base: 650 M rows, `SisyphusMonkey` + `nemesis_selector: 'NemesisSequence'`, `nemesis_interval: 30`, 3 db nodes / 4 loaders, `i3en.2xlarge` db + `c5.2xlarge` loaders, `use_hdrhistogram: true` |
| `configurations/strong_consistency/enable_experimental_sc.yaml` | `strongly-consistent-tables` feature, `--blocked-reactor-notify-ms 50`, `api_address: "0.0.0.0"` (needed by the leader-aware driver) |
| `configurations/strong_consistency/enable_commitlog_sync_batch.yaml` | `commitlog_sync: batch`, `commitlog_sync_batch_window_in_ms: 100` |
| `configurations/stress_images/cql-stress-strong-consistency-leader-awarness.yaml` | `aleksbykov/cql-stress:leader-aware-strong-consistency` |
| `configurations/strong_consistency/prepare_cql_stress_ks_with_sc_1kpershard.yaml` | SC keyspace (`consistency = 'global'`, tablets) + 4 cql-stress commands with `-mode connectionsPerShard=250` |
| `configurations/strong_consistency/prepare_cql_stress_ks_with_ec_1kpershard.yaml` | EC twin of the above (no `consistency` clause) |
| `configurations/aws/i4i_4xlarge.yaml` | `instance_type_db: 'i4i.4xlarge'` |
| `configurations/arm_instance_types/i8g_4xlarge.yaml` | `instance_type_db: 'i8g.4xlarge'` |
| `configurations/performance/latency-decorator-error-thresholds-nemesis-ent-tablets.yaml` | Per-disruption duration limits for write/read/mixed |
| `configurations/disable_kms.yaml`, `configurations/disable_speculative_retry.yaml` | Attached by every SC job |

### Config-merge semantics (verified in `sdcm/sct_config.py:2924-2934` + `:475`)

Config files are merged with `anyconfig.MS_DICTS`, so **dict** options (`append_scylla_yaml`) from several
fragments are merged key-by-key, while **scalar/string** options (`append_scylla_args`, `instance_type_*`) are
replaced by the last file in the list. Fragment order in the jenkinsfile therefore matters and is part of the
deliverable of each pipeline phase.

### Reference job being adapted

`scylla-enterprise-perf-regression-predefined-throughput-steps-cql-stress-write-unthrottle-tablets-sc.jenkinsfile`
(the "write-50k-unthrottled-commitlog-batch-tablets-sc" job) chains:
base steps test-case → `cassandra_stress_gradual_load_steps_sc_50k_unthrottled_only_write.yaml` → `disable_kms`
→ `disable_speculative_retry` → `latency-decorator-error-thresholds-steps-ent-tablets` → `enable_experimental_sc`
→ `enable_commitlog_sync_batch` → cql-stress image → `prepare_cql_stress_ks_with_sc_1kpershard`.
Its throttled step is **50 000 ops/s aggregate**, which is the anchor for the steady rate used here.

## Goals

1. **SC latency-during-topology-operations is measurable on i4i.** A single Jenkins job runs
   `test_latency_write_with_nemesis`, `test_latency_read_with_nemesis` and `test_latency_mixed_with_nemesis`
   against an SC keyspace with cql-stress leader-aware routing and reports per-disruption P90/P99 to Argus.
2. **Same coverage on i8g (arm64).**
3. **EC baselines exist for both shapes**, differing from the SC job only by the keyspace `consistency` clause
   and the SC feature flag, so an SC-vs-EC delta can be computed per disruption.
4. **1000 connections per shard in aggregate** — `-mode connectionsPerShard=250` × 4 loaders, matching the
   existing `*_1kpershard.yaml` convention.
5. **A cql-stress load killed at the end of the nemesis sequence does not fail the test** — no CRITICAL
   `CqlStressCassandraStressEvent` in a passing run.
6. **Each phase below lands as at least one commit** mapped 1:1 to a SCYLLADB-4346 sub-task.

## Implementation Phases

Phases 1-4 are prerequisites; 5-10 are the deliverable jobs; 11-14 are hardening and documentation.

---

### Phase 1: Stop cql-stress load cleanly when the nemesis sequence ends

**Importance**: Critical
**Description**: Teach `_stop_load_when_nemesis_threads_end()` to downgrade the stress event class that matches
the load actually running. Today it hard-codes `CassandraStressEvent`; a cql-stress workload killed by
`kill_stress_thread()` raises a CRITICAL `CqlStressCassandraStressEvent.failure` and fails the run.

**Deliverables**:
- `performance_regression_test.py`: select the severity-filter event class(es) from `self.stress_cmd`
  (or filter both `CassandraStressEvent` and `CqlStressCassandraStressEvent` via nested
  `EventsSeverityChangerFilter` contexts).
- `unit_tests/unit/` test asserting the filter set chosen for a cassandra-stress command and for a
  cql-stress command.

**Definition of Done**:
- [ ] A cql-stress `stress_cmd_w` produces a filter that covers `CqlStressCassandraStressEvent`
- [ ] Existing cassandra-stress behaviour is unchanged (same filter as today)
- [ ] New unit test passes; `uv run sct.py pre-commit` clean

---

### Phase 2: Shared base fragment for the 650 GB cql-stress nemesis runs

**Importance**: Critical
**Description**: Add `configurations/strong_consistency/nemesis_650gb_cql_stress_base.yaml` holding everything
that is common to all four new jobs and independent of consistency mode: loader shape sized for 250
connections/shard (`instance_type_loader: 'c7i.8xlarge'`, `n_loaders: 4`), `use_prepared_loaders: false`,
`round_robin: true`, and the `email_subject_postfix` / `user_prefix` identifying the new job family.

**Dependencies**: none (can land in parallel with Phase 1)

**Deliverables**:
- New config fragment, applied **after** the base test-case so it overrides `c5.2xlarge`.

**Definition of Done**:
- [ ] Fragment contains no consistency-specific or arch-specific option
- [ ] `instance_type_loader` resolves to `c7i.8xlarge` in the resolved config for the i4i chain
- [ ] Rationale for the loader shape (15 shards × 250 conns × 4 loaders) recorded as a YAML comment

---

### Phase 3: SC keyspace + cql-stress workload fragment (1000 conns/shard aggregate)

**Importance**: Critical
**Description**: Add `configurations/strong_consistency/prepare_cql_stress_nemesis_ks_with_sc_1kpershard.yaml`:
SC keyspace pre-creation plus `prepare_write_cmd` and the three steady-state workloads rewritten for
`cql-stress-cassandra-stress` against the 650 GB dataset.

**Dependencies**: Phase 2

**Deliverables**:
- `pre_create_keyspace`: `CREATE KEYSPACE keyspace1 ... consistency = 'global' and tablets = {'enabled': true}`
  (matching the wording already used by `prepare_cql_stress_ks_with_sc_1kpershard.yaml`).
- `prepare_write_cmd`: 4 × `cql-stress-cassandra-stress write cl=QUORUM n=162500001 ... -pop seq=<quarter>`
  (650 M rows × 1 KB ≈ 650 GB), one per loader via `round_robin`.
- `stress_cmd_w` / `stress_cmd_r` / `stress_cmd_m`: single commands (each runs on all 4 loaders) with
  `cl=QUORUM`, `-mode connectionsPerShard=250 cql3 native`, `-rate 'threads=<N> fixed=<per-loader>/s'`,
  `-col 'size=FIXED(1024) n=FIXED(1)'`, `-pop 'dist=gauss(1..650000000,325000000,9750000)'`.
- Starting rates, expressed as aggregate-÷-4: **write 50 000/s → `fixed=12500/s`** (anchored on the SC
  50k throttled step of the reference job); **read and mixed** start from the EC baseline rates of
  `perf-regression-latency-650gb-with-nemesis.yaml` (41 240/s and 35 000/s aggregate →
  `fixed=10310/s` / `fixed=8750/s`) — see Phase 14 for calibration.

**Verified against upstream cql-stress** (scylladb/cql-stress, master):
- `-pop 'dist=...'` is supported (`settings/option/population.rs`), and `GAUSSIAN(min..max,mean,stdev)` accepts
  the aliases `GAUSS` / `NORMAL` / `NORM` (`java_generate/distribution/normal.rs`), so
  `dist=gauss(1..650000000,325000000,9750000)` is valid.
- `-mode connectionsPerShard=` exists and takes precedence over `connectionsPerHost=` (`settings/option/mode.rs`).
- `-rate ... fixed=` is supported (`settings/option/rate.rs`), so coordinated-omission-corrected latency works.

**Needs Investigation**:
- Thread counts per workload for cql-stress at 250 conns/shard (the gradual SC job uses 500 write / 620 read
  threads per loader as a starting point).

**Definition of Done**:
- [ ] Aggregate connections per shard = 1000 (250 × 4 loaders) documented in a YAML comment
- [ ] All commands use `cl=QUORUM` (the leader-aware driver requires it — cf. commit `1505752b6`)
- [ ] Resolved config for the SC chain contains no `cassandra-stress` command in `prepare_write_cmd`,
      `stress_cmd_w`, `stress_cmd_r`, `stress_cmd_m`

---

### Phase 4: EC baseline workload fragment

**Importance**: Critical
**Description**: Add `configurations/strong_consistency/prepare_cql_stress_nemesis_ks_with_ec_1kpershard.yaml`
— byte-identical to Phase 3 except the keyspace is created without the `consistency = 'global'` clause.

**Dependencies**: Phase 3

**Deliverables**:
- EC fragment; a `diff` against the SC fragment shows only the `pre_create_keyspace` line.

**Decision needed from reviewer**: the existing EC steps job also drops `enable_commitlog_sync_batch.yaml` and
`enable_experimental_sc.yaml`, which silently changes `append_scylla_args`
(`--blocked-reactor-notify-ms 50` → `5`) and durability settings, so the EC/SC delta is not isolated to
consistency alone (noted in commit `172372628`). **Recommendation: keep `enable_commitlog_sync_batch.yaml` in
the EC chain** and add a tiny fragment pinning the same `append_scylla_args`, so the only variable is the
keyspace clause. Confirm before Phase 8.

**Definition of Done**:
- [ ] `diff` between SC and EC fragments is limited to `pre_create_keyspace`
- [ ] The isolation decision above is recorded in the fragment header comment

---

### Phase 5: SC latency-thresholds fragment for topology operations

**Importance**: Important
**Description**: Add `configurations/performance/latency-decorator-error-thresholds-nemesis-sc-tablets.yaml`.
SC writes go through Raft, so `add_new_nodes` / `decommission_nodes` / `replace_node` durations and the
latency error thresholds from the EC file are not transferable.

**Dependencies**: Phase 3

**Deliverables**:
- Fragment modelled on `latency-decorator-error-thresholds-nemesis-ent-tablets.yaml` with SC-appropriate
  `fixed_limit` values for write/read/mixed.

**Needs Investigation**: the SC limits themselves. First runs use the EC values with `fixed_limit: null`
(report-only) for the topology disruptions; real numbers land in Phase 14.

**Definition of Done**:
- [ ] Fragment covers `add_new_nodes`, `decommission_nodes`, `replace_node`, `terminate_node`, `_mgmt_repair_cli`
      for all three workloads
- [ ] Values that are placeholders are marked as such in comments

---

### Phase 6: i4i SC pipeline

**Importance**: Critical
**Description**: Add
`jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-latency-650gb-with-nemesis-cql-stress-tablets-sc.jenkinsfile`.

**Dependencies**: Phases 1-5

**Deliverables**: `perfRegressionParallelPipeline` with
`test_name: "performance_regression_test.PerformanceRegressionTest"`,
`sub_tests: ["test_latency_write_with_nemesis", "test_latency_read_with_nemesis", "test_latency_mixed_with_nemesis"]`,
`test_email_title: "SC latency during topology operations / tablets"`, and this config chain **in order**:

1. `test-cases/performance/perf-regression-latency-650gb-with-nemesis.yaml`
2. `configurations/disable_kms.yaml`
3. `configurations/disable_speculative_retry.yaml`
4. `configurations/performance/latency-decorator-error-thresholds-nemesis-sc-tablets.yaml`
5. `configurations/aws/i4i_4xlarge.yaml`
6. `configurations/strong_consistency/nemesis_650gb_cql_stress_base.yaml`
7. `configurations/strong_consistency/enable_experimental_sc.yaml`
8. `configurations/strong_consistency/enable_commitlog_sync_batch.yaml`
9. `configurations/stress_images/cql-stress-strong-consistency-leader-awarness.yaml`
10. `configurations/strong_consistency/prepare_cql_stress_nemesis_ks_with_sc_1kpershard.yaml`

**Definition of Done**:
- [ ] Resolved config shows `experimental_features: [strongly-consistent-tables]`, `api_address: "0.0.0.0"`,
      `commitlog_sync: batch` **all present together** (MS_DICTS merge of `append_scylla_yaml`)
- [ ] `instance_type_db: i4i.4xlarge`, `instance_type_loader: c7i.8xlarge`, `n_loaders: 4`
- [ ] `stress_image.cql-stress-cassandra-stress` = the leader-aware image
- [ ] `nemesis_class_name: SisyphusMonkey`, `nemesis_selector: NemesisSequence` inherited from the base test-case

---

### Phase 7: i8g (arm64) SC pipeline

**Importance**: Critical
**Description**: Same as Phase 6 with `configurations/arm_instance_types/i8g_4xlarge.yaml` replacing the i4i
fragment at position 5.

**Dependencies**: Phase 6

**Needs Investigation**: **does `aleksbykov/cql-stress:leader-aware-strong-consistency` have an arm64 manifest?**
The loaders stay x86 (`c7i.8xlarge`) so the stress container itself runs on x86 and this is likely a non-issue —
but `configurations/c-s-driver-version-4.yaml`, attached by the existing i8g nemesis job for cassandra-stress,
must **not** be carried over, since it is meaningless for cql-stress. Confirm before merging.

**Definition of Done**:
- [ ] `instance_type_db: i8g.4xlarge` in the resolved config
- [ ] No cassandra-stress-only fragment in the chain
- [ ] Arm image question answered and recorded in the plan

---

### Phase 8: i4i EC baseline pipeline

**Importance**: Important
**Description**: `...-latency-650gb-with-nemesis-cql-stress-tablets-ec.jenkinsfile` — Phase 6's chain with the
EC fragment from Phase 4 and the SC feature flag handled per the Phase 4 decision.

**Dependencies**: Phases 4, 6

**Definition of Done**:
- [ ] Resolved config has **no** `strongly-consistent-tables` feature
- [ ] Everything else (instance types, image, rates, thresholds) identical to the i4i SC job
- [ ] A documented diff of the two resolved configs is attached to the PR

---

### Phase 9: i8g EC baseline pipeline

**Importance**: Important
**Description**: EC twin of Phase 7.

**Dependencies**: Phases 7, 8

**Definition of Done**:
- [ ] Resolved config differs from the i8g SC job only in the SC-specific options
- [ ] `instance_type_db: i8g.4xlarge`

---

### Phase 10: `test_metadata` for the new pipelines

**Importance**: Important
**Description**: The base `perf-regression-latency-650gb-with-nemesis.yaml` carries no `test_metadata`. Add one
covering the new job family (description, tier, `test_type: performance`, `stress_tools: [cql-stress-cassandra-stress]`,
`nemesis_labels` for the topology disruptions, `features` incl. strongly-consistent tables and tablets), following
the `reviewing-pipeline-docs` skill.

**Dependencies**: Phases 6-9

**Definition of Done**:
- [ ] `uv run sct.py lint-test-docs` (or the documented equivalent) reports no gap for the new/edited files
- [ ] Metadata cross-checked against the actual nemesis sequence (`grow` / `terminate+replace` / `shrink` / manager repair)

---

### Phase 11: Config-chain regression test

**Importance**: Important
**Description**: Add a unit test that resolves each of the four new jenkinsfile config chains through
`SCTConfiguration` and asserts the invariants that silent merge-order bugs would break: SC flag present/absent,
`api_address` **and** `commitlog_sync` both surviving the `append_scylla_yaml` merge, `connectionsPerShard=250`
in every stress command, leader-aware image, correct instance types.

**Dependencies**: Phases 6-9

**Deliverables**:
- Parametrized test in `unit_tests/` over the four pipelines.

**Definition of Done**:
- [ ] Test fails if a fragment is reordered so that `append_scylla_args` or `api_address` is lost
- [ ] Test runs offline (no cloud credentials)

---

### Phase 12: Documentation

**Importance**: Important
**Description**: Extend `docs/strong-consistency-performance-tests-guide.md` with a
"Topology operations" section: the new job family, the config chain, what `NemesisSequence` actually does,
the 1000-connections-per-shard convention, and how to read the per-disruption latency in Argus. Fix the stale
statement in the guide that SC keyspaces use `consistency = 'local'` (the configs use `'global'`).

**Dependencies**: Phases 6-9

**Definition of Done**:
- [ ] Guide lists all four new pipelines with their config chains
- [ ] `consistency = 'local'` / `'global'` discrepancy resolved against the actual YAML
- [ ] Plan registered in `docs/plans/MASTER.md` and `docs/plans/progress.json`

---

### Phase 13: Dry-run validation of the four chains

**Importance**: Critical
**Description**: Resolve every new chain locally (config-only, no provisioning) and fix whatever the resolver
rejects — unknown options, type errors, `append_*` conflicts.

**Dependencies**: Phases 6-9

**Definition of Done**:
- [ ] All four chains resolve with no error
- [ ] `uv run sct.py pre-commit` clean on the whole branch
- [ ] Resolved-config excerpts pasted into the PR description

---

### Phase 14: Rate and threshold calibration after the first run

**Importance**: Important
**Description**: After the first i4i SC run (executed by the owner, not by CI), replace the placeholder
`fixed=` rates and the report-only latency thresholds with values derived from the measured steady-state
throughput, and re-check that the steady phase is not saturating the cluster before the nemesis starts.

**Dependencies**: first successful run of Phase 6's job

**Definition of Done**:
- [ ] Steady-state (pre-nemesis) load is between 50 % and 70 % of the measured SC unthrottled throughput
- [ ] `latency-decorator-error-thresholds-nemesis-sc-tablets.yaml` has real `fixed_limit` values
- [ ] Argus run linked from SCYLLADB-4346

## Testing Requirements

### Unit tests
- Phase 1: event-filter selection for cassandra-stress vs cql-stress commands.
- Phase 11: config-chain resolution invariants for all four pipelines.

### Integration tests
- None new. The existing `unit_tests/integration/test_cql_stress_cassandra_stress_thread.py` already covers the
  cql-stress thread against a docker Scylla; no thread-level change is introduced by this plan.

### Manual testing (owner-run, not CI)
1. i4i SC job, `test_latency_write_with_nemesis` only, to validate the end-to-end chain cheaply.
2. Full i4i SC job (all three sub-tests) — confirms Phase 1 (no CRITICAL cql-stress event at teardown).
3. i8g SC job.
4. EC baselines for both shapes.
5. Verify in Argus that per-disruption P90/P99 rows appear for `add_new_nodes`, `replace_node`,
   `decommission_nodes`.

### Performance-measurement acceptance
- HDR histogram files `hdrh-cscs-*.hdr` are collected from every loader.
- The steady-state segment before the first disruption is long enough (`nemesis_interval: 30` min) to give a
  clean baseline.

## Success Criteria

All phase DoD items are checked, plus:

1. Four jenkinsfiles exist and each resolves cleanly (Phase 13).
2. One full SC run per shape completes without a CRITICAL stress event (Phases 1, 6, 7).
3. An SC-vs-EC per-disruption latency delta can be computed for i4i and i8g (Phases 8, 9).
4. Every phase corresponds to a SCYLLADB-4346 sub-task with at least one commit.

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Topology operations are unsupported/broken on strongly-consistent keyspaces | Medium | High — job fails mid-sequence | Run the write-only sub-test first; a failure here is a genuine SCYLLADB finding, so capture the node logs and file it rather than working around it |
| `dist=gauss(...)` unsupported by cql-stress | Medium | Medium — no load at all | Verified fallback to `dist=UNIFORM(...)` / `seq=`; decided in Phase 3 before any run |
| `append_scylla_args` clobbered by fragment order (string option, last wins) | High if unchecked | Medium — wrong reactor-stall threshold | Phase 11 asserts the resolved value; fragment order fixed in the jenkinsfile |
| Leader-aware driver cannot reach the REST API | Low | High — routing silently degrades to non-leader | `enable_experimental_sc.yaml` sets `api_address: "0.0.0.0"`; Phase 11 asserts it survives the `append_scylla_yaml` merge |
| Loader saturation at 250 conns/shard on `c7i.8xlarge` | Medium | Medium — latency measures the loader, not Scylla | Check loader CPU in the first run (Phase 14); scale `n_loaders` or threads rather than connections, which are fixed by the requirement |
| ~14 h per job × 4 jobs of AWS cost | High | Medium | Land i4i SC first, calibrate, then roll out the remaining three |
| Custom cql-stress image is a personal Docker Hub tag | Medium | Medium — image may disappear or drift | Record the exact digest in the PR; follow-up ticket to move it to the ScyllaDB registry |

## Tracking

One row per phase = one SCYLLADB-4346 sub-task = at least one commit. Updated as implementation proceeds.

| # | Phase | Sub-task | Commit(s) | Status |
|---|-------|----------|-----------|--------|
| 1 | Stop cql-stress load cleanly when nemesis ends | [SCYLLADB-4404](https://scylladb.atlassian.net/browse/SCYLLADB-4404) | `8efd6ab2f` | done |
| 2 | Shared base fragment for 650 GB cql-stress nemesis runs | [SCYLLADB-4405](https://scylladb.atlassian.net/browse/SCYLLADB-4405) | `4aa785562` | done |
| 3 | SC keyspace + cql-stress workload fragment | [SCYLLADB-4406](https://scylladb.atlassian.net/browse/SCYLLADB-4406) | `6fb78b2a1` | done |
| 4 | EC baseline workload fragment | [SCYLLADB-4407](https://scylladb.atlassian.net/browse/SCYLLADB-4407) | `1c12c1558` | done |
| 5 | SC latency-thresholds fragment | [SCYLLADB-4408](https://scylladb.atlassian.net/browse/SCYLLADB-4408) | `486797d3f` | done |
| 6 | i4i SC pipeline | [SCYLLADB-4409](https://scylladb.atlassian.net/browse/SCYLLADB-4409) | `cd45ec3d1` | done |
| 7 | i8g SC pipeline | [SCYLLADB-4410](https://scylladb.atlassian.net/browse/SCYLLADB-4410) | `6578dcaed` | done |
| 8 | i4i EC baseline pipeline | [SCYLLADB-4411](https://scylladb.atlassian.net/browse/SCYLLADB-4411) | `1c5f55a4b` | done |
| 9 | i8g EC baseline pipeline | [SCYLLADB-4412](https://scylladb.atlassian.net/browse/SCYLLADB-4412) | `269193c97` | done |
| 10 | `test_metadata` for the new pipelines | [SCYLLADB-4413](https://scylladb.atlassian.net/browse/SCYLLADB-4413) | `e09dbba6d` | done |
| 11 | Config-chain regression test | [SCYLLADB-4414](https://scylladb.atlassian.net/browse/SCYLLADB-4414) | `bf64833ad` | done |
| 12 | Documentation | [SCYLLADB-4415](https://scylladb.atlassian.net/browse/SCYLLADB-4415) | `7b434362e` | done |
| 13 | Dry-run validation of the four chains | [SCYLLADB-4416](https://scylladb.atlassian.net/browse/SCYLLADB-4416) | see sub-task | done |
| 14 | Rate and threshold calibration after first run | [SCYLLADB-4417](https://scylladb.atlassian.net/browse/SCYLLADB-4417) | — | blocked - needs the first run |

### Validation performed for phase 13

All four chains were resolved offline through `SCTConfiguration`, including `verify_configuration()`
and `check_required_files()`, and every invariant held:

| Check | Result |
|-------|--------|
| chains resolve | 4/4, no errors |
| `experimental_features` | `['strongly-consistent-tables']` on SC, `[]` on EC |
| `append_scylla_yaml` merge | `api_address: 0.0.0.0` **and** `commitlog_sync: batch` / window 100 present in all four |
| `append_scylla_args` | identical string in all four (`--blocked-reactor-notify-ms 50 ... --abort-on-internal-error 0`) |
| `instance_type_db` | `i4i.4xlarge` / `i8g.4xlarge` as intended |
| `instance_type_loader` | `c7i.8xlarge`, `n_loaders: 4` |
| stress image | `aleksbykov/cql-stress:leader-aware-strong-consistency` |
| stress commands | all cql-stress, `cl=QUORUM`, `connectionsPerShard=250`, rates 12500 / 10310 / 8750 per loader |
| nemesis | `SisyphusMonkey` + `NemesisSequence`, interval 30 min |

`unit_tests/unit/test_sc_topology_operations_pipelines.py` (36 cases) and
`unit_tests/unit/test_perf_stress_event_classes.py` (10 cases) pass offline, and `ruff check` /
`ruff format --check` are clean on the changed Python files. `sct.py lint-test-docs` passes on
`test-cases/performance/perf-regression-latency-650gb-with-nemesis.yaml` (the remaining failures in
that directory are pre-existing test-cases with no `test_metadata` at all).
