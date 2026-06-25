# Test Metadata Rollout — Add test_metadata to All CI-Active Test Cases

## TL;DR

> Add `test_metadata:` documentation sections to all CI-active test-case YAML files.
> Work is split into Jira sub-tasks under SCT-35, one per team.
> Each team documents their own test-cases using canonical examples and skills from PR #14818.
> A new `team_ownership` label in taxonomy tracks which team owns each test.

---

## Prerequisites

- PR [#14818](https://github.com/scylladb/scylla-cluster-tests/pull/14818) merged
- New `team_ownership` field added to `docs/pipeline-labels/taxonomy.yaml`

## Resources for All Teams

- **Canonical examples** (in PR #14818):
  - `test-cases/longevity/longevity-10gb-3h.yaml` — tier1, cassandra-stress, SisyphusMonkey
  - `test-cases/artifacts/ami.yaml` — sanity, no loaders, NoOpMonkey
  - `test-cases/upgrades/generic-rolling-upgrade.yaml` — upgrade, mixed workload, tls-ssl
- **Skill**: `skills/labeling-pipelines/SKILL.md` — step-by-step guide for adding test_metadata
- **Review skill**: `skills/reviewing-pipeline-docs/SKILL.md`
- **Validation**: `uv run sct.py lint-test-docs --test-case-file <path>`
- **Taxonomy**: `docs/pipeline-labels/taxonomy.yaml`

## Composite Config Note

When a jenkinsfile uses multiple configs (`test_config: '["test-cases/A.yaml", "configurations/B.yaml"]'`),
the standard `anyconfig.merge` with `MS_DICTS` **overwrites** list fields. However, SCT supports
the `++` prefix for appending (see `merge_dicts_append_strings` in `sct_config.py`).

**Recommendation**: Document `test_metadata` in the **base test-case YAML** with the core identity.
If an overlay needs to ADD labels (e.g., an EBS overlay adding `ebs` to features, or a shard-aware
overlay adding `scylla-bench` to stress_tools), use the append syntax in the overlay:

```yaml
# In configurations/overlay.yaml — appends to base test_metadata
test_metadata:
  stress_tools:
    - "++"
    - scylla-bench
  features:
    - "++"
    - ebs
  description: "++; with EBS gp3 storage overlay"
```

This preserves the base metadata while allowing overlays to extend it.

---

## Jira Structure

Create one **Epic** for the test metadata rollout, with independent **Tasks** (not sub-tasks)
for each team. This allows parallel tracking without dependency on a parent issue.

- **Epic**: "Test Metadata Rollout — Document All CI-Active Test Cases"
- **Tasks**: One per team group below (independently assignable and trackable)

## Jira Tickets

### 1. Artsiom Mishuta (Core QA) — CDC, LWT, Raft, Topology, SLA, MV, Counters, Schema-Changes

**Files** (~50):
- `test-cases/cdc/` (all 5 files)
- `test-cases/longevity/longevity-cdc-*.yaml` (5 files)
- `test-cases/longevity/longevity-lwt-*.yaml` (10 files)
- `test-cases/longevity/longevity-*topology*.yaml` (2 files): topology-changes-3h, cdc-100gb-8h-multi-dc-topology-changes
- `test-cases/longevity/longevity-sla-100gb-4h.yaml`
- `test-cases/longevity/longevity-mv-si-4days.yaml`
- `test-cases/longevity/longevity-mv-basic-24h.yaml`
- `test-cases/longevity/longevity-mv-synchronous-updates-12h.yaml`
- `test-cases/longevity/longevity-counter-basic-24h.yaml`
- `test-cases/longevity/longevity-counters-3h.yaml`
- `test-cases/longevity/longevity-counters-multidc.yaml`
- `test-cases/longevity/longevity-schema-changes-3h.yaml`
- `test-cases/longevity/longevity-parallel-schema-changes-12h.yaml`
- `test-cases/longevity/longevity-parallel-schema-changes-12h-cql-stress.yaml`
- `test-cases/longevity/longevity-multidc-parallel-network-schema-changes-12h.yaml`
- `test-cases/performance/perf-regression-*cdc*.yaml` (4 files)
- `test-cases/performance/perf-regression-*lwt*.yaml` (4 files)
- `test-cases/features/system-sla-test.yaml`
- `test-cases/features/sl-workloads-test.yaml`
- `test-cases/gemini/gemini-3h-cdc-*.yaml` (3 files)
- `test-cases/gemini/gemini-3h-ics-cdc-with-nemesis.yaml`
- `test-cases/enterprise-features/workload-prioritization/longevity/longevity-sla-system-24h.yaml`

### 2. Petr Hala (Storage QA) — Tablets, ICS/Storage, KMS, LDAP, Encryption, Elasticity, Out-of-Space, Tombstone GC, Harry, TWCS

**Files** (~33):
- `test-cases/longevity/longevity-azure-kms-10gb-3h.yaml`
- `test-cases/longevity/longevity-gcp-kms-10gb-3h.yaml`
- `test-cases/longevity/longevity-encryption-at-rest-*.yaml` (3 files)
- `test-cases/longevity/longevity-harry-2h.yaml`
- `test-cases/longevity/longevity-multidc-parallel-topology-schema-changes-12h.yaml` (tier1)
- `test-cases/longevity/longevity-twcs-3h.yaml`
- `test-cases/longevity/longevity-twcs-48h.yaml`
- `test-cases/features/ics_space_amplification_goal_test.yaml`
- `test-cases/features/elasticity/` (7 files)
- `test-cases/features/out-of-space-prevention/` (5 files)
- `test-cases/features/tombstone_gc/longevity-tombstone-gc-modes.yaml`
- `test-cases/features/size-based-load-balancing/size-based-load-balancing.yaml`
- `test-cases/enterprise-features/ics/` (8 files)

### 3. Julia Yakovlev (Core Test Infra) — Performance (general) + Microbenchmarking

**Files** (~28):
- `test-cases/performance/` — all files NOT matching `*cdc*`, `*lwt*`, `*tablets*`, `*alternator*`
- `test-cases/microbenchmarking/` (2 files)

**Pipelines**: `jenkins-pipelines/performance/`, `jenkins-pipelines/performance_staging/`

### 4. Dmytro Kruglov (Core Test Infra) — Cassandra Migration, Platform Migration, Spark

**Files** (12):
- `test-cases/cassandra/` (4 files)
- `test-cases/platform-migration/` (5 files)
- `test-cases/spark-migrator/` (3 files)

### 5. fruch (Core Test Infra) — Longevity Core, Artifacts, Upgrades, Scale, Kafka, Misc Features, Infra

**Files** (~85):
- `test-cases/longevity/` — remaining 34 files (non-cdc/lwt/sla/topology/kms/encrypt/alternator/mv/counter/schema-changes/harry/twcs)
- `test-cases/artifacts/` (26 files)
- `test-cases/upgrades/` (8 files)
- `test-cases/scale/` (7 files)
- `test-cases/kafka/` (2 files)
- `test-cases/load/admission_control_overload_test.yaml`
- `test-cases/features/` remaining (3 files): 2mv-backpressure-4d, gce-multiple-dc-shutdown-30mins, google-cloud-snitch-multi-dc
- Root-level: PR-provision-test.yaml, PR-provision-test-docker.yaml, cassandra-aws-provision-test.yaml, cassandra-docker-provision-test.yaml, simple-data-validation-latte.yaml, agent-test-aws.yaml

### 6. Mikita Liapkovich (Cloud QA) — Manager + Vector Search

**Files** (24):
- `test-cases/manager/` (22 files)
- `test-cases/vector-search/` (2 files)

### 7. Lukasz Sojka (QA Tools) — Gemini (all, including ICS) + Streaming Features

**Files** (~12):
- `test-cases/gemini/` — all remaining files (including ICS variants)
- `test-cases/features/compaction-throughput-limit.yaml`
- `test-cases/features/per-partition-limit.yaml`
- `test-cases/features/limit-streaming-io.yaml`

### 8. Eugene Abramchuk (Cloud QA) — Scylla Operator (non-alternator)

**Files** (~20):
- `test-cases/scylla-operator/` — all files NOT matching `*alternator*`

### 9. Unassigned (@temichus) — Alternator + LDAP (all folders)

**Files** (~20):
- `test-cases/features/test_add_remove_ldap_role_permission.yaml`
- `test-cases/features/alternator-ttl/` (8 files)
- `test-cases/longevity/longevity-alternator-*.yaml` (4 files)
- `test-cases/performance/perf-regression-alternator-*.yaml` (3 files)
- `test-cases/manager/manager-regression-alternator-singleDC-set-distro.yaml`
- `test-cases/scylla-operator/longevity-scylla-operator-alternator-*.yaml` (2 files)
- `test-cases/performance/perf-regression-alternator.100threads.30M-keys.yaml`

---

## Quarantine (Skip — No Documentation Needed)

These files have no active pipeline or are deprecated:
- `test-cases/features/dns-cluster-5min.yaml`
- `test-cases/features/uda_udf.yaml`
- `test-cases/jepsen/jepsen.yaml`
- `test-cases/jepsen/jepsen_with_raft.yaml`
- `test-cases/longevity/longevity-100GB-48h-cloud-CloudLimitedChaosMonkey-tls.yaml`
- `test-cases/longevity/longevity-ycsb-a-100M.yaml`
- `test-cases/longevity/longevity-ycsb-a-10M.yaml`
- `test-cases/longevity/longevity-ycsb-a-1B.yaml`
- `test-cases/longevity/longevity-ycsb-a-1M.yaml`

---

## Jira Ticket Template

```
Title: Add test_metadata to <group> test cases
Type: Task (linked to Epic "Test Metadata Rollout")
Assignee: <person>

Description:
Add `test_metadata:` sections to the following test-case YAML files.
Each Jira item should reference the pipelines using those configurations.

## Files to Document
<file list with pipeline references>

## How To

1. Read the skill: `skills/labeling-pipelines/SKILL.md`
2. Look at canonical examples:
   - test-cases/longevity/longevity-10gb-3h.yaml
   - test-cases/artifacts/ami.yaml
   - test-cases/upgrades/generic-rolling-upgrade.yaml
3. For each file:
   - Add `test_metadata:` as the FIRST block in the YAML
   - Fill in: description (one-liner), test_type, tier, duration_class,
     supported_backends, stress_tools, nemesis_labels, team_ownership
   - Validate: `uv run sct.py lint-test-docs --test-case-file <path>`
4. Do NOT modify any existing YAML fields

## Acceptance Criteria
- [ ] All listed files have `test_metadata` sections
- [ ] `uv run sct.py lint-test-docs` → 0 errors on each file
- [ ] Each description is ≥20 chars and specific to the test
- [ ] `team_ownership` field set correctly
- [ ] PR does NOT modify existing YAML fields

## References
- PR #14818: https://github.com/scylladb/scylla-cluster-tests/pull/14818
- Skill: skills/labeling-pipelines/SKILL.md
- Taxonomy: docs/pipeline-labels/taxonomy.yaml
```

---

## New Taxonomy Field: team_ownership

Add to `docs/pipeline-labels/taxonomy.yaml`:

```yaml
team_ownership:
  description: "QA team responsible for maintaining this test"
  values:
    - core-test-infra    # fruch, julia-yakovlev, dmytro-kruglov
    - core-qa            # artsiom-mishuta
    - storage-qa         # petr-hala
    - cloud-qa           # mikita-liapkovich, eugene-abramchuk
    - qa-tools           # lukasz-sojka
```

### Team → Person Mapping (for Jira assignment)

| Team | Members | Jira Assignee |
|------|---------|---------------|
| Core Test Infra | fruch, julia-yakovlev, dmytro-kruglov | per ticket |
| Core QA | artsiom-mishuta | artsiom-mishuta |
| Storage QA | petr-hala | petr-hala |
| Cloud QA | mikita-liapkovich, eugene-abramchuk | per ticket |
| QA Tools | lukasz-sojka | lukasz-sojka |

---

## Success Criteria

After all tickets complete:
```bash
uv run sct.py lint-test-docs --missing-only  # 0 missing among CI-active files
```

Then add `lint-test-docs --missing-only` to pre-commit/CI to prevent regression.
