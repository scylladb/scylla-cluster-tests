---
status: draft
domain: config
created: 2026-04-24
last_updated: 2026-04-24
owner: cezarmoise
---


# AWS/GCE Backend Parity for Test Configurations

## 1. Problem Statement

Switching a test from AWS to GCE still requires manual YAML work because many test configs only
define AWS instance sizing.

**Concrete pain point:** 185 test-case YAML files define `instance_type_db` but not
`gce_instance_type_db`. On GCE those tests fall back to generic defaults from
`defaults/gce_config.yaml`, which can materially undersize the run.

The same problem applies to loader and monitor overrides, and to DB disk settings when a config is
later resized: if disk params are left implicit, changing the instance type does not force the
reviewer to reconsider disk sizing.

**Explicit non-goal:** This plan does **not** solve backend-specific image selection
(`ami_id_db_scylla` / `SCT_AMI_ID_DB_SCYLLA` on AWS vs `gce_image_db` / `SCT_GCE_IMAGE_DB` on
GCE). It is limited to **test-case YAML sizing parity and clarity**.

## 2. Current State

### Test Config YAMLs

A survey of `test-cases/` (270 YAML files) shows:

| Metric | Count |
|--------|-------|
| Files with `instance_type_db` (AWS) | 234 |
| Files with `gce_instance_type_db` | 49 |
| Files with `instance_type_db` **but not** `gce_instance_type_db` | **185** |
| Files with `gce_instance_type_db` but not `instance_type_db` | 0 |

Breakdown of the 185 files missing GCE DB params by subdirectory:

| Subdirectory | Count |
|--------------|-------|
| `longevity/` | 56 |
| `features/` | 32 |
| `performance/` | 29 |
| `manager/` | 17 |
| `gemini/` | 11 |
| `artifacts/` | 9 |
| `cdc/` | 5 |
| `scale/` | 5 |
| `upgrades/` | 5 |
| `scylla-operator/` | 4 |
| `other` (top-level YAMLs) | 3 |
| `kafka/` | 2 |
| `microbenchmarking/` | 2 |
| `enterprise-features/` | 1 |
| `load/` | 1 |
| `platform-migration/` | 1 |
| `quarantined/` | 1 |
| `spark-migrator/` | 1 |

Additional backend-parity gaps:

- **122** YAML files override `instance_type_loader`; **100** of them have no
  `gce_instance_type_loader` counterpart.
- **51** YAML files override `instance_type_monitor`; **46** of them have no
  `gce_instance_type_monitor` counterpart.

**Existing GCE configuration fragments** are available in `configurations/gce/`:
`n2-highmem-8.yaml`, `n2-highmem-16.yaml`, `n2-highmem-32.yaml`, `n2-highmem-48.yaml`,
`n2-highmem-64.yaml`, `z3-highmem-8-highlssd.yaml`, `preview_instance.yaml`.

**Important constraint for this work:** migrated YAMLs must set disk-related GCE params
explicitly, even when the chosen value matches today's default. Relying on defaults makes later
instance-type edits unsafe because the missing disk param is easy to forget to revisit.

### Existing Lint Coverage

`./utils/lint_test_cases.sh` is the authoritative config-load smoke test today. It invokes
`./sct.py lint-yamls` with several backend-specific include/exclude patterns:

1. **Generic AWS lint**: lints most non-cloud-specific YAMLs with
   `-i '.yaml' -e 'azure,oci,multi-dc,multiDC,multidc,multiple-dc,3dcs,5dcs,rolling,docker,artifacts,private-repo,ics/long,scylla-operator,gce,custom-d3,jepsen,repair-based-operations,add-new-dc,baremetal,x86-to-arm'`
2. **Single-region GCE lint**: currently only lints
   `rolling,artifacts,private-repo,gce,custom-d3,jepsen` with exclusions
   `multi-dc,multiDC,docker,azure,oci,5dcs,oel`
3. **2-region GCE lint**: lints
   `multi-dc-rolling,3h-multidc,multidc-4h,multidc-parallel,snitch-multi-dc,manager-regression-multiDC`
4. **3-region GCE lint**: lints
   `3dcs,24h-multidc,large-cluster,counters-multidc,cdc-8h-multi-dc`
5. **k8s-local-kind lint**: separately lints `scylla-operator.*\.yaml`

This means the current plan cannot simply say “make GCE lint match AWS lint.” The GCE coverage is
already split into distinct **single-region** and **multi-DC** blocks, and `scylla-operator` is
validated outside the plain GCE backend path.

### Jenkins Image Inputs Remain Backend-Specific

Jenkins pipelines currently export backend-specific image parameters independently:

- AWS: `SCT_AMI_ID_DB_SCYLLA`
- GCE: `SCT_GCE_IMAGE_DB`

For example, `vars/runSctTest.groovy` exports both values when present, and many pipelines expose
`gce_image_db` as a dedicated Jenkins parameter. As a result, flipping `backend: 'aws'` to
`backend: 'gce'` is only expected to work for jobs that already resolve images via a backend-neutral
input (`scylla_version`, `scylla_repo`, `unified_package`) or already provide a valid
`gce_image_db`. Jobs that rely only on `scylla_ami_id` are **out of scope** for this plan.

## 3. Goals

1. Every test-case YAML that sets `instance_type_db` also sets explicit GCE DB sizing:
   `gce_instance_type_db`, `gce_n_local_ssd_disk_db`, and `gce_root_disk_type_db`.

2. Every test-case YAML that sets `instance_type_loader` or `instance_type_monitor` also sets the
   corresponding GCE param when migrated.

3. Migrated configs annotate instance selections with comments showing the chosen instance's vCPU
   and memory, so later reviewers can reason about parity without external lookup.

4. `./utils/lint_test_cases.sh` is broadened in a controlled way while keeping the existing
   single-region, multi-DC, and `k8s-local-kind` blocks green.

5. For Jenkins jobs that already use backend-neutral image inputs (`scylla_version`,
   `scylla_repo`, `unified_package`) or already supply `gce_image_db`, operators can flip
   `backend: aws` ↔ `backend: gce` without any further **test YAML** edits.

6. A short mapping document (`docs/gce-instance-type-mapping.md`) captures the agreed AWS↔GCE
   DB/loader/monitor pairings.

## 4. Implementation Phases

### Phase 1 — AWS ↔ GCE Instance Type Mapping Reference

**Scope:** Documentation + `configurations/` fragments only (no test YAMLs modified yet)

**Description:**

Create a reference table mapping the most common AWS instance types used in SCT tests to their
closest GCE equivalents. This table drives all YAML edits in Phases 2–3 and prevents ad-hoc,
inconsistent choices.

Mapping criteria:
- Match vCPU count and RAM as closely as possible.
- Prefer local-SSD-backed GCE types (`n2-highmem-*`) for storage-heavy workloads; use `e2-*`
  for CPU-bound or loader nodes.
- Define explicit disk params for every migrated DB config, even when they match current defaults.
- Include monitor mappings in the same document.

**Starting equivalence table (subject to review):**

| AWS type | Intended role | GCE type | Extra params | Notes |
|----------|---------------|----------|--------------|-------|
| `i4i.large` | DB | `n2-highmem-4` | `gce_n_local_ssd_disk_db: 1` | 2 vCPU / 16 GB |
| `i4i.xlarge` | DB | `n2-highmem-8` | `gce_n_local_ssd_disk_db: 2` | 4 vCPU / 32 GB |
| `i4i.2xlarge` | DB | `n2-highmem-16` | `gce_n_local_ssd_disk_db: 4` | 8 vCPU / 64 GB |
| `i4i.4xlarge` | DB | `n2-highmem-32` | `gce_n_local_ssd_disk_db: 8` | 16 vCPU / 128 GB |
| `i4i.8xlarge` | DB | `n2-highmem-32` | `gce_n_local_ssd_disk_db: 16` | 32 vCPU / 256 GB |
| `i4i.16xlarge` | DB | `n2-highmem-64` | `gce_n_local_ssd_disk_db: 24` | 64 vCPU / 512 GB |
| `i3en.2xlarge` | DB | `n2-highmem-16` | `gce_n_local_ssd_disk_db: 4` | storage-optimised |
| `i3en.3xlarge` | DB | `n2-highmem-32` | `gce_n_local_ssd_disk_db: 8` | — |
| `i3en.6xlarge` | DB | `n2-highmem-32` | `gce_n_local_ssd_disk_db: 16` | — |
| `c6i.xlarge` | loader | `e2-standard-4` | none | compute loader |
| `c6i.2xlarge` | loader | `e2-standard-8` | none | — |
| `c6i.4xlarge` | loader | `e2-standard-16` | none | — |
| `c6i.16xlarge` | loader | `e2-highcpu-32` | none | high-CPU loader |
| `t3.small` | monitor | `n2-highmem-8` | none | default already matches many small monitors |
| `t3.large` | monitor | `n2-highmem-8` | none | default often acceptable |
| `m6i.xlarge` | monitor | `n2-highmem-8` | none | may need validation on scale tests |
| `m6i.2xlarge` | monitor | `n2-highmem-16` | none | large monitor footprint |

> **Needs Investigation:** `i4i.8xlarge` and `i4i.16xlarge` may be better served by
> `z3-highmem-*` or `n2-highmem-48/64`, depending on GCE local-SSD limits and cost. Validate the
> upper-tier mappings before finalising the document.

**Deliverables:**
- `docs/gce-instance-type-mapping.md` — compact reference table with rationale.
- New `configurations/gce/n2-highmem-4.yaml` fragment (currently missing).
- A comment convention for YAML annotations, e.g. `# 8 vCPU / 64 GB`.

**Definition of Done:**
- [ ] Reference table reviewed and agreed upon by the team.
- [ ] `configurations/gce/n2-highmem-4.yaml` added.
- [ ] Comment convention agreed.
- [ ] `docs/gce-instance-type-mapping.md` merged.

**Dependencies:** None.

---

### Phase 2 — Add GCE Params to Longevity and CDC Test Configs

**Scope:** `test-cases/longevity/` (56 files) and `test-cases/cdc/` (5 files)

**Description:**

For each of the 61 files, add the GCE equivalent params and annotate them with sizing comments.
Each migrated DB config must set disk params explicitly.

```yaml
instance_type_db: 'i4i.2xlarge'              # 8 vCPU / 64 GB
gce_instance_type_db: 'n2-highmem-16'       # 16 vCPU / 128 GB
gce_root_disk_type_db: 'pd-ssd'
gce_n_local_ssd_disk_db: 4
gce_instance_type_loader: 'e2-standard-4'   # 4 vCPU / 16 GB
```

Files that also override `instance_type_monitor` need a matching `gce_instance_type_monitor`
with the same comment style.

**Definition of Done:**
- [ ] All 61 longevity/cdc YAML files have explicit GCE DB sizing, explicit GCE disk params, and
  loader/monitor sizing where needed.
- [ ] Migrated instance fields carry vCPU / RAM comments.
- [ ] The existing single-region GCE invocation in `./utils/lint_test_cases.sh` is extended from
  `rolling,artifacts,private-repo,gce,custom-d3,jepsen` to
  `rolling,artifacts,private-repo,gce,custom-d3,jepsen,longevity,cdc`.
- [ ] The dedicated 2-region and 3-region GCE lint blocks continue to pass unchanged.
- [ ] `./utils/lint_test_cases.sh` passes end-to-end after the filter change.
- [ ] PR passes `uv run sct.py pre-commit`.

**Dependencies:** Phase 1 (mapping table must be agreed upon first).

**Importance:** High — longevity tests are the most frequently run test category on GCE.

---

### Phase 3 — Add GCE Params to Remaining Test Configs

**Scope:**
- `test-cases/features/` (32)
- `test-cases/performance/` (29)
- `test-cases/manager/` (17)
- `test-cases/gemini/` (11)
- `test-cases/artifacts/` (9)
- `test-cases/upgrades/` (5)
- `test-cases/scale/` (5)
- `test-cases/scylla-operator/` (4)
- `test-cases/kafka/` (2)
- `test-cases/microbenchmarking/` (2)
- top-level `test-cases/*.yaml` files (3)
- `test-cases/enterprise-features/` (1)
- `test-cases/load/` (1)
- `test-cases/platform-migration/` (1)
- `test-cases/quarantined/` (1)
- `test-cases/spark-migrator/` (1)

Total: **124 files**.

**Description:**

Same approach as Phase 2. Apply the mapping table to all remaining files that are missing
`gce_instance_type_db`, add explicit disk params to every migrated DB config, and fill in the
corresponding loader/monitor params. Keep the same vCPU / RAM comment convention.

Performance test configs that use large AWS instances (e.g. `i4i.4xlarge`, `i4i.8xlarge`) should
map to a GCE type that provides similar IOPS/bandwidth; document any cases where GCE cannot match
AWS performance characteristics.

`scylla-operator` requires special handling:
- keep the existing `k8s-local-kind` lint block in `./utils/lint_test_cases.sh` green,
- add the missing GCE-style sizing fields to the 4 operator YAMLs for future GKE/backend parity,
- spot-check those 4 files with `./sct.py conf -b k8s-gke <path>` because the repo does not yet
  have a dedicated `k8s-gke` lint block.

**Definition of Done:**
- [ ] All 124 remaining YAML files have explicit GCE DB sizing, explicit GCE disk params, and
  loader/monitor sizing where needed.
- [ ] Migrated instance fields carry vCPU / RAM comments.
- [ ] The existing single-region GCE invocation in `./utils/lint_test_cases.sh` is further extended
  to include:
  `features,performance,manager,gemini,upgrades,scale,kafka,microbenchmarking,enterprise-features,load,platform-migration,quarantined,spark-migrator`.
- [ ] The dedicated 2-region and 3-region GCE invocations still pass.
- [ ] The `oel` exclusion remains in place until the underlying lint limitation is resolved.
- [ ] The existing `k8s-local-kind` lint block still passes for `scylla-operator.*\.yaml`.
- [ ] The 4 modified `scylla-operator` YAMLs are spot-checked with `./sct.py conf -b k8s-gke <path>`.
- [ ] `grep -rL gce_instance_type_db test-cases/ | xargs grep -l instance_type_db 2>/dev/null`
  returns zero files.
- [ ] PR passes `uv run sct.py pre-commit`.

**Dependencies:** Phase 1 (mapping table), Phase 2 (establishes precedent / review patterns).

**Importance:** Medium — these tests are less frequently run on GCE today but are needed for full
backend parity.

---

### Phase 4 — Documentation Update

**Scope:** `docs/`, README or developer guide (if applicable)

**Description:**

- Finalise `docs/gce-instance-type-mapping.md` with any adjustments discovered during Phases 2–3.
- Add a short section to `AGENTS.md` (or a skill under `skills/`) describing the AWS↔GCE YAML
  convention: explicit GCE DB/loader/monitor params, explicit GCE disk params, and vCPU / RAM
  comments on migrated instance fields.
- Explicitly document the boundary of this work: YAML sizing parity is in scope; backend-specific
  image selection remains a separate concern.

**Definition of Done:**
- [ ] `docs/gce-instance-type-mapping.md` is up-to-date with final mapping decisions from Phase 3.
- [ ] AGENTS.md / skill note describes the AWS↔GCE YAML pairing convention.
- [ ] AGENTS.md / skill note explicitly states that image parity is out of scope for this plan.
- [ ] PR passes `uv run sct.py pre-commit`.

**Dependencies:** Phases 2–3.

## 5. Testing Requirements

### Config Load Linting
- `./utils/lint_test_cases.sh` must pass after every YAML-editing phase.
- Phase 2 broadens the existing **single-region GCE** lint block to include `longevity,cdc`.
- Phase 3 broadens the same single-region GCE block to include
  `features,performance,manager,gemini,upgrades,scale,kafka,microbenchmarking,enterprise-features,load,platform-migration,quarantined,spark-migrator`.
- The dedicated **2-region** and **3-region** GCE lint blocks must continue to pass unchanged.
- The existing `k8s-local-kind` block must continue to pass for `scylla-operator.*\.yaml`.
- `oel` remains excluded until the existing limitation is resolved.

### Unit Tests
- Existing `unit_tests/unit/test_config.py` tests must continue to pass. No new unit tests are
  required — this plan only adds YAML fields and documentation.

### Targeted Config Checks
- Spot-check the 4 modified `scylla-operator` YAMLs with `./sct.py conf -b k8s-gke <path>`.
- Spot-check at least 1 representative YAML in each newly broadened single-region category with
  `./sct.py conf -b gce <path>` before updating the lint script filter.

### Integration Tests
- Run `uv run sct.py run-test longevity_test.LongevityTest.test_custom_time --backend gce
  --config test-cases/longevity/longevity-10gb-3h.yaml` in a GCE dry-run environment to confirm
  the updated GCE instance selection is applied correctly.

### Manual Verification
- Pick 3 representative tests (one longevity, one performance, one manager) and run them end-to-end
  on GCE after Phase 3 is merged to validate DB/loader/monitor sizing.
- In those migrated YAMLs, verify that instance fields are annotated with vCPU / RAM comments and
  that GCE disk params are explicit rather than inherited from defaults.
- For one Jenkins job that already uses `scylla_version`, `scylla_repo`, `unified_package`, or an
  explicit `gce_image_db`, switch `backend: 'gce'` and confirm the run picks up the GCE instance
  types newly added to the test-case YAML rather than the generic defaults.

## 6. Success Criteria

- `grep -rL gce_instance_type_db test-cases/ | xargs grep -l instance_type_db 2>/dev/null`
  returns zero results.
- Migrated YAMLs set explicit `gce_root_disk_type_db` and `gce_n_local_ssd_disk_db` rather than
  relying on defaults.
- Migrated instance fields carry vCPU / RAM comments.
- `./utils/lint_test_cases.sh` passes with its broadened single-region GCE filter, while the
  dedicated multi-DC and k8s-local-kind blocks also remain green.
- No existing AWS pipeline regresses: all current tests in `unit_tests/unit/test_config.py` still
  pass.
- For jobs that already use backend-neutral image inputs or explicit `gce_image_db`, an AWS
  jenkinsfile can be switched to `backend: 'gce'` without further **test YAML** edits.

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Chosen GCE instance type is undersized for a performance test, causing test failures | Medium | Medium | Use the Phase 1 mapping table as a gate; perf tests should be validated manually (§5 Manual Verification) before marking Phase 3 done. |
| GCE disk sizing is forgotten when an instance type changes later | Medium | High | Require explicit `gce_root_disk_type_db` and `gce_n_local_ssd_disk_db` in every migrated config, even when matching current defaults. |
| Instance comments drift from actual instance choices | Low | Medium | Use a simple `# <vCPU> vCPU / <RAM> GB` convention and update comments in the same edit as any instance-type change. |
| Large PR for YAML edits (185 files) is hard to review | High | Medium | Split into per-category PRs and verify each with `./utils/lint_test_cases.sh`. |
| `scylla-operator` YAMLs gain GCE sizing fields but still lack dedicated `k8s-gke` lint coverage | Medium | Medium | Keep the existing `k8s-local-kind` lint block green and add targeted `./sct.py conf -b k8s-gke <path>` spot checks until a dedicated `k8s-gke` lint block exists. |
| Readers assume the plan also solves AWS↔GCE image selection parity | High | Medium | State explicitly that image parity (`scylla_ami_id` vs `gce_image_db`) is out of scope for this plan. |
| Instance type mapping becomes stale as new AWS/GCE types are introduced | Low | Low | Keep `docs/gce-instance-type-mapping.md` short and update it whenever new test configs are added. |
