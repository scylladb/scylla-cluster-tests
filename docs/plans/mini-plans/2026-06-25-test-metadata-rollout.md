# Test Metadata Rollout — Add test_metadata to All CI-Active Test Cases

## TL;DR

> **Quick Summary**: Systematically add `test_metadata:` documentation sections to all ~160 CI-active test-case YAML files, using an auto-inference script to pre-populate fields and LLM to generate descriptions, rolled out as one PR per folder from smallest to largest.
>
> **Deliverables**:
> - Auto-inference CLI command (`generate-test-metadata`) that pre-populates `test_metadata` from existing YAML fields + jenkinsfile scanning
> - Taxonomy extension PR for directories with no matching `test_type`
> - ~10 folder PRs adding `test_metadata` to all CI-active test-case YAMLs
> - CI enforcement: `lint-test-docs --missing-only` added to pre-commit to prevent regression
>
> **Estimated Effort**: Large (10-12 PRs over 2-4 weeks)
> **Parallel Execution**: YES — folder PRs are independent after Phase 1
> **Critical Path**: Merge PR #14818 → Build script → Taxonomy gaps PR → Folder PRs

---

## Context

### Original Request
Add `test_metadata` documentation to all test-case YAMLs, organized as one PR per folder/group, starting smallest-first. Should be as scripted as possible with LLM generating one-liner descriptions.

### Scope Analysis
- **286** total test-case YAMLs across 21 subdirectories
- **~160** actively referenced by Jenkins pipelines (CI-active) — these are in scope
- **~126** orphaned (no jenkinsfile) — explicitly out of scope
- **0** currently have `test_metadata` sections (greenfield)

### Hard Dependency
PR [#14818](https://github.com/scylladb/scylla-cluster-tests/pull/14818) (pipeline-labeling-impl) **must be merged first**. It provides:
- `sdcm/test_metadata.py` — TestMetadata pydantic model
- `docs/pipeline-labels/taxonomy.yaml` — authorized label values
- `sdcm/utils/lint/docs_linter.py` — linter with 12 validation rules
- `sdcm/utils/lint/jenkins_parser.py` — jenkinsfile scanner
- `sct.py lint-test-docs` — CLI command for validation
- 4 example test-cases with `test_metadata` sections

### Metis Review Findings (incorporated)
- Reuse `jenkins_parser.py` and `docs_linter.py` detection patterns — do NOT reimplement
- Handle composite configs (base YAML + overlay) by merging full config chain
- `tier` cannot be auto-inferred — it's a business decision, requires human input
- 8 directories have no matching `test_type` in taxonomy — resolve before folder PRs
- 37 files in nested subdirectories need special `test_type` handling

---

## Work Objectives

### Core Objective
Every CI-active test-case YAML file has a validated `test_metadata:` section with accurate description, test_type, duration_class, stress_tools, nemesis_labels, and supported_backends.

### Concrete Deliverables
- `sct.py generate-test-metadata` CLI command
- Taxonomy extension for missing `test_type` values
- `test_metadata:` sections in all ~160 CI-active test-case YAMLs
- CI enforcement preventing regression

### Definition of Done
- `uv run sct.py lint-test-docs --missing-only` reports 0 missing among CI-active files
- All CI-active files pass `lint-test-docs` with 0 errors
- Pre-commit hook prevents new test-cases without `test_metadata`

### Must Have
- Auto-inference of: test_type, duration_class, stress_tools, nemesis_labels, supported_backends, workload
- LLM-generated one-liner description for each test
- Human-reviewed `tier` assignment per folder PR

### Must NOT Have (Guardrails)
- Do NOT modify existing YAML fields — each PR adds ONLY `test_metadata:` section
- Do NOT add metadata to orphaned test-cases (no jenkinsfile)
- Do NOT add metadata to `configurations/nemesis/` overlay configs
- Do NOT auto-infer `tier` — leave null for human assignment
- Do NOT expand taxonomy in folder PRs — taxonomy changes in separate prerequisite PR
- Do NOT combine metadata addition with other refactoring (sizing, field renames, etc.)

---

## Verification Strategy

> **ZERO HUMAN INTERVENTION** — ALL verification is agent-executed.

### Test Decision
- **Infrastructure exists**: YES (pytest, pre-commit hooks)
- **Automated tests**: YES (tests-after) — unit tests for auto-inference script
- **Framework**: pytest (existing)

### QA Policy
- Per-file: `uv run sct.py lint-test-docs --test-case-file <file>` → 0 errors
- Per-PR: `uv run sct.py lint-test-docs --test-case-dir test-cases/<folder>/` → 0 errors
- Final: `uv run sct.py lint-test-docs --missing-only` → 0 missing among CI-active

---

## Execution Strategy

### Phase Overview

```
Phase 0: Merge PR #14818 (prerequisite — already in progress)

Phase 1 (Foundation):
├── Task 1: Build generate-test-metadata CLI command
├── Task 2: Extend taxonomy for missing test_type values
└── Task 3: Validate script against 4 existing examples

Phase 2 (Small folders — quick wins, one PR each):
├── Task 4: PR — small batch (jepsen, kafka, microbenchmarking, vector-search, cassandra, load, quarantined, root-level)
├── Task 5: PR — cdc + platform-migration
├── Task 6: PR — scale + upgrades
└── Task 7: PR — gemini

Phase 3 (Medium folders — one PR each):
├── Task 8: PR — features (incl. nested: alternator-ttl, elasticity, etc.)
├── Task 9: PR — manager
└── Task 10: PR — scylla-operator

Phase 4 (Large folders — one PR each):
├── Task 11: PR — artifacts
├── Task 12: PR — performance
└── Task 13: PR — longevity

Phase 5 (Enforcement):
└── Task 14: Add lint-test-docs --missing-only to pre-commit / CI
```

---

## TODOs

- [ ] 1. Build `generate-test-metadata` CLI command

  **What to do**:
  - Add `sct.py generate-test-metadata` command that takes a test-case YAML path (or directory) and outputs a `test_metadata:` block
  - Reuse existing modules — do NOT reimplement:
    - `sdcm.utils.lint.jenkins_parser.discover_pipeline_files()` + `parse_jenkinsfile()` for scanning jenkinsfiles
    - Stress tool detection regex from `docs_linter.py` TD-006
    - Duration thresholds from `docs_linter.py` TD-008 (short <360min, medium 360-1440, long >1440)
  - Auto-inference logic:
    - `test_type`: from parent directory name (with mapping for nested dirs)
    - `duration_class`: from `test_duration` field using TD-008 thresholds
    - `stress_tools`: parse `stress_cmd` fields for tool names (cassandra-stress, scylla-bench, ycsb, latte, gemini, cql-stress-cassandra-stress)
    - `nemesis_labels`: copy from `nemesis_class_name` field (normalize to list)
    - `supported_backends`: scan all jenkinsfiles referencing this test-case, collect unique `backend` values
    - `workload`: infer from stress_cmd (write/read/mixed/scan patterns)
    - `features`: detect from config (tls/ssl → tls-ssl, tablets, cdc, multi-dc, encryption keywords)
    - `description`: leave as placeholder `"TODO: add description"` for LLM/human to fill
    - `tier`: leave as null — human decision
  - Handle composite configs: when jenkinsfile references `["test-cases/A.yaml", "configurations/B.yaml"]`, merge both before inference
  - Output modes: `--dry-run` (print to stdout), `--write` (modify file in-place), `--directory` (batch process folder)
  - Place `test_metadata:` as FIRST section in YAML (before existing fields)
  - Add unit tests in `unit_tests/unit/test_generate_metadata.py`

  **Must NOT do**:
  - Do not modify any existing YAML fields
  - Do not write new jenkinsfile parsers — reuse `jenkins_parser.py`

  **References**:
  - `sdcm/utils/lint/jenkins_parser.py` — `discover_pipeline_files()`, `parse_jenkinsfile()`, `PipelineConfig` dataclass
  - `sdcm/utils/lint/docs_linter.py` — TD-006 stress tool regex, TD-008 duration thresholds, TD-009 feature detection
  - `sdcm/test_metadata.py` — `TestMetadata` pydantic model, `_load_taxonomy()`
  - `test-cases/longevity/longevity-10gb-3h.yaml` (PR #14818 branch) — reference format for test_metadata section
  - `sct.py:lint-test-docs` command — pattern for CLI command registration

  **Acceptance Criteria**:
  - [ ] `uv run sct.py generate-test-metadata --dry-run test-cases/longevity/longevity-10gb-3h.yaml` outputs valid test_metadata block
  - [ ] Script correctly infers backends by scanning jenkinsfiles
  - [ ] Script handles composite configs (merges overlay YAMLs)
  - [ ] Running on 4 existing example files from PR #14818 produces output with ≤2 field differences per file
  - [ ] Unit tests pass: `uv run python -m pytest unit_tests/unit/test_generate_metadata.py -x`

  **Commit**: YES
  - Message: `feature(test-metadata): add generate-test-metadata CLI for auto-inference`
  - Files: `sct.py`, `sdcm/utils/lint/metadata_generator.py`, `unit_tests/unit/test_generate_metadata.py`
  - Pre-commit: `uv run sct.py pre-commit`

- [ ] 2. Extend taxonomy for missing `test_type` values

  **What to do**:
  - Add missing `test_type` values to `docs/pipeline-labels/taxonomy.yaml` for directories with CI-active test-cases:
    - `operator` — for `test-cases/scylla-operator/` (22 files, referenced by `jenkins-pipelines/operator/`)
    - `cassandra` — for `test-cases/cassandra/` (4 files, referenced by `jenkins-pipelines/oss/longevity/`)
    - `kafka` — for `test-cases/kafka/` (2 files, referenced by `jenkins-pipelines/oss/kafka_connectors/`)
    - `microbenchmarking` — for `test-cases/microbenchmarking/` (2 files)
    - `load` — for `test-cases/load/` (1 file)
  - Define `test_type` mapping for nested directories:
    - `features/alternator-ttl/` → `test_type: features`
    - `features/elasticity/` → `test_type: features`
    - `enterprise-features/*/` → skip (likely orphaned, verify)
  - Do NOT add types for dirs with 0 CI-active files (spark-migrator, nemesis configs)

  **Must NOT do**:
  - Do not change existing taxonomy values
  - Do not combine with folder PRs

  **References**:
  - `docs/pipeline-labels/taxonomy.yaml` — current `test_type.values` list
  - `sdcm/test_metadata.py:_load_taxonomy()` — auto-loads taxonomy at import time

  **Acceptance Criteria**:
  - [ ] `python -c "from sdcm.test_metadata import _TAXONOMY; print(_TAXONOMY['test_type']['values'])"` includes new values
  - [ ] `uv run sct.py pre-commit` passes

  **Commit**: YES
  - Message: `docs(taxonomy): add test_type values for operator, cassandra, kafka, microbenchmarking, load`
  - Files: `docs/pipeline-labels/taxonomy.yaml`

- [ ] 3. Validate auto-inference script against existing examples

  **What to do**:
  - Run `generate-test-metadata --dry-run` on the 4 files that already have `test_metadata` in PR #14818:
    - `test-cases/artifacts/ami.yaml`
    - `test-cases/longevity/longevity-100gb-4h-cql-stress.yaml`
    - `test-cases/longevity/longevity-10gb-3h.yaml`
    - `test-cases/upgrades/generic-rolling-upgrade.yaml`
  - Compare auto-inferred output against human-written metadata
  - Fix any inference bugs discovered
  - Document field accuracy in a validation report

  **Acceptance Criteria**:
  - [ ] ≤2 field differences per file between auto-inferred and human-written metadata
  - [ ] All inferred `stress_tools` match the human-written values
  - [ ] All inferred `supported_backends` match the human-written values
  - [ ] Script handles the `cql-stress-cassandra-stress` tool correctly

  **Commit**: NO (validation only, fixes go into Task 1 if needed)

- [ ] 4. PR — Small batch: jepsen, kafka, microbenchmarking, vector-search, cassandra, load, quarantined, root-level

  **What to do**:
  - Run `generate-test-metadata --directory` for each folder, filter to CI-active files only
  - Use LLM to generate one-liner `description` for each test based on: test name, stress commands, nemesis, duration, backends
  - Manually assign `tier` for each file based on pipeline location (oss/tier1/ → tier1, etc.)
  - Validate each file: `uv run sct.py lint-test-docs --test-case-file <path>`
  - Estimated scope: ~10-15 CI-active files across these folders

  **Must NOT do**:
  - Do not modify existing YAML fields
  - Do not add metadata to files without a jenkinsfile reference

  **References**:
  - `jenkins-pipelines/oss/jepsen/`, `jenkins-pipelines/oss/kafka_connectors/`, `jenkins-pipelines/quarantined/` — for backend mapping

  **Acceptance Criteria**:
  - [ ] `lint-test-docs` returns 0 errors for every modified file
  - [ ] Every `description` is ≥20 characters and specific to the test (not generic)
  - [ ] `tier` is set for every file (not null)
  - [ ] `stress_tools` matches `stress_cmd` fields
  - [ ] `nemesis_labels` matches `nemesis_class_name`

  **Commit**: YES
  - Message: `docs(test-metadata): add test_metadata to jepsen, kafka, microbenchmarking, vector-search, cassandra, load, quarantined test cases`

- [ ] 5. PR — cdc + platform-migration

  **What to do**:
  - Same process as Task 4 for `test-cases/cdc/` and `test-cases/platform-migration/`
  - Estimated scope: ~7-10 CI-active files

  **Acceptance Criteria**:
  - [ ] `lint-test-docs` returns 0 errors for every modified file
  - [ ] `tier` assigned, descriptions specific, fields cross-referenced

  **Commit**: YES
  - Message: `docs(test-metadata): add test_metadata to cdc and platform-migration test cases`

- [ ] 6. PR — scale + upgrades

  **What to do**:
  - Same process for `test-cases/scale/` and `test-cases/upgrades/` (incl. `upgrades/customer-profile/`)
  - Estimated scope: ~10-14 CI-active files

  **Acceptance Criteria**:
  - [ ] `lint-test-docs` returns 0 errors for every modified file
  - [ ] `tier` assigned, descriptions specific, fields cross-referenced

  **Commit**: YES
  - Message: `docs(test-metadata): add test_metadata to scale and upgrades test cases`

- [ ] 7. PR — gemini

  **What to do**:
  - Same process for `test-cases/gemini/`
  - Estimated scope: ~10-13 CI-active files

  **Acceptance Criteria**:
  - [ ] `lint-test-docs` returns 0 errors for every modified file
  - [ ] `stress_tools` includes `gemini` for all applicable files
  - [ ] `tier` assigned, descriptions specific

  **Commit**: YES
  - Message: `docs(test-metadata): add test_metadata to gemini test cases`

- [ ] 8. PR — features (including nested subdirectories)

  **What to do**:
  - Same process for `test-cases/features/` including all nested subdirs:
    - `features/alternator-ttl/` (8 files)
    - `features/elasticity/` (7 files)
    - `features/out-of-space-prevention/` (5 files)
    - `features/tombstone_gc/`, `features/size-based-load-balancing/`
  - All use `test_type: features` regardless of subdirectory nesting
  - Estimated scope: ~12 CI-active files

  **Acceptance Criteria**:
  - [ ] `lint-test-docs` returns 0 errors for every modified file
  - [ ] All nested-subdir files use `test_type: features`
  - [ ] `features` field populated based on subdir context (e.g., alternator-ttl → alternator)

  **Commit**: YES
  - Message: `docs(test-metadata): add test_metadata to features test cases`

- [ ] 9. PR — manager

  **What to do**:
  - Same process for `test-cases/manager/` including `manager/prepare_snapshot/`
  - Estimated scope: ~15-18 CI-active files

  **Acceptance Criteria**:
  - [ ] `lint-test-docs` returns 0 errors for every modified file
  - [ ] `tier` assigned, descriptions specific

  **Commit**: YES
  - Message: `docs(test-metadata): add test_metadata to manager test cases`

- [ ] 10. PR — scylla-operator

  **What to do**:
  - Same process for `test-cases/scylla-operator/`
  - Uses `test_type: operator` (from taxonomy extension in Task 2)
  - Map backends from `jenkins-pipelines/operator/eks/` → k8s-eks, `operator/gke/` → k8s-gke
  - Estimated scope: ~18-22 CI-active files

  **Acceptance Criteria**:
  - [ ] `lint-test-docs` returns 0 errors for every modified file
  - [ ] `supported_backends` correctly reflects k8s-eks / k8s-gke from jenkinsfile structure

  **Commit**: YES
  - Message: `docs(test-metadata): add test_metadata to scylla-operator test cases`

- [ ] 11. PR — artifacts

  **What to do**:
  - Same process for `test-cases/artifacts/`
  - Most use `nemesis_class_name: NoOpMonkey` and `n_loaders: 0`
  - `stress_tools` should be `[]` and `workload` should be null for loader-less tests
  - Estimated scope: ~20-24 CI-active files

  **Acceptance Criteria**:
  - [ ] `lint-test-docs` returns 0 errors for every modified file
  - [ ] Files with `n_loaders: 0` have `stress_tools: []` (no false positives)

  **Commit**: YES
  - Message: `docs(test-metadata): add test_metadata to artifacts test cases`

- [ ] 12. PR — performance

  **What to do**:
  - Same process for `test-cases/performance/` including `performance/ycsb/`
  - Many use composite configs with overlays from `configurations/ebs/`, `configurations/perf-*`
  - Must merge overlay configs to get full stress_tools and feature picture
  - Estimated scope: ~30-35 CI-active files

  **Acceptance Criteria**:
  - [ ] `lint-test-docs` returns 0 errors for every modified file
  - [ ] Composite config overlays are accounted for in stress_tools/features

  **Commit**: YES
  - Message: `docs(test-metadata): add test_metadata to performance test cases`

- [ ] 13. PR — longevity (largest batch)

  **What to do**:
  - Same process for `test-cases/longevity/`
  - Largest folder: 77 files, most are CI-active
  - Many shared by multiple jenkinsfiles (different backends/regions)
  - Can split into sub-PRs if review load is too high (e.g., longevity A-L, longevity M-Z)
  - Estimated scope: ~60-70 CI-active files

  **Acceptance Criteria**:
  - [ ] `lint-test-docs` returns 0 errors for every modified file
  - [ ] `supported_backends` reflects all backends from all referencing jenkinsfiles
  - [ ] Multi-DC tests have `features: [multi-dc]`

  **Commit**: YES
  - Message: `docs(test-metadata): add test_metadata to longevity test cases`

- [ ] 14. CI enforcement — add lint-test-docs to pre-commit

  **What to do**:
  - Add `lint-test-docs --missing-only` check to `.pre-commit-config.yaml` (or CI pipeline)
  - Triggers on changes to `test-cases/**/*.yaml` files
  - Prevents new test-cases from being added without `test_metadata`
  - Optionally: add to the Jenkins precommit stage alongside existing lint-pipelines

  **Acceptance Criteria**:
  - [ ] Creating a new test-case YAML without `test_metadata` fails pre-commit
  - [ ] Existing files with `test_metadata` pass pre-commit
  - [ ] `uv run sct.py pre-commit` passes

  **Commit**: YES
  - Message: `ci(test-metadata): enforce test_metadata in pre-commit for new test cases`
  - Files: `.pre-commit-config.yaml`

---

## Final Verification Wave

- [ ] F1. **Coverage audit** — Run `lint-test-docs --missing-only` across all test-cases, confirm 0 missing among CI-active files
- [ ] F2. **Lint pass** — Run `lint-test-docs` on every modified file, confirm 0 errors
- [ ] F3. **Taxonomy consistency** — Confirm all `test_type`, `stress_tools`, `nemesis_labels` values in test-cases exist in `taxonomy.yaml`

---

## Commit Strategy

Each folder PR:
- Message: `docs(test-metadata): add test_metadata to <folder> test cases`
- Pre-commit: `uv run sct.py lint-test-docs --test-case-dir test-cases/<folder>/`

---

## Success Criteria

### Verification Commands
```bash
uv run sct.py lint-test-docs --missing-only  # Expected: 0 missing among CI-active
uv run sct.py lint-test-docs                 # Expected: 0 errors across all files
```

### Final Checklist
- [ ] All CI-active test-cases have `test_metadata` sections
- [ ] All sections pass `lint-test-docs` validation
- [ ] Pre-commit/CI blocks new test-cases without `test_metadata`
- [ ] Taxonomy covers all directories
