---
status: draft
domain: ci-cd
created: 2026-03-19
last_updated: 2026-03-19
owner: null
---

# Pipeline Labeling and Documentation Tools

## Problem Statement

SCT has ~997 Jenkins pipeline files and ~267 test-case YAML files with no structured metadata, labeling, or documentation system. This makes it difficult to:

1. **Categorize pipelines**: No way to query "which pipelines test CDC?", "which use encryption?", or "which run on GCE?" without manually reading each file. Teams waste time searching through filenames and file contents to find relevant tests.
2. **Document test purpose**: Test-case YAML files contain raw configuration (stress commands, node counts, nemesis classes) but no human-readable description of *what* the test validates or *why* it exists. New team members cannot understand a test's purpose without reading the full config and tracing it to its pipeline.
3. **Validate documentation quality**: When descriptions or labels do exist (e.g., in PR #12312's proposed `jobDescription` blocks), there is no automated way to check that they are complete, accurate, or consistent with the test configuration.
4. **Review at scale**: Manually reviewing ~997 pipelines for correct labeling and documentation is impractical. AI-assisted review through Copilot CLI and Claude Code skills can make this tractable.

### Prior Art: PR #12312

[PR #12312](https://github.com/scylladb/scylla-cluster-tests/pull/12312) proposed adding `/** jobDescription ... Labels: ... */` comment blocks to Jenkinsfiles with an `authorized_labels.yaml` whitelist. The reviewer raised concerns about the flat label structure being unwieldy and suggested hierarchical metadata. That PR was not merged.

This plan takes a different approach: instead of a custom comment format in Jenkinsfiles, we use **structured YAML metadata files** alongside test cases, and **AI agent skills** to review and lint them — making the system maintainable, queryable, and integrated with existing developer workflows.

## Current State

### Pipeline Files (`jenkins-pipelines/`, ~997 files)

Pipeline files follow a consistent Groovy pattern with named parameters:

```groovy
// jenkins-pipelines/oss/longevity/longevity-10gb-3h.jenkinsfile
longevityPipeline(
    backend: 'aws',
    region: 'eu-west-1',
    test_name: 'longevity_test.LongevityTest.test_custom_time',
    test_config: 'test-cases/longevity/longevity-10gb-3h.yaml'
)
```

Pipeline functions include: `longevityPipeline` (~600+), `perfRegressionParallelPipeline` (~55), `managerPipeline` (~34), `artifactsPipeline` (~32), `rollingUpgradePipeline` (~21), and others.

No structured labels or descriptions exist in pipeline files today.

The codebase has **infrastructure that supports** metadata but no actual metadata has been deployed:

- **`_display_name`** files — exist in 10+ pipeline directories (e.g., `jenkins-pipelines/oss/longevity/_display_name`), providing a folder display name only.
- **`_folder_definitions.yaml`** support — `utils/build_system/create_test_release_jobs.py` (line 167) can load `_folder_definitions.yaml` files that define `job-type`, `job-sub-type`, `folder-name`, and `folder-description` for Argus integration (documented in `docs/job_defines_format.md`). **However, no `_folder_definitions.yaml` files have been created yet** — this is one of the deliverables of the implementation.
- **`/** jobDescription */` comment block** support — `utils/build_system/create_test_release_jobs.py` (line 194) can extract `jobDescription` blocks from Jenkinsfiles and append them to Jenkins job descriptions. **However, no Jenkinsfiles currently contain `jobDescription` blocks** — adding them is part of the implementation.
- **`utils/build_system/create_test_release_jobs.py`** — the Jenkins job builder that generates Jenkins job XML. It already supports folder definitions and `jobDescription` extraction, so the plumbing exists.

The existing system provides only folder-level display names (`_display_name`) and lacks: per-test-case labels, folder definitions, job descriptions, stress tool/nemesis/feature metadata, a label taxonomy, cross-reference validation, and AI-assisted review.

### Test-Case YAML Files (`test-cases/`, ~267 files)

Test cases contain raw configuration parameters — no descriptions, labels, or documentation:

```yaml
# test-cases/longevity/longevity-10gb-3h.yaml
test_duration: 255
stress_cmd: ["cassandra-stress write cl=QUORUM duration=180m ..."]
n_db_nodes: 6
n_loaders: 2
instance_type_db: 'i4i.2xlarge'
nemesis_class_name: 'SisyphusMonkey'
nemesis_interval: 2
user_prefix: 'longevity-10gb-3h'
```

Categories exist via directory structure (`longevity/`, `performance/`, `manager/`, `upgrades/`, `scylla-operator/`, etc.) but not as machine-readable metadata.

### Configuration Fragments (`configurations/`, ~100+ files)

Reusable config fragments combined via `test_config` lists in pipelines:

```groovy
test_config: '''["test-cases/performance/perf-regression.yaml",
                 "configurations/disable_kms.yaml",
                 "configurations/tablets_disabled.yaml"]'''
```

These fragments also lack documentation about what they enable/disable.

### Existing Skills (`skills/`, 7 skills)

Skills exist for: `commit-summary`, `designing-skills`, `fix-backport-conflicts`, `profiling-sct-code`, `writing-plans`, `writing-unit-tests`, `writing-integration-tests`. No skill exists for pipeline documentation or test-case review.

### Existing Linting (`docs/plans/jenkins/jenkins-pipeline-config-linter.md`)

A plan exists for structural config validation (`sct.py lint-pipelines`). This plan complements it — structural linting validates config correctness; labeling validates documentation completeness and accuracy.

## Goals

1. **Define a structured metadata schema** for test-case YAML files that captures: description, labels (hierarchical), test category, stress tools used, features tested, and documentation links.
2. **Create a CLI tool** (`sct.py lint-test-docs`) that validates metadata completeness and label accuracy against an authorized taxonomy and the test configuration itself.
3. **Create two AI agent skills** — `reviewing-pipeline-docs` for reviewing/generating pipeline documentation, and `labeling-pipelines` for labeling/linting pipeline labels — usable from both Copilot CLI and Claude Code.
4. **Bootstrap metadata** for all ~267 test-case files using AI-assisted generation, with human review.
5. **Achieve >90% metadata coverage** across all test-case YAML files within the first release cycle.

## Implementation Phases

### Phase 1: Metadata Schema and Taxonomy (Importance: Critical)

**Objective**: Define the metadata format for test cases and the authorized label taxonomy.

**Deliverables**:

1. **Metadata sidecar files** — `<test-case>.meta.yaml` files alongside each test-case YAML:

```yaml
# test-cases/longevity/longevity-10gb-3h.meta.yaml
description: >-
  Basic longevity test running cassandra-stress write workload at QUORUM
  consistency for 3 hours on a 6-node cluster with SisyphusMonkey nemesis.
  Validates cluster stability under moderate write load with continuous
  chaos operations.

labels:
  test_type: longevity
  tier: tier2
  duration_class: short        # short (<6h), medium (6-24h), long (>24h)
  backends:
    - aws
  stress_tools:
    - cassandra-stress
  workload: write
  data_size: 10gb
  nemesis:
    - SisyphusMonkey
  features: []
  storage:
    - instance-store

pipelines:
  - jenkins-pipelines/oss/longevity/longevity-10gb-3h.jenkinsfile
```

2. **Authorized label taxonomy** — `docs/pipeline-labels/taxonomy.yaml`:

```yaml
# docs/pipeline-labels/taxonomy.yaml
# Hierarchical label taxonomy for SCT test cases.
# The lint tool validates that all labels in .meta.yaml files
# use values defined here.

test_type:
  description: "Primary test category"
  values:
    - longevity
    - performance
    - upgrade
    - artifacts
    - manager
    - functional
    - scale
    - jepsen
    - gemini
    - features
    - platform-migration
    - vector-search
    - cdc

tier:
  description: "Test priority tier (tier1 = highest priority, runs most frequently)"
  values:
    - tier1       # Sanity tests, run on every commit
    - tier2       # Core regression, run daily
    - tier3       # Extended regression, run weekly
    - tier4       # Long-running or niche, run on-demand

duration_class:
  description: "Test duration bucket"
  values:
    - short       # <6 hours
    - medium      # 6-24 hours
    - long        # >24 hours

backends:
  description: "Cloud/infra backends the test runs on"
  values:
    - aws
    - gce
    - azure
    - docker
    - k8s-eks
    - k8s-gke
    - k8s-local-kind
    - baremetal
    - xcloud

stress_tools:
  description: "Stress/load tools used in the test"
  values:
    - cassandra-stress
    - scylla-bench
    - ycsb
    - ndbench
    - nosqlbench
    - latte
    - gemini
    - harry
    - cdclog-reader

workload:
  description: "Primary workload type"
  values:
    - write
    - read
    - mixed
    - counter
    - lwt
    - cdc
    - mv             # materialized views
    - si              # secondary indexes
    - alternator
    - user-profile

nemesis:
  description: "Nemesis (chaos) classes used"
  values:
    - SisyphusMonkey
    - ChaosMonkey
    - LimitedChaosMonkey
    - ManagerOperationsMonkey
    - TopologyChangesMonkey
    - NetworkMonkey
    - SchemaChangesMonkey
    - CloudLimitedChaosMonkey
    - NoOpMonkey
    # ... (auto-populated from sdcm/nemesis_registry.py)

features:
  description: "Scylla features specifically tested"
  values:
    - encryption-at-rest
    - tls-ssl
    - authorization
    - cdc
    - tablets
    - vnodes
    - multi-dc
    - rack-aware
    - ipv6
    - kms
    - large-partitions
    - large-collections
    - sla
    - tombstone-gc
    - twcs                 # TimeWindowCompactionStrategy
    - zero-token-nodes
    - alternator-streams

storage:
  description: "Storage type used by DB nodes"
  values:
    - instance-store       # Local NVMe (i3, i4i, etc.)
    - ebs-gp3              # AWS EBS gp3
    - local-ssd            # GCE local SSD
    - managed-disk         # Azure managed disk
    - raid                 # Software RAID
```

3. **JSON schema for `.meta.yaml` validation** — `docs/pipeline-labels/meta-schema.json`:

A JSON Schema that `sct.py lint-test-docs` uses to validate the structure of every `.meta.yaml` file. Ensures required fields are present, types are correct, and `labels.*` values come from the taxonomy.

**Definition of Done**:
- [ ] `docs/pipeline-labels/taxonomy.yaml` defined with all label categories and values
- [ ] `docs/pipeline-labels/meta-schema.json` JSON Schema for `.meta.yaml` files
- [ ] 5 example `.meta.yaml` files created for representative test cases (one per test type: longevity, performance, manager, upgrade, operator)
- [ ] Schema validates all 5 examples successfully

**Dependencies**: None.

---

### Phase 2: CLI Lint Tool (`sct.py lint-test-docs`) (Importance: Critical)

**Objective**: Create a CLI command that validates `.meta.yaml` files for completeness, schema conformance, and cross-references against the test-case YAML and taxonomy.

**Deliverables**:

1. **New module**: `sdcm/utils/lint/test_docs_linter.py`

```python
@dataclass
class LintResult:
    file_path: str
    errors: list[str]      # Schema violations, missing required fields
    warnings: list[str]    # Suggestions (e.g., "nemesis_class_name is SisyphusMonkey but labels.nemesis does not include it")

def lint_meta_file(meta_path: Path, taxonomy: dict) -> LintResult:
    """Validate a single .meta.yaml file."""
    ...

def cross_reference_config(meta_path: Path, config_path: Path) -> list[str]:
    """Check that labels match the actual test configuration.

    Examples of cross-reference checks:
    - If config has nemesis_class_name: 'SisyphusMonkey', labels.nemesis should include 'SisyphusMonkey'
    - If config has stress_cmd containing 'cassandra-stress', labels.stress_tools should include 'cassandra-stress'
    - If config has n_db_nodes: '15 15 15' (multi-DC), labels.features should include 'multi-dc'
    - If meta lists backend 'aws' but pipeline file says backend: 'gce', flag mismatch
    """
    ...
```

2. **New CLI command** in `sct.py`:

```python
@cli.command("lint-test-docs", help="Validate test case documentation and labels")
@click.option("--test-case-dir", default="test-cases", help="Root directory of test cases")
@click.option("--fix", is_flag=True, help="Auto-fix trivially correctable issues")
@click.option("--missing-only", is_flag=True, help="Only report test cases missing .meta.yaml files")
def lint_test_docs(test_case_dir, fix, missing_only):
    ...
```

3. **Validation rules**:

| Rule ID | Check | Severity |
|---------|-------|----------|
| TD-001 | `.meta.yaml` exists for every `.yaml` test case | error |
| TD-002 | `description` field is non-empty and >20 characters | error |
| TD-003 | All label values exist in `taxonomy.yaml` | error |
| TD-004 | `test_type` matches the test-case directory name | warning |
| TD-005 | `nemesis` labels match `nemesis_class_name` in config | warning |
| TD-006 | `stress_tools` labels match stress commands in config | warning |
| TD-007 | `backends` labels match pipeline `backend:` parameter | warning |
| TD-008 | `duration_class` is consistent with `test_duration` value | warning |
| TD-009 | `data_size` label is consistent with test name/config | info |
| TD-010 | `pipelines` list references existing `.jenkinsfile` files | error |
| TD-011 | `features` labels are consistent with config (e.g., multi-dc detected from `n_db_nodes: '15 15 15'`) | warning |

**Output format**:
```
ERROR test-cases/longevity/longevity-10gb-3h.yaml: TD-001 Missing .meta.yaml file
WARN  test-cases/longevity/longevity-cdc-100gb-4h.meta.yaml: TD-005 Config has nemesis_class_name='ChaosMonkey' but labels.nemesis is ['SisyphusMonkey']
WARN  test-cases/longevity/longevity-cdc-100gb-4h.meta.yaml: TD-006 Config stress_cmd contains 'cassandra-stress' but labels.stress_tools is empty

---
245/267 test cases have .meta.yaml (92% coverage)
3 errors, 12 warnings
```

**Auto-fix (`--fix`)** capabilities:
- TD-005: Add missing nemesis labels from config
- TD-006: Add missing stress_tool labels from config
- TD-008: Correct `duration_class` based on `test_duration` value
- TD-011: Add `multi-dc` feature when multi-DC config detected

**Definition of Done**:
- [ ] `sct.py lint-test-docs` validates all `.meta.yaml` files
- [ ] All 11 validation rules implemented
- [ ] `--fix` auto-corrects TD-005, TD-006, TD-008, TD-011
- [ ] `--missing-only` reports coverage gaps
- [ ] Exit code 1 on errors, 0 on clean (warnings don't fail)
- [ ] Unit tests cover all validation rules with positive and negative cases

**Dependencies**: Phase 1 (schema and taxonomy).

---

### Phase 3: AI Skill — `reviewing-pipeline-docs` (Importance: High)

**Objective**: Create a skill that helps developers review and generate `.meta.yaml` documentation for test cases, usable from both Copilot CLI and Claude Code.

**Deliverables**:

1. **Skill directory**: `skills/reviewing-pipeline-docs/`

```
skills/reviewing-pipeline-docs/
├── SKILL.md
├── references/
│   ├── taxonomy-guide.md         # How to choose correct labels
│   └── description-examples.md   # Good vs bad description examples
└── workflows/
    ├── review-test-case.md       # Review an existing .meta.yaml
    └── generate-meta.md          # Generate .meta.yaml from scratch
```

2. **SKILL.md** frontmatter:

```yaml
---
name: reviewing-pipeline-docs
description: >-
  Review, generate, and improve .meta.yaml documentation files for SCT test cases.
  Use when creating documentation for test-cases/ YAML files, reviewing existing
  .meta.yaml files for accuracy, generating descriptions from test configuration,
  or batch-documenting undocumented test cases. Applies to test-case YAML files,
  pipeline Jenkinsfiles, and configuration fragments.
---
```

3. **Workflow: `generate-meta.md`** — step-by-step process for generating a `.meta.yaml` file:

```markdown
## Phase 1: Read the Test Configuration

1. Read the test-case YAML file
2. Read ALL configuration fragments listed in the pipeline's `test_config`
3. Read the pipeline `.jenkinsfile` to extract backend, region, and other parameters
4. Note: configuration precedence is pipeline env vars > last YAML file > first YAML file > defaults

## Phase 2: Extract Facts

From the configuration, extract:
- **Stress tool**: Parse `stress_cmd` for tool name (cassandra-stress, scylla-bench, ycsb, etc.)
- **Nemesis**: Read `nemesis_class_name` and `nemesis_selector`
- **Duration**: Read `test_duration` (minutes) and classify as short/medium/long
- **Data size**: Infer from test name or stress parameters
- **Multi-DC**: Detect from `n_db_nodes` format ('6' = single-DC, '15 15 15' = 3 DCs)
- **Features**: Detect from config keys (encryption, authorization, cdc, tablets, etc.)
- **Backend**: Read from pipeline `backend:` parameter
- **Storage**: Infer from `instance_type_db` (i3/i4i = instance-store, etc.)

## Phase 3: Write Description

Write a 2-4 sentence description that answers:
1. What workload does this test run? (tool, operation, consistency level)
2. What cluster topology? (node count, DCs, special nodes)
3. What chaos/nemesis operations? (nemesis class)
4. What is the test validating? (stability, performance, feature correctness)

**Do NOT mention provisioning details** like spot/on-demand instance types in descriptions.
These are infrastructure cost decisions, not test semantics — they can change without
affecting what the test validates and add noise to descriptions.

## Phase 4: Assemble and Validate

1. Create the .meta.yaml with all extracted labels
2. Run `sct.py lint-test-docs --file <path>` to validate
3. Fix any errors or warnings
```

4. **Reference: `description-examples.md`** — concrete good/bad examples:

```markdown
## Good Descriptions

### Example 1: Longevity test
**Config**: longevity-10gb-3h.yaml (6 nodes, cassandra-stress write, SisyphusMonkey)
**Description**:
> Basic longevity test running cassandra-stress write workload at QUORUM
> consistency for 3 hours on a 6-node cluster with SisyphusMonkey nemesis.
> Validates cluster stability under moderate write load with continuous
> chaos operations.

### Example 2: Multi-DC CDC test
**Config**: longevity-cdc-100gb-8h-multi-dc-large-cluster.yaml (45 nodes, 3 DCs)
**Description**:
> Large-scale CDC longevity test across 3 datacenters (15 nodes each) running
> 4 CDC workload profiles with pre-image and post-image variants. Runs for
> 8 hours with SisyphusMonkey topology-changes nemesis to validate CDC
> replication consistency under multi-DC topology disruptions.

### Example 3: Performance regression test
**Config**: perf-regression-predefined-throughput-steps.yaml
**Description**:
> Performance regression test measuring read, write, and mixed throughput at
> predefined load steps. Compares latency and throughput against baseline to
> detect performance regressions. Uses dedicated instances to minimize
> measurement noise.

## Bad Descriptions (and why)

### Bad: Too vague
> "Tests longevity."
**Why**: Doesn't say what workload, what cluster, or what it validates.

### Bad: Just restating the config
> "Runs cassandra-stress with n_db_nodes=6 and nemesis_interval=2."
**Why**: Repeats config values without explaining purpose.

### Bad: Mentions provisioning details
> "Installs from repo on a GCE spot instance."
**Why**: Spot vs on-demand is an infrastructure cost decision, not a test property.
It can change without affecting what the test validates.

### Bad: Too long
> "This test was created in 2022 to address concerns about cluster stability..."
**Why**: History doesn't help understand current purpose. Keep to 2-4 sentences.
```

5. **Reference: `taxonomy-guide.md`** — decision trees for choosing labels:

```markdown
## Choosing `test_type`

- Does the test run a stress workload for hours? → `longevity`
- Does the test measure latency/throughput against a baseline? → `performance`
- Does the test upgrade Scylla versions? → `upgrade`
- Does the test install/verify packages? → `artifacts`
- Does the test exercise Scylla Manager operations? → `manager`
- Does the test run on Kubernetes? → Check: is it a functional test (pytest-based)? → `functional`. Otherwise use the primary type (longevity, performance, etc.)

## Choosing `tier`

- tier1: Tests that run on every commit or PR. Usually <1h, sanity-level.
- tier2: Core regression suite. Runs daily. 1-12h duration.
- tier3: Extended regression. Runs weekly. 12-48h duration.
- tier4: Long-running, niche, or experimental. On-demand only.

## Detecting `features` from config

| Config Key/Value | Feature Label |
|------------------|---------------|
| `n_db_nodes: 'X Y Z'` (space-separated) | `multi-dc` |
| `authenticator: PasswordAuthenticator` | `authorization` |
| `client_encrypt: true` or `server_encrypt: true` | `tls-ssl` |
| `alternator_port` present or `alternator` in stress_cmd | `alternator-streams` or check further |
| `tablets` in config fragment name or `tablets_enabled: true` | `tablets` |
| `encryption_at_rest_enabled: true` | `encryption-at-rest` |
| `cdc` in test name or `cdc_enabled: true` | `cdc` |
```

**Definition of Done**:
- [ ] Skill directory created with SKILL.md, 2 references, 2 workflows
- [ ] SKILL.md under 500 lines, detailed content in references/workflows
- [ ] Description triggers correctly on 5+ should-trigger and 3+ should-NOT-trigger queries
- [ ] Registered in both `AGENTS.md` and `CLAUDE.md`
- [ ] Includes 3+ concrete input→output examples
- [ ] Follows SCT skill conventions from `skills/designing-skills/SKILL.md`

**Dependencies**: Phase 1 (taxonomy), Phase 2 (lint tool for validation step in workflow).

---

### Phase 4: AI Skill — `labeling-pipelines` (Importance: High)

**Objective**: Create a skill for linting and correcting pipeline labels at scale — reviewing existing `.meta.yaml` files for accuracy and suggesting fixes.

**Deliverables**:

1. **Skill directory**: `skills/labeling-pipelines/`

```
skills/labeling-pipelines/
├── SKILL.md
├── references/
│   ├── cross-reference-rules.md    # How labels must match config
│   └── common-mistakes.md          # Frequent labeling errors
└── workflows/
    ├── lint-and-fix.md             # Lint labels, fix issues
    └── batch-label.md             # Label multiple test cases
```

2. **SKILL.md** frontmatter:

```yaml
---
name: labeling-pipelines
description: >-
  Lint, validate, and fix labels in SCT test-case .meta.yaml files.
  Use when checking label accuracy against test configuration, finding
  mislabeled pipelines, batch-labeling undocumented test cases, or
  auditing label coverage across the test suite. Covers taxonomy
  validation, cross-reference checking, and automated fixes.
---
```

3. **Workflow: `lint-and-fix.md`** — process for reviewing and fixing labels:

```markdown
## Phase 1: Run the Linter

1. Run `sct.py lint-test-docs` to get current state
2. Note: errors (TD-001 through TD-003, TD-010) must be fixed; warnings are suggestions

## Phase 2: Fix Errors

For each error:
- **TD-001 (missing .meta.yaml)**: Use the `reviewing-pipeline-docs` skill's
  `generate-meta.md` workflow to create the file
- **TD-002 (empty description)**: Read the test config and write a description
- **TD-003 (invalid label)**: Check `docs/pipeline-labels/taxonomy.yaml` for valid values.
  If the label is genuinely new, add it to taxonomy.yaml with a description.
- **TD-010 (broken pipeline reference)**: Find the correct .jenkinsfile path or remove
  the reference if the pipeline was deleted

## Phase 3: Address Warnings

For each warning:
- **TD-005 (nemesis mismatch)**: Read `nemesis_class_name` from config, update labels
- **TD-006 (stress tool mismatch)**: Parse `stress_cmd` from config, update labels
- **TD-007 (backend mismatch)**: Read pipeline file's `backend:` parameter, update labels
- **TD-008 (duration class wrong)**: Calculate from `test_duration` in config
- **TD-011 (feature detection)**: Check config for multi-dc, encryption, CDC, etc.

## Phase 4: Verify

1. Run `sct.py lint-test-docs` again — should show 0 errors
2. Run `sct.py pre-commit` to check formatting
```

4. **Workflow: `batch-label.md`** — process for labeling many test cases:

```markdown
## Phase 1: Identify Gaps

Run `sct.py lint-test-docs --missing-only` to find all test cases without .meta.yaml.
Group them by directory (test type).

## Phase 2: Process by Category

For each category (e.g., all longevity/ tests):
1. Read 2-3 existing .meta.yaml files in the category to understand the pattern
2. For each unlabeled test case:
   a. Read the test-case YAML
   b. Find the corresponding .jenkinsfile (search jenkins-pipelines/ for the YAML path)
   c. Extract facts and generate .meta.yaml
   d. Validate with `sct.py lint-test-docs --file <path>`

## Phase 3: Validate Batch

Run `sct.py lint-test-docs` on the full suite to verify no new errors.
Report coverage improvement: "Added metadata for X test cases (Y% → Z% coverage)"
```

5. **Reference: `common-mistakes.md`**:

```markdown
## Mistake 1: Labeling based on filename instead of config

**Wrong**: File is `longevity-cdc-100gb-4h.yaml`, so labels.features = ['cdc']
**Right**: Read the config — does it actually have CDC-specific stress profiles
or `cdc_enabled: true`? The filename is often correct, but always verify.

## Mistake 2: Missing multi-DC detection

**Wrong**: `n_db_nodes: '15 15 15'` but labels.features doesn't include 'multi-dc'
**Right**: Space-separated n_db_nodes always means multi-DC. Add the label.

## Mistake 3: Wrong duration_class

**Wrong**: test_duration=720 (minutes) labeled as 'short'
**Right**: 720 minutes = 12 hours → 'medium' (6-24h range)

## Mistake 4: Listing all possible backends instead of actual

**Wrong**: backends: [aws, gce, azure] because the config has instance types for all
**Right**: Check the pipeline file — it says backend: 'aws'. The GCE/Azure instance
types are fallback configs, not active backends for this pipeline.
```

**Definition of Done**:
- [ ] Skill directory created with SKILL.md, 2 references, 2 workflows
- [ ] SKILL.md under 500 lines
- [ ] Description triggers correctly on 5+ should-trigger and 3+ should-NOT-trigger queries
- [ ] Registered in both `AGENTS.md` and `CLAUDE.md`
- [ ] Cross-references the `reviewing-pipeline-docs` skill where appropriate
- [ ] Follows SCT skill conventions

**Dependencies**: Phase 1 (taxonomy), Phase 2 (lint tool), Phase 3 (reviewing skill for cross-references).

---

### Phase 5: Bootstrap Metadata for Existing Test Cases (Importance: Medium)

**Objective**: Generate `.meta.yaml` files for all ~267 test cases using AI-assisted generation and the skills from Phases 3-4.

**Deliverables**:

1. **Bootstrap script**: `utils/bootstrap_test_metadata.py`
   - Reads each test-case YAML, finds its pipeline(s), extracts facts
   - Generates a draft `.meta.yaml` with auto-detectable labels (nemesis, stress tool, duration, multi-dc, backend)
   - Leaves `description` as a placeholder: `"TODO: Add description"`
   - Runs `lint-test-docs` validation on each generated file
   - Outputs a report of what was generated and what needs human review

2. **Human review process**:
   - AI skills (`reviewing-pipeline-docs`, `labeling-pipelines`) assist in writing descriptions and fixing label warnings
   - PRs are split by test-case category (one PR per directory: longevity, performance, manager, etc.)

3. **Coverage target**: >90% of test cases have `.meta.yaml` with complete labels; >70% have human-reviewed descriptions.

**Definition of Done**:
- [ ] Bootstrap script generates draft `.meta.yaml` for all test cases
- [ ] Auto-detected labels pass lint validation (0 errors from TD-003, TD-005, TD-006, TD-008, TD-011)
- [ ] Human-written descriptions for at least 3 categories (longevity, performance, manager)
- [ ] `sct.py lint-test-docs` shows >90% coverage

**Dependencies**: Phase 2 (lint tool), Phase 3 and 4 (skills for human review).

---

### Phase 6: CI Integration and Pre-commit Hook (Importance: Medium)

**Objective**: Enforce documentation standards in CI so new test cases require `.meta.yaml` and label changes are validated.

**Deliverables**:

1. **Pre-commit hook**: Add `sct.py lint-test-docs` to `.pre-commit-config.yaml`
   - Runs only on changed `.meta.yaml` and `.yaml` files in `test-cases/`
   - Validates schema, taxonomy, and cross-references

2. **CI stage**: Add `lint-test-docs` stage to `Jenkinsfile`
   - Runs after `lint-pipelines` (from the pipeline config linter plan)
   - Fails the build on errors (TD-001 for new test cases, TD-002, TD-003, TD-010)
   - Warnings are reported but don't fail the build

3. **PR template update**: Add checklist item:
   - "If adding a new test case, `.meta.yaml` file is included with description and labels"

**Definition of Done**:
- [ ] Pre-commit hook validates `.meta.yaml` on commit
- [ ] CI stage runs `sct.py lint-test-docs` and fails on errors
- [ ] PR template updated with documentation checklist item

**Dependencies**: Phase 2 (lint tool), Phase 5 (bootstrap complete so existing files don't cause CI failures).

---

### Phase 7: Documentation Update (Importance: Low)

**Objective**: Document the labeling system, taxonomy, and skills for the team.

**Deliverables**:

1. **User guide**: `docs/pipeline-labels/README.md`
   - What is the labeling system and why it exists
   - How to add `.meta.yaml` for a new test case
   - How to add new labels to the taxonomy
   - How to use the AI skills for review

2. **Update `AGENTS.md`**: Add both new skills to the Skills table
3. **Update `CLAUDE.md`**: Add `@skills/reviewing-pipeline-docs/SKILL.md` and `@skills/labeling-pipelines/SKILL.md`

**Definition of Done**:
- [ ] `docs/pipeline-labels/README.md` created
- [ ] `AGENTS.md` and `CLAUDE.md` updated with new skills

**Dependencies**: All previous phases.

## Testing Requirements

### Unit Tests (Phase 2)

File: `unit_tests/test_test_docs_linter.py`

```python
import pytest
from pathlib import Path
from sdcm.utils.lint.test_docs_linter import lint_meta_file, cross_reference_config, LintResult

@pytest.fixture
def taxonomy(tmp_path):
    """Create a minimal taxonomy for testing."""
    taxonomy_file = tmp_path / "taxonomy.yaml"
    taxonomy_file.write_text("""
test_type:
  values: [longevity, performance, upgrade]
tier:
  values: [tier1, tier2, tier3]
backends:
  values: [aws, gce, azure]
stress_tools:
  values: [cassandra-stress, scylla-bench]
nemesis:
  values: [SisyphusMonkey, ChaosMonkey]
""")
    return taxonomy_file

def test_td001_missing_meta_file(tmp_path, taxonomy):
    """TD-001: Report error when .meta.yaml is missing."""
    config = tmp_path / "test.yaml"
    config.write_text("test_duration: 60")
    result = lint_meta_file(tmp_path / "test.meta.yaml", taxonomy)
    assert any("TD-001" in e for e in result.errors)

def test_td003_invalid_label_value(tmp_path, taxonomy):
    """TD-003: Report error when label value is not in taxonomy."""
    meta = tmp_path / "test.meta.yaml"
    meta.write_text("""
description: "A valid description for testing"
labels:
  test_type: nonexistent_type
  tier: tier1
  backends: [aws]
""")
    result = lint_meta_file(meta, taxonomy)
    assert any("TD-003" in e for e in result.errors)

def test_td005_nemesis_mismatch(tmp_path):
    """TD-005: Warn when nemesis label doesn't match config."""
    config = tmp_path / "test.yaml"
    config.write_text("nemesis_class_name: 'ChaosMonkey'")
    meta = tmp_path / "test.meta.yaml"
    meta.write_text("""
description: "A valid test description"
labels:
  nemesis: [SisyphusMonkey]
""")
    warnings = cross_reference_config(meta, config)
    assert any("TD-005" in w for w in warnings)

def test_td006_stress_tool_detection(tmp_path):
    """TD-006: Warn when stress_tool label doesn't match stress_cmd."""
    config = tmp_path / "test.yaml"
    config.write_text('stress_cmd: ["cassandra-stress write ..."]')
    meta = tmp_path / "test.meta.yaml"
    meta.write_text("""
description: "A valid test description"
labels:
  stress_tools: []
""")
    warnings = cross_reference_config(meta, config)
    assert any("TD-006" in w for w in warnings)

def test_td011_multi_dc_detection(tmp_path):
    """TD-011: Warn when multi-DC config not reflected in features."""
    config = tmp_path / "test.yaml"
    config.write_text("n_db_nodes: '15 15 15'")
    meta = tmp_path / "test.meta.yaml"
    meta.write_text("""
description: "A valid test description"
labels:
  features: []
""")
    warnings = cross_reference_config(meta, config)
    assert any("TD-011" in w or "multi-dc" in w for w in warnings)

def test_valid_meta_passes(tmp_path, taxonomy):
    """A fully valid .meta.yaml should produce no errors."""
    meta = tmp_path / "test.meta.yaml"
    meta.write_text("""
description: "Basic longevity test with cassandra-stress write workload"
labels:
  test_type: longevity
  tier: tier2
  backends: [aws]
  stress_tools: [cassandra-stress]
  nemesis: [SisyphusMonkey]
pipelines:
  - jenkins-pipelines/oss/longevity/longevity-10gb-3h.jenkinsfile
""")
    result = lint_meta_file(meta, taxonomy)
    assert len(result.errors) == 0
```

### Integration Tests (Phase 2, Phase 5)

- Run `sct.py lint-test-docs` against the bootstrapped metadata
- Verify cross-reference checks work with real test-case configs

### Skill Validation (Phase 3, Phase 4)

- Trigger eval queries (5+ should-trigger, 3+ should-NOT-trigger per skill)
- Test workflows against 3 representative test cases per skill

## Success Criteria

1. **Schema defined**: `taxonomy.yaml` covers all test dimensions with hierarchical labels
2. **Lint tool works**: `sct.py lint-test-docs` validates schema, taxonomy, and cross-references with 11 rules
3. **Skills functional**: Both AI skills trigger correctly and produce accurate `.meta.yaml` files
4. **Coverage >90%**: At least 240 of 267 test cases have validated `.meta.yaml` files
5. **CI enforced**: New test cases without `.meta.yaml` fail CI
6. **Zero taxonomy violations**: All `.meta.yaml` files use only values from `taxonomy.yaml`

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Taxonomy too rigid — new tests need labels not in taxonomy | High | Low | Allow adding values to taxonomy.yaml via PR; lint tool reports unknown values as errors, guiding taxonomy updates |
| AI-generated descriptions are inaccurate | Medium | Medium | Human review required for descriptions; auto-generated labels can be verified by lint tool cross-references |
| `.meta.yaml` files drift out of sync with config changes | Medium | Medium | CI pre-commit hook catches mismatches on every PR; cross-reference rules (TD-005 through TD-011) detect drift |
| Team resistance to maintaining metadata | Medium | High | Bootstrap script minimizes initial effort; AI skills reduce ongoing maintenance burden; CI enforcement makes it part of the normal workflow |
| Skill description triggers false positives on unrelated tasks | Low | Low | Test trigger eval queries during Phase 3-4; refine description based on results |

## Appendix: Batch Operations

This section describes how to run labeling and description updates in bulk — either using the AI skills (once built) or directly via Claude Code / Copilot prompts.

### Bulk-Adding `jobDescription` Blocks to Pipeline Files

To add `jobDescription` comment blocks to all pipelines in a folder, use a Claude Code or Copilot prompt scoped to a single directory. Each folder should be one commit.

**Claude Code prompt example** (run from repo root):

```
Add /** jobDescription ... */ comment blocks to every .jenkinsfile in
jenkins-pipelines/oss/artifacts/. For each file:
1. Read the pipeline parameters (test_config, backend, region, special flags)
2. Write a 1-2 sentence description explaining what the pipeline verifies
   (OS, architecture, backend, any special mode like FIPS/SELinux/web-install)
3. Do NOT mention spot/on-demand provisioning — these are cost decisions, not test semantics
4. Place the block between the library import and the pipeline function call
5. Do NOT modify any pipeline parameters
Commit all changes in a single commit scoped to this folder.
```

**Adapt per folder** by changing the directory path:

| Folder | Prompt adjustment |
|--------|------------------|
| `jenkins-pipelines/oss/longevity/` | Describe workload, cluster size, nemesis, and duration |
| `jenkins-pipelines/oss/performance/` | Describe benchmark type, baseline comparison, and instance sizing |
| `jenkins-pipelines/oss/upgrades/` | Describe upgrade path (from → to version), rolling vs full restart |
| `jenkins-pipelines/oss/manager/` | Describe which Manager operations are tested (backup, repair, etc.) |
| `jenkins-pipelines/oss/artifacts-offline-install/` | Same as artifacts but note offline/nonroot install mode |
| `jenkins-pipelines/operator/` | Describe K8s operator test scenario |

**Commit convention**: One commit per folder, message format:
```
docs(pipelines): add jobDescription to <folder-name> pipelines
```

### Bulk-Generating `.meta.yaml` Files (Requires Phase 1-2)

Once the taxonomy and lint tool exist, generate metadata for an entire test-case directory:

**Using the bootstrap script** (Phase 5):

```bash
# Generate draft .meta.yaml for all artifact test cases
python utils/bootstrap_test_metadata.py --dir test-cases/artifacts/

# Validate the generated files
uv run sct.py lint-test-docs --test-case-dir test-cases/artifacts/

# Review and fix warnings
uv run sct.py lint-test-docs --test-case-dir test-cases/artifacts/ --fix
```

**Using Claude Code / Copilot** (works before or after Phase 5):

```
Generate .meta.yaml sidecar files for every test-case YAML in test-cases/artifacts/.
For each file:
1. Read the test-case YAML to extract nemesis, stress tools, duration, node count
2. Find the corresponding .jenkinsfile(s) that reference this test case
3. Read the pipeline to get backend, region, and special configuration
4. Write a .meta.yaml following the schema in docs/pipeline-labels/meta-schema.json
5. Validate labels against docs/pipeline-labels/taxonomy.yaml
Commit all .meta.yaml files in a single commit scoped to this directory.
```

### Batch Validation and Fixing

**Check coverage across the entire repo:**

```bash
uv run sct.py lint-test-docs --missing-only
```

**Auto-fix detectable issues:**

```bash
uv run sct.py lint-test-docs --fix
```

**Fix remaining issues with AI assistance:**

```
Run sct.py lint-test-docs and fix all warnings for test-cases/longevity/.
For each warning:
- TD-005: Read nemesis_class_name from the test config and update labels.nemesis
- TD-006: Parse stress_cmd from the test config and update labels.stress_tools
- TD-007: Read backend from the pipeline file and update labels.backends
- TD-008: Calculate duration_class from test_duration (short <6h, medium 6-24h, long >24h)
- TD-011: Check for multi-dc config and update labels.features
Commit fixes in a single commit.
```

### Recommended Batch Order

Process folders in this order to build confidence and establish patterns:

1. **`artifacts/`** — simplest pipelines (single function, few parameters), good for establishing the pattern
2. **`artifacts-offline-install/`** — similar to artifacts, adds offline/nonroot variant
3. **`manager/`** — moderate complexity, distinct Manager operations
4. **`longevity/`** — most files (~600+), process in sub-batches of ~50 files
5. **`performance/`** — requires careful description of benchmark methodology
6. **`upgrades/`** — requires documenting version upgrade paths
7. **`operator/`** — K8s-specific terminology
