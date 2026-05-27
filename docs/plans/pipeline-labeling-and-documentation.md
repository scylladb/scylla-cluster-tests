---
status: draft
domain: ci-cd
created: 2026-03-19
last_updated: 2026-05-25
owner: fruch
---

# Pipeline Labeling and Documentation Tools

## Problem Statement

SCT has ~997 Jenkins pipeline files and ~267 test-case YAML files with no structured metadata, labeling, or documentation system. This makes it difficult to:

1. **Categorize pipelines**: No way to query "which pipelines test CDC?", "which use encryption?", or "which run on GCE?" without manually reading each file. Teams waste time searching through filenames and file contents to find relevant tests.
2. **Document test purpose**: Test-case YAML files contain raw configuration (stress commands, node counts, nemesis classes) but no human-readable description of *what* the test validates or *why* it exists. New team members cannot understand a test's purpose without reading the full config and tracing it to its pipeline.
3. **Validate documentation quality**: When descriptions or labels do exist (e.g., in PR #12312's proposed `jobDescription` blocks), there is no automated way to check that they are complete, accurate, or consistent with the test configuration.
4. **Pass metadata to Argus**: Test metadata (description, labels, tier) is not propagated to Argus, making it hard to categorize and search test runs in the Argus UI.
5. **Review at scale**: Manually reviewing ~997 pipelines for correct labeling and documentation is impractical. AI-assisted review through Copilot CLI and Claude Code skills can make this tractable.

### Prior Art: PR #12312

[PR #12312](https://github.com/scylladb/scylla-cluster-tests/pull/12312) proposed adding `/** jobDescription ... Labels: ... */` comment blocks to Jenkinsfiles with an `authorized_labels.yaml` whitelist. The reviewer raised concerns about the flat label structure being unwieldy and suggested hierarchical metadata. That PR was not merged.

This plan takes a different approach: **embed structured metadata directly in test-case YAML files** as first-class SCT configuration fields, validated by a pydantic model. This keeps everything in one file, avoids drift, and enables the metadata to flow through SCT config loading to Argus.

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

### Argus Integration (Current Flow)

Today, test metadata flows to Argus as follows:

1. **Jenkins job creation** (`create_test_release_jobs.py`): Extracts `jobDescription` comment blocks and `_folder_definitions.yaml` → embeds in Jenkins job XML description field.
2. **Test execution** (`sdcm/tester.py` → `init_argus_run()`): Submits `job_name`, `job_url`, `sct_config` (full config dict), git info to Argus via `ArgusSCTClient.submit_sct_run()`.
3. **Config dump**: `sct_submit_config()` sends the entire merged config as JSON.

**Gap**: The Jenkins job description (with folder definitions and `jobDescription` text) is stored in Jenkins but **never reaches Argus**. Argus receives the raw config dict but has no structured way to extract test purpose, tier, or labels from it.

### Existing Skills (`skills/`, 7 skills)

Skills exist for: `commit-summary`, `designing-skills`, `fix-backport-conflicts`, `profiling-sct-code`, `writing-plans`, `writing-unit-tests`, `writing-integration-tests`. No skill exists for pipeline documentation or test-case review.

### Existing Linting (`docs/plans/jenkins/jenkins-pipeline-config-linter.md`)

A plan exists for structural config validation (`sct.py lint-pipelines`). This plan complements it — structural linting validates config correctness; labeling validates documentation completeness and accuracy.

## Goals

1. **Embed structured metadata in test-case YAML files** as first-class SCT config fields, validated by a pydantic model — capturing: description, labels (hierarchical), test tier, stress tools used, features tested, and supported backends.
2. **Create a CLI tool** (`sct.py lint-test-docs`) that validates metadata completeness and label accuracy against an authorized taxonomy and the test configuration itself.
3. **Create two AI agent skills** — `reviewing-pipeline-docs` for reviewing/generating pipeline documentation, and `labeling-pipelines` for labeling/linting pipeline labels — usable from both Copilot CLI and Claude Code.
4. **Flow metadata to Argus** — ensure test descriptions, labels, and tier are submitted to Argus at test runtime and available in the Argus UI for categorization and search.
5. **Bootstrap metadata** for all ~267 test-case files using AI-assisted generation, with human review.
6. **Achieve >90% metadata coverage** across all test-case YAML files within the first release cycle.

### Design Decisions

**Why embed metadata directly in test-case YAML (not sidecar `.meta.yaml` files)?**

We embed metadata directly in the test-case YAML because:
- **Single file to maintain**: Developers edit one file, not two. No drift between config and metadata.
- **SCT config is pydantic-based**: `SCTConfiguration` already uses pydantic `BaseModel` with `SctField` descriptors. Adding metadata fields is natural — they get validated on config load, appear in `dump_help_config_markdown()`, and flow through the standard config merge pipeline.
- **Argus integration is free**: Since `submit_sct_run()` already sends `sct_config` (the full merged config dict) to Argus, metadata fields are automatically available in Argus without additional plumbing.
- **No config loading changes needed**: New fields are defined in `SCTConfiguration` with defaults (`None` or `[]`). Existing test-case YAMLs without metadata simply get `None` values — no breakage.

The old plan proposed sidecar `.meta.yaml` files. This was rejected after review feedback (PR #14101 comments by @pehala) pointing out that two files per test case doubles maintenance burden and frequently desyncs.

**Relationship to `jenkins-pipeline-config-linter.md` plan:**

The two plans share the `sdcm/utils/lint/` package but have distinct responsibilities:
- **Config linter**: Validates structural correctness of pipeline configs (type mismatches, missing required params, invalid values). Produces `PipelineConfig` parsed objects.
- **This plan**: Validates documentation completeness and label accuracy against taxonomy and config content.

This plan's cross-reference checks (TD-007, TD-007b) will import and reuse the config linter's `PipelineConfig` parser to read backend/region from Jenkinsfiles rather than re-implementing Groovy parsing. Phase 2 of this plan has a soft dependency on Phase 1 of the config linter plan for the parser module. If the linter plan is not yet implemented, a minimal Jenkinsfile parser (regex-based, covering `backend:` extraction) will be used as a stopgap.

Both linters run as separate `sct.py` subcommands and can share a CI stage.

**Nemesis taxonomy auto-population:**

The `nemesis` values in the taxonomy are auto-populated by a pre-commit hook that reads all `NemesisBaseClass` subclasses from `sdcm/nemesis/`. The hook runs `python -c "from sdcm.nemesis import NemesisRegistry; ..."` and regenerates the nemesis section. This ensures taxonomy never lags behind the registry. The values shown in examples are illustrative, not exhaustive.

## Implementation Phases

### Phase 1: Pydantic Metadata Model and Taxonomy (Importance: Critical)

**Objective**: Define the metadata fields as first-class SCT configuration parameters with pydantic validation, and establish the authorized label taxonomy.

**Deliverables**:

1. **New pydantic model** — `sdcm/test_metadata.py`:

```python
from pydantic import BaseModel, Field, field_validator
from typing import Literal

class TestMetadata(BaseModel):
    """Structured metadata for test-case documentation and labeling.

    Embedded directly in test-case YAML files and validated on config load.
    Flows to Argus via sct_config submission.
    """

    description: str | None = Field(
        default=None,
        description="Human-readable description of what this test validates (2-4 sentences).",
    )
    test_type: Literal[
        "longevity", "performance", "upgrade", "artifacts", "manager",
        "functional", "scale", "jepsen", "gemini", "features",
        "platform-migration", "vector-search", "cdc",
    ] | None = Field(
        default=None,
        description="Primary test category.",
    )
    tier: Literal["sanity", "tier1", "release", "ondemand"] | None = Field(
        default=None,
        description=(
            "Test priority tier. "
            "sanity: basic smoke tests, run on every commit. "
            "tier1: core regression, run weekly. "
            "release: mandatory during release qualification. "
            "ondemand: run on-demand for investigation or niche scenarios."
        ),
    )
    duration_class: Literal["short", "medium", "long"] | None = Field(
        default=None,
        description="Test duration bucket: short (<6h), medium (6-24h), long (>24h).",
    )
    supported_backends: list[str] | None = Field(
        default=None,
        description=(
            "Backends this test supports. If omitted/None, the test supports ALL backends. "
            "Values: aws, gce, azure, docker, k8s-eks, k8s-gke, k8s-local-kind, baremetal, xcloud."
        ),
    )
    stress_tools: list[str] = Field(
        default_factory=list,
        description="Stress/load tools used: cassandra-stress, scylla-bench, ycsb, latte, gemini, etc.",
    )
    workload: Literal[
        "write", "read", "mixed", "counter", "lwt", "cdc",
        "mv", "si", "alternator", "user-profile",
    ] | None = Field(
        default=None,
        description="Primary workload type.",
    )
    nemesis_labels: list[str] = Field(
        default_factory=list,
        description="Nemesis (chaos) classes used, e.g. ['SisyphusMonkey', 'ChaosMonkey'].",
    )
    features: list[str] = Field(
        default_factory=list,
        description=(
            "Scylla features specifically tested: encryption-at-rest, tls-ssl, "
            "authorization, cdc, tablets, vnodes, multi-dc, rack-aware, ipv6, kms, etc."
        ),
    )

VALID_BACKENDS = {
    "aws", "gce", "azure", "docker", "k8s-eks", "k8s-gke",
    "k8s-local-kind", "baremetal", "xcloud",
}

class TestMetadata(BaseModel):
    # ... fields above ...

    @field_validator("supported_backends", mode="before")
    @classmethod
    def validate_backends(cls, v):
        if v is None:
            return v
        for b in v:
            if b not in VALID_BACKENDS:
                raise ValueError(f"Invalid backend '{b}'. Valid: {sorted(VALID_BACKENDS)}")
        return v
```

2. **Integration into `SCTConfiguration`** — add metadata field:

```python
# In sdcm/sct_config.py, add to SCTConfiguration class:
from sdcm.test_metadata import TestMetadata

test_metadata: TestMetadata | None = SctField(
    description="Structured metadata for test documentation and labeling. "
                "Validated by pydantic model. Flows to Argus.",
)
```

3. **Test-case YAML format** — metadata embedded directly:

```yaml
# test-cases/longevity/longevity-10gb-3h.yaml
test_metadata:
  description: >-
    Basic longevity test running cassandra-stress write workload at QUORUM
    consistency for 3 hours on a 6-node cluster with SisyphusMonkey nemesis.
    Validates cluster stability under moderate write load with continuous
    chaos operations.
  test_type: longevity
  tier: tier1
  duration_class: short
  supported_backends:
    - aws
    - gce
    - docker
  stress_tools:
    - cassandra-stress
  workload: write
  nemesis_labels:
    - SisyphusMonkey
  features: []

# ... existing config fields ...
test_duration: 255
stress_cmd: ["cassandra-stress write cl=QUORUM duration=180m ..."]
n_db_nodes: 6
```

4. **Authorized label taxonomy** — `docs/pipeline-labels/taxonomy.yaml`:

```yaml
# docs/pipeline-labels/taxonomy.yaml
# Reference taxonomy for SCT test metadata labels.
# The pydantic model enforces valid values via Literal types.
# This file documents allowed values and their meanings for human reference
# and for AI skill guidance.

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
  description: "Test priority tier"
  values:
    - sanity       # Basic smoke tests, run on every commit
    - tier1        # Core regression, run weekly
    - release      # Mandatory during release qualification (gate tests)
    - ondemand     # Run on-demand for investigation or niche scenarios

duration_class:
  description: "Test duration bucket"
  values:
    - short       # <6 hours
    - medium      # 6-24 hours
    - long        # >24 hours

supported_backends:
  description: "Cloud/infra backends the test supports. Omit field = all backends supported."
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
  description: "Nemesis (chaos) classes used — auto-populated from sdcm/nemesis/ registry"
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
    # ... auto-populated by pre-commit hook from NemesisRegistry

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
    - twcs
    - zero-token-nodes
    - alternator-streams
```

5. **Default value in `defaults/test_default.yaml`**:

```yaml
test_metadata: null
```

**Definition of Done**:
- [ ] `sdcm/test_metadata.py` pydantic model defined with all fields and validators
- [ ] `SCTConfiguration.test_metadata` field added in `sct_config.py`
- [ ] Default value set in `defaults/test_default.yaml`
- [ ] `docs/pipeline-labels/taxonomy.yaml` created as human-readable reference
- [ ] 5 example test-case YAMLs updated with `test_metadata` section
- [ ] Config loads successfully with and without `test_metadata` present
- [ ] `dump_help_config_markdown()` includes metadata field documentation
- [ ] Unit tests validate the pydantic model (valid/invalid inputs)
- [ ] Pre-commit hook auto-populates nemesis values in taxonomy.yaml from NemesisRegistry

**Dependencies**: None.

---

### Phase 2: CLI Lint Tool (`sct.py lint-test-docs`) (Importance: Critical)

**Objective**: Create a CLI command that validates `test_metadata` sections for completeness and cross-references against the test-case config and pipeline.

**Deliverables**:

1. **New module**: `sdcm/utils/lint/test_docs_linter.py`

```python
from dataclasses import dataclass
from pathlib import Path

@dataclass
class LintResult:
    file_path: str
    errors: list[str]      # Missing required fields, invalid values
    warnings: list[str]    # Suggestions (e.g., nemesis mismatch)

def lint_test_metadata(config_path: Path, taxonomy_path: Path) -> LintResult:
    """Validate test_metadata in a test-case YAML against taxonomy and config content."""
    ...

def cross_reference_config(config_path: Path) -> LintResult:
    """Check that metadata labels match the actual test configuration.

    Returns a LintResult with warnings for mismatches between metadata and config.

    Examples of cross-reference checks:
    - If config has nemesis_class_name: 'SisyphusMonkey', metadata.nemesis_labels should include it
    - If config has stress_cmd containing 'cassandra-stress', metadata.stress_tools should include it
    - If config has n_db_nodes: '15 15 15' (multi-DC), metadata.features should include 'multi-dc'
    - If pipeline says backend: 'gce' but metadata.supported_backends excludes 'gce', flag mismatch
    - If test method docstring declares 'Supported backends: aws, docker' but metadata lists others, flag mismatch.
      If docstring has no such declaration, all backends are assumed valid — no mismatch.
    """
    ...
```

2. **New CLI command** in `sct.py`:

```python
@cli.command("lint-test-docs", help="Validate test case metadata and labels")
@click.option("--test-case-dir", default="test-cases", help="Root directory of test cases")
@click.option("--fix", is_flag=True, help="Auto-fix trivially correctable issues")
@click.option("--missing-only", is_flag=True, help="Only report test cases missing test_metadata")
def lint_test_docs(test_case_dir, fix, missing_only):
    ...
```

3. **Validation rules**:

| Rule ID | Check | Severity |
|---------|-------|----------|
| TD-001 | `test_metadata` section exists in test-case YAML | error |
| TD-002 | `description` field is non-empty and >20 characters | error |
| TD-003 | All label values are valid per pydantic model (Literal types) | error |
| TD-004 | `test_type` matches the test-case directory name | warning |
| TD-005 | `nemesis_labels` match `nemesis_class_name` in config | warning |
| TD-006 | `stress_tools` match stress commands in config | warning |
| TD-007 | `supported_backends` is compatible with pipeline `backend:` parameter | warning |
| TD-007b | If test docstring declares `Supported backends:`, metadata must be subset. No declaration = all. | warning |
| TD-008 | `duration_class` is consistent with `test_duration` value | warning |
| TD-009 | `features` labels are consistent with config (e.g., multi-dc detected from `n_db_nodes: '15 15 15'`) | warning |
| TD-010 | `tier` is appropriate for test duration (e.g., `sanity` tests should be <1h) | warning |
| TD-011 | `supported_backends` includes the backend from associated pipeline(s) | warning |

**Auto-fix (`--fix`)** capabilities:
- TD-005: Add missing nemesis labels from config
- TD-006: Add missing stress_tool labels from config
- TD-008: Correct `duration_class` based on `test_duration` value
- TD-009: Add `multi-dc` feature when multi-DC config detected

**Definition of Done**:
- [ ] `sct.py lint-test-docs` validates all test-case YAML files
- [ ] All 12 validation rules implemented
- [ ] `--fix` auto-corrects TD-005, TD-006, TD-008, TD-009
- [ ] `--missing-only` reports coverage gaps
- [ ] Exit code 1 on errors, 0 on clean (warnings don't fail)
- [ ] Unit tests cover all validation rules with positive and negative cases

**Dependencies**: Phase 1 (pydantic model and taxonomy).

---

### Phase 3: Argus Integration (Importance: High)

**Objective**: Ensure test metadata flows from test-case YAML → SCT config → Argus, and is usable in the Argus UI for categorization and search.

**Deliverables**:

1. **Metadata submission in `init_argus_run()`** — update `sdcm/tester.py`:

```python
def init_argus_run(self):
    self.test_config.init_argus_client(self.params)
    git_status = get_git_status_info()

    # Extract metadata for explicit Argus submission
    metadata = self.params.get("test_metadata")
    metadata_dict = metadata.model_dump() if metadata else {}

    self.test_config.argus_client().submit_sct_run(
        job_name=get_job_name(),
        job_url=get_job_url(),
        started_by=get_username(),
        commit_id=git_status.get("branch.oid"),
        origin_url=git_status.get("upstream.url"),
        branch_name=git_status.get("branch.upstream"),
        sct_config=self.params.model_dump(),
        test_metadata=metadata_dict,  # NEW: explicit metadata
    )
```

2. **Argus client extension** — update `argus/client/sct/client.py`:

```python
def submit_sct_run(self, ..., test_metadata: dict | None = None) -> None:
    run_body = {
        "run_id": str(self.run_id),
        "job_name": job_name,
        # ... existing fields ...
        "test_metadata": test_metadata,  # NEW
    }
    response = super().submit_run(run_type=self.test_type, run_body=run_body)
```

3. **Source of truth and data flow**:

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA FLOW                                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  test-cases/*.yaml          (SOURCE OF TRUTH)                    │
│       │                                                          │
│       │ test_metadata:                                           │
│       │   description: "..."                                     │
│       │   tier: sanity                                           │
│       │   ...                                                    │
│       ▼                                                          │
│  SCTConfiguration.test_metadata   (pydantic-validated)           │
│       │                                                          │
│       ├──► sct_submit_config()    (full config JSON to Argus)    │
│       │                                                          │
│       └──► submit_sct_run(test_metadata=...)  (explicit field)   │
│                │                                                  │
│                ▼                                                  │
│         Argus Backend                                            │
│           - Stores metadata alongside test run                   │
│           - Enables filtering by tier, test_type, features       │
│           - Displays description in run detail view              │
│                                                                  │
│  Jenkins Job Description    (SECONDARY / DISPLAY ONLY)           │
│       │                                                          │
│       │ create_test_release_jobs.py                              │
│       │   reads _folder_definitions.yaml                         │
│       │   reads /** jobDescription */ blocks                     │
│       ▼                                                          │
│  Jenkins UI (job description field)                              │
│       - Human-readable in Jenkins job page                       │
│       - NOT sent to Argus (display only)                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

4. **Guidelines for Argus implementation**:

- **Source of truth**: The `test_metadata` section in test-case YAML is the canonical source. Argus receives it via `submit_sct_run()` at test start.
- **Caching**: Argus should store metadata **per test run** (not per branch). Different branches may have different metadata for the same test case (e.g., a branch adding CDC features). The metadata submitted at run start is the definitive version for that run.
- **Schema evolution**: Argus should store `test_metadata` as a JSON blob (schemaless on the Argus side). This allows SCT to add new fields without requiring Argus schema migrations. Argus UI can display known fields and ignore unknown ones.
- **Backward compatibility**: Runs without `test_metadata` (older SCT versions) will have `null` / missing metadata in Argus. The UI should handle this gracefully.
- **Jenkins job description**: Remains a secondary display mechanism. `create_test_release_jobs.py` can optionally generate the Jenkins job description from `test_metadata` fields for human readability in the Jenkins UI, but Jenkins is NOT the source of truth.
- **Per-branch behavior**: Since metadata lives in the test-case YAML and flows through the standard config merge, each branch's version of the YAML determines what metadata Argus receives. No special branch handling needed.

**Definition of Done**:
- [ ] `submit_sct_run()` accepts and sends `test_metadata` dict
- [ ] Argus backend stores `test_metadata` per run (API endpoint updated)
- [ ] Argus UI shows test description, tier, and test_type in run list/detail views
- [ ] Argus supports filtering/searching by tier and test_type
- [ ] Runs without metadata display gracefully (no errors)
- [ ] Integration test: run a test with metadata → verify it appears in Argus

**Dependencies**: Phase 1 (model defined), Argus backend team coordination.

---

### Phase 4: AI Skill — `reviewing-pipeline-docs` (Importance: High)

**Objective**: Create a skill that helps developers review and generate `test_metadata` sections for test cases, usable from both Copilot CLI and Claude Code.

**Deliverables**:

1. **Skill directory**: `skills/reviewing-pipeline-docs/`

```
skills/reviewing-pipeline-docs/
├── SKILL.md
├── references/
│   ├── taxonomy-guide.md         # How to choose correct labels
│   └── description-examples.md   # Good vs bad description examples
└── workflows/
    ├── review-test-case.md       # Review existing test_metadata
    └── generate-metadata.md      # Generate test_metadata from scratch
```

2. **Workflow: `generate-metadata.md`** — step-by-step process:

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
- **Multi-DC**: Detect from `n_db_nodes` format ('6' = single-DC, '15 15 15' = 3 DCs)
- **Features**: Detect from config keys (encryption, authorization, cdc, tablets, etc.)
- **Backend**: Read from pipeline `backend:` parameter AND from the test method's docstring.
  Tests may declare `Supported backends: aws, gce, docker` in their docstring to restrict
  which backends are valid. If the docstring has no `Supported backends:` line, omit the
  `supported_backends` field (meaning all backends are supported).

## Phase 3: Write Description

Write a 2-4 sentence description that answers:
1. What workload does this test run? (tool, operation, consistency level)
2. What cluster topology? (node count, DCs, special nodes)
3. What chaos/nemesis operations? (nemesis class)
4. What is the test validating? (stability, performance, feature correctness)

**Do NOT mention provisioning details** like spot/on-demand instance types.

## Phase 4: Assemble and Validate

1. Add `test_metadata:` section at the top of the test-case YAML
2. Run `sct.py lint-test-docs --test-case-file <path>` to validate
3. Fix any errors or warnings
```

3. **Reference: `description-examples.md`** — concrete good/bad examples:

```markdown
## Good Descriptions

### Example 1: Longevity test
> Basic longevity test running cassandra-stress write workload at QUORUM
> consistency for 3 hours on a 6-node cluster with SisyphusMonkey nemesis.
> Validates cluster stability under moderate write load with continuous
> chaos operations.

### Example 2: Multi-DC CDC test
> Large-scale CDC longevity test across 3 datacenters (15 nodes each) running
> 4 CDC workload profiles. Runs for 8 hours with topology-changes nemesis to
> validate CDC replication consistency under multi-DC disruptions.

## Bad Descriptions (and why)

### Bad: Too vague
> "Tests longevity."
**Why**: Doesn't say what workload, what cluster, or what it validates.

### Bad: Mentions provisioning details
> "Installs from repo on a GCE spot instance."
**Why**: Spot vs on-demand is a cost decision, not a test property.
```

4. **Reference: `taxonomy-guide.md`** — decision trees for choosing labels:

```markdown
## Choosing `tier`

- sanity: Tests that run on every commit or PR. Usually <1h, basic smoke tests.
- tier1: Core regression suite. Runs weekly. 1-12h duration.
- release: Gate tests mandatory during release qualification.
- ondemand: Long-running, niche, or experimental. Run on-demand only.

## Detecting `features` from config

| Config Key/Value | Feature Label |
|------------------|---------------|
| `n_db_nodes: 'X Y Z'` (space-separated) | `multi-dc` |
| `authenticator: PasswordAuthenticator` | `authorization` |
| `client_encrypt: true` or `server_encrypt: true` | `tls-ssl` |
| `tablets_enabled: true` | `tablets` |
| `encryption_at_rest_enabled: true` | `encryption-at-rest` |
| `cdc` in test name or `cdc_enabled: true` | `cdc` |
```

**Definition of Done**:
- [ ] Skill directory created with SKILL.md, 2 references, 2 workflows
- [ ] SKILL.md under 500 lines, detailed content in references/workflows
- [ ] Registered in both `AGENTS.md` and `CLAUDE.md`
- [ ] Follows SCT skill conventions from `skills/designing-skills/SKILL.md`

**Dependencies**: Phase 1 (taxonomy), Phase 2 (lint tool for validation step in workflow).

---

### Phase 5: AI Skill — `labeling-pipelines` (Importance: High)

**Objective**: Create a skill for linting and correcting pipeline labels at scale — reviewing existing `test_metadata` for accuracy and suggesting fixes.

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

2. **Reference: `common-mistakes.md`**:

```markdown
## Mistake 1: Labeling based on filename instead of config

**Wrong**: File is `longevity-cdc-100gb-4h.yaml`, so features = ['cdc']
**Right**: Read the config — does it actually have CDC-specific stress profiles?

## Mistake 2: Missing multi-DC detection

**Wrong**: `n_db_nodes: '15 15 15'` but features doesn't include 'multi-dc'
**Right**: Space-separated n_db_nodes always means multi-DC.

## Mistake 3: Listing all possible backends instead of actual

**Wrong**: supported_backends: [aws, gce, azure] because the config has instance types for all
**Right**: If the test works on ALL backends, omit supported_backends entirely (null = all).
Only list specific backends if the test is RESTRICTED to a subset.
```

**Definition of Done**:
- [ ] Skill directory created with SKILL.md, 2 references, 2 workflows
- [ ] Registered in both `AGENTS.md` and `CLAUDE.md`
- [ ] Cross-references the `reviewing-pipeline-docs` skill where appropriate
- [ ] Follows SCT skill conventions

**Dependencies**: Phase 1 (taxonomy), Phase 2 (lint tool), Phase 4 (reviewing skill).

---

### Phase 6: Bootstrap Metadata for Existing Test Cases (Importance: Medium)

**Objective**: Generate `test_metadata` sections for all ~267 test cases using AI-assisted generation and the skills from Phases 4-5.

**Deliverables**:

1. **Bootstrap script**: `utils/bootstrap_test_metadata.py`
   - Reads each test-case YAML, finds its pipeline(s), extracts facts
   - Generates a draft `test_metadata` section with auto-detectable labels
   - Leaves `description` as a placeholder: `"TODO: Add description"`
   - Inserts `test_metadata:` at the top of the YAML file (before existing fields)
   - Runs `lint-test-docs` validation on each modified file
   - Outputs a report of what was generated and what needs human review

2. **Human review process**:
   - AI skills assist in writing descriptions and fixing label warnings
   - PRs are split by test-case category (one PR per directory: longevity, performance, manager, etc.)
   - **Ownership**: Each category PR is assigned to the team owning those tests. The PR author (AI-assisted) provides drafts; the owning team reviews descriptions for accuracy within 2 sprint cycles.
   - **Converting TODOs to descriptions**: After bootstrap, run the `reviewing-pipeline-docs` skill per-category to generate description drafts. Human reviewers approve/edit. Target: all TODO placeholders replaced within 4 weeks.

3. **Coverage target**: >90% of test cases have `test_metadata` with complete labels; >70% have human-reviewed descriptions.

**Definition of Done**:
- [ ] Bootstrap script generates `test_metadata` for all test cases
- [ ] Auto-detected labels pass lint validation
- [ ] Human-written descriptions for at least 3 categories (longevity, performance, manager)
- [ ] `sct.py lint-test-docs` shows >90% coverage

**Dependencies**: Phase 2 (lint tool), Phases 4-5 (skills for human review).

---

### Phase 7: CI Integration and Pre-commit Hook (Importance: Medium)

**Objective**: Enforce documentation standards in CI so new test cases require `test_metadata` and label changes are validated.

**Deliverables**:

1. **Pre-commit hook**: Add `sct.py lint-test-docs` to `.pre-commit-config.yaml`
   - Runs only on changed `.yaml` files in `test-cases/`
   - Validates metadata completeness and cross-references

2. **CI stage**: Add `lint-test-docs` stage to `Jenkinsfile`
   - Runs after `lint-pipelines` (from the pipeline config linter plan)
   - Fails the build on errors (TD-001 for new test cases, TD-002, TD-003)
   - Warnings are reported but don't fail the build

3. **PR template update**: Add checklist item:
   - "If adding a new test case, `test_metadata` section is included with description and labels"

**Definition of Done**:
- [ ] Pre-commit hook validates `test_metadata` on commit
- [ ] CI stage runs `sct.py lint-test-docs` and fails on errors
- [ ] PR template updated with documentation checklist item

**Dependencies**: Phase 2 (lint tool), Phase 6 (bootstrap complete so existing files don't cause CI failures).

---

### Phase 8: Documentation Update and _display_name Migration (Importance: Low)

**Objective**: Document the labeling system, taxonomy, and skills for the team.

**Deliverables**:

1. **User guide**: `docs/pipeline-labels/README.md`
   - What is the labeling system and why it exists
   - How to add `test_metadata` for a new test case
   - How to add new labels to the taxonomy
   - How to use the AI skills for review

2. **Update `AGENTS.md`**: Add both new skills to the Skills table
3. **Update `CLAUDE.md`**: Add skills references
4. **Migrate `_display_name` files**: Replace existing `_display_name` files (10+ directories) with `_folder_definitions.yaml` equivalents. Remove old files after migration.

**Definition of Done**:
- [ ] `docs/pipeline-labels/README.md` created
- [ ] `AGENTS.md` and `CLAUDE.md` updated with new skills
- [ ] All `_display_name` files migrated to `_folder_definitions.yaml` and removed

**Dependencies**: All previous phases.

## Testing Requirements

### Unit Tests (Phase 1-2)

File: `unit_tests/test_test_metadata.py`

```python
import pytest
from sdcm.test_metadata import TestMetadata


def test_valid_metadata():
    """Valid metadata should pass validation."""
    meta = TestMetadata(
        description="Basic longevity test with cassandra-stress write workload",
        test_type="longevity",
        tier="tier1",
        duration_class="short",
        supported_backends=["aws", "gce"],
        stress_tools=["cassandra-stress"],
        workload="write",
        nemesis_labels=["SisyphusMonkey"],
        features=[],
    )
    assert meta.test_type == "longevity"
    assert meta.tier == "tier1"


def test_invalid_backend_raises():
    """Invalid backend value should raise ValidationError."""
    with pytest.raises(Exception):
        TestMetadata(supported_backends=["invalid-backend"])


def test_null_backends_means_all():
    """None supported_backends means all backends are valid."""
    meta = TestMetadata(supported_backends=None)
    assert meta.supported_backends is None


def test_invalid_tier_raises():
    """Invalid tier value should raise ValidationError."""
    with pytest.raises(Exception):
        TestMetadata(tier="tier99")


def test_empty_metadata():
    """All-None metadata should be valid (test has no metadata yet)."""
    meta = TestMetadata()
    assert meta.description is None
    assert meta.test_type is None
```

File: `unit_tests/test_test_docs_linter.py`

```python
import pytest
from pathlib import Path
from sdcm.utils.lint.test_docs_linter import lint_test_metadata, cross_reference_config


def test_td001_missing_metadata(tmp_path):
    """TD-001: Report error when test_metadata is missing."""
    config = tmp_path / "test.yaml"
    config.write_text("test_duration: 60\nstress_cmd: ['cassandra-stress write']")
    result = lint_test_metadata(config, tmp_path / "taxonomy.yaml")
    assert any("TD-001" in e for e in result.errors)


def test_td005_nemesis_mismatch(tmp_path):
    """TD-005: Warn when nemesis_labels don't match config."""
    config = tmp_path / "test.yaml"
    config.write_text("""
test_metadata:
  description: "A valid test description here"
  nemesis_labels: [SisyphusMonkey]
nemesis_class_name: 'ChaosMonkey'
""")
    warnings = cross_reference_config(config)
    assert any("TD-005" in w for w in warnings)


def test_valid_metadata_passes(tmp_path):
    """Fully valid test_metadata should produce no errors."""
    config = tmp_path / "test.yaml"
    config.write_text("""
test_metadata:
  description: "Basic longevity test with cassandra-stress write workload"
  test_type: longevity
  tier: tier1
  stress_tools: [cassandra-stress]
  nemesis_labels: [SisyphusMonkey]
nemesis_class_name: 'SisyphusMonkey'
stress_cmd: ['cassandra-stress write']
test_duration: 180
""")
    result = lint_test_metadata(config, tmp_path / "taxonomy.yaml")
    assert len(result.errors) == 0
```

### Integration Tests (Phase 6)

- Run `sct.py lint-test-docs` against the bootstrapped metadata
- Verify cross-reference checks work with real test-case configs

### Skill Validation (Phases 4-5)

- Trigger eval queries (5+ should-trigger, 3+ should-NOT-trigger per skill)
- Test workflows against 3 representative test cases per skill

## Success Criteria

1. **Pydantic model validates**: Test metadata is validated on config load via pydantic
2. **Lint tool works**: `sct.py lint-test-docs` validates completeness and cross-references with 10 rules
3. **Argus receives metadata**: Test description, tier, and labels are visible in Argus UI
4. **Skills functional**: Both AI skills trigger correctly and produce accurate metadata
5. **Coverage >90%**: At least 240 of 267 test cases have validated `test_metadata`
6. **CI enforced**: New test cases without `test_metadata` fail CI

## Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Taxonomy too rigid — new tests need labels not in Literal types | High | Low | Add new values to pydantic model via PR; clear error messages guide developers |
| AI-generated descriptions are inaccurate | Medium | Medium | Human review required; lint cross-references catch label errors |
| test_metadata fields drift out of sync with config changes | Medium | Low | CI lint hook catches mismatches on every PR; single-file approach reduces drift vs sidecar files |
| Argus backend changes needed | Medium | High | Store metadata as JSON blob — no Argus schema migration needed for new SCT fields |
| Team resistance to adding metadata | Medium | High | Bootstrap script minimizes initial effort; AI skills reduce ongoing burden; CI enforcement makes it part of normal workflow |

## Appendix: Batch Operations

### Bulk-Adding `test_metadata` to Test Cases

Use the bootstrap script or AI skills to add metadata in bulk:

```bash
# Generate draft test_metadata for all longevity test cases
python utils/bootstrap_test_metadata.py --dir test-cases/longevity/

# Validate the generated metadata
uv run sct.py lint-test-docs --test-case-dir test-cases/longevity/

# Auto-fix detectable issues
uv run sct.py lint-test-docs --test-case-dir test-cases/longevity/ --fix
```

### Bulk-Adding `jobDescription` Blocks to Pipeline Files

To add `jobDescription` comment blocks to all pipelines in a folder (for Jenkins UI display):

**Commit convention**: One commit per folder, message format:
```
docs(pipelines): add jobDescription to <folder-name> pipelines
```

### Recommended Batch Order

Process folders in this order to build confidence and establish patterns:

1. **`artifacts/`** — simplest pipelines, good for establishing the pattern
2. **`manager/`** — moderate complexity, distinct Manager operations
3. **`longevity/`** — most files (~600+), process in sub-batches of ~50 files
4. **`performance/`** — requires careful description of benchmark methodology
5. **`upgrades/`** — requires documenting version upgrade paths
6. **`operator/`** — K8s-specific terminology
