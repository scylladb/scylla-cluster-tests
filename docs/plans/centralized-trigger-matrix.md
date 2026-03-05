# Centralized Trigger Matrix: YAML-Driven Job Triggering

## 1. Problem Statement

SCT's Jenkins test triggering is fragmented across multiple mechanisms:

- **Tier1 triggers**: XML files (`tier1-aws-custom-time-trigger.xml`, `tier1-azure-custom-time-trigger.xml`, etc.) in `jenkins-pipelines/oss/sct_triggers/` and `jenkins-pipelines/master-triggers/sct_triggers/` that hard-code job lists and parameters per-backend and per-architecture.
- **Sanity triggers**: Separate Groovy-based `simpleTrigger()` calls (`sanity-aws-trigger.jenkinsfile`, `sanity-azure-trigger.jenkinsfile`, `sanity-gcp-trigger.jenkinsfile`, `additional-aws-trigger.jenkinsfile`) each containing a hard-coded list of jobs.
- **Performance triggers**: A 250+ line Groovy `testRegionMatrix` list embedded in `vars/perfRegressionParallelPipelinebyRegion.groovy` that defines jobs, regions, version filtering, labels, and sub-tests.

**Problems with the current approach:**

1. **No single source of truth**: Job lists are duplicated across XML, Groovy, and multiple trigger files. Adding a new job or changing parameters requires edits in multiple places.
2. **Untestable**: Trigger logic lives in Groovy/XML which cannot be unit-tested. Mistakes are caught only at runtime in Jenkins.
3. **Backend/architecture fragmentation**: Separate trigger files exist per backend (AWS, Azure, GCE) and per architecture (x86_64, aarch64), leading to 4+ XML files just for tier1.
4. **No version-based triggering with full version tags**: PR #13192 (merged) added support for full version tags like `2024.2.5-0.20250221.cb9e2a54ae6d-1` in `scylla_version`. The trigger system should leverage this — passing full versions directly, with no need for AMI/image ID translation, architecture detection, or cross-backend image mapping.

**Key simplification**: Since `scylla_version` now supports full version tags (PR #13192) and SCT itself handles image lookup per-backend and per-region, the trigger system no longer needs to:
- Translate AMI IDs to GCE/Azure images
- Detect architecture from AMI metadata
- Find cross-backend equivalent images
- Pin specific image IDs at trigger time

The trigger just needs to pass `scylla_version` (a full version string) and let each downstream job resolve its own images.

## 2. Current State

### Tier1 Triggers (XML-based, per-backend)

**Files:**
- `jenkins-pipelines/oss/sct_triggers/tier1-aws-custom-time-trigger.xml` — Triggers 5 AWS tier1 jobs
- `jenkins-pipelines/oss/sct_triggers/tier1-azure-custom-time-trigger.xml` — Triggers 1 Azure tier1 job
- `jenkins-pipelines/oss/sct_triggers/tier1-gce-custom-time-trigger.xml` — Triggers 1 GCE tier1 job
- `jenkins-pipelines/oss/sct_triggers/tier1-aws-aarch64-custom-time-trigger.xml` — Triggers 1 ARM64 tier1 job
- `jenkins-pipelines/master-triggers/sct_triggers/tier1-custom-time-trigger.xml` — Triggers 10 tier1 jobs (master branch)

**Mechanism**: Jenkins `TimerTrigger` + `ParameterizedTrigger` plugin. Each XML file:
- Accepts parameters (`scylla_ami_id`/`azure_image_db`/`gce_image_db`, `scylla_version`, `region`, `stress_duration`)
- Lists downstream jobs as project paths
- Passes parameters via `PredefinedBuildParameters`

**Downstream tier1 jobs** (in `jenkins-pipelines/oss/tier1/`):
- `gemini-1tb-10h.jenkinsfile`
- `longevity-1tb-5days-azure.jenkinsfile`
- `longevity-50gb-3days.jenkinsfile`
- `longevity-150gb-asymmetric-cluster-12h.jenkinsfile`
- `longevity-large-partition-200k-pks-4days-gce.jenkinsfile`
- `longevity-multidc-schema-topology-changes-12h.jenkinsfile`
- `longevity-mv-si-4days-streaming.jenkinsfile`
- `longevity-schema-topology-changes-12h.jenkinsfile`
- `longevity-twcs-48h.jenkinsfile`

Each uses `longevityPipeline()` (defined in `vars/longevityPipeline.groovy`) with: `backend`, `region`/`azure_region_name`, `test_name`, `test_config`, and optional `email_recipients`.

### Sanity Triggers (Groovy, per-backend)

**Files** (in `jenkins-pipelines/oss/sct_triggers/`):
- `sanity-aws-trigger.jenkinsfile` — 1 job: `longevity/longevity-100gb-4h-test`
- `sanity-azure-trigger.jenkinsfile` — 1 job: `longevity/longevity-10gb-3h-azure-test`
- `sanity-gcp-trigger.jenkinsfile` — 1 job: `longevity/longevity-10gb-3h-gce-test`
- `additional-aws-trigger.jenkinsfile` — 6 jobs including LWT, TWCS, Alternator, CDC tests

**Mechanism**: Each calls `simpleTrigger(List jobs)` (defined in `vars/simpleTrigger.groovy`) which iterates over the job list and triggers each with `build job: "..${job}", wait: false`.

### Performance Triggers (Groovy matrix in `vars/`)

**File**: `vars/perfRegressionParallelPipelinebyRegion.groovy` (~600 lines)
- Contains `testRegionMatrix` — a Groovy list of 25+ map entries, each defining:
  - `job_name`, `region`, `ignore_versions`, `pre_release`, `sub_tests`, `labels`
  - Optional: `rolling_upgrade_test`, `microbenchmark`, `cloud_provider`
- Label-based scheduling: `master-daily`, `master-weekly`, `master-3weeks`, `alternator-weekly`
- Version filtering: `ignore_versions` list per job
- Called from `jenkins-pipelines/master-triggers/sct_triggers/perf-regression-trigger.jenkinsfile`

**Trigger file**: `jenkins-pipelines/master-triggers/sct_triggers/perf-regression-trigger.jenkinsfile` — a one-liner that calls `perfRegressionParallelPipelinebyRegion()`.

**Downstream perf jobs** (in `jenkins-pipelines/performance/branch-perf-v17/`):
Each calls `perfRegressionParallelPipeline()` with: `backend`, `region`, `test_name`, `test_config`, `sub_tests`, `provision_type`.

### Shared Pipeline Functions

- `vars/longevityPipeline.groovy` — Runs longevity/tier1 tests. Accepts: `backend`, `region`, `test_name`, `test_config`, `scylla_version`, `scylla_ami_id`, etc.
- `vars/perfRegressionParallelPipeline.groovy` — Runs perf regression tests. Accepts similar params plus `sub_tests`.
- `vars/simpleTrigger.groovy` — Thin wrapper that triggers a list of jobs with passthrough parameters.

### Full Version Tag Support (PR #13192, merged)

`scylla_version` in `sdcm/sct_config.py` now supports full version tags:
- Simple: `5.2.1`, `2024.2.0`
- Branch: `master:latest`, `branch-2019.1:all`
- **Full tag (NEW)**: `2024.2.5-0.20250221.cb9e2a54ae6d-1`

SCT resolves the correct image (AMI, GCE image, Azure image, Docker tag) automatically based on the full version tag, backend, region, and architecture. This eliminates the need for the trigger layer to handle image translation.

## 3. Goals

1. **Single YAML source of truth** for each job group (tier1, sanity, performance) — one file to edit when adding/removing jobs.
2. **General-purpose trigger pipeline** — one Groovy pipeline (`vars/triggerMatrixPipeline.groovy`) and one Python module (`sdcm/utils/trigger_matrix.py`) that work for all job categories.
3. **Version-first triggering** — triggers pass `scylla_version` (including full version tags), not image IDs. No AMI/image translation logic in the trigger layer.
4. **Label-based filtering** — jobs have labels (e.g., `tier1`, `sanity`, `weekly`, `daily`, `aws`, `gce`, `azure`, `arm64`) that can be used to filter what gets triggered.
5. **Testable** — core logic in Python with unit tests; Groovy pipeline is a thin wrapper.
6. **Backward compatible** — existing downstream `.jenkinsfile` jobs are not modified. The trigger system only changes how they are invoked.
7. **Cron schedule support** — YAML can define cron schedules for automatic triggering.
8. **Dry-run mode** — ability to preview what would be triggered without actually starting jobs.
9. **Job folder auto-detection** — auto-derive Jenkins job folder from version (`master` → `scylla-master`, `2025.4` → `branch-2025.4`).

## 4. Implementation Phases

### Phase 1: YAML Schema Design and Validation

**Description**: Define the YAML schema for trigger matrix files and implement the Python loader with validation.

**YAML schema for all job categories:**

```yaml
# configurations/triggers/tier1.yaml
# Single source of truth for tier1 job definitions

# Optional: cron schedules that auto-trigger this matrix
cron_triggers:
  - schedule: "00 06 * * 6"  # Saturday 6am UTC
    params:
      scylla_version: "{branch}:latest"
      labels_selector: "weekly"
      requested_by_user: "scheduled"

# Default parameters applied to all jobs (overridable per-job)
defaults:
  provision_type: "on_demand"
  post_behavior_db_nodes: "destroy"
  post_behavior_loader_nodes: "destroy"
  post_behavior_monitor_nodes: "destroy"

# Job definitions
jobs:
  - job_name: "tier1/longevity-50gb-3days-test"
    test_config:
      - "test-cases/longevity/longevity-50GB-3days-authorization-and-tls-ssl.yaml"
      - "configurations/network_config/two_interfaces.yaml"
      - "configurations/advanced-rpc-compression.yaml"
    backend: "aws"
    region: "eu-west-1"
    labels:
      - "tier1"
      - "weekly"
    exclude_versions: []
    params:
      availability_zone: "c"
      stress_duration: "4320"

  - job_name: "tier1/longevity-1tb-5days-azure-test"
    test_config:
      - "test-cases/longevity/longevity-1TB-5days-azure.yaml"
    backend: "azure"
    region: "eastus"
    labels:
      - "tier1"
      - "weekly"
    exclude_versions: []

  - job_name: "tier1/longevity-twcs-48h-test"
    test_config:
      - "test-cases/longevity/longevity-twcs-48h.yaml"
    backend: "aws"
    region: "us-east-1"
    labels:
      - "tier1"
      - "weekly"
      - "arm64"        # informational label — arch is handled at runtime by the downstream job
    exclude_versions: []
```

**Performance trigger YAML example:**

```yaml
# configurations/triggers/perf-regression.yaml

cron_triggers:
  - schedule: "H 2 * * 1"  # Mondays 2am
    params:
      scylla_version: "{branch}:latest"
      labels_selector: "master-weekly"
      requested_by_user: "scheduled"

defaults:
  provision_type: "on_demand"
  post_behavior_db_nodes: "destroy"
  post_behavior_loader_nodes: "destroy"
  post_behavior_monitor_nodes: "destroy"

jobs:
  - job_name: "scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-vnodes"
    test_config:
      - "test-cases/performance/perf-regression-predefined-throughput-steps.yaml"
      - "configurations/performance/cassandra_stress_gradual_load_steps_enterprise.yaml"
      - "configurations/disable_kms.yaml"
      - "configurations/tablets_disabled.yaml"
    backend: "aws"
    region: "us-east-1"
    labels:
      - "perf"
      - "master-weekly"
    exclude_versions: []
    params:
      sub_tests: '["test_read_gradual_increase_load", "test_mixed_gradual_increase_load", "test_read_disk_only_gradual_increase_load", "test_write_gradual_increase_load"]'

  - job_name: "scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-tablets"
    test_config:
      - "test-cases/performance/perf-regression-predefined-throughput-steps.yaml"
      - "configurations/performance/cassandra_stress_gradual_load_steps_enterprise.yaml"
      - "configurations/disable_kms.yaml"
    backend: "aws"
    region: "us-east-1"
    labels:
      - "perf"
      - "master-weekly"
    exclude_versions: []
    params:
      sub_tests: '["test_read_gradual_increase_load", "test_mixed_gradual_increase_load"]'
```

**Sanity trigger YAML example:**

```yaml
# configurations/triggers/sanity.yaml

defaults:
  provision_type: "spot"
  post_behavior_db_nodes: "destroy"
  post_behavior_loader_nodes: "destroy"
  post_behavior_monitor_nodes: "destroy"

jobs:
  - job_name: "longevity/longevity-100gb-4h-test"
    backend: "aws"
    region: "eu-west-1"
    labels:
      - "sanity"
      - "aws"
    exclude_versions: []

  - job_name: "longevity/longevity-10gb-3h-azure-test"
    backend: "azure"
    region: "eastus"
    labels:
      - "sanity"
      - "azure"
    exclude_versions: []

  - job_name: "longevity/longevity-10gb-3h-gce-test"
    backend: "gce"
    region: "us-east1"
    labels:
      - "sanity"
      - "gce"
    exclude_versions: []

  - job_name: "longevity/longevity-10gb-3h-test"
    backend: "aws"
    region: "eu-west-1"
    labels:
      - "sanity"
      - "aws"
      - "additional"
    exclude_versions: []

  - job_name: "no_tablets/longevity-lwt-3h-no-tablets-test"
    backend: "aws"
    region: "eu-west-1"
    labels:
      - "sanity"
      - "aws"
      - "additional"
    exclude_versions: []
```

**Schema notes:**

- `job_name`: Relative path under the job folder. Actual Jenkins path constructed as `{job_folder}/{job_name}` where `job_folder` is derived from the version (e.g., `scylla-master`, `branch-2025.4`).
- `test_config`: List of YAML config files. This field is **optional and documentation-only** — it exists to make the YAML self-documenting and to enable validation that referenced configs exist. The downstream Jenkinsfile remains the authority for which configs to use. If in the future we want the trigger to be fully authoritative, this field can be promoted to override the downstream job's config.
- `backend`: Cloud provider (`aws`, `gce`, `azure`, `docker`). Used for filtering.
- `region`: Default region. Can be overridden at trigger time.
- `labels`: List of string labels for filtering. A job runs when its labels match the `--labels-selector` filter.
- `exclude_versions`: List of version prefixes where this job should NOT run. Empty means "run on all versions".
- `params`: Additional parameters passed to the downstream job (overrides defaults).
- `defaults`: Default parameters applied to all jobs. Per-job `params` take precedence.

**Definition of Done:**
- [ ] YAML schema documented and agreed upon
- [ ] Python dataclasses for `JobConfig`, `CronTriggerConfig`, `MatrixConfig`
- [ ] `load_matrix_config(path)` function that loads and validates YAML
- [ ] Schema validation with clear error messages for malformed YAML
- [ ] Unit tests for loading, validation, and error cases

**Dependencies**: None

---

### Phase 2: Python Core Logic — Filtering, Parameter Building, Job Triggering

**Description**: Implement the Python module that processes the loaded matrix, applies filters, builds parameters, and triggers Jenkins jobs.

**Module**: `sdcm/utils/trigger_matrix.py`

**Key functions:**

```python
def determine_job_folder(scylla_version: str, job_folder: str | None = None) -> str:
    """Derive Jenkins job folder from version.

    'master:latest' or 'master' → 'scylla-master'
    '2025.4' or '2025.4.1' → 'branch-2025.4'
    '2024.2.5-0.20250221.cb9e2a54ae6d-1' → 'branch-2024.2'
    Explicit job_folder parameter overrides auto-detection.
    """

def filter_jobs(
    jobs: list[JobConfig],
    scylla_version: str,
    labels_selector: str | None = None,
    backend: str | None = None,
    skip_jobs: list[str] | None = None,
) -> list[JobConfig]:
    """Filter jobs based on version exclusion, labels, backend, and skip list."""

def build_job_parameters(
    job: JobConfig,
    defaults: dict,
    scylla_version: str,
    cli_overrides: dict,
) -> dict:
    """Build final parameter dict for a Jenkins job.

    Priority: cli_overrides > job.params > defaults
    Always includes scylla_version.
    """

def trigger_jenkins_job(job_name: str, parameters: dict, dry_run: bool = False):
    """Trigger a Jenkins job via REST API or print in dry-run mode."""

def trigger_matrix(
    matrix_file: str,
    scylla_version: str,
    job_folder: str | None = None,
    labels_selector: str | None = None,
    backend: str | None = None,
    skip_jobs: str | None = None,
    dry_run: bool = False,
    **overrides,
):
    """Main entry point: load matrix, filter, build params, trigger jobs."""
```

**Key design decisions:**

1. **No image translation**: The trigger passes `scylla_version` only. Each downstream job uses SCT's built-in image lookup (enhanced by PR #13192) to find the correct image for its backend/region/arch.
2. **Labels filtering**: `--labels-selector "tier1,weekly"` means job must have ALL listed labels (AND logic). A single `--labels-selector` value is supported per invocation. To trigger different subsets, run the command multiple times with different selectors.
3. **Version exclusion**: If `scylla_version` starts with any prefix in `exclude_versions`, the job is skipped.
4. **Parameter priority**: CLI overrides > per-job params > matrix defaults. This allows the same YAML to work for scheduled runs and manual triggers with custom parameters.
5. **Jenkins triggering**: Use Jenkins REST API (`/job/{path}/buildWithParameters`) similar to how it's done today. The Jenkins URL and auth token come from environment variables available on the Jenkins agent.

**Definition of Done:**
- [ ] `filter_jobs()` handles version exclusion, labels matching, backend filtering, skip list
- [ ] `build_job_parameters()` merges defaults, job params, and CLI overrides correctly
- [ ] `determine_job_folder()` handles all version formats including full tags
- [ ] `trigger_jenkins_job()` calls Jenkins API (or prints in dry-run mode)
- [ ] `trigger_matrix()` orchestrates the full flow
- [ ] Unit tests for all functions with 90%+ coverage
- [ ] Unit tests verify label filtering, version exclusion, parameter merging

**Dependencies**: Phase 1

---

### Phase 3: CLI Command in `sct.py`

**Description**: Add the `trigger-matrix` CLI command to `sct.py`.

**Command signature:**

```
sct.py trigger-matrix
    --matrix <path>                  # YAML file (required)
    --scylla-version <version>       # Full version tag or branch:latest (required)
    --job-folder <folder>            # Override auto-detection (optional)
    --labels-selector <labels>       # Comma-separated labels to match (optional)
    --backend <backend>              # Filter by backend (optional)
    --skip-jobs <jobs>               # Comma-separated job names to skip (optional)
    --stress-duration <minutes>      # Override stress duration (optional)
    --region <region>                # Override region (optional)
    --provision-type <type>          # Override provision type (optional)
    --dry-run                        # Print without triggering (optional)
    --requested-by-user <user>       # User requesting the run (optional)
```

**Examples:**

```bash
# Weekly tier1 run with latest master
sct.py trigger-matrix --matrix configurations/triggers/tier1.yaml \
    --scylla-version "master:latest" --labels-selector "weekly"

# Release trigger with full version tag
sct.py trigger-matrix --matrix configurations/triggers/tier1.yaml \
    --scylla-version "2024.2.5-0.20250221.cb9e2a54ae6d-1"

# Sanity test for specific backend
sct.py trigger-matrix --matrix configurations/triggers/sanity.yaml \
    --scylla-version "2025.4.0-0.20260301.abc123-1" --backend aws

# Performance regression with label filter
sct.py trigger-matrix --matrix configurations/triggers/perf-regression.yaml \
    --scylla-version "master:latest" --labels-selector "master-weekly"

# Dry run to see what would be triggered
sct.py trigger-matrix --matrix configurations/triggers/tier1.yaml \
    --scylla-version "master:latest" --dry-run
```

**Definition of Done:**
- [ ] `trigger-matrix` command registered in `sct.py` with click decorators
- [ ] All options documented with help text
- [ ] Input validation (matrix file exists, version format valid)
- [ ] Dry-run output shows: job name, full Jenkins path, all parameters
- [ ] Integration test with a test YAML and `--dry-run`

**Dependencies**: Phase 2

---

### Phase 4: Groovy Pipeline Wrapper (`vars/triggerMatrixPipeline.groovy`)

**Description**: Create a thin Groovy pipeline that wraps the Python `trigger-matrix` command. This is used by Jenkins trigger jobs.

**Design:**

```groovy
// vars/triggerMatrixPipeline.groovy
def call(Map pipelineParams = [:]) {
    pipeline {
        agent { /* lightweight agent */ }

        parameters {
            string(name: 'matrix_file', defaultValue: pipelineParams.get('matrix_file', ''),
                   description: 'Path to trigger matrix YAML')
            string(name: 'scylla_version', defaultValue: '',
                   description: 'Scylla version (e.g., master:latest, 2024.2.5-0.20250221.xxx-1)')
            string(name: 'labels_selector', defaultValue: '',
                   description: 'Comma-separated labels to filter jobs')
            string(name: 'backend', defaultValue: '',
                   description: 'Filter by backend (aws/gce/azure)')
            string(name: 'skip_jobs', defaultValue: '',
                   description: 'Comma-separated job names to skip')
            string(name: 'stress_duration', defaultValue: '',
                   description: 'Override stress duration')
            string(name: 'region', defaultValue: '',
                   description: 'Override region for all jobs')
            string(name: 'provision_type', defaultValue: '',
                   description: 'spot|on_demand|spot_fleet')
            string(name: 'requested_by_user', defaultValue: '',
                   description: 'User requesting the run')
            booleanParam(name: 'dry_run', defaultValue: false,
                   description: 'Preview mode - do not trigger jobs')
        }

        // Optional cron triggers from pipelineParams
        triggers {
            parameterizedCron(pipelineParams.get('cron', ''))
        }

        stages {
            stage('Trigger Matrix') {
                steps {
                    script {
                        // Input sanitization
                        // Build command: ./docker/env/hydra.sh trigger-matrix --matrix ... [options]
                        // Execute
                    }
                }
            }
        }
    }
}
```

**Key points:**
- All input parameters are sanitized against regex patterns before shell execution
- The Groovy pipeline is a thin wrapper — all logic is in Python
- `matrix_file` can be baked into the Jenkinsfile (e.g., `matrix_file: 'configurations/triggers/tier1.yaml'`)
- Cron schedules can be defined in the YAML and passed as `pipelineParams.cron`

**Definition of Done:**
- [ ] `vars/triggerMatrixPipeline.groovy` created with parameter sanitization
- [ ] Input validation for all parameters (regex patterns)
- [ ] Builds and executes the `trigger-matrix` CLI command
- [ ] Supports `dry_run` parameter
- [ ] Supports cron scheduling from pipeline params

**Dependencies**: Phase 3

---

### Phase 5: YAML Matrix Files for Tier1, Sanity, and Performance

**Description**: Create the actual YAML matrix files that replace the existing XML/Groovy trigger definitions.

**Files to create:**
- `configurations/triggers/tier1.yaml` — All tier1 jobs (from XML triggers + master triggers)
- `configurations/triggers/sanity.yaml` — All sanity jobs (from sanity trigger Jenkinsfiles)
- `configurations/triggers/perf-regression.yaml` — All performance regression jobs (from `testRegionMatrix` in Groovy)

**Migration mapping:**

| Current Source | New YAML | Jobs |
|---|---|---|
| `tier1-aws-custom-time-trigger.xml` | `tier1.yaml` | 5 AWS tier1 jobs |
| `tier1-azure-custom-time-trigger.xml` | `tier1.yaml` | 1 Azure tier1 job |
| `tier1-gce-custom-time-trigger.xml` | `tier1.yaml` | 1 GCE tier1 job |
| `tier1-aws-aarch64-custom-time-trigger.xml` | `tier1.yaml` | 1 ARM64 tier1 job |
| `master-triggers/tier1-custom-time-trigger.xml` | `tier1.yaml` | 10 master tier1 jobs |
| `sanity-aws-trigger.jenkinsfile` | `sanity.yaml` | 1 AWS sanity job |
| `sanity-azure-trigger.jenkinsfile` | `sanity.yaml` | 1 Azure sanity job |
| `sanity-gcp-trigger.jenkinsfile` | `sanity.yaml` | 1 GCE sanity job |
| `additional-aws-trigger.jenkinsfile` | `sanity.yaml` | 6 additional AWS jobs |
| `perfRegressionParallelPipelinebyRegion.groovy` testRegionMatrix | `perf-regression.yaml` | 25+ perf jobs |

**Definition of Done:**
- [ ] `configurations/triggers/tier1.yaml` — complete with all tier1 jobs and cron schedules
- [ ] `configurations/triggers/sanity.yaml` — complete with all sanity jobs
- [ ] `configurations/triggers/perf-regression.yaml` — complete with all perf regression jobs
- [ ] Each YAML validated: loads without errors, dry-run shows correct job list
- [ ] Cross-reference with existing triggers to ensure no jobs are missing

**Dependencies**: Phase 1 (schema), Phase 3 (CLI for dry-run validation)

---

### Phase 6: Jenkins Trigger Job Definitions

**Description**: Create the thin Jenkinsfile trigger jobs that use `triggerMatrixPipeline()` with the appropriate YAML matrix files.

**Files to create/replace:**

```groovy
// jenkins-pipelines/oss/sct_triggers/tier1-trigger.jenkinsfile
def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)
triggerMatrixPipeline(
    matrix_file: 'configurations/triggers/tier1.yaml'
)

// jenkins-pipelines/oss/sct_triggers/sanity-trigger.jenkinsfile
def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)
triggerMatrixPipeline(
    matrix_file: 'configurations/triggers/sanity.yaml'
)

// jenkins-pipelines/master-triggers/sct_triggers/perf-regression-trigger.jenkinsfile
def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)
triggerMatrixPipeline(
    matrix_file: 'configurations/triggers/perf-regression.yaml'
)
```

Each is a one-liner that points to its YAML matrix. The pipeline handles parameters, cron scheduling, and invocation of the Python trigger logic.

**Definition of Done:**
- [ ] New trigger Jenkinsfiles created
- [ ] Jenkins jobs configured to use new Jenkinsfiles (manual step — outside PR scope)
- [ ] Old XML/Groovy trigger files marked for deprecation (not deleted yet)
- [ ] Documentation for how to switch Jenkins jobs to new triggers

**Dependencies**: Phase 4, Phase 5

---

### Phase 7: Deprecation and Cleanup

**Description**: After the new trigger system is validated in production, deprecate and remove the old trigger files.

**Files to deprecate/remove:**
- `jenkins-pipelines/oss/sct_triggers/tier1-aws-custom-time-trigger.xml`
- `jenkins-pipelines/oss/sct_triggers/tier1-azure-custom-time-trigger.xml`
- `jenkins-pipelines/oss/sct_triggers/tier1-gce-custom-time-trigger.xml`
- `jenkins-pipelines/oss/sct_triggers/tier1-aws-aarch64-custom-time-trigger.xml`
- `jenkins-pipelines/master-triggers/sct_triggers/tier1-custom-time-trigger.xml`
- `jenkins-pipelines/oss/sct_triggers/sanity-aws-trigger.jenkinsfile`
- `jenkins-pipelines/oss/sct_triggers/sanity-azure-trigger.jenkinsfile`
- `jenkins-pipelines/oss/sct_triggers/sanity-gcp-trigger.jenkinsfile`
- `jenkins-pipelines/oss/sct_triggers/additional-aws-trigger.jenkinsfile`
- `testRegionMatrix` from `vars/perfRegressionParallelPipelinebyRegion.groovy`

**Definition of Done:**
- [ ] New trigger system running successfully in production for at least 2 weeks
- [ ] Old trigger files removed
- [ ] No references to old trigger mechanism remain

**Dependencies**: Phase 6 validated in production

## 5. Testing Requirements

### Unit Tests (`unit_tests/test_trigger_matrix.py`)

| Test Area | Description |
|---|---|
| `test_load_matrix_config` | Valid YAML loads correctly, all fields populated |
| `test_load_matrix_config_invalid` | Missing required fields raise clear errors |
| `test_load_matrix_config_empty_jobs` | Empty job list handled gracefully |
| `test_determine_job_folder_master` | `master:latest` → `scylla-master` |
| `test_determine_job_folder_release` | `2025.4` → `branch-2025.4` |
| `test_determine_job_folder_full_tag` | `2024.2.5-0.20250221.xxx-1` → `branch-2024.2` |
| `test_determine_job_folder_explicit` | Explicit override takes precedence |
| `test_filter_jobs_no_filter` | All jobs returned when no filters applied |
| `test_filter_jobs_by_labels` | Only matching-label jobs returned |
| `test_filter_jobs_by_backend` | Only matching-backend jobs returned |
| `test_filter_jobs_by_version_exclusion` | Excluded version jobs removed |
| `test_filter_jobs_skip_list` | Named jobs skipped |
| `test_build_job_parameters_defaults` | Defaults applied correctly |
| `test_build_job_parameters_override` | CLI overrides take precedence |
| `test_build_job_parameters_version` | `scylla_version` always included |
| `test_trigger_matrix_dry_run` | Dry run produces output, no API calls |
| `test_trigger_matrix_full_flow` | End-to-end with mocked Jenkins API |

### Integration Tests

- [ ] `--dry-run` with real YAML files produces correct output
- [ ] Parameter merging works correctly with real config files

### Manual Testing

- [ ] Run `sct.py trigger-matrix --dry-run` in Jenkins environment
- [ ] Compare dry-run output with existing trigger behavior
- [ ] Trigger one job from each category (tier1, sanity, perf) via new system
- [ ] Verify cron scheduling works in Jenkins

## 6. Success Criteria

1. **Functional parity**: Every job currently triggered by XML/Groovy triggers is represented in the YAML matrix files and can be triggered via `trigger-matrix`.
2. **Dry-run validation**: `--dry-run` output for each YAML matrix file matches the expected job list and parameters from existing triggers.
3. **Version-first**: No AMI/image translation logic exists in the trigger layer. Only `scylla_version` is passed to downstream jobs.
4. **Test coverage**: 90%+ unit test coverage for `sdcm/utils/trigger_matrix.py`.
5. **Simplicity**: Adding a new job to any category requires only adding an entry to the YAML file — no Groovy, XML, or Python changes needed.
6. **One pipeline**: A single `triggerMatrixPipeline.groovy` serves tier1, sanity, and performance triggers — differentiated only by which YAML matrix file is referenced.

## 7. Risk Mitigation

### Risks

| Risk | Mitigation |
|---|---|
| **Jenkins API compatibility** | Use the same REST API mechanism already used by `simpleTrigger.groovy`. Test with dry-run before live execution. |
| **Missing jobs in migration** | Cross-reference each YAML with existing trigger files. Unit test that validates job count per category. |
| **Cron schedule regression** | Document existing schedules, verify new schedules match. Run parallel for initial period. |
| **Performance trigger complexity** | Perf triggers have additional fields (`sub_tests`, `labels` for cadence). YAML schema supports these via `params` and `labels`. Validate with dry-run. |
| **Full version tag not available for all backends** | PR #13192 is merged and supports all backends. If a backend doesn't have the requested version, SCT's existing error handling reports this. |
| **Rollback** | Old trigger files are not deleted until Phase 7. Can revert to old triggers at any time during validation. |

### Open Questions

1. **Cron schedule source**: Should cron schedules live in the YAML file (as `cron_triggers`), or remain as Jenkins pipeline trigger configuration? Current plan has them in YAML for single-source-of-truth, but Jenkins UI editing becomes harder.
2. **Job folder structure**: Tier1/sanity jobs follow the pattern `{job_folder}/tier1/longevity-*-test` where `job_folder` is derived from version (e.g., `scylla-master`, `branch-2025.4`). Performance jobs use a fixed structure like `scylla-enterprise/perf-regression/...` that does not change with version. **Proposed resolution**: `job_name` in the YAML can be either:
   - A **relative path** like `tier1/longevity-50gb-3days-test` — prefixed with auto-detected `{job_folder}` at runtime.
   - An **absolute path** (starting with `/` or containing the enterprise/master prefix already) — used as-is without prefixing.
   This allows both patterns to coexist in the same YAML. The `determine_job_folder()` function only applies to relative job names.
3. **`test_config` field purpose**: `test_config` in the YAML is documentation-only. Downstream Jenkinsfiles define their own `test_config`. The YAML `test_config` enables: (a) self-documentation of what each job runs, (b) validation that referenced config files exist, (c) future possibility of making triggers authoritative. It is optional — jobs work without it.
