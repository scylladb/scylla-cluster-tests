---
status: done
domain: ci-cd
created: 2026-04-06
last_updated: 2026-04-13
owner: null
---
# Triggers for perf-simple-query Tests Run as Offline Installer After PGO Builds

## 1. Problem Statement

The perf-simple-query microbenchmark tests currently run only via the weekly cron trigger in `perfRegressionParallelPipelinebyRegion.groovy`, using pre-installed Scylla AMIs or `scylla_version` parameters. There is no trigger to run these tests using the **offline installer** (unified package) after **PGO (Profile-Guided Optimization) builds** complete.

PGO builds produce optimized Scylla binaries distributed as unified (relocatable) packages. To validate that PGO-optimized builds maintain or improve performance, we need trigger pipelines that:

1. Hook into the PGO build completion flow (invoked as downstream jobs after PGO builds finish).
2. Run perf-simple-query tests against the PGO build artifacts using the offline installer mode (`unified_package` + `nonroot_offline_install`).
3. Cover both architectures (x86_64 and aarch64) and both read/write variants via **separate per-architecture triggers**.
4. Follow the same structure, parameters, and coding style as `perfRegressionParallelPipelinebyRegion.groovy`.

This work is tracked in Jira sub-task [SCT-194](https://scylladb.atlassian.net/browse/SCT-194).

## 2. Current State

### Existing perf-simple-query Jobs

Four Jenkins pipeline jobs exist under `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/`:

| Jenkinsfile | Architecture | Variant | Test Config |
|---|---|---|---|
| `scylla-enterprise-perf-simple-query-weekly-microbenchmark_arm64.jenkinsfile` | aarch64 | read | `amazon_perf_simple_query_ARM.yaml` + error thresholds |
| `scylla-enterprise-perf-simple-query-weekly-microbenchmark_arm64-write.jenkinsfile` | aarch64 | write | `amazon_perf_simple_query_ARM.yaml` + `perf_simple_write_option.yaml` |
| `scylla-enterprise-perf-simple-query-weekly-microbenchmark_x86_64.jenkinsfile` | x86_64 | read | `amazon_perf_simple_query_ARM.yaml` + `amazon_perf_simple_query_x86.yaml` + error thresholds |
| `scylla-enterprise-perf-simple-query-weekly-microbenchmark_x86_64-write.jenkinsfile` | x86_64 | write | `amazon_perf_simple_query_ARM.yaml` + `amazon_perf_simple_query_x86.yaml` + `perf_simple_write_option.yaml` |

All four use `longevityPipeline()` with:
- `test_name: 'microbenchmarking_test.PerfSimpleQueryTest.test_perf_simple_query'`
- `backend: 'aws'`, `region: 'eu-west-1'`, `provision_type: 'on_demand'`

### Current Trigger Mechanism

These jobs are triggered from `perfRegressionParallelPipelinebyRegion.groovy` (lines 174–209) as part of the `testRegionMatrix`. Each is configured with:
- `microbenchmark: true` — skips AMI image lookup
- `labels: ['master-weekly']` — only triggered on `master-weekly` cron schedule
- `ignore_versions: []` — runs for all versions
- `sub_tests: ['microbenchmark']`

The trigger pipeline (`perf-regression-trigger.jenkinsfile`) calls `perfRegressionParallelPipelinebyRegion()` which iterates the matrix, filters by version/labels, and invokes `build job:` for each matching entry.

### Offline Installer Support

The offline installer mode is already fully supported through the pipeline stack:

1. **`longevityPipeline.groovy`** (lines 78–83) — exposes `unified_package` and `nonroot_offline_install` parameters.
2. **`runSctTest.groovy`** (lines 145–147) — exports `SCT_UNIFIED_PACKAGE` and `SCT_NONROOT_OFFLINE_INSTALL` environment variables.
3. **`sdcm/sct_config.py`** — defines `unified_package`, `nonroot_offline_install`, and `install_mode` configuration fields.
4. **`sdcm/cluster.py`** — handles unified package download, architecture validation, and nonroot installation.

### What Is Missing

- **No trigger pipelines** exist to invoke perf-simple-query jobs with a `unified_package` URL after PGO builds.
- **No `unified_package` parameter** is passed from `perfRegressionParallelPipelinebyRegion.groovy` to downstream jobs.
- **No PGO-specific entry point** exists — the PGO build system has no way to invoke these perf tests with the built artifact URL.

## 3. Goals

1. **Two architecture-specific trigger pipelines** — `vars/perfSimpleQueryOfflineInstallerTriggerX86.groovy` and `vars/perfSimpleQueryOfflineInstallerTriggerArm.groovy` — each accepting a `unified_package` URL and triggering the corresponding architecture's perf-simple-query jobs using the offline installer mode.
2. **PGO build integration** — each trigger is invokable as a downstream job from the PGO build completion flow, accepting the unified package URL as a parameter. Separate triggers allow PGO builds to invoke only the relevant architecture.
3. **Full architecture coverage** — the x86 trigger covers x86_64 read and write variants; the ARM trigger covers arm64 read and write variants.
4. **Consistent style** — follows the structure, parameter naming, and coding patterns of `perfRegressionParallelPipelinebyRegion.groovy`.
5. **Reusable Jenkinsfiles** — reuses the existing perf-simple-query `.jenkinsfile` definitions (which already support `unified_package` via `longevityPipeline()`).
6. **New Jenkinsfile trigger entry points** for Jenkins to discover and invoke each pipeline independently.

## 4. Implementation Phases

### Phase 1: Create the Trigger Pipelines

**Description**: Implement two shared library functions — `vars/perfSimpleQueryOfflineInstallerTriggerX86.groovy` and `vars/perfSimpleQueryOfflineInstallerTriggerArm.groovy` — each accepting a `unified_package` URL and triggering its architecture's perf-simple-query jobs with offline installer parameters.

**Key design decisions**:
- Follow the same `def call(Map pipelineParams)` signature as `perfRegressionParallelPipelinebyRegion.groovy`.
- **Two separate triggers per architecture** instead of one combined trigger. This was chosen because PGO builds produce separate per-architecture packages, and each architecture can be triggered independently without requiring the other.
- Each trigger defines a `testMatrix` list of job entries with `job_name` and `region` fields scoped to its architecture.
- Accept parameters: `unified_package` (required), `nonroot_offline_install` (boolean, default true), `requested_by_user`, `billing_project`.
- **No `scylla_version` parameter** — removed as unnecessary; the unified package URL is sufficient for tracking and the downstream jobs do not need a separate version string.
- Use `build job:` with `wait: false` to trigger each job asynchronously, passing `unified_package` and `nonroot_offline_install` through to the downstream `longevityPipeline()`.
- No cron triggers — these pipelines are invoked externally by PGO build jobs.
- No AMI/image lookup needed — offline installer uses the unified package directly.

**Files created**:
- `vars/perfSimpleQueryOfflineInstallerTriggerX86.groovy` — triggers x86_64 read and write jobs
- `vars/perfSimpleQueryOfflineInstallerTriggerArm.groovy` — triggers arm64 read and write jobs

**Definition of Done**:
- Each pipeline accepts a `unified_package` URL and triggers its two architecture-specific perf-simple-query jobs.
- Parameters are consistent with `perfRegressionParallelPipelinebyRegion.groovy` naming.
- Jobs receive `unified_package` and `nonroot_offline_install` parameters.
- No cron trigger — designed for external invocation.

### Phase 2: Create the Jenkinsfile Entry Points

**Description**: Create two Jenkinsfiles that call their respective trigger functions, providing Jenkins-discoverable entry points.

**Files created**:
- `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/perf-simple-query-offline-installer-trigger-x86.jenkinsfile` — calls `perfSimpleQueryOfflineInstallerTriggerX86()`
- `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/perf-simple-query-offline-installer-trigger-arm.jenkinsfile` — calls `perfSimpleQueryOfflineInstallerTriggerArm()`

**Definition of Done**:
- Both Jenkinsfiles exist and call their respective shared library functions.
- Follow the same pattern as `perf-regression-trigger.jenkinsfile`.
- Each can be registered as a Jenkins job that PGO build pipelines can invoke as a downstream build for the relevant architecture.

### Phase 3: Validate End-to-End (Manual)

**Description**: Validate both trigger pipelines work correctly by manually triggering them with test unified package URLs.

**Definition of Done**:
- Each pipeline can be triggered manually from Jenkins with a `unified_package` URL.
- The x86 trigger starts the two x86_64 downstream jobs; the ARM trigger starts the two arm64 downstream jobs.
- Downstream jobs receive the correct `unified_package` and `nonroot_offline_install` parameters.
- Jobs use the offline installer to install Scylla from the unified package.

**Dependencies**: Requires Jenkins access and valid architecture-specific unified package URLs.

## 5. Testing Requirements

### Automated Testing

- **No unit tests needed** — this is a Groovy pipeline definition. Groovy pipeline code in this repository is not unit-tested (consistent with existing patterns).
- **Syntax validation** — verify the Groovy files parse correctly by running pre-commit checks.

### Manual Testing

1. **Trigger each pipeline manually** in Jenkins with a `unified_package` URL pointing to a valid unified package for the corresponding architecture.
2. **Verify downstream jobs are invoked** — check Jenkins build queue for the two architecture-specific perf-simple-query jobs per trigger.
3. **Verify parameter passthrough** — confirm downstream jobs receive `unified_package` and `nonroot_offline_install` values.
4. **Verify offline installation** — confirm at least one downstream job per architecture successfully installs Scylla from the unified package and runs the perf-simple-query test.
5. **Verify PGO integration** — trigger from an actual PGO build completion flow and confirm the full chain works for both architectures.

## 6. Success Criteria

1. Two new Jenkins jobs exist (one per architecture) that can be invoked with a `unified_package` URL after PGO builds complete.
2. The x86 trigger invokes x86_64 read and write variants; the ARM trigger invokes arm64 read and write variants.
3. The offline installer mode is correctly configured for each downstream job.
4. The trigger pipelines follow the same coding style and parameter conventions as `perfRegressionParallelPipelinebyRegion.groovy`.
5. No existing pipelines or jobs are modified or broken.

## 7. Risk Mitigation

| Risk | Mitigation |
|---|---|
| PGO build system integration details unknown | The trigger pipelines are designed to be invoked generically via `build job:` with a `unified_package` URL. PGO build integration requires only adding a downstream `build` step in the PGO pipeline. No SCT-side coupling to PGO internals. |
| Unified package URL format may vary by architecture | **Resolved**: By splitting into two separate per-architecture triggers, each PGO build invokes only the trigger matching its architecture with the correct unified package URL. No need for a single trigger to handle multiple architecture-specific URLs. |
| Nonroot installation may fail on certain AMI bases | The offline installer automatically selects Ubuntu 24.04 base AMIs (handled in `sct_config.py` lines 2737–2758). This is well-tested for existing offline install flows. |
| Downstream jobs may not be registered in Jenkins yet | The existing four perf-simple-query Jenkinsfiles are already registered. No new downstream jobs need to be created. |
| Pre-commit hooks may generate auto-generated files | Follow the convention of not committing auto-generated nemesis/Jenkins pipeline files. Only the new files in `vars/` and `jenkins-pipelines/` should be committed. |
