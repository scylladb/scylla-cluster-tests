---
status: draft
domain: ci-cd
created: 2026-04-06
last_updated: 2026-04-06
owner: null
---
# Trigger for perf-simple-query Tests Run as Offline Installer After PGO Builds

## 1. Problem Statement

The perf-simple-query microbenchmark tests currently run only via the weekly cron trigger in `perfRegressionParallelPipelinebyRegion.groovy`, using pre-installed Scylla AMIs or `scylla_version` parameters. There is no trigger to run these tests using the **offline installer** (unified package) after **PGO (Profile-Guided Optimization) builds** complete.

PGO builds produce optimized Scylla binaries distributed as unified (relocatable) packages. To validate that PGO-optimized builds maintain or improve performance, we need a trigger pipeline that:

1. Hooks into the PGO build completion flow (invoked as a downstream job after PGO builds finish).
2. Runs perf-simple-query tests against the PGO build artifacts using the offline installer mode (`unified_package` + `nonroot_offline_install`).
3. Covers both architectures (x86_64 and aarch64) and both read/write variants.
4. Follows the same structure, parameters, and coding style as `perfRegressionParallelPipelinebyRegion.groovy`.

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

- **No trigger pipeline** exists to invoke perf-simple-query jobs with a `unified_package` URL after PGO builds.
- **No `unified_package` parameter** is passed from `perfRegressionParallelPipelinebyRegion.groovy` to downstream jobs.
- **No PGO-specific entry point** exists — the PGO build system has no way to invoke these perf tests with the built artifact URL.

## 3. Goals

1. **New trigger pipeline** (`vars/perfSimpleQueryOfflineInstallerTrigger.groovy`) that accepts a `unified_package` URL and triggers perf-simple-query jobs using the offline installer mode.
2. **PGO build integration** — the trigger is invokable as a downstream job from the PGO build completion flow, accepting the unified package URL as a parameter.
3. **Full architecture coverage** — triggers all four perf-simple-query variants (arm64 read, arm64 write, x86_64 read, x86_64 write).
4. **Consistent style** — follows the structure, parameter naming, and coding patterns of `perfRegressionParallelPipelinebyRegion.groovy`.
5. **Reusable Jenkinsfiles** — reuses the existing perf-simple-query `.jenkinsfile` definitions (which already support `unified_package` via `longevityPipeline()`).
6. **New Jenkinsfile trigger entry point** for Jenkins to discover and invoke the pipeline.

## 4. Implementation Phases

### Phase 1: Create the Trigger Pipeline

**Description**: Implement `vars/perfSimpleQueryOfflineInstallerTrigger.groovy` — a shared library function that accepts a `unified_package` URL and triggers the four perf-simple-query jobs with offline installer parameters.

**Key design decisions**:
- Follow the same `def call(Map pipelineParams)` signature as `perfRegressionParallelPipelinebyRegion.groovy`.
- Define a `testMatrix` list of job entries with `job_name`, `region`, `arch`, and `sub_tests` fields.
- Accept parameters: `unified_package` (required), `nonroot_offline_install` (boolean, default true), `scylla_version` (for Argus tracking), `requested_by_user`, `billing_project`.
- Use `build job:` with `wait: false` to trigger each job asynchronously, passing `unified_package` and `nonroot_offline_install` through to the downstream `longevityPipeline()`.
- No cron triggers — this pipeline is invoked externally by PGO build jobs.
- No AMI/image lookup needed — offline installer uses the unified package directly.

**Files to create**:
- `vars/perfSimpleQueryOfflineInstallerTrigger.groovy`

**Definition of Done**:
- Pipeline accepts `unified_package` URL and triggers all four perf-simple-query jobs.
- Parameters are consistent with `perfRegressionParallelPipelinebyRegion.groovy` naming.
- Jobs receive `unified_package` and `nonroot_offline_install` parameters.
- No cron trigger — designed for external invocation.

### Phase 2: Create the Jenkinsfile Entry Point

**Description**: Create a Jenkinsfile that calls `perfSimpleQueryOfflineInstallerTrigger()`, providing a Jenkins-discoverable entry point.

**Files to create**:
- `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/perf-simple-query-offline-installer-trigger.jenkinsfile`

**Definition of Done**:
- Jenkinsfile exists and calls the shared library function.
- Follows the same pattern as `perf-regression-trigger.jenkinsfile`.
- Can be registered as a Jenkins job that PGO build pipelines can invoke as a downstream build.

### Phase 3: Validate End-to-End (Manual)

**Description**: Validate the trigger pipeline works correctly by manually triggering it with a test unified package URL.

**Definition of Done**:
- Pipeline can be triggered manually from Jenkins with a `unified_package` URL.
- All four downstream perf-simple-query jobs are started.
- Downstream jobs receive the correct `unified_package` and `nonroot_offline_install` parameters.
- Jobs use the offline installer to install Scylla from the unified package.

**Dependencies**: Requires Jenkins access and a valid unified package URL.

## 5. Testing Requirements

### Automated Testing

- **No unit tests needed** — this is a Groovy pipeline definition. Groovy pipeline code in this repository is not unit-tested (consistent with existing patterns).
- **Syntax validation** — verify the Groovy file parses correctly by running pre-commit checks.

### Manual Testing

1. **Trigger pipeline manually** in Jenkins with a `unified_package` URL pointing to a valid unified package.
2. **Verify downstream jobs are invoked** — check Jenkins build queue for all four perf-simple-query jobs.
3. **Verify parameter passthrough** — confirm downstream jobs receive `unified_package` and `nonroot_offline_install` values.
4. **Verify offline installation** — confirm at least one downstream job successfully installs Scylla from the unified package and runs the perf-simple-query test.
5. **Verify PGO integration** — trigger from an actual PGO build completion flow and confirm the full chain works.

## 6. Success Criteria

1. A new Jenkins job exists that can be invoked with a `unified_package` URL after PGO builds complete.
2. All four perf-simple-query variants (arm64/x86_64 × read/write) are triggered.
3. The offline installer mode is correctly configured for each downstream job.
4. The trigger pipeline follows the same coding style and parameter conventions as `perfRegressionParallelPipelinebyRegion.groovy`.
5. No existing pipelines or jobs are modified or broken.

## 7. Risk Mitigation

| Risk | Mitigation |
|---|---|
| PGO build system integration details unknown | The trigger pipeline is designed to be invoked generically via `build job:` with a `unified_package` URL. PGO build integration requires only adding a downstream `build` step in the PGO pipeline. No SCT-side coupling to PGO internals. |
| Unified package URL format may vary by architecture | The trigger matrix includes explicit `arch` fields per job entry. Each job's test config already specifies the correct instance type for its architecture (`c7g.large` for ARM, `c7i.large` for x86). The upstream PGO build should provide architecture-specific unified package URLs, or a single URL if the package is multi-arch. **Needs Investigation**: Confirm whether PGO builds produce separate per-arch packages or a single multi-arch package. If per-arch, the trigger may need separate `unified_package_arm64` and `unified_package_x86_64` parameters. |
| Nonroot installation may fail on certain AMI bases | The offline installer automatically selects Ubuntu 24.04 base AMIs (handled in `sct_config.py` lines 2737–2758). This is well-tested for existing offline install flows. |
| Downstream jobs may not be registered in Jenkins yet | The existing four perf-simple-query Jenkinsfiles are already registered. No new downstream jobs need to be created. |
| Pre-commit hooks may generate auto-generated files | Follow the convention of not committing auto-generated nemesis/Jenkins pipeline files. Only the new files in `vars/` and `jenkins-pipelines/` should be committed. |
