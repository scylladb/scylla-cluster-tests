---
status: draft
domain: ci-cd
created: 2026-03-09
last_updated: 2026-03-15
owner: fruch
---

# Jenkins Pipeline Configuration Linter

## Problem Statement

The current YAML configuration linting approach (`utils/lint_test_cases.sh` + `sct.py lint-yamls`) has fundamental design issues:

1. **Guessing instead of knowing**: The bash script uses filename patterns and hard-coded include/exclude regex filters to guess which backend and environment variables a test case needs. This means every new test case or renamed file risks being silently skipped or misconfigured for linting.
2. **Disconnect from reality**: Jenkins pipelines are the source of truth for how test configurations are actually used — they define the `backend`, `region`, `test_config` (YAML files), and additional environment overrides. The current linter ignores this entirely and invents its own mapping.
3. **Slow image resolution**: `SCTConfiguration()` instantiation triggers cloud API calls for image resolution (AWS AMI, GCE image, Azure image lookups), which dominates lint time. With ~995 pipeline files, this makes comprehensive linting impractical.
4. **Incomplete coverage**: The bash script covers a subset of backends and configurations via hand-curated filters. New pipelines added to `jenkins-pipelines/` are not automatically picked up.
5. **Difficult to maintain**: The review discussion on [PR #12787](https://github.com/scylladb/scylla-cluster-tests/pull/12787) highlighted that both the original bash script and the proposed Python replacement were hard to extend, understand, and maintain.

The core issue is that the linter guesses what it should validate, rather than using the actual source of truth.

### Key Insight from PR #12787 Review

[Comment by @soyacz](https://github.com/scylladb/scylla-cluster-tests/pull/12787#issuecomment-2597946253):
> "For each jenkinsfile (our test case) → load yamls like sct config does → each param that requires other params to be specific (based on backend or other param) should implement 'lint' function"

[Comment by @fruch](https://github.com/scylladb/scylla-cluster-tests/pull/12787#issuecomment-2608523987):
> "the scan of yamls should be replaced with a scan of jenkinsfile we have and using [them] as it would be used in pipeline with the same environment vars overwrites and the set of configuration files together"

This plan implements that vision: scan Jenkins pipeline files directly, extract the configuration each pipeline uses, and validate it — no guessing, no filters.

### Dependency: Lazy Image Resolution (PR #13877)

[PR #13877](https://github.com/scylladb/scylla-cluster-tests/pull/13877) adds a plan (`docs/plans/sct-config-validation-and-lazy-images.md`) to extract cloud image resolution out of `SCTConfiguration.__init__()` into an explicit `resolve_images()` step. Once that work is completed (Phase 2 of that plan), `SCTConfiguration()` can be instantiated **without network access** — no AWS AMI lookups, no GCE image resolution, no Azure image queries.

This is a critical enabler for the pipeline linter:
- **Before Phase 2 of PR #13877**: Every `SCTConfiguration()` call triggers cloud API calls, requiring mock environment variables (`SCT_AMI_ID_DB_SCYLLA=ami-1234`, etc.) and making parallel execution slow and flaky.
- **After Phase 2 of PR #13877**: `SCTConfiguration()` is a pure data operation — fast, deterministic, and safe to run hundreds of times in parallel without network access.

**Implementation order**: Phase 2 of the lazy image resolution plan should be completed first. The pipeline linter can then skip all image-related mock variables and focus purely on structural configuration validation.

## Current State

### File: `utils/lint_test_cases.sh`

A bash script that runs `sct.py lint-yamls` multiple times with different backend/filter/env-var combinations:

```bash
# AWS — broad include, long exclude list
SCT_AMI_ID_DB_SCYLLA=ami-1234 ./sct.py lint-yamls -i '.yaml' -e 'azure,multi-dc,...'

# Azure — only azure-matching files
SCT_AZURE_IMAGE_DB=image SCT_AZURE_REGION_NAME="eastus" ./sct.py lint-yamls --backend azure -i azure

# GCE — specific includes, exclude azure/docker/multi-dc
SCT_GCE_IMAGE_DB=image SCT_SCYLLA_REPO='...' ./sct.py lint-yamls -b gce -i 'rolling,...'

# Multi-DC GCE with 2 regions
SCT_GCE_IMAGE_DB=image SCT_GCE_DATACENTER="us-east1 us-west1" ./sct.py lint-yamls -b gce -i 'multi-dc-rolling,...'

# Multi-DC GCE with 3 regions
# ...same pattern with 3 datacenters...

# Multi-DC AWS with 5 regions
SCT_AMI_ID_DB_SCYLLA="ami-1 ami-2 ami-3 ami-4 ami-5" SCT_REGION_NAME="..." ./sct.py lint-yamls -b aws -i '5dcs'

# Baremetal
SCT_DB_NODES_PUBLIC_IP='["127.0.0.1", "127.0.0.2"]' ./sct.py lint-yamls -b baremetal -i 'baremetal'

# K8S
SCT_AMI_ID_DB_SCYLLA=ami-1234 ./sct.py lint-yamls -b k8s-local-kind -i 'scylla-operator.*\.yaml'
```

**Problems**: Hard-coded filters, no alignment with how pipelines actually use configs, manual multi-DC region counts, must be updated for every new test case pattern.

### File: `sct.py` — `lint_yamls` command

```python
@cli.command(help="Test yaml in test-cases directory")
@click.option("-b", "--backend", ...)
@click.option("-i", "--include", ...)
@click.option("-e", "--exclude", ...)
def lint_yamls(backend, exclude, include):
    process_pool = ProcessPoolExecutor(max_workers=5)
    features = []
    for root, _, files in os.walk("./test-cases"):
        for file in files:
            # apply include/exclude regex filters
            features.append(process_pool.submit(_run_yaml_test, backend, full_path, original_env))
    # collect results, print errors in red, successes in green
```

### File: `sct.py` — `_run_yaml_test` helper

```python
def _run_yaml_test(backend, full_path, env):
    os.environ["SCT_CLUSTER_BACKEND"] = backend
    os.environ["SCT_CONFIG_FILES"] = full_path
    config = SCTConfiguration()
    config.verify_configuration()
    config.check_required_files()
```

**Key observations**:
- Already uses `ProcessPoolExecutor(max_workers=5)` for parallel validation
- Walks `test-cases/` directory only — ignores `configurations/` and `jenkins-pipelines/`
- Validates a single YAML file at a time, but pipelines often use multiple YAML files together
- Requires mock image variables in environment to prevent cloud API failures

### File: `jenkins-pipelines/` (~995 `.jenkinsfile` files)

Pipeline files follow a consistent pattern:

```groovy
def lib = library identifier: 'sct@snapshot', retriever: legacySCM(scm)

<pipelineFunction>(
    backend: '<backend>',
    region: '<region>',
    test_name: '<test.module.Class.method>',
    test_config: '<yaml_path_or_list>',
    // ... additional parameters
)
```

**Pipeline functions** (defined in `vars/*.groovy`):
- `longevityPipeline` — most common (~600+ files)
- `managerPipeline` — manager tests (~34 files)
- `rollingUpgradePipeline` — upgrade tests (~21 files)
- `perfRegressionParallelPipeline` — performance tests (~55 files)
- `artifactsPipeline` — artifact tests (~32 files)
- `jepsenPipeline` — Jepsen tests
- `rollingOperatorUpgradePipeline` — K8S operator upgrades
- `perfSearchBestConfigParallelPipeline` — performance search
- `byoLongevityPipeline` — bring-your-own (no test_config)
- `createTestJobPipeline` — job creation (no test_config)

**`test_config` formats observed**:
1. Single file string: `'test-cases/longevity/longevity-200GB-48h.yaml'`
2. Python-style list string: `"['configs/a.yaml', 'configs/b.yaml']"`
3. JSON array string: `'''["test-cases/x.yaml", "configurations/y.yaml"]'''`

**Backends observed**: `aws`, `gce`, `azure`, `docker`, `k8s-eks`, `k8s-gke`, `k8s-local-kind-aws`, `k8s-local-kind-gce`, `xcloud`, `baremetal`

### File: `vars/runSctTest.groovy` — Environment variable mapping

The Groovy pipeline functions convert pipeline parameters to SCT environment variables:

```groovy
def test_config = groovy.json.JsonOutput.toJson(params.test_config)
// ...
export SCT_CLUSTER_BACKEND="${params.backend}"
export SCT_CONFIG_FILES=${test_config}
export SCT_REGION_NAME=${current_region}
// + many more SCT_* variables based on pipeline parameters
```

**Parameters that become environment variables**:

| Pipeline Parameter | Environment Variable |
|---|---|
| `backend` | `SCT_CLUSTER_BACKEND` |
| `test_config` | `SCT_CONFIG_FILES` |
| `region` | `SCT_REGION_NAME` |
| `gce_datacenter` | `SCT_GCE_DATACENTER` |
| `azure_region_name` | `SCT_AZURE_REGION_NAME` |
| `availability_zone` | `SCT_AVAILABILITY_ZONE` |
| `scylla_version` | `SCT_SCYLLA_VERSION` |
| `new_version` | `SCT_NEW_VERSION` |
| `provision_type` | `SCT_INSTANCE_PROVISION` |
| `extra_environment_variables` | Parsed and exported directly |

### File: `sdcm/sct_config.py` — `SCTConfiguration` class

- `__init__` (~470 lines): Loads YAML files, env vars, merges config, resolves images, runs inline validation
- `verify_configuration()`: Runs backend-specific validation, checks required fields
- `check_required_files()`: Validates that referenced files exist

## Goals

1. **Source-of-truth linting**: Validate configurations exactly as Jenkins pipelines define them — same backend, same YAML file list, same environment overrides — eliminating all guesswork.
2. **Complete coverage**: Automatically discover and lint all `~995` pipeline files under `jenkins-pipelines/`, not just a hand-curated subset.
3. **Maximum parallelism**: Lint hundreds of pipelines concurrently using `ProcessPoolExecutor` with worker count scaled to available CPUs, targeting significant improvement over the current sequential bash script approach (which runs 8 serial `sct.py lint-yamls` invocations).
4. **Failure-only output**: Print nothing for passing pipelines; show the pipeline file path and validation error only for failures. Exit code 0 if all pass, 1 if any fail.
5. **Replace the bash script**: The new `sct.py lint-pipelines` command replaces `utils/lint_test_cases.sh` and the Jenkinsfile stage that calls it.
6. **No backend guessing**: The backend comes directly from the pipeline file's `backend:` parameter — not from filename patterns.

## Implementation Phases

### Phase 1: Jenkinsfile Parser Module

**Objective**: Create a module that parses `.jenkinsfile` files and extracts pipeline parameters as structured data.

**Deliverables**:
- New module: `sdcm/utils/lint/jenkins_parser.py`
- Dataclass `PipelineConfig` holding extracted parameters:
  ```python
  @dataclass
  class PipelineConfig:
      file_path: str               # path to the .jenkinsfile
      pipeline_function: str       # e.g. "longevityPipeline", "managerPipeline"
      test_config: list[str]       # list of YAML config file paths (parsed from all formats)
      params: dict[str, str]       # ALL named parameters from the pipeline function call
  ```
  The parser extracts **every** named parameter from the pipeline call into `params` — no pre-defined field list, no filtering. This mirrors how `runSctTest.groovy` iterates over `params.*` and exports each one. Convenience accessors like `config.backend` can be properties that read from `params["backend"]`, but the underlying storage is the raw dict.
- Parser function: `parse_jenkinsfile(path: Path) -> PipelineConfig | None`
  - Uses regex to extract the pipeline function name and named parameters
  - Handles all 3 `test_config` formats (single string, Python list, JSON array)
  - Returns `None` for pipelines without `test_config` (e.g., `byoLongevityPipeline`, `createTestJobPipeline`)
- Discovery function: `discover_pipeline_files(root: Path) -> list[Path]`
  - Recursively finds all `.jenkinsfile` files under `jenkins-pipelines/`

**Parsing approach**: Use [`python-groovy-parser`](https://github.com/inab/python-groovy-parser) (`groovy-parser` on PyPI) — a Python library that implements a full Groovy 3.0.X parser using Pygments + Lark + the official Groovy grammar. This provides a proper parse tree instead of fragile regex matching, significantly reducing the risk of edge-case failures.

The library produces a JSON-serializable parse tree from which named parameters can be extracted by walking the tree nodes. It also supports caching parsed results for faster subsequent runs.

**Fallback**: If `python-groovy-parser` proves too slow for ~995 files or has compatibility issues with our Jenkinsfile patterns, a regex-based fallback can be implemented. The pipeline files follow a consistent pattern (single function call with named arguments), so regex extraction is viable but less robust. The regex would need to handle:
- Single-quoted strings: `backend: 'aws'`
- Double-quoted strings: `backend: "aws"`
- Triple-quoted strings: `test_config: '''["a.yaml", "b.yaml"]'''`
- Groovy list literals: `sub_tests: ["test_write", "test_read"]`
- Map literals: `timeout: [time: 530, unit: 'MINUTES']`

**Evaluation criteria for Phase 1**: Test `python-groovy-parser` against a representative sample of pipeline files early. If it correctly parses all patterns and completes ~995 files within acceptable time (with caching), use it. If not, fall back to regex.

**`pipelineParams` vs `params`**: `runSctTest.groovy` receives two arguments: `params` (Jenkins job parameters) and `pipelineParams` (a map passed from individual pipeline functions like `longevityPipeline.groovy`). Some k8s parameters (`k8s_enable_performance_tuning`, `k8s_log_api_calls`, `k8s_deploy_monitoring`, `k8s_scylla_utils_docker_image`) come via `pipelineParams`. However, these also appear as named parameters in `.jenkinsfile` files, so the parser extracts them normally — the distinction is an implementation detail of `runSctTest.groovy`, not of the parser.

**Pipelines to skip** (no `test_config`): `byoLongevityPipeline()`, `createTestJobPipeline()`, raw `pipeline { }` blocks without a wrapper function call (e.g., `qa/hydra-show-jepsen-results.jenkinsfile`).

**Definition of Done**:
- [ ] Parser correctly extracts parameters from all ~995 pipeline files
- [ ] Unit tests cover all `test_config` formats, all backends, multi-region, edge cases
- [ ] `discover_pipeline_files()` finds all `.jenkinsfile` files
- [ ] Pipelines without `test_config` return `None` (skipped gracefully)

**Dependencies**: Add `groovy-parser` to `pyproject.toml` dependencies. No SCT config involvement — pure parsing.

---

### Phase 2: Environment Builder

**Objective**: Convert `PipelineConfig` into the environment variables that `SCTConfiguration` expects, mirroring what the Groovy pipeline functions do in `vars/runSctTest.groovy`.

**Deliverables**:
- New module: `sdcm/utils/lint/env_builder.py`
- Function: `build_env(config: PipelineConfig) -> dict[str, str]`
  - Maps pipeline parameters to `SCT_*` environment variables
  - Handles multi-region: counts regions and generates matching dummy image/AMI placeholders
  - For backends requiring images (aws, gce, azure): generates placeholder values (e.g., `SCT_AMI_ID_DB_SCYLLA=ami-placeholder`) **only if** lazy image resolution (PR #13877 Phase 2) is not yet available. Detection: attempt to instantiate `SCTConfiguration()` and check whether `resolve_images()` exists as a method — if it does, skip placeholder generation.
  - For `baremetal`: generates `SCT_DB_NODES_PUBLIC_IP` with placeholder IPs
  - For `k8s-*`: sets `SCT_CLUSTER_BACKEND` to the k8s variant
  - Handles `extra_environment_variables` parameter (parses `'SCT_N_DB_NODES=9'` style strings)

**Environment variable mapping** (mirrors `vars/runSctTest.groovy`):

The `build_env()` function iterates over **all** parameters in `PipelineConfig.params`, exactly as `runSctTest.groovy` iterates over `params.*`. For each parameter, the mapping table determines the corresponding `SCT_*` env var. The full mapping table (derived from `vars/runSctTest.groovy`) is:

| Pipeline parameter | Environment variable | Notes |
|---|---|---|
| `backend` | `SCT_CLUSTER_BACKEND` | Always set |
| `test_config` | `SCT_CONFIG_FILES` (JSON-encoded list) | Always set |
| `region` | `SCT_REGION_NAME` | JSON-encoded if list |
| `gce_datacenter` | `SCT_GCE_DATACENTER` | JSON-encoded if list |
| `azure_region_name` | `SCT_AZURE_REGION_NAME` | Direct |
| `availability_zone` | `SCT_AVAILABILITY_ZONE` | Direct |
| `scylla_version` | `SCT_SCYLLA_VERSION` | Direct |
| `provision_type` | `SCT_INSTANCE_PROVISION` | Adapted name |
| `instance_provision_fallback_on_demand` | `SCT_INSTANCE_PROVISION_FALLBACK_ON_DEMAND` | Direct |
| `new_version` | `SCT_NEW_VERSION` | Direct |
| `k8s_enable_performance_tuning` | `SCT_K8S_ENABLE_PERFORMANCE_TUNING` | From `pipelineParams` in `runSctTest.groovy` (line 96) |
| `k8s_log_api_calls` | `SCT_K8S_LOG_API_CALLS` | From `pipelineParams` in `runSctTest.groovy` (line 99) |
| `k8s_deploy_monitoring` | `SCT_K8S_DEPLOY_MONITORING` | From `pipelineParams` in `runSctTest.groovy` (line 225) |
| `k8s_scylla_utils_docker_image` | `SCT_K8S_SCYLLA_UTILS_DOCKER_IMAGE` | From `pipelineParams` in `runSctTest.groovy` (line 229) |
| `linux_distro` | `SCT_SCYLLA_LINUX_DISTRO` | Adapted name (note: `SCT_SCYLLA_LINUX_DISTRO` not `SCT_LINUX_DISTRO`) |
| `scylla_ami_id` | `SCT_AMI_ID_DB_SCYLLA` | Adapted name |
| `gce_image_db` | `SCT_GCE_IMAGE_DB` | Direct |
| `azure_image_db` | `SCT_AZURE_IMAGE_DB` | Direct |
| `scylla_repo` | `SCT_SCYLLA_REPO` | Direct |
| `new_scylla_repo` | `SCT_NEW_SCYLLA_REPO` | Direct |
| `oracle_scylla_version` | `SCT_ORACLE_SCYLLA_VERSION` | Direct |
| `manager_version` | `SCT_MANAGER_VERSION` | Direct |
| `target_manager_version` | `SCT_TARGET_MANAGER_VERSION` | Direct |
| `scylla_mgmt_agent_version` | `SCT_SCYLLA_MGMT_AGENT_VERSION` | Direct |
| `scylla_mgmt_agent_address` | `SCT_SCYLLA_MGMT_AGENT_ADDRESS` | Direct |
| `scylla_mgmt_address` | `SCT_SCYLLA_MGMT_ADDRESS` | Direct |
| `backup_bucket_backend` | `SCT_BACKUP_BUCKET_BACKEND` | Adapted name |
| `k8s_version` | `SCT_EKS_CLUSTER_VERSION` + `SCT_GKE_CLUSTER_VERSION` | Sets both simultaneously (see `runSctTest.groovy` lines 72-74) |
| `k8s_scylla_operator_docker_image` | `SCT_K8S_SCYLLA_OPERATOR_DOCKER_IMAGE` | Direct |
| `k8s_scylla_operator_upgrade_docker_image` | `SCT_K8S_SCYLLA_OPERATOR_UPGRADE_DOCKER_IMAGE` | Direct |
| `k8s_scylla_operator_helm_repo` | `SCT_K8S_SCYLLA_OPERATOR_HELM_REPO` | Direct |
| `k8s_scylla_operator_upgrade_helm_repo` | `SCT_K8S_SCYLLA_OPERATOR_UPGRADE_HELM_REPO` | Direct |
| `k8s_scylla_operator_chart_version` | `SCT_K8S_SCYLLA_OPERATOR_CHART_VERSION` | Direct |
| `k8s_scylla_operator_upgrade_chart_version` | `SCT_K8S_SCYLLA_OPERATOR_UPGRADE_CHART_VERSION` | Direct |
| `k8s_enable_tls` | `SCT_K8S_ENABLE_TLS` | Direct |
| `k8s_enable_sni` | `SCT_K8S_ENABLE_SNI` | Direct |
| `docker_image` | `SCT_DOCKER_IMAGE` | Direct |
| `xcloud_provider` | `SCT_XCLOUD_PROVIDER` | Direct |
| `xcloud_env` | `SCT_XCLOUD_ENV` | Direct |
| `stress_duration` | `SCT_STRESS_DURATION` | Direct |
| `prepare_stress_duration` | `SCT_PREPARE_STRESS_DURATION` | Direct |
| `gemini_seed` | `SCT_GEMINI_SEED` | Direct |
| `post_behavior_db_nodes` | `SCT_POST_BEHAVIOR_DB_NODES` | Direct |
| `post_behavior_loader_nodes` | `SCT_POST_BEHAVIOR_LOADER_NODES` | Direct |
| `post_behavior_monitor_nodes` | `SCT_POST_BEHAVIOR_MONITOR_NODES` | Direct |
| `post_behavior_k8s_cluster` | `SCT_POST_BEHAVIOR_K8S_CLUSTER` | Direct |
| `post_behavior_vector_store_nodes` | `SCT_POST_BEHAVIOR_VECTOR_STORE_NODES` | Direct |
| `use_preinstalled_scylla` | `SCT_USE_PREINSTALLED_SCYLLA` | Direct |
| `disable_raft` | `SCT_DISABLE_RAFT` | Direct |
| `internode_compression` | `SCT_INTERNODE_COMPRESSION` | Direct |
| `update_db_packages` | `SCT_UPDATE_DB_PACKAGES` | Direct |
| `ip_ssh_connections` | `SCT_IP_SSH_CONNECTIONS` | Direct |
| `pytest_addopts` | `PYTEST_ADDOPTS` | Non-SCT env var |
| `test_email_title` | `SCT_EMAIL_SUBJECT_POSTFIX` | Adapted name |
| `extra_environment_variables` | Parsed and exported directly | `'KEY=val'` strings |
| `requested_by_user` | `BUILD_USER_REQUESTED_BY` | Conditional: only if non-empty (line 41) |
| `perf_extra_jobs_to_compare` | `SCT_PERF_EXTRA_JOBS_TO_COMPARE` | Conditional: JSON-encoded, only if non-empty (line 207) |
| `email_recipients` | `SCT_EMAIL_RECIPIENTS` | Conditional: JSON-encoded, only if non-empty (line 211) |
| `stop_on_hw_perf_failure` | `SCT_STOP_ON_HW_PERF_FAILURE` | Conditional: only when value is `"true"` (line 215) |
| `base_versions` | N/A | Not exported by `runSctTest.groovy` — used internally by `rollingUpgradePipeline.groovy` for multi-version iteration |
| Non-config params (`test_name`, `timeout`, `sub_tests`) | N/A | Not mapped to env vars — used for pipeline orchestration only |

Parameters found in pipeline calls that don't appear in the mapping table should be **logged as warnings** so that new parameters are surfaced for assessment. Validation errors from `SCTConfiguration` will guide which unmapped parameters actually need mapping.

**Multi-region handling**:
- Count regions from `region` / `gce_datacenter` field (e.g., `'''["eu-west-1", "us-west-2", "us-east-1"]'''` → 3 regions)
- Generate matching number of placeholder AMI IDs: `SCT_AMI_ID_DB_SCYLLA="ami-1 ami-2 ami-3"` (only if pre-PR #13877)
- Generate matching region name list: `SCT_REGION_NAME="eu-west-1 us-west-2 us-east-1"`

**Definition of Done**:
- [ ] All observed pipeline parameter → env var mappings are implemented
- [ ] Multi-region configs produce correctly sized placeholder lists
- [ ] Unit tests cover every backend type and multi-region variants
- [ ] Placeholder image values are used only as a fallback (documented as temporary until PR #13877 Phase 2)

**Dependencies**: Phase 1 (parser).

---

### Phase 3: Parallel Validation Engine + CLI Command

**Objective**: Add `sct.py lint-pipelines` command that discovers all Jenkins pipeline files, parses them, builds environments, and validates configurations in parallel — printing only failures.

**Deliverables**:
- New module: `sdcm/utils/lint/__init__.py` (package init)
- New module: `sdcm/utils/lint/validator.py`
  - Function: `validate_pipeline(pipeline_path: Path, env: dict[str, str]) -> tuple[bool, str]`
    - Sets environment, instantiates `SCTConfiguration()`, calls `verify_configuration()` and `check_required_files()`
    - Returns `(is_error, error_message)` — empty message on success
    - Mirrors existing `_run_yaml_test()` logic but with pipeline-derived environment
- New CLI command in `sct.py`:
  ```python
  @cli.command("lint-pipelines", help="Validate configurations from Jenkins pipeline files")
  @click.option("--pipeline-dir", default="jenkins-pipelines", help="Root directory of pipeline files")
  @click.option("--pipeline-file", default=None, help="Validate a single pipeline file (ad-hoc mode)")
  @click.option("--workers", default=None, type=int, help="Number of parallel workers (default: CPU count)")
  @click.option("--include", "-i", default="", help="Regex filter to include specific pipeline files")
  @click.option("--exclude", "-e", default="", help="Regex filter to exclude specific pipeline files")
  def lint_pipelines(pipeline_dir, pipeline_file, workers, include, exclude):
  ```
  - **Single-file mode** (`--pipeline-file`): Validates one specific `.jenkinsfile` path. Skips discovery and runs validation directly. This enables ad-hoc debugging: `sct.py lint-pipelines --pipeline-file jenkins-pipelines/oss/longevity/longevity-50gb-36h-gce.jenkinsfile`
  - **Directory mode** (default): Discovers all `.jenkinsfile` files under `--pipeline-dir` and validates in parallel.
  - **Note**: `--include`/`--exclude` are for **ad-hoc debugging only** and will not be used in the CI Jenkinsfile invocation. The CI default (no flags) always validates all discovered pipeline files — this is the core design goal of eliminating filename-pattern-based filtering.
- Parallel execution using `ProcessPoolExecutor`:
  - Default worker count: `os.cpu_count()` (not hard-coded 5 as in current `lint-yamls`)
  - Submit all pipeline validations, collect futures
  - **Failure-only output**: print `FAIL: <pipeline_path>\n<error>` only for failures
  - Summary line at the end: `X/Y pipelines passed (Z skipped)`
  - Exit code: 0 if all pass, 1 if any fail

**Output format**:
```
FAIL: jenkins-pipelines/oss/longevity/longevity-50gb-36h-gce.jenkinsfile
  Configuration error: 'n_db_nodes' is required for backend 'gce'

FAIL: jenkins-pipelines/manager/ubuntu24-manager-install.jenkinsfile
  File not found: test-cases/manager/manager-installation-set-distro.yaml

---
990/995 pipelines passed (3 skipped, 2 failed)
```

**Definition of Done**:
- [ ] `sct.py lint-pipelines` discovers and validates all pipeline files
- [ ] Parallel execution with configurable worker count
- [ ] Only failures are printed (no output for passing pipelines)
- [ ] Summary line with pass/fail/skip counts
- [ ] Exit code 1 on any failure
- [ ] `--include` / `--exclude` regex filters work for targeted validation
- [ ] `--pipeline-file` validates a single pipeline file (ad-hoc mode)
- [ ] Unit tests for the CLI command integration

**Dependencies**: Phase 1 (parser), Phase 2 (env builder). Soft dependency on PR #13877 Phase 2 for speed — works without it but slower and requires placeholder image values.

---

### Phase 4: Replace Bash Script and Jenkinsfile Integration

**Objective**: Replace `utils/lint_test_cases.sh` with the new `sct.py lint-pipelines` command in the CI pipeline.

**Deliverables**:
- Update `Jenkinsfile` (line 176): Replace:
  ```groovy
  sh ''' ./docker/env/hydra.sh bash ./utils/lint_test_cases.sh '''
  ```
  with:
  ```groovy
  sh ''' ./docker/env/hydra.sh lint-pipelines '''
  ```
- Mark `utils/lint_test_cases.sh` as deprecated (add comment header, delete in follow-up after stable release cycle)
- Deprecate `sct.py lint-yamls`: With `sct.py lint-pipelines --pipeline-file <path>` supporting ad-hoc single-pipeline validation, `lint-yamls` is no longer needed. Add a deprecation warning to `lint-yamls` pointing users to `lint-pipelines --pipeline-file`, and remove it in a follow-up.

**Rollback strategy**: Keep `utils/lint_test_cases.sh` for one release cycle after `lint-pipelines` proves stable in CI (mark it deprecated with a comment, don't delete immediately). The Jenkinsfile change is a one-line revert if the new linter produces false positives at scale.

**Definition of Done**:
- [ ] Jenkinsfile updated to use `lint-pipelines`
- [ ] `utils/lint_test_cases.sh` marked deprecated (deleted in follow-up after one stable release cycle)
- [ ] CI pipeline passes with the new linting command
- [ ] `lint-yamls` emits deprecation warning directing to `lint-pipelines --pipeline-file`

**Dependencies**: Phase 3 (CLI command).

---

### Phase 5: Post-PR #13877 Cleanup (Remove Image Placeholders)

**Objective**: Once PR #13877 Phase 2 (lazy image resolution) is merged, remove all placeholder image logic from the environment builder.

**Deliverables**:
- Remove placeholder AMI/image generation from `build_env()`
- Remove `SCT_AMI_ID_DB_SCYLLA`, `SCT_GCE_IMAGE_DB`, `SCT_AZURE_IMAGE_DB` placeholder logic
- Remove `SCT_SCYLLA_REPO` placeholder logic
- Simplify `_run_yaml_test()` helper and `validate_pipeline()` — no more mock image env vars
- Expected speed improvement: significant reduction in per-pipeline validation time since `SCTConfiguration()` no longer triggers cloud API calls

**Definition of Done**:
- [ ] No placeholder image values anywhere in the lint code
- [ ] Validation relies purely on `SCTConfiguration()` without image resolution
- [ ] All ~995 pipelines still pass lint
- [ ] Measurable speed improvement documented

**Dependencies**: PR #13877 Phase 2 merged.

## Testing Requirements

### Unit Tests (all phases)

- **Phase 1**: Test parser against representative samples of each pipeline pattern:
  - Single YAML config, list config, JSON array config
  - All backends (aws, gce, azure, docker, k8s-eks, k8s-gke, k8s-local-kind-*, xcloud, baremetal)
  - Multi-region pipelines (2, 3, 5 regions)
  - Pipelines without `test_config` (returns `None`)
  - Edge cases: extra whitespace, comments, non-standard quoting
- **Phase 2**: Test environment builder:
  - Correct env var mapping for each backend
  - Multi-region placeholder generation
  - `extra_environment_variables` parsing
- **Phase 3**: Test CLI command:
  - Mock `ProcessPoolExecutor` to verify parallel dispatch
  - Verify failure-only output format
  - Verify exit codes
  - Verify include/exclude filtering

### Integration Tests

- Run `sct.py lint-pipelines` against the full `jenkins-pipelines/` directory (marked with `@pytest.mark.integration`)
- Verify no regressions compared to current `lint_test_cases.sh` coverage

### Manual Testing

- [ ] Run `sct.py lint-pipelines` locally and verify output format
- [ ] Compare pass/fail results with current `lint_test_cases.sh` to identify coverage gaps
- [ ] Verify CI pipeline passes with the new command

## Success Criteria

1. **Zero guessing**: Every validated pipeline uses the backend and config files extracted directly from the `.jenkinsfile` — no filename pattern matching.
2. **Full coverage**: All ~995 pipeline files are discovered and either validated or explicitly skipped (with skip reason).
3. **Fast execution**: Full lint completes in < 2 minutes on CI (after PR #13877 Phase 2). Before that, < 10 minutes with placeholder images.
4. **Clean output**: Developers see only failures — no noise from passing pipelines.
5. **Maintainability**: Adding a new Jenkins pipeline file requires zero changes to the linter — it is automatically discovered and validated.

## Risk Mitigation

### Risk: Groovy parsing edge cases
- **Mitigation**: Use [`python-groovy-parser`](https://github.com/inab/python-groovy-parser) for proper Groovy AST parsing instead of regex. This library implements the full Groovy 3.0.X grammar (derived from Apache Groovy's ANTLR grammar) and produces a parse tree, eliminating regex edge cases entirely. It supports caching parsed results for performance. If the library has issues with our specific Jenkinsfile patterns, a regex-based fallback targets the known consistent pattern across all ~995 files. Files that don't match (raw `pipeline {}` blocks) are skipped gracefully. A `--verbose` flag logs skipped files for debugging.

### Risk: `SCTConfiguration` side effects in parallel processes
- **Mitigation**: The existing `_run_yaml_test()` already runs in `ProcessPoolExecutor` workers and resets `os.environ` per process. The same isolation model is reused.

### Risk: False positives from placeholder image values
- **Mitigation**: Placeholder values are minimal and designed to pass type validation only. Phase 5 eliminates them entirely after PR #13877 Phase 2.

### Risk: CI time increase
- **Mitigation**: Current bash script runs 8 sequential `sct.py lint-yamls` invocations, each spawning its own process pool. The new approach runs a single process pool across all pipelines, which should be faster. Worker count defaults to CPU count for maximum throughput.

### Risk: Breaking existing `lint-yamls` users
- **Mitigation**: `lint-pipelines --pipeline-file <path>` provides ad-hoc single-pipeline validation, covering the main `lint-yamls` use case. `lint-yamls` is deprecated with a warning (not immediately removed) to give developers time to switch.

## Design Decision: Extract All Pipeline Parameters

All pipeline parameters must be extracted — not just a pre-selected core set. The parser captures **every** named parameter from the pipeline function call into `PipelineConfig.params`, and the environment builder iterates over all of them, exactly as `runSctTest.groovy` does with `params.*`.

The complete parameter → env var mapping table is in Phase 2. It was derived by reading every `params.*` reference in `vars/runSctTest.groovy` and the individual pipeline functions (`longevityPipeline.groovy`, `rollingUpgradePipeline.groovy`, `managerPipeline.groovy`, etc.).

**Implementation approach**:
1. First, collect all unique parameter names found across all ~995 pipeline files
2. Assess each one: does it map to an `SCT_*` env var? Is it used only for pipeline orchestration?
3. Build the mapping table from this assessment
4. Validation errors from `SCTConfiguration` confirm whether unmapped parameters are needed
5. Parameters not in the mapping table are logged as warnings so gaps are surfaced early

## Open Questions

1. **Mapping completeness**: The parameter mapping table in Phase 2 covers all parameters observed in `vars/runSctTest.groovy`. As new pipeline parameters are added, the mapping table should be extended. The linter warns on unmapped parameters to catch gaps early. Validation errors guide which unmapped parameters actually need mapping.
