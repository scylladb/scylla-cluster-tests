# Scylla Doctor Release Gating Suite — Implementation Plan

**Date:** 2026-04-06
**Updated:** 2026-04-09
**Status:** Draft
**Scope:** Integrate the full SD test suite into existing artifact tests for validating Scylla Doctor releases

---

## Background

After a detailed chat with Israel, here's the plan to run the full SD test suites. The approach integrates SD validation into the existing artifact test infrastructure to keep things efficient and simple, rather than creating a separate standalone suite.

---

## Plan Summary

### 1. Integration with Artifact Tests

The full SD suite will be added into the **current artifact tests** (`artifacts_test.py`). No separate test file or test configs are needed. The existing `run_scylla_doctor()` method and `utils/scylla_doctor.py` utility class already provide the core SD validation flow (install → collect vitals → analyze → verify). The changes extend this to support new modes and parameters.

### 2. SD Installation

SD will be installed using a **tarball from S3**. The SD tarball must be stored in an S3 bucket that the QA user can access, so the installation goes smoothly. The existing `download_scylla_doctor()` method in `utils/scylla_doctor.py` already handles S3-based tarball downloads from `downloads.scylladb.com/downloads/scylla-doctor/tar/`.

---

## Required Actions

### Action 1: Merge PR #14051 — Full Edition Download from Private S3 Bucket

**PR:** [feature(scylla-doctor): support full edition download from private S3 bucket](https://github.com/scylladb/scylla-cluster-tests/pull/14051)

This PR correctly identifies the SD release version and adds support for downloading the full SD edition (Collectors + Analyzers) instead of just the Collectors-only tarball. This is a prerequisite for the remaining actions.

**Status:** Awaiting merge.

### Action 2: New Parameter for Custom S3 Tarball URL

Add a new SCT configuration parameter that provides a **direct link to an S3 tarball** for SD versions that are not official releases. This enables testing pre-release or unofficial SD builds.

#### 2.1 New Config Parameter

| Parameter | Type | Description |
|-----------|------|-------------|
| `scylla_doctor_tarball_url` | str | Direct URL to a Scylla Doctor tarball in S3. When set, bypasses the standard version-based S3 lookup and downloads SD directly from this URL. Use for testing unofficial or pre-release SD versions. |

Environment variable: `SCT_SCYLLA_DOCTOR_TARBALL_URL`

#### 2.2 Implementation in `sdcm/sct_config.py`

Add the new parameter alongside the existing SD config options (`run_scylla_doctor`, `scylla_doctor_version`):

```python
scylla_doctor_tarball_url: String = SctField(
    description="""Direct URL to a Scylla Doctor tarball in S3. When set, bypasses the
            standard version-based S3 lookup and downloads SD directly from this URL.
            Use for testing unofficial or pre-release SD versions.
            Example: 'https://s3.amazonaws.com/my-bucket/scylla-doctor-1.11-rc1.tar.gz'""",
)
```

#### 2.3 Implementation in `utils/scylla_doctor.py`

Update `download_scylla_doctor()` to check for the custom tarball URL before falling back to the existing version-based S3 lookup:

```python
def download_scylla_doctor(self):
    # ... existing curl check ...

    tarball_url = self.test_config.tester_obj().params.get("scylla_doctor_tarball_url")
    if tarball_url:
        LOGGER.info("Using custom SD tarball URL: %s", tarball_url)
        self.node.remoter.run(f"curl -JL {tarball_url} | tar -xvz")
        self.scylla_doctor_exec = f"{self.current_dir}/{self.SCYLLA_DOCTOR_OFFLINE_BIN}"
        return

    # ... existing version-based S3 lookup logic (configured_version → latest) ...
```

**Priority order when resolving the SD artifact:**
1. `scylla_doctor_tarball_url` — direct URL (for pre-release / unofficial testing)
2. `scylla_doctor_version` — specific version looked up in S3 (for pinned releases)
3. Latest available in S3 (fallback when neither is set)

### Action 3: Auto-Discover Supported Scylla Versions

Pick the **latest minor (patch) version** from each currently supported official Scylla release branch and run SD against all of them. This ensures SD is validated across the full range of Scylla versions that customers may be running.

#### 3.1 Version Discovery

SCT already has infrastructure for discovering supported Scylla versions:

- `get_s3_scylla_repos_mapping()` in `sdcm/utils/version_utils.py` — enumerates all published release branches from S3 repo files
- `UpgradeBaseVersion.get_version_list()` in `utils/get_supported_scylla_base_versions.py` — returns all supported release version prefixes (e.g., `2025.4`, `2026.1`)
- `get_all_versions()` in `sdcm/utils/version_utils.py` — can resolve all patch versions for a given branch repo

The gating pipeline will use this to automatically determine which Scylla versions to test, selecting the latest patch of each supported branch.

#### 3.2 Scylla Version Matrix

| Branch | Type | Example Latest Patch |
|--------|------|----------------------|
| 2025.4.x | LTS | 2025.4.3 |
| 2026.1.x | Latest | 2026.1.1 |

*Versions are auto-discovered at pipeline runtime — no manual maintenance required.*

#### 3.3 Implementation

The Jenkins pipeline will call a helper script (or Groovy function in `vars/`) that:
1. Calls `get_s3_scylla_repos_mapping()` to enumerate all supported release branches
2. For each branch, resolves the latest available patch version
3. Feeds each version as a parameter (`SCT_SCYLLA_VERSION`) into the parallel artifact test runs

This means a single SD release gating trigger produces tests across **all supported Scylla versions × all OS variants**.

### Action 4: SD-Only Test Mode Flag

Add a flag that runs **SD validation only**, skipping the full artifact test flow (scylla stop/start, cassandra-stress, housekeeping checks, perftune, etc.). This provides a fast path for SD release gating.

#### 4.1 New Config Parameter

| Parameter | Type | Description |
|-----------|------|-------------|
| `run_scylla_doctor_only` | bool | When true, the artifact test runs only the Scylla Doctor validation subtests (install, collect, analyze, verify) and skips all other artifact checks. Default: false. |

Environment variable: `SCT_RUN_SCYLLA_DOCTOR_ONLY`

#### 4.2 Implementation in `sdcm/sct_config.py`

```python
run_scylla_doctor_only: Boolean = SctField(
    description="""When true, the artifact test runs only the Scylla Doctor validation
            (install, collect vitals, analyze, verify) and skips all other artifact checks
            such as stop/start, cassandra-stress, housekeeping, etc. Useful for fast SD
            release gating. Implies run_scylla_doctor=true.""",
)
```

#### 4.3 Default in `defaults/test_default.yaml`

```yaml
run_scylla_doctor_only: false
```

#### 4.4 Implementation in `artifacts_test.py`

Modify `test_scylla_service()` to support the SD-only mode. When `run_scylla_doctor_only` is set, the test performs minimal Scylla validation (verify it's running via `nodetool status`) and then runs only the SD subtests:

```python
def test_scylla_service(self):
    self.run_pre_create_schema()
    backend = self.params.get("cluster_backend")

    if self.params.get("run_scylla_doctor_only"):
        # SD-only mode: minimal Scylla validation, then full SD test
        with self.logged_subtest("verify node health"):
            self.verify_node_health()
        with self.logged_subtest("check scylla_doctor results"):
            self.run_scylla_doctor()
        return

    # ... existing full artifact test flow unchanged ...
```

### Action 5: Trigger to Run All Artifact Tests in Parallel

Create a Jenkins pipeline that triggers **all artifact tests simultaneously**, waits for all of them to finish, and produces **detailed failure reports** so issues can be fixed quickly.

#### 5.1 New Jenkins Pipeline

**File:** `jenkins-pipelines/scylla-doctor/sd-release-gating.jenkinsfile`

This pipeline:
1. Accepts `SD_VERSION` (or `SD_TARBALL_URL`) as a parameter
2. Auto-discovers the latest patch version for each supported Scylla release branch (Action 3)
3. Triggers all existing artifact test jobs in parallel — one per OS/backend, each parameterized with the SD version under test
4. Passes SD parameters to each job via `SCT_SCYLLA_DOCTOR_VERSION` (or `SCT_SCYLLA_DOCTOR_TARBALL_URL`)
5. Sets `SCT_RUN_SCYLLA_DOCTOR_ONLY=true` for SD-focused gating (fast path)
6. Waits for all jobs to complete
7. Aggregates results and produces a summary report with per-OS, per-Scylla-version pass/fail status

#### 5.2 Pipeline Design

```groovy
// sd-release-gating.jenkinsfile
pipeline {
    parameters {
        string(name: 'SD_VERSION', defaultValue: '',
               description: 'SD version to test (e.g., 1.11)')
        string(name: 'SD_TARBALL_URL', defaultValue: '',
               description: 'Direct URL to SD tarball (overrides SD_VERSION)')
        booleanParam(name: 'SD_ONLY_MODE', defaultValue: true,
                     description: 'Run only SD tests, skip full artifact flow')
    }

    stages {
        stage('Discover Scylla Versions') {
            steps {
                // Use get_s3_scylla_repos_mapping() / UpgradeBaseVersion
                // to find latest patch of each supported branch
                // Result: [2025.4.3, 2026.1.1, ...]
            }
        }
        stage('Run Artifact Tests') {
            // For each Scylla version × OS combination, trigger in parallel
            parallel {
                stage('CentOS 9')     { steps { triggerArtifactTest('centos9') } }
                stage('Rocky 9')      { steps { triggerArtifactTest('rocky9') } }
                stage('Ubuntu 22.04') { steps { triggerArtifactTest('ubuntu2204') } }
                stage('Ubuntu 24.04') { steps { triggerArtifactTest('ubuntu2404') } }
                stage('Debian 12')    { steps { triggerArtifactTest('debian12') } }
                stage('AMI')          { steps { triggerArtifactTest('ami') } }
                stage('Docker')       { steps { triggerArtifactTest('docker') } }
                // ... additional OS variants as needed ...
            }
        }
        stage('Report') {
            steps {
                // Aggregate results from all parallel jobs
                // Produce summary with per-OS, per-Scylla-version pass/fail
                // Notify SD team on failures
            }
        }
    }
}
```

Each `triggerArtifactTest(os)` call builds the corresponding existing artifact pipeline with additional environment variables:
- `SCT_SCYLLA_DOCTOR_VERSION` or `SCT_SCYLLA_DOCTOR_TARBALL_URL`
- `SCT_SCYLLA_VERSION` (the specific Scylla release to install)
- `SCT_RUN_SCYLLA_DOCTOR_ONLY=true` (when `SD_ONLY_MODE` is enabled)

#### 5.3 Failure Reporting

The pipeline collects results from all parallel jobs and produces:
- A **summary table** showing pass/fail per OS variant × Scylla version
- **Links to individual job logs** for failed runs
- The specific SD version and tarball URL used
- **Notification** to the SD team (email or Slack) on any failure

Example summary output:

```
SD Version: 1.11 | Status: FAILED

                    2025.4.3   2026.1.1
CentOS 9            ✅          ✅
Rocky 9             ✅          ✅
Ubuntu 22.04        ✅          ❌
Ubuntu 24.04        ✅          ✅
Debian 12           ✅          ✅
AMI                 ✅          ✅
Docker              ✅          ✅

Failed: Ubuntu 22.04 × 2026.1.1 — FirewallRulesCollector (new failure)
  → [Job Link]
```

---

## Files to Create or Modify

| File | Action | Description |
|------|--------|-------------|
| `sdcm/sct_config.py` | Modify | Add `scylla_doctor_tarball_url` and `run_scylla_doctor_only` parameters |
| `defaults/test_default.yaml` | Modify | Add default values for new parameters |
| `utils/scylla_doctor.py` | Modify | Support custom tarball URL in `download_scylla_doctor()` |
| `artifacts_test.py` | Modify | Add SD-only mode early-return path in `test_scylla_service()` |
| `jenkins-pipelines/scylla-doctor/sd-release-gating.jenkinsfile` | Create | Parallel trigger pipeline with version discovery and failure reporting |

---

## Test Matrix

The SD gating pipeline combines:
- **Rows:** Auto-discovered latest patch of each supported Scylla release branch (e.g., 2025.4.x, 2026.1.x)
- **Columns:** Existing artifact test OS configs from `test-cases/artifacts/`

OS coverage from existing artifact tests:

| OS | Config File | Backend |
|----|-------------|---------|
| CentOS 9 | `test-cases/artifacts/centos9.yaml` | GCE |
| Rocky 9 | `test-cases/artifacts/rocky9.yaml` | GCE |
| Ubuntu 22.04 | `test-cases/artifacts/ubuntu2204.yaml` | GCE |
| Ubuntu 24.04 | `test-cases/artifacts/ubuntu2404.yaml` | GCE |
| Debian 12 | `test-cases/artifacts/debian12.yaml` | GCE |
| AMI (Amazon Linux) | via AWS artifact pipelines | AWS |
| Docker | `test-cases/artifacts/docker.yaml` | Docker |
| RHEL 8 | `test-cases/artifacts/rhel8.yaml` | GCE |
| RHEL 10 | `test-cases/artifacts/rhel10.yaml` | GCE |
| OEL 9 | `test-cases/artifacts/oel9.yaml` | GCE |
| + ARM variants | via `-arm` pipelines | AWS |

**Total combinations per SD release:** ~2 Scylla versions × ~10 OS variants = **~20 test runs** (all executed in parallel).

---

## Pass/Fail Criteria

The SD release is **approved** if all artifact tests pass the SD subtests:

1. SD installs successfully from S3 tarball on all OS variants
2. All collectors pass (excluding documented known exceptions in `filter_out_failed_collectors()`)
3. Analyzer runs without errors
4. Vitals JSON and scylla_logs archive are created (non-Docker)
5. No version-specific failures across any supported Scylla release

A failure in **any** cell of the matrix blocks the SD release.

---

## Estimated Effort

| Component | Effort |
|-----------|--------|
| Merge PR #14051 (prerequisite) | External dependency |
| `scylla_doctor_tarball_url` parameter + `scylla_doctor.py` changes | 1 day |
| `run_scylla_doctor_only` flag + `artifacts_test.py` changes | 1 day |
| Scylla version auto-discovery in pipeline | 1-2 days |
| `sd-release-gating.jenkinsfile` (parallel trigger + reporting) | 2-3 days |
| End-to-end testing & validation | 1-2 days |
| **Total** | **~6-9 days** |

---

## Open Questions

| # | Question | Impact | Decision Needed By |
|---|----------|--------|--------------------|
| 1 | Should the approved SD version auto-update `scylla_doctor_version` in `defaults/test_default.yaml` via automated PR? | CI/CD integration | During implementation |
| 2 | Where should the gating pass/fail result be published (Argus, email, Slack)? | Notification setup | During implementation |
| 3 | Should the S3 bucket for pre-release SD tarballs require specific IAM permissions beyond QA user access? | Access setup | Before implementation |
