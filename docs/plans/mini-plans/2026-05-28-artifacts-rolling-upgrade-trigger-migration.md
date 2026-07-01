# Artifacts & Rolling-Upgrade Trigger Migration to SCT (Phase 2)

## Context

Phase 1 (PR #14145) migrates tier1/perf-regression/PGO/sanity triggers to SCT-owned YAML matrices.
This plan covers Phase 2: migrating artifacts tests and rolling-upgrade tests.

## Problem

Currently, `scylla-pkg` owns both the list of SCT jobs to trigger AND the triggering logic for artifacts tests and rolling-upgrade tests. This means:
- Adding/removing a test requires a PR to scylla-pkg
- scylla-pkg pipelines have complex per-artifact Groovy loops iterating `triggers_file.yaml`
- Job parameters differ per artifact type (AMI ID vs repo URL vs docker tag) but the pattern is repetitive
- No single source of truth on the SCT side for what runs when

## Approach

### Phase 1: New SCT trigger YAML files

Create `configurations/triggers/` YAMLs for the artifact/rolling-upgrade groups:
- `artifacts-ami.yaml` — AMI-based artifact tests (x86 + arm)
- `artifacts-gce.yaml` — GCE image artifact tests
- `artifacts-azure.yaml` — Azure image artifact tests
- `artifacts-oci.yaml` — OCI image artifact tests
- `artifacts-rpm.yaml` — RPM repo-based artifact tests (x86 + arm)
- `artifacts-deb.yaml` — DEB repo-based artifact tests (x86 + arm)
- `artifacts-docker.yaml` — Docker image artifact tests
- `artifacts-offline-installer.yaml` — Offline installer artifact tests
- `rolling-upgrade.yaml` — All rolling-upgrade tests (triggered by deb/rpm)

Each YAML declares:
- The jobs to run
- Which branch context triggers them (`master` vs `release` via `include_versions` or new `branch_filter` field). `branch_filter` is a planned YAML field that restricts a job to specific Jenkins pipeline branch contexts (e.g. `master` or `release`), allowing a single YAML to behave differently when triggered from different branches.
- Tier (tier1 wait=true vs tier2 fire-and-forget)
- Architecture filtering

### Phase 2: SCT trigger-matrix pipeline changes

Extend `triggerMatrixPipeline.groovy` and `trigger_matrix.py` to accept artifact-type parameters:
- `--scylla-ami-id` / `--gce-image-db` / `--scylla-repo` / `--scylla-docker-image` etc.
- `--branch-context master|release` (replaces `triggerBranch` logic in scylla-pkg)
- `--architecture x86|aarch64`
- Pass the artifact identifier through to triggered jobs as the appropriate parameter

### Phase 3: scylla-pkg PR

Simplify each scylla-pkg pipeline to call a single SCT trigger-matrix pipeline:
```groovy
build job: "scylla-master/sct_triggers/artifacts-ami-trigger",
    parameters: [
        string(name: 'scylla_ami_id', value: amiImageIdX86),
        string(name: 'architecture', value: 'x86'),
        string(name: 'branch_context', value: triggerBranch),
    ],
    wait: true  // for tier1
```

Remove `triggers_file.yaml` entries for migrated groups (keep reloc/driver-tests which aren't SCT).

## Key Parameters by Artifact Type

| Artifact | SCT Param | Source Pipeline |
|----------|-----------|----------------|
| AMI | `scylla_ami_id` + `region` | ami.jenkinsfile |
| GCE | `gce_image_db` | gce-image.jenkinsfile |
| Azure | `azure_image_db` | azure-image.jenkinsfile |
| OCI | `oci_image_db` | oci-image.jenkinsfile |
| RPM | `scylla_repo` (.repo URL) | centos-rpm.jenkinsfile |
| DEB | `scylla_repo` (.list URL) | unified-deb.jenkinsfile |
| Docker | `scylla_docker_image` + `scylla_version` | docker.jenkinsfile |
| Offline | installer URL | offline-installer.jenkinsfile |

## Verification

- Dry-run each new trigger YAML against staging
- Compare triggered job list with current `triggers_file.yaml` for parity
- Run one real trigger per artifact type on a release branch to verify parameters pass correctly
