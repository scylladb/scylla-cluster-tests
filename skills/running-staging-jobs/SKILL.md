---
name: running-staging-jobs
description: >-
  Guides running staging Jenkins jobs for SCT pull requests, tracking build results,
  diagnosing failures from console logs, and updating PR descriptions with run status.
  Use when triggering staging tests for a PR, monitoring Jenkins build progress,
  analyzing build failures from console output, retrying failed builds with corrected
  parameters, or updating a PR with staging run results. Covers staging_trigger.py CLI,
  jenkins CLI for build-info and console-log, unified_package URL construction, and
  PR description formatting with emoji status indicators.
---

# Running Staging Jobs

Trigger, track, diagnose, and report staging Jenkins job results for SCT pull requests.

## Essential Principles

### Jobs Must Be Generated Before Triggering

Staging jobs are per-PR Jenkins copies. They don't exist until `staging_trigger.py generate --pr <N>` creates them. The generate step copies pipeline definitions and configures SCM to point at the PR branch. Without this, trigger commands will 404.

### Use Short Job Paths with staging_trigger.py

The `staging_trigger.py` CLI internally prepends `scylla-staging/<user>/` to job paths. Always pass the **short path** (e.g., `oss/artifacts/artifacts-ubuntu2604-arm-test`), never the full Jenkins path. Using full paths causes double-prefixing and 404 errors.

### Track Builds with jenkins CLI, Not the Trigger Output

The trigger script outputs a build number but doesn't wait for completion. Use `/home/fruch/.local/bin/jenkins build-info --job <full-path> --build <N>` to poll status and `console-log` to download failure logs. The full path IS required for the jenkins CLI (e.g., `scylla-staging/fruch/oss/artifacts/...`).

### Unified Package URLs Are Ephemeral

S3 paths under `unstable/scylla/master/relocatable/latest/` change with every nightly build. Always verify the URL exists before triggering offline jobs. The build hash in the filename changes daily. Use `aws s3 ls` to get the current filename.

### PR Description Is the Single Source of Truth

All staging run results must be documented in the PR body with emoji status (✅ ❌ 🕐) and direct Jenkins links. Previous failures should be listed with root cause to document the debugging journey.

## When to Use

- Triggering staging Jenkins jobs for a new or updated PR
- Monitoring in-progress Jenkins builds for pass/fail
- Diagnosing build failures from Jenkins console logs
- Retrying failed builds with corrected parameters or after code fixes
- Updating PR descriptions with staging run results
- Constructing unified_package URLs for offline artifact tests

## When NOT to Use

- Creating Jenkins pipeline files (use writing-plans or direct editing)
- Modifying test-case YAML configurations (edit directly)
- Running local tests with `uv run sct.py` (not staging)
- Creating the PR itself (use git/gh commands directly)

## Workflow

The full process is in [run-and-track.md](workflows/run-and-track.md).

## Quick Reference

### staging_trigger.py Commands

```bash
# Generate staging jobs for a PR (required first step)
python3 ./staging_trigger.py generate --pr <PR_NUMBER>

# Trigger with scylla_version (online tests)
python3 ./staging_trigger.py trigger --pr <N> --set scylla_version=master:latest <short-job-path>

# Trigger with unified_package (offline tests)
python3 ./staging_trigger.py trigger --pr <N> --set unified_package=<S3_URL> <short-job-path>
```

### jenkins CLI Commands

```bash
# Check build status (result/building/duration)
/home/fruch/.local/bin/jenkins build-info --job <full-job-path> --build <N>

# Download console log for failure analysis
/home/fruch/.local/bin/jenkins console-log --job <full-job-path> --build <N> --dest /tmp/opencode
```

### S3 Unified Package Lookup

```bash
# x86_64
aws s3 ls s3://downloads.scylladb.com/unstable/scylla/master/relocatable/latest/ | grep "x86_64.tar.gz$" | grep unified

# aarch64
aws s3 ls s3://downloads.scylladb.com/unstable/scylla/master/relocatable/latest/ | grep "aarch64.tar.gz$" | grep unified
```

URL format: `https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/master/relocatable/latest/scylla-unified-<version>.tar.gz`

### PR Description Format

```markdown
## Staging Runs ✅

### Online (scylla_version=master:latest)
- ✅ [x86 online #N](https://jenkins.scylladb.com/job/scylla-staging/job/<user>/job/...)
- ❌ [ARM online #N](https://jenkins.scylladb.com/job/...) — <failure reason>

### Offline (unified_package)
- ✅ [x86 offline #N](https://jenkins.scylladb.com/job/...)
- 🕐 [ARM offline #N](https://jenkins.scylladb.com/job/...) — running

### Previous Failures (resolved)
- ❌ ARM #4: `NodeSetupFailed: Failed to find disks!` — fixed by adding pd-standard data disk
```

### Common Failure Patterns

| Error | Root Cause | Fix |
|-------|-----------|-----|
| `NodeSetupFailed: Failed to find disks!` | No data disk attached (local SSD or PD) | Add `gce_pd_standard_disk_size_db` or fix provisioner |
| `curl: (22) The requested URL returned error: 404` | Stale unified_package URL | Re-lookup current S3 filename |
| No `scylla_version` provided | Missing trigger parameter | Add `--set scylla_version=master:latest` |
| Job 404 on trigger | Full path used instead of short path | Use short path without `scylla-staging/<user>/` prefix |

## Success Criteria

- [ ] All staging jobs generated for the PR
- [ ] All jobs triggered with correct parameters
- [ ] All builds reach terminal state (SUCCESS/FAILURE)
- [ ] Failures diagnosed with root cause from console logs
- [ ] Fixes applied, committed, pushed, and retried
- [ ] PR description updated with final ✅/❌ status and links
- [ ] Previous failures documented with root cause explanation
