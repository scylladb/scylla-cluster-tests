# Trigger Matrix Manual Testing Guide

Manual testing procedures for the `sct.py trigger-matrix` command and the YAML trigger matrix system.

## Matrix YAML Layout

A job entry has two kinds of keys, and putting one in the other's place is rejected at load time.

**Job-level keys** decide *whether and how* the job is triggered — they are consumed by the trigger itself:

`job_name`, `backend`, `arch`, `disabled`, `labels`, `include_versions`, `exclude_versions`, `pre_release`,
`job_throttle_category`, `params`, `wait`, `wait_timeout`, `fail_on_error`, `collect_results`

One exception: `job_throttle_category` is *also* forwarded to Jenkins as a build parameter of the same
name (via `setdefault`, so a `params:` entry of the same name wins). Every other job-level key stops at
the trigger.

**Everything else is a Jenkins job parameter and belongs under `params:`** — including `region` and
`availability_zone`, which used to sit at job level.

```yaml
jobs:
  - job_name: "tier1/longevity-multidc-schema-topology-changes-12h-test"
    backend: "aws"
    params:
      region: '["eu-west-1", "eu-west-2"]'   # multi-region: a JSON *string*, not a YAML list
      availability_zone: "a,b,c"
    labels:
      - "weekly"
```

### Migrating an existing matrix

`region` and `availability_zone` used to be job-level keys. Moving them under `params:` is a breaking
layout change — a matrix still using the old form fails to load. To migrate:

1. Move every job-level `region:` / `availability_zone:` line under that job's `params:`, creating the
   `params:` block if the job doesn't have one.
2. Drop empty ones (`region: ""`). They were a no-op before, and keeping them would start passing an
   empty region to Jenkins.
3. Quote multi-valued parameters as JSON *strings* — a YAML list is rejected.
4. Validate: `uv run python -c "from sdcm.utils.trigger_matrix import load_matrix_config; load_matrix_config('<matrix>.yaml')"`

```yaml
# before                                    # after
- job_name: "tier1/multidc-test"            - job_name: "tier1/multidc-test"
  backend: "aws"                              backend: "aws"
  region: '["eu-west-1", "eu-west-2"]'        params:
  params:                                       region: '["eu-west-1", "eu-west-2"]'
    availability_zone: "a,b,c"                  availability_zone: "a,b,c"
  labels:                                     labels:
    - "weekly"                                  - "weekly"
```

`load_matrix_config()` validates the layout and reports every problem at once, naming the offending job:

- unknown job-level key → `job 'tier1/foo' (jobs[3]): unknown job-level key 'region' — Jenkins job parameters belong under 'params:'`
- job-level key nested under `params:` → `'labels' is a job-level key and must not be nested under 'params:'`
- near-miss key names get a `did you mean 'labels'?` hint
- non-scalar values in `params`/`defaults`/`cron_triggers[].params` → every Jenkins parameter is a string,
  so multi-valued ones (multi-region, `sub_tests`) must be quoted JSON strings

Check a file after editing it:

```bash
uv run python -c "from sdcm.utils.trigger_matrix import load_matrix_config; load_matrix_config('configurations/triggers/tier1.yaml')"
```

## Prerequisites

- SCT environment set up (`uv sync`)
- Access to the `configurations/triggers/` YAML files

For non-dry-run testing, you also need:
- `JENKINS_URL` environment variable set
- `JENKINS_API_TOKEN` environment variable set

## Dry-Run Testing

Dry-run mode (`--dry-run`) validates the full pipeline — YAML loading, version parsing, job filtering, parameter building — without making any Jenkins API calls.

### Basic Dry-Run

```bash
# Trigger tier1 jobs for version 2025.4
uv run sct.py trigger-matrix \
    --matrix configurations/triggers/tier1.yaml \
    --scylla-version "2025.4" \
    --dry-run
```

**Expected**: All tier1 jobs listed with `[DRY-RUN]` prefix. No jobs excluded (version `2025.4` doesn't match any `exclude_versions` entries).

### Version Exclusion

```bash
# Trigger perf-regression jobs for master — jobs with exclude_versions=["master"] should be excluded
uv run sct.py trigger-matrix \
    --matrix configurations/triggers/perf-regression.yaml \
    --scylla-version "master:latest" \
    --dry-run
```

**Expected**: Jobs with `exclude_versions: ["master"]` appear in the "Skipped" list.

Note: Version exclusion uses **prefix matching**. `exclude_versions: ["2024.1"]` excludes `2024.1`, `2024.1.5`, `2024.1-rc1`, etc.

### Label Filtering

```bash
# Only trigger weekly-labeled jobs from tier1
uv run sct.py trigger-matrix \
    --matrix configurations/triggers/tier1.yaml \
    --scylla-version "2025.4" \
    --labels-selector "weekly" \
    --dry-run
```

**Expected**: Only jobs with `labels: ["weekly", ...]` are triggered. Jobs without the `weekly` label appear in the "Skipped" list.

Labels use AND logic: `--labels-selector "weekly,additional"` requires jobs to have **both** labels.

### Backend Filtering

```bash
# Only trigger AWS jobs from tier1
uv run sct.py trigger-matrix \
    --matrix configurations/triggers/tier1.yaml \
    --scylla-version "2025.4" \
    --backend aws \
    --dry-run
```

**Expected**: Only jobs with `backend: aws` are triggered.

### Skip-Jobs Filtering

```bash
# Skip specific jobs by name
uv run sct.py trigger-matrix \
    --matrix configurations/triggers/tier1.yaml \
    --scylla-version "2025.4" \
    --skip-jobs "tier1/longevity-50gb-3days-test,tier1/longevity-1tb-5days-azure-test" \
    --dry-run
```

**Expected**: The named jobs appear in the "Skipped" list.

### Parameter Overrides

```bash
# Override stress_duration and provision_type
uv run sct.py trigger-matrix \
    --matrix configurations/triggers/tier1.yaml \
    --scylla-version "2025.4" \
    --stress-duration "120" \
    --provision-type on_demand \
    --region "us-west-2" \
    --dry-run
```

**Expected**: Each `[DRY-RUN]` log line shows the overridden parameters in the parameter output.

Note: **`--region` and `--availability-zone` do not override jobs that pin their own value.** They are location parameters — a job that declares `region:` under its `params:` owns it, and the CLI value only fills in for jobs that don't. Everything else (`--provision-type`, `--stress-duration`, `--new-scylla-repo`, ...) overrides per-job values as usual.

Precedence, highest first:

| Parameter | Precedence |
|-----------|------------|
| `region`, `availability_zone` | job `params` → CLI flag → matrix `defaults` |
| everything else | CLI flag → job `params` → matrix `defaults` |

This is what keeps a multi-DC job configured with `region: '["eu-west-1", "eu-west-2"]'` from collapsing into a single region when a trigger run passes `--region us-east-1` (SCT-693).

### Job Folder Resolution

```bash
# Explicit job folder override
uv run sct.py trigger-matrix \
    --matrix configurations/triggers/tier1.yaml \
    --scylla-version "2025.4" \
    --job-folder "my-custom-folder" \
    --dry-run
```

**Expected**: All relative job paths prefixed with `my-custom-folder/` instead of `branch-2025.4/`.

### Absolute Job Paths

Jobs starting with `/` bypass the job folder entirely:

```yaml
# In YAML: job_name: "/scylla-enterprise/perf-regression/perf-test"
# Resolves to: scylla-enterprise/perf-regression/perf-test (no folder prefix)
```

### Version Format Variants

Test different version formats to verify folder resolution:

```bash
# Branch:qualifier format
uv run sct.py trigger-matrix --matrix configurations/triggers/sanity.yaml --scylla-version "master:latest" --dry-run
# Expected folder: scylla-master

# Simple version
uv run sct.py trigger-matrix --matrix configurations/triggers/sanity.yaml --scylla-version "2025.4" --dry-run
# Expected folder: branch-2025.4

# Full version tag
uv run sct.py trigger-matrix --matrix configurations/triggers/sanity.yaml --scylla-version "2024.2.5-0.20250221.cb9e2a54ae6d-1" --dry-run
# Expected folder: branch-2024.2

# Bare "master"
uv run sct.py trigger-matrix --matrix configurations/triggers/sanity.yaml --scylla-version "master" --dry-run
# Expected folder: scylla-master
```

## Error Handling Verification

### Invalid Matrix File

```bash
echo "not-valid-yaml: [" > /tmp/bad-matrix.yaml
uv run sct.py trigger-matrix --matrix /tmp/bad-matrix.yaml --scylla-version "2025.4" --dry-run
```

**Expected**: User-friendly error message (not a Python traceback).

### Invalid Version Format

```bash
uv run sct.py trigger-matrix \
    --matrix configurations/triggers/tier1.yaml \
    --scylla-version "not-a-version" \
    --dry-run
```

**Expected**: Error message: "Cannot determine job folder from version 'not-a-version'".

### Zero Jobs After Filtering

```bash
uv run sct.py trigger-matrix \
    --matrix configurations/triggers/tier1.yaml \
    --scylla-version "2025.4" \
    --labels-selector "nonexistent-label" \
    --dry-run
```

**Expected**: Warning that no jobs matched filters. Output shows 0 triggered, all jobs in skipped list.

## Validating Real Trigger YAML Files

Verify all production YAML files load without errors:

```bash
for yaml_file in configurations/triggers/*.yaml; do
    echo "--- Testing $yaml_file ---"
    uv run sct.py trigger-matrix --matrix "$yaml_file" --scylla-version "2025.4" --dry-run 2>&1 | tail -5
done
```

**Expected**: Each file loads successfully and reports triggered/skipped counts.

## Version Resolution

The trigger matrix always passes a **full version tag** (e.g., `2026.2.0~dev-0.20260322.f51126483167`) to all triggered jobs via the `scylla_version` parameter. Image parameters (`scylla_ami_id`, `gce_image_db`, etc.) are **not** passed to downstream jobs — they are only used to resolve the version.

### Resolution Flow

1. **Full version tag provided** (e.g., `2024.2.5-0.20250221.cb9e2a54ae6d-1`): used as-is
2. **Partial version** (e.g., `master:latest`, `2025.4`): resolved to full tag via AMI lookup (`get_branched_ami`)
3. **Image param only** (e.g., `--scylla-ami-id ami-xxx`): version extracted from image tags, then used for all jobs

### Triggering from an Image (scylla-pkg flow)

When scylla-pkg builds an AMI and wants to trigger tests:

```bash
# Image is used ONLY to extract the version — not passed to triggered jobs
uv run sct.py trigger-matrix \
    --matrix configurations/triggers/tier1.yaml \
    --scylla-ami-id ami-00e3e5063b0ca4dd0 \
    --job-folder test \
    --dry-run
```

**What happens**:
1. Reads AMI tags (using `get_ami_tags` which checks both Scylla images account and default credentials)
2. Extracts `scylla_version` or `ScyllaVersion` tag → e.g., `2026.2.0~dev-0.20260322.f51126483167`
3. Uses that version for job folder detection and triggers ALL matrix jobs with `scylla_version=<full-tag>`
4. Downstream jobs resolve their own backend-specific images from the version

### Supported Image Parameters

| Parameter | Backend | Tag lookup method |
|-----------|---------|-------------------|
| `--scylla-ami-id` | AWS | `get_ami_tags()` — checks both Scylla images account and default credentials |
| `--gce-image-db` | GCE | `get_gce_image_tags()` — handles URL and family-based references |
| `--azure-image-db` | Azure | `azure_utils.get_image_tags()` |
| `--oci-image-db` | OCI | `oci_utils.get_image_tags()` |

### Version Tag Names

Different backends use different tag key names:
- **AWS**: `scylla_version` or `ScyllaVersion`
- **GCE**: `scylla_version` label (with dashes instead of dots)
- **Azure**: `scylla_version` tag
- **OCI**: `scylla_version` freeform tag

### Triggering with Partial Version

```bash
# master:latest → resolves to full version via AMI lookup
uv run sct.py trigger-matrix \
    --matrix configurations/triggers/tier1.yaml \
    --scylla-version "master:latest" \
    --dry-run

# 2025.4 → resolves to full version via AMI lookup (tries as 2025.4:latest)
uv run sct.py trigger-matrix \
    --matrix configurations/triggers/tier1.yaml \
    --scylla-version "2025.4" \
    --dry-run
```

## Unit Tests

```bash
# Run all trigger matrix unit tests
uv run python -m pytest unit_tests/test_trigger_matrix.py -v -n0

# Run with coverage
uv run python -m pytest unit_tests/test_trigger_matrix.py --cov=sdcm.utils.trigger_matrix --cov-report=term-missing -n0
```
