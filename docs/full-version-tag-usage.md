# Full Version Tag Support in SCT

## Overview

SCT now supports **full version tags** for the `scylla_version` parameter across all backends (AWS, GCE, Azure, and Docker). This enables precise targeting of specific Scylla builds including development, nightly, and custom builds.

## Supported Version Formats

SCT supports three version formats for the `scylla_version` parameter:

### 1. Simple Versions (Existing)
Regular release versions:
```bash
export SCT_SCYLLA_VERSION=5.2.1
export SCT_SCYLLA_VERSION=2024.2.0
export SCT_SCYLLA_VERSION=2025.1.3
```

### 2. Branch Versions (Existing)
Branch-based nightly builds:
```bash
export SCT_SCYLLA_VERSION=master:latest
export SCT_SCYLLA_VERSION=branch-2019.1:all
```

### 3. Full Version Tags (NEW)
Precise build identification with commit hash:

**Format**: `<base_version>-<build>.<date>.<commit_id>[-suffix]`

**Examples**:
```bash
# Standard full version tag
export SCT_SCYLLA_VERSION=2024.2.5-0.20250221.cb9e2a54ae6d-1

# Development build with tilde separator
export SCT_SCYLLA_VERSION=2026.1.0~dev-0.20260119.4cde34f6f20b

# Release candidate
export SCT_SCYLLA_VERSION=5.2.0-rc1.20220829.67c91e8bcd61
```

## Full Version Tag Components

A full version tag consists of:

| Component | Description | Example |
|-----------|-------------|---------|
| **Base Version** | Major.Minor.Patch version, may include `-dev` | `2024.2.5`, `5.2.0-dev`|
| **Build** | Build number or release candidate | `0`, `rc1`, `rc2` |
| **Date** | Build date in YYYYMMDD format | `20250221`, `20260119` |
| **Commit ID** | Git commit hash (short form) | `cb9e2a54ae6d`, `4cde34f6f20b` |
| **Suffix** | Optional suffix for package version | `-1`, `-2` (optional) |

### 4. Relocatable / Unified Package Versions (NEW)

Install from the latest unified (relocatable) tarball published to S3:

**Format**: `relocatable:<branch>:<arch>`

| Part | Required | Default | Description |
|------|----------|---------|-------------|
| `relocatable` | yes | — | Literal prefix that triggers unified-package resolution |
| `<branch>` | no | `master` | ScyllaDB branch, e.g. `master`, `branch-2025.1`, `enterprise` |
| `<arch>` | no | `x86_64` | CPU architecture — `x86_64` or `aarch64`. Auto-detected from AWS instance type on the AWS backend |

**Examples**:
```bash
# Latest master build for x86_64 (default arch)
export SCT_SCYLLA_VERSION=relocatable:master

# Latest master build, explicitly x86_64
export SCT_SCYLLA_VERSION=relocatable:master:x86_64

# Latest from a release branch for ARM
export SCT_SCYLLA_VERSION=relocatable:branch-2025.1:aarch64

# Enterprise branch
export SCT_SCYLLA_VERSION=relocatable:enterprise:x86_64
```

**How it works**:
1. SCT fetches `00-Build.txt` from `s3://downloads.scylladb.com/unstable/<product>/<branch>/relocatable/latest/`
2. The `url-id` field in that file gives the timestamp of the latest build
3. SCT lists the S3 prefix for that timestamp and finds the matching `scylla-unified-*.tar.gz`
4. The resolved URL is set as `unified_package`, `use_preinstalled_scylla` is set to `False`, and `scylla_version` is cleared

**Architecture auto-detection** (AWS only):
When `<arch>` is omitted and the backend is `aws`, SCT inspects the configured `instance_type_db` to determine the architecture automatically. On non-AWS backends, `x86_64` is used as the default and a warning is logged suggesting explicit specification.

**Usage with all backends**:
```bash
# AWS — arch auto-detected from instance type
export SCT_SCYLLA_VERSION=relocatable:master
hydra run-test longevity_test.LongevityTest.test_custom_time --backend aws \
  --config test-cases/PR-provision-test.yaml

# GCE — specify arch explicitly
export SCT_SCYLLA_VERSION=relocatable:master:x86_64
hydra run-test longevity_test.LongevityTest.test_custom_time --backend gce \
  --config test-cases/PR-provision-test.yaml

# Docker
export SCT_SCYLLA_VERSION=relocatable:master:x86_64
hydra run-test artifacts_test --backend docker \
  --config test-cases/artifacts/docker.yaml
```


## Usage Examples

### Example 1: Testing a Specific Development Build

```bash
# Use exact development build from CI
export SCT_SCYLLA_VERSION=2026.1.0~dev-0.20260119.4cde34f6f20b
export SCT_ENABLE_ARGUS=false

hydra run-test longevity_test.LongevityTest.test_custom_time \
  --backend aws \
  --config test-cases/PR-provision-test.yaml \
  --config configurations/network_config/test_communication_public.yaml
```

### Example 2: Testing a Release Candidate

```bash
# Use specific RC build
export SCT_SCYLLA_VERSION=5.2.0-rc1.20220829.67c91e8bcd61
export SCT_ENABLE_ARGUS=false

hydra run-test longevity_test.LongevityTest.test_custom_time \
  --backend gce \
  --config test-cases/PR-provision-test.yaml
```

## Backward Compatibility

Full version tag support is **fully backward compatible**:

- ✅ Simple versions continue to work: `5.2.1`, `2024.2.0`
- ✅ Branch versions continue to work: `master:latest`, `enterprise:latest`
- ✅ Relocatable prefix resolves to unified package: `relocatable:master:x86_64`
- ✅ Existing test configs require no changes
- ✅ Automatic detection determines version format


## Troubleshooting

### AMI/Image Not Found

If you get an error like:
```
ValueError: AMIs for scylla_version='2026.1.0~dev-0.20260119.4cde34f6f20b' not found
```

**Solutions**:
1. Verify the version tag exists in the target region/project
2. Check the version format matches the regex pattern
3. Ensure you have access to the images (correct AWS account, GCE project, etc.)
4. Try listing available images to find the correct tag

### Version Format Not Recognized

If the version isn't being detected as a full tag:
1. Verify the date is exactly 8 digits (YYYYMMDD)
2. Ensure the commit ID contains only hex characters (a-f, 0-9)
3. Check that the build is `0` or `rc<digits>`
4. Use the correct separator: `-` or `~` for dev versions

### Docker Repository Not Found

If Docker can't find the image:
1. Full version tags use nightly repositories
2. Verify the image exists: `docker search scylladb/scylla-nightly`
3. Check if it's an enterprise vs community build

## Additional Resources

- **Design Document**: `docs/plans/config/full-version-tag-lookup.md`
- **Configuration Options**: `docs/configuration_options.md`
- **Test Examples**: `test-cases/PR-provision-test.yaml`
- **Unit Tests**: `unit_tests/test_version_utils.py`

## Support

For issues or questions about full version tag support:
1. Check this documentation first
2. Review the design document in `docs/plans/config/full-version-tag-lookup.md`
3. Look at unit tests in `unit_tests/test_version_utils.py` for examples
4. Open an issue on GitHub with version tag details and error messages
