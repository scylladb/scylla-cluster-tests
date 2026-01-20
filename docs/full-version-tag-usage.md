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

- **Design Document**: `docs/plans/full-version-tag-lookup.md`
- **Configuration Options**: `docs/configuration_options.md`
- **Test Examples**: `test-cases/PR-provision-test.yaml`
- **Unit Tests**: `unit_tests/test_version_utils.py`

## Support

For issues or questions about full version tag support:
1. Check this documentation first
2. Review the design document in `docs/plans/full-version-tag-lookup.md`
3. Look at unit tests in `unit_tests/test_version_utils.py` for examples
4. Open an issue on GitHub with version tag details and error messages
