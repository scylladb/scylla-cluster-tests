# Design Plan: Full Version Tag Lookup for scylla_version Parameter

## Problem Statement

Currently, `scylla_version` parameter supports:
- Simple versions: `5.2.1`, `2024.2.0`
- Branch versions: `branch-2019.1:latest`, `master:latest`

**Need to support full version tags** like: `2024.2.5-0.20250221.cb9e2a54ae6d-1`

This format includes:
- Base version: `2024.2.5`
- Build number: `0`
- Date: `20250221`
- Commit ID: `cb9e2a54ae6d`
- Extra suffix: `-1`

## Current Implementation Analysis

### 1. AWS (AMI Lookup)

- **Location**: `sdcm/utils/common.py`
- **Functions**: `get_scylla_ami_versions()`, `get_branched_ami()`
- **Current behavior**:
  - Uses tag filter: `tag:scylla_version` with version pattern matching
  - Supports prefix matching for versions like `4.6.4*`
  - Branch format: `branch-name:build-id`

### 2. GCE (Image Lookup)

- **Location**: `sdcm/utils/common.py`
- **Functions**: `get_scylla_gce_images_versions()`, `get_branched_gce_images()`
- **Current behavior**:
  - Uses label filter: `labels.scylla_version`
  - Replaces `.` with `-` for label matching
  - Branch format: `branch:build-id`

### 3. Azure (Image Lookup)

- **Location**: `sdcm/provision/azure/utils.py`
- **Functions**: `get_scylla_images()`, `get_scylla_images_private_galleries()`
- **Current behavior**:
  - Uses tag: `scylla_version`
  - Supports prefix matching
  - Uses `SCYLLA_VERSION_GROUPED_RE` for parsing
  - Branch format: `branch:build-id`

### 4. Docker (Image Tag Lookup)

- **Location**: `sdcm/utils/version_utils.py`
- **Functions**: `get_scylla_docker_repo_from_version()`, `get_specific_tag_of_docker_image()`
- **Current behavior**:
  - Resolves repo based on version
  - For `latest`, fetches specific tag from S3
  - Tags format: `5.2.0-dev-0.20220829.67c91e8bcd61`

## Design Approach

### Option 1: Extend Current Pattern Matching (Recommended)

**Rationale**: Minimal changes, leverages existing infrastructure

**Implementation**:

1. **Update `sct_config.py` version detection**
   - Add detection for full version tag format
   - Extract base version from full tag for lookup
   - Pattern: `(?P<base_version>[\d.]+)-(?P<build>\d+)\.(?P<date>\d+)\.(?P<commit>\w+).*`

2. **Enhance AWS lookup** (`sdcm/utils/common.py`)
   - In `get_scylla_ami_versions()`:
     - Detect full version tag format
     - Extract base version and commit ID
     - Add filter for `tag:build-id` or `tag:commit-id`
   - Keep backward compatibility with simple versions

3. **Enhance GCE lookup** (`sdcm/utils/common.py`)
   - In `get_scylla_gce_images_versions()`:
     - Parse full version tag
     - Use `labels.scylla_version` with full tag (dots→dashes)
     - Filter by commit hash if needed

4. **Enhance Azure lookup** (`sdcm/provision/azure/utils.py`)
   - In `get_scylla_images()`:
     - Already uses `SCYLLA_VERSION_GROUPED_RE` - leverage this
     - Add exact match support for full tags
     - Filter by tags: `scylla_version`, `build-id`, `commit-id`

5. **Enhance Docker lookup** (`sdcm/utils/version_utils.py`)
   - In `get_scylla_docker_repo_from_version()`:
     - Parse full version tag to extract base version
     - Determine correct repo (scylla vs enterprise)
   - Docker tags already support this format

### Option 2: New Dedicated Lookup Path

**Rationale**: Clean separation, but requires more changes

**Implementation**:
- Create new functions: `get_*_by_full_version_tag()`
- Separate code path in `sct_config.py`
- More code duplication

**Decision**: Choose Option 1 (minimal changes, backward compatible)

## Implementation Plan

### Phase 1: Core Version Parsing Enhancement

- [ ] Add full version tag regex pattern to `version_utils.py`
- [ ] Create helper function to parse full version tags
- [ ] Extract base version, build ID, date, commit ID
- [ ] Add unit tests for version parsing

### Phase 2: AWS Implementation

- [ ] Update `get_scylla_ami_versions()` to handle full tags
- [ ] Add commit ID and build ID filtering
- [ ] Update `sct_config.py` AWS section
- [ ] Add unit tests for AWS image lookup

### Phase 3: GCE Implementation

- [ ] Update `get_scylla_gce_images_versions()` for full tags
- [ ] Handle label format conversion
- [ ] Update `sct_config.py` GCE section
- [ ] Add unit tests for GCE image lookup

### Phase 4: Azure Implementation

- [ ] Update `get_scylla_images()` for exact tag matching
- [ ] Leverage existing `SCYLLA_VERSION_GROUPED_RE`
- [ ] Update `sct_config.py` Azure section
- [ ] Add unit tests for Azure image lookup

### Phase 5: Docker Implementation

- [ ] Update `get_scylla_docker_repo_from_version()`
- [ ] Handle full tag format in repo detection
- [ ] Update `sct_config.py` Docker section
- [ ] Add unit tests for Docker tag handling

### Phase 6: Integration & Documentation

- [ ] Update configuration documentation
- [ ] Add examples to test configs
- [ ] Integration testing across all backends
- [ ] Update README with examples

## Key Technical Details

### Version Format Support Matrix

| Format | Example | AWS | GCE | Azure | Docker |
|--------|---------|-----|-----|-------|--------|
| Simple | `5.2.1` | ✓ | ✓ | ✓ | ✓ |
| Branch | `master:latest` | ✓ | ✓ | ✓ | ✓ |
| Full Tag (NEW) | `2024.2.5-0.20250221.cb9e2a54ae6d-1` | ✓ | ✓ | ✓ | ✓ |

### Regex Pattern

```python
FULL_VERSION_TAG_RE = re.compile(
    r"(?P<base_version>[\d.]+)"
    r"-(?P<build>\d+|rc\d+)"
    r"\.(?P<date>\d{8})"
    r"\.(?P<commit>[a-f0-9]+)"
    r"(?P<suffix>.*)"
)
```

### Backend-Specific Considerations

#### AWS

- AMI tags: `scylla_version`, `build-id`, `commit-id`
- Filter strategy: Exact match on scylla_version OR match base + commit

#### GCE

- Labels: `scylla_version` (dots become dashes)
- Filter: Exact label match after normalization

#### Azure

- Tags: `scylla_version`, `build-id`
- Already has version parsing via `SCYLLA_VERSION_GROUPED_RE`
- Enhance to support exact matching

#### Docker

- Tags already in this format
- Parse to determine repo (scylla vs enterprise vs nightly)

## Backward Compatibility

✅ All existing formats remain supported:
- Simple versions: `5.2.1`, `2024.2.0`
- Branch versions: `master:latest`, `branch-2019.1:all`
- Prefix matching: Will be attempted as fallback

## Testing Strategy

### 1. Unit Tests

- Version parsing
- Each backend's image lookup function
- Edge cases (malformed versions, missing images)

### 2. Integration Tests

- Mock AWS/GCE/Azure APIs
- Test with real version tags
- Verify correct image selection

### 3. Manual Testing

- Documented in PR for each backend
- Real provisioning with full version tags

## Files to Modify

1. `sdcm/utils/version_utils.py` - Add regex and parser
2. `sdcm/utils/common.py` - Update AWS/GCE functions
3. `sdcm/provision/azure/utils.py` - Update Azure functions
4. `sdcm/sct_config.py` - Update version handling logic
5. `unit_tests/test_version_utils.py` - Add tests
6. `unit_tests/provisioner/test_azure_get_scylla_images.py` - Add tests
7. `defaults/test_default.yaml` - Add examples (if needed)

## Risk Assessment

### Low Risk

- Changes are additive (new format support)
- Existing code paths remain unchanged
- Comprehensive unit tests will validate

### Mitigation

- Feature flag to enable/disable if needed
- Thorough testing before merge
- Gradual rollout per backend

## Success Criteria

✅ Users can specify `scylla_version: 2024.2.5-0.20250221.cb9e2a54ae6d-1`
✅ System correctly identifies and provisions images on AWS, GCE, Azure, Docker
✅ All existing version formats continue to work
✅ Unit tests pass with >90% coverage
✅ Documentation updated with examples

## Status

**Design complete** - awaiting approval to proceed with implementation
