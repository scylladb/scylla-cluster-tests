# Docker Cleanup for All Backends Plan

## Problem Statement

Docker cleanup currently only runs when `cluster_backend` is "docker" or empty string. Tests on AWS/Azure/GCE backends create local Docker containers (for stress tools, monitoring, etc.) that are never cleaned up, causing resource accumulation on Jenkins builders.

### Pain Points
- Docker containers accumulate on builders over time
- Tests on cloud backends (aws, gce, azure) leave Docker resources uncleaned
- Unused builder scanning code complicates the cleanup logic
- No way to clean local Docker resources when testing on cloud backends

## Current State

**File: `sdcm/utils/resources_cleanup.py`**
- Line 127-129: Docker cleanup only runs when `cluster_backend in ("docker", "")`
- Function `clean_resources_docker()` has unused `builder_name` parameter
- Calls `list_resources_docker()` with unused parameters

**File: `sdcm/utils/common.py`**
- Function `list_resources_docker()` has unused `builder_name` and `group_as_builder` parameters
- Contains obsolete builder scanning logic via `list_clients_docker()`
- Static builders are no longer used in the system

## Goals

1. Enable Docker cleanup for ALL backends (aws, gce, azure, docker, k8s-*, xcloud)
2. Remove unused builder scanning code and parameters
3. Simplify Docker cleanup to scan local daemon only
4. Maintain backward compatibility where possible

## Implementation Phases

### Phase 1: Enable Docker Cleanup for All Backends

**Objective**: Make Docker cleanup run regardless of backend choice

**Changes**:
- File: `sdcm/utils/resources_cleanup.py`
- Change line 127 condition from `if cluster_backend in ("docker", ""):`
- To: `if not cluster_backend.startswith("k8s-local"):`
- Add comment explaining why all backends need Docker cleanup
- Add INFO-level logging for each container/image cleaned (e.g., "Cleaned Docker container: <name>")

**Definition of Done**:
- [ ] Condition changed to run for all backends except k8s-local
- [ ] Comment added explaining rationale
- [ ] Docker cleanup runs for aws/azure/gce test runs
- [ ] INFO-level logging added for each resource cleaned

**Dependencies**: None

---

### Phase 2: Remove Unused Parameters from clean_resources_docker

**Objective**: Simplify function signature by removing unused parameters

**Changes**:
- File: `sdcm/utils/resources_cleanup.py`
- Remove `builder_name` parameter from `clean_resources_docker()` function
- Update function call to `list_resources_docker()` to not pass `builder_name`
- Update docstring to clarify local-only scanning

**Definition of Done**:
- [ ] `builder_name` parameter removed from function signature
- [ ] All callers updated (should be only one in same file)
- [ ] Docstring updated
- [ ] No references to `builder_name` remain in function

**Dependencies**: Phase 1 (so tests pass during development)

---

### Phase 3: Simplify list_resources_docker

**Objective**: Remove unused parameters and builder scanning logic

**Changes**:
- File: `sdcm/utils/common.py`
- Remove `builder_name` and `group_as_builder` parameters from `list_resources_docker()`
- Remove call to `list_clients_docker()`
- Use `docker.from_env()` directly to get local Docker client
- Update docstring

**Definition of Done**:
- [ ] Parameters removed from function signature
- [ ] Direct use of `docker.from_env()` implemented
- [ ] No builder scanning logic remains
- [ ] Function works with local Docker daemon only
- [ ] All callers updated

**Dependencies**: Phase 2

---

### Phase 4: Remove Obsolete list_clients_docker Function

**Objective**: Clean up unused builder scanning code

**Changes**:
- File: `sdcm/utils/common.py`
- Check if `list_clients_docker()` is used elsewhere
- If not used, remove the function entirely
- If used elsewhere, document but don't remove

**Definition of Done**:
- [ ] Usage of `list_clients_docker()` verified across codebase
- [ ] Function removed if unused, or documented if still needed
- [ ] Related builder connection helper functions assessed for removal

**Dependencies**: Phase 3

## Testing Requirements

### Unit Tests
- Verify Docker cleanup is called for all backend types (aws, gce, azure, docker)
- Verify Docker cleanup is skipped for k8s-local backend
- Verify `list_resources_docker()` works with simplified signature
- Mock `docker.from_env()` to test without real Docker daemon

### Integration Tests
- Manual test: Run artifacts test with `backend=aws`, verify Docker cleanup executes
- Manual test: Run test with `backend=docker`, verify cleanup still works
- Manual test: Verify no builder scanning attempts in logs

## Success Criteria

1. Docker cleanup runs for all backends except k8s-local
2. Local Docker containers are cleaned after tests on any backend
3. No unused parameters remain in cleanup functions
4. No builder scanning code remains in cleanup path
5. Cleanup time not significantly affected (should be faster without builder scanning)

## Risk Mitigation

### Risk: Breaking existing cleanup behavior
- **Mitigation**: Incremental changes in separate phases
- **Rollback**: Each phase is independently revertible

### Risk: Missing Docker containers if pattern is wrong
- **Mitigation**: Test with various test scenarios on different backends
- **Validation**: Check Docker container count before/after tests

### Risk: list_clients_docker used elsewhere
- **Mitigation**: Phase 4 includes codebase search before removal
- **Fallback**: Document function instead of removing if still used

## Open Questions

- ~~Are there any test scenarios that create Docker containers with non-standard labels?~~
  - **Resolution**: Can assume all test containers use standard TestId/NodeType labels (to be verified during Phase 1 implementation)
- ~~Should we add logging to show how many containers were cleaned up?~~
  - **Resolution**: Yes, add INFO-level logging for each container/image cleaned
