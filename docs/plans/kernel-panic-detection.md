# Kernel Panic Detection for Cloud Providers

## Problem Statement

Kernel panics during testing are critical failures that can cause nodes to become unresponsive, leading to test hangs and wasted time. Currently, SCT has no automated mechanism to detect kernel panics, requiring manual log inspection after failures.

### Current Behavior
- No automatic kernel panic detection
- Test hangs when nodes panic during testing
- Manual log inspection required to diagnose failures
- No integration with event system for automated response
- Kernel panics discovered only after test timeout or manual investigation

### Impact
- Wasted test time due to hangs
- Delayed failure detection
- Manual effort required for diagnosis
- No automated alerting or response capability

## Goals

1. **Automatic kernel panic detection** across AWS, GCE, and Azure platforms
2. **Early failure detection** to minimize wasted test time
3. **Event system integration** for automated response and alerting
4. **Cloud-agnostic API** with provider-specific implementations
5. **Thread-safe and reliable** monitoring with minimal overhead
6. **Comprehensive testing** to prevent false positives

## Proposed Solution

This is a clean re-implementation of PR #12753, addressing all review feedback.

### Architecture Overview

**Event System**
- `KernelPanicEvent` with CRITICAL severity
- Published when kernel panic detected
- Consumable by Argus, dashboards, alerting systems

**Checker Threads**
- Daemon threads with 30-second polling interval
- Auto-start in node `init()`, auto-stop in `destroy()`
- Thread-safe using `threading.Event` for state management
- Context manager support for manual testing
- Stop automatically after panic detection

**Cloud Provider Implementations**
- AWS: EC2 console output monitoring
- GCE: Serial port output monitoring
- Azure: Boot diagnostics blob monitoring

---

## Implementation Plan

### Phase 1: Core Event System

**Objective**: Add kernel panic event to SCT event system

**Files Modified**:
- `sdcm/sct_events/system.py` - Add `KernelPanicEvent` class
- `defaults/severities.yaml` - Register event with CRITICAL severity

**Expected Outcome**: Event system ready for kernel panic reporting

---

### Phase 2: AWS Implementation

**Objective**: Implement kernel panic detection for AWS EC2 instances

**Files Modified**:
- `sdcm/cluster_aws.py` - Add `AWSKernelPanicChecker` class and integrate into `AWSNode`

**Key Requirements** (addressing PR #12753 review feedback):
1. ✅ **Import placement**: All imports at top of file (threading, KernelPanicEvent)
2. ✅ **Thread safety**: Use `threading.Event()` instead of boolean flag for `_panic_detected`
3. ✅ **AWS API fix**: Remove invalid `Latest=True` from `get_console_output()` call
4. ✅ **Logging**: Use module-level `LOGGER`, no timestamp prefixes
5. ✅ **Daemon thread**: Set `daemon=True` for auto-cleanup

**Expected Outcome**: AWS nodes automatically monitor for kernel panics

---

### Phase 3: GCE Implementation

**Objective**: Implement kernel panic detection for GCE instances

**Files Modified**:
- `sdcm/cluster_gce.py` - Add `GCPKernelPanicChecker` class and integrate into `GCENode`

**Key Requirements** (addressing PR #12753 review feedback):
1. ✅ **Import placement**: All imports at top of file
2. ✅ **Thread safety**: Use `threading.Event()` for `_panic_detected`
3. ✅ **Logging cleanup**: Remove timestamp prefixes (framework adds them)
4. ✅ **Logger consistency**: Use module-level `LOGGER` instead of `logging.debug/info/error`
5. ✅ **Daemon thread**: Set `daemon=True`

**Expected Outcome**: GCE nodes automatically monitor for kernel panics

---

### Phase 4: Azure Implementation

**Objective**: Implement kernel panic detection for Azure VMs

**Files Modified**:
- `sdcm/provision/azure/provisioner.py` - Add public `resource_group_name` property
- `sdcm/cluster_azure.py` - Add `AzureKernelPanicChecker` class and integrate into `AzureNode`

**Key Requirements** (addressing PR #12753 review feedback):
1. ✅ **Import placement**: All imports at top of file
2. ✅ **Thread safety**: Use `threading.Event()` for `_panic_detected`
3. ✅ **Logging cleanup**: Remove timestamp prefixes
4. ✅ **Encapsulation fix**: Add public property for resource group instead of accessing `_provisioner._resource_group_name`
5. ✅ **Error handling**: Handle `ResourceNotFoundError` when VM is terminated (race condition)
6. ✅ **Daemon thread**: set `daemon=True`

**Expected Outcome**: Azure nodes automatically monitor for kernel panics

---

### Phase 5: Comprehensive Unit Testing

**Objective**: Create pytest-style unit tests with full coverage

**Files Created**:
- `unit_tests/test_kernel_panic_checkers.py` - Tests for all checker implementations

**Key Requirements** (addressing PR #12753 review feedback):
1. ✅ **Pytest style**: Use pure pytest, NOT `unittest.TestCase`
2. ✅ **Exception types**: Use `AssertionError` instead of generic `Exception`
3. ✅ **Fixtures**: Use `@pytest.fixture` for setup
4. ✅ **Parametrize**: Use `@pytest.mark.parametrize` for multiple test cases
5. ✅ **Mocking**: Mock all cloud provider APIs for fast, deterministic tests

**Test Coverage**:
- Initialization tests (thread starts, daemon mode, correct parameters)
- Panic detection tests (detect patterns, publish events, stop after detection)
- False positive prevention tests (normal output, partial matches, case sensitivity)
- Thread lifecycle tests (start/stop, cleanup, timeouts, context manager)
- Error handling tests (API errors, ResourceNotFoundError for Azure, transient errors)

**Expected Outcome**: 
- 15+ tests covering all scenarios
- 100% pass rate
- ~5-10 second execution time
- No false positives

---

### Phase 6: Code Quality and Security Validation

**Objective**: Ensure code quality and security before finalizing

**Tasks**:
1. Pre-commit checks (autopep8, ruff, YAML validation, commit message validation)
2. Code review (common issues, best practices)
3. Security scan (vulnerability detection, fixes)
4. Final validation (full test suite, regression check)

**Expected Outcome**: Clean, secure, well-tested implementation ready for merge

---

## Success Criteria

1. ✅ All three cloud providers (AWS, GCE, Azure) have kernel panic detection
2. ✅ Events are published correctly with CRITICAL severity
3. ✅ All review feedback from PR #12753 addressed
4. ✅ 15+ unit tests with 100% pass rate
5. ✅ No false positives in testing
6. ✅ Code passes pre-commit, code review, and security scans
7. ✅ Thread-safe implementation with proper cleanup
8. ✅ Documentation in plan format

## Non-Goals

- Kernel panic recovery/remediation (only detection)
- Historical panic analysis
- Panic prevention mechanisms
- Integration with specific alerting systems (events can be consumed by any system)
- Docker/Kubernetes backend support (cloud providers only)

## Timeline

- Phase 1 (Event System): ~30 minutes
- Phase 2 (AWS): ~1 hour
- Phase 3 (GCE): ~1 hour
- Phase 4 (Azure): ~1.5 hours (includes encapsulation fix)
- Phase 5 (Testing): ~2 hours
- Phase 6 (Validation): ~1 hour

**Total Estimated Time**: ~7 hours

## References

- Original PR: #12753
- Related Issue: #924 (unrelated, old issue)
- Review Comments: 24 review threads addressing code quality issues
