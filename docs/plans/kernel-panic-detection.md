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

**Implementation Details**:

`sdcm/sct_events/system.py`:
```python
class KernelPanicEvent(SystemEvent):
    """Published when a kernel panic is detected on a node."""
    
    def __init__(self, node, message: str, severity=Severity.CRITICAL):
        super().__init__(severity=severity)
        self.node = node
        self.message = message
```

`defaults/severities.yaml`:
```yaml
KernelPanicEvent: CRITICAL
```

**Testing**:
- Verify event can be instantiated
- Verify event publishes correctly
- Verify severity is CRITICAL

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

**Implementation Details**:

At top of file (with other imports):
```python
import threading
from sdcm.sct_events.system import KernelPanicEvent
```

Constants:
```python
CHECK_INTERVAL_SECONDS = 30  # Check every 30 seconds
```

`AWSKernelPanicChecker` class:
```python
class AWSKernelPanicChecker(threading.Thread):
    """Monitor AWS EC2 instance for kernel panics via console output."""

    def __init__(self, node, instance_id, region="us-east-1"):
        super().__init__()
        self.node = node
        self.instance_id = instance_id
        self.region = region
        self.ec2 = boto3.client("ec2", region_name=region)
        self._stop_event = threading.Event()
        self._panic_detected = threading.Event()  # Thread-safe flag
        self.daemon = True

    def run(self):
        while not self._stop_event.is_set():
            try:
                # Check console output for panic
                console = self.ec2.get_console_output(InstanceId=self.instance_id)
                output = console.get("Output", "")
                output_lower = output.lower()
                
                if ("kernel panic" in output_lower or "not syncing" in output_lower) and not self._panic_detected.is_set():
                    self._panic_detected.set()
                    
                    # Extract panic lines
                    panic_lines = []
                    for line in output.splitlines():
                        line_lower = line.lower()
                        if "kernel panic" in line_lower or "not syncing" in line_lower:
                            panic_lines.append(line.strip())
                    
                    panic_text = " | ".join(panic_lines) if panic_lines else "Kernel panic detected"
                    message = f"Kernel panic detected in console log for instance {self.instance_id}: {panic_text}"
                    
                    LOGGER.error("[AWS] %s", message)
                    LOGGER.error("[AWS] Full console output for %s:\n%s", self.instance_id, output)
                    
                    KernelPanicEvent(node=self.node, message=message).publish()
                    self._stop_event.set()
                    
            except Exception as exc:  # noqa: BLE001
                LOGGER.error("[AWS] Error checking %s: %s", self.instance_id, exc)
            
            self._stop_event.wait(CHECK_INTERVAL_SECONDS)

    def stop(self):
        self._stop_event.set()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        self.join()
```

Integration in `AWSNode`:
```python
class AWSNode(cluster.BaseNode):
    def __init__(self, ...):
        # ... existing code ...
        self.kernel_panic_checker = None

    def init(self):
        # ... existing code ...
        
        # Start kernel panic monitoring
        self.kernel_panic_checker = AWSKernelPanicChecker(
            node=self,
            instance_id=self._instance.id,
            region=self._ec2_service.meta.client.meta.region_name
        )
        self.kernel_panic_checker.start()
        LOGGER.info("Started kernel panic monitoring for node %s (instance: %s)", 
                   self.name, self._instance.id)

    def destroy(self):
        # Stop kernel panic monitoring
        if self.kernel_panic_checker:
            LOGGER.info("Stopping kernel panic monitoring for node %s", self.name)
            self.kernel_panic_checker.stop()
            self.kernel_panic_checker.join(timeout=5)
            self.kernel_panic_checker = None
        
        # ... existing code ...
```

**Testing**:
- Unit test: Mock EC2 client, verify panic detection
- Unit test: Verify no false positives with normal output
- Unit test: Verify thread lifecycle (start/stop/cleanup)
- Unit test: Verify context manager support

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

**Implementation Details**:

At top of file (with other imports):
```python
import threading
from sdcm.sct_events.system import KernelPanicEvent
from sdcm.utils.gce_utils import get_gce_compute_instances_client
```

`GCPKernelPanicChecker` class:
```python
class GCPKernelPanicChecker(threading.Thread):
    """Monitor GCE instance for kernel panics via serial port output."""

    def __init__(self, node, instance_name, project, zone):
        super().__init__()
        self.node = node
        self.instance_name = instance_name
        self.project = project
        self.zone = zone
        self.compute_client = get_gce_compute_instances_client()
        self._stop_event = threading.Event()
        self._panic_detected = threading.Event()
        self.daemon = True

    def run(self):
        while not self._stop_event.is_set():
            try:
                # Get serial port output
                serial_output = self.compute_client.get_serial_port_output(
                    project=self.project,
                    zone=self.zone,
                    instance=self.instance_name
                )
                output = serial_output.contents if hasattr(serial_output, 'contents') else ""
                output_lower = output.lower()
                
                if ("kernel panic" in output_lower or "not syncing" in output_lower) and not self._panic_detected.is_set():
                    self._panic_detected.set()
                    
                    # Extract panic lines
                    panic_lines = []
                    for line in output.splitlines():
                        line_lower = line.lower()
                        if "kernel panic" in line_lower or "not syncing" in line_lower:
                            panic_lines.append(line.strip())
                    
                    panic_text = " | ".join(panic_lines) if panic_lines else "Kernel panic detected"
                    message = f"Kernel panic detected in serial output for instance {self.instance_name}: {panic_text}"
                    
                    LOGGER.error("[GCP] %s", message)
                    LOGGER.error("[GCP] Full serial output for %s:\n%s", self.instance_name, output)
                    
                    KernelPanicEvent(node=self.node, message=message).publish()
                    self._stop_event.set()
                    
            except Exception as exc:  # noqa: BLE001
                LOGGER.error("[GCP] Error checking %s: %s", self.instance_name, exc)
            
            self._stop_event.wait(CHECK_INTERVAL_SECONDS)

    def stop(self):
        self._stop_event.set()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        self.join()
```

Integration in `GCENode`: Similar to AWS pattern

**Testing**:
- Unit test: Mock GCE client, verify panic detection
- Unit test: Verify no false positives
- Unit test: Verify thread lifecycle
- Unit test: Verify context manager support

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

**Implementation Details**:

`sdcm/provision/azure/provisioner.py`:
```python
class AzureProvisioner:
    # ... existing code ...
    
    @property
    def resource_group_name(self):
        """Public property to access resource group name."""
        return self._resource_group_name
```

`sdcm/cluster_azure.py` - at top of file (with other imports):
```python
import threading
from sdcm.sct_events.system import KernelPanicEvent
from sdcm.utils.azure_utils import AzureService
```

`AzureKernelPanicChecker` class:
```python
class AzureKernelPanicChecker(threading.Thread):
    """Monitor Azure VM for kernel panics via boot diagnostics."""

    def __init__(self, node, vm_name, region, resource_group):
        super().__init__()
        self.node = node
        self.vm_name = vm_name
        self.region = region
        self.resource_group = resource_group
        self.compute_client = AzureService().compute
        self._stop_event = threading.Event()
        self._panic_detected = threading.Event()
        self.daemon = True

    def run(self):
        while not self._stop_event.is_set():
            try:
                # Handle potential VM termination race condition
                try:
                    instance_view = self.compute_client.virtual_machines.instance_view(
                        resource_group_name=self.resource_group,
                        vm_name=self.vm_name
                    )
                except Exception as exc:
                    if "ResourceNotFoundError" in str(type(exc).__name__):
                        LOGGER.debug("[Azure] VM %s no longer exists, stopping monitoring", self.vm_name)
                        self._stop_event.set()
                        return
                    raise
                
                # Check boot diagnostics
                # Implementation depends on Azure boot diagnostics API
                # Similar pattern to AWS/GCE
                
            except Exception as exc:  # noqa: BLE001
                LOGGER.error("[Azure] Error checking %s: %s", self.vm_name, exc)
            
            self._stop_event.wait(CHECK_INTERVAL_SECONDS)

    def stop(self):
        self._stop_event.set()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        self.join()
```

Integration in `AzureNode`:
```python
class AzureNode(cluster.BaseNode):
    def __init__(self, ...):
        # ... existing code ...
        self.kernel_panic_checker = None

    def init(self):
        # ... existing code ...
        
        # Start kernel panic monitoring - use public property
        self.kernel_panic_checker = AzureKernelPanicChecker(
            node=self,
            vm_name=self._instance.name,
            region=self.region,
            resource_group=self._instance._provisioner.resource_group_name  # Now public
        )
        self.kernel_panic_checker.start()
        LOGGER.info("Started kernel panic monitoring for node %s (VM: %s)", 
                   self.name, self._instance.name)

    def destroy(self):
        # Stop kernel panic monitoring
        if self.kernel_panic_checker:
            LOGGER.info("Stopping kernel panic monitoring for node %s", self.name)
            self.kernel_panic_checker.stop()
            self.kernel_panic_checker.join(timeout=5)
            self.kernel_panic_checker = None
        
        # ... existing code ...
```

**Testing**:
- Unit test: Mock Azure client, verify panic detection
- Unit test: Verify ResourceNotFoundError handling
- Unit test: Verify no false positives
- Unit test: Verify thread lifecycle

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

1. **Initialization Tests**
   - Verify checker initializes with correct parameters
   - Verify thread starts successfully
   - Verify daemon mode is enabled

2. **Panic Detection Tests**
   - Verify detection of "kernel panic" pattern
   - Verify detection of "not syncing" pattern
   - Verify event is published with correct message
   - Verify thread stops after detection

3. **False Positive Prevention**
   - Verify normal output doesn't trigger detection
   - Verify partial matches don't trigger detection
   - Verify case-insensitive matching works

4. **Thread Lifecycle Tests**
   - Verify start/stop cycle works correctly
   - Verify cleanup after stop
   - Verify timeout handling
   - Verify context manager works

5. **Error Handling Tests**
   - Verify API errors don't crash thread
   - Verify ResourceNotFoundError handling (Azure)
   - Verify thread continues after transient errors

**Example Test Structure**:
```python
import pytest
from unittest.mock import Mock, patch, MagicMock

@pytest.fixture
def mock_node():
    """Fixture for mocked node."""
    node = Mock()
    node.name = "test-node"
    return node

@pytest.fixture
def mock_ec2_client():
    """Fixture for mocked EC2 client."""
    with patch('boto3.client') as mock:
        client = MagicMock()
        mock.return_value = client
        yield client

def test_aws_checker_detects_panic(mock_node, mock_ec2_client):
    """Test that AWS checker detects kernel panic in console output."""
    # Arrange
    mock_ec2_client.get_console_output.return_value = {
        "Output": "Kernel panic - not syncing: VFS: Unable to mount root fs"
    }
    
    # Act
    checker = AWSKernelPanicChecker(
        node=mock_node,
        instance_id="i-12345",
        region="us-east-1"
    )
    
    # Assert
    with checker:
        import time
        time.sleep(1)  # Allow thread to run
        assert checker._panic_detected.is_set()

def test_aws_checker_no_false_positive(mock_node, mock_ec2_client):
    """Test that AWS checker doesn't false-trigger on normal output."""
    # Arrange
    mock_ec2_client.get_console_output.return_value = {
        "Output": "Normal boot sequence, no issues"
    }
    
    # Act
    checker = AWSKernelPanicChecker(
        node=mock_node,
        instance_id="i-12345",
        region="us-east-1"
    )
    
    # Assert
    with checker:
        import time
        time.sleep(1)
        assert not checker._panic_detected.is_set()

@pytest.mark.parametrize("panic_text", [
    "Kernel panic - not syncing",
    "kernel panic: Fatal exception",
    "KERNEL PANIC - NOT SYNCING"
])
def test_aws_checker_panic_patterns(mock_node, mock_ec2_client, panic_text):
    """Test various kernel panic text patterns."""
    mock_ec2_client.get_console_output.return_value = {"Output": panic_text}
    
    checker = AWSKernelPanicChecker(
        node=mock_node,
        instance_id="i-12345",
        region="us-east-1"
    )
    
    with checker:
        import time
        time.sleep(1)
        assert checker._panic_detected.is_set()
```

**Testing Execution**:
```bash
uv run sct.py unit-tests -t test_kernel_panic_checkers.py
```

**Expected Outcome**: 
- 15+ tests covering all scenarios
- 100% pass rate
- ~5-10 second execution time
- No false positives
- All edge cases covered

---

### Phase 6: Code Quality and Security Validation

**Objective**: Ensure code quality and security before finalizing

**Tasks**:

1. **Pre-commit Checks**
   ```bash
   uv run sct.py pre-commit
   ```
   - autopep8 formatting
   - ruff linting
   - YAML validation
   - Commit message validation

2. **Code Review**
   ```bash
   # Via code_review tool
   ```
   - Check for common issues
   - Verify best practices
   - Address feedback

3. **Security Scan**
   ```bash
   # Via codeql_checker tool
   ```
   - Scan for vulnerabilities
   - Fix any issues found
   - Document any false positives

4. **Final Validation**
   - Run full unit test suite
   - Verify no regressions
   - Check git status for unwanted files

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
