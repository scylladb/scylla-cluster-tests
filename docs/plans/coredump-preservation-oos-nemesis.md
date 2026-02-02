# Preserve Coredump Storage During Out-of-Space Nemesis

## 1. Problem Statement

During out-of-space nemesis tests, when Scylla crashes, coredumps cannot be saved, resulting in loss of critical debugging information.

**Current Issue:**
- The `scylla_coredump_setup --dump-to-raiddir` script creates a bind mount: `/var/lib/scylla/coredump` → `/var/lib/systemd/coredump`
- Out-of-space nemesis fills `/var/lib/scylla` to ~100% capacity using `fallocate`
- When partition is full, systemd-coredump refuses to save cores: `"Not enough disk space for coredump of <PID> (scylla), refusing"`
- Without coredumps, we cannot debug crashes that occur during disk-full scenarios

**Why This Matters:**
- Lost debugging information for crashes during out-of-space conditions
- Historically seen in issues: https://github.com/scylladb/scylla/issues/7085, https://github.com/scylladb/scylla/issues/4614
- Prevents proper root cause analysis of production-like failure scenarios

## 2. Current State

### Coredump Setup Process
**File:** `sdcm/cluster.py`
- **Method:** `BaseNode.scylla_setup()` (line ~2616)
  - Calls `sudo /usr/lib/scylla/scylla_setup --nic {} --disks {} --setup-nic-and-disks {}`
  - This internally runs `scylla_coredump_setup --dump-to-raiddir`
  - Creates bind mount at `/var/lib/systemd/coredump` pointing to `/var/lib/scylla/coredump`

**File:** `sdcm/coredump.py`
- **Class:** `CoredumpExportSystemdThread` (line 354)
  - Monitors and collects coredumps from systemd-coredump
  - Expects coredumps in `/var/lib/systemd/coredump`

### Out-of-Space Nemesis Flow
**File:** `sdcm/nemesis.py`
- **Method:** `Nemesis.disrupt_nodetool_enospc()` (line 1817)
  - Calls `reach_enospc_on_node(target_node)` to fill disk
  - Calls `clean_enospc_on_node(target_node, sleep_time)` to clean up

**File:** `sdcm/utils/common.py`
- **Function:** `reach_enospc_on_node()` (line 2461)
  - Uses `fallocate` to fill `/var/lib/scylla` to ~100% capacity
  - Creates files named `occupy_90percent.<timestamp>`
- **Function:** `clean_enospc_on_node()` (line 2483)
  - Removes `occupy_90percent.*` files
  - Restarts scylla-server

### The Problem
When the bind mount is active:
1. Coredumps are written to `/var/lib/systemd/coredump`
2. Which is actually `/var/lib/scylla/coredump` (bind mount)
3. When `/var/lib/scylla` is 100% full, no space for coredumps
4. systemd-coredump refuses to write: "Not enough disk space for coredump"

### Current Workaround
None - coredumps are simply lost during out-of-space tests.

## 3. Goals

1. **Primary Goal:** Ensure coredumps are successfully saved during out-of-space nemesis execution
2. **Secondary Goal:** Maintain backward compatibility - non-OOS tests continue using bind mount
3. **Tertiary Goal:** Minimize changes to existing test infrastructure

**Success Metrics:**
- 100% coredump capture rate during out-of-space nemesis tests
- No regression in coredump capture for non-OOS tests
- Works across all backends (AWS, GCE, Azure, Docker, K8s)

## 4. Implementation Phases

### Phase 1: Add Runtime Coredump Location Switching
**Description:** Implement a context manager to dynamically switch coredump location between data partition and root filesystem.

**Definition of Done:**
- [ ] Create `coredump_to_root()` context manager in `sdcm/utils/context_managers.py`
  - On entry: Unmounts `/var/lib/systemd/coredump` if bind-mounted
  - Ensures coredumps are written to root partition during context
  - On exit: Restores bind mount to previous state
  - Handles errors gracefully with proper cleanup
- [ ] Context manager works idempotently (safe to call multiple times)
- [ ] Add logging for all mount/unmount operations

**Implementation Details:**

```python
@contextmanager
def coredump_to_root(target_node):
    """
    Context manager to temporarily switch coredump storage to root filesystem.
    
    Usage:
        with coredump_to_root(node):
            # Coredumps are written to root partition
            reach_enospc_on_node(target_node=node)
        # Bind mount is automatically restored
    
    Args:
        target_node: The node to switch coredump location on
    """
    # Check if bind mount exists
    mount_info = target_node.remoter.run(
        "mount | grep '/var/lib/systemd/coredump'", 
        ignore_status=True
    )
    
    was_mounted = mount_info.ok
    
    if was_mounted:
        # Unmount the bind mount
        target_node.log.info("Switching coredump storage to root filesystem")
        target_node.remoter.sudo("systemctl stop var-lib-systemd-coredump.mount")
        target_node.remoter.sudo("systemctl disable var-lib-systemd-coredump.mount")
        # Wait for unmount to complete
        time.sleep(5)
    
    try:
        yield
    finally:
        # Restore bind mount if it was previously active
        if was_mounted:
            target_node.log.info("Restoring coredump storage to data partition")
            target_node.remoter.sudo("systemctl enable var-lib-systemd-coredump.mount")
            target_node.remoter.sudo("systemctl start var-lib-systemd-coredump.mount")
            time.sleep(5)
```

**Dependencies:** None

**Deliverables:**
- Context manager in `sdcm/utils/context_managers.py`
- Unit tests for the context manager

### Phase 2: Integrate with Out-of-Space Nemesis
**Description:** Modify the out-of-space nemesis to use runtime coredump switching via context manager.

**Definition of Done:**
- [ ] Modify `disrupt_nodetool_enospc()` in `sdcm/nemesis.py`
  - Use `coredump_to_root()` context manager around disk fill operations
  - Automatic restoration on context exit
- [ ] Add similar logic to `disrupt_end_of_quota_nemesis()` (line 1848)
- [ ] Add logging for coredump location switches
- [ ] Test with manual nemesis execution

**Implementation Details:**

```python
def disrupt_nodetool_enospc(self, sleep_time=30, all_nodes=False):
    # ... existing code ...
    
    for node in nodes:
        with coredump_to_root(node):
            # Coredumps are now written to root partition
            with ignore_no_space_errors(node=node):
                if self._is_it_on_kubernetes():
                    self._k8s_fake_enospc_error(node)
                else:
                    result = node.remoter.run("cat /proc/mounts")
                    if "/var/lib/scylla" not in result.stdout:
                        self.log.error("Scylla doesn't use an individual storage, skip enospc test")
                        continue

                    try:
                        with DbNodeLogger(self.cluster.nodes, "fill disk space", target_node=node):
                            self.actions_log.info(f"Filling disk space to reach enospc error on {node.name}")
                            reach_enospc_on_node(target_node=node)
                    finally:
                        with DbNodeLogger(self.cluster.nodes, "clean disk space", target_node=node):
                            self.actions_log.info(f"Cleaning disk space with scylla restart on {node.name}")
                            clean_enospc_on_node(target_node=node, sleep_time=sleep_time)
        # Bind mount is automatically restored here
```

**Dependencies:** Phase 1

**Deliverables:**
- Modified `disrupt_nodetool_enospc()` method
- Modified `disrupt_end_of_quota_nemesis()` method
- Integration tests with docker backend

### Phase 3: Ensure Root Partition Has Adequate Space
**Description:** Verify and document root partition size requirements for coredump storage.

**Definition of Done:**
- [ ] Document root partition size requirements in code comments
- [ ] Add validation check during node setup (optional warning)
- [ ] Update test configurations if needed

**Implementation Details:**
- Root partition should have at least 50GB free space
- With 10 core limit, compressed cores typically use 1-10GB each
- 50GB provides buffer for OS + logs + ~5 compressed cores

**Dependencies:** Phase 1, Phase 2

**Deliverables:**
- Documentation in code comments
- Optional: Validation check in node setup

### Phase 4: Testing and Validation
**Description:** Comprehensive testing across backends and scenarios.

**Definition of Done:**
- [ ] Unit tests for mount/unmount functions
- [ ] Integration test: Run out-of-space nemesis with docker backend
- [ ] Verify coredumps are saved during OOS nemesis
- [ ] Verify coredumps are restored to data partition after nemesis
- [ ] Test with real crash scenario (SIGQUIT) during OOS nemesis
- [ ] Regression test: Verify non-OOS tests still use bind mount

**Test Scenarios:**
1. **Unit Tests:**
   - `test_coredump_to_root_context_manager()` - verify mount/unmount cycle
   - `test_coredump_to_root_exception_handling()` - verify restoration on failure
   - `test_coredump_to_root_idempotency()` - verify safe to call multiple times
   - `test_coredump_to_root_no_mount()` - verify behavior when bind mount doesn't exist

2. **Integration Tests:**
   - Run `longevity_oos_test.py` with docker backend
   - Inject SIGQUIT during disk-full phase
   - Verify coredump is successfully saved to root partition
   - Verify bind mount is restored after nemesis completes

3. **Regression Tests:**
   - Run non-OOS longevity test
   - Verify bind mount is still present
   - Verify coredumps still work normally

**Dependencies:** All previous phases

**Deliverables:**
- Unit tests in `unit_tests/`
- Integration test results
- Documentation of test procedures

## 5. Testing Requirements

### Unit Tests (Phase 1)
**File:** `unit_tests/test_context_managers.py` (new or existing)
- Test `coredump_to_root()` context manager with mock node
- Test successful entry and exit (mount/unmount cycle)
- Test exception handling - ensure restoration on failure
- Test idempotency (safe when called multiple times)
- Test when bind mount doesn't exist (no-op scenario)

### Integration Tests (Phase 2, 4)
**Test Environment:** Docker backend (fastest iteration)
```bash
export SCT_SCYLLA_VERSION=2025.3.0
uv run sct.py run-test longevity_oos_test.LongevityOutOfSpaceTest.test_oos_write \
  --backend docker \
  --config test-cases/PR-provision-test.yaml
```

**Validation Steps:**
1. Check logs for "Switching coredump to root filesystem"
2. Verify no bind mount during disk fill phase: `mount | grep coredump`
3. Inject crash: `pkill -SIGQUIT scylla`
4. Verify coredump appears in `/var/lib/systemd/coredump`
5. After nemesis, verify bind mount is restored
6. Verify subsequent coredumps go to `/var/lib/scylla/coredump`

### Manual Testing
- Test on AWS backend with full longevity_oos_test
- Monitor disk usage during test
- Verify coredump collection works

## 6. Success Criteria

1. **Functional Requirements:**
   - ✅ Coredumps are successfully saved during out-of-space nemesis
   - ✅ Coredump mount is restored after nemesis completes
   - ✅ Non-OOS tests continue using bind mount (no regression)

2. **Quality Requirements:**
   - ✅ All unit tests pass
   - ✅ Integration tests pass on docker backend
   - ✅ Manual validation on AWS backend
   - ✅ Code review approval

3. **Documentation Requirements:**
   - ✅ Code comments explain the approach
   - ✅ This implementation plan is complete
   - ✅ Test procedures are documented

## 7. Risk Mitigation

### Risk 1: Mount/Unmount Operations Fail
**Impact:** Coredumps might not be saved, or mount might not be restored

**Mitigation:**
- Use try/finally blocks to ensure restoration
- Add retry logic for mount operations
- Log all operations for debugging
- Test extensively with docker backend first

### Risk 2: Kubernetes Backend Incompatibility
**Impact:** K8s might handle mounts differently

**Mitigation:**
- K8s already has different path (`_k8s_fake_enospc_error`)
- Document K8s-specific considerations
- Test with k8s-local-kind backend if needed

### Risk 3: Timing Issues with Systemd
**Impact:** Mount/unmount might not complete before next operation

**Mitigation:**
- Add sleep delays after mount/unmount (5 seconds)
- Check mount status before proceeding
- Use `systemctl is-active` to verify mount state

### Risk 4: Coredumps Lost During Transition
**Impact:** Crash during mount switch might lose coredump

**Mitigation:**
- Switch happens before filling disk (safe state)
- Restoration happens after cleaning disk (safe state)
- Document the safe windows for operations

### Risk 5: Root Partition Runs Out of Space
**Impact:** Coredumps still cannot be saved

**Mitigation:**
- Document minimum root partition size (50GB)
- Consider adding check during node setup
- Default root_disk_size_db is usually sufficient (varies by backend)

## Rollback Strategy

If implementation causes issues:
1. **Phase 1:** Simply don't integrate with nemesis (contained)
2. **Phase 2:** Revert nemesis changes, keep helper functions
3. **Complete rollback:** Remove all changes, back to losing coredumps (known issue)

## Open Questions

None - approach is clear based on systemd-coredump bind mount mechanism.

## Implementation Order

1. Phase 1 (1-2 days): Implement and test helper functions
2. Phase 2 (1-2 days): Integrate with nemesis
3. Phase 3 (1 day): Documentation and validation
4. Phase 4 (2-3 days): Comprehensive testing

**Total Estimated Time:** 5-8 days

## References

- Original issue: https://github.com/scylladb/scylla/issues/7085
- Related issue: https://github.com/scylladb/scylla/issues/4614
- Scylla coredump setup script: https://github.com/scylladb/scylladb/blob/master/dist/common/scripts/scylla_coredump_setup
- systemd-coredump documentation: `man systemd-coredump`
