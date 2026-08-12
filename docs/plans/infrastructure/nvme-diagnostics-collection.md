---
status: draft
domain: cluster
created: 2026-06-28
last_updated: 2026-06-28
owner: juliayakovlev
---

# NVMe Diagnostics Collection for DB Nodes

## 1. Problem Statement

ScyllaDB runs on NVMe storage in production-like test environments (AWS i3/i4i, GCE local SSDs, bare metal). Currently, SCT has **no visibility into NVMe device health** during or after test runs. This means:

- **Silent disk degradation** — NVMe devices may report increasing `media_errors`, rising `percentage_used`, or critical temperature warnings that go undetected until they cause test failures.
- **No root cause for IO errors** — When Scylla reports IO errors or unexpected latency spikes, there is no NVMe-level telemetry to correlate with (the only related data is `dmesg.log`).
- **No proactive detection** — NVMe devices expose SMART health data, error logs, and self-test capabilities, but none of these are leveraged by the test framework.
- **Missing timestamps for errors** — When NVMe errors occur, there is no mechanism to capture exactly when they happened relative to test operations.

The `nvme-cli` tool (open-source NVMe management utility) provides the necessary commands: `nvme list`, `nvme smart-log`, `nvme error-log`, `nvme device-self-test`, and `nvme self-test-log`.

## 2. Current State

### Existing NVMe-Related Code

- **`sdcm/cluster.py` — `BaseNode.detect_disks()`** detects NVMe devices via `ls /dev/nvme*n*` pattern matching. Returns a list of device paths like `/dev/nvme0n1`.
- **`artifacts_test.py` — `verify_nvme_write_cache()`** reads `/sys/block/{device}/queue/write_cache` to verify write-back/write-through settings.
- **`sdcm/cluster_cassandra.py` — `BaseCassandraCluster._setup_data_device()`** — NVMe device detection for Cassandra node setup using `lsblk`.

### Package Installation

- **`sdcm/cluster.py` — `BaseNode.install_package()`** provides cross-distro package installation (apt/yum/dnf/zypper) with retry logic.
- **`sdcm/utils/apt.py`** — `apt_cmd()` builder with safe defaults for Debian-based distros.
- **`sdcm/utils/rpm.py`** — `rpm_cmd()` builder with lock-wait logic for RHEL-based distros.

### Log Collection Infrastructure

- **`sdcm/logcollector.py` — `CommandLog`** class: runs a command remotely, saves output to file. Used extensively by `ScyllaLogCollector`.
- **`sdcm/logcollector.py` — `ScyllaLogCollector.log_entities`** list: includes `CommandLog` entries for `cpu_info`, `mem_info`, `dmesg.log`, etc.
- **`sdcm/logcollector.py` — `collect_diagnostic_data()`**: mid-test diagnostic collection pattern using `CommandLog` + `collect_log_entities()`.
- **`sdcm/logcollector.py` — `collect_log_entities()`**: generic function to collect a list of log entities from a node.

### Health Checking

- **`sdcm/utils/health_checker.py`** — Health check functions that return `HealthEventsGenerator` (generators of `ClusterHealthValidatorEvent`). Pattern: each function yields events with severity levels.
- **`sdcm/cluster.py` — `BaseNode.node_health_events()`**: chains multiple health check generators.
- **`sdcm/cluster.py` — `BaseScyllaCluster.check_cluster_health()`**: iterates nodes and calls health checks.

### Failure Statistics

- **`sdcm/tester.py` — `ClusterTester.gather_failure_statistics()`**: collects nodetool output on test failure, saves to `self.logdir`.

### What Does NOT Exist

- No `nvme-cli` installation anywhere in the codebase.
- No SMART log reading or parsing.
- No NVMe error log collection.
- No NVMe self-test execution.
- No NVMe health event generation.
- No utility module for NVMe operations.

## 3. Goals

1. **Install `nvme-cli`** on all DB nodes during cluster setup, supporting both Debian and RHEL-based distros.
2. **Detect NVMe devices** using `nvme list` and identify which devices are relevant (data disks used by Scylla).
3. **Collect SMART logs** (`nvme smart-log <device> -H`) for each relevant device and parse key health indicators.
4. **Detect errors** from SMART data (`media_errors > 0`, `num_err_log_entries > 0`, critical warnings, high `percentage_used`).
5. **Collect error logs** (`nvme error-log <device>`) when errors are detected, with timestamp association.
6. **Run device self-tests** (`nvme device-self-test -s <type> <device>`, default short test type 1) and collect results (`nvme self-test-log <device> -v`). Extended tests (type 2) are opt-in via configuration.
7. **Integrate with SCT event system** — generate `ClusterHealthValidatorEvent` or a new event type when NVMe issues are detected.
8. **Integrate with log collection** — ensure NVMe diagnostics are captured in end-of-test log collection.
9. **Make the feature configurable** — allow enabling/disabling via SCT config parameter.

## 4. Implementation Phases

### Phase 1: NVMe Utility Module (Foundation)

**Importance:** Critical — all other phases depend on this.

**Deliverables:**
- New module `sdcm/utils/nvme.py` with the core NVMe diagnostics logic.
- Data classes for parsed SMART log, error log, and self-test results.
- Functions to:
  - Install `nvme-cli` (via `node.install_package()`).
  - List NVMe devices (`nvme list` with JSON output parsing).
  - Retrieve SMART log data (`nvme smart-log <device> -H`).
  - Retrieve error logs (`nvme error-log <device>`).
  - Trigger self-test (`nvme device-self-test -s <type> <device>`).
  - Retrieve self-test log (`nvme self-test-log <device> -v`).
  - Parse output into structured data.
- Detection of "relevant" devices: use `nvme list` JSON output as the primary discovery mechanism rather than `BaseNode.detect_disks()`. **Caveat:** `detect_disks(nvme=True)` raises `AssertionError` when no NVMe devices are found (see the `assert disks, "Failed to find disks!"` guard in `BaseNode.detect_disks()`), so it cannot be called safely on docker or EBS-only backends. The new module must implement its own device listing via `nvme list -o json` (which returns an empty list gracefully) and optionally cross-reference with `detect_disks()` only after confirming devices exist.

**Definition of Done:**
- [ ] Module created at `sdcm/utils/nvme.py`
- [ ] Data classes defined: `NvmeDevice`, `NvmeSmartLog`, `NvmeErrorLogEntry`, `NvmeSelfTestResult`
- [ ] All `nvme-cli` command wrappers implemented with proper error handling and timeouts
- [ ] Device discovery uses `nvme list -o json` and returns an empty list (not an error) when no NVMe devices are present
- [ ] SMART log parsing extracts: `critical_warning`, `temperature`, `available_spare`, `percentage_used`, `media_errors`, `num_err_log_entries`, `unsafe_shutdowns`, `power_on_hours`
- [ ] Error log parsing extracts individual entries with `error_count`, `status_field`, `lba`, `nsid`, `opcode`
- [ ] Self-test log parsing extracts: current operation status, completion percentage, individual result entries
- [ ] Device filtering logic identifies Scylla data disks (not boot/OS disks)
- [ ] Unit tests cover parsing logic with sample command outputs
- [ ] `uv run sct.py pre-commit` passes

**Dependencies:** None.

### Phase 2: SCT Configuration Parameter

**Importance:** High — enables feature toggle.

**Deliverables:**
- New boolean config parameter `collect_nvme_diagnostics` (default: `True` for cloud backends with NVMe storage, `False` for docker).
- Optional parameter `nvme_self_test_type` to control self-test type (1=short, 2=extended; default: 1).

**Definition of Done:**
- [ ] Parameter added to `sdcm/sct_config.py` with proper type and description
- [ ] Default value added to `defaults/test_default.yaml`
- [ ] Parameter documented with valid values and behavior description
- [ ] Unit test validates parameter loading
- [ ] `uv run sct.py pre-commit` passes

**Dependencies:** None (can run in parallel with Phase 1).

### Phase 3: Integration with Node Setup

**Importance:** High — installs `nvme-cli` so diagnostics can be collected.

**Deliverables:**
- Install `nvme-cli` during DB node setup (after node is ready but before stress starts).
- Graceful handling for nodes without NVMe devices (e.g., docker backend, EBS-only instances).
- Initial SMART log collection after setup to establish baseline.

**Definition of Done:**
- [ ] `nvme-cli` installed on DB nodes when `collect_nvme_diagnostics` is enabled
- [ ] Installation uses `BaseNode.install_package()` for cross-distro support
- [ ] Nodes without NVMe devices skip gracefully (log info, no error)
- [ ] Baseline SMART log collected and logged after node setup
- [ ] No impact on test setup time for nodes without NVMe (early exit)
- [ ] `uv run sct.py pre-commit` passes

**Dependencies:** Phase 1, Phase 2.

### Phase 4: SMART Log Collection and Error Detection

**Importance:** High — core diagnostic value.

**Deliverables:**
- Method on `BaseNode` (or called from health checks) to collect SMART logs from all relevant NVMe devices.
- Error detection logic with thresholds:
  - `critical_warning != 0` → CRITICAL
  - `media_errors > 0` → ERROR
  - `num_err_log_entries > 0` → WARNING (trigger error log collection)
  - `percentage_used > 90%` → WARNING
  - `available_spare < available_spare_threshold` → WARNING
  - `temperature` above critical threshold → WARNING
- When errors detected: automatically collect `nvme error-log` and save with timestamp.
- Integration with `ClusterHealthValidatorEvent` or a new `NvmeHealthEvent`.

**Definition of Done:**
- [ ] SMART log collection method callable per-node
- [ ] Error thresholds configurable (with sensible defaults)
- [ ] Error detection yields appropriate severity events
- [ ] Error log automatically collected when `media_errors > 0` or `num_err_log_entries > 0`
- [ ] Error log saved with timestamp in filename (e.g., `nvme_error_log_nvme0n1_20260628_143022.log`)
- [ ] Events integrate with existing SCT event publishing
- [ ] Unit tests cover all threshold conditions
- [ ] `uv run sct.py pre-commit` passes

**Dependencies:** Phase 1, Phase 3.

### Phase 5: Self-Test Execution and Result Collection

**Importance:** Medium — provides deeper diagnostics but not required for basic monitoring.

**Deliverables:**
- Ability to trigger NVMe self-test (short or extended) on demand.
- Poll self-test completion status.
- Collect and parse self-test log results.
- Report failures via events.
- Option to run self-test at end of test (post-workload, before teardown).

**Definition of Done:**
- [ ] Self-test trigger function with configurable test type (short=1, extended=2)
- [ ] Polling loop for self-test completion with timeout
- [ ] Self-test log collection and parsing after completion
- [ ] Failed self-test results generate WARNING/ERROR events
- [ ] Integration point in test teardown (optional, controlled by config)
- [ ] Documentation of self-test types and expected durations
- [ ] Unit tests cover self-test result parsing
- [ ] `uv run sct.py pre-commit` passes

**Dependencies:** Phase 1, Phase 3.

### Phase 6: End-of-Test Log Collection Integration

**Importance:** High — ensures NVMe data is always captured for post-mortem analysis.

**Deliverables:**
- Add NVMe diagnostic commands to `ScyllaLogCollector.log_entities` in `sdcm/logcollector.py`.
- Ensure `nvme list`, `nvme smart-log`, and (if errors exist) `nvme error-log` are collected during log collection.
- Conditional collection: only if `nvme-cli` is installed and NVMe devices exist.

**Definition of Done:**
- [ ] `CommandLog` entries added to `ScyllaLogCollector` for NVMe diagnostics
- [ ] Collection is conditional (check if `nvme` command exists before running)
- [ ] SMART log collected for all NVMe devices
- [ ] Error log collected only when needed (errors detected)
- [ ] Self-test log collected if a self-test was run
- [ ] Log filenames are descriptive (include device name)
- [ ] No failures if `nvme-cli` is not installed (graceful skip)
- [ ] `uv run sct.py pre-commit` passes

**Dependencies:** Phase 1.

### Phase 7: Periodic Collection During Long Tests

**Importance:** Medium — valuable for longevity tests to track degradation over time.

**Deliverables:**
- Periodic SMART log collection during long-running tests (e.g., every 1 hour).
- Track changes in key metrics over time (detect degradation trends).
- Log warnings if metrics change significantly between collections.

**Definition of Done:**
- [ ] Periodic collection integrated (reuse existing periodic health check mechanism or add new timer)
- [ ] Collection interval configurable via parameter (default: 3600 seconds)
- [ ] Delta detection: warn if `media_errors` increases between checks
- [ ] Delta detection: warn if `percentage_used` increases significantly
- [ ] Results saved with timestamps for post-mortem timeline correlation
- [ ] Minimal performance impact (commands are fast, run sequentially per node)
- [ ] `uv run sct.py pre-commit` passes

**Dependencies:** Phase 4.

### Phase 8: Documentation Update

**Importance:** Low — can be done last.

**Deliverables:**
- Update `docs/sct-configuration.md` with new parameters.
- Add section to developer docs explaining NVMe diagnostics feature.
- Document how to interpret NVMe SMART data in test results.

**Definition of Done:**
- [ ] Configuration parameters documented
- [ ] Feature usage documented
- [ ] SMART log field meanings documented for SCT users
- [ ] `uv run sct.py pre-commit` passes

**Dependencies:** All previous phases.

## 5. Testing Requirements

### Unit Tests

| Test | Phase | Location |
|------|-------|----------|
| SMART log output parsing (various formats) | 1 | `unit_tests/unit/test_nvme.py` |
| Error log output parsing | 1 | `unit_tests/unit/test_nvme.py` |
| Self-test log output parsing | 1 | `unit_tests/unit/test_nvme.py` |
| Device list parsing (`nvme list` JSON output) | 1 | `unit_tests/unit/test_nvme.py` |
| Device filtering (relevant vs. OS disks) | 1 | `unit_tests/unit/test_nvme.py` |
| Error threshold detection | 4 | `unit_tests/unit/test_nvme.py` |
| Config parameter loading | 2 | `unit_tests/unit/test_config.py` (existing) |
| Graceful handling when no NVMe devices | 3 | `unit_tests/unit/test_nvme.py` |
| Self-test completion polling logic | 5 | `unit_tests/unit/test_nvme.py` |

### Integration Tests

| Test | Phase | Notes |
|------|-------|-------|
| Install `nvme-cli` on Docker Scylla node | 3 | Requires docker fixture; verify command exists after install |
| Run `nvme list` on node (expect empty on docker) | 3 | Verify graceful handling when no NVMe devices present |

### Manual Testing

| Test | Phase | Backend |
|------|-------|---------|
| Full flow on AWS i3/i4i instance | 4-6 | AWS with local NVMe storage |
| Verify SMART log collection in test results | 6 | AWS |
| Verify self-test execution and log | 5 | AWS |
| Verify periodic collection during longevity test | 7 | AWS |

## 6. Success Criteria

- NVMe SMART data is automatically collected for all DB nodes with NVMe storage at end of test.
- Any NVMe error (`media_errors > 0`, `critical_warning != 0`) generates a visible event in test results.
- Error logs are collected with timestamps when errors are detected.
- Self-tests can be triggered and results collected on demand.
- Feature has zero impact on backends without NVMe (docker, EBS-only).
- All parsing logic has >90% unit test coverage.
- No test setup time increase >5 seconds from `nvme-cli` installation.

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| `nvme-cli` not available in distro package repos | Low | Medium | Use `install_package()` with `ignore_status=True`; skip gracefully if unavailable |
| `nvme-cli` output format varies between versions | Medium | Medium | Parse with flexible regex; use JSON output (`-o json`) where supported; unit test with multiple format samples |
| Self-test blocks device IO or impacts performance | Low | High | Only run short self-tests (type 1) by default; run at end of test, not during workload; make configurable |
| Extended self-test takes hours to complete | Medium | Low | Default to short self-test (type 1, ~2 min); extended (type 2) opt-in only |
| Permission issues running `nvme` commands | Low | Medium | All commands use `remoter.sudo()`; verify root access in setup |
| NVMe devices not present on all backends | Expected | None | Guard all operations with device presence check; skip silently on docker/EBS |
| Adding log collection increases teardown time | Low | Low | NVMe commands are fast (<1s each); run in parallel across devices |
| `nvme error-log` output very large on degraded drives | Low | Medium | Limit error log collection to last N entries (e.g., 64); truncate if needed |
| Self-test not supported on all NVMe controllers | Medium | Low | Catch errors from `device-self-test` command; log warning and skip |
| Rate-limiting of SMART log reads by NVMe controller | Very Low | Low | Collect at most once per hour during periodic checks |
