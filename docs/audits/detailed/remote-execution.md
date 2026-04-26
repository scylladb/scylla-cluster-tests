# Audit Report: Remote Execution (`sdcm/remote/`)

**Date:** April 2025
**Severity Summary:** 3 Critical, 3 High, 4 Medium

## Critical Findings

### C1. Socket Leak in `RemoteCmdRunnerBase._init_socket()`

**File:** `sdcm/remote/remote_cmd_runner.py`
**Severity:** Critical
**Impact:** Leaked file descriptors accumulate over long test runs, eventually hitting OS limits

The method creates a `socket.socket()` object, then calls `sock.connect()`. If `connect()` raises an exception (timeout, connection refused), the socket is never closed. In long-running longevity tests with node failures, this accumulates leaked FDs.

**What to fix:** Wrap socket creation and connection in a try/except that closes the socket on failure, or use a context manager pattern.

---

### C2. Unbounded Memory in `SSHReaderThread`

**File:** `sdcm/remote/remote_cmd_runner.py`
**Severity:** Critical
**Impact:** Out-of-memory conditions during tests that produce large command output

`SSHReaderThread` reads SSH command output into a `BytesIO` buffer with no size limit. Commands that produce large output (e.g., `nodetool status` on large clusters, log dumps) can grow this buffer without bound. There's no backpressure mechanism or size cap.

**What to fix:** Add a configurable maximum buffer size. When exceeded, either truncate with a warning or stream to a temporary file.

---

### C3. API Key Exposure in `ssh_debug_cmd()`

**File:** `sdcm/remote/remote_cmd_runner.py`
**Severity:** Critical
**Impact:** API keys/credentials visible in logs and debug output

The `ssh_debug_cmd()` method constructs SSH debug commands that may include sensitive parameters (API keys, tokens) in the command string. These get logged at debug level and can appear in test result artifacts that are stored in shared locations.

**What to fix:** Sanitize or redact sensitive parameters before including them in debug command strings. Add a credential-scrubbing utility for log output.

---

## High Findings

### H1. SSH Threads Not Daemonized

**File:** `sdcm/remote/remote_cmd_runner.py`
**Severity:** High
**Impact:** Orphaned threads prevent clean process exit

SSH reader/writer threads are created without `daemon=True`. If the parent process crashes or the test framework exits abnormally, these threads keep the process alive indefinitely. This causes hung test processes that consume CI resources.

**What to fix:** Set `daemon=True` on all SSH helper threads, or implement explicit thread join with timeout during cleanup.

---

### H2. Missing Connection Pool Limits

**File:** `sdcm/remote/remote_cmd_runner.py`
**Severity:** High
**Impact:** Unbounded SSH connections to nodes under high concurrency

There's no limit on concurrent SSH connections to a single node. During parallel operations (e.g., multiple nemesis threads + monitoring + log collection), a single node can accumulate dozens of SSH connections, exhausting the node's `MaxSessions` limit.

**What to fix:** Add a per-node connection semaphore or connection pooling with a configurable max.

---

### H3. Inconsistent Error Recovery in `run()` Method

**File:** `sdcm/remote/remote_cmd_runner.py`
**Severity:** High
**Impact:** Silent failures during remote command execution

The `run()` method has multiple exception handlers with inconsistent behavior — some retry, some re-raise, some log-and-continue. The retry logic doesn't account for idempotency of the command being executed, risking repeated side effects.

**What to fix:** Standardize error recovery: classify exceptions as retryable vs. fatal, respect command idempotency markers, and ensure consistent logging.

---

## Medium Findings

### M1. Hardcoded Timeout Values

**File:** Multiple files in `sdcm/remote/`
**Severity:** Medium
**Impact:** Inflexible timeouts that don't adapt to operation type

Timeout values are scattered as magic numbers throughout the code (e.g., `timeout=300`, `timeout=3600`). Different operations need different timeouts, but there's no centralized timeout configuration or operation-type-based defaults.

### M2. Incomplete `__del__` Cleanup

**File:** `sdcm/remote/remote_cmd_runner.py`
**Severity:** Medium
**Impact:** Resource cleanup depends on garbage collector timing

`__del__` methods are used for cleanup, which is unreliable in Python (not guaranteed to run, order-dependent). Critical resources like SSH connections should use explicit cleanup methods or context managers.

### M3. Log Level Inconsistency

**File:** `sdcm/remote/remote_cmd_runner.py`
**Severity:** Medium
**Impact:** Debug noise in production, missing info in debug mode

Some error conditions log at `DEBUG` level while routine operations log at `INFO`. This makes it hard to diagnose issues without enabling full debug logging, which produces massive output.

### M4. No Circuit Breaker for Failing Nodes

**File:** `sdcm/remote/remote_cmd_runner.py`
**Severity:** Medium
**Impact:** Wasted time retrying connections to nodes that are known to be down

When a node is unreachable, every operation independently retries. There's no shared "this node is currently down" state that would let other operations fail fast. During nemesis operations that intentionally take nodes down, this causes unnecessary retry storms.
