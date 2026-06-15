# Hang-Proof Shutdown — Minimal Fixes

## TL;DR

> **Quick Summary**: Fix post-test hangs by surgically addressing 4 specific deadlock/timeout points in event process and node thread shutdown. No architectural rewrites.
>
> **Deliverables**:
> - `EventsDevice.stop()` — never hangs on queue feeder thread
> - `BaseEventsProcess.stop()` — escalates to kill instead of asserting
> - `EventsDevice.run()` — bounded drain loop (5s max after stop signal)
> - `_coredump_thread.join()` — reduced from 3600s to 300s
>
> **Estimated Effort**: Medium (4 independent commits, each <20 lines)
> **Parallel Execution**: YES — 4 independent tasks, all Wave 1
> **Critical Path**: None — all fixes are independent

---

## Context

### Original Request
Fix post-test hang that blocks minicloud test runs for 30+ minutes. The test completes successfully but teardown never finishes.

### Root Cause Analysis
The hang occurs at `EventsDevice.stop()` → `self._queue.join_thread()`. After the EventsDevice child process exits, the parent calls `join_thread()` which waits for the queue's background feeder thread to flush its buffer to the pipe. Since the consumer (child) is dead, the pipe is full, the feeder blocks, and the parent blocks forever.

Secondary hang points that can bite in different scenarios:
- `_coredump_thread.join(3600)` — 1-hour timeout for daemon thread
- `BaseEventsProcess.stop()` — `assert False` crashes instead of escalating
- `EventsDevice.run()` — `while _running.is_set() or not queue.empty()` is unbounded

### Metis Review
**Identified Gaps** (addressed):
- Validate `cancel_join_thread()` works on already-exited process — safe per Python docs
- Coredump timeout: 300s chosen as safe middle ground (real collections take <60s typically)
- Assert removal: verified nothing catches AssertionError upstream as signal
- Multiple stop() calls: `_running.clear()` is idempotent, safe to call twice

---

## Work Objectives

### Core Objective
Eliminate all unbounded waits in the test teardown path so shutdown completes within 60 seconds in common cases.

### Concrete Deliverables
- Modified `sdcm/sct_events/events_device.py` — stop() and run() methods
- Modified `sdcm/sct_events/events_processes.py` — BaseEventsProcess.stop()
- Modified `sdcm/cluster.py` — _coredump_thread join timeout

### Definition of Done
- [ ] `uv run sct.py run-test ... --backend docker` teardown completes within 30s
- [ ] `uv run sct.py unit-tests` passes (no regressions)
- [ ] No process left alive after teardown (verified via `ps aux | grep sct`)

### Must Have
- Each fix independently revertable (separate commits)
- Events generated before stop() are still delivered (best-effort within drain window)
- No change to process start/initialization paths
- Error logged when force-kill is needed

### Must NOT Have (Guardrails)
- No changes to `suppress_interrupt()` implementation
- No parallelization of process stopping sequence
- No new classes (ShutdownDeadline etc) — pure inline fixes
- No changes to event delivery contract during normal operation
- No touching queue buffer sizes or pipe configuration
- No adding configuration parameters for timeouts (hardcode sensible values)

---

## Verification Strategy

> **ZERO HUMAN INTERVENTION** — ALL verification is agent-executed.

### Test Decision
- **Infrastructure exists**: YES
- **Automated tests**: Tests-after (verify existing unit tests pass, no new test files)
- **Framework**: pytest via `uv run sct.py unit-tests`

### QA Policy
Primary verification: run a docker-backend test and confirm teardown completes.
Evidence saved to `.sisyphus/evidence/task-{N}-{scenario-slug}.{ext}`.

---

## Execution Strategy

### Parallel Execution Waves

```
Wave 1 (All independent — run in parallel):
├── Task 1: Fix EventsDevice.stop() queue cleanup [quick]
├── Task 2: Fix BaseEventsProcess.stop() assert→kill [quick]
├── Task 3: Bound EventsDevice.run() drain loop [quick]
└── Task 4: Reduce _coredump_thread.join() timeout [quick]

Wave FINAL (After ALL tasks):
├── Task F1: Run unit tests — confirm no regressions
├── Task F2: Run docker backend test — confirm teardown completes
└── Task F3: Verify no processes left alive after test
```

### Dependency Matrix

| Task | Depends On | Blocks |
|------|-----------|--------|
| 1 | None | F1, F2, F3 |
| 2 | None | F1, F2, F3 |
| 3 | None | F1, F2, F3 |
| 4 | None | F1, F2, F3 |

### Agent Dispatch Summary

- **Wave 1**: 4 tasks → all `quick`
- **Wave FINAL**: 3 tasks → `quick`

---

## TODOs

- [x] 1. Fix EventsDevice.stop() — eliminate queue.join_thread() hang

  **What to do**:
  - In `sdcm/sct_events/events_device.py`, method `stop()` (line 91), which currently is:
    ```python
    def stop(self, timeout: Optional[float] = None) -> None:
        self._running.clear()
        self.join(timeout)
    ```
  - Expand it to:
    1. After `self.join(timeout)`, check `super().is_alive()` (NOT `self.is_alive()` — that method is overridden to return `self._running.is_set()` which is always False after step 1)
    2. If alive: log warning, call `self.kill()`, then `self.join(5)`
    3. After process is dead: call `self._queue.cancel_join_thread()` then `self._queue.close()`
  - This prevents the parent's queue feeder thread from blocking forever on a full pipe

  **IMPORTANT**: `EventsDevice.is_alive()` (line 214) is overridden to return `self._running.is_set()`. After `_running.clear()`, it ALWAYS returns False regardless of actual process state. MUST use `super().is_alive()` or `multiprocessing.Process.is_alive(self)` for real liveness check.

  **Must NOT do**:
  - Do not change the run() method in this task
  - Do not change how _running Event is used
  - Do not add new parameters or classes

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 2, 3, 4)
  - **Blocks**: F1, F2, F3
  - **Blocked By**: None

  **References**:
  - `sdcm/sct_events/events_device.py:91-93` — current stop() implementation (only `_running.clear()` + `self.join(timeout)`, no queue cleanup)
  - `sdcm/sct_events/events_device.py:214-215` — `is_alive()` override returns `self._running.is_set()`
  - Python docs: `multiprocessing.Queue.cancel_join_thread()` — prevents parent from waiting for feeder thread flush

  **Acceptance Criteria**:
  - [ ] `stop()` no longer calls `join_thread()`
  - [ ] `stop()` calls `cancel_join_thread()` before `close()`
  - [ ] `stop()` escalates to `self.kill()` if process alive after join(timeout)

  **QA Scenarios**:
  ```
  Scenario: EventsDevice stops without hanging
    Tool: Bash
    Preconditions: SCT installed, venv active
    Steps:
      1. Run: timeout 60 uv run python -c "
         import tempfile, time
         from sdcm.sct_events.events_device import EventsDevice
         from sdcm.sct_events.events_processes import EventsProcessesRegistry
         r = EventsProcessesRegistry(log_dir=tempfile.mkdtemp())
         d = EventsDevice(r)
         d.start()
         time.sleep(2)
         d.stop(timeout=5)
         print('SUCCESS')
         "
      2. Assert output contains "SUCCESS"
      3. Assert exit code is 0 (not killed by timeout)
    Expected Result: Process exits cleanly within 5s, prints SUCCESS
    Evidence: .sisyphus/evidence/task-1-events-device-stop.txt
  ```

  **Commit**: YES
  - Message: `fix(events): prevent hang on queue.join_thread() during shutdown`
  - Files: `sdcm/sct_events/events_device.py`

- [x] 2. Fix BaseEventsProcess.stop() — escalate instead of assert

  **What to do**:
  - In `sdcm/sct_events/events_processes.py`, method `stop()` (line 96):
    - Replace `assert False, f"Events process {name} is still alive after timeout"` with:
      - `LOGGER.error(...)`
      - If process has `kill` method (multiprocessing.Process): call `self.kill()` then `self.join(2)`
      - If thread (no kill): log warning and continue (daemon threads die with process)

  **Must NOT do**:
  - Do not change terminate() method
  - Do not change the stop_event mechanism
  - Do not add retry loops

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 3, 4)
  - **Blocks**: F1, F2, F3
  - **Blocked By**: None

  **References**:
  - `sdcm/sct_events/events_processes.py:96-106` — current stop() with assert False
  - `sdcm/sct_events/events_processes.py:65` — stop_event is either multiprocessing.Event or threading.Event

  **Acceptance Criteria**:
  - [ ] No `assert False` in stop() method
  - [ ] Process/thread alive after timeout → logged as error, not crash
  - [ ] multiprocessing.Process subclasses get kill() escalation
  - [ ] Unit tests pass: `uv run python -m pytest unit_tests/ -x -q --no-header -k "event" -n0`

  **QA Scenarios**:
  ```
  Scenario: Process stop timeout does not crash framework
    Tool: Bash
    Steps:
      1. Run: uv run python -m pytest unit_tests/ -x -q --no-header -k "event" -n0
      2. Assert exit code is 0
    Expected Result: All event-related tests pass without AssertionError
    Evidence: .sisyphus/evidence/task-2-no-assert-crash.txt
  ```

  **Commit**: YES
  - Message: `fix(events): escalate to kill instead of assert on process stop timeout`
  - Files: `sdcm/sct_events/events_processes.py`

- [x] 3. Bound EventsDevice.run() drain loop

  **What to do**:
  - In `sdcm/sct_events/events_device.py`, method `run()` (line ~115):
    - Change loop condition from `while self._running.is_set() or not self._queue.empty():`
    - To a bounded drain: when `_running` is cleared, set a drain deadline (5 seconds from now). Exit loop when deadline expires OR queue is empty.
  - Implementation:
    ```python
    drain_deadline = None
    while True:
        if not self._running.is_set():
            if drain_deadline is None:
                drain_deadline = time.monotonic() + 5.0
            if time.monotonic() >= drain_deadline:
                break
        try:
            event = self._queue.get(timeout=self.pub_queue_wait_timeout)
        except queue.Empty:
            if not self._running.is_set():
                break
            continue
        # ... existing publish logic ...
    ```

  **Must NOT do**:
  - Do not remove ZMQ pub/sub verification (that's future work)
  - Do not change suppress_interrupt()
  - Do not change queue.get timeout value

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 2, 4)
  - **Blocks**: F1, F2, F3
  - **Blocked By**: None

  **References**:
  - `sdcm/sct_events/events_device.py:113-131` — current run() loop with unbounded drain
  - `sdcm/sct_events/events_device.py:48` — PUB_QUEUE_WAIT_TIMEOUT = 1 second

  **Acceptance Criteria**:
  - [ ] Loop exits within 5s of `_running.clear()` even if queue has items
  - [ ] Loop still drains available events during the 5s window
  - [ ] Loop exits immediately on empty queue after `_running.clear()`

  **QA Scenarios**:
  ```
  Scenario: EventsDevice exits within bounded time after stop
    Tool: Bash
    Steps:
      1. Run: timeout 30 uv run python -c "
         import tempfile, time
         from sdcm.sct_events.events_device import EventsDevice
         from sdcm.sct_events.events_processes import EventsProcessesRegistry
         r = EventsProcessesRegistry(log_dir=tempfile.mkdtemp())
         d = EventsDevice(r)
         d.start()
         time.sleep(2)
         # Stuff queue with items that won't be consumed
         for i in range(100):
             try: d._queue.put(b'fake_event', timeout=0.1)
             except: pass
         start = time.monotonic()
         d.stop(timeout=10)
         elapsed = time.monotonic() - start
         print(f'ELAPSED={elapsed:.1f}')
         assert elapsed < 12, f'Took too long: {elapsed}'
         print('SUCCESS')
         "
      2. Assert output contains "SUCCESS"
      3. Assert ELAPSED < 12 seconds
    Expected Result: Stop completes within ~10s (5s drain + 5s join margin)
    Evidence: .sisyphus/evidence/task-3-bounded-drain.txt
  ```

  **Commit**: YES
  - Message: `fix(events): bound EventsDevice drain loop to 5 seconds`
  - Files: `sdcm/sct_events/events_device.py`

- [x] 4. Reduce _coredump_thread.join() timeout

  **What to do**:
  - In `sdcm/cluster.py`, method `wait_till_tasks_threads_are_stopped()` (line ~1626):
    - Change `self._coredump_thread.join(60 * 60)` to `self._coredump_thread.join(300)`
    - Add a warning log if thread is still alive after join: `LOGGER.warning("Coredump thread still alive after 300s, abandoning")`

  **Must NOT do**:
  - Do not change coredump thread's internal logic
  - Do not change daemon flag
  - Do not change other thread joins in same method

  **Recommended Agent Profile**:
  - **Category**: `quick`
  - **Skills**: []

  **Parallelization**:
  - **Can Run In Parallel**: YES
  - **Parallel Group**: Wave 1 (with Tasks 1, 2, 3)
  - **Blocks**: F1, F2, F3
  - **Blocked By**: None

  **References**:
  - `sdcm/cluster.py:1626` — `self._coredump_thread.join(60 * 60)`
  - `sdcm/coredump.py:109` — thread has `daemon=True`

  **Acceptance Criteria**:
  - [ ] Join timeout is 300 (not 3600)
  - [ ] Warning logged if thread alive after join
  - [ ] No other changes in the method

  **QA Scenarios**:
  ```
  Scenario: Coredump timeout is reduced
    Tool: Bash
    Steps:
      1. Run: grep -n "join(60 \* 60)" sdcm/cluster.py
      2. Assert: no output (old pattern removed)
      3. Run: grep -n "join(300)" sdcm/cluster.py
      4. Assert: matches found
    Expected Result: Old 3600s timeout replaced with 300s
    Evidence: .sisyphus/evidence/task-4-coredump-timeout.txt
  ```

  **Commit**: YES
  - Message: `fix(cluster): reduce coredump thread join timeout from 3600s to 300s`
  - Files: `sdcm/cluster.py`

---

## Final Verification Wave

- [x] F1. **Unit Tests**
  ```
  Scenario: All unit tests pass after shutdown fixes
    Tool: Bash
    Steps:
      1. Run: set -o pipefail; uv run sct.py unit-tests 2>&1 | tee /tmp/sct-unit-test-output.txt; echo "EXIT=$?"
      2. Assert: EXIT=0 in output
      3. Assert: output contains "passed" and does not contain "FAILED"
    Expected Result: All existing unit tests pass, no regressions
    Evidence: .sisyphus/evidence/F1-unit-tests.txt
  ```

- [x] F2. **Docker Backend Teardown Timing**
  ```
  Scenario: Docker backend test teardown completes within 30s
    Tool: Bash
    Preconditions: Docker running, SCT_SCYLLA_VERSION set
    Steps:
      1. Run test with teardown timing:
         set -o pipefail
         timeout 300 uv run sct.py run-test longevity_test.LongevityTest.test_custom_time \
           --backend docker --config test-cases/PR-provision-test-docker.yaml 2>&1 | \
           tee /tmp/sct-teardown-test.log
         TEST_EXIT=$?
      2. Measure teardown duration from log timestamps:
         TEARDOWN_START=$(grep -m1 "Stop all events consumers" /tmp/sct-teardown-test.log | grep -oP '\d{2}:\d{2}:\d{2}' | head -1)
         TEARDOWN_END=$(grep -m1 "All events consumers stopped" /tmp/sct-teardown-test.log | grep -oP '\d{2}:\d{2}:\d{2}' | head -1)
         START_SEC=$(date -d "$TEARDOWN_START" +%s 2>/dev/null || echo 0)
         END_SEC=$(date -d "$TEARDOWN_END" +%s 2>/dev/null || echo 0)
         ELAPSED=$((END_SEC - START_SEC))
         echo "TEARDOWN_ELAPSED=${ELAPSED}s"
      3. Assert: TEST_EXIT is 0 or expected test failure (NOT 124 which means timeout killed it)
      4. Assert: ELAPSED < 30 (teardown_elapsed seconds)
      5. Assert: no "EventsDevice" processes remain: ps aux | grep -i events_device | grep -v grep
    Expected Result: Teardown phase itself takes < 30s, no hanging processes
    Evidence: .sisyphus/evidence/F2-docker-teardown.txt
  ```

- [x] F3. **No Zombie Processes**
  ```
  Scenario: No SCT processes remain after test completion
    Tool: Bash
    Steps:
      1. Wait 5 seconds after F2 completes
      2. Run: ps aux | grep -E "sct|events_device|EventsDevice" | grep -v grep | grep -v "ps aux"
      3. Assert: output is empty (no orphan processes)
    Expected Result: All SCT processes have exited
    Evidence: .sisyphus/evidence/F3-no-zombies.txt
  ```

---

## Commit Strategy

Each task = one commit. All can land independently or together.

- Task 1: `fix(events): prevent hang on queue.join_thread() during shutdown`
- Task 2: `fix(events): escalate to kill instead of assert on process stop timeout`
- Task 3: `fix(events): bound EventsDevice drain loop to 5 seconds`
- Task 4: `fix(cluster): reduce coredump thread join timeout from 3600s to 300s`

---

## Success Criteria

### Verification Commands
```bash
uv run sct.py unit-tests  # Expected: all pass
# Docker backend test teardown < 30s (measured via timestamps in log)
```

### Final Checklist
- [ ] Teardown completes within 30s (docker backend)
- [ ] No assert crashes during shutdown
- [ ] Events from test execution present in final log
- [ ] No zombie processes after exit
- [ ] Each commit independently revertable

---

## Future Work (Document Only — NOT Implementation Tasks)

These are larger changes identified during analysis that would further improve shutdown robustness but are out of scope for this minimal fix:

### 1. ShutdownDeadline Class
A single budget object passed through teardown phases instead of independent timeouts:
```python
class ShutdownDeadline:
    def __init__(self, seconds):
        self._end = time.monotonic() + seconds
    def remaining(self, cap=None):
        r = max(0.0, self._end - time.monotonic())
        return min(r, cap) if cap else r
    @property
    def expired(self):
        return time.monotonic() >= self._end
```
**Why deferred**: Requires threading deadline object through tester.py → cluster.py → setup.py. Large surface area.

### 2. Parallel Event Process Shutdown
Stop all 11 event processes concurrently instead of sequentially:
```python
# Signal all, then wait all, then kill survivors
for proc in alive: proc.terminate()
for proc in alive: proc.join(timeout=shared_deadline)
for proc in still_alive: proc.kill()
```
**Why deferred**: Requires understanding inter-process dependencies in shutdown order. Currently EVENTS_MAIN_DEVICE_ID is stopped last for a reason (other processes may still write to it).

### 3. Replace suppress_interrupt() with Signal Handlers
Install proper SIGTERM handler in child processes that sets `_running.clear()` instead of suppressing:
```python
def handle_shutdown(signum, frame):
    self._running.clear()
signal.signal(signal.SIGTERM, handle_shutdown)
```
**Why deferred**: `suppress_interrupt()` is used in multiple event process types. Need to audit all usages and verify no event corruption during signal delivery.

### 4. Remove ZMQ Delivery Verification
The loopback pub/sub verification in EventsDevice.run() adds complexity and potential hangs:
```python
# Current: publish → subscribe → verify loopback
# Proposed: just publish, trust ZMQ in-process delivery
```
**Why deferred**: Need to understand why verification was added. May be covering a real reliability issue.

### 5. Global Teardown Timeout Watchdog
A top-level thread that force-kills the entire process if teardown exceeds a hard limit:
```python
threading.Timer(300, lambda: os._exit(1)).start()
```
**Why deferred**: Extreme measure. Should only be needed if all other fixes fail. Also prevents proper resource cleanup.

### 6. Nemesis Stop Timeout Reduction
`stop_nemesis(timeout=1800)` (30 min) is excessive. Should be 30-60s with force-kill:
**Why deferred**: Nemesis operations may legitimately need time to unwind (e.g., node rebuild). Need to audit all nemesis types for safe interrupt points.
