# Kernel Panic Detection

A kernel panic takes a node down without warning: Scylla stops responding, SSH dies, and the node's own logs stop
before recording a cause. Because the node is dead, nothing on the node can report what happened — the evidence only
exists in the cloud provider's serial console.

SCT therefore polls each node's serial console from the *runner* side via cloud APIs, publishes a
`KernelPanicEvent` when it sees a panic, and saves every console dump as `console_output.log` so the boot and
runtime console is available in the collected logs even when no panic occurs.

## Supported Backends

| Backend | Checker class | Console API | Node coverage |
|---------|---------------|-------------|---------------|
| AWS | `AWSKernelPanicChecker` | `ec2.get_console_output(Latest=True)` | DB, loader, monitor |
| GCE | `GCPKernelPanicChecker` | `get_serial_port_output(port=1)` | DB, loader, monitor |
| Azure | `AzureKernelPanicChecker` | `retrieve_boot_diagnostics_data()` → serial console blob | DB, loader, monitor |
| OCI | `OCIKernelPanicChecker` | `capture_console_history` + instance lifecycle state | DB, loader, monitor |

All four are defined in [`sdcm/kernel_panic_checker.py`](../sdcm/kernel_panic_checker.py). Coverage is per *node
class*, not per role: the factory is overridden on `AWSNode`, `GCENode`, `AzureNode`, and `OciNode`, so every node
built on those — DB, loader, monitor, and specialisations like `CassandraAWSNode` or `VectorStoreAWSNode` — is
monitored. Backends without a serial console equivalent (Docker, Kubernetes, minicloud) have no checker: the base
`BaseNode._create_kernel_panic_checker()` returns `None` and monitoring is silently skipped.

## Configuration

One option, enabled by default:

```yaml
enable_kernel_panic_checker: true  # default, see defaults/test_default.yaml
```

```bash
export SCT_ENABLE_KERNEL_PANIC_CHECKER=false  # to disable
```

`configurations/minicloud.yaml` turns it off, since minicloud instances are local QEMU/KVM VMs with no cloud
console API to poll.

The polling interval and SSH thresholds are **not** configurable — they are class constants on
`BaseKernelPanicChecker`:

| Constant | Value | Meaning |
|----------|-------|---------|
| `CHECK_INTERVAL_SECONDS` | 30 | Seconds between console polls |
| `SSH_CHECK_PORT` | 22 | Port used for the reachability probe |
| `SSH_CONNECT_TIMEOUT` | 5 | Per-probe TCP connect timeout |
| `SSH_FAILURE_THRESHOLD` | 3 | Consecutive failures before `ssh_lost` is set |

## Architecture

### Class hierarchy

```
threading.Thread
└── BaseKernelPanicChecker          # polling loop, detection, event, file save
    ├── AWSKernelPanicChecker       # _get_console_output() per provider
    ├── GCPKernelPanicChecker
    ├── AzureKernelPanicChecker
    └── OCIKernelPanicChecker
```

Subclasses implement only two methods — `_get_console_output()` and `_get_instance_identifier()`. All detection,
event publishing, file writing, and suspend logic lives in the base class. Both are marked `@abstractmethod`, but
the base derives from `threading.Thread` rather than `ABC`, so nothing enforces them at instantiation — a subclass
that forgets one fails at the first poll instead.

The checker deliberately takes **plain strings, not a node object**: `node_name`, `host`, and `logdir`. This keeps
the checker usable without a live `BaseNode` (it must keep working when the node is dead) and keeps it trivially
unit-testable.

### Lifecycle

The checker is a daemon thread owned by the node, started and stopped by `BaseNode`:

| Stage | Location | What happens |
|-------|----------|--------------|
| Create | `_create_kernel_panic_checker()` per backend (`sdcm/cluster_aws.py`, `_gce`, `_azure`, `_oci`) | Builds the provider checker with instance id/name, `host=self.external_address`, `logdir=self.logdir` |
| Start | `BaseNode.init()` → `_start_kernel_panic_checker()` (`sdcm/cluster.py`) | Skipped entirely when `enable_kernel_panic_checker` is false |
| Poll | `BaseKernelPanicChecker.run()` | Loop below, every 30s |
| Stop | `BaseNode.destroy()` → `_stop_kernel_panic_checker()` | `stop()` attempts one final console fetch and save, then `join(timeout=5)` |

> **The final fetch usually fails in real teardown.** Every backend's `destroy()` terminates the instance *before*
> calling `super().destroy()`, which is where the checker is stopped. By then the console API has nothing to return,
> and the failure is swallowed at DEBUG level (`Failed to get final console output`). In practice the
> `console_output.log` you get from a run is **the last successful poll — up to 30 seconds before teardown**, not a
> fresh dump taken at teardown. The final fetch only does useful work when `stop()` is called while the instance is
> still alive, such as via the `with checker:` context manager.

### Polling loop

Each iteration:

1. **Suspended?** If so, sleep and skip the whole iteration (see [Reboot suppression](#reboot-suppression)).
2. **Fetch** the full console output via the provider API.
3. **Save** it to `<logdir>/console_output.log` if non-empty. The file is opened in `"w"` mode, so each poll
   **overwrites** it with the latest full dump — it is a snapshot, not an append log.
4. **Detect**: a case-insensitive substring test for `"kernel panic"` on the whole dump. That is the entire
   detection rule — there is no matching on `BUG:`, `Oops:`, or `Call Trace:`.
5. **Publish** on first match only: `KernelPanicEvent` is published, then the thread sets its stop event and
   exits. At most **one event per node per test run**.
6. **SSH probe**: a raw TCP connect to port 22, tracked as a secondary signal.

Because the interval is implemented as `self._stop_event.wait(CHECK_INTERVAL_SECONDS)`, `stop()` interrupts the
sleep immediately rather than waiting out the remaining 30 seconds.

### `KernelPanicEvent`

Defined in [`sdcm/sct_events/system.py`](../sdcm/sct_events/system.py) as an `InformationalEvent` with
`Severity.CRITICAL` (also registered in `defaults/severities.yaml`). A CRITICAL event makes the events analyzer
fail the test.

It carries exactly two fields, `node` and `message`. The matched panic lines are folded into `message`, joined with
` | ` — there is no separate `panic_output` field:

```
(KernelPanicEvent Severity.CRITICAL) period_type=one-time event_id=...:
  node=longevity-db-node-1 message=Kernel panic detected in console log for instance i-0abc123:
  [12345.678] Kernel panic - not syncing: Fatal exception
```

The full console dump is not put in the event. It is written to the SCT log at ERROR level and saved to
`console_output.log`.

### SSH connectivity signal

`_check_ssh_connectivity()` does a plain `socket.create_connection((host, 22), timeout=5)`. Failures are only
counted once the node has been reachable at least once, so a node that never finished booting does not accumulate
failures. After three consecutive failures the `ssh_lost` property becomes true and a warning is logged.

> **This signal is advisory only.** It does not publish a `KernelPanicEvent`, does not gate console-based
> detection, and `ssh_lost` currently has no consumers outside the unit tests. It exists as a breadcrumb in the SCT
> log for the case where the console API is unreliable. Do not rely on it to fail a test.

When `host` is unset, the probe returns "reachable" rather than counting a failure.

### OCI: lifecycle state as the primary signal

OCI is the one backend that does not rely on console text. With `kernel.panic=0` a panicking instance **halts
instead of rebooting**, and after the halt `capture_console_history` may return stale pre-crash content or fail
silently. (`kernel.panic=0` is not set by SCT provisioning — the integration test sets it explicitly to make the
panic observable, and OCI images may ship with it.)

So `OCIKernelPanicChecker` checks the instance lifecycle state *first* on every poll. A `STOPPED` or `STOPPING`
state short-circuits the console fetch and returns the synthetic string `"Kernel panic - instance stopped
unexpectedly"`, which then trips the normal substring check. Only when the state looks healthy does it fall back to
capturing console history (up to 1 MB), deleting each capture afterwards so they don't accumulate. The delete is
skipped if fetching the content itself raises, so a run with repeated content failures can leave console history
captures behind.

This is also why OCI is the backend most prone to false positives — any intentional stop/start looks identical to a
panic. Hence the suppression mechanism below.

### Reboot suppression

An intentional reboot looks exactly like a crash: the node goes away, SSH drops, and on OCI the instance reports
`STOPPED`. Without suppression, every nemesis reboot would raise a CRITICAL event and fail the test
(SCT-459, SCT-658).

`BaseKernelPanicChecker` therefore exposes `suspend()` / `resume()` and a `suspended()` context manager, and
`BaseNode` wraps both reboot paths in it:

```python
# sdcm/cluster.py — BaseNode.reboot(); restart() has the same shape around _restart_inner()
if self.kernel_panic_checker:
    with self.kernel_panic_checker.suspended():
        self._reboot_inner(hard, verify_ssh, uptime_changed)
else:
    self._reboot_inner(hard, verify_ssh, uptime_changed)
```

`restart()` is wrapped as well as `reboot()` because a backend is free to implement restart as a stop/start of the
underlying instance — `AWSNode._restart_inner()` does exactly that — which trips the OCI state heuristic and any
future state-based check. (GCE, Azure, and OCI currently implement `_restart_inner()` as a soft reboot.)

**Known gap:** `hard_reboot()` is only suppressed when reached through `reboot(hard=True)`. Calling a backend's
`hard_reboot()` directly bypasses the suppression window and can produce a false positive.

## Console Output Collection

`console_output.log` is written on the **runner**, not on the node — there is no remote copy to fetch, since the
data comes from cloud APIs. It lands at:

```
<base_logdir>/<node.name>/console_output.log
```

The log collector picks it up with `FileLog(name="console_output.log", search_locally=True)` — note the absence of
a `command=`, which is what makes it local-only. It is registered for all three cluster collectors in
`sdcm/logcollector.py`: `ScyllaLogCollector`, `LoaderLogCollector`, and `MonitorLogCollector`.

After a run, the file appears in the per-node directory of the cluster archive:

```
db-cluster-<test-id>.tar.zst
└── longevity-db-node-1/
    ├── console_output.log    ← serial console (kernel panic checker)
    ├── system.log            ← Scylla log
    ├── dmesg.log             ← kernel ring buffer, from inside the node
    └── ...
```

It is collected whether or not a panic happened, which makes it the first place to look for boot failures, kernel
warnings, or hardware errors. See [collected-logs.md](collected-logs.md) for the full archive layout.

## Troubleshooting

### `console_output.log` is missing or empty

Work through the write conditions in order — the file is only written when all of them hold:

1. **Checker disabled.** Confirm `enable_kernel_panic_checker` is true for the run. Grep the SCT log for
   `Started kernel panic monitoring for node` — one line per node. No lines at all means the option is off.
2. **Backend has no checker.** Docker, Kubernetes, and minicloud have no serial console; no file is expected.
3. **Console output was empty.** An empty dump is skipped rather than written as an empty file. Common early in a
   run before the provider has posted any console data.
4. **OCI could not resolve the instance OCID.** `OciNode._get_oci_instance_id()` returns `""` on
   `AttributeError`/`IndexError`, which makes the checker `None` and disables monitoring for that node. Grep for
   `Could not resolve OCI instance ID`.
5. **Azure boot diagnostics unavailable.** SCT enables boot diagnostics at provision time
   (`diagnosticsProfile.bootDiagnostics.enabled = true` in `sdcm/provision/azure/virtual_machine_provider.py`), but
   `ResourceNotFoundError` and `OperationNotAllowed` are swallowed and return an empty string. Look for
   `[Azure] Cannot retrieve boot diagnostics` at DEBUG level. Two more Azure paths return empty quietly: a missing
   `serial_console_log_blob_uri`, and a failed blob fetch (`[Azure] Error fetching serial console log`).
6. **Save failed.** Look for `Failed to save console output` — a warning, never fatal.
7. **API errors.** Per-poll failures are logged as `[<provider>] Error checking for kernel panic: ...` and do not
   kill the thread, so a transient API failure produces a stale file rather than a missing one. The one exception
   is an OCI `404`: `Instance ... not found, stopping monitoring` ends monitoring for that node permanently.

Two caveats worth knowing when the file exists but disappoints:

- **A node that never finished `init()` has no checker at all**, because the checker is started at the end of
  `init()`. A node that failed early in provisioning therefore has no `console_output.log` — exactly the case where
  you most want one. Use the backend's own diagnostics (for example Azure boot diagnostics screenshots) instead.
- **The file reflects the last successful poll, not teardown**, so its tail can be up to 30 seconds behind the
  node's real final state.

### A `KernelPanicEvent` fired but the node was fine (false positive)

Almost always an intentional reboot or stop/start that escaped the suppression window. Check, in order:

1. **Which backend?** OCI is by far the most likely, because it treats a `STOPPED`/`STOPPING` lifecycle state as a
   panic. Look for `[OCI] Instance ... is STOPPED — likely kernel panic`, and note the event message
   `Kernel panic - instance stopped unexpectedly`, which is *synthetic* — it means "instance halted", not
   "console said panic".
2. **Was the instance stopped on purpose?** Correlate the event timestamp with nemesis activity and with
   `Suspending kernel panic detection` / `Resuming kernel panic detection` at DEBUG level. A panic event
   *between* a suspend and its resume should be impossible; a panic event just *outside* that window points at a
   race with a slow instance-state transition.
3. **Did the code path go through `reboot()`/`restart()`?** Only those two are wrapped. A direct `hard_reboot()`
   call, or any new code that stops and starts an instance itself, needs to be wrapped in
   `with node.kernel_panic_checker.suspended():` — that is the fix, not a change to detection.
4. **Stale console content.** Detection reads the *whole* dump every poll, so a panic string from an earlier boot
   still present in the provider's buffer will match. This is the one false-positive class that is not
   reboot-related: check whether the matched line's timestamp predates the current boot.

### A kernel panic clearly happened but no event fired

1. **Already fired once.** Detection is one-shot per node: after the first event the thread exits. A second panic
   on the same node produces nothing. Check whether an earlier event exists for that node.
2. **The panic never reached the console.** Console text is the only signal on AWS/GCE/Azure. If the panic string
   was truncated out of the provider's buffer, or the panic happened too late for a final poll, detection misses
   it. Inspect `console_output.log` directly — the saved dump is exactly what detection saw.
3. **Wording mismatch.** Only the literal substring `"kernel panic"` matches. A hard lockup, an
   `Oops:`/`BUG:`/`Call Trace:` without a panic line, or an OOM kill does **not** trigger the checker by design.
   Those are visible in `console_output.log` and `dmesg.log` but will not fail the test.
4. **The node was destroyed first.** Teardown terminates the instance and then stops the checker, so a panic in
   the last polling window before teardown is never seen — and the final console fetch fails too, because the
   instance is already gone (see the note under [Lifecycle](#lifecycle)).
5. **Suspended at the wrong moment.** A real panic during an intentional reboot is invisible — that window
   suppresses detection unconditionally. This is a deliberate trade-off in favour of avoiding false positives.
6. **Monitoring stopped early (OCI).** An OCI `404` on any poll ends the thread for good — look for
   `Instance ... not found, stopping monitoring`. Everything after that point is unmonitored.

### Timing expectations

Console output is not real-time; expect a lag between the panic and the event on top of the 30s poll interval.

| Backend | Behaviour to expect |
|---------|---------------------|
| AWS | Console output is buffered and posted periodically; `Latest=True` returns the most recent output only. Very early boot panics can be truncated away. |
| GCE | Serial port output is capped at the most recent output; long-running nodes lose early boot lines. |
| Azure | Depends on the boot diagnostics blob being refreshed; the blob fetch has a 30s timeout. |
| OCI | Lifecycle state detection is fast; console history capture is asynchronous with a 30s wait and may time out. |

For a same-run reference of what a genuine detection looks like end to end, see the integration test below.

## Tests

| Test | Scope |
|------|-------|
| [`unit_tests/unit/test_kernel_panic_checker.py`](../unit_tests/unit/test_kernel_panic_checker.py) | Detection, line extraction, file save/overwrite semantics, SSH probe, thread lifecycle, suspend/resume. Uses a `FakeKernelPanicChecker` with scripted output — no cloud APIs. |
| [`unit_tests/integration/test_kernel_panic.py`](../unit_tests/integration/test_kernel_panic.py) | Provisions a real runner per backend (`aws`, `gce`, `azure`, `oci`), triggers a genuine panic via `echo c > /proc/sysrq-trigger`, and waits for `KernelPanicEvent`. |

The integration test is gated on both the `integration` marker and the `SCT_TEST_KERNEL_PANIC` environment
variable, since it provisions real cloud instances and intentionally crashes them:

```bash
SCT_TEST_KERNEL_PANIC=1 uv run sct.py integration-tests -t integration/test_kernel_panic.py
```

Run the unit tests with:

```bash
uv run sct.py unit-tests -t unit/test_kernel_panic_checker.py
```

Note the subdirectory in `-t`: both commands prefix the value with `unit_tests/`, so the path has to include
`unit/` or `integration/`.

## Adding a Checker for a New Backend

1. Subclass `BaseKernelPanicChecker` in `sdcm/kernel_panic_checker.py`, set `provider_name`, and implement
   `_get_console_output()` and `_get_instance_identifier()`.
2. Return `""` rather than raising for expected "not available yet" conditions — an empty dump is skipped cleanly,
   an exception is logged every 30 seconds.
3. Override `_create_kernel_panic_checker()` on the backend's node class, passing `node_name=self.name`,
   `host=self.external_address`, and `logdir=self.logdir`. Return `None` when the instance identifier cannot be
   resolved.
4. Import the provider SDK client inside the method if the module has a cyclic import with `sdcm.provision` — the
   existing GCE/Azure/OCI checkers do this and document why.
5. If the backend implements restart as a stop/start, make sure it goes through `BaseNode.restart()` so it inherits
   the suppression window.

## See Also

- [Collected Log Files](collected-logs.md) — where `console_output.log` ends up in the archives
- [SCT Events](sct-events.md) — event severities and how CRITICAL events fail a test
- [Configuration Options Reference](configuration_options.md) — the `enable_kernel_panic_checker` entry
