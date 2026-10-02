# Kernel Panic Detection

SCT includes automatic kernel panic detection for cloud-based test clusters. When enabled, a background thread polls the serial console output of each node and publishes a `KernelPanicEvent` if a kernel panic is detected.

## Supported Backends

| Backend | API Used | Coverage |
|---------|----------|----------|
| AWS | `get_console_output` | DB, Loader, Monitor nodes |
| GCE | `getSerialPortOutput` (port 1) | DB, Loader, Monitor nodes |
| Azure | Boot diagnostics serial console blob | DB, Loader, Monitor nodes |
| OCI | `capture_console_history` + lifecycle state | DB, Loader, Monitor nodes |

## Configuration

The feature is controlled by a single configuration option:

```yaml
enable_kernel_panic_checker: true  # default
```

To disable:

```yaml
enable_kernel_panic_checker: false
```

Or via environment variable:

```bash
export SCT_ENABLE_KERNEL_PANIC_CHECKER=false
```

## How It Works

1. **Polling**: Every 30 seconds, the checker fetches the full serial console output for each node.
2. **Detection**: Scans for patterns like `Kernel panic`, `BUG:`, `Oops:`, `Call Trace:` in the output.
3. **SSH Verification**: Monitors SSH reachability (TCP port 22). If a node was previously reachable and becomes unreachable for 3+ consecutive checks, it's flagged as potentially crashed. For db nodes this is advisory only (a warning in the SCT log), since nemeses take them down on purpose.
4. **Event**: On detection, a `KernelPanicEvent` (severity: `CRITICAL`) is published, which triggers test failure via the events analyzer.
5. **Console Log Saved**: The full serial console output is saved to `console_output.log` in the node's log directory on every poll cycle.

### Unreachable loader and monitor nodes

Loaders and monitors are never disrupted by a nemesis, so one that stops answering is dead. Every stress command that picks a dead loader afterwards only hangs on SSH timeouts, and so do the nemeses that reconfigure monitoring after a topology change or drive Scylla Manager, which runs on the monitor node.

`BaseNode._start_kernel_panic_checker()` sets `fail_on_ssh_lost` on the checker of every loader and monitor node. After `SSH_LOST_CRITICAL_THRESHOLD` (10) consecutive failed probes, which is about 5 minutes, the checker publishes a CRITICAL `NodeUnreachableEvent` and exits. A console-detected panic in the same poll wins and publishes `KernelPanicEvent` instead. `stop_task_threads()` clears `fail_on_ssh_lost` because teardown terminates the instance before the checker is stopped. Intentional reboots are covered by the same suspension as panics.

```
(NodeUnreachableEvent Severity.CRITICAL) period_type=one-time event_id=...:
  node=longevity-loader-node-1 message=SSH port 22 on 10.12.9.198 is unreachable for 10 consecutive checks
  (at least 300s), the node is considered dead
```

## Console Output Collection

The `console_output.log` file is automatically collected by the log collector at test teardown and included in the node's log archive uploaded to Argus/S3. This means you can inspect the full boot and runtime serial console even when no panic occurs — useful for diagnosing boot failures, kernel warnings, or hardware errors.

## Reading the Results

### In Argus

After a test run, download the `db-cluster-<test-id>.tar.zst` archive. Each node directory contains:

```
pr-provision-test-pr-13354-db-node-<id>-0-1/
├── console_output.log    ← Serial console (kernel panic checker)
├── system.log            ← Scylla log
├── messages.log          ← /var/log/messages
├── dmesg.log             ← Kernel ring buffer
└── ...
```

### In Test Events

When a panic is detected, you'll see in the test events:

```
(KernelPanicEvent Severity.CRITICAL): node=Node db-node-xxx
  message=Kernel panic detected in console output
  panic_output=<relevant panic lines>
```

## Limitations

- **AWS**: Console output has a ~60-second delay from instance. Output may be truncated for very early boot panics.
- **GCE**: Serial port output is limited to the last 1MB.
- **Azure**: Requires boot diagnostics to be enabled on the VM (SCT enables this automatically).
- **OCI**: Console history capture is asynchronous and may take a few seconds to become available.
- **Docker backend**: Not supported (no serial console equivalent).
