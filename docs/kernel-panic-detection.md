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
3. **SSH Verification**: Monitors SSH reachability (TCP port 22). If a node was previously reachable and becomes unreachable for 3+ consecutive checks, it's flagged as potentially crashed.
4. **Event**: On detection, a `KernelPanicEvent` (severity: `CRITICAL`) is published, which triggers test failure via the events analyzer.
5. **Console Log Saved**: The full serial console output is saved to `console_output.log` in the node's log directory on every poll cycle.

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
