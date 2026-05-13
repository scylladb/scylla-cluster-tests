# Collected Log Files

After each SCT test run, logs are collected from all nodes and uploaded to Argus/S3 as compressed archives. This document describes the log archives and the files they contain.

## Archive Structure

Each test run produces several archives named by cluster type:

| Archive | Contents |
|---------|----------|
| `db-cluster-<test-id>.tar.zst` | Scylla DB node logs |
| `loader-set-<test-id>.tar.zst` | Loader node logs |
| `monitor-set-<test-id>.tar.zst` | Monitor node logs (Prometheus, Grafana) |
| `sct-runner-events-<test-id>.tar.zst` | SCT framework event logs |
| `sct-<test-id>.log.tar.zst` | Main SCT runner log |

## DB Cluster Node Logs

Each node directory (`db-node-<id>/`) contains:

| File | Description |
|------|-------------|
| `system.log` | Scylla database log (main operational log) |
| `console_output.log` | Serial console output from cloud provider (boot messages, kernel output) |
| `messages.log` | System log (`/var/log/messages` or equivalent) |
| `dmesg.log` | Kernel ring buffer (`dmesg` output) |
| `cloud-init.log` | Cloud-init execution log |
| `cloud-init-output.log` | Cloud-init stdout/stderr |
| `scylla.yaml` | Scylla configuration at time of collection |
| `cassandra-rackdc.properties` | Rack/DC configuration |
| `scylla-manager-agent.yaml` | Scylla Manager agent configuration |
| `io-properties.yaml` | Disk I/O properties |
| `cpu_info` | `/proc/cpuinfo` snapshot |
| `mem_info` | `/proc/meminfo` snapshot |
| `interrupts` | `/proc/interrupts` snapshot |
| `vmstat` | Virtual memory statistics |
| `coredumps.info` | Core dump listing (if any) |
| `systemctl.status` | Systemd service status |
| `setup_scripts_errors.log` | Errors from node setup scripts |
| `scylla_doctor.vitals.json` | Scylla Doctor health check results |
| `kallsyms_<timestamp>` | Kernel symbol table (Azure/OCI, for stack trace symbolization) |

## Loader Node Logs

Each loader node directory contains:

| File | Description |
|------|-------------|
| `system.log` | Loader system log |
| `cloud-init.log` | Cloud-init execution log |
| `cloud-init-output.log` | Cloud-init stdout/stderr |
| `console_output.log` | Serial console output (when kernel panic checker is enabled) |

## Monitor Node Logs

Each monitor node directory contains:

| File | Description |
|------|-------------|
| `console_output.log` | Serial console output (when kernel panic checker is enabled) |
| `messages.log` | System log |
| `system.log` | Monitor system log |
| `cloud-init.log` | Cloud-init execution log |
| `cloud-init-output.log` | Cloud-init stdout/stderr |
| `aprom.log` | Prometheus container log |
| `agraf.log` | Grafana container log |
| `aalert.log` | Alert manager container log |
| `scylla_manager.log` | Scylla Manager server log |
| `scylla-manager.yaml` | Scylla Manager configuration |
| `manager_scylla_backend.log` | Manager's Scylla backend log |
| `prometheus_data_<timestamp>.tar.zst` | Prometheus TSDB snapshot |
| `monitoring_data_stack_branch-*.tar.gz` | Monitoring stack dashboards/configs |
| `grafana-screenshot-*.png` | Grafana dashboard screenshots at test end |

## SCT Runner Events

The `sct-runner-events-<test-id>.tar.zst` archive contains framework-level logs:

| File | Description |
|------|-------------|
| `events.log` | All SCT events (human-readable) |
| `raw_events.log` | All SCT events (machine-parseable) |
| `critical.log` | Critical severity events only |
| `error.log` | Error severity events only |
| `warning.log` | Warning severity events only |
| `normal.log` | Normal/info severity events only |
| `debug.log` | Debug severity events only |
| `summary.log` | Test result summary |
| `output.log` | SCT stdout/stderr |
| `actions.log` | Nemesis and other action logs |
| `argus.log` | Argus client communication log |
| `console_output.log` | SCT runner node serial console |
| `left_processes.log` | Processes still running at test end |

## SCT Runner Log

The `sct-<test-id>.log.tar.zst` contains a single file — the full SCT framework log with all debug output from the test execution.

## Downloading Logs

### Via Argus CLI

```bash
# List available logs for a test run
argus run logs list --run-id <test-id>

# Download and extract a specific archive
argus run logs download "db-cluster-<test-id>.tar.zst" --run-id <test-id> --dest ./logs
```

### Via S3

Logs are stored at:
```
s3://cloudius-jenkins-test/<test-id>/<timestamp>/<archive-name>.tar.zst
```
