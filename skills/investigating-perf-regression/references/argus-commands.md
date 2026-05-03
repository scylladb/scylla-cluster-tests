# Argus CLI Commands for Performance Investigation

Commands for retrieving SCT test run metadata, events, and logs using the Argus CLI.

## Prerequisites

The `argus` CLI must be installed and authenticated:

```bash
which argus          # verify installation
argus auth           # authenticate via Cloudflare Access (if needed)
argus auth-token     # save token to keyring (alternative)
```

## Run Metadata

### Get run details

Returns JSON with run ID, region, instance type, Scylla version, SCM revision, AMI, status, Jenkins URL, and events summary.

```bash
argus run get --run-id <UUID>
```

Key fields to extract for comparison:
- `region` -- AWS region (us-east-1, eu-west-1, etc.)
- `instance_type` -- EC2 instance type (e.g., i8g.4xlarge)
- `scylla_version` -- Scylla version under test
- `scm_revision_id` -- Git commit hash of the Scylla build
- `image_id` -- AMI used for the nodes
- `build_job_url` -- Jenkins build URL
- `status` -- Run status (passed, failed)
- `investigation_status` -- Whether the run has been investigated

### Get run type

```bash
argus run get-type --run-id <UUID>
```

## Events

### Get error and critical events

Returns events with severity ERROR or CRITICAL. For perf regressions, look for `FailedResultEvent` entries.

```bash
argus run events --run-id <UUID>
```

The `message` field in `FailedResultEvent` identifies the failing step:

```
"Argus validation failed for the result in read - 1200000 - latencies"
```

This tells you: workload type (read), step rate (1200000), and what failed (latencies).

## Results

### Get test results

Requires both run-id and test-id (get test-id from `argus run get`).

```bash
argus run results --run-id <UUID> --test-id <TEST_UUID>
```

## Logs

### List available logs

```bash
argus run logs list --run-id <UUID>
```

Typical log files for performance tests:

| Log Name | Contents |
|----------|----------|
| `db-cluster-<ID>.tar.zst` | Scylla system.log, scylla.yaml, mem_info, vmstat, dmesg for each node |
| `sct-<ID>.log.tar.zst` | Main SCT test log with HDR histograms, stress commands, latency results |
| `loader-set-<ID>.tar.zst` | Loader node logs (cassandra-stress output) |
| `monitor-set-<ID>.tar.zst` | Monitoring stack logs |
| `schema-logs-<ID>.tar.zst` | Schema change logs |
| `sct-runner-events-<ID>.tar.zst` | SCT runner event logs |
| `builder-<ID>.log.tar.gz` | Build/setup logs |

### Download and extract logs

```bash
argus run logs download <log-name> --run-id <UUID> --dest <directory>
```

Example for downloading DB cluster logs for a failed run:

```bash
mkdir -p /tmp/investigation/failed
argus run logs download db-cluster-5a958af9.tar.zst \
  --run-id 5a958af9-8c9b-4b22-aa58-a347c1a0f70c \
  --dest /tmp/investigation/failed
```

### Extracted directory structure

After extracting `db-cluster-<ID>.tar.zst`:

```
db-cluster-<ID>/
├── messages.log                    # Aggregated kernel/system messages
├── kallsyms_*                      # Kernel symbol tables
├── <node-name-1>/
│   ├── system.log                  # Scylla log (reactor stalls, errors)
│   ├── scylla.yaml                 # Scylla config (has broadcast_rpc_address for IP mapping)
│   ├── mem_info                    # /proc/meminfo snapshot
│   ├── vmstat                      # /proc/vmstat snapshot
│   ├── cpu_info                    # CPU info
│   ├── dmesg.log                   # Kernel ring buffer
│   ├── io-properties.yaml          # I/O scheduler config
│   ├── scylla_doctor.vitals.json   # Scylla doctor output (may be empty)
│   └── ...
├── <node-name-2>/
│   └── ...
└── <node-name-3>/
    └── ...
```

## Comments

### View run comments

```bash
argus run comments --run-id <UUID>
```

### Add a comment to a run

```bash
argus comment create --run-id <UUID> --message "RCA: read queuing on node-3 caused P99 regression"
```

## Activity Log

### View run activity

```bash
argus run activity --run-id <UUID>
```

## Caching

By default, Argus CLI caches API responses locally. To bypass the cache for fresh data:

```bash
argus --no-cache run get --run-id <UUID>
```

To override cache TTL:

```bash
argus --cache-ttl 5m run get --run-id <UUID>
```
