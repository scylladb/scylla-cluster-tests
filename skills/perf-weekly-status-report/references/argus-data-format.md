# Argus CLI Data Format Reference

## `argus run list --full` Output

Each element in the returned JSON array is a run object:

```json
{
  "id": "4716baf7-c507-44ab-a3c1-27a14b785700",
  "status": "passed",
  "scylla_version": "2026.1.5",
  "build_number": 48,
  "build_id": "scylla-enterprise/perf-regression/scylla-enterprise-perf-regression-predefined-throughput-steps-i8g-tablets",
  "build_job_url": "https://jenkins.scylladb.com/job/...",
  "branch_name": "origin/branch-perf-v17",
  "start_time": "2026-06-14T...",
  "end_time": "2026-06-14T...",
  "cloud_setup": {
    "backend": "aws",
    "db_node": {
      "instance_type": "i8g.4xlarge",
      "node_amount": 3
    },
    "loader_node": {
      "instance_type": "c7i.8xlarge",
      "node_amount": 4
    }
  },
  "packages": [
    {
      "name": "scylla-server-target",
      "version": "2026.1.5",
      "date": "20260612",
      "revision_id": "91ada5517d59"
    }
  ],
  "config_files": [
    "test-cases/performance/perf-regression-predefined-throughput-steps.yaml",
    "configurations/performance/cassandra_stress_gradual_load_steps_i8g.yaml"
  ]
}
```

### Key Fields for Filtering

| Field | Description | Use |
|-------|-------------|-----|
| `scylla_version` | Product version string | Filter master (~dev) vs release |
| `status` | Run outcome | Display in overview |
| `id` | Run UUID | Fetch results |
| `build_job_url` | Jenkins URL | Link to CI |
| `packages[].version` | Package versions | Fallback version source |

### Status Values

- `passed` -- All checks passed
- `failed` -- Test completed but checks failed (e.g., latency threshold exceeded)
- `test_error` -- Test infrastructure error (setup failure, timeout)
- `running` -- Still executing
- `aborted` -- Manually stopped

## `argus run results` Output

Returns array of result tables:

```json
[
  {
    "name": "write - 350000 - latencies",
    "description": "write workload - Gradual test step 350000 op/s",
    "status": "PASS",
    "rows": [
      {
        "name": "Cycle #1",
        "cells": {
          "P90 write": {"value": 1.59, "status": "PASS"},
          "P99 write": {"value": 2.21, "status": "PASS"},
          "Throughput write": {"value": 349772, "status": "UNSET"},
          "Overview": {"value": "https://...screenshot.png", "status": "UNSET"},
          "duration": {"value": 4622, "status": "UNSET"},
          "start time": {"value": "22:35:24", "status": "UNSET"}
        }
      }
    ]
  }
]
```

### Table Name Patterns

| Pattern | Test Type | Example |
|---------|-----------|---------|
| `<workload> - <rate> - latencies` | Predefined steps | `write - 350000 - latencies` |
| `<workload> - unthrottled - latencies` | Max throughput step | `read - unthrottled - latencies` |
| `<workload> - <nemesis> - latencies` | Latency during nemesis | `mixed - _mgmt_repair_cli - latencies` |
| `<workload> - Steady State - latencies` | Baseline measurement | `mixed - Steady State - latencies` |
| `<workload> - <step> - stalls - REACTOR_STALLED` | Stall counts | `write - unthrottled - stalls - REACTOR_STALLED` |

### Cell Name Patterns

| Cell Name | Type | Unit | Description |
|-----------|------|------|-------------|
| `P90 <op>` | float | ms | 90th percentile latency (available but NOT included in report) |
| `P99 <op>` | float | ms | 99th percentile latency |
| `Throughput <op>` | int | op/s | Actual sustained throughput |
| `Overview` | URL | - | Grafana screenshot link |
| `QA dashboard` | URL | - | Per-server metrics screenshot |
| `duration` | int | seconds | Step duration |
| `start time` | string | HH:MM:SS | When step started |

Where `<op>` is one of: `read`, `write`, `read_disk_only`, `mixed`

**Note on P90**: The P90 metric is available in the Argus data but is **NOT reported** in the weekly status report tables. Only P99 is shown in the Failed Results and Max Throughput tables.

**Mixed workload throughput**: The `mixed` workload reports BOTH `Throughput read` and `Throughput write` in the same row. To get total throughput for mixed, sum both values:
```
Total mixed throughput = Throughput read + Throughput write
```
Example: `Throughput read: 484366` + `Throughput write: 484283` = `968649` total op/s.

Single-operation workloads (read, write, read_disk_only) have only one `Throughput <op>` cell.

### Cell Status Values

| Status | Meaning | Badge Color |
|--------|---------|-------------|
| `PASS` | Within acceptable threshold | Green (#28a745) |
| `FAIL` | Exceeded threshold (regression) | Red (#dc3545) |
| `ERROR` | Could not evaluate | Orange (#fd7e14) |
| `UNSET` | Informational only (no threshold) | No badge |

## Microbenchmark Results

Microbenchmark tests have a different cell structure:

```json
{
  "name": "read - Perf Simple Query",
  "description": "{\"concurrency\": 100, ...}",
  "status": "PASS",
  "rows": [{
    "name": "#1",
    "cells": {
      "allocs_per_op": {"value": 58.12, "status": "PASS"},
      "cpu_cycles_per_op": {"value": 16626.26, "status": "PASS"},
      "instructions_per_op": {"value": 33000.59, "status": "PASS"},
      "tps": {"value": 60123.45, "status": "UNSET"},
      "mad tps": {"value": 36.55, "status": "UNSET"}
    }
  }]
}
```

### Microbenchmark Cell Names

| Cell Name | Type | Description |
|-----------|------|-------------|
| `allocs_per_op` | float | Memory allocations per operation |
| `cpu_cycles_per_op` | float | CPU cycles per operation |
| `instructions_per_op` | float | Instructions per operation |
| `logallocs_per_op` | float | Log allocations per operation |
| `tps` | float | Transactions per second |
| `mad tps` | float | Median absolute deviation of TPS |

## `argus issue list` Output

Returns array of issue objects linked to a run, test, or other entity:

```bash
argus issue list \
  --run-id <RUN_UUID> \
  --url https://argus.scylladb.com
```

```json
[
  {
    "key": "<PROJECT>-<NUMBER>",
    "subtype": "jira",
    "title": "<issue summary text>",
    "state": "<state>",
    "url": "https://scylladb.atlassian.net/browse/<PROJECT>-<NUMBER>"
  }
]
```

The array may contain zero, one, or many issues. Any Jira project prefix is possible (e.g., `SCYLLADB-*`, `SCT-*`, or others). The number and content of issues varies between runs and report periods.

### Issue Fields

| Field | Type | Description |
|-------|------|-------------|
| `key` | string | Jira issue key (e.g., `PROJECT-123`) |
| `subtype` | string | Issue tracker type (always `"jira"` currently) |
| `title` | string | Issue title/summary |
| `state` | string | Jira issue state (see values below) |
| `url` | string | Direct link to the Jira issue |

### Issue State Values

| State | Description |
|-------|-------------|
| `new` | Issue is newly created (Jira "Open" or similar) |
| `todo` | Issue is in backlog/to-do state |
| `done` | Issue has been resolved/closed |
| `duplicate` | Issue is a duplicate of another |

### Filter Flags

Exactly one filter flag must be provided:

| Flag | Description |
|------|-------------|
| `--run-id` | Issues linked to a specific run |
| `--test-id` | All issues linked to any run of a test |
| `--release-id` | Issues linked to a release |
| `--group-id` | Issues linked to a group |

### Important Notes

- The `state` field reflects the Jira workflow state but does NOT indicate when the issue was created
- The CLI does NOT expose Jira issue creation dates
- To determine if an issue is "new" (created during the report period) vs "reproduced" (pre-existing), the agent must ask the user to classify issues
- De-duplicate issues by `key` when querying multiple runs (same issue often linked to multiple failed runs)
- Empty array `[]` is returned when no issues are linked to the entity
