# Skill: perf-version-comparison

# Performance Version Comparison Reports

Generate Confluence-ready HTML reports comparing predefined-throughput-steps test results across multiple ScyllaDB versions.

## When to Use

- Comparing throughput test results between 2+ ScyllaDB versions (e.g., release vs RC vs master)
- Creating a Confluence page summarizing predefined-steps throughput regression results
- Generating side-by-side latency/throughput comparison from Argus test run IDs
- Documenting performance changes across version branches

## When NOT to Use

- Comparing latency-focused elasticity tests (use `test-run-comparison-reports` skill)
- Generating weekly performance status reports (use `perf-weekly-status-report` skill)
- Analyzing a single test run without cross-version comparison

## Phase 1: Gather Inputs

**Start by asking the user:**

1. **Test type and subtests (load types)** to compare. The predefined-throughput-steps test has these subtests:
   - `read` - Pure read workload
   - `write` - Pure write workload
   - `mixed` - 50/50 read/write workload
   - `read_disk_only` - Read from disk (no cache) workload

2. **Versions and test run IDs** for each subtest. Format:
   ```
   Load type: <load>
   Version A (name): <test_id_uuid>
   Version B (name): <test_id_uuid>
   Version C (name): <test_id_uuid>
   ```

Example prompt to user:
> Which load types do you want to compare? (read, write, mixed, read_disk_only)
> For each load type, provide the version names and Argus test run IDs.

## Phase 2: Fetch Run Metadata from Argus

Use the SCT KeyStore to authenticate with Argus API:

```python
from sdcm.keystore import KeyStore
import requests

creds = KeyStore().get_argus_rest_credentials_per_provider()
headers = {
    'Authorization': f'token {creds["token"]}',
    'Accept': 'application/json',
    'Content-Type': 'application/json',
}
if 'extra_headers' in creds:
    headers.update(creds['extra_headers'])

base_url = creds['baseUrl']
url = f'{base_url}/api/v1/client/testrun/scylla-cluster-tests/{run_id}/get'
resp = requests.get(url, headers=headers, timeout=30)
data = resp.json()['response']
```

Extract from each run:
- `status` (passed/failed)
- `start_time`
- `cloud_setup` (instance types, node counts)
- `packages` (ScyllaDB version)
- `scylla_version`
- `test_method`

## Phase 3: Fetch Linked Issues via Argus CLI

Issues attached to runs are in the **activity log**. Fetch them:

```bash
argus run activity --run-id <UUID>
```

Filter events with `kind == "ARGUS_TEST_RUN_ISSUE_ADDED"`. The `body` field is JSON containing:
- `url` - Jira ticket URL (e.g., `https://scylladb.atlassian.net/browse/SCYLLADB-1902`)
- `summary` - Issue title

Use these to populate the "Issues" column in Test Outcome tables and the "Known Issues" section.

## Phase 4: Fetch Test Results via Argus CLI

The predefined-throughput-steps results are stored as Argus result tables. Fetch them using the `argus` CLI:

```bash
argus run results --run-id <UUID>
```

This returns JSON with structure:
```json
[
  {
    "name": "<workload> - <step_rate> - latencies",
    "description": "<workload> workload - Gradual test step <rate> op/s",
    "status": "PASS",
    "rows": [
      {
        "name": "Cycle #1",
        "cells": {
          "P90 read": {"value": 0.56, "status": "PASS"},
          "P99 read": {"value": 0.72, "status": "PASS"},
          "Throughput read": {"value": 499683, "status": "UNSET"},
          "duration": {"value": 1813, "status": "UNSET"},
          "start time": {"value": "14:22:20", "status": "UNSET"}
        }
      }
    ]
  }
]
```

**Key metrics to extract per step:**
- `P99 read` / `P99 write` - P99 latency in ms
- `P90 read` / `P90 write` - P90 latency in ms
- `Throughput read` / `Throughput write` - ops/s
- `duration` - step duration in seconds

**Tables with "stalls" in the name** contain reactor stall events (ignore for main metrics, count for analysis).

## Phase 4: Build the HTML Report

### Overall Page Structure

The report has a top-level structure followed by per-workload sections:

```
<h1>Performance Comparison: ScyllaDB Enterprise <version1> vs <version2> vs <version3></h1>
<h1>Predefined Throughput Steps</h1>

Infrastructure: <instance types and counts>
Date: <dates>

---
<h2>Executive Summary</h2>
[TABLE: Workload | Test | Version1 status | Version2 status | Version3 status | Key Issue]

---
<h3>Overall Assessment</h3>
[Numbered list comparing version pairs, listing regressions and improvements]

---
<h3>Known Issues</h3>
[Links to any related Jira tickets, or "No linked issues"]

---
<h2><Workload> workload</h2>  (repeated per load type)
...per-workload details...
```

### Executive Summary Table

One row per workload showing PASSED/FAILED per version and a one-line "Key Issue" summary.

### Overall Assessment

Numbered list with:
1. **Version B vs Version A (baseline):** High-level comparison
2. **Version C vs Version B:** What changed between the two newer versions
3. **Regressions requiring investigation:** Bulleted list with red-highlighted specific failures
4. **Improvements in newer versions:** Bulleted list with green-highlighted wins

### Per-Workload Section Structure

```
<h2><Workload> workload</h2>

Test: <test_method>
Date: <date>

---
### Summary Results
[TABLE with columns: Load Step | Metric | Version1 | Version2 | Version3 | Delta (V2 vs V1) | Delta (V3 vs V1) | Status]

---
### Key Findings
[Numbered list with ANALYTICAL observations, NOT repeating table data]

---
### Test Outcome
[TABLE with columns: Version | Result | Issues]

---
Links:
- Version1: <argus_url>
- Version2: <argus_url>
- Version3: <argus_url>
```

### Summary Results Table Rules

1. **Baseline** is the first (oldest stable) version
2. **Delta** columns show % change vs baseline: `((new - base) / base) * 100`
3. **Status** thresholds:
   - Latency increase >15%: `REGRESSION` (red)
   - Latency increase >10%: `WARNING` (orange)
   - Throughput decrease >10%: `REGRESSION` (red)
   - Throughput decrease >5%: `WARNING` (orange)
   - Otherwise: `OK`
4. **Unthrottled step** also shows "Max Throughput" row
5. Alternating row backgrounds (blue/white) for readability

### Key Findings Rules

Key Findings must be **analytical**, not repetitive of the table:
- Explain **why** a test passed/failed (e.g., "likely caused by tablet split starving the read path")
- Identify **patterns** (e.g., "regression only at saturation", "systemic across all steps")
- Compare **behavior** between versions (e.g., "2026.2.0 avoids the spike but at lower peak throughput")
- Note **reactor stalls** and their correlation with latency spikes
- Highlight if lower steps are stable (confirms issue is saturation-specific)

**Do NOT:**
- Repeat exact numbers from the table
- List values for each version without interpretation
- Use generic statements like "performance changed"

### Conclusion Paragraph Rules

The conclusion after Test Outcome must:
- Provide a **root cause hypothesis** (compaction, tablet splits, scheduling, I/O contention)
- State whether the issue is **isolated to saturation** or **systemic**
- Recommend **next steps** (re-run, investigation, monitoring)
- Note if a workload is a **clear win** for newer versions (especially disk-read)

### HTML Template

See [html-template.md](references/html-template.md) for the complete CSS and HTML structure.

## Phase 5: Save and Deliver

1. Save the HTML file to `~/Downloads/` with a descriptive name
2. Inform the user of the file location
3. Note that the file can be pasted into Confluence editor or uploaded

## Argus URL Format

```
https://argus.scylladb.com/tests/scylla-cluster-tests/{test_id}
```

## Reference Index

| File | Content |
|------|---------|
| [html-template.md](references/html-template.md) | Complete HTML/CSS template |
| [generate-report.md](workflows/generate-report.md) | Step-by-step workflow |

## Success Criteria

- [ ] User was asked for load types and test IDs before starting
- [ ] Summary Results table has Delta columns and Status labels
- [ ] Key Findings are analytical (explain WHY, not repeat WHAT)
- [ ] Test Outcome has Issues column (empty if no linked issues)
- [ ] Conclusion paragraph provides root cause hypothesis and next steps
- [ ] All Argus links are correct and clickable
- [ ] HTML renders correctly when opened in browser
