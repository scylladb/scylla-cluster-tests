# Workflow: Generate a Performance Version Comparison Report

Step-by-step process for producing an HTML comparison page from predefined-throughput-steps or microbenchmark test runs.

## Step 1: Ask User for Inputs

Before doing any work, ask the user:

1. **Test type**: predefined-throughput-steps or microbenchmark
2. **Which load types/variants** to compare
   - Throughput: read, write, mixed, read_disk_only
   - Microbenchmark: arm64, arm64-write, x86_64, x86_64-write
3. **Which versions** are being compared (e.g., 2026.1.5, 2026.2.0, 2026.3.0.dev)
4. **Test run IDs** (Argus UUIDs) for each version and load type

Example:
```
For each load type, I need the Argus test run ID per version:
1. read: version1 (UUID), version2 (UUID), version3 (UUID)
2. write: version1 (UUID), version2 (UUID), version3 (UUID)
...
```

**If user provides versions but not run IDs for microbenchmarks:** Use `argus run list` with the test UUIDs from the SKILL.md table to find runs, then query each run's metadata to match `scylla_version`. Include failed runs in the search - microbenchmark tests fail on threshold violations but still have valid results.

## Step 2: Fetch Run Metadata

For each run ID, call Argus API to get metadata:

```python
from sdcm.keystore import KeyStore
import requests

creds = KeyStore().get_argus_rest_credentials_per_provider()
headers = {'Authorization': f'token {creds["token"]}', 'Accept': 'application/json', 'Content-Type': 'application/json'}
if 'extra_headers' in creds:
    headers.update(creds['extra_headers'])
base_url = creds['baseUrl']

url = f'{base_url}/api/v1/client/testrun/scylla-cluster-tests/{run_id}/get'
resp = requests.get(url, headers=headers, timeout=30)
data = resp.json()['response']
# Extract: status, start_time, cloud_setup, packages, test_method
```

## Step 3: Fetch Results via Argus CLI

For each run ID, fetch the results tables:

```bash
argus run results --run-id <UUID> > /tmp/opencode/results_<load>_<version>.json
```

Run these in parallel for all run IDs to save time.

Parse the JSON to extract per-step metrics:
- Filter tables: only those with "latencies" in name (skip "stalls")
- For each table, extract from `rows[0].cells`:
  - `P99 read` or `P99 write` (latency in ms)
  - `Throughput read` or `Throughput write` (ops/s)
  - `duration` (seconds)

## Step 4: Build Summary Results Table

For each load type and step:
1. Collect P99 latency and throughput values across all versions
2. Calculate delta % vs baseline (first/oldest version)
3. Assign status label based on thresholds
4. For unthrottled step, add "Max Throughput" row
5. Use alternating row colors

## Step 5: Generate Key Findings

Analyze the data to produce analytical findings:

1. **Test pass/fail**: Check if any version failed. If so, identify the step with the worst latency spike and hypothesize root cause
2. **Throughput trend**: Compare max throughput across versions. Note if regression is isolated to saturation or systemic
3. **Low-step stability**: Check if lower steps have <10% delta. If yes, note that steady-state is unaffected
4. **Latency at highest throttled step**: Note if it crosses WARNING/REGRESSION thresholds
5. **Reactor stalls**: Count stall tables per version, correlate with latency spikes

**Rules:**
- Do NOT repeat numbers from the table
- DO explain patterns, root causes, and implications
- DO compare behavior between versions qualitatively

## Step 6: Generate Test Outcome Table

| Version | Result | Issues |
|---------|--------|--------|

- Result: PASSED (bold) or FAILED (bold red)
- Issues: Fetch from `argus run activity --run-id <UUID>`, filter for `ARGUS_TEST_RUN_ISSUE_ADDED` events, extract Jira URL and summary from the `body` JSON field

## Step 7: Generate Conclusion Paragraph

Write 2-4 sentences that:
1. Provide a root cause hypothesis for any regression
2. State whether issue is saturation-specific or systemic
3. Recommend next steps (re-run, investigation, monitoring)
4. Note clear improvements where applicable

## Step 8: Assemble and Save

1. Combine all sections into complete HTML document
2. Save to `~/Downloads/compare_<versions>_throughput_results.html`
3. Report file location to user

## Notes

- The first version in the list is always the baseline for delta calculations
- Runs may be in different AWS regions - note this in the report if significant
- The argus CLI requires authentication; if it prompts for browser auth, the session token may have expired
- For mixed workload, include both P99 read AND P99 write metrics in the table

## Microbenchmark-Specific Steps

When generating a microbenchmark report, adapt the steps as follows:

### Step 3 (Microbenchmark): Parse Results

The results JSON contains a single table per run (e.g., `"read - Perf Simple Query"` or `"write - Perf Simple Query"`). Extract from `rows[0].cells`:
- `median tps`, `max tps`, `min tps`, `mad tps` (throughput)
- `allocs_per_op`, `cpu_cycles_per_op`, `instructions_per_op` (efficiency)
- `tasks_per_op` (scheduling overhead)

### Step 4 (Microbenchmark): Build Summary Table

Use a flat metric-per-row table (no "Load Step" column):

| Metric | Version1 | Version2 | Version3 | Delta (V2 vs V1) | Delta (V3 vs V1) | Status |

Key rows to include:
1. **Median TPS** (primary metric)
2. Max TPS
3. Min TPS
4. MAD TPS (stability)
5. CPU cycles/op
6. Instructions/op
7. Allocs/op

### Step 5 (Microbenchmark): Key Findings

Focus on:
- Per-op efficiency vs actual throughput correlation (do alloc improvements translate to TPS?)
- Architecture-specific patterns (ARM64 vs x86_64 divergence)
- Threshold violation causes (which metric has ERROR status?)
- Stability trends (MAD TPS across versions)

### Step 8 (Microbenchmark): File Naming

Save as: `~/Downloads/compare_<versions>_microbenchmark_<arch>.html`

### Report Structure for Microbenchmarks

Each architecture/workload variant gets its own section:

```
<h2>ARM64 Read Microbenchmark</h2>
<h2>ARM64 Write Microbenchmark</h2>
<h2>x86_64 Read Microbenchmark</h2>
<h2>x86_64 Write Microbenchmark</h2>
```

The Executive Summary table has one row per variant (not per workload).
