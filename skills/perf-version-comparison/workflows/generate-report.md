# Workflow: Generate a Performance Version Comparison Report

Step-by-step process for producing an HTML comparison page from predefined-throughput-steps test runs.

## Step 1: Ask User for Inputs

Before doing any work, ask the user:

1. **Which load types** to compare (read, write, mixed, read_disk_only)
2. **Which versions** are being compared (e.g., 2026.1.5, 2026.2.0, 2026.3.0.dev)
3. **Test run IDs** (Argus UUIDs) for each version and load type

Example:
```
For each load type, I need the Argus test run ID per version:
1. read: version1 (UUID), version2 (UUID), version3 (UUID)
2. write: version1 (UUID), version2 (UUID), version3 (UUID)
...
```

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
- Issues: Leave empty (no issues linked in Argus API currently)

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
