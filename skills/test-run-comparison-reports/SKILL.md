---
name: test-run-comparison-reports
description: >-
  Generate Confluence-compatible HTML comparison pages from multiple SCT test runs.
  Use when comparing performance or elasticity test results across runs with different
  parameters (instance types, configurations, versions). Covers fetching run metadata
  and latency results from Argus API and S3 logs, extracting hdr_summary P99/P90 and
  throughput metrics per phase, building styled HTML tables with pass/fail color coding,
  and linking to Argus run pages. Applies to perf-regression, elasticity, and latency
  tests that use the latency_calculator_decorator.
---

# Test Run Comparison Reports

Generate Confluence-compatible HTML pages that compare multiple SCT performance or elasticity test runs side-by-side.

## Essential Principles

### Use hdr_summary as the Source of Truth

The correct P99/P90 values come from the `hdr_summary` field in the `latency_results` dict, not from averaging per-bucket HDR data. The `hdr_summary` is computed from the merged HDR histogram spanning the entire phase duration and matches what Argus displays in the Results tab. Per-bucket data in the `hdr` array is for timeline analysis only.

### Color Code by Threshold

P99 values exceeding 10 ms are failures and must be marked red. Values within threshold that are the best among compared runs should be marked green. This 10 ms threshold matches the default Argus validation rule in `LATENCY_ERROR_THRESHOLDS`.

### Emphasize What Varies

The key differentiator between runs must be immediately visible in the Overview section. Use an info-box at the top that calls out exactly what parameter varies between runs and lists the per-run values. Readers should understand the comparison axis within 3 seconds.

### Link Column Headers to Argus

Every instance-type or run-identifier column header in phase tables must be a clickable link to the corresponding Argus run page. The URL format is `https://argus.scylladb.com/tests/scylla-cluster-tests/{test_id}`.

### Show Only Relevant Metrics

Include only P99 write, P99 read, throughput write, throughput read, and duration per phase. Do not include c-s P99/P95/max/stdev, Scylla per-node metrics, or P90 unless specifically requested. The report should be scannable, not exhaustive.

## When to Use

- Comparing 2+ test runs that differ by a single parameter (instance type, ScyllaDB version, config flag)
- Creating a Confluence page summarizing elasticity or performance regression results
- Generating a side-by-side latency/throughput comparison from Argus test IDs
- Building an HTML report from SCT latency_results data for stakeholder review
- Documenting the impact of a configuration change across multiple test executions

## When NOT to Use

- Generating weekly performance status reports (use `perf-weekly-status-report` skill)
- Analyzing a single test run without comparison
- Debugging test failures (check logs directly)
- Creating Argus result tables programmatically (that's `sdcm/argus_results.py`)

## Data Extraction

### Step 1: Get Run Metadata from Argus API

```python
from sdcm.keystore import KeyStore
import requests

creds = KeyStore().get_argus_rest_credentials_per_provider()
headers = {
    'Authorization': f'token {creds["token"]}',
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    **creds.get('extra_headers', {})
}

# Get run info (status, config, packages, cloud_setup, build_job_url)
url = f'{creds["baseUrl"]}/api/v1/client/testrun/scylla-cluster-tests/{run_id}/get'
resp = requests.get(url, headers=headers, timeout=30)
run_data = resp.json()['response']
```

### Step 2: Get Latency Results from S3 Logs

The `fetch_results` API endpoint may be unavailable (502). Fall back to extracting results from SCT log files stored in S3:

1. List objects in `cloudius-jenkins-test` bucket under `{run_id}/` prefix
2. Download `.sct-*.log.zst` files
3. Decompress with `zstd -d`
4. Search for lines matching `latency_results: {`
5. Parse with `ast.literal_eval`

### Step 3: Extract Metrics from latency_results

The `latency_results` dict has this structure per phase:

```python
{
    'Steady State': {  # flat dict for baseline
        'hdr_summary': {
            'WRITE--WRITE-rt': {'percentile_99': ..., 'throughput': ...},
            'READ--READ-rt': {'percentile_99': ..., 'throughput': ...},
        },
        'cycle_hdr_throughput': ...,  # combined write+read
        'duration_in_sec': ...,
    },
    'add_new_nodes': {  # phases with cycles
        'cycles': [{
            'hdr_summary': {
                'WRITE--WRITE-rt': {'percentile_99': ..., 'percentile_90': ..., 'throughput': ...},
                'READ--READ-rt': {'percentile_99': ..., 'percentile_90': ..., 'throughput': ...},
            },
            'cycle_hdr_throughput': ...,
            'duration': '0:45:07',
            'duration_in_sec': 2707,
        }],
    },
}
```

Key fields to extract per phase:
- `hdr_summary['WRITE--WRITE-rt']['percentile_99']` -> P99 write
- `hdr_summary['READ--READ-rt']['percentile_99']` -> P99 read
- `hdr_summary['WRITE--WRITE-rt']['throughput']` -> Throughput write
- `hdr_summary['READ--READ-rt']['throughput']` -> Throughput read
- `duration_in_sec` -> Duration

### Phase Ordering for Elasticity Tests

The grow/shrink nemesis produces phases in this order:
1. **Steady State** - 30 min baseline before any operation
2. **add_new_nodes** - nodes added in parallel
3. **_double_cluster_load** - load doubled for 30 min on expanded cluster
4. **decommission_nodes** - added nodes removed in parallel

The latency_results dict accumulates across subtests. Use the latest log entry that contains the phase you need.

## HTML Structure

See [html-template.md](references/html-template.md) for the full HTML template with CSS styles.

Key structural elements:
- Overall Results box after the title (short pass/fail summary with conclusion)
- Overview with info-box highlighting what varies between runs
- Parameters table (base cluster, version, dataset, workload, nemesis)
- Test Runs table (run ID, date, status badge, Argus link)
- Per-phase tables with linked column headers and color-coded P99 cells
- Duration comparison table
- Summary table (one row per aspect, color-coded)

## Argus URL Format

```
https://argus.scylladb.com/tests/scylla-cluster-tests/{test_id}
```

Where `{test_id}` is the run UUID (e.g., `9fa33b8a-3e70-4860-ae74-2f1b885e24c7`).

## Reference Index

| File | Content |
|------|---------|
| [html-template.md](references/html-template.md) | Complete HTML/CSS template for comparison pages |
| [generate-report.md](workflows/generate-report.md) | Step-by-step workflow for producing a comparison report |

## Success Criteria

- [ ] Overall Results box appears after title with pass/fail summary and conclusion
- [ ] Overview immediately shows what parameter varies between runs
- [ ] All P99 values > 10 ms are marked red
- [ ] All column headers in phase tables link to the correct Argus run
- [ ] Only P99 write, P99 read, throughput write, throughput read, and duration shown per phase
- [ ] Values come from `hdr_summary` (merged histogram), not per-bucket averages
- [ ] HTML renders correctly when pasted into Confluence
- [ ] Status badges (PASSED/FAILED) match actual run outcomes
