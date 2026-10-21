---
name: perf-version-comparison
description: >-
  Generate a standalone HTML report with tables and bar charts comparing the
  performance results of the same test across two or more ScyllaDB versions
  (release vs RC vs master) from Argus runs. Use when comparing
  predefined-throughput-steps results (read, write, mixed, read_disk_only) or
  simple-query microbenchmark results (arm64, x86_64, read and write) between
  versions, building a version comparison page, or showing P99 latency,
  throughput, TPS and per-op efficiency side by side per version, with the best
  and worst value highlighted. Not for comparing runs with different parameters
  or elasticity tests (use test-run-comparison-reports) or for weekly perf
  status emails (use perf-weekly-status-report).
---

# Performance Version Comparison Reports

Generate a standalone HTML page comparing predefined-throughput-steps or simple-query microbenchmark results across ScyllaDB versions: one table and one bar chart per metric, with versions as rows. [example-report.html](references/example-report.html) shows the expected result. Its data is synthetic.

## When to Use

- Comparing throughput test results between 2+ ScyllaDB versions (e.g., release vs RC vs master)
- Comparing microbenchmark (simple-query) results between versions
- Repeating a step (e.g. the unthrottled step) several times per version and comparing the cycles
- Documenting performance changes across version branches

## When NOT to Use

- Comparing runs with different parameters, or latency-focused elasticity tests (use `test-run-comparison-reports` skill)
- Generating weekly performance status reports (use `perf-weekly-status-report` skill)
- Analyzing a single test run without cross-version comparison

## Phase 1: Gather Inputs

**Ask the user:**

1. **Test type**:
   - `predefined-throughput-steps` - Gradual load increase throughput test, tablets or vnodes
   - `microbenchmark` - Simple query microbenchmark (perf-simple-query)

2. **Subtests (load types or variants)**:
   - predefined-throughput-steps: `read`, `write`, `mixed`, `read_disk_only`, or all of them
   - microbenchmark: `arm64`, `arm64-write`, `x86_64`, `x86_64-write`

3. **Versions**, and optionally the Argus run IDs per subtest:
   ```
   Load type: <load>
   Version A (name): <run_id>
   Version B (name): <run_id>
   ```

4. **Background** (optional): why the comparison is being made, and any related Jira ticket. It goes at the top of the report.

A *run ID* identifies one run. It is not the Argus *test ID* (the job), which is what the tables below list.

| Test | Argus Test ID |
|---|---|
| predefined-throughput-steps-i8g-tablets | `d6ebf1a5-135f-43fc-a7ba-0716b60dfa94` |
| predefined-throughput-steps-i8g-vnodes | `6ffbef10-7138-457c-b386-73574805ca00` |
| simple-query-weekly-microbenchmark_arm64 | `a0063c73-efcf-4878-988d-72af779dc59d` |
| simple-query-weekly-microbenchmark_arm64-write | `dcc1afa0-2225-468c-9f45-5cfc8486f7f8` |
| simple-query-weekly-microbenchmark_x86_64 | `03464849-60e8-46c8-91b9-955cdeb07ea6` |
| simple-query-weekly-microbenchmark_x86_64-write | `6e745123-cb53-482b-836c-0609bd36a4e6` |

**How subtests map to runs:**
- **predefined-throughput-steps:** each workload is a separate run under the same test ID, and one Jenkins build produces all of them with the same `build_number`. The workload is only visible in `test_method` (`PerformanceRegressionPredefinedStepsTest.test_<workload>_gradual_increase_load`); `subtest_name` and `test_name` are null. Group runs by `build_number`, and read the workload from `test_method`.
- **microbenchmark:** each variant is a separate Argus test (table above).

**Finding runs by version.** If the user gives versions but no run IDs, list the runs of the test with full metadata:

```bash
timeout 60 argus run list --test-id <test_id> --limit 50 --full --no-cache
```

`--full` adds `scylla_version`, `packages`, `cloud_setup`, `test_method`, `build_number` and `region_name`, so no per-run calls are needed. `--no-cache` matters for "latest X": the CLI caches responses. Do not pass `--non-interactive`; it blocks the Cloudflare token refresh.

**Run selection rules:**
- Take `passed` and `failed` runs: a run fails on a threshold violation but still has results. Skip `test_error` and `aborted` runs.
- Identify the build by the `scylla-server` package (`version`, `date`, `revision_id`), not by `scylla_version`. Every master build reports the same `scylla_version` (e.g. `2026.4.0~dev`), and `scylla-server-target` often has a null date and revision.
- An exact version (e.g. `2026.3.2`) means that build only. If a subtest has no completed run on it, leave the cell empty and say so; don't substitute another version.
- "Latest 2026.3" means the newest release build of that line by package date. Exclude `~dev` builds unless the user asks for them.
- Prefer scheduled builds over manual reruns. `started_by` can't tell them apart, because the perf trigger sets `requested_by_user` to a fixed user. Check the Jenkins build cause instead (Jenkins API, credentials from the KeyStore `jenkins.json`): scheduled builds are started by the upstream trigger job, reruns say "Rebuilds build #N".
- If two subtests ran different builds of the same version, say so in the report.

## Phase 2: Fetch Run Metadata

`argus run list --full` already returns the metadata. For a single run, use the REST API with KeyStore credentials:

```python
from sdcm.keystore import KeyStore
import requests

creds = KeyStore().get_argus_rest_credentials_per_provider()
headers = {'Authorization': f'token {creds["token"]}', 'Accept': 'application/json', 'Content-Type': 'application/json'}
headers.update(creds.get('extra_headers', {}))
url = f'{creds["baseUrl"]}/api/v1/client/testrun/scylla-cluster-tests/{run_id}/get'
data = requests.get(url, headers=headers, timeout=30).json()['response']
```

Fields used by the report:

| Report field | Argus field |
|---|---|
| Test Method | `test_method` |
| Backend | `cloud_setup.backend` |
| Region | `region_name` (a list) |
| Instance Type, Node Count | `cloud_setup.db_node.instance_type`, `.node_amount` |
| Loaders | `cloud_setup.loader_node.instance_type`, `.node_amount` |
| AMI | `cloud_setup.db_node.image_id` |
| SCT Branch | `branch_name` |
| Scylla Version | `scylla-server` entry of `packages` |
| Date | `start_time` |
| Tablets / vnodes | the test name (`...-tablets` / `...-vnodes`); not in the run metadata |

For microbenchmarks, `loader_node` and `monitor_node` appear with `node_amount: 0`. Leave them out of the configuration.

## Phase 3: Fetch Linked Issues

```bash
timeout 60 argus issue list --run-id <run_id>
```

Take issue titles from Jira, not from Argus: Argus stores the title as it was when the issue was linked. Mention linked issues in the Summary of the subtest they explain.

## Phase 4: Fetch Test Results

```bash
timeout 60 argus run results --run-id <run_id>
```

Every cell has a `status`: `PASS` (within threshold), `ERROR` (threshold exceeded, which is why the run failed) or `UNSET` (no threshold). Always name `ERROR` cells in the Summary.

### Predefined-throughput-steps

One result table per step, named `<workload> - <step> - latencies`; split the name on `" - "` to get the step. Tables named `... - stalls - ...` hold reactor stalls; leave them out of the metric tables. Each table has one row per cycle (`Cycle #1`, ...), with cells:

- `P99 read` / `P99 write` - P99 latency in ms
- `P90 read` / `P90 write` - P90 latency in ms
- `Throughput read` / `Throughput write` - ops/s

**Steps to columns:**
- Each workload has its own steps (e.g. read has 5, write has 3). Build the columns per workload.
- Sort steps numerically, with the unthrottled step(s) last.
- One column per step when each step has one cycle. When a step has several cycles, or a step was repeated as `unthrottled_1`, `unthrottled_2`, ..., each cycle or repeat is its own column (`<step>_<n>`).
- **Max throughput** comes only from the unthrottled step(s): throttled steps just reach their target rate. For `mixed`, it is `Throughput read + Throughput write`.

### Microbenchmark

One table per run, `read - Perf Simple Query` or `write - Perf Simple Query`, with one row (`#1`). Its `description` is a JSON string of the benchmark parameters (concurrency, partitions, CPUs, duration). Cells:

- `median tps` (primary), `max tps`, `min tps`, `mad tps`
- `allocs_per_op`, `cpu_cycles_per_op`, `instructions_per_op`, `tasks_per_op`

## Phase 5: Build the HTML Report

The skeleton, CSS and chart code are in [html-template.md](references/html-template.md), and [example-report.html](references/example-report.html) is a complete page. Sections, in order:

1. **`<h1>`**: `<Workload> Workload: <focus> with <v1> and <v2> Scylla versions`, or `All Workloads: ...` for several workloads. List every compared version.
2. **Test line**: e.g. `<p><strong>Test:</strong> Predefined Throughput Steps (tablets) on 3 × i8g.4xlarge</p>`
3. **Background** (if given): why the comparison was made, with the Jira link.
4. **`<h2>Test Configuration</h2>`**: Parameter / Value table with Test Method (in `<code>`; for several workloads use the `test_<workload>_gradual_increase_load` pattern), Backend, Region, Instance Type, Node Count, Loaders, Replication (tablets or vnodes), SCT Branch and Workloads. If runs differ in a parameter, list the value per version.
5. **`<h2>Scylla Versions &amp; Packages</h2>`**: one row per run with Workload, Scylla Version (the full `<version>-<date>.<revision>`), Run ID (linked to Argus), Status, Start and AMI.
6. **Metric sections.** With one workload, each metric gets an `<h2>`. With several workloads, each workload gets an `<h2><Workload> Workload</h2>` and its metrics get `<h3>`:
   - predefined-throughput-steps: `P99 Latency (ms) – Read`, `P99 Latency (ms) – Write` (only for workloads with writes), and `Max Throughput (op/s) – Read`, `– Write` or `– Read + Write` (mixed).
   - microbenchmark: `Median TPS`, `MAD TPS`, `CPU Cycles per Op`, `Instructions per Op`, `Allocs per Op`, `Tasks per Op`. The columns are the variants (`x86_64`, `arm64`).

   Each metric has a table, then a noise note (`<p class="noise">`), then a bar chart.
7. **Summary**: a `<div class="summary">` bullet list, one per workload when there are several.

### Metric Table Rules

- Rows are versions, ordered by version number (then package date), not by run date.
- Columns are steps, cycles or variants. The first column is the version, and numbers use `<td class="num">`.
- Highlight per column: `best` (green, bold) and `worst` (red). For latency and per-op metrics lower is best; for throughput and TPS higher is best. Highlight only differences beyond the noise threshold; with two versions, a column within noise gets no highlight.
- Values: latency with 2 decimals; throughput and TPS with thousands separators; per-op with 2 decimals.

### Noise

A difference counts only if it is bigger than the noise threshold. Print the threshold under each table.

- **predefined-throughput-steps:** the threshold is the larger of 5% and the run-to-run spread of the same workload and step over the test's recent history (about 30 runs). Measure the spread as `1.4826 × MAD / median`. Typical unthrottled throughput spread: about 5% for write, 7% for read, 8% for mixed, 11% for read_disk_only.
- **microbenchmark:** TPS varies about 8-10% on x86_64 (c7i.large) and under 1% on arm64 (c7g.large). Per-op metrics vary much less, so a TPS change with unchanged per-op work is noise or a system effect, not code.

### Bar Chart Rules

- One grouped bar chart per metric table, placed under the noise note: x-axis = the table's columns, one dataset per version.
- **P99 latency charts use a logarithmic y-axis**, because the unthrottled P99 (often 100+ ms) otherwise flattens the throttled steps (2-4 ms). Throughput and TPS charts use a linear y-axis from zero.
- The chart data must be the same numbers as the table: generate both from the same data.
- Draw the charts only once `window.Chart` exists (the template's `whenChartReady`). HTML previewers can run inline scripts before Chart.js loads.
- Use the version colors from the template, in the same version order everywhere.

### Summary Rules

Bullets in the form `<strong>Best P99 read latency:</strong> 2099.1.4 at 400K (3.05 vs 3.31 ms, +8.5%)`:
- The best and worst version per metric, with the value or range. When no difference is beyond noise, say "no difference beyond noise" instead of naming a winner.
- Anomalies: a single-step spike, or a version that differs from the rest on one metric only.
- `ERROR` cells and failed runs, with the linked issue (Jira key and title from Jira).
- A short interpretation where the data supports one, checked against the test's run history: a one-off result in one run is a reason to rerun, not a regression.

### Consistency Rules

- Use one label per version everywhere (h1, tables, charts, Summary): `<version>` for releases, `<version>-<date>` for dev builds (e.g. `2026.3.0.dev-20260710`). The full build string goes in the Scylla Versions & Packages table.
- The h1 lists every compared version.
- Summary numbers must match the tables: for `mixed`, throughput is read + write.
- Keep the same version order in every table and chart.

## Phase 6: Save and Deliver

1. Save to `~/Downloads/perf-comparison-<test>-<workload or all>-<v1>-<v2>[-<v3>]-<YYYY-MM-DD>.html`, e.g. `perf-comparison-predefined-throughput-steps-all-2099.1.4-2099.2.1-2099-01-20.html`. Several subtests go on one page.
2. Give the user the path.

## Argus URL Format

```
https://argus.scylladb.com/tests/scylla-cluster-tests/{run_id}
```

## Reference Index

| File | Content |
|------|---------|
| [html-template.md](references/html-template.md) | Page skeleton, CSS, table markup and Chart.js code |
| [example-report.html](references/example-report.html) | Complete example page (all workloads, two versions, synthetic data) |
| [generate-report.md](workflows/generate-report.md) | Step-by-step workflow |

## Success Criteria

- [ ] Versions and run IDs were confirmed with the user, or found with the run selection rules
- [ ] Test Configuration and Scylla Versions & Packages tables are filled from Argus metadata
- [ ] Every Run ID links to its Argus page
- [ ] One table, noise note and bar chart per metric; versions as rows, steps/cycles/variants as columns
- [ ] Best and worst values are highlighted only beyond noise, in the right direction for each metric
- [ ] The Summary names the best and worst version per metric, any `ERROR` cells and linked issues, and doesn't call a difference within noise a regression
- [ ] Version labels, version order and numbers are consistent across the h1, tables, charts and Summary
- [ ] The page opens in a browser with readable charts
