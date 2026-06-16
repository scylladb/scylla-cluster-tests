# Generate Weekly Performance Status Report

Step-by-step process for generating the Gmail-compatible HTML performance report.

## Input Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `DAYS` | 7 | Number of days to look back for runs |

## Phase 1: Collect Run Data

**Entry criteria:** Argus CLI is available and authenticated.

1. Calculate the time window (default 7 days, user can override):
   ```bash
   DAYS=${DAYS:-7}
   AFTER=$(date -d "${DAYS} days ago" +%s)
   ```

2. For each test in the registry, list runs:
   ```bash
   argus run list \
     --test-id <TEST_ID> \
     --after $AFTER \
     --limit 200 \
     --full \
     --url https://argus.scylladb.com
   ```

3. Parse the JSON output. Each run object contains:
   - `id` (run UUID)
   - `scylla_version` (version string)
   - `status` (passed/failed/test_error)
   - `build_number`
   - `packages[]` (installed packages)

**Exit criteria:** JSON data collected for all 16 tests.

## Phase 2: Filter to Master (~dev) Builds

**Entry criteria:** Raw run data collected.

1. Apply version regex filter: `^\d{4}\.\d+\.\d+.+dev$`
2. This matches versions like `2026.3.0~dev`, `2025.2.0~dev`
3. Excludes release versions like `2026.1.5`, `2025.1.13`, `None`

Example filter logic:
```python
import re
MASTER_RE = re.compile(r"^\d{4}\.\d+\.\d+.+dev$")

master_runs = [
    run for run in all_runs
    if MASTER_RE.match(run.get("scylla_version") or "")
]
```

**Exit criteria:** Only master (~dev) version runs remain per test.

## Phase 3: Fetch Results Per Run

**Entry criteria:** Filtered master runs identified.

1. For each master run, fetch results:
   ```bash
   argus run results \
     --run-id <RUN_ID> \
     --url https://argus.scylladb.com
   ```

2. Parse result tables. Each table has structure:
   ```json
   {
     "name": "write - 350000 - latencies",
     "status": "PASS",
     "rows": [{
       "name": "Cycle #1",
       "cells": {
         "P90 write": {"value": 1.59, "status": "PASS"},
         "P99 write": {"value": 2.21, "status": "PASS"},
         "Throughput write": {"value": 349772, "status": "UNSET"}
       }
     }]
   }
   ```

3. Skip "stalls" tables (name contains "stalls - REACTOR_STALLED") for the latency section.

**Exit criteria:** Results fetched for all master runs with non-empty data.

## Phase 4: Group and Aggregate Data

**Entry criteria:** All results fetched.

1. Parse table names into components:
   - `"write - 350000 - latencies"` -> workload=write, step=350000, type=latencies
   - `"mixed - Steady State - latencies"` -> workload=mixed, step=Steady State, type=latencies
   - `"mixed - _mgmt_repair_cli - latencies"` -> workload=mixed, step=_mgmt_repair_cli, type=latencies

2. Group by test category:
   - **i8g Tablets** -- tests with "i8g-tablets" in name
   - **i8g Vnodes** -- tests with "i8g-vnodes" in name
   - **i4i Tablets** -- tests with "tablets" (but not "i8g") in name
   - **i4i Vnodes** -- tests with "vnodes" (but not "i8g") in name
   - **Microbenchmarks** -- tests with "microbenchmark" in name

3. Compute full version with build date and revision hash for display:
   ```python
   def get_full_version(run):
       """e.g. 2026.3.0.dev.20260612.91ada5517d59"""
       for pkg in run.get("packages", []):
           if pkg.get("name") == "scylla-server-target":
               ver = pkg["version"].replace("~", ".")
               date = pkg.get("date", "")
               rev = pkg.get("revision_id", "")
               parts = [ver]
               if date:
                   parts.append(date)
               if rev:
                   parts.append(rev)
               return ".".join(parts)
       return run.get("scylla_version", "").replace("~", ".")
   ```

4. Important: Each run covers a single workload. Build a mapping of test_name -> workload -> [run entries].
   This means the Argus link for each workload points to the run that produced that workload's results.

**Exit criteria:** Data organized by category > test > workload > step, throughput tracker populated.

## Phase 5: Generate HTML Report

**Entry criteria:** Data grouped and aggregated.

1. Generate the HTML file with these sections:
   - Header (solid navy background `#1a237e`, title, date range)
   - Summary box (total/passed/failed counts per run + Scylla version). **Count individual runs, not test groups.** Each workload is a separate run (e.g., a test with mixed/read/write/read_disk_only = 4 runs). Microbenchmarks = 1 run each. Total must equal Passed + Failed/Error.
   - Conclusion (auto-generated text summary)
   - Overview table (grouped by workload, no Argus links, no version column)
   - Detailed results (per-category tables with metrics + Argus links)

   **Conclusion section** (between Summary and Overview):
   - Heading "Conclusion" must use same style as "Summary" heading: `font-size:16px;font-weight:bold;padding-bottom:10px;`
   - Auto-generate bullet-point lines summarizing the week's performance results
   - Each line starts with "- " and is rendered on its own line (use table rows)
   - Include: overall pass/fail status, any ERROR/FAIL steps
   - Do NOT mention registered tests with no runs
   - Example output:
     ```
     - All 5 tests with dev runs passed this week.
     ```
   - Render in a white-background box with border

2. **Overview table structure:**
   - Columns: Category | Test | Workload | Status | Link (NO version column, NO Runs column)
   - Group by category first, then test, then workload
   - Category shown only on first row of that category (empty on subsequent)
   - Test shown only on first workload row for that test (empty on subsequent)
   - Each run covers a single workload, so each workload has its own runs
   - **Microbenchmarks**: Use "-" as workload since they don't have separate workload results
   - Status column: just the status badge (PASSED/FAILED/ERROR) -- no counts
   - Link column: Argus link to the specific run for that workload
   - Full Scylla version is displayed in the Summary section title instead

   **Important**: When fetching results for microbenchmark runs, if no workload-specific tables are found (e.g., no "workload - step - latencies" tables), treat the entire results array as belonging to workload="-". This ensures microbenchmarks appear in the overview table.

3. **Detailed results structure (per category):**
   - **CRITICAL**: The entire "Detailed Results" section is ONLY shown when there are actual failures (run status `failed` or `test_error`)
   - When all tests pass, completely omit the Detailed Results section from the report
   - When failures exist:
     - Category heading with blue underline (`#007bff`)
     - For each test with failures: sub-heading with test name and full version, NO status badge
       Example: `predefined-throughput-steps-i8g-tablets (2026.3.0.dev.20260612.91ada5517d59)`
     - **Failed Results table**:
       - Columns: Workload | Step | P90 (ms) | P99 (ms) | Throughput (op/s) | Version | Link
       - Shows all failed steps across ALL runs in the period
       - P90/P99 values highlighted in red bold
      - **Max Throughput table** (ONLY for `predefined-throughput-steps` tests that have failures):
        - Columns: Workload | Max Throughput (run) | P90 (ms) | P99 (ms) | Status | Link
        - One row per workload, using latest run's data
        - Each workload has its own Argus link to its specific run
        - Argus link format: `https://argus.scylladb.com/test/{test_id}/runs?additionalRuns[]={run_id}` (singular `/test/`)
        - NOT shown for nemesis or rolling-upgrade tests (they have no unthrottled steps)

4. Key HTML rules for Gmail compatibility:
   - All styles inline: `style="..."` on each element
   - Use `<table>` for layout, not CSS grid/flex
   - Use `<span>` with inline background-color for badges
   - No `<style>` tags, no `class` attributes, no `border-radius`
   - Content table width: `width="700"` (not 1400)
   - Font-family: Arial,Helvetica,sans-serif on each cell
   - Always use `bgcolor` attribute alongside `background-color` style

5. Status badge format:
   ```html
   <span style="background-color:#28a745;color:#ffffff;padding:2px 8px;font-size:11px;font-weight:bold;display:inline-block;">PASSED</span>
   ```

6. Color scheme:
   - Passed: `#28a745` (green)
   - Failed: `#dc3545` (red)
   - Error: `#fd7e14` (orange)
   - No runs: `#6c757d` (gray)

**Exit criteria:** File `perf-weekly-status-report.html` written to /tmp/opencode/ (NOT in the SCT repo) and renderable in a browser.

## Phase 6: Verify Output

**Entry criteria:** HTML file generated.

1. Check file exists and has reasonable size (>5KB for a real report)
2. Verify no `<style>` tags present (Gmail would strip them)
3. Verify no `class=` attributes present
4. Verify no `border-radius` (Gmail strips it)
5. Verify `width="700"` is used for main content table (not 1400)
6. Verify `bgcolor` attributes present alongside `background-color`
7. Verify overview table DOES contain Argus links in Link column
8. Verify overview table has columns: Category | Test | Workload | Status | Link (no Runs column, no Version column)
9. Verify overview Status column shows just badge (PASSED/FAILED/ERROR) -- no counts
10. Verify microbenchmarks appear in overview with "-" as workload
11. Verify Summary title: "Summary for Scylla version {full_version}" with build date and revision hash
12. Verify detailed section has per-workload Argus links (format: `/test/` singular)
13. Verify full version with revision hash appears in Summary title (e.g., `2026.3.0.dev.20260612.91ada5517d59`)
14. Verify Detailed Results section is completely omitted when all tests pass
15. Verify report is NOT placed inside the SCT repository

**Exit criteria:** Report is ready for email distribution.

## Complete Example Command Sequence

```bash
# 1. Set time window (default 7 days, override with DAYS=14 etc.)
DAYS=${DAYS:-7}
AFTER=$(date -d "${DAYS} days ago" +%s)

# 2. Collect data for one test (repeat for all 16)
argus run list \
  --test-id d6ebf1a5-135f-43fc-a7ba-0716b60dfa94 \
  --after $AFTER \
  --limit 200 \
  --full \
  --url https://argus.scylladb.com > /tmp/opencode/runs.json

# 3. Filter master (~dev) versions (in Python/jq)
# Keep only runs where scylla_version matches ^\d{4}\.\d+\.\d+.+dev$

# 4. Fetch results per filtered run
argus run results \
  --run-id <RUN_ID> \
  --url https://argus.scylladb.com > /tmp/opencode/results.json

# 5. Generate HTML report (output to /tmp/opencode/, NOT to repo)
# python3 script generates /tmp/opencode/perf-weekly-status-report.html
```
