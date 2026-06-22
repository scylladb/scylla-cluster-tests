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

## Phase 4a: Collect Issues

**Entry criteria:** Filtered master runs identified (from Phase 2).

1. For each run with status `failed` or `test_error`, fetch linked issues:
   ```bash
   argus issue list \
     --run-id <RUN_ID> \
     --url https://argus.scylladb.com
   ```

2. Each issue object contains:
   ```json
   {
     "key": "<PROJECT>-<NUMBER>",
     "subtype": "jira",
     "title": "<issue summary text>",
     "state": "<state>",
     "url": "https://scylladb.atlassian.net/browse/<PROJECT>-<NUMBER>"
   }
   ```
   The array may contain zero, one, or many issues of any Jira project.

3. De-duplicate issues by `key` (same issue may appear on multiple runs).

4. **Classification: New vs Reproduced**

   Since the Argus CLI does not expose Jira issue creation dates, the agent MUST present the full de-duplicated issue list to the user and ask them to classify each issue:
   - **New Issues - Regression**: Jira ticket was created during the report period (newly filed regression)
   - **Reproduced Issues**: Jira ticket existed before the report period but was reproduced this week

   Example interaction:
   ```
   I found the following issues linked to failed runs this week:

   1. PROJECT-123: <issue title>
   2. PROJECT-456: <issue title>
   3. PROJECT-789: <issue title>

   Which of these are NEW issues (Jira ticket created this week)?
   Please provide the issue keys that are new (e.g., "PROJECT-789"), or "none" if all are reproduced.
   ```

5. Store the classification for use in the HTML report generation.

**Exit criteria:** Issues collected and classified as new vs reproduced based on user input.

## Phase 5: Generate HTML Report

**Entry criteria:** Data grouped and aggregated.

1. Generate the HTML file with these sections:
   - Header (solid navy background `#1a237e`, title, date range)
   - Summary box (total/passed/failed counts per run + Scylla version). **Count individual runs, not test groups.** Each workload is a separate run (e.g., a test with mixed/read/write/read_disk_only = 4 runs). Microbenchmarks = 1 run each. Total must equal Passed + Failed/Error.
   - Conclusion (auto-generated hierarchical text summary)
   - New Issues - Regression (issues created during the period, if any)
   - Reproduced Issues (pre-existing issues seen again this week)
   - Overview table (grouped by workload, with Argus links in Link column)
   - Detailed results (per-category tables with metrics + Argus links)

   **Conclusion section** (between Summary and Overview):
   - Heading "Conclusion" must use same style as "Summary" heading: `font-size:16px;font-weight:bold;padding-bottom:10px;`
   - Auto-generate **hierarchical** bullet-point lines summarizing the week's performance results
   - Structure uses two levels:
     - **Top-level items**: Test name in bold, prefixed with `- ` (indented 15px from left)
     - **Sub-items**: Specific observations, prefixed with `&#8226;` bullet (indented 30px from left)
   - Each top-level item and sub-item is rendered as its own table row
   - Include: which workloads failed/passed for each test, specific failure details (metric, value)
   - Do NOT mention registered tests with no runs
   - Example output structure:
     ```
     - predefined-throughput-steps-i8g-tablets:
       * write workload failed with P99 latency regression at 600K op/s step (225.44ms).
       * Read workload failed with P99 spike at 1.5M op/s step (8933.87ms).
       * Mixed and read_disk_only workloads passed.
     - Microbenchmark:
       * write tests (arm64 and x86_64) both failed with ERROR on instructions_per_op (~8% regression).
       * Read tests passed on both architectures.
     ```
   - Render in a white-background box with border
   - **CRITICAL: Before saving the report, print the generated conclusion text to the user and ask them to confirm or provide edits.** Wait for user response. If the user provides changes, incorporate them. Only then write the final HTML file.

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
        - Columns: Workload | Step | P99 (ms) | Throughput (op/s) | Version | Link
        - Shows all failed steps across ALL runs in the period
        - P99 values highlighted in red bold
      - **Max Throughput table** (ONLY for `predefined-throughput-steps` tests where the **unthrottled step itself** has status `FAIL` or `ERROR`):
        - If all unthrottled steps pass (status `PASS`), **omit this table entirely** -- the throughput is as expected and does not need to be highlighted
        - Columns: Workload | Max Throughput (run) | P99 (ms) | Status | Link
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

**Exit criteria:** Conclusion text printed to user and confirmed/edited. Issues classified by user. File `perf-weekly-status-report.html` written to /tmp/opencode/ (NOT in the SCT repo) and renderable in a browser.

## Phase 5a: Conclusion and Issues Review (Interactive)

**Entry criteria:** HTML report content is ready to be generated (all data collected and processed, issues collected from Phase 4a).

Before writing the final HTML file, the agent MUST perform TWO interactive steps:

### Step 1: Conclusion Review

1. Print the auto-generated Conclusion bullet points to the user in plain text format (hierarchical structure)
2. Ask the user to confirm the conclusion or provide edits
3. Wait for user response
4. If the user approves: proceed to Step 2
5. If the user provides changes: incorporate the edits

Example interaction:
```
Here is the generated Conclusion for the report:

- predefined-throughput-steps-i8g-tablets:
  * write workload failed with P99 latency regression at 600K op/s step (225.44ms).
  * Read workload failed with P99 spike at 1.5M op/s step (8933.87ms).
  * Mixed and read_disk_only workloads passed.
- Microbenchmark:
  * write tests (arm64 and x86_64) both failed with ERROR on instructions_per_op (~8% regression).
  * Read tests passed on both architectures.

Would you like to use this conclusion as-is, or would you like to edit it?
```

### Step 2: Issue Classification

1. Present the de-duplicated list of all issues found on failed/errored runs
2. Ask the user to identify which issues are **new** (Jira ticket created during the report period)
3. Wait for user response
4. Classify accordingly:
   - Issues identified as new → "New Issues - Regression" section
   - Remaining issues → "Reproduced Issues" section

Example interaction:
```
I found the following issues linked to failed runs:

1. PROJECT-123: <issue title>
2. PROJECT-456: <issue title>
3. PROJECT-789: <issue title>

Which of these are NEW issues (Jira ticket created this week)?
Please provide the keys (e.g., "PROJECT-789"), or "none" if all are reproduced.
```

This ensures the user has final control over both the conclusion wording and issue classification before they appear in the report.

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
15. Verify Max Throughput table is omitted when all unthrottled steps pass (throughput as expected)
16. Verify Conclusion text was shown to user before final save
17. Verify Conclusion uses hierarchical format (bold test names + indented sub-bullets)
18. Verify issues are split into "New Issues - Regression" and "Reproduced Issues" sections
19. Verify user was asked to classify issues before final save
20. Verify report is NOT placed inside the SCT repository

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

# 5. Fetch issues for failed/errored runs
argus issue list \
  --run-id <RUN_ID> \
  --url https://argus.scylladb.com

# 6. Generate HTML report (output to /tmp/opencode/, NOT to repo)
# - Ask user to confirm conclusion text
# - Ask user to classify issues as new vs reproduced
# - Write /tmp/opencode/perf-weekly-status-report.html
```
