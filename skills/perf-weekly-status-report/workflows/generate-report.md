# Generate Weekly Performance Status Report

Step-by-step process for generating the Gmail-compatible HTML performance report.

## Input Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `DAYS` | 7 | Number of days to look back for runs |

## Phase 1: Collect Run Data

**Entry criteria:** Argus CLI is available and authenticated.

0. Resolve and authenticate the CLI first. Use `argus` from `PATH` (`which argus`) -- never a
   hardcoded path under someone's home directory. Then make one cheap call so the interactive
   browser login happens once, up front, rather than in the middle of the fan-out:
   ```bash
   argus run list --test-id d6ebf1a5-135f-43fc-a7ba-0716b60dfa94 --limit 1 --url https://argus.scylladb.com
   ```
   If the output says `Waiting for login...`, tell the user a browser login is needed and wait.

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
     --no-cache \
     --url https://argus.scylladb.com
   ```

   Use `--no-cache` to ensure fresh data. Run status and issue linkage can change between
   refreshes (e.g., a run status updated from `failed` to `passed`, or an issue linked to a
   previously uninvestigated run).

3. Parse the JSON output. Each run object contains:
   - `id` (run UUID)
   - `scylla_version` (version string)
   - `status` (passed/failed/test_error)
   - `build_number`
   - `packages[]` (installed packages)

**Exit criteria:** JSON data collected for all 16 tests.

## Phase 1a: Ask Build Type (if not specified)

**Entry criteria:** Raw run data collected (Phase 1 complete).

If the user has not specified the build type in their prompt, ask them to choose:
1. **Master (~dev)** -- dev builds from master branch
2. **Release** -- stable release builds

Use the interactive question tool. If the user already said "release" or "master" in their prompt, skip this step.

**Exit criteria:** Build type is determined (master or release).

## Phase 2: Filter by Build Type

**Entry criteria:** Raw run data collected, build type determined.

Apply the version regex filter based on the chosen build type:

- **Master mode:** `^\d{4}\.\d+\.\d+.+dev$` -- matches `2026.3.0~dev`, `2025.2.0~dev`
- **Release mode:** `^\d{4}\.\d+\.\d+$` -- matches `2026.2.3`, `2026.1.10`

Example filter logic:
```python
import re
MASTER_RE = re.compile(r"^\d{4}\.\d+\.\d+.+dev$")
RELEASE_RE = re.compile(r"^\d{4}\.\d+\.\d+$")

version_re = MASTER_RE if build_type == "master" else RELEASE_RE
filtered_runs = [
    run for run in all_runs
    if version_re.match(run.get("scylla_version") or "")
]
```

Also include runs with status `"running"`.

**Exit criteria:** Only runs matching the chosen build type remain per test.

## Phase 3: Fetch Results Per Run

**Entry criteria:** Filtered runs identified (master or release).

1. For each filtered run, fetch results:
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

5. **Runs with an empty results array still need a workload.** A `test_error` run that died during
   provisioning has no tables, so step 1 cannot classify it -- and skipping it would hide most of a
   bad week. Recover the workload from Jenkins:

   ```bash
   # run object carries build_id + build_number
   # Jenkins MCP: get_build_parameters(fullname=<build_id>, number=<build_number>)
   # -> sub_tests = ["test_read_gradual_increase_load", "test_write_gradual_increase_load"]
   ```

   Sub-tests run sequentially, one Argus run each. Sort that build's runs by `start_time` and zip
   positionally against `sub_tests`. Validate against the runs in the same build that *do* have
   tables -- their table-name workload must match their assigned position; if not, report the
   ambiguity instead of guessing.

   Then find the failure cause using `argus run events`:

   ```bash
   argus run events --run-id <RUN_UUID> --url https://argus.scylladb.com
   ```

   This returns CRITICAL/ERROR events. Look for `CapacityReservationError` in the message text.
   This is more reliable than Jenkins console output, which often requires authentication (403).

6. **Detect re-runs for CapacityReservationError failures.**

   Group runs by test name + version. Within each group, sort by `build_number`. If a later build
   exists for the same test + version with runs that passed for the same workloads, the earlier
   CapacityReservationError run was successfully re-run. Exclude it from all report sections
   (overview, detailed results, counts, uninvestigated table).

   Note: There is no explicit "re-run of build #X" field in Argus. The link is inferred by
   matching test + version + sequential build numbers.

**Exit criteria:** Data organized by category > test > workload > step, throughput tracker populated.
Every master run in the window is accounted for -- including those with zero result tables.
CapacityReservationError runs with successful re-runs are marked for exclusion.

## Phase 4a: Collect Issues

**Entry criteria:** Filtered runs identified (from Phase 2).

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

4. **Classification: New vs Reproduced -- look it up, don't ask**

   Fetch each issue from Jira via the Atlassian MCP `getJiraIssue` tool with
   `fields: ["summary","status","created","updated","resolution"]`, then compare `created` to the
   report window:
   - `created` **inside** the window -> **New Issues - Regression**
   - `created` **before** the window -> **Reproduced Issues**

   `status` / `resolution` fill the State column. `updated` supports any "no progress since last
   week" claim -- verify it rather than asserting it. Only fall back to asking the user if Jira is
   unreachable.

5. **If `argus issue list` returns `[]` for every failed run** -- which is common, since linking runs
   to tickets is manual -- do not report "no issues". Ask the user whether a known ticket covers the
   failure, then confirm that ticket's `created` date via Jira as above. State in your summary that
   the attribution came from the user, not from Argus.

6. Store the classification for use in the HTML report generation.

7. **Build the Uninvestigated Failures list.** For each run with status `failed` or `test_error`
   that has no linked issues (empty `argus issue list`), add it to the uninvestigated list with:
   - Test name, workload, version, status
   - Cause: determined from `argus run events` (e.g. "CapacityReservationError, not re-run") or
     from failed result tables (e.g. "P99 ERROR at 750K step") or "All tables PASS, run marked failed"
   - For CapacityReservationError re-runs that also failed, exclude intermediate attempts and keep
     only the latest one, noting all build numbers in the cause (e.g. "CapacityReservationError,
     re-run also failed (builds #57,#59,#61)")
   - Argus link to the specific run
   - Exclude CapacityReservationError runs that were successfully re-run (identified in Phase 4 step 6)

   **Important:** Issue linkage in Argus is manual and may change between data refreshes. When
   re-collecting data, always use `--no-cache` on `argus issue list` calls. A run that was previously
   uninvestigated may now have an issue linked -- remove it from the uninvestigated list.

**Exit criteria:** Issues collected and classified as new vs reproduced from Jira creation dates
(user consulted only for unlinked failures or if Jira is unreachable).

## Phase 5: Generate HTML Report

**Entry criteria:** Data grouped and aggregated.

1. Generate the HTML file with these sections:
   - Header (solid navy background `#1a237e`, title, date range). Subtitle text varies by build type: "Master (~dev) builds only" or "Release builds only".
   - Summary box (total/passed/failed/running counts per run + Scylla version). **Count individual runs, not test groups.** Each workload is a separate run (e.g., a test with mixed/read/write/read_disk_only = 4 runs). Microbenchmarks = 1 run each. Total must equal Passed + Failed/Error + Running. **Exclude CapacityReservationError runs that were successfully re-run from all counts.**
   - Conclusion (auto-generated hierarchical text summary)
   - Uninvestigated Failures table (failed/test_error runs with no linked issue)
   - New Issues - Regression (issues created during the period, if any)
   - Reproduced Issues (pre-existing issues seen again this week)
   - Overview table (grouped by workload, with Argus links in Link column)
   - Detailed results (per-category tables with metrics + Argus links)

   **Conclusion section** (between Summary and Overview):
   - Heading "Conclusion" must use same style as "Summary" heading: `font-size:16px;font-weight:bold;padding-bottom:10px;`
   - **Warning banner** (optional): After collecting all issues from failed runs, present the
     de-duplicated list to the user and ask which issues (if any) should be highlighted in a
     warning banner. If the user selects issues, render a banner stating those issues had no
     updates during the report period. If the user selects none, omit the banner entirely.
     Banner markup:
     ```html
     <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:10px;"><tr>
     <td bgcolor="#fff3cd" style="background-color:#fff3cd;border:2px solid #dc3545;padding:8px 12px;font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#dc3545;font-weight:bold;">
     &#9888; No updates on {issue_keys} during last week.
     </td></tr></table>
     ```
     This banner uses yellow background with red border and red bold text to stand out visually.
   - Auto-generate **hierarchical** bullet-point lines summarizing the week's performance results
   - Structure uses two levels:
     - **Top-level items**: Test name in bold, prefixed with `- ` (indented 15px from left)
     - **Sub-items**: Specific observations, prefixed with `&#8226;` bullet (indented 30px from left)
   - Each top-level item and sub-item is rendered as its own table row
   - **Version numbers must be bold** in sub-items (e.g., `<b>2026.2.3</b>`)
   - Include: which workloads failed/passed for each test, specific failure details (metric, value)
   - Do NOT mention CapacityReservationError runs in the Conclusion -- they are covered in the Uninvestigated Failures table. If a test only had CapacityReservationError runs, omit it from the Conclusion entirely.
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
   - **CRITICAL**: The entire "Detailed Results" section is ONLY shown when there are actual failed result tables (table status `FAIL` or `ERROR`)
   - Runs where status is `failed` but all result tables show `PASS`, and `test_error` runs with no result tables, must NOT appear in Detailed Results -- they belong in the Uninvestigated Failures table
   - When all tests pass (or failures have no failed tables), completely omit the Detailed Results section from the report
   - When failures with actual failed tables exist:
     - First collect all failed result tables across all runs, grouped by category and test
     - Only render category headings and test sub-headings that have at least one failed table
     - Category heading with blue underline (`#007bff`)
     - For each test with failed tables: sub-heading with test name and full version, NO status badge
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
   - **Copy the table markup from [references/html-template.md](../references/html-template.md) -- do not invent your own.**
     Data tables there use `border="1"` on the table plus an explicit `border:1px solid #dee2e6` on
     every `<th>`/`<td>`. CSS-only borders (e.g. `border-bottom` on a cell of a `border="0"` table)
     get stripped, and the table renders as unaligned floating text with no gridlines.
   - All styles inline: `style="..."` on each element
   - Use `<table>` for layout, not CSS grid/flex
   - Badges: one-cell table with `bgcolor` on the `<td>` (see rule 5) -- not a bare `<span>`
   - Dark header row (`bgcolor="#343a40"`, white bold text), alternating row backgrounds
     (`#ffffff` / `#f8f9fa`) with `bgcolor` set on each cell
   - No `<style>` tags, no `class` attributes, no `border-radius`
   - Content table width: `width="700"` (not 1400)
   - Font-family: Arial,Helvetica,sans-serif on each cell
   - Always use `bgcolor` attribute alongside `background-color` style

5. Status badge format -- `bgcolor` on a real `<td>`, so white text can never land on a
   white background if the CSS background is stripped:
   ```html
   <table cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;"><tr>
     <td bgcolor="#28a745" align="center" style="background-color:#28a745;padding:3px 10px;font-size:11px;font-weight:bold;color:#ffffff;font-family:Arial,Helvetica,sans-serif;">PASSED</td>
   </tr></table>
   ```

6. Color scheme:
   - Passed: `#28a745` (green)
   - Failed: `#dc3545` (red)
   - Error: `#fd7e14` (orange)
   - No runs: `#6c757d` (gray)

**Exit criteria:** Conclusion text printed to user and confirmed/edited. Issues classified (from Jira `created` dates; user asked only if Jira is unreachable). File `perf-weekly-status-report.html` written outside the SCT repo and renderable in a browser.

### Verify the rendering, don't assume it

Open the generated file in a browser and look at it before drafting any email. A `file://` URL may be
blocked by browser-automation tooling; serve it over loopback instead:

```bash
cd <output-dir> && python3 -m http.server 8791 --bind 127.0.0.1
# then screenshot http://127.0.0.1:8791/perf-weekly-status-report.html and stop the server
```

Check specifically that every data table shows gridlines and that each status badge shows coloured
fill behind white text -- these are the two things that break when the markup drifts from
`references/html-template.md`.

## Phase 5b: Email Draft (Optional)

Only when the user asks for an email draft.

1. Find the previous week's message to reuse its audience -- search Gmail for
   `subject:"Weekly performance status"` and take the most recent thread.
2. Reuse that message's recipients exactly (check `cc`/`bcc` too, not just `to`).
3. Subject follows the series format: `Weekly performance status, MM/DD/YYYY` (the report end date).
4. Match the established header wording so the series stays consistent:
   `ScyllaDB Enterprise - Performance Weekly Status` / `Period: YYYY-MM-DD to YYYY-MM-DD | Master (~dev) builds only`.
5. Pass the report HTML as the draft's `htmlBody`, **and** supply a plain-text `body` that carries the
   same Summary / Conclusion / Overview content -- some clients show only the text alternative.
6. **Create a draft only. Never send.** Sending on the user's behalf needs explicit per-message
   consent, and this report goes to a wide internal audience.

> **Cost note:** the draft tool takes inline content only, so the whole HTML is retyped as a tool
> argument. Keep the generated HTML lean -- set `font-family` once on the outer table rather than
> repeating it on all ~80 cells -- or the draft call becomes slow and expensive.

## Phase 5a: Conclusion and Issues Review (Interactive)

**Entry criteria:** HTML report content is ready to be generated (all data collected and processed, issues collected from Phase 4a).

Before writing the final HTML file, the agent MUST perform THREE interactive steps:

### Step 1: Conclusion Review

1. Print the auto-generated Conclusion bullet points to the user in plain text format (hierarchical structure)
2. Also print the **Uninvestigated Failures** table (failed/test_error runs with no linked issue, including cause)
3. Ask the user to confirm the conclusion or provide edits, and to investigate the uninvestigated failures
4. Wait for user response
5. If the user approves: proceed to Step 2
6. If the user provides changes: incorporate the edits

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

Uninvestigated failures (no issue linked):

| Test | Workload | Version | Status | Cause | Link |
|------|----------|---------|--------|-------|------|
| predefined-throughput-steps-i8g-tablets | mixed | 2026.1.10 | failed | P99 ERROR at 750K step | Argus |
| latency-650gb-with-nemesis-i8g-vnodes | mixed | 2026.2.3 | test_error | CapacityReservationError, not re-run | Argus |

Would you like to use this conclusion as-is, or would you like to edit it?
```

### Step 2: Warning Banner Selection

1. Present the de-duplicated list of all issues found on failed/errored runs
2. Ask the user which issues (if any) should be highlighted in a warning banner at the top of the Conclusion
3. Wait for user response
4. If the user selects issues: generate a warning banner stating those issues had no updates during the report period
5. If the user selects none (or there are no issues): omit the warning banner entirely

Example interaction:
```
I found the following issues linked to failed runs:

1. SCYLLADB-2353: Tablet load-balancer oscillation causes P99 latency explosion...
2. SCYLLADB-3459: Oversized allocation (262144 bytes) in circular_buffer...

Which of these issues (if any) should be highlighted in a warning banner
stating they had no updates this week? Provide the keys (e.g., "SCYLLADB-2353, SCYLLADB-3459"),
or "none" to skip the banner.
```

### Step 3: Issue Classification

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

This ensures the user has final control over the conclusion wording, warning banner, and issue classification before they appear in the report.

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
19. Verify issues were classified from Jira `created` dates (user asked only for unlinked failures)
20. Verify report is NOT placed inside the SCT repository
21. Verify every data table has `border="1"` plus per-cell `border:1px solid #dee2e6` (no CSS-only borders)
22. Verify status badges use `bgcolor` on a `<td>`, not a bare `<span>` (`grep -c '<span[^>]*background-color'` must be 0)
23. Verify the page was actually opened in a browser and the tables/badges checked visually
24. Verify every master run in the window appears somewhere in the report, including runs with zero result tables
25. Verify Uninvestigated Failures table is present when there are failed/test_error runs with no linked issues
26. Verify Uninvestigated Failures table has columns: Test | Workload | Version | Status | Cause | Link
27. Verify CapacityReservationError runs that were successfully re-run are excluded from all sections
28. Verify test_error causes are determined via `argus run events`, not Jenkins console
29. Verify Uninvestigated Failures table was printed to user during conclusion review

**Exit criteria:** Report is ready for email distribution.

## Complete Example Command Sequence

```bash
# OUT = any directory outside the repo (agent scratchpad, or $HOME)
OUT=${OUT:-$HOME/perf-weekly}
mkdir -p "$OUT"

# 0. Resolve + authenticate the CLI (one cheap call, so the browser login happens up front)
which argus
argus run list --test-id d6ebf1a5-135f-43fc-a7ba-0716b60dfa94 --limit 1 \
  --url https://argus.scylladb.com >/dev/null

# 1. Set time window (default 7 days, override with DAYS=14 etc.)
DAYS=${DAYS:-7}
AFTER=$(date -d "${DAYS} days ago" +%s)

# 2. Collect data for one test (repeat for all 16)
argus run list \
  --test-id d6ebf1a5-135f-43fc-a7ba-0716b60dfa94 \
  --after $AFTER \
  --limit 200 \
  --full \
  --url https://argus.scylladb.com > "$OUT/runs.json"

# 3. Filter master (~dev) versions (in Python/jq)
# Keep only runs where scylla_version matches ^\d{4}\.\d+\.\d+.+dev$
# Expect most of the 16 tests to yield zero master runs -- that is normal.

# 4. Fetch results per filtered run
argus run results \
  --run-id <RUN_ID> \
  --url https://argus.scylladb.com > "$OUT/results.json"

# 4b. For runs whose results are empty (test_error), recover the workload from Jenkins
#     sub_tests order (build_id + build_number on the run object).
#     Determine error cause via argus run events (not Jenkins console):
argus run events \
  --run-id <RUN_ID> \
  --url https://argus.scylladb.com
#     Look for CapacityReservationError in event messages.

# 4c. Detect re-runs: group runs by test+version, sort by build_number.
#     If a later build passed the same workloads, exclude the earlier
#     CapacityReservationError run from the report.

# 5. Fetch issues for failed/errored runs (often returns [] -- linking is manual)
argus issue list \
  --run-id <RUN_ID> \
  --url https://argus.scylladb.com

# 6. Generate HTML report (output to $OUT, NOT to repo)
# - Ask user to confirm conclusion text + uninvestigated failures table
# - Classify issues from Jira `created` dates (Atlassian MCP getJiraIssue)
# - Write "$OUT/perf-weekly-status-report.html"

# 7. Verify rendering before drafting any email
cd "$OUT" && python3 -m http.server 8791 --bind 127.0.0.1   # screenshot, then stop
```
