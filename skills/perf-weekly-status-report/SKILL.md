---
name: perf-weekly-status-report
description: >-
  Generate Gmail-compatible HTML performance weekly status reports from Argus CLI
  data. Use when asked to produce a weekly perf summary, create a performance
  status email, aggregate latency and throughput results across enterprise perf
  tests, or generate an HTML report for stakeholders. Covers predefined-throughput-steps,
  latency-650gb-with-nemesis, rolling-upgrade, and microbenchmark tests using
  argus run list and argus run results commands.
---

# Performance Weekly Status Report

Generate Gmail-compatible HTML reports summarizing ScyllaDB Enterprise performance test results from the past week using the Argus CLI.

## First Step: Ask the User Which Build Type to Report

**Before collecting any data, ask the user which build type to report on.**

Use the interactive question tool to present two options:
1. **Master (~dev)** -- Filter to dev versions matching `^\d{4}\.\d+\.\d+.+dev$`. These are builds from the master/development branch.
2. **Release** -- Filter to release versions matching `^\d{4}\.\d+\.\d+$` (no `dev` suffix). These are builds from stable release branches.

The chosen build type determines:
- The version filter regex applied in Phase 2
- The header subtitle text ("Master (~dev) builds only" vs "Release builds only")
- The output filename (`perf-weekly-status-report.html` vs `perf-weekly-status-report-release.html`)

If the user has already specified the build type in their prompt (e.g., "generate report for release versions"), skip the question and proceed with that choice.

## Essential Principles

### Version Filtering by Build Type

**Master mode:** Filter to dev versions matching `^\d{4}\.\d+\.\d+.+dev$` -- exclude release builds.

Release builds (`2026.1.5`) run on stable branches and represent prior releases. The master report only includes master/dev versions (e.g., `2026.3.0~dev`) because these represent the latest development state being validated.

**Release mode:** Filter to release versions matching `^\d{4}\.\d+\.\d+$` -- exclude dev builds.

Dev builds run on the master branch and represent unreleased code. The release report only includes release versions (e.g., `2026.2.3`, `2026.1.10`) because these represent validated stable releases. Note that release reports often span multiple versions (e.g., `2026.2.3` and `2026.1.10` in the same week).

### Use Argus CLI Directly

**The `argus` binary (Go CLI) is the data source -- not the Python argus client library.**

The Go-based `argus` CLI provides `run list` and `run results` subcommands that return JSON. This is faster and more reliable than the Python library for batch data collection. Always use `--url https://argus.scylladb.com` to target the production Argus instance.

Resolve the binary from `PATH` (`which argus`) -- do **not** hardcode a path under any particular user's home directory. A typical install lives in `~/.local/bin/argus`.

> **Name collision:** `argus --version` is not a valid flag. Verify with `argus --help` and confirm `run` and `issue` subcommands are listed.

### Authenticate Argus Before Collecting

**The first `argus` call opens a browser and blocks on `Waiting for login...`.**

This violates the repo-wide rule that commands must be non-interactive, and in a headless or automated run it hangs until timeout. Do a cheap pre-flight call before the batch collection so the login happens once, visibly, at a predictable moment:

```bash
argus run list --test-id <ANY_TEST_ID> --limit 1 --url https://argus.scylladb.com
```

If the output contains `Waiting for login...`, tell the user a browser login is required and wait for it to complete before proceeding. Never launch the 16-test collection loop as the first Argus call -- a login prompt mid-fan-out produces confusing partial failures.

### Gmail-Compatible HTML

**Output must render correctly in Gmail, which strips `<style>` blocks and external CSS.**

Gmail only supports inline CSS styles on elements. Never use `<style>` tags, `<link>` stylesheets, or CSS classes. All styling must be inline via `style="..."` attributes. Keep the HTML structure simple: use tables for layout, not divs with flexbox or grid.

### Overview Table: Group by Workload

**The overview table shows each workload as its own row. Columns: Category | Test | Workload | Status | Issues | Link.**

- Category column shows the category name only on the first row of that category group (empty on subsequent rows)
- Test column shows the test name only on the first workload row for that test (empty on subsequent rows)
- Workload column shows the actual workload name (mixed, read, write, read_disk_only). **For microbenchmarks use "-"** since they don't have separate workloads
- Status column shows just the status badge (PASSED/FAILED/ERROR/RUNNING) of the latest run -- no counts
- Issues column shows linked Jira issue keys as clickable links (comma-separated if multiple); empty for runs with no linked issues
- Link column shows an Argus link to the specific run for that workload
- Each run covers a single workload, so each workload has its own run(s)

**Per-version tables:** When the report spans multiple versions, create a separate Overview table for each version with a version label above it (e.g., "Version 2026.2.3"). When only a single version is present, show one table without a version label.

**Microbenchmark handling**: Microbenchmarks don't report results in the same table structure as performance tests. When a microbenchmark run has no workload-specific tables, use "-" as the workload value. Each microbenchmark test appears as a single row in the overview with workload="-".

**Runs column format:** Removed. Status column shows just the badge.

**Status column format:** Show just the status badge (PASSED/FAILED/ERROR/RUNNING) of the latest run for that workload. No counts.

### Running Tests

**Include runs with status `running` in the report if they match the version filter.**

Running tests should appear in the Overview table with a RUNNING status badge (blue, `#17a2b8`). They are counted separately in the Summary (not as passed or failed). Determine the workload from partial results (`argus run results`) if available, or from Jenkins `sub_tests` positional mapping. Do not include running tests in Detailed Results or Uninvestigated Failures.

### Runs With No Results: Recover the Workload From Jenkins

**A `test_error` run often has ZERO result tables, so its workload cannot be read from table names. Do not drop these runs.**

This is common and can dominate a week: a run that dies during provisioning has `end_time` of `1970-01-01`, an empty `results` array, and no field anywhere in the run object naming its workload. Since the skill's normal path derives workload from table names (`"<workload> - <step> - latencies"`), these runs would silently vanish from the overview -- making a week of failures look like a week with few runs.

Recover the workload from the Jenkins build that produced the run:

1. Each run object has `build_id` and `build_number`. Fetch that build's parameters (Jenkins MCP `get_build_parameters`, or the Jenkins REST API).
2. The `sub_tests` parameter is an ordered JSON list of the workloads that build ran, e.g.
   `["test_read_gradual_increase_load", "test_write_gradual_increase_load"]`.
3. Sub-tests execute **sequentially**, each creating one Argus run. Sort that build's runs by `start_time` and zip them positionally against `sub_tests`.
4. **Validate the mapping** against the runs in the same build that *do* have tables -- their table-name workload must match the position they were assigned. If it doesn't, stop and report the ambiguity rather than guessing.

Map sub-test names to workload labels: `test_read_gradual_increase_load` -> `read`, `test_write_gradual_increase_load` -> `write`, `test_mixed_gradual_increase_load` -> `mixed`, `test_read_disk_only_gradual_increase_load` -> `read_disk_only`, `test_latency_mixed_with_nemesis` -> `mixed`.

Also determine *why* the run produced nothing using `argus run events`:

```bash
argus run events --run-id <RUN_UUID> --url https://argus.scylladb.com
```

This returns CRITICAL and ERROR events for the run. The common cause is AWS capacity:

```
(TestFrameworkEvent Severity.CRITICAL) Failed to provision aws resources: CapacityReservationError: Failed to create capacity reservation in any availability zone.
```

State the real reason (e.g. `CapacityReservationError`) rather than reporting a bare `ERROR`.

### CapacityReservationError Runs: Detect Re-runs and Exclude

**If a `test_error` run caused by CapacityReservationError was successfully re-run, exclude it from the report entirely.**

To detect re-runs:
1. Group runs by test name + version.
2. Within each group, sort by `build_number`.
3. If a later build exists for the same test + version, and it has runs with matching workloads that passed, the earlier CapacityReservationError run was re-run successfully.
4. Exclude the original CapacityReservationError run from all report sections (overview, detailed results, counts).
5. If the re-run also failed with CapacityReservationError, exclude the intermediate attempts and keep only the latest one. Note this in the Uninvestigated Failures table cause column (e.g., "CapacityReservationError, re-run also failed (builds #57,#59,#61)").

Note: There is no explicit "re-run of build #X" field in Argus. The link is inferred by matching test + version + sequential build numbers.

**CapacityReservationError runs that were NOT re-run** (or whose re-run also failed) should appear in the **Uninvestigated Failures** table with the cause noted as "CapacityReservationError, not re-run". Do **not** list them in the Failed Results table -- they have no metrics to show.

### Uninvestigated Failures Table

**Show a table of all failed/test_error runs that have no linked Argus issue, placed between the Conclusion and Issues sections.**

This table helps the user quickly identify runs that need investigation. Columns: Test | Workload | Version | Status | Cause | Link.

- **Include**: Any run with status `failed` or `test_error` where `argus issue list` returns `[]`
- **Exclude**: CapacityReservationError runs that were successfully re-run (same test + version, later build passed)
- **Cause column**: Show the specific failure reason:
  - For `test_error` runs: the error from `argus run events` (e.g. "CapacityReservationError, not re-run")
  - For `failed` runs with failed tables: the specific metric failure (e.g. "P99 ERROR at 750K step")
  - For `failed` runs where all tables pass: "All tables PASS, run marked failed"
- The table heading is "Uninvestigated Failures (no issue linked)"
- Print this table to the user during the conclusion review step so they can investigate before finalizing the report

### Version Display

**Show full version with build date AND revision hash: `2026.3.0.dev-20260612.91ada5517d59`.**

The full version is constructed from the `packages[]` array: take `scylla-server-target` package's `version` field (normalize `~` to `.`), append `.` + the `date` field, append `.` + the `revision_id` field. Example: version=`2026.3.0~dev`, date=`20260612`, revision_id=`91ada5517d59` becomes `2026.3.0.dev.20260612.91ada5517d59`.

The "short version" (e.g., `2026.3.0.dev`) is derived by normalizing `~` to `.` in the `scylla_version` field.

**When the period spans more than one build** (common -- e.g. most tests on `...20260723.4fc12a81b3c5` but a nemesis run on `...20260720.a78d406c2ed8`), the Summary title takes the version covering the **most runs**; break ties with the most recent build date. Every per-run version still appears in the Detailed Results `Version` column, and the detailed test sub-heading uses that test's own version -- so a test built from a different revision is never mislabelled with the Summary's version.

### Detailed Results: Argus Links and Throughput

**Each workload row in the Detailed Results section includes an Argus link to the specific run for that workload.**

Each run covers a single workload. The Argus link must point to the run that produced results for that specific workload, NOT to the test generally.

Link format: `https://argus.scylladb.com/test/{test_id}/runs?additionalRuns[]={run_id}`
(Note: `/test/` singular, not `/tests/` plural)

**Test sub-heading format:** Show test name and full version (with date and revision hash), NO status badge.
Example: `predefined-throughput-steps-i8g-tablets (2026.3.0.dev.20260612.91ada5517d59)`

**For unthrottled steps, show "Max Throughput (run)"** -- the throughput from the latest run's unthrottled step for that workload.

**Mixed workload throughput**: For the `mixed` workload, throughput is the SUM of `Throughput read` + `Throughput write` (since mixed runs report separate read and write throughput values). For single-operation workloads (read, write, read_disk_only), use the single `Throughput <op>` value directly.

**Show failed results AND max throughput for ALL runs performed in the period.** The detailed table should include data from every run in the time window, not just the latest one. However, the Max Throughput table is only shown when unthrottled steps have FAIL/ERROR status -- if throughput is as expected (unthrottled steps pass), the table is omitted.

The detailed results table columns: Workload | Max Throughput (run) | P99 (ms) | Status | Link

### Detailed Results: Only Failed Steps

**In the detailed results, show a "Failed Results" table ONLY when there are actual failures (table status `FAIL` or `ERROR`).** This table lists individual failed/error steps across ALL runs in the period with their workload, step name, P99, throughput, version, and Argus link. If there are no failures or errors, this table is omitted entirely.

Note: A run may have status `"failed"` but its individual tables may show `ERROR` status (not `FAIL`). Both `FAIL` and `ERROR` table statuses must be included in the failed results.

### Detailed Results: Only Show When Failures Exist

**The entire "Detailed Results" section is ONLY shown when there are actual failures (run status `failed` or `test_error`).** When all tests pass, the Detailed Results section is completely omitted from the report.

When failures exist, the section includes:
- **Failed Results table**: Lists individual failed/error steps with workload, step name, P99, throughput, version, and Argus link. Only tests that have actual failed result tables (status `FAIL` or `ERROR`) are shown. Tests where the run is marked `failed` but all result tables show `PASS`, or `test_error` runs with no result tables, are excluded from Detailed Results -- they belong in the Uninvestigated Failures table instead.
- **Max Throughput table** (only for `predefined-throughput-steps` tests where the **unthrottled step itself** has status `FAIL` or `ERROR`): Shows per-workload max throughput from the latest run. If the unthrottled steps all pass (status `PASS`), the Max Throughput table is **omitted** even if other throttled steps failed -- the throughput is as expected.

This means:
- ✅ All tests passed → No Detailed Results section at all
- ✅ Some tests failed → Detailed Results section appears with failed steps
- ✅ Throughput test failed AND unthrottled step failed → Show both Failed Results table AND Max Throughput table
- ✅ Throughput test failed but unthrottled step passed → Show only Failed Results table (throughput is as expected)
- ✅ Nemesis/upgrade test failed → Show only Failed Results table (no throughput table)

### Table Width

**Use width="700" for the main content table.**

This provides better readability for tables with multiple columns.

### Output Location

**The report file (`perf-weekly-status-report.html`) must NOT be saved into the SCT repository.**

Write it anywhere outside the working tree -- the agent's own scratchpad/temp directory is the natural choice, or the user's home directory. Never commit it to the repo. (`/tmp/opencode/` also works but is specific to one agent harness; don't treat it as required.)

## When to Use

- Generating a weekly performance status email for stakeholders
- Creating an HTML summary of enterprise perf test results from the past week
- Aggregating latency and throughput data across multiple Argus test IDs
- Producing a Gmail-friendly report of test pass/fail status with detailed metrics
- When asked for "perf weekly report", "perf status", or "weekly performance summary"

## When NOT to Use

- Comparing two specific versions (use `perf-comparison-report` skill instead)
- Investigating root cause of a specific regression
- Running or configuring performance tests
- Generating reports for non-enterprise (OSS) tests

## Test Registry

These are the enterprise performance tests tracked in the weekly report:

| Test Name | Test ID | Category |
|-----------|---------|----------|
| predefined-throughput-steps-i8g-tablets | d6ebf1a5-135f-43fc-a7ba-0716b60dfa94 | i8g Tablets |
| latency-650gb-with-nemesis-i8g-tablets | c3e46c77-2068-4ea1-b351-9329ed4e4161 | i8g Tablets |
| latency-650gb-during-rolling-upgrade-i8g-tablets | 01945e9c-ccbc-4248-8eb9-6b80ed7e29fe | i8g Tablets |
| predefined-throughput-steps-i8g-vnodes | 6ffbef10-7138-457c-b386-73574805ca00 | i8g Vnodes |
| latency-650gb-with-nemesis-i8g-vnodes | 4bd86f85-49a4-454a-aeb7-252e83fc533d | i8g Vnodes |
| predefined-throughput-steps-tablets | d0b4711b-bc62-41e8-a619-41a61ffab0e3 | i4i Tablets |
| predefined-throughput-steps-write-tablets | 4c91ab7e-b6ec-4591-9b39-cf8bc838ebe2 | i4i Tablets |
| latency-650gb-with-nemesis-tablets | fd8ef431-3485-4232-9f0f-2b46b818a63b | i4i Tablets |
| latency-650gb-during-rolling-upgrade-tablets | 7b96ec0a-7dec-4aae-9f37-dfbad8a6d98f | i4i Tablets |
| predefined-throughput-steps-vnodes | 5c8777b4-9bf1-49bc-8b96-fa3426b05e86 | i4i Vnodes |
| latency-650gb-with-nemesis | 2a4db9d5-80e6-437e-8871-a4d5e54cc35c | i4i Vnodes |
| latency-650gb-during-rolling-upgrade | 9148b8ed-5b2e-4dfa-ab7b-b845d6117bdb | i4i Vnodes |
| simple-query-weekly-microbenchmark_arm64 | a0063c73-efcf-4878-988d-72af779dc59d | Microbenchmarks |
| simple-query-weekly-microbenchmark_arm64-write | dcc1afa0-2225-468c-9f45-5cfc8486f7f8 | Microbenchmarks |
| simple-query-weekly-microbenchmark_x86_64 | 03464849-60e8-46c8-91b9-955cdeb07ea6 | Microbenchmarks |
| simple-query-weekly-microbenchmark_x86_64-write | 6e745123-cb53-482b-836c-0609bd36a4e6 | Microbenchmarks |

**Most of this registry is usually empty for a given week, and that is expected.** These tests are not all scheduled weekly, and several run predominantly on release branches -- their runs get filtered out by the master-only rule. It is normal for whole categories (i8g Vnodes, i4i Tablets, i4i Vnodes) to contribute zero rows because they only ran release builds such as `2026.2.2` / `2026.1.9`.

Do not treat a mostly-empty result as a collection failure, and do not list tests with no runs in the report. Do state in the Conclusion which categories had no master runs, so readers can tell "passed" apart from "never ran".

## Input Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| Build type | (ask user) | Master (~dev) or Release. Controls version filter regex. If not specified, ask the user before collecting data. |
| Time window (days) | 7 | Number of days to look back for runs. Controls `--after` timestamp in `argus run list`. User can specify a different period (e.g., "last 14 days", "last 30 days"). |

Example user prompts with time window:
- "Generate perf weekly report" -- uses default 7 days
- "Generate perf report for last 14 days" -- uses 14 days
- "Performance status for the past month" -- uses 30 days

The time window is computed as: `--after $(date -d '<N> days ago' +%s)`

## Argus CLI Quick Reference

### List runs for a test

```bash
argus run list \
  --test-id <TEST_UUID> \
  --after $(date -d '<N> days ago' +%s) \
  --limit 200 \
  --full \
  --url https://argus.scylladb.com
```

Returns JSON array of run objects. Key fields:
- `id` -- Run UUID (needed for `run results`)
- `scylla_version` -- Version string to filter by
- `status` -- "passed", "failed", "test_error", "running"
- `packages[].version` -- Package versions (alternative version source)
- `build_number` -- Jenkins build number

### Fetch results for a run

```bash
argus run results \
  --run-id <RUN_UUID> \
  --url https://argus.scylladb.com
```

Returns JSON array of result tables. Each table has:
- `name` -- Format: `"<workload> - <step> - latencies"` or `"<workload> - <step> - stalls - REACTOR_STALLED"`
- `status` -- "PASS", "FAIL", "ERROR"
- `rows[].cells` -- Key-value pairs with `value` and `status` per metric:
  - `"P90 <op>"` -- 90th percentile latency in ms
  - `"P99 <op>"` -- 99th percentile latency in ms
  - `"Throughput <op>"` -- Actual throughput in op/s

### Fetch events (errors) for a run

```bash
argus run events \
  --run-id <RUN_UUID> \
  --url https://argus.scylladb.com
```

Returns JSON array of CRITICAL and ERROR event objects. Key fields:
- `severity` -- "CRITICAL" or "ERROR"
- `message` -- Event message text (contains the error description)

Use this to determine why a `test_error` run failed (e.g., CapacityReservationError). This is more reliable than fetching Jenkins console output, which often requires authentication.

### Fetch issues for a run

```bash
argus issue list \
  --run-id <RUN_UUID> \
  --url https://argus.scylladb.com
```

Returns JSON array of issue objects. Key fields:
- `key` -- Jira issue key (e.g., "SCYLLADB-2794")
- `title` -- Issue title/summary
- `state` -- Jira state ("new", "todo", "done", "duplicate")
- `url` -- Direct Jira link

Note: the Argus payload does NOT include Jira creation dates -- but you can look them up directly instead of asking the user (see below).

**`argus issue list` frequently returns `[]` even for failed runs.** Linking a run to a Jira ticket is a manual step in Argus, so an unlinked failure is normal, not a collection bug. When every failed run returns no issues, do not conclude there are no relevant issues -- ask the user whether a known ticket covers the failure, and say plainly in your summary that the attribution came from them rather than from Argus.

### Resolve new vs reproduced from Jira, not from the user

Fetch each issue's `created` field via the Atlassian MCP `getJiraIssue` tool (`fields: ["summary","status","created","resolution"]`) and compare it to the report window:

- `created` **inside** the window -> **New Issues - Regression**
- `created` **before** the window -> **Reproduced Issues**

Use `status` / `resolution` for the State column, and to support any "no progress since last week" statement -- an issue whose `updated` timestamp predates the previous report genuinely has had no activity. Only fall back to asking the user if Jira is unreachable.

## Report Structure

The output HTML file must contain:

1. **Header** -- Report title, date range, "Master (~dev) builds only" indicator
2. **Summary** -- Title format: "Summary for Scylla version {full_version}" where full_version includes build date and revision hash (e.g., "2026.3.0.dev.20260612.91ada5517d59"). Body: Total tests run, passed count, failed count, error count, running count. **Counts are per run, not per test group.** Each workload is a separate run, so a test with 4 workloads (mixed, read, write, read_disk_only) counts as 4 runs. Microbenchmark tests count as 1 run each. Running tests are counted separately and not included in passed/failed totals.
3. **Conclusion** -- Hierarchical bullet-point lines summarizing weekly results. Structure: top-level items are test names in bold (prefixed with `- `), sub-items are specific observations (prefixed with `&#8226;`). Version numbers must be bold. Do NOT mention CapacityReservationError runs (those are in the Uninvestigated Failures table). Do NOT mention issue numbers per test in the conclusion body -- issue references belong only in the warning banner; the Issues column in Overview and the Known Issues table provide the per-run linkage. **Per-version grouping:** When the report spans multiple versions, create a separate list per version with a bold version header (e.g., "Version 2026.2.3:"). When only a single version is present, omit the version header and list tests directly. **Warning banner** (optional): After collecting all issues, present the de-duplicated list to the user and ask which issues (if any) should be highlighted in a warning banner at the top of the Conclusion. If the user selects issues, render a yellow/red banner with `&#9888;` icon stating "No updates on [issues] during last week." **Issue numbers in the banner must be clickable links** (e.g., `<a href="...">SCYLLADB-3459</a>`), not plain text. If the user selects none, omit the banner entirely. The agent MUST print the generated conclusion text to the user and ask for confirmation or edits BEFORE saving it into the final HTML report file. This ensures the user can review and adjust the conclusion wording.
4. **Uninvestigated Failures** -- Table of failed/test_error runs with no linked Argus issue. Shown after Conclusion when such runs exist. Columns: Test | Workload | Version | Status | Cause | Link. Excludes CapacityReservationError runs that were successfully re-run. Printed to the user during conclusion review for investigation before finalizing.
5. **New Issues - Regression** -- Shown after Uninvestigated Failures when new issues exist. Lists Jira issues whose tickets were created during the report period (i.e., newly filed regressions). Classify by fetching each issue's `created` date from Jira (Atlassian MCP `getJiraIssue`) and comparing it to the report window; only ask the user if Jira is unreachable. If no new issues, this section is omitted.
6. **Reproduced Issues** -- Always shown after New Issues (or after Uninvestigated Failures if no new issues). Lists issues linked to runs whose Jira tickets were created before the report period. If no reproduced issues, displays "No reproduced issues in this period."
7. **Overview Table** -- Grouped by category, then test, then workload. Columns: Category | Test | Workload | Status | Issues | Link. Issues column shows linked Jira keys as clickable links. Microbenchmarks use "-" as workload. **When multiple versions exist, create a separate table per version with a version label above it; when single version, show one table without a version label.**
8. **Detailed Results** -- **ONLY shown when there are actual failures.** When all tests pass, this section is completely omitted. When failures exist, shows per-category breakdown of failed steps with P99/throughput, and optionally a Max Throughput table for failed predefined-throughput-steps tests where the unthrottled step itself has FAIL/ERROR status. **When multiple versions exist, group detailed results by version with version sub-headings; when single version, omit the version sub-heading.**
   - **Microbenchmarks**: Shown only when they have failures.

## Reference Index

| File | Content |
|------|---------|
| [workflows/generate-report.md](workflows/generate-report.md) | Step-by-step process for generating the report |
| [references/argus-data-format.md](references/argus-data-format.md) | Detailed Argus CLI output format documentation |
| [references/html-template.md](references/html-template.md) | Gmail-compatible HTML template patterns |

## Success Criteria

A valid weekly status report:

- [ ] User asked to choose build type (master or release) before data collection, unless already specified in prompt
- [ ] Filters exclusively to the chosen build type (master ~dev or release -- no mixing)
- [ ] Shows only tests that were actually run (no "NO_RUNS" entries)
- [ ] Uses table-based layout with inline CSS only (no style blocks, no div layout, no border-radius)
- [ ] Uses bgcolor attribute alongside background-color for Gmail compatibility
- [ ] Renders correctly when opened in a browser and in Gmail
- [ ] Summary title format: "Summary for Scylla version {full_version}" with build date and revision hash
- [ ] Overview table: grouped by category/test/workload with Argus links in Link column
- [ ] Overview table: microbenchmarks appear with "-" as workload
- [ ] Overview Status column: just the status badge (PASSED/FAILED/ERROR/RUNNING) -- no counts
- [ ] Running tests included in Overview with RUNNING badge (blue #17a2b8) when they match version filter
- [ ] Overview columns: Category | Test | Workload | Status | Issues | Link (no Runs column, no Version column)
- [ ] Overview Issues column: linked Jira keys as clickable links, comma-separated; empty for runs with no issues
- [ ] Overview and Detailed Results: separate table per version with version header when multiple versions; no version header when single version
- [ ] Conclusion grouped by version when multiple versions present; single list when only one version
- [ ] Detailed results: ONLY shown when there are actual failures (completely omitted when all pass)
- [ ] Detailed results: test sub-heading has full version, NO status badge
- [ ] Detailed results: Failed Results table lists all failed steps with metrics
- [ ] Detailed results: Max Throughput table ONLY for predefined-throughput-steps tests where unthrottled step has FAIL/ERROR status
- [ ] Detailed results: Max Throughput table omitted when unthrottled steps pass (throughput as expected)
- [ ] Argus link format uses `/test/` (singular), not `/tests/` (plural)
- [ ] Detailed results Argus link points to the specific run for each workload
- [ ] Groups tests by platform category in detailed results
- [ ] States the reporting period in the header
- [ ] Table width is 700px
- [ ] Output file is NOT saved into the SCT repository (scratchpad/temp dir or home dir)
- [ ] Conclusion text is printed to the user for review/editing BEFORE being saved into the HTML report
- [ ] Conclusion uses hierarchical format: bold test names as top-level items, specific observations as sub-bullets
- [ ] Conclusion version numbers are bold (e.g., `<b>2026.2.3</b>`)
- [ ] Conclusion does not mention CapacityReservationError runs (those are in the Uninvestigated Failures table)
- [ ] Conclusion warning banner: user asked to select which issues to highlight from the found issues list; omitted if user selects none or no issues found
- [ ] Conclusion warning banner: issue numbers rendered as clickable links, not plain text
- [ ] CapacityReservationError re-runs that also failed: intermediate attempts excluded, only latest kept in Uninvestigated table with cause noting all build numbers
- [ ] Issues are split into "New Issues - Regression" (created during period) and "Reproduced Issues" (pre-existing)
- [ ] Issue classification comes from Jira `created` dates; the user is asked only for unlinked failures or if Jira is unreachable
- [ ] `argus` resolved from PATH, not a hardcoded home directory; login handled by a pre-flight call
- [ ] Every master run in the window is accounted for, including `test_error` runs with zero result tables
- [ ] Workload for runs with no result tables recovered from the Jenkins `sub_tests` order and validated against runs that do have tables
- [ ] Runs that produced nothing state the actual cause (e.g. `CapacityReservationError`) rather than a bare ERROR
- [ ] Test error causes determined via `argus run events`, not Jenkins console
- [ ] CapacityReservationError runs that were successfully re-run are excluded from the report entirely
- [ ] Re-runs detected by matching same test + version with a later build number that passed
- [ ] Uninvestigated Failures table shown for failed/test_error runs with no linked Argus issue
- [ ] Uninvestigated Failures table includes Cause column with specific failure reason
- [ ] Uninvestigated Failures table excludes CapacityReservationError runs that were successfully re-run
- [ ] Uninvestigated Failures table printed to user during conclusion review for investigation
- [ ] Categories with no master runs are named in the Conclusion, so "passed" is distinguishable from "never ran"
- [ ] Summary version chosen by run count when the period spans multiple builds; per-run versions shown in Detailed Results
- [ ] Data tables use `border="1"` plus per-cell borders (CSS-only cell borders get stripped)
- [ ] Status badges use `bgcolor` on a `<td>`, not a bare `<span>`
- [ ] Rendering verified visually in a browser before any email draft is created
- [ ] Email step (if requested) creates a DRAFT only -- never sends
