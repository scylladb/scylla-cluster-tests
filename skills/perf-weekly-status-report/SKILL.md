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

## Essential Principles

### Master (~dev) Builds Only

**Filter to dev versions matching `^\d{4}\.\d+\.\d+.+dev$` -- exclude release builds.**

Release builds (`2026.1.5`) run on stable branches and represent prior releases. The weekly report must only include master/dev versions (e.g., `2026.3.0~dev`) because these represent the latest development state being validated. Mixing release and dev data would produce misleading conclusions about the current master branch performance.

### Use Argus CLI Directly

**The `argus` binary (Go CLI) is the data source -- not the Python argus client library.**

The Go-based `argus` CLI (`/home/juliayakovlev/.local/bin/argus`) provides `run list` and `run results` subcommands that return JSON. This is faster and more reliable than the Python library for batch data collection. Always use `--url https://argus.scylladb.com` to target the production Argus instance.

### Gmail-Compatible HTML

**Output must render correctly in Gmail, which strips `<style>` blocks and external CSS.**

Gmail only supports inline CSS styles on elements. Never use `<style>` tags, `<link>` stylesheets, or CSS classes. All styling must be inline via `style="..."` attributes. Keep the HTML structure simple: use tables for layout, not divs with flexbox or grid.

### Overview Table: Group by Workload

**The overview table shows each workload as its own row. Columns: Category | Test | Workload | Status | Link.**

- Category column shows the category name only on the first row of that category group (empty on subsequent rows)
- Test column shows the test name only on the first workload row for that test (empty on subsequent rows)
- Workload column shows the actual workload name (mixed, read, write, read_disk_only). **For microbenchmarks use "-"** since they don't have separate workloads
- Status column shows just the status badge (PASSED/FAILED/ERROR) of the latest run -- no counts
- Link column shows an Argus link to the specific run for that workload
- Each run covers a single workload, so each workload has its own run(s)

**Microbenchmark handling**: Microbenchmarks don't report results in the same table structure as performance tests. When a microbenchmark run has no workload-specific tables, use "-" as the workload value. Each microbenchmark test appears as a single row in the overview with workload="-".

**Runs column format:** Removed. Status column shows just the badge.

**Status column format:** Show just the status badge (PASSED/FAILED/ERROR) of the latest run for that workload. No counts.

### Version Display

**Show full version with build date AND revision hash: `2026.3.0.dev-20260612.91ada5517d59`.**

The full version is constructed from the `packages[]` array: take `scylla-server-target` package's `version` field (normalize `~` to `.`), append `.` + the `date` field, append `.` + the `revision_id` field. Example: version=`2026.3.0~dev`, date=`20260612`, revision_id=`91ada5517d59` becomes `2026.3.0.dev.20260612.91ada5517d59`.

The "short version" (e.g., `2026.3.0.dev`) is derived by normalizing `~` to `.` in the `scylla_version` field.

### Detailed Results: Argus Links and Throughput

**Each workload row in the Detailed Results section includes an Argus link to the specific run for that workload.**

Each run covers a single workload. The Argus link must point to the run that produced results for that specific workload, NOT to the test generally.

Link format: `https://argus.scylladb.com/test/{test_id}/runs?additionalRuns[]={run_id}`
(Note: `/test/` singular, not `/tests/` plural)

**Test sub-heading format:** Show test name and full version (with date and revision hash), NO status badge.
Example: `predefined-throughput-steps-i8g-tablets (2026.3.0.dev.20260612.91ada5517d59)`

**For unthrottled steps, show "Max Throughput (run)"** -- the throughput from the latest run's unthrottled step for that workload.

**Mixed workload throughput**: For the `mixed` workload, throughput is the SUM of `Throughput read` + `Throughput write` (since mixed runs report separate read and write throughput values). For single-operation workloads (read, write, read_disk_only), use the single `Throughput <op>` value directly.

**Show failed results AND max throughput for ALL runs performed in the period.** The detailed table should include data from every run in the time window, not just the latest one.

The detailed results table columns: Workload | Max Throughput (run) | P90 (ms) | P99 (ms) | Status | Link

### Detailed Results: Only Failed Steps

**In the detailed results, show a "Failed Results" table ONLY when there are actual failures (table status `FAIL` or `ERROR`).** This table lists individual failed/error steps across ALL runs in the period with their workload, step name, P90, P99, throughput, version, and Argus link. If there are no failures or errors, this table is omitted entirely.

Note: A run may have status `"failed"` but its individual tables may show `ERROR` status (not `FAIL`). Both `FAIL` and `ERROR` table statuses must be included in the failed results.

### Detailed Results: Only Show When Failures Exist

**The entire "Detailed Results" section is ONLY shown when there are actual failures (run status `failed` or `test_error`).** When all tests pass, the Detailed Results section is completely omitted from the report.

When failures exist, the section includes:
- **Failed Results table**: Lists individual failed/error steps with workload, step name, P90, P99, throughput, version, and Argus link
- **Max Throughput table** (only for `predefined-throughput-steps` tests with failures): Shows per-workload max throughput from the latest run

This means:
- ✅ All tests passed → No Detailed Results section at all
- ✅ Some tests failed → Detailed Results section appears with failed steps
- ✅ Throughput test failed → Show both Failed Results table AND Max Throughput table
- ✅ Nemesis/upgrade test failed → Show only Failed Results table (no throughput table)

### Table Width

**Use width="700" for the main content table.**

This provides better readability for tables with multiple columns.

### Output Location

**The report file (`perf-weekly-status-report.html`) must NOT be saved into the SCT repository.**

Write it to `/tmp/opencode/perf-weekly-status-report.html` or the user's home directory. Never commit it to the repo.

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

## Input Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
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

## Report Structure

The output HTML file must contain:

1. **Header** -- Report title, date range, "Master (~dev) builds only" indicator
2. **Summary** -- Title format: "Summary for Scylla version {full_version}" where full_version includes build date and revision hash (e.g., "2026.3.0.dev.20260612.91ada5517d59"). Body: Total tests run, passed count, failed count, error count. **Counts are per run, not per test group.** Each workload is a separate run, so a test with 4 workloads (mixed, read, write, read_disk_only) counts as 4 runs. Microbenchmark tests count as 1 run each. This ensures that Total = Passed + Failed/Error always holds.
3. **Conclusion** -- Auto-generated bullet-point lines summarizing weekly results.
4. **Reproduced Issues** -- Always shown after Conclusion. Lists issues linked to runs (from `argus issue list --run-id`). If no issues, displays "No reproduced issues in this period."
5. **Overview Table** -- Grouped by category, then test, then workload. Columns: Category | Test | Workload | Status | Link. Microbenchmarks use "-" as workload.
6. **Detailed Results** -- **ONLY shown when there are actual failures.** When all tests pass, this section is completely omitted. When failures exist, shows per-category breakdown of failed steps with P90/P99/throughput, and optionally a Max Throughput table for failed predefined-throughput-steps tests.
   - **Microbenchmarks**: Shown only when they have failures.

## Reference Index

| File | Content |
|------|---------|
| [workflows/generate-report.md](workflows/generate-report.md) | Step-by-step process for generating the report |
| [references/argus-data-format.md](references/argus-data-format.md) | Detailed Argus CLI output format documentation |
| [references/html-template.md](references/html-template.md) | Gmail-compatible HTML template patterns |

## Success Criteria

A valid weekly status report:

- [ ] Filters exclusively to master (~dev) versions (no release builds included)
- [ ] Shows only tests that were actually run (no "NO_RUNS" entries)
- [ ] Uses table-based layout with inline CSS only (no style blocks, no div layout, no border-radius)
- [ ] Uses bgcolor attribute alongside background-color for Gmail compatibility
- [ ] Renders correctly when opened in a browser and in Gmail
- [ ] Summary title format: "Summary for Scylla version {full_version}" with build date and revision hash
- [ ] Overview table: grouped by category/test/workload with Argus links in Link column
- [ ] Overview table: microbenchmarks appear with "-" as workload
- [ ] Overview Status column: just the status badge (PASSED/FAILED/ERROR) -- no counts
- [ ] Overview columns: Category | Test | Workload | Status | Link (no Runs column, no Version column)
- [ ] Detailed results: ONLY shown when there are actual failures (completely omitted when all pass)
- [ ] Detailed results: test sub-heading has full version, NO status badge
- [ ] Detailed results: Failed Results table lists all failed steps with metrics
- [ ] Detailed results: Max Throughput table ONLY for predefined-throughput-steps tests with failures
- [ ] Argus link format uses `/test/` (singular), not `/tests/` (plural)
- [ ] Detailed results Argus link points to the specific run for each workload
- [ ] Groups tests by platform category in detailed results
- [ ] States the reporting period in the header
- [ ] Table width is 700px
- [ ] Output file is NOT saved into the SCT repository (use /tmp/opencode/ or home dir)
