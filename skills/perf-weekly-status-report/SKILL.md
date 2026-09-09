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

If the output contains `Waiting for login...`, tell the user a browser login is required and wait for it to complete before proceeding. Never launch the 20-test collection loop as the first Argus call -- a login prompt mid-fan-out produces confusing partial failures.

### Gmail-Compatible HTML

**Output must render correctly in Gmail, which strips `<style>` blocks and external CSS.**

Gmail only supports inline CSS styles on elements. Never use `<style>` tags, `<link>` stylesheets, or CSS classes. All styling must be inline via `style="..."` attributes. Keep the HTML structure simple: use tables for layout, not divs with flexbox or grid.

### Overview Table: Group by Workload

**The overview table shows each workload as its own row. Columns: Category | Test | Workload | Status | Cause | Issues | Link.**

- Category column shows the category name only on the first row of that category group (empty on subsequent rows)
- Test column shows the test name only on the first workload row for that test (empty on subsequent rows)
- Workload column shows the actual workload name (mixed, read, write, read_disk_only). **For microbenchmarks use "-"** since they don't have separate workloads
- Status column shows just the status badge (PASSED/FAILED/ERROR/RUNNING) of the latest run -- no counts
- **Cause column** shows why the run failed, and is empty for PASSED rows. See [Cause Column](#cause-column-failed-value-plus-error-threshold)
- **Issues column** shows linked Jira issue keys as clickable links (comma-separated if multiple).
  When a FAILED row has **no** linked issue, it shows `Investigation in progress` instead --
  italic amber (`#fd7e14`), not a link. PASSED rows with no issue stay empty. This is the only
  place unlinked failures are reported; there is no separate table for them.
- Link column shows an Argus link to the specific run for that workload
- Each run covers a single workload, so each workload has its own run(s)

**One row per workload per version: the LATEST run wins.** When a build re-ran a workload (or a
sub-test was re-run in a later build), sort that workload's runs by `start_time` and keep the last
one. Earlier attempts still count in the Summary totals but must not add extra overview rows.

**Per-version tables:** When the report spans multiple versions, create a separate Overview table for each version with a version label above it (e.g., "Version 2026.2.3"). When only a single version is present, show one table without a version label.

**Microbenchmark handling**: Microbenchmarks don't report results in the same table structure as performance tests. When a microbenchmark run has no workload-specific tables, use "-" as the workload value. Each microbenchmark test appears as a single row in the overview with workload="-".

**Runs column format:** Removed. Status column shows just the badge.

**Status column format:** Show just the status badge (PASSED/FAILED/ERROR/RUNNING) of the latest run for that workload. No counts.

### Cause Column: Failed Value Plus Error Threshold

**Every FAILED row states the metric that failed, the step, the measured value AND the configured
error threshold.** A bare "FAILED" forces the reader into Argus; the threshold is what makes the
number meaningful.

Format one entry per failed metric:

```
<metric> <STATUS> at <step> step (<value> <unit>, threshold <limit> <unit>)
```

Real examples:

```
Throughput read ERROR at unthrottled step (571,128 op/s, threshold 580,000 op/s)
P99 read ERROR at 1,500,000 op/s step (916.98 ms, threshold 10 ms)
duration ERROR at decommission_nodes step (1,608 s, threshold 1,600 s)
```

- Units: `P90`/`P99` -> `ms`, `Throughput` -> `op/s`, `duration` -> `s`, microbenchmark counters -> none.
- Numeric steps are rendered with thousands separators plus `op/s` (`1500000` -> `1,500,000 op/s`).
  Named steps (`unthrottled`, `decommission_nodes`) are printed as-is. Microbenchmark tables have no
  meaningful step -- omit the `at <step> step` clause entirely.
- **When a run has several failed metrics, put each one on its own line** (`<br>`-separated inside the
  cell). Do not join them with `;` -- a multi-failure cell is unreadable as one run-on line.
- When a run failed without any failed result table, state the real cause from `argus run events`
  instead, e.g. `OVERSIZED_ALLOCATION error in DB log (all result tables PASS)` or
  `cassandra-stress write pre-population failed (CL=ALL, OperationOnKey timeouts) -- no result tables produced`.

### Result Status: the Argus Run Status Is Authoritative

**A run whose Argus status is `passed` is PASSED, even if one of its result tables reports FAIL or
ERROR.** Those table-level errors have already been reviewed and dismissed by whoever set the run
status; re-flagging them as failures produces phantom regressions in the report.

- FAILED <=> run `status` is `failed` or `test_error`.
- PASSED <=> anything else, table statuses notwithstanding.
- The same rule drives the Summary counts, so a `passed` run with an errored table counts as passed.

Cause text still comes from the failed result tables -- but only for runs that are genuinely failed.

### Error Thresholds: Resolve `default | step` Across TWO Files

**The threshold for a step is NOT always in the test's threshold config. The per-workload `default`
block lives in `defaults/test_default.yaml`, and every step without an explicit override inherits
it.** Reading only the test-specific file makes steps look like they have no threshold at all.

`sct_config` deep-merges every config file (`anyconfig` with `ac_merge=MS_DICTS`), then
`send_result_to_argus` resolves one step as:

```python
error_thresholds = error_thresholds[workload]["default"] | error_thresholds[workload].get(step_name, {})
```

So resolve a limit in exactly this order:

1. Read the run's `config_files` and `scm_revision_id` from `argus run details` -- the perf jobs run
   `branch-perf-v17`, so read the YAMLs **at that revision** (`git show <sha>:<path>`), never from
   the checked-out branch.
2. Load `latency_decorator_error_thresholds` from `defaults/test_default.yaml` at that revision.
   It defines `<workload>.default.P99 <op>.fixed_limit: 10` (10 ms) for every workload.
3. Deep-merge each `configurations/performance/latency-decorator-error-thresholds-*.yaml` listed in
   `config_files` on top of it.
4. For the failing step: `merged[workload]["default"] | merged[workload].get(step, {})`, then read
   `[metric]["fixed_limit"]`.

A step-specific entry always wins over the default (e.g. i4i tablets read has `P99 read: 1` at the
150,000 step), but a step the file never mentions -- such as the i8g `1500000` read step, or any
throttled step in a `write` section that only lists `unthrottled` -- correctly falls back to 10 ms.

**Microbenchmarks use a different code path.** `set_validation_rules` in `sdcm/argus_results.py`
puts the metric directly under the workload (`write.instructions_per_op.fixed_limit`) with no step
level and no `default` merge, defaulting to `ValidationRule(best_pct=5)` when unset.

### Running Tests

**Include runs with status `running` in the report if they match the version filter.**

Running tests should appear in the Overview table with a RUNNING status badge (blue, `#17a2b8`). They are counted separately in the Summary (not as passed or failed). Determine the workload from partial results (`argus run results`) if available, or from Jenkins `sub_tests` positional mapping. Do not mark running tests `Investigation in progress` -- that marker is for FAILED rows only.

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

### Manually Triggered Re-runs Are Not Part of the Weekly Picture

**Exclude every run whose `started_by` is `avi`.** These are ad-hoc re-runs kicked off by hand
during an investigation -- often a burst of partial runs on the same job across several builds --
and they are not the scheduled weekly result. Counting them distorts both the totals and the
failure picture.

`started_by` comes from `argus run details`, not from `argus run list`. It has been wrong before, so
confirm the classification against the Jenkins build causes before relying on it:

```bash
curl -s -u "$JENKINS_USER:$JENKINS_TOKEN" \
  "https://jenkins.scylladb.com/<job path>/<build>/api/json?tree=actions[causes[shortDescription,userId,userName]]"
```

A manual run reads `Started by user Avi Kivity`; a scheduled one reads
`Started by upstream project "scylla-master/sct_triggers/perf-regression-trigger" build number N`
(Argus attributes those to the job owner, which is not `avi`). Credentials come from the SCT
KeyStore: `KeyStore().get_json("jenkins.json")` -> `username` / `password`.

Other trigger values (`timer`, a job owner's username) are all kept.

Report how many runs this dropped to the user in the review step.

### CapacityReservationError Runs: Never Reported

**Exclude every run whose failure is CapacityReservationError from the report entirely** -- whether
or not it was re-run, and whether or not a later attempt also failed.

These runs died during AWS provisioning. They carry no result tables and no signal about ScyllaDB;
listing them makes an infrastructure outage look like a product regression and inflates the
failure count. Drop them from the Overview and from the Summary totals alike.

Detect them via `argus run events` -- the message contains:

```
(TestFrameworkEvent Severity.CRITICAL) Failed to provision aws resources: CapacityReservationError: Failed to create capacity reservation in any availability zone.
```

Apply the same treatment to `aborted` runs whose workload was re-run in a later build (a sub-test
that was cancelled and retried); count only the run that actually produced results.

Report the number of excluded runs to the user in the review step -- the report itself stays silent
about them.

### Unlinked Failures: Mark Them in the Issues Column

**A FAILED run with no linked Argus issue is reported in the Test Overview's Issues column as
`Investigation in progress` -- not in a section of its own.**

```html
<td style="...">P99 read ERROR at 700,000 op/s step (18,236.83 ms, threshold 50 ms)</td>
<td style="..."><i style="color:#fd7e14;">Investigation in progress</i></td>
```

Keeping it in the row means the reader sees, in one pass, which failures are accounted for by a
ticket and which are not -- no cross-referencing a second table. Earlier versions of this skill
rendered a separate "Failed, investigation in progress" table; that section no longer exists.

The marker applies to the run shown in the row -- the latest run for that version/test/workload.
A failure already superseded by a later passing run never reaches the table, so it is never marked.

Still print the list of unlinked failures to the user during the review step, so they can
investigate before the report is finalised.

### Version Display

**Show full version with build date AND revision hash: `2026.3.0.dev-20260612.91ada5517d59`.**

The full version is constructed from the `packages[]` array: take `scylla-server-target` package's `version` field (normalize `~` to `.`), append `.` + the `date` field, append `.` + the `revision_id` field. Example: version=`2026.3.0~dev`, date=`20260612`, revision_id=`91ada5517d59` becomes `2026.3.0.dev.20260612.91ada5517d59`.

The "short version" (e.g., `2026.3.0.dev`) is derived by normalizing `~` to `.` in the `scylla_version` field.

**When the period spans more than one version, the Overview gets one table per version -- on master
as well as on release.** Master reports one product version (e.g. `2026.4.0~dev`) but roll through
several builds a week; group by the **full** version (`2026.4.0.dev.20260830.dc4ad9bfdb94`), exactly
as release groups by `2026.2.6`. The Summary lists **only the versions whose results are reported**,
each with the number of tests it contributes, so the per-version counts add up to Total Tests:

```
Summary for Scylla version 2026.4.0.dev, builds:

  2026.4.0.dev.20260902.5f352afcf41a (4 tests)
  2026.4.0.dev.20260901.d0f9c3a48a11 (4 tests)
  2026.4.0.dev.20260830.dc4ad9bfdb94 (13 tests)
```

A build that ran in the period but whose every run was superseded contributes no rows, so it is not
listed. Do **not** add a Version column to the table -- the per-version heading already says it. Group a run by its
`scylla_version`; for rolling-upgrade tests that is the upgrade target, so read the full version
from the `scylla-server-upgrade-target` package rather than `scylla-server-target` (which holds the
base version there). Microbenchmark runs often have no date/revision on `scylla-server-target` --
fall back to the `scylla-server` package.

### Runs Whose Results Endpoint Errors Out

**`argus run results` can fail server-side for an entire test** -- it returns
`Error: server returned status "error"` for every run of that test, with or without `--test-id`,
and retrying does not help. Do not silently drop those runs.

Recover what you can and say so plainly in the summary you give the user:

1. `argus run events` still works. A `FailedResultEvent` names the exact table that failed --
   `Argus validation failed for the result in read_disk_only - 165000 - latencies` -- which gives
   both the workload and the failing step.
2. A `TestFrameworkEvent` carries `source=PerformanceRegressionTest.test_latency_read_with_nemesis()`,
   which also identifies the workload.
3. The measured value is not recoverable from events. Take it from the previous week's report if the
   same run appears there, and tell the user which rows came from that fallback rather than from a
   live Argus fetch.

Report the affected test names to the user -- a persistent failure of this endpoint is an Argus bug
worth raising, not a quirk to work around silently.

### Table Width

**Use width="950" for the main content table.**

The Cause column carries long text -- metric, step, value and threshold, sometimes several lines --
and 700px squeezes it into unreadable wrapping. 950px still fits inside a typical desktop Gmail
reading pane. Inner tables stay at `width="100%"` so they follow the outer table.

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
| cql-raw-weekly-microbenchmark_arm64 | 474ac937-cbc7-45d1-9375-746b8328f51b | Microbenchmarks |
| cql-raw-weekly-microbenchmark_arm64-write | 68b9d59b-bf4e-4aa1-abea-032e0fd7fa8d | Microbenchmarks |
| cql-raw-weekly-microbenchmark_x86_64 | e62a078f-924b-4662-a77b-bd9daa6e2241 | Microbenchmarks |
| cql-raw-weekly-microbenchmark_x86_64-write | 006ddbcc-b344-4bb5-ac1b-374588305aa4 | Microbenchmarks |

Test names in this table are the Jenkins job names with the `scylla-enterprise-perf-` prefix
stripped; the jobs live under `scylla-enterprise/perf-regression/` and their pipeline definitions
under `jenkins-pipelines/performance/branch-perf-v17/scylla-enterprise/perf-regression/`.

> **`cql-raw` vs `simple-query`:** these are different tests, not architecture variants of one.
> `cql-raw` runs `microbenchmarking_test.PerfCqlRawTest`, `simple-query` runs `PerfSimpleQueryTest`.
> Beware the `scylla-staging/yulia/...` copies of the cql-raw jobs: they default `test_name` to
> `PerfSimpleQueryTest`, so a staging run can silently be a different test. Confirm via the run's
> `test_method` field before treating a cql-raw run as a cql-raw result.

**Most of this registry is usually empty for a given week, and that is expected.** These tests are not all scheduled weekly, and several run predominantly on release branches -- their runs get filtered out by the master-only rule. It is normal for whole categories (i8g Vnodes, i4i Tablets, i4i Vnodes) to contribute zero rows because they only ran release builds such as `2026.2.2` / `2026.1.9`.

Do not treat a mostly-empty result as a collection failure, and do not list tests with no runs in the report. Do state in the "Issues Found in the Runs" section which categories had no runs, so readers can tell "passed" apart from "never ran".

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
2. **Summary** -- Title format: "Summary for Scylla version(s) {full_version}", listing only the versions whose results are reported, each with its test count. Body: Total tests, passed, failed/error, running. **Counts follow the Overview: one per reported row -- the latest run per version, test and workload** -- so the per-version counts add up to Total Tests and the reader can reconcile the box against the table. Add a note line underneath giving the number of scheduled runs actually executed in the period and how many of those failed, so superseded re-runs are still visible: *"Latest run per version, test and workload; 52 scheduled runs in total were executed in the period (13 of them failed, the rest of the failures were superseded by a later run). Manually triggered ad-hoc re-runs are not included."*
3. **Issues Found in the Runs** -- (formerly "Conclusion") Hierarchical bullet-point lines summarizing weekly results. Structure: top-level items are test names in bold (prefixed with `- `), sub-items are specific observations (prefixed with `&#8226;`). Version numbers must be bold. Do NOT mention CapacityReservationError runs (they are excluded from the report entirely). Do NOT mention issue numbers per test in the conclusion body -- issue references belong only in the warning banner; the Issues column in Overview and the Known Issues table provide the per-run linkage. **Per-version grouping:** When the report spans multiple versions, create a separate list per version with a bold version header (e.g., "Version 2026.2.3:"). When only a single version is present, omit the version header and list tests directly. **Warning banner** (optional): After collecting all issues, present the de-duplicated list to the user and ask which issues (if any) should be highlighted in a warning banner at the top of the section. If the user selects issues, render a yellow/red banner with `&#9888;` icon stating "No updates on [issues] during last week." A second `&#9888;` line lists the issues opened during the period ("New issues opened during last week: ..."). **Issue numbers in the banner must be clickable links** (e.g., `<a href="...">SCYLLADB-3459</a>`), not plain text. If the user selects none, omit the banner entirely. The agent MUST print the generated conclusion text to the user and ask for confirmation or edits BEFORE saving it into the final HTML report file. This ensures the user can review and adjust the conclusion wording.
4. **New Issues - Regression** -- Shown after "Issues Found in the Runs" when new issues exist. Lists Jira issues whose tickets were created during the report period (i.e., newly filed regressions). Classify by fetching each issue's `created` date from Jira (Atlassian MCP `getJiraIssue`) and comparing it to the report window; only ask the user if Jira is unreachable. If no new issues, this section is omitted.
5. **Reproduced Issues** -- Always shown after New Issues (or after "Issues Found in the Runs" if no new issues). Lists issues linked to runs whose Jira tickets were created before the report period. If no reproduced issues, displays "No reproduced issues in this period."
6. **Test Overview** -- Grouped by category, then test, then workload. Columns: Category | Test | Workload | Status | Cause | Issues | Link. Cause states the failed value and its error threshold, one line per failed metric; it is empty for PASSED rows. Issues shows linked Jira keys, or `Investigation in progress` for a FAILED row with no ticket. Microbenchmarks use "-" as workload. **When multiple versions exist -- including several master builds -- create a separate table per version with the full version as a label above it; with a single version, show one table without a label.**

There is **no "Detailed Results" section** -- the Cause column carries the failure detail, and the
Argus link in every row carries the rest.

## Reference Index

| File | Content |
|------|---------|
| [workflows/generate-report.md](workflows/generate-report.md) | Step-by-step process for generating the report |
| [references/argus-data-format.md](references/argus-data-format.md) | Detailed Argus CLI output format documentation |
| [references/html-template.md](references/html-template.md) | Gmail-compatible HTML template patterns |
| [references/perf-weekly-status-report-release-example.html](references/perf-weekly-status-report-release-example.html) | Reference rendering of a **release** report -- four release versions, one Overview table each |
| [references/perf-weekly-status-report-master-example.html](references/perf-weekly-status-report-master-example.html) | Reference rendering of a **master** report -- three `~dev` builds, one Overview table each, and a row carrying the `Investigation in progress` marker |

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
- [ ] Overview: separate table per FULL version (master builds included) with a version header when several exist; no header with a single version
- [ ] Overview has no Version column -- the per-version heading carries it
- [ ] Overview: one row per workload per version -- the LATEST run, earlier attempts not duplicated
- [ ] Overview columns include Cause between Status and Issues; Cause empty on PASSED rows
- [ ] Cause states metric, step, measured value AND configured threshold with units
- [ ] Cause with several failed metrics puts each metric on its own line (`<br>`), never joined with `;`
- [ ] Thresholds resolved as `default | step` across `defaults/test_default.yaml` + the test's threshold YAML, read at the run's `scm_revision_id`
- [ ] A step with no explicit override falls back to the default P99 limit (10 ms) -- never reported as "no threshold"
- [ ] FAILED is decided by the Argus run status alone; a `passed` run with an errored result table stays PASSED
- [ ] Report contains NO "Detailed Results" section
- [ ] "Issues Found in the Runs" section grouped by version when multiple versions present; single list when only one version
- [ ] Argus link format uses `/test/` (singular), not `/tests/` (plural)
- [ ] States the reporting period in the header
- [ ] Main content table width is 950px; inner tables at 100%
- [ ] Output file is NOT saved into the SCT repository (scratchpad/temp dir or home dir)
- [ ] "Issues Found in the Runs" text is printed to the user for review/editing BEFORE being saved into the HTML report
- [ ] "Issues Found in the Runs" uses hierarchical format: bold test names as top-level items, specific observations as sub-bullets
- [ ] Version numbers in that section are bold (e.g., `<b>2026.2.3</b>`)
- [ ] That section does not mention CapacityReservationError runs (they are excluded from the report entirely)
- [ ] Warning banner: user asked to select which issues to highlight from the found issues list; omitted if user selects none or no issues found
- [ ] Warning banner: issue numbers rendered as clickable links, not plain text
- [ ] Issues are split into "New Issues - Regression" (created during period) and "Reproduced Issues" (pre-existing)
- [ ] Issue classification comes from Jira `created` dates; the user is asked only for unlinked failures or if Jira is unreachable
- [ ] `argus` resolved from PATH, not a hardcoded home directory; login handled by a pre-flight call
- [ ] Every run in the window is accounted for, including `test_error` runs with zero result tables
- [ ] Workload for runs with no result tables recovered from `argus run events` or the Jenkins `sub_tests` order, and validated against runs that do have tables
- [ ] Test error causes determined via `argus run events`, not Jenkins console
- [ ] CapacityReservationError runs excluded from every section AND from the Summary counts
- [ ] Aborted runs whose workload was re-run in a later build are excluded too
- [ ] Count of excluded runs reported to the user, though the report itself stays silent about them
- [ ] FAILED rows with no linked Argus issue show `Investigation in progress` in the Issues column, in italic amber
- [ ] Report contains NO separate "Failed, investigation in progress" section
- [ ] The list of unlinked failures is still printed to the user during the review step
- [ ] Categories with no runs are named in "Issues Found in the Runs", so "passed" is distinguishable from "never ran"
- [ ] Summary lists only the versions whose results are reported, each with its test count, and those add up to Total Tests
- [ ] Summary counts follow the Overview rows (latest run per version/test/workload), with a note giving the total scheduled runs executed
- [ ] Runs whose `started_by` is `avi` are excluded, and the classification was confirmed against Jenkins build causes
- [ ] Tests whose `argus run results` endpoint errors out are still reported, with workload recovered from events and the fallback disclosed to the user
- [ ] Data tables use `border="1"` plus per-cell borders (CSS-only cell borders get stripped)
- [ ] Status badges use `bgcolor` on a `<td>`, not a bare `<span>`
- [ ] Rendering verified visually in a browser before any email draft is created
- [ ] Email step (if requested) creates a DRAFT only -- never sends
