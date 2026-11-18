---
name: perf-microbenchmark-trends
description: >-
  Builds and refreshes the Microbenchmark Trends dashboard: Argus timeline charts of
  instructions_per_op, cpu_cycles_per_op and allocs_per_op for the weekly perf-simple-query and perf-cql-raw
  microbenchmarks (read/write, arm64/x86_64) per Scylla release line, with a master trend over
  every master run and an across-releases comparison, and a draft email. Use when asked for a
  microbenchmark report, microbenchmark trends or graphs, instructions, CPU cycles or
  allocations per op over time, comparing simple-query against cql-raw, or refreshing or emailing that
  dashboard. Not for the throughput/latency weekly status email, which covers the
  predefined-throughput-steps, nemesis, rolling-upgrade and elasticity tests.
---

# Microbenchmark Trends Dashboard

Turn three months of weekly microbenchmark runs in Argus into one dashboard of timeline charts and
a summary of how master moves and how each release improves on the one before, then draft the
email that links to it.

## Essential Principles

### Show Every Run on a Time Axis, Grouped by Release Line

**One point per Argus run, x = run start time, color = release line (master, 2026.4, 2026.3, ...).**

This mirrors the Argus result graph people already read. A per-version table hides how noisy a
benchmark is; a timeline shows that perf-cql-raw read moves 12-17% between consecutive runs while
perf-simple-query stays within 0.5%, which is what decides whether a change is real. Map any
`~dev` scylla_version to `master` (it spans 2026.4.0~dev and 2027.1.0~dev builds); rc builds belong
to their release line.

### Keep simple-query and cql-raw Side by Side, Per Metric

**Layout per metric: a heading, the release legend, then a perf-simple-query row and a
perf-cql-raw row of four small charts (read/write x arm64/x86_64).**

The user asked for this comparison explicitly: the same workload through the two front ends shows
whether a change is in the storage path or the CQL layer. Keep all four charts of a row on one line
at desktop width, and repeat the legend under every metric title so a reader never scrolls to find
the colors. Add a metric by adding a section, never by mixing metrics in one chart.

### Summarize the Master Trend and the Release Ladder

**The summary has two parts, each with one table per benchmark (perf-simple-query, perf-cql-raw):
"Master trend" and "Across releases".**

The audience (R&D management) asked for two things: a trend needs more than two data points, and
the main interest is master plus how each release improves on the previous one. A last-run-vs-
previous-run delta answered neither, so it was replaced. This is also why the default window is
three months: master runs less often than releases, and two months gave only 3-4 master points.

- **Master trend:** rows are the metrics, named exactly as in Argus (`instructions_per_op`,
  `cpu_cycles_per_op`, `allocs_per_op`); columns are read arm64, read x86_64, write arm64,
  write x86_64. Each cell is a sparkline of every master run in the window (oldest to newest, the
  last point dark), the change of the last run against the first, and the run count. Master builds
  map from any `~dev` version; the last one is "the last master run", never "the latest master",
  because it is the last build the microbenchmarks were triggered on, not current master.
- **Across releases:** per metric and chart, the latest run of each release line in order
  2025.1, 2026.1, 2026.2, 2026.3, 2026.4, master, each with its change against the previous
  release that has a value. A release line missing from a benchmark shows a dash (2026.1 runs only
  perf-simple-query).

Changes of 1% or more are bold green (lower) or red (higher); lower is better for all three
metrics. Gray borders on every cell, soft colors.

Show the four charts separately and never an average, of charts or of benchmarks. Averages hid what
mattered: master simple-query instructions_per_op averaged -4.9% once, but that was write -8.5%
with read near 0, and cql-raw averages of +1.6% were the sum of +-12-17% swings in both
directions.

### Every Number Comes From the Data File

**The page reads only the attached CSV; computed figures are `dash.calc` outputs and are marked.**

The dashboard is a copy of Argus taken on a given day; readers must be able to trace each figure
to a run and see when the copy was made. Never type a number into the page text. Runs without a
result table are left out of the CSV, because they have nothing to plot; report how many were
dropped.

### Generate the Email From the Same Data, Keep One Draft

**Build the email body with `build_email.py` from the dashboard's CSV, and keep exactly one current
draft per week.**

The email repeats the dashboard summary; a hand-typed table drifts from the page at the first
refresh. The script applies the same rules as the page's `master_trend` and `ladder` calcs and the
same layout. Gmail strips SVG, so the script draws the sparklines as small PNGs (filled with the
row's background, reduced to 8 colors, ~250 bytes each) referenced by `cid:`. Only the notes and
signature are written by hand. When the summary changes, update the
existing draft in place rather than adding another, because stale drafts get sent by mistake.

### Never Send, Never Share

**Create the email as a draft and tell the user the artifact is private until they share it.**

Recipients cannot open a private artifact link. Sharing and sending are the user's decisions. A
draft labeled `SENT` was already sent by the user: never edit or delete it.

## When to Use

- "Create the microbenchmark report / dashboard / graphs"
- "Show instructions_per_op (or allocs_per_op) over the last three months"
- "How does master trend, and how do the releases compare?"
- "Refresh the microbenchmark trends with this week's runs"
- "Add tasks_per_op to the microbenchmark dashboard"
- "Draft the microbenchmark trends email"

## When NOT to Use

- Weekly throughput/latency status email for master or releases: use `perf-weekly-status-report`
- Comparing a handful of specific runs side by side (instance types, configs): use `test-run-comparison-reports`
- Investigating why one microbenchmark run failed: read its Argus events directly
- Changing the microbenchmark jobs or their thresholds: edit the jenkinsfiles and test-case YAMLs

## Quick Reference

| Item | Value |
|---|---|
| Dashboard | `<DASHBOARD_URL>`: the team's Microbenchmark Trends artifact (Data artifact type); ask its owner for the URL and edit access |
| Dataset id | `ipo` (CSV file source; every chart and calc reads it) |
| Collector | `python3 skills/perf-microbenchmark-trends/collect_microbench.py --since YYYY-MM-DD --out FILE` |
| Email body | `python3 skills/perf-microbenchmark-trends/build_email.py --csv FILE --dashboard-url URL --out email.html --spark-dir DIR` (needs Pillow), then fill `NOTES-GO-HERE` and `SIGNATURE` |
| Email | Subject `Microbenchmark trends, MM/DD/YYYY`; To: the regular perf weekly status report list; draft only |
| Argus tests | 8 jobs, ids in `TESTS` in the collector |
| Metric cells | first row of the single result table: `instructions_per_op`, `cpu_cycles_per_op`, `allocs_per_op` (also available: `tasks_per_op`, `logallocs_per_op`, `median tps`) |
| Full version | `version.date.revision` from `scylla-server-target`, falling back to `scylla-server` when the target lacks a date |
| Window | last three months unless the user names one |

### Adding a Metric

1. Add it to `METRICS` in the collector (value column + status column) and re-collect.
2. In the page: copy one metric `<section>` (heading, legend, two `.mb-group` rows) and set
   `data-metric` on its eight charts; add the metric to the loops in the `master_trend` and
   `ladder` calcs and to `METRICS` in `build_email.py`; add its status column to the
   `stField` and `unit` maps in the `chart` drawing. A metric without an Argus validation rule
   has status `UNSET`; only `ERROR`/`FAIL` draw the red ring, so it shows no false alarms.

## Reference Index

| File | Content |
|------|---------|
| [dashboard-page.html](references/dashboard-page.html) | The current page (`files/index.html`): charts, legend, `master_trend` and `ladder` calcs and their tables |

| Workflow | Purpose |
|----------|---------|
| [build-dashboard.md](workflows/build-dashboard.md) | Collect, publish data, publish page, verify, draft email (body layout table in Phase 5) |

## Success Criteria

- [ ] CSV collected for the requested window; dropped-run count reported
- [ ] Dataset `ipo` points at the new blob, with `updated.at` set
- [ ] Page `check` is `passed` (or the user is told it is unverified)
- [ ] Each metric section shows simple-query and cql-raw rows with its own legend
- [ ] Summary has a master trend (sparkline, first-to-last change, run count per chart) and an across-releases table per benchmark, no averages
- [ ] Email body generated by `build_email.py` from the same CSV; notes filled; one current draft, stale ones named; user reminded to share the artifact
