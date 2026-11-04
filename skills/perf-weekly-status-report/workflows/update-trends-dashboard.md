# Update the Perf Weekly Trends Dashboard

The email report shows one week. The trends dashboard is a Claude Artifact that graphs the same
tests over months, so a reader can tell a one-off spike from a drift. Refresh it in the same session
as the email report, so both describe the same data.

**Claude Code only.** The steps use the `Artifact` and `ArtifactData` tools (claude.ai artifacts). On
other agents (GitHub Copilot) skip this workflow and say so in the summary.

Dashboard: https://claude.ai/artifact/UFWcoRLTBXfJdsdJb7Wtnd

One dashboard covers both build types. It shows master and release lines together by default, and
**Master (~dev)** / **Release** buttons filter to one of them, so refresh it the same way whichever
build type the email covers.

## What the dashboard shows

- A runs table, collapsed by default with this week's pass/fail counts on its header, listing every
  run of the selected build types in the selected range. It sorts by any column (newest first by
  default) and highlights the runs from the 7 days before the fetch. Columns: category, test, workload,
  branch, Scylla version (with the commit SHA for `~dev` builds), date, run status,
  how many cells Argus flagged ERROR or FAIL, the linked issues, and the run's Jenkins build and Argus
  links. Each issue key opens its Argus issue page (`https://argus.scylladb.com/issues/<KEY>`, every
  run linked to that issue), with the Jira ticket beside it. Runs without results appear as
  "no results".
- A control bar pinned to the top while scrolling: build-type filter, range, Y axis, category tabs
  and the branch legend.
- A tab per registry category, in registry order, so i8g Tablets opens first and the
  throughput-steps tests sit on top. Each test is a collapsible group (only the first starts open),
  each workload inside it collapses too (open by default), and each workload gets one chart per step and metric:
  `P99 <op>` for every step, `Throughput` (summed over ops) for the `unthrottled` step,
  `Duration` (minutes) for operation steps such as a nemesis, an upgrade or a node add, and
  `allocs_per_op` / `cpu_cycles_per_op` / `instructions_per_op` for microbenchmarks. Fixed-length
  steps get no Duration chart: numbered throughput steps, `unthrottled` and `Steady State` are
  skipped by the script, and the page also hides a Duration whose values spread less than 2% over all the
  data (for example the elasticity test's fixed 30-minute `_double_cluster_load`).
- Hovering a dotted-underlined header pops up its explanation at once: a chart title shows the Argus result-table description (for microbenchmarks, the
  parameters such as concurrency and partitions) and what the metric means. A workload header lists its steps, and a test
  header shows the newest run's test method, cluster, config files and Jenkins job.
- One line per branch: `~dev` builds are master, `X.Y.Z` and `X.Y.0~rcN` are on branch `X.Y`.
  The range defaults to 6 months (1 and 3 month buttons and From/To pickers narrow it).
  The Y axis defaults to **Clip outliers**: it fits the typical values (3x IQR fences) and draws
  points beyond them as hollow triangles on the edge, so one huge P99 does not flatten the chart.
  **Full range** and **Log scale** show everything.
  ERROR/FAIL points are ringed in red. Hover shows the value, version, branch, date, build and any
  linked issues. A click opens the run in Argus, or, when the run has linked issues, pins a popup listing
  them (Argus issue page and Jira for each) plus the run's Jenkins build and Argus run. Dragging zooms in, and the expand icon grows the chart in place to
  the window height, scrolls to it and dims the rest of the page (one chart at a time) (the collapse icon or Esc closes it). The copy icon redraws the chart
  at 1100x550 (2x pixels) and puts it on the clipboard as a PNG with a caption naming the test, workload, metric and date range.
- A "Data fetched" note that turns red when the data is more than 7 days old, computed against the
  viewer's clock.

The test list comes from the Test Registry table in `SKILL.md`. Add a test there and the next
refresh picks it up; there is no second list to keep in sync.

## Phase 1: Fetch

**Entry criteria:** `argus` is authenticated (the pre-flight call in `SKILL.md` succeeded).

```bash
python3 skills/perf-weekly-status-report/scripts/fetch_perf_trends.py "$OUT/perf-trends.csv" --days 180
```

`$OUT` is the same scratch directory as the email report, outside the repo. The script lists every
registry test's runs in the window (skipping `running` runs), pulls each run's result tables in
parallel and writes one row per (run, step, metric). For failed, `test_error` and `error` runs it also
stores the linked issues (`argus issue list --run-id`) in the `issues` column. It takes a few minutes for ~1000 runs.

Several result rows for one step (nemesis cycles) collapse into one value: the worst P99, the mean
of anything else.

**Exit criteria:** the script printed `Wrote N rows for M runs`. A test with 0 runs is normal, as in
the email report.

## Phase 2: Refresh the dashboard

**Entry criteria:** Phase 1 wrote the CSV.

Load `ArtifactData` with ToolSearch if it is deferred, then on the dashboard URL:

1. `get` `datasets/runs`: note its `version` and the old asset id (the part of `source.url` after
   `/_blob/`).
2. Upload the CSV: `Artifact` with `action: publish`, `url`, `asset: true`, `file_path` = the CSV.
3. `update` `datasets/runs` with `if_version`: `source: {kind: "file", url: <new /_blob/ url>,
   name: "perf-trends.csv"}`, `updated: {at: <UTC ISO now>, by: "<your name> (fetch_perf_trends.py)"}`,
   `edit: {via: "claude", at: <UTC ISO now>}`.
4. `get` `files/index.html`. If its `text` differs from `references/trends-dashboard.html`, `set` it
   from the template with `if_version`: wrap the file with
   `jq -Rs '{text: ., edit: {via: "claude", at: "<UTC ISO now>"}}'` into a scratch JSON file and
   pass it as `file_path`. Edit the template in the repo, never the copy in the artifact.
5. Delete the old CSV asset (`Artifact` `action: delete`, `url`, `path` = old asset id). Nothing else
   references it.

If `write_db` is refused, you do not have edit access to the dashboard: ask its owner to share it
with you as an editor and stop here. Do not create a new dashboard instead, since readers would keep
opening the old link.

**Exit criteria:** the dataset points at the new asset and the old one is deleted.

## Phase 3: Report

Add the dashboard link to the summary you give the user, and to the email if they want it there.
Mention any chart whose newest master point is more than 10% worse than the median of the previous
4 weeks. Only call it a regression if Argus also flagged the point, or the user agrees.

## Creating a new dashboard

Only when the URL above is gone, or the user asks for a separate one:

1. `Artifact` `action: quickstart`, `intent: other`. Then publish with the Dashboard `type_url` it
   names, `title: "Perf Weekly Trends"`, `auto_open: "after_first_write"`.
2. Upload the CSV as in Phase 2.2.
3. One `ArtifactData` `batch`: `dash/meta` `{title: "Perf Weekly Trends"}`, `datasets/runs`
   `{title: "Perf weekly runs, 6 months", description, source, updated, staleAfterHours: 192, edit}`,
   and `files/index.html` from the template.
4. Update the URL at the top of this file in a PR.
