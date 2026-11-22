# Build or Refresh the Microbenchmark Trends Dashboard

Five phases: collect, publish data, publish page, verify, draft the email. Run them in order,
because each phase consumes the previous phase's output.

## Phase 1: Collect From Argus

**Entry:** `argus` CLI is on PATH and logged in (`argus run list --test-id a0063c73-efcf-4878-988d-72af779dc59d --limit 1 --url https://argus.scylladb.com` returns JSON, not `Waiting for login...`).

1. Pick the window. The default is the last three months: `--since` = today minus ~92 days, so
   master has at least 4-5 runs to show a trend.
2. Run the collector into a scratch directory, never into the repository:
   ```bash
   python3 skills/perf-microbenchmark-trends/collect_microbench.py --since 2026-08-18 --out <scratch>/microbench.csv
   ```
   It takes 4-6 minutes (about 160 runs, two Argus calls each, 8 in parallel).
3. Sanity-check the output: one row per run with a result table, 12 distinct `chart` values,
   and the release lines you expect (`master`, the current rc line, the maintained releases).

**Exit:** the CSV exists and the printed line reads `N runs listed, M with results`. Tell the user
M and how many runs were dropped (N - M); dropped runs are test_error or result-less failures.

## Phase 2: Publish the Data File

**Entry:** Phase 1 CSV.

1. **New dashboard:** create it from the Data (Dashboard) Artifact type, title `Microbenchmark Trends`.
   **Existing dashboard:** reuse its URL (`<DASHBOARD_URL>`; ask the dashboard's owner, it is not
   kept in the repository).
2. Upload the CSV as an asset of that artifact (`asset: true`). Keep the returned `/_blob/<id>`.
3. Point the `datasets/ipo` document at it (create with `set`, refresh with `update` pinned to the
   version you read): `source: {kind: "file", url: "/_blob/<id>", name: "microbench.csv", format: "csv"}`
   and `updated: {at: <now ISO>, by: "Claude, from Argus ..."}`. Keep the dataset id `ipo`, because
   every chart, calc and mark in the page references it.

**Exit:** the dataset document names the new blob. Tell the user the previous blob is still
attached; delete it only if they ask.

## Phase 3: Publish the Page

**Entry:** Phase 2 done. Skip this phase on a data-only refresh.

1. Read `files/index.html` from the artifact store first: someone may have edited it in place.
2. For a new dashboard, write `dash/meta` (`{title}`) and `files/index.html` with the text of
   [dashboard-page.html](../references/dashboard-page.html). For a change, edit the stored copy and
   write it back pinned to the version you read.
3. Run `node --check` on the extracted `<script>` before writing, because a syntax error blanks
   the whole page and the store accepts it silently.

**Exit:** the write is committed.

## Phase 4: Verify

**Entry:** Phase 3 (or Phase 2) write committed.

1. Read `data/users/me` in the artifact store. Wait for `check.status` to leave `checking`.
2. `passed` with no `runs` document means the page loaded and every mark matched its data.
   `failed` lists what threw and where; fix it and repeat.
3. If no `check` appears, nobody has the page open; say so instead of claiming it renders.
4. Re-run the `master_trend` and `ladder` calcs locally under node against the CSV and read the numbers, because those
   are the figures you will quote in the email.

**Exit:** `check` is `passed`, or the user knows it is unverified.

## Phase 5: Draft the Email

**Entry:** Phase 4 numbers in hand.

1. Generate the body from the same CSV the dashboard reads, never by retyping numbers:
   ```bash
   python3 skills/perf-microbenchmark-trends/build_email.py --csv <scratch>/microbench.csv \
       --dashboard-url <dashboard url> --out <scratch>/email.html --spark-dir <scratch>/sparks
   ```
   It applies the rules of the dashboard's `master_trend` and `ladder` calcs, so email and page agree to the decimal. Gmail strips
   SVG, so the master-trend sparklines are PNGs in `--spark-dir`, referenced as `cid:att0..attN` in
   the order of `attachments.json`. Attach them inline (`inline: true`, `image/png`) in that order.
   Gmail drafts accept at most 10 attachments by file reference, so pass the small PNGs (12 per
   benchmark, 36 in all) as base64
   content instead; and `update_draft` cannot add files, so a changed body means a new draft (then
   delete the superseded one).
2. Fill the two placeholders in the generated file:
   - `NOTES-GO-HERE`: 2-4 `<li>` notes, each a fact the tables cannot show by themselves:
     - where a master change sits and which build brought it (e.g. "write only, both
       architectures, mostly at the 11-03 build; read barely moved");
     - what each release cut against the previous one, and where master is above the last release;
     - perf-cql-raw run-to-run noise (instructions +-12-17%, cycles up to ~50%), so its single
       moves need a repeat before they count;
     - releases that run only some benchmarks (2026.1 runs only perf-simple-query;
       perf-alternator has master runs only, since 2026-11-22).
   - `SIGNATURE`: the user's first name.
3. Recipients: the regular perf weekly status report list (same To: as the master/release status
   emails). Subject: `Microbenchmark trends, MM/DD/YYYY`.
4. The body layout, which the script produces and which must stay in sync with the dashboard:

   | Part | Content |
   |---|---|
   | Intro | One sentence on what the dashboard covers, then the link |
   | Date line | "Runs from START to END", lower is better, the color rule |
   | Master trend | Run count and dates, then one table per benchmark: rows per metric (Argus names, monospace), columns read arm64, read x86_64, write arm64, write x86_64; each cell a sparkline image, the last-vs-first change and "N runs" |
   | Across releases | One table per benchmark: metric, chart, then 2025.1 ... 2026.4, master; latest value with the change against the previous release under it |
   | Styling | Inline CSS only (Gmail strips `<style>`); light gray `#c8ccd0` borders on every cell, light gray header row, `cpu_cycles_per_op` rows lightly shaded, changes of 1% or more in bold muted green (lower) or muted red (higher); no averages |
   | Notes | The placeholder list from step 2 |

5. Before creating, list existing drafts with subject `Microbenchmark trends`. If one exists for
   this week, update it in place instead of creating another; check its label first, because a
   draft the user already sent shows `SENT` and must not be edited or deleted. If an earlier
   version was already sent, offer to send the new one as a reply in that thread.
6. Create or update it as a draft. Never send.

**Exit:** one current draft, its link given to the user, any stale drafts named for deletion, and a
reminder that the artifact is private until they share it from its Share menu; recipients cannot
open the link before that.
