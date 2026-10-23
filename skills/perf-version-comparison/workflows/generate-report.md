# Workflow: Generate a Performance Version Comparison Report

Step-by-step process for producing the HTML comparison page described in SKILL.md. [example-report.html](../references/example-report.html) shows the result.

## Step 1: Ask User for Inputs

Ask for the test type (and tablets or vnodes), the load types or variants, the versions, optionally the Argus run IDs, and an optional background (why, which Jira ticket).

```
For each load type, I need the Argus run ID per version (or I can find them):
1. mixed: 2026.2.8 (run_id), 2026.3.2 (run_id)
...
```

## Step 2: Find the Runs

List the test's runs with `argus run list --test-id <test_id> --full --no-cache --limit 50` and apply the run selection rules in SKILL.md Phase 1:
- **predefined-throughput-steps:** group runs by `build_number`; the workload comes from `test_method` (`test_<workload>_gradual_increase_load`).
- Identify builds by the `scylla-server` package.
- Skip `test_error` and `aborted` runs.
- Prefer builds whose Jenkins build cause is the upstream trigger over "Rebuilds build #N".

Keep the run list: Step 5 uses it again for the noise threshold.

## Step 3: Fetch Results and Issues

For each selected run, in parallel:

```bash
timeout 60 argus run results --run-id <run_id> > <scratch_dir>/results_<workload>_<version>.json
timeout 60 argus issue list --run-id <run_id>
```

Use a scratch directory, not a fixed `/tmp` path. Read the title of any linked issue from Jira.

## Step 4: Extract the Metrics

**Predefined-throughput-steps**, per workload:
1. Take the `<workload> - <step> - latencies` tables, skipping `stalls`. The step is the middle part of the name.
2. Sort steps numerically, with unthrottled last. Make one column per step, or one per cycle or repeat (`<step>_<n>`) when there are several.
3. Read `P99 read` and `P99 write` per column.
4. Max throughput comes from the unthrottled column(s) only. For `mixed`, it is read + write.
5. Note every cell whose status is `ERROR`.

**Microbenchmark:** from the single row of the `Perf Simple Query` table, read `median tps`, `mad tps`, `cpu_cycles_per_op`, `instructions_per_op`, `allocs_per_op` and `tasks_per_op`. Note every `ERROR` cell.

## Step 5: Compute the Noise Threshold

- **predefined-throughput-steps:** for each workload, step and metric, take the same value from the test's other recent completed runs (about 30). The threshold is the larger of 5% and `1.4826 × MAD / median`.
- **microbenchmark:** use the per-arch figures in SKILL.md, or compute them the same way from the run history.

## Step 6: Build the Page

Generate the tables and the chart data from one data structure, so they can't drift apart:
1. Test Configuration and Scylla Versions & Packages (one row per run, with workload and status).
2. Per workload, and per metric:
   - a table with versions as rows, ordered by version number;
   - `best` / `worst` marks only where the difference is beyond the threshold;
   - the noise note;
   - the bar chart, with a log y-axis for P99 and a linear one for throughput.
3. A Summary per workload: the best and worst version per metric (or "no difference beyond noise"), anomalies, `ERROR` cells with linked issues, and a short interpretation.

## Step 7: Check and Save

1. Check the version labels, version order and numbers against the Consistency Rules in SKILL.md.
2. Open the page in a browser, or headless Chrome, and confirm every chart renders.
3. Save to `~/Downloads/perf-comparison-<test>-<workload or all>-<v1>-<v2>-<YYYY-MM-DD>.html`, and give the user the path.

## Notes

- Runs may be in different AWS regions or on different AMIs: show it in the configuration tables.
- If the `argus` CLI returns HTML or an auth error, its Cloudflare session expired: run it again without `--non-interactive`.
