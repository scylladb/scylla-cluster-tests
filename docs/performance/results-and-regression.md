# Results, Validation and Regression Detection

How a performance number travels from the loader to a PASS/FAIL verdict.

---

## 1. The chain

```
stress tool  ->  .hdr files on loaders          Prometheus (SCT + Scylla)
                          |                                 |
                          +----------------+----------------+
                                           |
                         latency_calculator_decorator  (one cycle)
                                           |
                            result dict {hdr_summary, hdr, latency,
                                         throughput, duration,
                                         screenshots, reactor_stalls_stats}
                                           |
                    +----------------------+----------------------+
                    v                                             v
       latency_results.json (local, informational)     send_result_to_argus()
       result_gradual_increase.log                     -> GenericResultTable
                                                       -> validation_rules per column
                                                                  |
                                                       Argus server evaluates cells
                                                                  |
                                              ArgusClientError("DataValidationError")
                                                                  |
                                                    FailedResultEvent (Severity.ERROR)
                                                                  |
                                                       get_test_status() -> "FAILED"
```

> **Elasticsearch is gone.** `sdcm/results_analyze/` is an empty directory; `sdcm/es.py`,
> `sdcm/utils/es_queries.py` and the ES-based `PerformanceResultsAnalyzer` were removed in
> commit `1747db8f9`. `sdcm/send_email.py` was removed in `159e9208c` — email reporting is
> Argus-side, selected by the `argus_email_report_template` config param.
> **Argus is the single source of truth for perf comparison today.**

---

## 2. Where the numbers come from

### 2.1 HDR histograms — `sdcm/utils/hdrhistogram.py`

The authoritative source of latency and throughput for perf tests. Percentiles reported are
50, 90, 95, 99, 99.9, 99.99 and 99.999; the per-interval window is 600 s.

Entry points, both no-ops unless `use_hdrhistogram` is set:

| Function | Called from | Returns |
|---|---|---|
| `make_hdrhistogram_summary` | `ClusterTester.get_hdrhistogram` | one merged summary for `[start, end]` |
| `make_hdrhistogram_summary_by_interval` | `ClusterTester.get_hdrhistogram_by_interval` | a list of summaries, one per 600 s window |
| `make_hdrhistogram_summary_from_log_line` | live/streaming parsing | one decoded log line |

Mechanics, in `_HdrRangeHistogramBuilder`:

- Globs `*/hdrh-*.hdr` under the loaders logdir.
- Reads intervals via `hdrh.log.HistogramLogReader`, adding every interval whose tag matches —
  **case-insensitively**, because user-profile cassandra-stress writes lowercase tags. Corrupt
  lines are tolerated.
- Percentiles are converted ns -> ms and rounded to 2 decimals; throughput is
  `total_count / duration_seconds`.

`_get_workload_type_by_hdr_tag` maps a tool-specific tag to READ or WRITE by substring:

| Tool | Tags |
|---|---|
| cassandra-stress | `WRITE-st`, `WRITE-rt`, `READ-st`, ... |
| latte | `fn--write`, `fn--get` (Rune function names) |
| scylla-bench | `co-fixed`, `raw` -> falls back to `stress_operation` |
| YCSB | `INSERT`, `SCAN`, ... |

It raises `ValueError` when the workload type cannot be determined — **this is the failure you
hit when adding a new stress tool with unfamiliar tags.** The decorator turns that into a
`TestFrameworkEvent(ERROR)`, which fails the run.

Result keys are `"{WORKLOAD}--{hdr_tag}"`, e.g. `"READ--READ-st"`, each holding `start_time`,
`end_time`, `stddev`, `percentile_50` ... `percentile_99_9` and `throughput`.

### 2.2 Prometheus-derived metrics — `sdcm/utils/latency.py`

`collect_latency` builds a `PrometheusDBStats` range query (step = Scylla scrape interval) and
issues two query families:

- **Client-side**, from the cassandra-stress gauges pushed into SCT's Prometheus
  (`sct_cassandra_stress_<load>_gauge{type="lat_<precision>"}`), for P99 and P95. It records
  the average, the standard deviation, a count of points above 10 ms, and the max.
- **Server-side**, from the Scylla coordinator histograms
  (`scylla_storage_proxy_coordinator_<load>_latency_bucket`), P99 only, per node, keyed
  `Scylla P99_<load> - node-<idx>` in ms.

`mixed` fans out to read and write; `read_disk_only` to read only. Output is one flat dict, e.g.
`{"c-s P99": 3.2, "c-s P99_stdev": 0.4, "c-s P99_points_above_threshold": 5, "c-s P99 max": 12.1,
"Scylla P99_read - node-1": 1.8, ...}`.

> This whole block is **best-effort** — skipped entirely when there is no monitoring set. It
> feeds the local JSON and the Argus screenshots, not the pass/fail decision.

---

## 3. The collection engine — `latency_calculator_decorator`

`sdcm/utils/decorators.py`. Parameters: `legend`, `cycle_name`, `workload_type`, `row_name`.
Usable bare, parameterised, or applied dynamically when the cycle name is only known at runtime
(which is what the staircase loop does — see [anatomy.md](anatomy.md#5-measure--the-cycle)).

### What it wraps

Anything that constitutes one measured cycle: a nemesis disruption (`sdcm/nemesis/__init__.py`
— replace node, `repair_nodetool_repair`, `_mgmt_repair_cli`, Manager backup, `add_new_nodes`,
`decommission_nodes`, doubling load, `steady_state_latency`), a perf test's steady-state window
or `upgrade_node` call, one gradual step, an Alternator run, or a Manager operation in
`mgmt_cli_test.py`.

### Mechanics

1. Resolves `_self` — a `ClusterTester` (giving `db_cluster` / `monitors`) or a `NemesisRunner`
   (giving `cluster` / `tester` / `monitoring_set`).
2. Runs the wrapped function inside an `EventCounterContextManager` counting
   `DatabaseLogEvent.REACTOR_STALLED`. **Swallows the exception**, collects results anyway, then
   re-raises at the end — so a failing disruption still produces measurements.
3. Returns early unless `use_hdrhistogram` is true.
4. Determines `workload` from the `workload_type` kwarg, else a test-name substring
   (`read_disk_only` / `read` / `write` / `mixed`), else the `workload_name` param.
5. Finds HDR tags via `_find_hdr_tags`: a `hdr_tags` dict key, a `.hdr_tags` attribute on
   `stress_queue` or the nemesis, or recursion into lists and tuples.

### What it emits

One result dict per cycle, containing the Prometheus latency block, Grafana screenshot S3 links
(`BaseMonitorSet.get_grafana_screenshots` -> `GrafanaScreenShot.collect()`), `duration` and
`duration_in_sec`, the per-interval `hdr` list, the merged `hdr_summary`, a
`cycle_hdr_throughput` total, and `reactor_stalls_stats`.

Then it:

6. Persists into `<logdir>/latency_results.json` as
   `latency_results[func_name] = {"legend": ..., "cycles": [...]}`, or
   `latency_results["Steady State"]` when the function name contains "steady".
7. Calls `send_result_to_argus` with `name` = the function name (or `"Steady State"`),
   `cycle` = `row_name` or the cycle count, and
   `error_thresholds` = the `latency_decorator_error_thresholds` param.
8. On any failure inside this block, publishes a `TestFrameworkEvent(Severity.ERROR)` — which by
   itself fails the test.

### Logs to look at

All of these are DEBUG level in `sct.log`, from `sdcm/utils/decorators.py`:

| Log line | Tells you |
|---|---|
| `latency_calculator_decorator cluster: ...` | A cycle started, and against which cluster |
| `latency_calculator_decorator: <name> raised <exc>, will still collect latency results` | The wrapped function failed; measurement continued and the exception is re-raised at the end |
| `HDR summary added to results: {}` | Empty summary — no HDR file matched or no tag resolved, so every Argus latency cell for this cycle will be missing |
| `HDR throughput: ...` | The per-cycle throughput total that goes to Argus |
| `Reactor stalls stats: ...` | What lands in the stalls table |
| `Send to Argus` / `Saved in Argus` | Submission bracket; a missing `Saved in Argus` means the submit path raised |
| `Failed to collect/report latency results for <name>` | The whole collect block failed — published as `TestFrameworkEvent(ERROR)`, which fails the run |

Two more, from outside the decorator:

| Log line | Tells you |
|---|---|
| `Failed to detect the workload type for the following hdr_tag: <tag>` | A tag the stress tool wrote is not classifiable — see §2.1 |
| `Argus validation failed for the result in <table>` | The server rejected a cell; open the Argus **Results** tab to see which |

`<logdir>/result_gradual_increase.log` is the human-readable per-step summary. Informational
only — it never affects pass/fail.

---

## 4. Argus result tables — `sdcm/argus_results.py`

Built on `argus.client.generic_result` (`StaticGenericResultTable`, `ColumnMetadata`,
`ResultType`, `Status`, `ValidationRule`); vendored copy at `argus/client/generic_result.py`.

### Latency tables

| Class | Columns |
|---|---|
| `LatencyCalculatorMixedResult` | `P90 write`, `P90 read`, `P99 write`, `P99 read` (ms, FLOAT, `higher_is_better=False`); `Throughput write`, `Throughput read` (op/s, INTEGER, `higher_is_better=True`); `duration`; `start time`, `Overview`, `QA dashboard` (TEXT) |
| `LatencyCalculatorWriteResult` | write-only subset |
| `LatencyCalculatorReadResult` | read-only subset |
| `LatencyCalculatorReadDiskOnlyResult` | read-only subset |
| `ReactorStallStatsResult` | `total` plus one column per stall bucket (10, 20, 30, 50, 100, 200, 1000, 2000 ms) |

The table is selected by `workload`: `mixed` and `throughput` -> mixed table, `write` -> write
table, `read` -> read table, `read_disk_only` -> read-disk-only table.

### Other perf tables

| Class | Purpose | Validation |
|---|---|---|
| `MicrobenchmarkResult` | `allocs_per_op`, `cpu_cycles_per_op`, `instructions_per_op`, `logallocs_per_op`, `tasks_per_op` (lower better); `min/max/median/mad tps` (higher better). Table named `<workload> - <benchmark_name>`. | see §6 |
| `LatteStressLatencyComparison` | before/after upgrade: `before_ops`, `before_mean`, `before_p99`, `after_p99`, `after_mean`, `after_ops`. Only consumer is `upgrade_test.py`. | **none — judged by a human in Argus** |
| `IOPropertiesResultsTable` / `IOPropertiesDeviationResultsTable` | disk IO properties | deviation table has a class-level `ValidationRule(fixed_limit=15)` plus a client-side PASS/WARNING split at 15 |
| `ManagerRestoreBenchmarkResult` | Manager restore timings | class-level rules with `best_pct=10` on every timing/bandwidth column |
| `ManagerOneOneRestoreBenchmarkResult`, `ManagerBackupBenchmarkResult`, `ManagerBackupReadResult`, `ManagerSnapshotDetails`, `MigratorBenchmarkResult` | Manager / migrator | varies |
| `PeriodicDiskUsageToArgus` | background thread, per-rack disk-usage delta | client-side PASS/WARNING/ERROR |

### `send_result_to_argus`

Builds **two** tables per cycle, `<workload> - <name> - latencies` and
`<workload> - <name> - Summary latencies`. When `error_thresholds` is set, the effective rules
for a cycle are the workload's `default` block **overridden by** the block whose key equals the
decorator's `name` (the `cycle_name`, the function name, or `"Steady State"`); each entry is
splatted into a `ValidationRule` and attached to both tables.

Row and value population:

- The row name is the cycle: `Cycle #<n>` for ints, or `row_name` verbatim. With more than one
  HDR tag it becomes `<cycle> (HDR tag: <hdr_tag>)`.
- Per HDR tag: `P90 <workload_type>` and `P99 <workload_type>` from the 90th and 99th
  percentiles, `Throughput <workload_type>` from `throughput`.
  **All submitted with `status=Status.UNSET` — the server decides.**
- The summary table keeps the **worst** P90/P99 across tags and the **sum** of throughput, and
  is only submitted for multi-command scenarios (more than two HDR summaries — the latte
  customer workloads).
- `duration`, `start time` and the `Overview` / `QA dashboard` S3 links are added once.
- Finally one `ReactorStallStatsResult` table per stall event type,
  `<workload> - <name> - stalls - <event_name>`.

---

## 5. Threshold configuration — `configurations/performance/`

### Structure

Every threshold file has one top-level key matching the SCT config param:

```yaml
latency_decorator_error_thresholds:
  <workload: write|read|mixed|read_disk_only>:
    <cycle name: default | "Steady State" | step name | nemesis method name>:
      <Argus column name>:
        fixed_limit: <number|null>   # or best_pct / best_abs
```

The innermost mapping is splatted into `ValidationRule`, so the allowed keys are exactly its
fields:

| Rule | Meaning |
|---|---|
| `fixed_limit` | Absolute bound, evaluated server-side. For latency columns (`higher_is_better=False`) a ceiling in ms; for `Throughput *` (`higher_is_better=True`) a floor in ops/s. |
| `best_pct` / `best_abs` | Limit **relative to the best historic result** in Argus for the same table and column, as a percentage or an absolute delta. This is the comparison-to-history mechanism that already exists today. |
| `null` | Column is recorded but **not** validated. Keep the key explicitly rather than deleting it — the microbenchmark path falls back to `best_pct=5` when a key is absent entirely. |

Throughput `fixed_limit` values carry a provenance comment in the YAMLs:
*"10% below the avg. of 5 best results in the last 3 months."*

### Two different shapes

| Path | Nesting | Because |
|---|---|---|
| Latency (`send_result_to_argus`) | `workload -> cycle -> metric -> rule` | it merges the workload's `default` block with the block named after the cycle |
| Microbenchmark (`send_microbenchmark_result_to_argus`) | `workload -> metric -> rule` | it looks a column up directly under the workload |

### The files

| File | Cycle keys | Metrics validated |
|---|---|---|
| `latency-decorator-error-thresholds-nemesis-ent-{tablets,vnodes}.yaml` | nemesis method names: `_mgmt_repair_cli`, `terminate_node`, `add_new_nodes`, `decommission_nodes`, `replace_node`, `_run_manager_backup` | `duration` only (seconds) |
| `latency-decorator-error-thresholds-steps-ent-{tablets,vnodes,i8g-tablets}.yaml` | throttle step names (`"150000"`, ..., `unthrottled`) | `P90/P99 read\|write` (ms); `Throughput read\|write` on `unthrottled` only |
| `latency-decorator-error-thresholds-steps-latte-{tablets,vnodes}.yaml` | same | same (mostly `null`, pending baseline) |
| `latency-decorator-error-thresholds-steps-lwt-{heavy,light}-{tablets,vnodes}.yaml` | LWT steps | same |
| `latency-decorator-error-thresholds-steps-ent-tablets-custom-d3-w1.yaml` | custom scenario | same |
| `perf_simple/...-perf-simple-query-microbenchmark_{x86_64,arm64}.yaml` | *none* — metrics sit directly under `write`/`read` | `allocs_per_op`, `cpu_cycles_per_op`, `instructions_per_op` |
| `perf_cql_raw/...-perf-cql-raw-microbenchmark_{x86_64,arm64}.yaml` | same | same (all `null`) |

### Loading

- Declared in `sdcm/sct_config.py` as `latency_decorator_error_thresholds: DictOrStr`.
- The baseline value lives in `defaults/test_default.yaml`: a `default` block for all four
  workloads with `P99 *: fixed_limit: 10` and `P90 *: null`.
- A threshold file is just another entry in the pipeline's `test_config` list. Because the merge
  is recursive, that `default` block **survives** and the step-specific keys are added alongside
  it.
- Some test cases inline the dict instead of using a separate file, e.g. the Alternator cases
  and the latte custom steady-state overlay.
- Consumed in exactly two places: `sdcm/utils/decorators.py` and `microbenchmarking_test.py`.

### Key-matching gotcha

The cycle key must equal, character for character, the string the decorator passes as `name`.
For gradual steps that string comes from `get_sequential_throttle_steps` / `_step_names`:

- a repeated step becomes `unthrottled_1`, `unthrottled_2`, ...
- a per-step thread variation becomes `<rate>_<threads>_threads`

Neither matches a plain `unthrottled` key. **A non-matching key fails silently** — the cycle
falls back to the `default` block (`P99 <= 10 ms`) with no warning in the log.

Worked example, `cassandra_stress_gradual_load_steps_enterprise.yaml` against
`latency-decorator-error-thresholds-steps-ent-tablets.yaml`:

| Workload | Steps defined | Threshold keys present | Falls back to `default` |
|---|---|---|---|
| `read` | `150000, 300000, 450000, 600000, 700000, unthrottled` | all six | — |
| `mixed` | `50000, 150000, 300000, 450000, unthrottled` | `50000, 150000, 300000, unthrottled` | **`450000`** |
| `write` | `200000, 300000, unthrottled` | `unthrottled` | **`200000`**, **`300000`** |
| `read_disk_only` | `80000, 165000, 250000, 300000, unthrottled` | all five | — |

Whenever you add or change a step, check the threshold file in the same change.

---

## 6. Microbenchmark validation

`send_microbenchmark_result_to_argus` validates **only** `instructions_per_op` and
`allocs_per_op`. For each, it uses the threshold from the YAML if one is present, and otherwise
falls back to `best_pct=5` — a regression against the best historic run. `cpu_cycles_per_op`
entries in the YAMLs are therefore **inert**: the column is recorded and displayed but never
gated.

---

## 7. How pass/fail is actually decided

There is **no in-SCT numeric comparison** for latency or throughput. The chain is:

1. SCT submits each row with `status=Status.UNSET`, plus a `validation_rules` dict on the table.
2. The **Argus server** evaluates each cell against its rule — `fixed_limit` as an absolute
   bound, `best_pct` / `best_abs` against the best historic result for that table and column,
   with direction taken from `ColumnMetadata.higher_is_better` — and assigns PASS or ERROR per
   cell.
3. On failure `argus_client.submit_results()` raises `ArgusClientError("DataValidationError")`,
   which `submit_results_to_argus` converts into a `FailedResultEvent` carrying
   *"Argus validation failed for the result in `<table>`. Please check the 'Results' tab for
   more details."* Any other `ArgusClientError` is re-raised.
4. `FailedResultEvent` defaults to `Severity.ERROR`, and `ClusterTester.get_test_status` returns
   `"FAILED"` if the event summary contains any ERROR or CRITICAL.

`argus_finalize_test_run` maps that to `TestStatus.FAILED`, or to `TEST_ERROR` when
`_is_test_error()` matches an infrastructure pattern such as `InsufficientInstanceCapacity`.

### What this means in practice

| | Gated today? |
|---|---|
| **Latency** | Yes, by `P90`/`P99` `fixed_limit`. Defaults to `P99 <= 10 ms` from `test_default.yaml`; `P90` is `null` (unchecked) by default. |
| **Throughput** | Only where a `Throughput *` `fixed_limit` or `best_pct` exists — in practice the `unthrottled` step of the `steps-ent-*` files. Elsewhere recorded but never checked. |
| **Nemesis duration** | Yes, by the `nemesis-ent-*` files. |
| **Upgrade before/after** | **No.** `LatteStressLatencyComparison` has no validation rules. |
| **Microbenchmarks** | `instructions_per_op` and `allocs_per_op` only. |
| **Reactor stalls** | Recorded, not gated. |
| **System metrics (CPU, IO, memory, compaction)** | Not collected at all — see [strategy.md](../performance-testing-strategy.md), Pillar 2. |

---

## 8. Informational-only paths

These affect **nothing** in the pass/fail decision. Know them so you do not chase them.

| Path | Status |
|---|---|
| `calculate_latency` (`sdcm/utils/latency.py`) | Computes `Cycles Average`, `Relative to Steady` and a `color` (red if the delta is >= 10 ms, yellow if >= 5, else blue) into the local JSON. Log only. |
| `analyze_hdr_percentiles` (`sdcm/utils/latency.py`) | Colours HDR percentiles against a hardcoded `LATENCY_ERROR_THRESHOLDS` table. Cosmetic; its only consumer writes `<logdir>/result_gradual_increase.log`. |
| `PerformanceRegressionTest.display_results()` | Logs a table and writes `<logdir>/jenkins_perf_PerfPublisher.xml` (legacy Jenkins PerfPublisher format). No thresholds. |
| `save_total_summary_in_file()` in the gradual test | Writes `<logdir>/result_gradual_increase.log`. |
| `sdcm/utils/benchmarks.py` | `ScyllaClusterBenchmarkManager` (sysbench/fio node benchmarks) still references ES with `self._es = None`; broken since the ES removal. |

Useful, not dead: `sdcm/utils/argus.py` (`get_argus_client`, `ReplayOnlyArgusSCTClient`,
`create_proxy_argus_s3_url`).

---

## 9. Local artifacts of a perf run

| File | Contents |
|---|---|
| `<logdir>/latency_results.json` | Every cycle's full result dict, plus `Steady State` |
| `<logdir>/result_gradual_increase.log` | Gradual-steps summary with `Cycles Average`, `Relative to Steady`, `color`, `ops_rate` |
| `<logdir>/jenkins_perf_PerfPublisher.xml` | Legacy Jenkins PerfPublisher XML |
| `<loaders logdir>/*/hdrh-*.hdr` | Raw HDR histogram logs — the input to `hydra hdr-investigate` |
