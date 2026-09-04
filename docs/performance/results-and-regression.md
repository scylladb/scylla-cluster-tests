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

The authoritative source of latency and throughput for perf tests.

Constants: `TIME_INTERVAL = 600` (seconds), `PERCENTILES = [50, 90, 95, 99, 99.9, 99.99, 99.999]`.

Entry points, both no-ops unless `use_hdrhistogram` is set:

| Function | Called from | Returns |
|---|---|---|
| `make_hdrhistogram_summary(...)` | `ClusterTester.get_hdrhistogram` (`sdcm/tester.py:5037`) | one merged summary for `[start, end]` |
| `make_hdrhistogram_summary_by_interval(..., interval=600)` | `ClusterTester.get_hdrhistogram_by_interval` (`sdcm/tester.py:5059`) | list of summaries, one per window |
| `make_hdrhistogram_summary_from_log_line(...)` | live/streaming parsing | one decoded log line |

Mechanics (`_HdrRangeHistogramBuilder`):

- Globs `*/hdrh-*.hdr` under the loaders logdir.
- Reads via `hdrh.log.HistogramLogReader.get_next_interval_histogram(..., absolute=True)`,
  `add()`-ing every interval whose tag matches — **case-insensitively**, because
  user-profile cassandra-stress writes lowercase tags. Tolerates `IndexError` on corrupt lines.
- Histogram config: `lowest=1`, `highest=24h in ns`, `significant_figures=3`.
- Percentiles converted ns -> ms and rounded to 2 decimals; throughput is
  `total_count / duration_seconds`.

`_get_workload_type_by_hdr_tag` maps a tool-specific tag to READ/WRITE:

| Tool | Tags |
|---|---|
| cassandra-stress | `WRITE-st`, `WRITE-rt`, `READ-st`, ... |
| latte | `fn--write`, `fn--get` (rune function names) |
| scylla-bench | `co-fixed`, `raw` -> falls back to `stress_operation` |
| YCSB | `INSERT`, `SCAN`, ... |

It raises `ValueError` when the workload type cannot be determined — **this is the failure
you hit when adding a new stress tool with unfamiliar tags.**

Result keys are `"{WORKLOAD}--{hdr_tag}"`:

```python
{"READ--READ-st": {"start_time": ..., "end_time": ..., "stddev": ...,
                   "percentile_50": ..., "percentile_90": ..., "percentile_99": ...,
                   "percentile_99_9": ..., "throughput": 123456}}
```

### 2.2 Prometheus-derived metrics — `sdcm/utils/latency.py`

`collect_latency(monitor_node, start, end, load_type, cluster, nodes_list)` builds a
`PrometheusDBStats` (`sdcm/db_stats.py:197`, range query, step = Scylla scrape interval) and
issues two query families:

**Client-side** (cassandra-stress metrics pushed into SCT's Prometheus), for P99 and P95:

```python
query = f'sct_cassandra_stress_{load_type}_gauge{{type="lat_{precision}"}}'
res[metric]                               = avg(latency_values_lst)
res[f"{metric}_stdev"]                    = statistics.stdev(latency_values_lst)
res[f"{metric}_points_above_threshold"]   = len([v for v in latency_values_lst if v > 10])
res[f"{metric} max"]                      = max(max_latency_values_lst)
```

**Server-side** (Scylla coordinator histograms), per node, P99 only:

```python
query = (f"histogram_quantile(0.{precision},sum(rate(scylla_storage_proxy_coordinator_{load}_"
         f"latency_bucket{{}}[{duration}s])) by (instance, le))")
# key: f"Scylla P99_{load} - node-{idx}", value in ms
```

`mixed` fans out to `["read", "write"]`; `read_disk_only` to `["read"]`.

Output is a flat dict:
`{"c-s P99": 3.2, "c-s P99_stdev": 0.4, "c-s P99_points_above_threshold": 5, "c-s P99 max": 12.1, "Scylla P99_read - node-1": 1.8, ...}`.

> This whole block is **best-effort** — it is skipped entirely when there is no monitoring set.
> It feeds the local JSON and the Argus screenshots, not the pass/fail decision.

---

## 3. The collection engine — `latency_calculator_decorator`

`sdcm/utils/decorators.py:188`. Signature: `legend`, `cycle_name`, `workload_type`, `row_name`.

Usable bare, parameterised, or applied dynamically:

```python
run_step = latency_calculator_decorator(
    legend=f"Gradual test step {current_throttle_step} op/s",
    cycle_name=current_throttle_step)(self.run_step)
```
(`performance_regression_gradual_grow_throughput.py:566`)

### What it wraps

Anything that constitutes one measured cycle:

| Site | Example |
|---|---|
| Nemesis disruptions | `sdcm/nemesis/__init__.py` — replace node (`:1367`, `:1371`), `repair_nodetool_repair` (`:2133`), `_mgmt_repair_cli` (`:2153`), Manager Backup (`:2906`), `add_new_nodes` (`:3919`), `decommission_nodes` (`:3926`), doubling load (`:3978`), `steady_state_latency` (`:4136`) |
| Perf tests | `performance_regression_test.py:515` (steady state), `:651`, `:848` (`upgrade_node`), `:861`, `:967`, `:974` |
| Gradual steps | `performance_regression_gradual_grow_throughput.py:566` |
| Alternator | `performance_regression_alternator_test.py:67` |
| Manager | `mgmt_cli_test.py:1380` |

### Mechanics

1. Resolves `_self` — `ClusterTester` (-> `db_cluster`/`monitors`) or `NemesisRunner`
   (-> `cluster`/`tester`/`monitoring_set`).
2. Runs the wrapped function inside
   `EventCounterContextManager(event_type=(DatabaseLogEvent.REACTOR_STALLED,))`.
   **Swallows the exception**, collects results anyway, then re-raises at the end — so a
   failing disruption still produces measurements.
3. Returns early unless `use_hdrhistogram` is true.
4. Determines `workload` from the `workload_type` kwarg -> test-name substring
   (`read_disk_only` / `read` / `write` / `mixed`) -> `workload_name` param.
5. Finds HDR tags via `_find_hdr_tags` (`decorators.py:167`): a `hdr_tags` dict key, a
   `.hdr_tags` attribute (`stress_queue`, nemesis), or recursion into lists/tuples.

### What it emits

```python
result = latency.collect_latency(monitor, start, end, workload, cluster, all_nodes_list) if monitor else {}
result["screenshots"]          = monitoring_set.get_grafana_screenshots(node=monitor, test_start_time=start)
result["duration"]             = f"{datetime.timedelta(seconds=int(end - start))}"
result["duration_in_sec"]      = int(end - start)
result["hdr"]                  = tester.get_hdrhistogram_by_interval(hdr_tags, workload, start, end)
result["hdr_summary"]          = tester.get_hdrhistogram(hdr_tags, workload, start, end)
result["cycle_hdr_throughput"] = round(sum(v["throughput"] for v in result["hdr_summary"].values()))
result["reactor_stalls_stats"] = reactor_stall_stats
```

Then it:

6. Persists into `<logdir>/latency_results.json` (`tester.latency_results_file`,
   `sdcm/test_config.py:241`) as `latency_results[func_name] = {"legend": ..., "cycles": [...]}`,
   or `latency_results["Steady State"] = result` when `"steady" in func_name.lower()`.
7. Calls `send_result_to_argus(...)` with `name = func_name` (or `"Steady State"`),
   `cycle = row_name or len(cycles)`, and
   `error_thresholds = tester.params.get("latency_decorator_error_thresholds")`.
8. On any failure inside this block, publishes `TestFrameworkEvent(Severity.ERROR)` — which
   by itself fails the test.

Grafana screenshots come from `BaseMonitorSet.get_grafana_screenshots` (`sdcm/cluster.py:7967`)
-> `GrafanaScreenShot.collect()` -> S3 links. `send_result_to_argus` picks the `overview` and
`scylla-per-server-metrics-nemesis` ones as TEXT columns.

---

## 4. Argus result tables — `sdcm/argus_results.py`

Built on `argus.client.generic_result` (`StaticGenericResultTable`, `ColumnMetadata`,
`ResultType`, `Status`, `ValidationRule`). Vendored copy at `argus/client/generic_result.py`.

### Latency tables

| Class | Columns |
|---|---|
| `LatencyCalculatorMixedResult` (`:48`) | `P90 write`, `P90 read`, `P99 write`, `P99 read` (ms, FLOAT, `higher_is_better=False`); `Throughput write`, `Throughput read` (op/s, INTEGER, `higher_is_better=True`); `duration` (DURATION); `start time`, `Overview`, `QA dashboard` (TEXT) |
| `LatencyCalculatorWriteResult` (`:67`) | write-only subset |
| `LatencyCalculatorReadResult` (`:83`) | read-only subset |
| `LatencyCalculatorReadDiskOnlyResult` (`:99`) | read-only subset |
| `ReactorStallStatsResult` (`:115`) | `total` + one column per `STALL_INTERVALS = [10,20,30,50,100,200,1000,2000]` ms bucket |

Selection:

```python
workload_to_table = {"mixed": LatencyCalculatorMixedResult, "write": LatencyCalculatorWriteResult,
                     "read": LatencyCalculatorReadResult,
                     "read_disk_only": LatencyCalculatorReadDiskOnlyResult,
                     "throughput": LatencyCalculatorMixedResult}
```

### Other perf tables

| Class | Purpose | Validation |
|---|---|---|
| `MicrobenchmarkResult` (`:214`) | `allocs_per_op`, `cpu_cycles_per_op`, `instructions_per_op`, `logallocs_per_op`, `tasks_per_op` (lower better); `min/max/median/mad tps` (higher better). Table name `f"{workload} - {benchmark_name}"`. | see §6 |
| `LatteStressLatencyComparison` (`:275`) | before/after upgrade: `before_ops`, `before_mean`, `before_p99`, `after_p99`, `after_mean`, `after_ops`. Only consumer: `upgrade_test.py:1203`. | **none — judged by a human in Argus** |
| `IOPropertiesResultsTable` / `IOPropertiesDeviationResultsTable` (`:236`, `:250`) | disk IO properties | deviation table has class-level `ValidationRule(fixed_limit=15)` plus a client-side `Status.PASS if value < 15 else WARNING` |
| `ManagerRestoreBenchmarkResult` (`:128`) | Manager restore timings | class-level `ValidationRules` with `best_pct=10` on every timing/bandwidth column |
| `ManagerOneOneRestoreBenchmarkResult`, `ManagerBackupBenchmarkResult`, `ManagerBackupReadResult`, `ManagerSnapshotDetails`, `MigratorBenchmarkResult` (`:195`) | Manager / migrator | varies |
| `PeriodicDiskUsageToArgus` (`:524`) | background thread, per-rack disk-usage delta | client-side PASS/WARNING/ERROR |

### `send_result_to_argus(...)` (`:311`)

Builds **two** tables per cycle:

```python
result_table.name         = f"{workload} - {name} - latencies"
result_table_summary.name = f"{workload} - {name} - Summary latencies"
if error_thresholds:
    error_thresholds = error_thresholds[workload]["default"] | error_thresholds[workload].get(name, {})
    result_table.validation_rules = {metric: ValidationRule(**rules) for metric, rules in error_thresholds.items()}
    result_table_summary.validation_rules = result_table.validation_rules
```

So the effective rules for a cycle are the `default` block **overridden by** the block whose
key equals the decorator's `name` (`cycle_name` / function name / `"Steady State"`).

Row and value population:

- Row name is the cycle: `f"Cycle #{cycle}"` for ints, or `row_name` verbatim. With more than
  one HDR tag it becomes `f"{cycle} (HDR tag: {hdr_tag})"`. (`skip_hdr_tag` is true when there
  is one tag, or two tags for `mixed`.)
- Per HDR tag: `P90 <workload_type>` and `P99 <workload_type>` from
  `percentile_90` / `percentile_99`; `Throughput <workload_type>` from `throughput`.
  **All submitted with `status=Status.UNSET` — the server decides.**
- The summary table keeps the **worst** P90/P99 across tags and the **sum** of throughput, and
  is only submitted when `hdr_summary_len > 2` (multi-command latte customer scenarios).
- `duration`, `start time` (UTC `%H:%M:%S`) and the `Overview` / `QA dashboard` S3 links are
  added once.
- Finally one `ReactorStallStatsResult` table **per stall event type**:
  `f"{workload} - {name} - stalls - {event_name}"`.

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

The innermost mapping is splatted into `ValidationRule(**rules)`, so allowed keys are exactly
the dataclass fields (`argus/client/generic_result.py:46`):

```python
@dataclass
class ValidationRule:
    best_pct: float | None = None    # max value limit relative to best result, percent
    best_abs: float | None = None    # max value limit relative to best result, absolute
    fixed_limit: float | None = None
```

| Rule | Meaning |
|---|---|
| `fixed_limit` | Absolute bound, evaluated server-side. For latency columns (`higher_is_better=False`) a ceiling in ms; for `Throughput *` (`higher_is_better=True`) a floor in ops/s. |
| `best_pct` / `best_abs` | **Relative to the best historic result** in Argus for the same table+column. This is the comparison-to-history mechanism that already exists today. |
| `null` | Column is recorded but **not** validated. Kept explicitly rather than deleted, because the microbenchmark path falls back to `best_pct=5` when a key is absent entirely. |

Throughput `fixed_limit` values carry a provenance comment in the YAMLs:
*"10% below the avg. of 5 best results in the last 3 months."*

### Two different shapes

| Path | Nesting | Because |
|---|---|---|
| Latency (`send_result_to_argus`) | `workload -> cycle -> metric -> rule` (3 levels) | does `error_thresholds[workload]["default"] \| error_thresholds[workload].get(name, {})` |
| Microbenchmark (`send_microbenchmark_result_to_argus`) | `workload -> metric -> rule` (2 levels) | does `error_thresholds.get(workload, {}).get(column, {})` |

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

- Declared at `sdcm/sct_config.py:1567` as `latency_decorator_error_thresholds: DictOrStr`.
- Baseline value in `defaults/test_default.yaml:322` — a `default` block for all four
  workloads with `P99 *: fixed_limit: 10` and `P90 *: null`.
- Overridden by SCT's layered merge — the threshold file is just another entry in the
  pipeline's `test_config` list. Because the merge is recursive
  (`anyconfig.merge(..., ac_merge=MS_DICTS)`), the `default` block from `test_default.yaml`
  **survives** and the step-specific keys are added alongside it.
- Some test-cases inline the dict instead of using a separate file, e.g.
  `test-cases/performance/perf-regression-alternator-{basic,full}.yaml:88`,
  `configurations/performance/latte-perf-regression-latency-steady-state-custom-d1-workload1.yaml:32`.
- Consumed in exactly two places: `sdcm/utils/decorators.py:318` and `microbenchmarking_test.py:35`.

### Key-matching gotcha

The cycle key must equal, character for character, the string the decorator passes as `name`.
For gradual steps that string comes from `get_sequential_throttle_steps` /
`_step_names` (`performance_regression_gradual_grow_throughput.py:454,479`):

- a repeated step becomes `unthrottled_1`, `unthrottled_2`, ...
- a per-step thread variation becomes `<rate>_<threads>_threads`

Neither matches a plain `unthrottled` key. **A non-matching key fails silently** — the cycle
falls back to the `default` block (`P99 <= 10 ms`) with no warning.

Worked example of the failure mode, using
`configurations/performance/cassandra_stress_gradual_load_steps_enterprise.yaml` against
`configurations/performance/latency-decorator-error-thresholds-steps-ent-tablets.yaml`:

| Workload | Steps defined | Threshold keys present | Falls back to `default` |
|---|---|---|---|
| `read` | `150000, 300000, 450000, 600000, 700000, unthrottled` | all six | — |
| `mixed` | `50000, 150000, 300000, 450000, unthrottled` | `50000, 150000, 300000, unthrottled` | **`450000`** |
| `write` | `200000, 300000, unthrottled` | `unthrottled` | **`200000`, `300000`** |
| `read_disk_only` | `80000, 165000, 250000, 300000, unthrottled` | all five | — |

Whenever you add or change a step, check the threshold file in the same change.

---

## 6. Microbenchmark validation — `send_microbenchmark_result_to_argus` (`:433`)

```python
def set_validation_rules(column_metadata):
    if column_threshold := error_thresholds.get(workload, {}).get(column_metadata, {}):
        return ValidationRule(**column_threshold)
    else:
        return ValidationRule(best_pct=5)   # fallback: regression vs best historic run

validation_rules["instructions_per_op"] = set_validation_rules("instructions_per_op")
validation_rules["allocs_per_op"]       = set_validation_rules("allocs_per_op")
```

Only those two columns are validated. `cpu_cycles_per_op` entries in the YAMLs are currently
**inert** — the column is recorded and displayed but never gated.

---

## 7. How pass/fail is actually decided

There is **no in-SCT numeric comparison** for latency or throughput. The chain is:

1. SCT submits each row with `status=Status.UNSET` plus a `validation_rules` dict on the table.
2. The **Argus server** evaluates each cell against its rule — `fixed_limit` as an absolute
   bound, `best_pct`/`best_abs` against the best historic result for that table+column, with
   direction taken from `ColumnMetadata.higher_is_better` — and assigns PASS/ERROR per cell.
3. On failure `argus_client.submit_results()` raises `ArgusClientError` with
   `args[1] == "DataValidationError"`; `submit_results_to_argus` (`:298`) converts it:

```python
except ArgusClientError as exc:
    if exc.args[1] == "DataValidationError":
        FailedResultEvent(f"Argus validation failed for the result in {result_table.name}."
                          f" Please check the 'Results' tab for more details.").publish()
    else:
        raise
```

4. `FailedResultEvent` (`sdcm/sct_events/system.py:362`) defaults to `Severity.ERROR`, and any
   ERROR/CRITICAL event fails the run (`sdcm/tester.py:1039`):

```python
def get_test_status(self) -> str:
    summary = self.get_event_summary()
    if summary.get("ERROR", 0) or summary.get("CRITICAL", 0):
        return "FAILED"
    return "SUCCESS"
```

`argus_finalize_test_run` (`sdcm/tester.py:728`) maps that to `TestStatus.FAILED`, or
`TEST_ERROR` when `_is_test_error()` matches infra patterns such as
`InsufficientInstanceCapacity`.

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

## 8. Dead and informational code paths

These affect **nothing** in the pass/fail decision. Know them so you do not chase them.

| Path | Status |
|---|---|
| `calculate_latency(latency_results)` (`latency.py:95`) | Computes `Cycles Average`, `Relative to Steady` and `color` (`red` if delta >= 10 ms, `yellow` if >= 5, else `blue`) into the local JSON. Skips `NON_METRIC_FIELDS` and any `*stdev*` / `*threshold*` key. Log only. |
| `analyze_hdr_percentiles(result_stats)` (`latency.py:141`) | Colors HDR percentiles against the hardcoded `LATENCY_ERROR_THRESHOLDS` (`{"replace_node": {...}, "default": {"percentile_90": 5, "percentile_99": 10}}`). Cosmetic. Only consumer is `performance_regression_gradual_grow_throughput.py:385`, output to `<logdir>/result_gradual_increase.log`. |
| `PerformanceRegressionTest.display_results()` (`:152`) | Logs a table and writes `<logdir>/jenkins_perf_PerfPublisher.xml` (legacy Jenkins PerfPublisher format). No thresholds. |
| `save_total_summary_in_file()` (`gradual_grow_throughput.py:604`) | Writes `<logdir>/result_gradual_increase.log`. |
| `sdcm/utils/benchmarks.py` | `ScyllaClusterBenchmarkManager` (sysbench/fio node benchmarks) still references ES with `self._es = None`; noted as broken since the ES removal. |

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
