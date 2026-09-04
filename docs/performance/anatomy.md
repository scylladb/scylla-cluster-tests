# Anatomy of a Performance Test

The phases every SCT performance test shares, and where each lives in code.

Not every test runs every phase, but the order never changes. If you are adding a test, this
is the skeleton you are filling in.

---

## The phases

```
 0. Provision        clusters, loaders, monitor
 1. Schema           keyspace/table, compaction strategy, tablets vs vnodes
 2. Preload          write the dataset
 3. Settle           wait for compactions to drain  +  fstrim
 4. Warm-up          populate caches at max concurrency, discard results
 5. Measure          one or more *cycles* under the latency decorator
 6. Report           HDR -> Argus, local JSON, Grafana screenshots
 7. Teardown         post_behavior_*_nodes
```

---

## 0. Provision

Standard SCT cluster provisioning from the merged config: `n_db_nodes`, `n_loaders`,
`n_monitor_nodes`, and the instance type (literal `instance_type_db` or constraint-based
`sizing_db` — see [../cross-cloud-sizing.md](../cross-cloud-sizing.md)).

Perf-specific concerns:

- **Loaders must not be the bottleneck.** Loader CPU should stay under ~70%; an overloaded
  client under-reports the latency spikes it causes (coordinated omission). Rule of thumb in
  [strategy.md](../performance-testing-strategy.md): loader-to-DB core ratio of at least 1:2.
- **Local NVMe only.** Never network-attached storage for a perf run.
- **Region assignment matters** — concurrent perf jobs in the same region interfere. See
  [pipelines-and-scheduling.md](pipelines-and-scheduling.md).

## 1. Schema

`prepare_schema` in the gradual test; `post_prepare_cql_cmds` in configs generally. Sets
compaction strategy (STCS / LCS / ICS), replication factor, and — critically — whether the
keyspace is tablet- or vnode-based. Tablets and vnodes are separate job families with
separate thresholds; they are never compared to each other.

## 2. Preload

`preload_data()` (`performance_regression_test.py:214`,
`performance_regression_gradual_grow_throughput.py:328`) runs `prepare_write_cmd`.

Sizing convention: read and latency tests preload to roughly **10x RAM** so the working set
cannot sit entirely in cache. `read_disk_only` workloads depend on this.

## 3. Settle — compactions and fstrim

```python
self.run_fstrim_on_all_db_nodes()
self.wait_no_compactions_running(n=240, sleep_time=180)
self.run_fstrim_on_all_db_nodes()
```
(`performance_regression_test.py:567`, and the same triplet at `:590`, `:615`, `:675`, ...)

**Why this matters more than it looks.** Measuring while a compaction backlog is draining
invalidates the baseline — the run competes with background I/O that has nothing to do with
the workload under test. `fstrim` on either side of the wait keeps SSD write amplification
from leaking between phases.

For tablets there is an extra condition: after `wait_no_compactions_running` returns,
the gradual test also verifies no tablet split/merge is active by checking that
`resize_type` is `none` for all relevant rows in `system.tablets`, continuously for three
minutes. Tablet resizes can start several minutes after compactions look idle.

## 4. Warm-up

`warmup_cache(stress_cmd_templ, params_dict)`
(`performance_regression_gradual_grow_throughput.py:639`), driven by the
`stress_cmd_cache_warmup` config param.

Two details that are easy to get wrong:

- Warm-up runs at the **maximum** thread count and **maximum** concurrency across all steps,
  not the first step's values — so the cache is populated under the highest concurrency the
  test will later use (`gradual_grow_throughput.py:530-539`).
- A fixed `time.sleep(240)` follows, to let background work from the warm-up drain before the
  first measured step.

Warm-up results are discarded. Its purpose is to populate the OS page cache and the ScyllaDB
row cache and to establish connections, so that step 1 is not measuring a cold start.

## 5. Measure — the cycle

A **cycle** is one measured window, and it is always produced by wrapping a callable in
`latency_calculator_decorator`. What the cycle *is* differs per family:

| Family | One cycle = |
|---|---|
| Throughput staircase | one throttle step |
| Latency under load | the whole steady-state window |
| Nemesis | one disruption |
| Upgrade | one node's upgrade |

The staircase loop (`run_gradual_increase_load`,
`performance_regression_gradual_grow_throughput.py:526`) is the clearest example:

```python
for throttle_step_dict, num_threads, current_throttle_step in zip(
        workload.throttle_steps, workload.num_threads, sequential_steps):
    self.prepare_schema(workload=workload)
    step_params = dict(throttle_step_dict)
    step_params.setdefault("threads", num_threads)
    step_params["throttle"] = self.current_throttle(throttle_step_dict, num_loaders, stress_num,
                                                    workload.cs_cmd_tmpl[0])
    run_step = latency_calculator_decorator(
        legend=f"Gradual test step {current_throttle_step} op/s",
        cycle_name=current_throttle_step)(self.run_step)
    results, _ = run_step(stress_cmds=workload.cs_cmd_tmpl, step_params=step_params,
                          step_duration=workload.step_duration)
```

### Throttle syntax is per-tool

`current_throttle()` divides the target rate across loaders and stress processes, then formats
it for the tool (`gradual_grow_throughput.py:505`):

| Tool | Flag |
|---|---|
| latte | `--rate=<value>` |
| scylla-bench | `-max-rate=<value>` |
| cassandra-stress / cql-stress | `fixed=<value>/s` |
| any, `rate == "unthrottled"` | `""` (no throttle) |

`throttle_value = int(rate) // (num_loaders * stress_num)` — the YAML rate is the **cluster
total**, not per-loader.

### Between steps

- 3-minute minimum gap. If `wait_no_compactions` is set for the workload, the test waits for
  compactions and then tops the wait up to 180 s; otherwise it just sleeps.
- `drop_keyspace` per workload, when the step should start from a clean table.

## 6. Report

Handled entirely by the decorator — see
[results-and-regression.md](results-and-regression.md). In short: HDR summary + Prometheus
metrics + Grafana screenshots + reactor stall counts go to `latency_results.json` locally and
to two Argus result tables per cycle, with validation rules attached from
`latency_decorator_error_thresholds`.

Note that `check_latency_during_steps()` and `_calculate_average_max_latency()` in the gradual
test build `<logdir>/result_gradual_increase.log`. That file is **informational** — it does
not participate in pass/fail.

## 7. Teardown

`post_behavior_db_nodes` / `_loader_nodes` / `_monitor_nodes`. Perf jobs default to `destroy`
(see `defaults:` in `configurations/triggers/perf-regression.yaml`).

---

## Checklist for a new test

- [ ] Preload sized so the working set does not fit in cache (unless that is the point)
- [ ] `wait_no_compactions_running` + `fstrim` before the first measurement
- [ ] Warm-up at max concurrency, results discarded
- [ ] Every measured window wrapped in `latency_calculator_decorator` with an explicit `cycle_name`
- [ ] `cycle_name` values match keys in the threshold YAML — see the gotcha in
      [results-and-regression.md](results-and-regression.md#key-matching-gotcha)
- [ ] `use_hdrhistogram` enabled, or the decorator collects nothing
- [ ] HDR tags resolvable by `_get_workload_type_by_hdr_tag`
