# Runbook: Recalibrate Throughput Steps and Latency Thresholds

How to derive the throughput staircase and its latency limits from scratch.

Do this when: new hardware, new stress tool, a changed dataset size, or when the `unthrottled`
step is far above the top defined step (the ladder has gone stale).

**Time:** ~1 day of cluster time per (hardware, config) combination, plus 2-3 validation runs.

---

## Step 1 — Measure the ceiling (the uncapped run)

Before you can define percentage-based steps you need the absolute physical limit of the
system, measured so that the bottleneck is unambiguously server-side.

Do this for **each** load type you intend to ship (`read`, `write`, `mixed`,
`read_disk_only`) and each config (tablets, vnodes).

### A. Size the loaders

Provision enough client power to generate ~150% of estimated cluster capacity.

Rule of thumb: if the DB cluster has **N** total vCPUs, the loaders should have at least
**0.75 x N** vCPUs in total.

> Two different ratios circulate. This 0.75:1 figure is the one used for baseline discovery.
> [strategy.md](../performance-testing-strategy.md) states a general loader-to-DB ratio of "at least 1:2" (0.5:1)
> for steady-state regression runs. The uncapped run needs the more generous ratio because it
> is deliberately trying to overwhelm the server.

**Hard gate:** loader CPU must stay under ~70% for the whole run. Above that, the client
pauses for GC and context switching and stops recording the very latency spikes it is
causing (coordinated omission), and the numbers are invalid. Add loaders and repeat.

### B. Split the load across processes, not threads

`cassandra-stress` uses a thread-per-client model. A single process stops scaling past roughly
16-32 threads because of JVM locks and context-switching overhead — you cannot saturate a
modern cluster with one process, no matter how high you set `threads=`.

- **Per process:** `-rate threads=50`. Keep it in the 50-150 range. Do **not** set 1000+.
- **Per loader machine:** run ~4 independent processes.
- **Aggregate:** 3 loaders x 4 processes x 50 threads = 600 concurrent clients, which is
  enough to saturate most clusters without overwhelming the clients.

For range queries, give each process a distinct token range (`-token range=...`). For random
writes distinct ranges are not strictly required, but are still recommended.

```bash
# 4 parallel instances on one loader machine
seq 1 4 | xargs -I{} -P4 cassandra-stress mixed duration=10m -rate threads=50 limit=0 \
    -node 10.0.0.1,10.0.0.2,10.0.0.3
```

latte and scylla-bench do not have this constraint — they are async and shared-nothing, so a
single process scales with cores. For those, raise concurrency rather than process count.

### C. Prepare the cluster

Preload the standard dataset for the test family, wait for compactions to drain
(`wait_no_compactions_running`), then `fstrim`. Measuring against a compaction backlog
invalidates the baseline.

### D. Run uncapped

`limit=0` (cassandra-stress) / no `--rate` (latte) / no `-max-rate` (scylla-bench), for at
least 10 minutes.

### E. Record the result

The average OPS across all processes is `max_throughput` for this (load, config).

**Record the thread and process count alongside it** — a max-throughput number without its
concurrency configuration is not reproducible and cannot be re-derived later.

## Step 2 — Derive the ladder

Steps are **percentages of measured max**, not round numbers carried over from other hardware.
The convention used by the i8g migration (`docs/plans/i8g-performance-jobs-migration.md`,
Phase 4) is:

| Rung | % of max | Purpose |
|---|---|---|
| 1 | ~10% | Sanity — catches OS/network errors, should be near-zero latency |
| 2 | ~50% | Comfortable load |
| 3 | ~75% | Production peak — where the SLA matters most |
| 4 | ~90% | Saturation — queues fill, tests scheduler efficiency |
| 5 | `unthrottled` | The ceiling; also re-measures max every run |

Not every workload needs five rungs. `write` typically ships three.

## Step 3 — Write the step config

Create `configurations/performance/<tool>_gradual_load_steps_<hardware>.yaml`.

**Always record the max-throughput measurements in comments.** Without them nobody can tell
later whether the steps are still percentage-correct. `cassandra_stress_gradual_load_steps_i8g.yaml`
is the model to copy:

```yaml
perf_gradual_threads: {"read": 620, "write": 400, "mixed": 1900, "read_disk_only": 620}
## The value of perf_gradual_throttle_steps[load] is a percentage of the max throughput
## (unthrottled) for that load type, determined by a preliminary run of the stress command
## with no throttle and the specified thread count.
## max throughput for read load with 620 threads is 1,782,000 ops
## max throughput for write load with 400 threads is 690,000 ops
## max throughput for mixed load with 1900 threads is 850,000 ops
## max throughput for read_disk_only load with 620 threads is 450,000 ops
perf_gradual_throttle_steps: {"read": ['500000', '900000', '1200000', '1500000', 'unthrottled'], ...}
perf_gradual_step_duration: {"read": '30m', "write": None, "mixed": '30m', "read_disk_only": '30m'}
```

Rates are **cluster totals** — `current_throttle()` divides by
`num_loaders * stress_num` before handing the value to the tool.

`perf_gradual_threads[load]` must be either a single value (applied to all steps) or a list the
same length as that load's `throttle_steps`.

For latte and other multi-parameter tools, use the dict form instead — see
[runbook-new-stress-tool.md](runbook-new-stress-tool.md#step-configuration-format).

### Step duration

30 minutes is the standard. Shorter runs do not expose compaction fall-behind: a cluster can
hold a rate for two minutes and then degrade at ten. `None` means "run until the stress command
completes" and is used for `write`, where the row count bounds the run.

## Step 4 — Validation runs

Run the full test with the new steps, at least twice:

- [ ] Every step completes without saturating or being throttled by the client
- [ ] No OOM, crashes, or timeouts
- [ ] Metrics scale roughly as expected between rungs
- [ ] Record the observed P99 at each rung — this is the input to Step 5

If a rung saturates (achieved ops well below target), the max measurement was optimistic.
Re-measure and re-derive; do not just lower that one rung.

## Step 5 — Set the latency thresholds

Create `configurations/performance/latency-decorator-error-thresholds-steps-<...>.yaml`.

**The cycle key must match the generated step name exactly.** From
`get_sequential_throttle_steps` (`performance_regression_gradual_grow_throughput.py:454`):

| Situation | Generated name |
|---|---|
| All steps share one thread count | the rate string — `"300000"`, `unthrottled` |
| Thread count varies per step | `<rate>_<threads>_threads` |
| Name repeats | occurrence suffix — `unthrottled_1`, `unthrottled_2` |

A key that does not match **fails silently** and falls back to the `default` block
(`P99 <= 10 ms` from `defaults/test_default.yaml`).

Pick values from the validation runs — achievable, but tight enough to catch a regression.
Tiering convention from [strategy.md](../performance-testing-strategy.md):

| Rung | SLA |
|---|---|
| 50% and 75% | Hard — low single-digit ms P99 |
| 90% | Soft — queuing delay is physics here; verify it stays linear, not exponential |
| `unthrottled` | Latency usually `null`; gate **throughput** instead |

Throughput floors go on the `unthrottled` step. The existing convention for picking one, per
the comments in the shipped files:

> *10% below the avg. of 5 best results in the last 3 months.*

Alternatively use `best_pct` / `best_abs` to let Argus compare against the best historic run
rather than an absolute — see
[results-and-regression.md](results-and-regression.md#structure).

Use explicit `fixed_limit: null` rather than omitting a metric you deliberately do not gate.
It documents the intent, and for the microbenchmark path an omitted key silently becomes
`best_pct=5`.

## Step 6 — Cross-check config against thresholds

Every step defined must have a matching threshold key, or a deliberate `null`. Verify by eye:

```bash
grep -o "'[0-9]*'\|unthrottled" configurations/performance/<steps-file>.yaml
grep -E '^\s{4}"?[0-9]+"?:|^\s{4}unthrottled' configurations/performance/<thresholds-file>.yaml
```

> **Known gap.** In `cassandra_stress_gradual_load_steps_enterprise.yaml` vs
> `latency-decorator-error-thresholds-steps-ent-tablets.yaml`, the `mixed` step `450000` and
> the `write` steps `200000` / `300000` have no threshold entry and silently use the 10 ms
> default. Confirm whether that is intended before copying this pair as a template.

## Checklist

- [ ] Max throughput measured per (load, config), with thread count recorded
- [ ] Loader CPU stayed under ~70% during the uncapped run
- [ ] Steps derived as percentages, rationale in YAML comments
- [ ] Base max values documented in the same file
- [ ] 2+ validation runs clean
- [ ] Threshold file created, keys match generated step names exactly
- [ ] Every step has a threshold entry or an explicit `null`
- [ ] Throughput floor set on `unthrottled`
