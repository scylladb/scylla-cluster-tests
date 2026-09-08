# Performance Tests

## HDR investigate utility

The hdr_investigate utility is essential for performance analysis because it allows users to scan HDR (High Dynamic Range) histogram files
with fine-grained time intervals, rather than only looking at overall or coarse-grained metrics.
By analyzing latency metrics (such as P99) in smaller intervals, the tool helps pinpoint the exact time windows where latency spikes occur.
This makes it possible to correlate these spikes with specific events or Scylla processes, enabling users to identify which Scylla process
or operation is causing performance problems.
This targeted approach greatly improves the efficiency and accuracy of performance troubleshooting in distributed database environments.

Key features:
- Supports multiple stress tools and operations (READ/WRITE).
- Can fetch HDR files from Argus by test ID or use a local folder.
- Allows specifying the time window and scan interval for analysis.
- Reports intervals where P99 latency exceeds a user-defined threshold.

Usage example:

```bash
hydra hdr-investigate \
  --stress-operation READ \
  --throttled-load true \
  --test-id 8732ecb1-7e1f-44e7-b109-6d789b15f4b5 \
  --start-time "2025-09-14\ 20:45:18" \
  --duration-from-start-min 30
```

Main options:
- --test-id: Test run identifier (fetches logs from Argus if --hdr-folder is not provided).
- --stress-tool: Name of the stress tool (cassandra-stress, scylla-bench, or latte) (default: cassandra-stress).
- --stress-operation: Operation type (READ or WRITE).
- --throttled-load: Whether the load was throttled (True or False).
- --start-time: Start time for analysis (format: YYYY-MM-DD\ HH:MM:SS).
- --duration-from-start-min: Duration in minutes to analyze from the start time.
- --error-threshold-ms: P99 latency threshold in milliseconds (default: 10).
- --hdr-summary-interval-sec: Interval in seconds for summary scan (default: 600).
- --hdr-folder: Path to local folder with HDR files (optional).

This utility is useful for performance engineers and developers investigating latency issues in distributed database clusters.

## Loader JVM safepoint logging

Throttled perf steps sometimes fail a fixed P99 threshold because of a short (~1-2s) stall that the aggregate
tail magnifies through coordinated-omission correction. The cassandra-stress JVM frequently shows **0 GC** in
those runs, so the GC log cannot explain the pause and there is no way to tell a **non-GC safepoint** in the
loader JVM (biased lock revocation, JIT deoptimization, thread dump, class redefinition, monitor deflation, the
periodic "guaranteed safepoint") apart from a server-side or network stall.

`cs_safepoint_logging` makes every cassandra-stress JVM log all of its safepoints, so the loaders can be
implicated or exonerated within a single run.

### Enabling it

```yaml
cs_safepoint_logging: true
```

or `SCT_CS_SAFEPOINT_LOGGING=true`. It is off by default; enable it for the pipeline that is being diagnosed.
Before turning it on for a throughput-sensitive baseline, do one control run and compare the tail latency -
`-Xlog:safepoint` only emits at safepoints that happen anyway and is cheap, but `safepoint+stats` is a bit
heavier.

The flag appends the unified logging options to whatever `cs_extra_jvm_opts` already carries (the ZGC and heap
flags used by the perf pipelines are kept), so both end up in the `JVM_OPTS` of the c-s container:

```
-Xlog:safepoint*=info:file=/cs-safepoint-<operation>-<log-id>.log:time,uptime,level,tags:filecount=0
```

`safepoint*=info` selects both the `safepoint` and the `safepoint+stats` tag sets; a bare `safepoint=info`
would match the exact tag set only and drop the stats.

Not supported for k8s backends or `use_prepared_loaders: true` - `JVM_OPTS` is only injected into the c-s
docker container, so the configuration is rejected at validation time.

### Where the log ends up

The log file is bind mounted into the c-s container from the loader host (the container is removed as soon as
the stress command finishes), pulled into the loader log directory when the stress thread is over, and
collected into the run log archive as `cs-safepoint-<operation>-l<loader_idx>-c<cpu_idx>-k<keyspace_idx>-<timestamp>-<uuid>.log`
next to the `hdrh-*.hdr` files - one file per stress thread, sharing the log id with the `cassandra-stress-*.log`
and `hdrh-*.hdr` files of the same thread. Empty logs are dropped instead of being collected.

### Reading it

Each safepoint is one line:

```
[2026-06-09T17:10:55.207+0000][0.278s][info][safepoint] Safepoint "G1CollectForAllocation", Time since last: 64444016 ns, Reaching safepoint: 3047 ns, Cleanup: 34865 ns, At safepoint: 2472966 ns, Leaving safepoint: 2519 ns, Total: 2513397 ns
```

- the quoted name is the operation/reason: `G1CollectForAllocation` / `ZMarkStart` (GC), `Deoptimize`,
  `ICBufferFull`, `ThreadDump`, `RevokeBias`, `Cleanup`, `no vm operation` = the periodic guaranteed safepoint;
- **`Total`** is the stop-the-world time, what the application actually lost;
- **`Reaching safepoint`** is the TTSP - how long it took to bring all threads to the safepoint;
- the `time`/`uptime` decorators map every line to a UTC wall clock.

`safepoint+stats` adds, at JVM exit, a per-operation table plus a `Maximum sync time` / `Maximum cleanup time` /
`Maximum vm operation time` summary - a one-line answer to "was there any long safepoint in this run at all".

To attribute a latency-step ERROR that happened at, say, `2026-06-09 17:10:55 UTC`:

1. Find the failing interval in the c-s HDR histograms - `hydra hdr-investigate` (see above) reports the
   intervals whose P99 crosses the threshold.
2. Check the `Maximum vm operation time` summary line of every loader log first - if the worst safepoint of
   the whole run is a couple of ms, the loaders are out of the picture already.
3. Otherwise grep the safepoint logs of all loaders around that wall clock second:

   ```bash
   grep '17:10:5' cs-safepoint-*.log | sort
   ```

4. Interpret what shows up at that second:
   - **A pause of comparable magnitude** (hundreds of ms up to ~2s) - the loader JVM is the cause. A large
     `Total` with a small `Reaching safepoint` means a long VM operation, and the quoted operation name says
     which one.
   - **A small `Total` but a large `Reaching safepoint`** - threads were slow to reach the safepoint: a
     blocked/JNI thread, page faults or swap, CPU steal. Cross-check the loader `node_exporter` CPU steal and
     memory metrics.
   - **Nothing aligned with the stall** - the loader JVM is exonerated. Move to the server side
     (`scylla_reactor_stalls_*`, `scylla_database_queued_reads` per shard on the DB nodes at that second) or to
     the network.

Because the stall is usually visible on all loaders at once, compare the loaders against each other: a
synchronized pause across independent JVMs points away from the loaders and towards the cluster or the network.

## Small-dataset runs for cheap feature testing

The predefined-throughput-steps pipelines populate ~650GB per node and run 30-minute throttle steps, so a
single run takes many hours of loader and DB instance time. When the thing being tested is the *feature* -
a new SCT option, a stress-command change, a pipeline wiring, a nemesis, a reporting change - and not the
absolute numbers, two configuration files cut that down to a fast and cheap run:

| file | what it overrides |
|---|---|
| `configurations/performance/perf-predefined-throughput-steps-small-dataset.yaml` | `prepare_write_cmd`, `stress_cmd_w`, `stress_cmd_r`, `stress_cmd_cache_warmup`, `stress_cmd_m`, `stress_cmd_read_disk` - the dataset of each, divided by 6 |
| `configurations/performance/cassandra_stress_gradual_load_steps_small_dataset.yaml` | `perf_gradual_throttle_steps` - one throttled step plus `unthrottled` per load; `perf_gradual_step_duration` - 30m -> 10m (a copy of `cassandra_stress_gradual_load_steps_i8g.yaml`; `perf_gradual_threads` is left untouched) |

### What the reduction is

| stress command | full size | small dataset |
|---|---|---|
| `prepare_write_cmd`, `stress_cmd_read_disk` | 650,000,004 rows (~650GB) | 108,333,336 rows (~108GB) |
| `stress_cmd_w` | 1,610,612,736 rows | 268,435,456 rows |
| `stress_cmd_r`, `stress_cmd_cache_warmup`, `stress_cmd_m` | 20,000,000 rows | 3,333,336 rows |

Everything else is deliberately unchanged: four commands per parameter (one per loader, `round_robin: true`),
contiguous non-overlapping `-pop seq` ranges, `-col 'size=FIXED(1024) n=FIXED(1)'`, the `threads`/`throttle`
rates and the `$threads` / `$throttle` / `$duration` placeholders the test substitutes per step. The commands
therefore exercise exactly the same code paths as the full-size ones.

The load ramp is shortened the same way - every load keeps its lowest rate and the final `unthrottled` step,
and drops the intermediate ones:

| load | full-size steps (ops) | small-dataset steps (ops) |
|---|---|---|
| `read` | 500000, 900000, 1200000, 1500000, unthrottled | 500000, unthrottled |
| `mixed` | 250000, 480000, 600000, 750000, unthrottled | 250000, unthrottled |
| `write` | 350000, 600000, unthrottled | 350000, unthrottled |
| `read_disk_only` | 110000, 220000, 330000, 400000, unthrottled | 110000, unthrottled |

In practice the population phase drops from ~72 min to ~12 min (four loaders at `throttle=37500/s`), and each
of the read, mixed and read_disk_only workloads from 5x30m of load to 2x10m - the throttled step is still
compared against the unthrottled one, which is what most feature checks need.

### How to use them

Both files are overrides layered on top of the base test case. `test_config` files are merged in order, so
they must come *after* `test-cases/performance/perf-regression-predefined-throughput-steps.yaml` and after any
`cassandra_stress_gradual_load_steps_*.yaml`:

```groovy
perfRegressionParallelPipeline(
    backend: "aws",
    region: "us-east-1",
    test_name: "performance_regression_gradual_grow_throughput.PerformanceRegressionPredefinedStepsTest",
    test_config: '''["test-cases/performance/perf-regression-predefined-throughput-steps.yaml",
                     "configurations/performance/cassandra_stress_gradual_load_steps_small_dataset.yaml",
                     "configurations/performance/perf-predefined-throughput-steps-small-dataset.yaml",
                     "configurations/disable_kms.yaml",
                     "configurations/arm_instance_types/i8g_4xlarge.yaml"]''',
    sub_tests: ["test_mixed_gradual_increase_load"],
)
```

or locally:

```bash
SCT_CONFIG_FILES='["test-cases/performance/perf-regression-predefined-throughput-steps.yaml","configurations/performance/cassandra_stress_gradual_load_steps_small_dataset.yaml","configurations/performance/perf-predefined-throughput-steps-small-dataset.yaml"]' \
  hydra run-test performance_regression_gradual_grow_throughput.PerformanceRegressionPredefinedStepsTest --backend aws
```

The two files are independent - the steps one can be used alone to shorten a full-size run, and the dataset
one alone to shrink a run that keeps the full 30m ramp.

### Caveats

These configurations are for **validating that something works**, not for measuring performance:

- **The results are not comparable to the baseline runs** and must not be used for regression tracking or
  reported as perf numbers. Fewer partitions, a smaller working set and shorter steps all move the throughput
  and latency figures.
- **The surviving throttle rates are not rescaled.** The rates that are kept (500000 / 250000 / 350000 /
  110000 ops) and `perf_gradual_threads` are still the values derived from max-throughput measurements on the
  full-size i8g runs, so a step that was "28% of max" there is a different fraction of max here. The
  `unthrottled` step is the only one that keeps its meaning.
- **There is no load ramp any more.** With one throttled step and one unthrottled step per load, a trend
  across increasing load cannot be read out of the run at all - use the full step list for that.
- **`read_disk_only` is no longer 100% disk.** That workload has no cache warmup and nothing that drops the
  cache - it reads from disk only because the full-size data set is ~5x the node RAM (~650GB per node with
  RF=3, against the 128GiB of an `i8g.4xlarge` / `i4i.4xlarge`). At ~108GB per node the data set is smaller
  than the machine's memory and Scylla populates the cache on the write path, so a large part of it is already
  resident when the population phase ends. The hit rate is not predictable - the row cache is only ~75-90GB
  after the memory reserve and memtables, and each loader sweeps its quarter of the range sequentially, which
  can thrash an LRU cache - but the workload is no longer the disk-bound read its name promises. Keep the
  full-size data set for anything that depends on reading from disk.
- **Two 10m steps are short for latency validation.** `latency_calculator_decorator` and the
  `latency-decorator-error-thresholds-*` thresholds get a third of the samples per step and a fraction of the
  steps, so a threshold breach in a small-dataset run is worth reproducing at full size before believing it.
