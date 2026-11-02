# Runbook: Calibrate a Performance Test That Reproduces a Bug

How to go from "users see X under load" to a predefined-throughput-steps test that shows X
with the buggy build and does not show it with the fix. This is a one-off set of calibration
experiments, run once per new test. Re-deriving steps for an existing test is
[runbook-recalibrate-steps.md](runbook-recalibrate-steps.md); writing the test itself is
[runbook-new-perf-test.md](runbook-new-perf-test.md).

**Worked example:** SMI-324 (Jira SCT-1383). Multi-disk i8g images shipped io_properties that
overstate the disks, so the I/O scheduler never throttles. The reproducer is
`test-cases/performance/latte-perf-regression-mixed-disk-bound-custom-s1-w2-i8g-8xlarge.yaml`,
on the scylla-qa-internal `custom_s1/workload2` rune script. Numbers below come
from its calibration runs on 3 × i8g.8xlarge with 4 × c7i.8xlarge loaders.

**Time and cost:** 4–6 runs of ~2.5 h each. The preload takes ~1.5 h of every run, so run
both sides of each experiment back to back and expect ~1 day.

---

## Step 0 — Write down the signature and the control

Before any run, write down:

- **The user-visible symptom.** For SMI-324: write P99 above 1 s, write timeouts, memtable
  flush backlog.
- **The internal metric that should separate buggy from fixed.** For SMI-324: device queue
  depth and device latency, compared with the wait inside Scylla's I/O queue. Wrong
  io_properties move the queueing from Scylla into the NVMe.
- **The A/B control.** The same binary and image, toggling only the suspected cause. For
  io_properties: no extra settings (shipped preset) vs `SCT_FORCE_RUN_IOTUNE=true` (measured
  figures). Keep everything else identical: version, AMI, region, AZ, instance types.

A test that stresses something else will not show the bug, however many runs it gets. Several
weeks of c-s steps runs (write, `read_disk_only`, mixed on a hot set) showed no difference or a
small one in the wrong direction. The first workload that actually loaded the disks showed it
within one run.

## Step 1 — Design the workload around the suspected resource

Aim the load at the resource the bug is about, and check that it hits it:

| To stress | Do | Check in Prometheus |
|---|---|---|
| Disks | Data ≥ ~9× RAM per node; uniform random keys over all of it; cells large enough that the disks saturate before the CPU (16 KB, not 4 KB) | Cache hit < ~20%; device read and write throughput near the drives' figures |
| CPU | Hot set that fits in cache; small cells | Cache hit > 90%; reactor near 100% |
| Priorities and isolation | Separate commands per class, each in its own service level (roles and service levels created by the rune's `schema` function, picked per command with `--user`/`--password`) | I/O queue wait per class differs between classes |

Data size per node ≈ rows × cell size × RF ÷ nodes. For the example: 150M × 16 KB × 3 ÷ 3
≈ 2.4 TB, against 256 GiB RAM.

With 4 KB cells, the example workload saturated the CPU and the scheduler first, and the disk
effect did not show. Moving to 16 KB fixed it.

## Step 2 — Validate the commands before paying for a preload

A failed command at the start of a run wastes the whole preload (~$25 in the example). Run the
exact stress commands for a minute or two first: locally against a Docker Scylla, or on
minicloud ([downscaling-for-minicloud](../../skills/downscaling-for-minicloud/SKILL.md)).

latte-specific traps:

- String `-P` values need escaped quotes: `-P distribution=\"random\"`. Without them every
  command dies at startup with `Failed to build rune sources`.
- Always add `--request-timeout 30 --retry-number=10 --retry-interval '1s,5s'`. Without them,
  one timed-out request ends the command and the test.
- Split a big preload into parallel commands with `--start-cycle`/`--end-cycle`, one per
  loader.

## Step 3 — Concurrency sweep, both sides (2 runs)

Find the peak and the knee without editing the YAML. Override the steps through Jenkins
`extra_environment_variables`:

```text
SCT_PERF_GRADUAL_STEP_DURATION={"mixed": "10m"}
SCT_PERF_GRADUAL_THROTTLE_STEPS={"mixed": [{"threads": 32, "concurrency": 8, "rate": "unthrottled"}, {"threads": 32, "concurrency": 16, "rate": "unthrottled"}, {"threads": 32, "concurrency": 32, "rate": "unthrottled"}, {"threads": 32, "concurrency": 64, "rate": "unthrottled"}]}
```

- latte and scylla-bench are async: set threads ≈ loader cores and use concurrency per thread
  as the knob. For c-s, sweep threads (it is thread-per-request).
- Run the same sweep on both sides of the A/B.
- **The knee** is where throughput stops rising. Past it, more requests in flight only add
  queueing: by Little's law, mean latency = in-flight ÷ throughput.

Example result from the first sweep (#4 old preset, #5 iotune; ops/s, total of writes and
reads). A repeat sweep (#8, #9) gave ~177k vs ~210k peaks, within the 10–15% spread between
runs:

| Concurrency per thread | Old preset | iotune |
|---|---|---|
| 8 | 121k | 238k |
| 64 | 165k (write P99 1.4 s) | 204k |

The two sides behave differently. iotune peaks at the lowest concurrency. The old preset only
climbs by queueing deeper in the disks: device queue 500–900, device latency 14–24 ms.

## Step 4 — Find out who the limit is, at every step

Throughput alone does not say whether the test measured Scylla. For every step, average
minutes 4–10 (skip the ramp) and read:

| Signature | Meaning |
|---|---|
| Loader CPU > ~70%, Scylla reactor well below 100% | Loaders are the cap. Add loaders; a c7i.8xlarge does ~350k mixed c-s ops/s cleanly. Fewer threads do not help |
| Reactor ~90–100%, loader CPU low, throughput flat as in-flight grows | Scylla CPU is the cap. Extra in-flight only builds a queue |
| Thousands of active+queued reads, read timeouts climbing, ops flat | Too much in flight (c-s 8 × 3,800 = 30,400 on 8xlarge). The test will abort on a failed op. Lower in-flight to ≈ peak × 2–3 × unloaded latency |
| Device queue in the hundreds, device latency ≥ 10 ms, Scylla I/O queue wait ≈ 0 | Scheduler not throttling: io_properties overstated (the SMI-324 signature) |
| Device queue short, Scylla I/O queue wait per class in ms | Scheduler doing its job; priorities apply |

### Pulling the data

- **Per-step latency:** the SCT log lines `HDR histogram summary result` (one per step,
  whole-step summary) and `Run cs command` (step start time). They are also in the Jenkins
  console.
- **Grafana:** `hydra investigate show-monitor <test_id>`.
- **Scripted PromQL:** download `prometheus_data_*.tar.zst` from the run's monitor-set logs
  (Argus) and serve it with a plain `prom/prometheus` container:
  `--storage.tsdb.path=/prometheus --storage.tsdb.retention.time=3650d`.
  node_exporter is scraped every 60 s, so use `rate(...[3m])` for its metrics.

Queries used in the example. `DB` and `LD` are instance regexes; `nv` is
`device=~"nvme[1-9]n1",instance=~"DB"`.

| Metric | PromQL |
|---|---|
| Loader CPU % | `100*(1-avg(rate(node_cpu_seconds_total{mode="idle",instance=~"LD"}[3m])))` |
| Scylla reactor | `avg(scylla_reactor_utilization)` |
| Device queue depth | `avg(rate(node_disk_io_time_weighted_seconds_total{nv}[3m]))` |
| Device read latency (ms) | `1e3*sum(rate(node_disk_read_time_seconds_total{nv}[3m]))/sum(rate(node_disk_reads_completed_total{nv}[3m]))` |
| Scylla I/O queue wait per class (ms) | `1e3*sum(rate(scylla_io_queue_total_delay_sec{class="commitlog",mountpoint!=""}[3m]))/sum(rate(scylla_io_queue_total_operations{class="commitlog",mountpoint!=""}[3m]))`, the same with `class` = `memtable`, `compaction`, or the service level class (`sl:<name>`) |
| Cache hit % | `100*sum(rate(scylla_cache_row_hits[3m]))/(sum(rate(scylla_cache_row_hits[3m]))+sum(rate(scylla_cache_row_misses[3m])))` |
| Blocked writes | `sum(scylla_database_requests_blocked_memory_current)` |
| Active + queued reads | `sum(scylla_database_active_reads)+sum(scylla_database_queued_reads)` |
| Timeouts/s | `sum(rate(scylla_storage_proxy_coordinator_write_timeouts[3m]))`, `..._read_timeouts` |

## Step 5 — Throttled A/B at the same rate (2 runs)

The sweep compares two systems at different throughputs. The proof is both sides at the
**same fixed rate**:

- Two steps of ~20 min at ~65% and ~87% of the **weaker side's** peak.
  Example: 105k and 145k, from the old preset's 165k.
- In-flight per loader must be at least rate per loader × latency budget. Otherwise the
  loaders, not the cluster, fall behind. Example: 32 × 64 = 2,048 in flight ≥ 36k/s × 50 ms.
- **Check the achieved rate before any latency.** latte `--rate` and c-s `fixed=` measure
  latency from each request's scheduled time, so a side that cannot hold the rate shows a
  growing backlog as latency.

Example at 145k:

| | Writes achieved | Write P99 | Read P50 / P99 |
|---|---|---|---|
| Old preset | 69.8k of 72.5k | 43 s | 33 / 369 ms |
| iotune | 72.5k of 72.5k | 2.1 ms | 1.6 / 9.2 ms |

This table is the reproducer. When a fix comes, run this pair again: old vs fix.

## Step 6 — Set the test's final steps

- **Throttled steps:** absolute rates spanning the weaker side's breaking point, up to the
  stronger side's peak. Example: 40k–160k, with the old preset failing around 145k.
- **Unthrottled step:** set concurrency at the knee from Step 3, not at the maximum.
  Example: 32 × 16. Past the knee, both c-s and latte only queue until requests time out.
- Record the sweep peaks, the knee and the A/B result in YAML comments next to the steps.
- Thresholds: [runbook-recalibrate-steps.md](runbook-recalibrate-steps.md), steps 5 and 6.
- The best-throughput-under-a-P99-limit tests (SCT-580, PRs #16563 and #16567) are c-s based.
  If they are converted to latte, the same lessons apply:
  - For latte, raise concurrency rather than threads.
  - Add the retry flags from Step 2.
  - For a disk-bound workload, set the latency limit well above the 10 ms default: with
    iotune, the example's reads already have a P99 of 23 ms at 105k.

## Reading results without fooling yourself

- **Jenkins `FAILED` is not a failure by itself.** Every overloaded calibration step fails the
  Argus latency validation (`FailedResultEvent ... Argus validation failed`). Look for
  CRITICAL events and stress errors before calling a run broken.
- **P99.9 is dominated by the first 10 s of each step.** c-s runs with `No Warmup` and
  undershoots the rate for one interval, which gives 0.3–2.6 s P99.9 on random loaders. Two
  identical runs differed by 2× in step P99.9 from this alone. Compare P99.9 only after
  excluding the first interval (per-10 s lines in the loader logs).
- **Repeat small differences.** Two runs of the same configuration differed by up to 30 ms
  in read P99 (124–158 ms). Claim a difference smaller than that only with ≥ 2 runs per side,
  interleaved (A, B, A, B).

## Checklist

- [ ] Symptom, separating metric and A/B control written down before the first run
- [ ] Workload checked against its target resource (cache hit, device throughput or reactor)
- [ ] Stress commands smoke-tested before the first full preload
- [ ] Concurrency sweep on both sides; knee and peak recorded
- [ ] Limit identified at every step (loaders, CPU, disk, or in-flight overload)
- [ ] Throttled A/B at the same rate, achieved rate checked before latency
- [ ] Final steps and unthrottled concurrency set from the measurements, numbers in YAML comments
