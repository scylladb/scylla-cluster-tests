# Stress Tool Tips

Practical lessons for writing and reading perf runs with latte and cassandra-stress. They come
from a series of disk-bound i8g performance runs (SCT-1383). The procedures for wiring a tool in
and deriving steps live in [runbook-new-stress-tool.md](runbook-new-stress-tool.md) and
[runbook-recalibrate-steps.md](runbook-recalibrate-steps.md); this page covers what bites once
the tool is already running.

---

## latte

### Command line

- **Quote string `-P` values.** Inside YAML folded strings, write
  `-P distribution=\"random\"`. Without the escaped quotes every command dies at startup with
  `Failed to build rune sources`.
- **Raise the request timeout for disk-bound steps.** latte defaults to `--request-timeout 5s`
  with 10 retries (`--retry-interval 100ms,5s`). Disk-bound steps stall past 5 s, so set
  `--request-timeout 30` (the repo uses 30 or 60) and a longer interval such as `'1s,5s'`.
- **Retries are not free.** Each retry adds load to a cluster that is already behind, so in
  perf runs they can turn one stall into a longer one. Keep the count low where a failed
  request is acceptable.

### Concurrency

- Requests in flight per loader = **threads x concurrency**. Set threads to roughly the
  loader's vCPU count (32 on c7i.8xlarge) and treat concurrency as the knob.
- Find the knee with a short unthrottled sweep before fixing step values: for example
  concurrency 2 / 4 / 8 / 16 / 32 / 64, 10 minutes each, using
  `SCT_PERF_GRADUAL_THROTTLE_STEPS` and `SCT_PERF_GRADUAL_STEP_DURATION` overrides.
- More is not always better. On 3x i8g.8xlarge with a disk-bound mixed workload, throughput was
  flat from concurrency 2 to 64. Concurrency 128 (4,096 in flight per loader) made reads queue
  past the 30 s timeout and exhaust their retries.
- For throttled steps, size in-flight per loader to at least
  **rate per loader x latency budget** — e.g. 40k ops/s x 50 ms = about 2,048. Otherwise the
  loader, not the cluster, becomes the limit the moment the cluster stalls.

### Making a workload disk-bound

- **Large cells.** 16 KB cells saturate the disks before the CPU. With 4 KB cells the CPU and
  the I/O scheduler saturated first, so the run never showed disk behaviour.
- **Uniform random keys over a dataset much larger than RAM.** `distribution=random` over
  roughly 9x RAM gave a 6-17% cache hit rate.

### Preload and command layout

- Preload in parallel cycle ranges: one command per loader, each with its own
  `--start-cycle` / `--end-cycle` slice.
- Split a mixed load into separate write and read commands when each needs its own service
  level or its own `--rate`. For per-function latency alone, one command with
  `-f write:1 -f read:1` is enough.
- Service levels belong in the rune: create the service levels and login roles in its `schema`
  function (SCT runs it with the superuser), then pick one per command with
  `--user X --password Y`. Examples: scylla-qa-internal `custom_d2/workload1` and
  `custom_s1/workload2`.

### Reading results

- HDR tags are `fn--<function>` for each `-f` function, e.g. `fn--write` and `fn--read`, so
  each function gets its own latency summary.
- Use the whole-step HDR summary, not per-interval values.

---

## cassandra-stress

### The unthrottled step is not "max"

- An unthrottled c-s step is a fixed number of requests in flight: threads per command x
  number of commands across loaders.
- Too many, and Scylla queues reads until they time out — the 8xlarge mixed step aborted at
  30,400 in flight. Too few, and the loaders cap the result.
- Size it as **in-flight ≈ expected peak ops/s x 2-3 x unloaded latency**.
- The cache warm-up runs once with the largest per-step thread count, so an oversized thread
  count hurts there too.

### Loader budget

- One c7i.8xlarge tops out around 450k mixed c-s ops/s. To leave headroom, plan for at most
  **350k mixed ops/s per c7i.8xlarge**.
- Check loader CPU per step before calling a result a Scylla ceiling, using the loader-CPU gate
  in [runbook-recalibrate-steps.md](runbook-recalibrate-steps.md).

### Noise at the tail

- SCT adds `no-warmup` to every c-s command. The first ~10 s of every step undershoot the rate and then catch
  up, which inflates P99.9 (0.3-2.6 s seen) and makes it noisy between identical runs.
  **Do not compare P99.9 across runs.**

### What c-s is good for, and what not

- Good for: the existing steps baselines and CPU-bound regressions. The default mixed steps
  use a cache-resident hot set, so they load the CPU and the cache paths.
- Not for: disk or I/O scheduler problems, which a cache-resident hot set never reaches. For
  disk-bound workloads use latte rather than building a c-s equivalent (see
  [runbook-new-stress-tool.md](runbook-new-stress-tool.md)).

---

## Any tool

### Check the achieved rate before reading P99

- latte (`--rate`) and cassandra-stress (`-rate fixed=N/s`) both measure latency from the
  **scheduled** send time (coordinated-omission corrected). When the cluster falls behind a
  fixed rate, the backlog shows up as latency.
- One latte run reported a write P99 of 43 s while achieving 69.8k of a 72.5k ops/s target.
  **Always compare the achieved rate with the target before trusting a P99.**

### Monitoring snapshot

- node_exporter is scraped every 60 s on SCT monitors. When querying the saved Prometheus data,
  use `rate(...[3m])`; a 1-minute window often has a single sample and returns nothing.
- Start-of-step effects shorter than a minute are invisible in node_exporter metrics.

### Run-to-run spread

- On a single step, expect about 10-15% spread in throughput and up to ~30 ms on read P99
  between identical runs.
- An A/B claim needs a gap clearly larger than that, or repeated runs interleaved per side.

### The disk-bound signature

When a disk-bound workload runs with old or overstated `io_properties`:

- device queue depth 400-1,000 and device latency 12-26 ms;
- Scylla I/O queue wait near zero — the scheduler never throttles;
- blocked writes.

With correct `io_properties`:

- device queue below 40 and sub-millisecond device latency;
- the waiting moves inside Scylla's I/O queue, per scheduling class.

If you see the first pattern, the result measures the disk's queue, not Scylla's scheduler.
