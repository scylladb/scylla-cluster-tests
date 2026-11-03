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
- **Always set a request timeout and retries:**
  `--request-timeout 30 --retry-number=10 --retry-interval '1s,5s'`. Without them a single
  timed-out request ends the whole command, and with it the step.

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
- Split a mixed load into separate write and read commands. Each one can then run under its own
  service level (the `<sla credentials N>` placeholder), and each gets its own HDR tag.

### Reading results

- HDR tags are the Rune function names, typically `fn--write` and `fn--read`.
- Use the whole-step HDR summary, not per-interval values.
- latte measures latency from the **scheduled** send time (coordinated-omission corrected).
  When the cluster falls behind a fixed `--rate`, the backlog shows up as latency. One run
  reported a write P99 of 43 s while achieving 69.8k of a 72.5k ops/s target. **Always compare
  achieved rate with target before trusting a P99.**

---

## cassandra-stress

### The unthrottled step is not "max"

- An unthrottled c-s step is a fixed number of requests in flight: threads per command x
  number of commands across loaders.
- Too many, and Scylla queues reads until they time out — the 8xlarge mixed step aborted at
  30,400 in flight. Too few, and the loaders cap the result.
- Size it as **in-flight ≈ expected peak ops/s x 2-3 x unloaded latency**.
- Per-step threads also apply to the cache warm-up, so an oversized thread count hurts there
  too.

### Loader budget

- One c7i.8xlarge tops out around 450k mixed c-s ops/s. Rule of thumb: plan for at most
  **350k mixed or 450k write ops/s per c7i.8xlarge**.
- Check loader CPU per step before calling a result a Scylla ceiling. If doubling the loaders
  does not raise throughput and the DB reactor is near 100%, the limit is Scylla; otherwise it
  is the loaders.

### Noise at the tail

- c-s runs with "No Warmup". The first ~10 s of every step undershoot the rate and then catch
  up, which inflates P99.9 (0.3-2.6 s seen) and makes it noisy between identical runs.
  **Do not compare P99.9 across runs.**
- `-rate fixed=N/s` charges the backlog into latency the same way latte's `--rate` does —
  check achieved rate before reading P99.

### What c-s is good for

- The default mixed steps use a cache-resident hot set. That is CPU-bound and will not show
  disk or I/O scheduler problems.
- For disk-bound workloads use latte rather than building a c-s equivalent. Team direction:
  new workloads go to latte; existing c-s tests stay as they are.

---

## Any tool

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
