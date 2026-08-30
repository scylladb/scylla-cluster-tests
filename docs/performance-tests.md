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

## Loader CPU diagnostics

A client-observed P99 spike with flat server-side metrics is not necessarily a ScyllaDB problem: the
loaders themselves can be CPU starved, either by the stress tool or by a noisy neighbour on the
hypervisor. Until now the only loader-side signal was `node_load1` from node_exporter, which says
that a loader was busy but not why, so such a failure could only be correlated, never root-caused.

`loader_cpu_diagnostics` starts a 1 Hz sampler on every loader for the whole run:

```yaml
loader_cpu_diagnostics: true
loader_cpu_diagnostics_per_thread: false  # see "Per-thread samples" below
```

or `SCT_LOADER_CPU_DIAGNOSTICS=true`. It is off by default; enable it for the pipeline that is being
diagnosed. The sampler is a systemd service (`loader-cpu-diag`) that costs one wakeup per second, so
it is independent of how many stress threads come and go on a loader. It is not supported on k8s
backends, and - like the node_exporter installation - it is skipped for `reuse_cluster` runs.

### Where the log ends up

The sampler writes `/var/tmp/loader-cpu.log` on the loader. The file is streamed off the loader while
the run goes on (perf loaders get terminated, and the interesting samples are the last ones), lands
in the loader log directory as `loader-cpu.log` and is collected into the run log archive next to
`system.log` - one file per loader.

### Reading it

Every sample is one block, every line tagged with its kind:

```
=== 2026-08-27T09:13:33Z sample=3 uptime=1308728.49
cpu 12:13:34     all    8.97    0.00    1.07    0.63    0.00    0.13    0.00    0.00    0.00   89.22
cpu 12:13:34       7  100.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00    0.00
proc 12:13:34     1000   3017381   99.02    0.00    0.00    0.00   99.02     7  java
ctxt 3017381 14 163 1
pressure some avg10=0.11 avg60=0.92 avg300=0.78 total=3883844852
pressure full avg10=0.00 avg60=0.00 avg300=0.00 total=0
loadavg 2.45 2.57 2.14 3/3349 3017478
```

- `cpu` - `mpstat -P ALL`: `TIME CPU %usr %nice %sys %iowait %irq %soft %steal %guest %gnice %idle`,
  the `all` line first, then one line per vCPU.
- `proc` - `pidstat -u`: `TIME UID PID %usr %system %guest %wait %CPU CPU Command` for the stress
  tool processes. `%wait` is the share of time the process was runnable but not running - the direct
  measure of "the loader could not schedule the stress tool".
- `ctxt` - `PID voluntary_ctxt_switches nonvoluntary_ctxt_switches threads`, cumulative: a jump in
  the *nonvoluntary* counter means the scheduler preempted the process.
- `pressure` - PSI from `/proc/pressure/cpu`: `some` is the share of time at least one task was
  stalled waiting for CPU, `full` the share where every task was. `some avg10` is the single best
  "was this loader CPU starved" number.
- `loadavg` - `/proc/loadavg`, including the `running/total` task counts.

To attribute a latency-step failure that happened at, say, `2026-08-27 09:13:33 UTC`:

1. Find the failing interval in the c-s HDR histograms - `hydra hdr-investigate` (see above) reports
   the intervals whose P99 crosses the threshold.
2. Grep that wall clock second in the loader logs, all loaders at once:

   ```bash
   grep -A 30 '09:13:3' loader-*/loader-cpu.log | grep -E 'pressure some|^.*cpu .* all |proc '
   ```

3. Interpret what shows up:
   - **High `%steal` on the `cpu all` line** - the hypervisor took the CPU away: a noisy neighbour or
     a burstable/credit-limited instance. Not a Scylla problem and not a stress-tool problem.
   - **High `pressure some avg10` with low `%steal`** - the loader oversubscribed itself: the stress
     tool asks for more CPU than the instance has. Check `%wait` on the `proc` lines and the
     nonvoluntary context switches.
   - **Everything quiet on every loader** - the loaders are exonerated; move to the server side
     (`scylla_reactor_stalls_*`, `scylla_database_queued_reads` per shard at that second) or to the
     network.

Because a client-side stall is usually visible on all loaders at once, compare the loaders against
each other: a synchronized pause across independent loaders points away from the loaders and towards
the cluster or the network.

### Per-step figures in Argus

Independently of the sampler, every latency step reports the loader load of its own time window next
to its latencies, in the same Argus table:

| Column | Query | Reads as |
|--------|-------|----------|
| `Loader CPU busy max` | `node_cpu_seconds_total{mode="idle"}` | how hot the hottest loader was, averaged over its vCPUs |
| `Loader CPU steal max` | `node_cpu_seconds_total{mode="steal"}` | CPU the hypervisor took away |
| `Loader CPU pressure max` | `node_pressure_cpu_waiting_seconds_total` | PSI: the loader was CPU starved |
| `Loader load1 max` | `node_load1` | the coarse signal, kept for comparison with older runs |

These need no configuration - they come from the loader `node_exporter` that every run already
scrapes, over a 1 minute range vector, and are the peak across all loaders during the step. Busy is
the peak of the *per-loader average* over its vCPUs, not of a single vCPU: the stress tools spread
their load over all of them, and per-vCPU detail is in the sampler log anyway. They are
there to compare two runs at a glance ("this step failed with the loaders at 90% busy, the passing
one ran them at 56%"); use the sampler log for the second-by-second detail. A column is left out
when its query returned nothing, so an empty cell means no data, not an idle loader.

### Per-thread samples

`loader_cpu_diagnostics_per_thread` adds `thread` lines (`pidstat -tu`, same columns as `proc` with a
`TGID`/`TID` pair) for the 20 hottest threads, on every 10th sample only. Both bounds are deliberate:
a cassandra-stress run has hundreds of threads, and sampling all of them every second would produce
hundreds of MB per run for data that is only useful once the per-process lines already show the stress
tool fighting for CPU. `pidstat` reports OS thread ids; mapping them to JVM thread names needs a
thread dump (`jcmd <pid> Thread.print`), which must be taken **after** the stress command has
finished - a thread dump is itself a safepoint and would perturb the tail latency being measured.

The sampler stops appending at 2 GB (`=== ... size cap reached`), and falls back to raw `/proc`
counters (`rawcpu`/`rawproc` lines, cumulative - diff two samples) when `sysstat` cannot be installed
on the loader.
