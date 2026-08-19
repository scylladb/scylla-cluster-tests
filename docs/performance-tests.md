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
-Xlog:safepoint+stats=info,safepoint=info:file=/cs-safepoint-<operation>-<log-id>.log:time,uptime,level,tags:filecount=0
```

Requires a JDK 11+ cassandra-stress - the default `scylladb/cassandra-stress` image ships Temurin 21.
**Do not enable it together with a JDK 8 loader image** (`configurations/stress_images/cs-java8.yaml`): JDK 8
does not know `-Xlog` and refuses to start the JVM. Its JDK 8 equivalents are
`-XX:+PrintGCApplicationStoppedTime -XX:+PrintSafepointStatistics -XX:PrintSafepointStatisticsCount=1`, which
can be passed through `cs_extra_jvm_opts` instead. On JDK 11 the `safepoint+stats` tag does not exist yet and
the JVM prints a harmless "No tag set matches selection" warning at startup.

Not supported for k8s backends or `use_prepared_loaders: true` - `JVM_OPTS` is only injected into the c-s
docker container.

### Where the log ends up

The log file is bind mounted into the c-s container from the loader host (the container is removed as soon as
the stress command finishes), pulled into the loader log directory when the stress thread is over, and
collected into the run log archive as `cs-safepoint-<operation>-l<loader_idx>-c<cpu_idx>-k<keyspace_idx>-<timestamp>-<uuid>.log`
next to the `hdrh-*.hdr` files - one file per stress thread, sharing the log id with the `cassandra-stress-*.log`
and `hdrh-*.hdr` files of the same thread. Empty logs are dropped instead of being collected.

### Reading it

On JDK 17+ (so on every current loader image) each safepoint is one line:

```
[2026-06-09T17:10:55.207+0000][0.278s][info][safepoint] Safepoint "G1CollectForAllocation", Time since last: 64444016 ns, Reaching safepoint: 3047 ns, Cleanup: 34865 ns, At safepoint: 2472966 ns, Leaving safepoint: 2519 ns, Total: 2513397 ns
```

- the quoted name is the operation/reason: `G1CollectForAllocation` / `ZMarkStart` (GC), `Deoptimize`,
  `ICBufferFull`, `ThreadDump`, `RevokeBias`, `Cleanup`, `no vm operation` = the periodic guaranteed safepoint;
- **`Total`** is the stop-the-world time, what the application actually lost;
- **`Reaching safepoint`** is the TTSP - how long it took to bring all threads to the safepoint;
- the `time`/`uptime` decorators map every line to a UTC wall clock.

On JDK 11 the same data comes in the older wording - `Entering safepoint region: <operation>` followed by
`Total time for which application threads were stopped: N seconds, Stopping threads took: N seconds`.

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
