# Investigating a Performance Regression Failure

A 7-phase process for comparing a failed and passed SCT performance regression test run to identify the root cause of a latency regression.

---

## Phase 1: Gather Run Metadata

**Entry:** You have two run IDs (or URLs) -- one failed, one passed -- for the same performance test.

**Actions:**

1. **Get run details from Argus.** For both runs:
   ```bash
   argus run get --run-id <FAILED_UUID>
   argus run get --run-id <PASSED_UUID>
   ```
   Extract: region, instance type, Scylla version, SCM revision, AMI, node count, status, Jenkins URL.

2. **Check for code/AMI differences.** Compare `scm_revision_id` and `image_id` between runs. Different revisions mean potential code changes that could affect performance. Flag these immediately.

3. **Get events for the failed run.** Look for `FailedResultEvent` entries:
   ```bash
   argus run events --run-id <FAILED_UUID>
   ```
   The event message identifies which step and metric failed (e.g., "read - 1200000 - latencies").

4. **Identify the Prometheus endpoints.** The user should provide the Grafana/monitoring URLs. Extract the host IPs. Prometheus is typically on port 9090 of the same host. Verify connectivity:
   ```bash
   curl -s --max-time 10 "http://<HOST>:9090/api/v1/status/config"
   ```

5. **Determine the time window.** From the Grafana URLs or the test results, extract the start and end timestamps for the specific step being investigated. Convert to UTC ISO format for Prometheus queries.

**Exit:** Both run details documented, Prometheus endpoints confirmed reachable, time windows identified for the step under investigation.

---

## Phase 2: Query Prometheus Metrics

**Entry:** Phase 1 complete. Prometheus endpoints and time windows known.

**Actions:**

1. **Query P99 read latency per node.** This is the primary metric:
   ```bash
   curl -s 'http://<HOST>:9090/api/v1/query_range' \
     --data-urlencode 'query=max(rlatencyp99) by (instance)' \
     --data-urlencode 'start=<ISO_START>' \
     --data-urlencode 'end=<ISO_END>' \
     --data-urlencode 'step=20s'
   ```
   Run against both Prometheus instances. Compare max values. The `rlatencyp99` metric is a pre-computed summary (renamed by Prometheus relabel rules from `scylla_storage_proxy_coordinator_read_latency_summary`).

2. **Query queued reads per node.** This is the #1 indicator of read admission throttling:
   ```bash
   # query: max(scylla_database_queued_reads) by (instance)
   ```
   Any value above ~20 is suspicious. Values in the hundreds or thousands indicate severe throttling.

3. **Query cache evictions per node.** High eviction rates cause memory churn:
   ```bash
   # query: sum(rate(scylla_cache_row_evictions[1m])) by (instance)
   ```
   Compare max rates between runs. 10x difference is a strong signal.

4. **Query reactor utilization.** Verify CPU isn't the bottleneck:
   ```bash
   # query: avg(scylla_reactor_utilization) by (instance)
   ```
   Similar utilization between runs means the regression isn't from CPU saturation.

5. **Query secondary metrics.** If primary metrics don't explain the issue, check:
   - LSA memory compaction: `sum(rate(scylla_lsa_memory_compacted[1m])) by (instance)`
   - IO queue delays: `max(rate(scylla_io_queue_total_delay_sec[1m])) by (instance, class)`
   - Cache hit rate: `1 - sum(rate(scylla_cache_row_misses[1m])) by (instance) / sum(rate(scylla_cache_reads[1m])) by (instance)`
   - TCP retransmissions: `rate(node_netstat_Tcp_RetransSegs[1m])`

See [references/prometheus-queries.md](../references/prometheus-queries.md) for complete query syntax with parsing scripts.

**Exit:** Key metrics collected for both runs. Anomalous metrics identified. At least one clear signal found (e.g., queued reads, evictions, IO delays).

---

## Phase 3: Identify the Offending Node

**Entry:** Phase 2 complete. Anomalous metrics identified.

**Actions:**

1. **Rank nodes by the anomalous metric.** For queued reads, find which node had the highest max value and at what time.

2. **Get the detailed timeline for the offending node.** Query with `instance="<IP>"` and a finer step (20s) to pinpoint exact spike times:
   ```bash
   # query: max(scylla_database_queued_reads{instance="<IP>"})
   # step=20s
   ```
   Record timestamps of all significant spikes.

3. **Cross-reference with other nodes.** Verify the issue is localized to one node. If all nodes spike simultaneously, the cause is likely external (loader, network, coordinator).

4. **Check correlated metrics on the offending node at spike times.** At each spike timestamp, check:
   - Cache evictions (did they spike at the same time?)
   - Reactor utilization (was CPU saturated?)
   - IO queue delays (was disk slow?)

**Exit:** Offending node identified with timestamped spikes. Correlated metrics documented. Single-node vs cluster-wide determination made.

---

## Phase 4: Correlate with HDR Histograms

**Entry:** Phase 3 complete. Spike timestamps known.

**Actions:**

1. **Find the HDR data in the SCT log.** Search for the step name in the SCT log:
   ```bash
   grep -n "<STEP_RATE>" <sct-log-file> | grep "latency_results\|hdr"
   ```
   The log contains full HDR histogram data with per-interval breakdowns.

2. **Parse the interval data.** Each step is split into ~10-minute intervals with:
   - `start_time` / `end_time` (Unix timestamps in milliseconds)
   - `percentile_50/90/95/99/99_9` (in milliseconds)
   - `throughput` (actual ops/s)
   - `stddev` (latency standard deviation)

3. **Map Prometheus spikes to HDR intervals.** Convert the spike timestamps from Phase 3 to the HDR interval boundaries. Identify which intervals contain the spikes.

4. **Determine the pattern.** Common patterns:
   - **First-interval-only spike:** Startup transient (compaction catch-up, cache warming). The test may need a warmup period.
   - **Periodic spikes:** Compaction cycles or background tasks interfering.
   - **Progressive degradation:** Resource exhaustion (memory, disk space).
   - **Random single spike:** Infrastructure event (noisy neighbor, EBS hiccup).

**Exit:** HDR intervals mapped to Prometheus spikes. Degradation pattern identified and classified.

---

## Phase 5: Download and Analyze Logs

**Entry:** Phase 4 complete. Spikes correlated with time windows.

**Actions:**

1. **Download DB cluster logs via Argus:**
   ```bash
   argus run logs list --run-id <UUID>
   argus run logs download db-cluster-<ID>.tar.zst --run-id <UUID> --dest <dir>
   ```

2. **Map node numbers to IPs.** Check `scylla.yaml` for each node:
   ```bash
   grep "broadcast_rpc_address" <dir>/<node-dir>/scylla.yaml
   ```

3. **Check reactor stalls on the offending node.** Search the system.log:
   ```bash
   grep "Reactor stall" <node-dir>/system.log
   ```
   Note: stall timestamps, durations, scheduling groups (compaction, statement, etc.), and affected shards. Stalls in the `compaction` group during earlier steps indicate compaction pressure that may carry over.

4. **Check for errors/warnings in system.log.** Look for:
   - `large allocation` messages
   - `Failed to delete` (SSTable cleanup issues)
   - `oom` or memory-related messages
   - Raft/group0 leadership changes

5. **Check kernel/OS logs.** The `messages.log` may contain:
   - OOM killer events
   - NVMe/disk errors
   - Network interface resets

6. **Check memory state.** Review `mem_info` and `vmstat` files for the node:
   - Total memory, free memory, swap usage
   - Locked (mlock) memory amount
   - Any swap activity indicates severe memory pressure

7. **Optionally download the SCT log** for more context:
   ```bash
   argus run logs download sct-<ID>.log.tar.zst --run-id <UUID> --dest <dir>
   ```

**Exit:** Log evidence collected. Reactor stalls quantified. System-level anomalies (or lack thereof) documented.

---

## Phase 6: Write Root Cause Analysis

**Entry:** Phase 5 complete. All evidence collected.

**Actions:**

1. **Document the run environment.** Create a comparison table: region, instance type, Scylla version, AMI, SCM revision, node count.

2. **Document key findings.** Create a side-by-side metrics table (failed vs passed) for all queried metrics.

3. **Build the root cause chain.** Start from the symptom (high client P99) and trace backward through the evidence:
   - High client P99 <- queued reads on node X
   - Queued reads <- cache eviction pressure
   - Cache evictions <- memory pressure / compaction backlog
   - etc.

4. **Document the timeline.** List all significant events with timestamps on the offending node.

5. **List contributing factors.** Include infrastructure differences (region, AMI), code differences (SCM revision), and data distribution questions.

6. **Write recommendations.** Split into:
   - **Immediate investigation:** Specific things to check next (diff SCM revisions, check tablet distribution, re-run in same region)
   - **Long-term improvements:** Test methodology changes (warmup periods, per-node metric tracking, region pinning)

7. **Save as markdown.** Use the output format defined in SKILL.md. Include all data sources (Prometheus URLs, Argus commands used).

**Exit:** Root cause analysis document written and saved. All evidence referenced. Recommendations are actionable.

---

## Phase 7: Root Cause Deep Dive — Determine the Bottleneck Layer

**Entry:** Phase 6 complete. You have the symptoms documented but need to determine exactly *where* in the stack the latency originates (client vs server, which Scylla subsystem).

**Actions:**

1. **Compare Scylla-reported latency vs client-reported latency.** The `latency_results` in the SCT log contain both:
   - `Scylla P99_read - node-X` (from `rlatencyp99` Prometheus metric, coordinator processing time only)
   - `c-s P99` (from cassandra-stress HDR histograms, end-to-end including queue wait time)

   A large gap (e.g., Scylla reports 12ms but client sees 23,000ms) means the time is spent **waiting in a queue**, not in actual request processing.

2. **Check read admission control saturation.** Query active reads to see if the concurrency limit is hit:
   ```bash
   # query: max(scylla_database_active_reads) by (instance)
   ```
   If active reads hit the internal limit (typically ~100 per shard), all additional reads queue. Compare the offending node (hitting 93-94) vs healthy nodes (3-22).

3. **Check transport layer blocking.** Rule out CQL connection-level backpressure:
   ```bash
   # query: max(scylla_transport_requests_blocked_memory) by (instance)
   ```
   Zero means the transport layer is NOT the bottleneck — requests are accepted but queued internally.

4. **Check memory headroom.** Compare allocated vs total memory per shard:
   ```bash
   # query: scylla_memory_allocated_memory{shard="0"}
   # query: scylla_memory_total_memory{shard="0"}
   ```
   If allocated/total > 99%, there is no memory headroom. This contributes to admission control being more aggressive and evictions triggering under load.

5. **Check cache hit rate.** If hit rate is 100% but evictions are occurring, the working set fits in cache but memory churn from evict-and-recache holds read slots longer:
   ```bash
   # query: sum(rate(scylla_cache_row_hits[1m])) by (instance) / (sum(rate(scylla_cache_row_hits[1m])) by (instance) + sum(rate(scylla_cache_row_misses[1m])) by (instance))
   ```

6. **Check for uneven load distribution.** If only some nodes are saturated while others are idle, this indicates uneven tablet ownership or topology-aware routing sending disproportionate traffic to specific nodes. Compare queued reads and active reads across all nodes — healthy nodes should have similar values.

7. **Build the bottleneck layer table.** Summarize findings:

   | Layer | Problem? | Evidence |
   |-------|----------|----------|
   | Client (cassandra-stress) | No/Yes | Reports latency correctly / thread contention |
   | Network | No/Yes | No transport blocking / TCP retransmissions |
   | Scylla transport/CQL layer | No/Yes | 0 requests blocked / connection shedding |
   | **Scylla read admission control** | No/Yes | Queued reads count, active reads at limit |
   | Scylla cache/memory | No/Yes | Memory %, eviction rate, hit rate |
   | Disk IO | No/Yes | Cache hit rate, IO queue delays |

8. **Classify the root cause.** Common patterns:
   - **Admission control saturation + 99% memory + cache evictions + 100% hit rate:** Memory pressure causes reads to hold admission slots longer → cascading queue buildup. Server-side issue, transient if caused by specific instance/placement.
   - **Low active reads + high IO delays + low cache hit rate:** Disk IO bottleneck from cold cache or data larger than memory.
   - **Transport blocking + high connections:** Client overloading the CQL connection pool.
   - **Uneven queued reads (1-2 nodes high, others zero):** Tablet/token distribution imbalance or noisy-neighbor on specific EC2 instance.

**Exit:** Bottleneck layer identified and documented. The analysis explains not just *what* happened but *why* and at which layer the time was spent.
