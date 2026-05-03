---
name: investigating-perf-regression
description: >-
  Guides investigation of SCT performance regression test failures by comparing
  failed and passed runs. Use when a perf-regression test fails with high P99
  latency, when comparing two SCT performance test runs, when querying Prometheus
  metrics from SCT monitoring stacks, when downloading and analyzing Scylla logs
  via Argus CLI, or when diagnosing read queuing, cache eviction, reactor stalls,
  or compaction-related latency spikes. Covers predefined-throughput-steps tests,
  gradual-grow-throughput tests, and HDR histogram analysis.
---

# Investigating Performance Regression Failures

Systematically compare failed and passed SCT performance test runs to identify root causes of latency regressions.

## Essential Principles

### Compare, Don't Assume

**Always compare the failed run against a known-good passed run.**

A single metric in isolation is meaningless without a baseline. A max queued reads count of 29 looks bad until you see the passed run had 16. Every query must be run against both runs to establish what's normal vs anomalous. Without comparison, you risk chasing red herrings.

### Server-Side vs Client-Side Latency

**Distinguish between Scylla-reported latency and cassandra-stress-reported latency.**

The P99 reported by the test result (from HDR histograms / cassandra-stress) includes network round-trip, client-side queuing, and coordinator overhead. The Scylla-side P99 (from `rlatencyp99` Prometheus metric) only measures server processing time. A huge gap between them (e.g., 57ms server vs 1754ms client) points to read queuing, network issues, or client-side bottlenecks rather than raw Scylla performance.

### Follow the Queuing Chain

**Latency regressions are almost always caused by queuing, not raw slowness.**

The investigation order should follow the queuing chain: `scylla_database_queued_reads` -> `scylla_cache_row_evictions` -> `scylla_lsa_memory_compacted` -> `scylla_reactor_utilization` -> `scylla_io_queue_*`. A node with 1600 queued reads and 79% reactor utilization tells a different story than one with 0 queued reads and 99% utilization. Start from queued reads and work backward to the cause.

### Identify the Offending Node

**Performance regressions are usually caused by a single node, not the whole cluster.**

Always break down metrics by instance. A cluster-wide average hides single-node spikes. Query with `by (instance)` and look for outliers. Map node IPs to node names using `scylla.yaml` from the downloaded logs to make findings actionable.

### Correlate Timestamps

**Every spike must be correlated with a specific time window.**

Don't just report "max queued reads was 1601." Report "max queued reads was 1601 at 05:57:48 on node-3." Then check what else happened at that exact time: cache evictions, compaction activity, reactor stalls, memtable flushes. The HDR histogram intervals from cassandra-stress provide natural time boundaries for correlation.

## When to Use

- A perf-regression test (predefined-throughput-steps, gradual-grow-throughput) fails with high P99 latency
- Comparing metrics between two SCT performance test runs (failed vs passed)
- Investigating latency spikes during a specific load step (e.g., 1200000 op/s)
- Diagnosing whether a regression is caused by Scylla (server-side) or infrastructure (network, disk, client)
- Analyzing HDR histogram data from cassandra-stress results
- Querying Prometheus metrics from SCT monitoring stacks that are still running

## When NOT to Use

- Investigating functional test failures (non-performance) -- use standard SCT debugging
- Writing or modifying performance test code -- use the codebase directly
- Profiling SCT framework code itself -- use the profiling-sct-code skill
- Analyzing tests that don't use Prometheus monitoring (e.g., unit tests)
- Infrastructure provisioning issues (instance launch failures, network setup) -- these are not perf regressions

## Investigation Workflow Overview

The full step-by-step process is in [workflows/investigate-perf-regression.md](workflows/investigate-perf-regression.md).

**Phase summary:**

1. **Gather run metadata** -- Use Argus CLI to get run details, events, and identify both runs
2. **Query Prometheus metrics** -- Compare key metrics between failed and passed runs
3. **Identify the offending node** -- Break down metrics by instance, find outliers
4. **Correlate with HDR histograms** -- Map Prometheus spikes to cassandra-stress intervals
5. **Download and analyze logs** -- Use Argus to get system.log, check for stalls, errors
6. **Write root cause analysis** -- Document findings in structured markdown
7. **Root cause deep dive** -- Determine the exact bottleneck layer (admission control, memory, IO, transport)

## Key Metrics Priority Order

Query these metrics in order. Each level explains a different layer of the problem:

| Priority | Metric | What It Reveals |
|----------|--------|-----------------|
| 1 | `rlatencyp99` (per instance) | Server-side tail latency -- is Scylla itself slow? |
| 2 | `scylla_database_queued_reads` | Read admission control -- are reads being throttled? |
| 3 | `scylla_database_active_reads` | Admission saturation -- is the concurrency limit hit? |
| 4 | `scylla_cache_row_evictions` | Memory pressure -- is the cache being evicted? |
| 5 | `scylla_memory_allocated_memory` | Memory headroom -- is the node at 99%+ usage? |
| 6 | `scylla_reactor_utilization` | CPU saturation -- is the reactor overloaded? |
| 7 | `scylla_transport_requests_blocked_memory` | Transport layer -- is CQL backpressure the bottleneck? |
| 8 | `scylla_lsa_memory_compacted` | LSA pressure -- is memory compaction active? |
| 9 | `scylla_io_queue_total_delay_sec` | I/O delays -- is disk I/O the bottleneck? |
| 10 | `node_netstat_Tcp_RetransSegs` | Network issues -- are packets being retransmitted? |

See [references/prometheus-queries.md](references/prometheus-queries.md) for exact query syntax.

## Argus CLI Quick Reference

See [references/argus-commands.md](references/argus-commands.md) for the full command reference.

**Essential commands:**

```bash
# Get run metadata
argus run get --run-id <UUID>

# Get error/critical events
argus run events --run-id <UUID>

# List available logs
argus run logs list --run-id <UUID>

# Download and extract logs
argus run logs download <log-name> --run-id <UUID> --dest <directory>
```

## Node IP Mapping

After downloading DB cluster logs, map node numbers to IPs:

```bash
for n in 1 2 3; do
  echo "=== Node $n ==="
  grep "broadcast_rpc_address" <logs-dir>/db-node-*-$n/scylla.yaml
done
```

This is critical because Prometheus metrics use IPs while Argus logs use node names.

## HDR Histogram Interpretation

The SCT log contains HDR histogram data split into 10-minute intervals. Key fields:

- `percentile_50/90/95/99` -- Latency percentiles in milliseconds
- `throughput` -- Actual ops/s achieved in that interval
- `stddev` -- Standard deviation (high values indicate instability)
- `color` -- SCT's own regression detection (`red` = regression)

When the first interval shows extreme latency but later intervals are normal, the cause is typically startup transient (compaction catch-up, cache warming).

## Common Root Cause Patterns

### Pattern 1: Read Admission Control Saturation

**Symptoms:** Client P99 >> Scylla P99 (e.g., 23s vs 12ms), queued reads in hundreds/thousands, active reads at limit (~93-94).

**Mechanism:** Memory is at 99% capacity → cache evictions occur under load → some reads hold admission slots longer due to memory allocation pressure → queue builds up → new requests wait 10-20+ seconds in the admission queue before being processed (in 12ms).

**Key insight:** Scylla's coordinator latency metric only measures processing time, NOT queue wait time. The gap between server and client P99 IS the queue wait time.

**Distinguishing evidence:**
- `scylla_transport_requests_blocked_memory` = 0 (transport is fine)
- `scylla_database_active_reads` at limit on affected nodes
- `scylla_memory_allocated_memory / scylla_memory_total_memory` > 99%
- Cache hit rate = 100% (not a cold cache problem)

### Pattern 2: Uneven Node Load

**Symptoms:** 1-2 nodes show massive queued reads while others are near zero. All nodes have similar reactor utilization.

**Cause:** Uneven tablet distribution, token-aware routing concentrating load, or specific EC2 instance with degraded performance (noisy neighbor).

**Evidence:** Compare queued reads per node — healthy cluster shows uniform distribution.

### Pattern 3: Compaction Interference

**Symptoms:** Periodic latency spikes, reactor stalls in `compaction` scheduling group, elevated IO queue delays.

**Cause:** Background compaction consuming IO bandwidth or CPU, competing with read path.

**Evidence:** `scylla_compaction_manager_compactions > 0` during the step, reactor stalls timestamped during spikes.

### Pattern 4: Disk IO Bottleneck

**Symptoms:** Low cache hit rate, high IO queue delays, high disk queue length.

**Cause:** Working set doesn't fit in memory, reads go to disk, NVMe performance degraded.

**Evidence:** `scylla_cache_row_hits / (hits + misses) < 1.0`, `scylla_io_queue_total_delay_sec` rate > 0.

## Output Format

The final analysis should be a markdown document with:

1. **Header** -- Test name, step, run IDs, Jenkins links
2. **Run environment comparison** -- Region, instance type, Scylla version, AMI, SCM revision
3. **Key findings table** -- Side-by-side metrics comparison
4. **Timeline** -- Timestamped events on the offending node
5. **Root cause chain** -- Sequence of events leading to the failure
6. **HDR histogram breakdown** -- Per-interval latency data
7. **Log analysis** -- Reactor stalls, kernel messages, system events
8. **Contributing factors** -- Infrastructure, code, data distribution differences
9. **Recommendations** -- Immediate investigation steps and long-term improvements

## Reference Index

| File | Content |
|------|---------|
| [references/prometheus-queries.md](references/prometheus-queries.md) | Exact Prometheus HTTP API queries for all key metrics |
| [references/argus-commands.md](references/argus-commands.md) | Argus CLI commands for run inspection and log retrieval |

| Workflow | Purpose |
|----------|---------|
| [workflows/investigate-perf-regression.md](workflows/investigate-perf-regression.md) | 7-phase investigation process from run IDs to root cause analysis |

## Trigger Eval Queries

**Should trigger:**
- "Compare these two perf regression runs"
- "Why did the predefined throughput steps test fail?"
- "Investigate high P99 latency in run 5a958af9"
- "Check Prometheus metrics for the failed performance test"
- "Download logs from Argus for run X and analyze them"
- "What caused the latency spike at the 1200000 op/s step?"

**Should NOT trigger:**
- "Write a new performance test" (use codebase directly)
- "Profile the SCT framework code" (use profiling-sct-code)
- "Fix a unit test failure" (use writing-unit-tests)
- "Review this PR" (use code-review)

## Success Criteria

- [ ] Both failed and passed runs are compared side-by-side
- [ ] Metrics are broken down per-node to identify the offender
- [ ] Prometheus spikes are correlated with HDR histogram intervals
- [ ] System logs are checked for reactor stalls and errors
- [ ] Root cause chain is documented (not just symptoms)
- [ ] Recommendations include both immediate and long-term actions
- [ ] Analysis is saved as structured markdown
