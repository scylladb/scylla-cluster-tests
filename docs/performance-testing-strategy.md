# ScyllaDB Performance Regression Testing Strategy

## 1. Abstract

This document defines the principles, execution flow, comparison methodology, and strategic roadmap for SCT's performance regression testing infrastructure. It covers the predefined throughput steps methodology, metrics collection, regression detection, performance under operations, upgrade tracking, customer workload reproduction, and cross-cloud comparison.

The test suite is designed to validate strict latency SLAs at specific throughput plateaus, answering: "Can the system sustain X load with Y latency stability?"

## 2. Test Definition & Principles

### 2.1 The "Staircase" Load Philosophy

Unlike "ramp-up" tests that linearly increase load until failure, this test implements a Step-Function approach.

**Steady-State Validation**: The test forces the database to hold a specific load (e.g., 50% capacity) for a sustained period (15-30 minutes).

*Why*: Transient caches, memtables, and compaction strategies behave differently under sustained pressure than during a brief ramp-up. A system might handle 100k OPS for 2 minutes but fall behind on compaction and spike latency after 10 minutes.

**The "Predefined" Contract**: The throughput_targets defined in the YAML file act as a contract. "If Version 1.0 could handle 100k OPS, Version 1.1 must handle 100k OPS." Failing a step is a hard regression.

**Implementation**: `PerformanceRegressionPredefinedStepsTest` in `performance_regression_gradual_grow_throughput.py` iterates over `perf_gradual_throttle_steps` from YAML configs.

> **Reference**: [Maximizing ScyllaDB Performance -- "Proper warmup & duration"](https://docs.scylladb.com/manual/stable/operating-scylla/procedures/tips/benchmark-tips.html)
> **Reference**: [Best Practices for Benchmarking ScyllaDB](https://www.scylladb.com/2021/03/04/best-practices-for-benchmarking-scylla/)

### 2.2 Latency-First Architecture

High throughput is meaningless if latency violates the SLA. P99 latency is the primary metric.

**The Constraint**: Latency_P99 < Threshold (e.g., 10ms).

**The Check**: For every predefined step, the test parses the HDR histogram. If P99 exceeds the threshold, the test reports a failure immediately, even if the server successfully processed all requests.

> **Reference**: Gil Tene, ["How NOT to Measure Latency"](https://www.youtube.com/watch?v=lJ8ydIuPFeU) -- why "average latency" is meaningless and P99 under sustained load is the only valid responsiveness metric.
> **Reference**: [HdrHistogram](https://github.com/HdrHistogram/HdrHistogram) -- accurate percentile measurement, used in SCT via `sdcm/utils/hdrhistogram.py`.

### 2.3 Client Over-Provisioning & Configuration Strategy

To ensure valid results, the bottleneck must be the Database Under Test (DUT), not the Load Generators.

**The Golden Rule**: Loader CPU usage should never exceed 70%. Overloaded clients cause Coordinated Omission -- the client fails to record the latency spikes it causes.

**Resource Sizing**: Maintain a Loader-to-DB CPU core ratio of at least 1:2.

**Process vs. Thread Strategy (cassandra-stress)**: Keep threads per process moderate (50-150). To utilize large loader nodes, run multiple independent processes rather than increasing thread count.

> **Reference**: [On Coordinated Omission -- ScyllaDB](https://www.scylladb.com/2021/04/22/on-coordinated-omission/)
> **Reference**: [wrk2 by Gil Tene](https://github.com/giltene/wrk2) -- constant-throughput load generation avoiding coordinated omission

### 2.4 Consistency & Topology

**Consistency Level**: QUORUM (or LOCAL_QUORUM for multi-DC). Tests the coordination path, not just local disk speed.

**Replication**: Standard RF=3.

**VNodes / Tablets**: Both architectures are explicitly tested to validate dynamic load balancing, tablet splitting, and migration efficiency under load.

> **Reference**: [ScyllaDB Tablets Architecture](https://www.scylladb.com/tech-talk/tablets-a-new-sharding-technique-for-scylladb/)
> **Reference**: [Scaling Performance: Tablets vs vNodes](https://www.scylladb.com/2024/01/11/scylladb-tablets-at-scale/)

### 2.5 Guidelines: Selecting Steps & SLAs

**Baseline Discovery (The "Uncapped" Run)**: Before defining a regression suite for new hardware, find the absolute physical limit:
- Provision enough client power to generate 150% of estimated cluster capacity
- Run 4 parallel processes per loader machine with 50 threads each
- Run with `limit=0` (no throttling) for 10 minutes
- Average OPS = Max_OPS baseline

**The Standard Regression Ladder**:
- Step 1 (50% Load -- "Sanity"): Checks for OS/Network errors. Near-zero latency.
- Step 2 (75% Load -- "Production"): Typical peak hour. SLA matters most here.
- Step 3 (90% Load -- "Saturation"): Queues fill. Tests scheduler efficiency.
- Step 4 (105% Load -- "Overload"): Optional. Tests graceful degradation.

**Latency SLA Tiers**:
- Hard SLA (<5ms P99): Applied to 50% and 75% steps.
- Soft SLA (<20ms P99): Applied to 90% step -- physics dictates queuing delay; verify it stays linear, not exponential.

## 3. Test Execution Flow

### Phase 1: Environment & Tuning
- Deploy 3 DB nodes and sufficient loaders.
- Apply standard ScyllaDB perftuning (IRQ pinning, CPU governor, RAID setup).

### Phase 2: Warm-up (The "Cold Start" Mitigator)
- Run a 10-minute moderate load (30% of expected max), discarded from results.
- Goal: Populate OS page cache, ScyllaDB row cache, establish TCP connections, trigger JIT compilation.

### Phase 3: Population (Data Ingestion)
- Write the dataset.
- **Critical Wait**: After population, wait for `nodetool compactionstats` to reach 0. Running perf tests with pending compaction backlog invalidates the baseline.

### Phase 4: The Throughput Ladder (Core Loop)
For each step in throughput_targets:
1. Configure stress tool with target_ops throttle
2. Execute for duration (e.g., 15m)
3. Analyze: Did Actual OPS >= Target OPS? Did P99 Latency < SLA?
4. Result: Pass/Fail

### Phase 5: The Uncapped Finale (Optional)
- Run with `limit=0` and high thread count.
- Goal: Discover "Headroom". If predefined steps end at 200k but system hits 350k, the YAML needs updating.

## 4. Strategic Pillars

### Pillar 1: Statistical Baseline & Regression Detection

**Problem**: Current regression detection uses hardcoded thresholds (`fixed_limit` values in `configurations/performance/latency-decorator-error-thresholds-*.yaml`). No mechanism to detect gradual drift (e.g., P99 moving from 2ms to 5ms when threshold is 10ms).

**Approach**: Replace fixed thresholds with statistically-derived baselines.

**Regression Definition**: `observed_value > baseline_mean + k * baseline_stdev`
- k = 3 for P99 latency (matches 3-sigma rule: 0.27% false positive rate under normality)
- k = 2 for throughput
- N = 10 run sliding window
- Baselines partitioned by (test_name, step_name, instance_type, architecture)

**Hardware Adaptation**: When hardware changes (i4i to i8g), baselines start fresh for the new hardware key. Eliminates manual threshold recalibration.

**Trend Detection**: Alert on M consecutive degrading runs (>D% each) or monotonic slope over W-run window. Default: M=5, D=1%, W=10, S=0.5%/run.

**Retained Fixed Limits**: `fixed_limit` remains as an SLA backstop -- "P99 must never exceed 50ms regardless of baseline."

> **Reference**: [Netflix: Fixing Performance Regressions Before They Happen](https://netflixtechblog.com/fixing-performance-regressions-before-they-happen-eab2602b86fe)
> **Reference**: Trubiani et al., ["Performance Regression Testing: A Systematic Mapping"](https://cs.gssi.it/catia.trubiani/download/2025-IST-performance-regression-testing-mapping-study.pdf) -- Wilcoxon rank-sum test, Cliff's Delta effect size
> **Reference**: [Netflix: Sequential A/B Testing](https://netflixtechblog.com/sequential-a-b-testing-keeps-the-world-streaming-netflix-part-1-continuous-data-cba6c7ed49df)

**Suggested enhancements** (not started):
- Statistical baselines instead of hand-written fixed limits: [SCT-1633](https://scylladb.atlassian.net/browse/SCT-1633)
- Trend detection across runs: [SCT-1634](https://scylladb.atlassian.net/browse/SCT-1634)
- When a single A/B pair is enough and when runs need repeats: [SCT-1405](https://scylladb.atlassian.net/browse/SCT-1405)

### Pillar 2: Metrics Expansion

**Problem**: Only latency and throughput are collected. When a regression is detected, there's no data to attribute it (CPU-bound? memory pressure? disk I/O? compaction contention?).

**Approach**: Collect system-level metrics alongside latency/throughput at each step, following Brendan Gregg's USE (Utilization/Saturation/Errors) methodology.

**Metrics Catalog**:

#### Currently Collected

| Metric | Source | Collection Point | Purpose |
|--------|--------|-----------------|---------|
| P50-P99.999 latency | HDR histograms | `sdcm/utils/hdrhistogram.py` | Primary SLA metric |
| Throughput (ops/sec) | HDR histograms | `sdcm/utils/decorators.py` | Capacity metric |
| Scylla P99 read/write | Prometheus `scylla_storage_proxy_coordinator_*_latency_bucket` | `sdcm/utils/latency.py` | Server-side validation |
| Client P95/P99 | Prometheus `sct_cassandra_stress_*_gauge` | `sdcm/utils/latency.py` | Client-side latency |
| Reactor stall counts | SCT events `DatabaseLogEvent.REACTOR_STALLED` | `sdcm/utils/decorators.py` | Internal stall indicator |
| Grafana screenshots | Grafana HTTP API | `sdcm/utils/decorators.py` | Visual evidence |

#### Proposed Additions

| Metric | Prometheus Query | Purpose | Priority |
|--------|-----------------|---------|----------|
| CPU utilization (%) | `avg(rate(scylla_reactor_utilization{}[60s]))` | Is the cluster compute-saturated? | High |
| Memory allocated (bytes) | `scylla_memory_allocated_memory` | Is memory pressure contributing to latency? | High |
| Disk read IOPS | `rate(scylla_io_queue_total_operations{class="default",direction="read"}[60s])` | Is disk I/O bottlenecking reads? | High |
| Disk write IOPS | `rate(scylla_io_queue_total_operations{class="default",direction="write"}[60s])` | Is disk I/O bottlenecking writes? | High |
| Compaction throughput | `rate(scylla_compaction_bytes_written[60s])` | Is background compaction contending with queries? | Medium |
| Cache hit rate | `1 - (rate(scylla_cache_reads_with_misses[60s]) / rate(scylla_cache_reads[60s]))` | Is workload cache-friendly? | Medium |
| Network bytes in/out | `rate(node_network_receive_bytes_total[60s])` | Is network bandwidth a bottleneck? | Medium |
| Pending compactions | `scylla_compaction_manager_compactions_pending` | Are compactions keeping up? | Medium |
| Allocation failures | `scylla_memory_allocation_failed` | Is memory exhaustion causing issues? | Low |

> **Reference**: [ScyllaDB Metrics Reference](https://docs.scylladb.com/manual/stable/reference/metrics.html)
> **Reference**: [How the ScyllaDB Data Cache Works](https://www.scylladb.com/2018/07/26/how-scylla-data-cache-works/)
> **Reference**: Brendan Gregg, [USE Method](https://www.brendangregg.com/usemethod.html)

**Suggested enhancements**:
- Push per-step DB/loader CPU and I/O summaries into Argus: [SCT-1400](https://scylladb.atlassian.net/browse/SCT-1400)

### Pillar 3: Performance Under Operations (Nemesis)

**Problem**: Some nemesis-latency tests exist (`test_latency_read_with_nemesis`, `test_latency_write_with_nemesis`, `test_latency_mixed_with_nemesis` in `performance_regression_test.py`) and do collect per-operation latency. However, the SLA is not compared to a baseline automatically, and the throughput used during nemesis tests was originally aimed at 50% of maximum throughput but has never been recalibrated. There is no structured phase breakdown (baseline, disruption, recovery) and no per-operation SLA validation framework.

**Framework**:
1. **Baseline Phase** -- steady-state load for 5-10 min, establish P99 baseline
2. **Disruption Phase** -- trigger specific operation, measure latency separately
3. **Recovery Phase** -- measure latency until it returns to baseline
4. **Report** -- impact_factor = P99_during_nemesis / P99_steady_state

**Nemesis SLA Matrix** (starting guesses, not measured yet):

| Operation | Max P99 Impact Factor | Max Recovery Time | Notes |
|-----------|----------------------|-------------------|-------|
| Rolling restart | 2x | 5 min | Standard maintenance operation |
| Decommission | 3x | 10 min | Streaming-intensive |
| Node replace | 3x | 10 min | Streaming + bootstrap |
| Rolling upgrade | 2x | 5 min | Version change under load |
| Tablet split/merge | 1.5x | 2 min | Dynamic rebalancing |
| Major compaction | 2x | Duration + 2 min | Heavy disk I/O |
| Schema change | 1.5x | 1 min | Lightweight |

> **Reference**: [Principles of Chaos Engineering](https://principlesofchaos.org/)

**Suggested enhancements** (not started):
- Impact factor and recovery time per operation, sent to Argus: [SCT-1635](https://scylladb.atlassian.net/browse/SCT-1635)

### Pillar 4: Upgrade Performance Tracking

**Problem**: Rolling upgrade tests exist (`perf-regression-latency-650gb-upgrade.yaml`, `LatteStressLatencyComparison` in `sdcm/argus_results.py`). Version-to-version comparison and impact tracking is largely already happening in Argus. What's missing is automatic trend detection across releases and integration with statistical baselines.

**Enhancement**: Build on existing Argus version tracking to add automatic trend detection across releases. Integrate with Pillar 1 statistical baselines so that version-over-version degradation is flagged automatically rather than requiring manual inspection.

> **Reference**: [ScyllaDB Rolling Upgrade Procedure](https://docs.scylladb.com/stable/upgrade/)

**Suggested enhancements**:
- Release-to-release trend check: [SCT-1634](https://scylladb.atlassian.net/browse/SCT-1634) (not started)
- Results tables across releases in Argus: [ARGUS-96](https://scylladb.atlassian.net/browse/ARGUS-96)

### Pillar 5: Customer Workload Reproduction

**Purpose**: Enable QA engineers to translate customer-reported performance issues into reproducible SCT test configurations.

**In SCT repo**: Reference workload templates in `test-cases/performance/` for common patterns.

**In qa-internal repo**: Customer Performance Case Template, case-specific workload definitions, results. Never contains customer PII in public SCT repo.

**Environment Mapping**:

| Customer Attribute | SCT Parameter | Example |
|---|---|---|
| Node count / type | `n_db_nodes`, `instance_type_db` | 3x i3.4xlarge |
| Data size | `prepare_write_cmd` duration | 500GB |
| Workload pattern | `stress_cmd_*` templates | 70R/30W |
| Compaction strategy | `post_prepare_cql_cmds` | STCS / LCS / ICS |
| Replication | `replication_factor` | RF=3 |

> **Reference**: [How to Test and Benchmark Database Clusters -- ScyllaDB](https://www.scylladb.com/2020/11/04/how-to-benchmark-database-clusters/)

**Suggested enhancements**:
- First case, custom_s1 user-profile workload (longevity + perf): [SCT-1460](https://scylladb.atlassian.net/browse/SCT-1460)

### Pillar 6: Comparison & Efficiency Scoring

> **TODO**: never tried. Explained and tracked as an experiment in [SCT-1636](https://scylladb.atlassian.net/browse/SCT-1636).

**Efficiency Margin Score** (per step):

```
Step_Score = Target_Throughput_OPS / P99_Latency_ms
Total_Score = Sum(Step_Score_i)
```

This is an adaptation of Kleinrock's Power Metric (Power = Throughput / Delay), which identifies the optimal operating point of a system by penalizing throughput gains that come at the cost of disproportionate latency.

**Value Score** (cost-normalized):

```
Value_Score = Total_Score / Hourly_Cluster_Cost
```

Cluster Cost = (Node Price * Node Count) + (Loader Price * Loader Count). Include loader cost -- inefficient clouds require more loaders, increasing TCO.

A simpler alternative, to try first: once Pillar 7 finds the best throughput under the P99 SLA, divide it by the hourly cluster cost.

> **Reference**: Kleinrock, L. (1979), "Power and Deterministic Rules of Thumb for Probabilistic Problems in Computer Communications"
> **Reference**: [TPC Pricing Specification](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc_pricing_v3.5.0.pdf)

### Pillar 7: Max Throughput Discovery

**Problem**: The current implementation (`MaximumPerformanceSearchTest` in `performance_search_max_throughput_test.py`) was never proven useful.

**Approach**: Rebuild it on latte, which controls the rate precisely. Search for the highest throttled rate whose P99 stays under the SLA, and use the result to seed the staircase steps of Section 2.5.

**Suggested enhancements**:
- Rate search on latte for read, write, mixed and read_disk_only: [SCT-580](https://scylladb.atlassian.net/browse/SCT-580)

## 5. Regression Detection

**Today**: each run is checked on its own against the `fixed_limit` values of `configurations/performance/latency-decorator-error-thresholds-*.yaml`. Argus evaluates them server-side. See [performance/results-and-regression.md](performance/results-and-regression.md).

**Suggested** (none implemented):
- Compare against a baseline of the last runs on the same hardware: [SCT-1633](https://scylladb.atlassian.net/browse/SCT-1633)
- Detect trends across runs and across releases: [SCT-1634](https://scylladb.atlassian.net/browse/SCT-1634)
- Score efficiency per configuration: [SCT-1636](https://scylladb.atlassian.net/browse/SCT-1636)

## 6. Stress Tool Guidelines

**latte is the default tool for all new performance tests.** It is the most versatile option, offering precise latency measurement, Rune scripting for custom workloads, and Rust-based efficiency with no GC overhead.

Only use other tools when there is a specific reason:

| Tool | When to Use Instead of latte |
|------|------------------------------|
| cassandra-stress | Legacy profile compatibility (existing c-s YAML profiles that would be costly to rewrite) |
| scylla-bench | Go driver testing (when the goal is specifically to test the Go driver path) |

**Alternator**: use latte as well. [scylladb/latte](https://github.com/scylladb/latte) has an Alternator client. The Alternator perf test still runs YCSB today; moving it to latte is [SCT-534](https://scylladb.atlassian.net/browse/SCT-534).

> **Reference**: [scylladb/latte](https://github.com/scylladb/latte), [SCT PR #13574: Adding latte support](https://github.com/scylladb/scylla-cluster-tests/pull/13574/)
> **Reference**: [cassandra-stress docs](https://docs.scylladb.com/manual/master/operating-scylla/admin-tools/cassandra-stress.html)
> **Reference**: [scylla-bench](https://github.com/scylladb/scylla-bench)

## 7. Cloud Tuning (Suggestion)

Not proven. The idea is to derive each instance type's throughput steps, loader threads and connection count from one calibration run, instead of copying them from the previous type. Today the z4d steps file is a copy of the i8g one. Tracked as an experiment in [SCT-1637](https://scylladb.atlassian.net/browse/SCT-1637).

## 8. Pipeline & Scheduling

The perf jobs, their versions and their cadence are defined in [configurations/triggers/perf-regression.yaml](../configurations/triggers/perf-regression.yaml). [performance/pipelines-and-scheduling.md](performance/pipelines-and-scheduling.md) explains how a new test gets scheduled.
