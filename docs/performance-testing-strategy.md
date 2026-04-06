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

### Pillar 3: Performance Under Operations (Nemesis)

**Problem**: Some nemesis-latency tests exist (`test_latency_read_with_nemesis`, `test_latency_write_with_nemesis`, `test_latency_mixed_with_nemesis` in `performance_regression_test.py`) but measure impact coarsely -- no per-operation breakdown, no structured SLA per operation type.

**Framework**:
1. **Baseline Phase** -- steady-state load for 5-10 min, establish P99 baseline
2. **Disruption Phase** -- trigger specific operation, measure latency separately
3. **Recovery Phase** -- measure latency until it returns to baseline
4. **Report** -- impact_factor = P99_during_nemesis / P99_steady_state

**Nemesis SLA Matrix**:

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
> **Reference**: [ScyllaDB Rolling Upgrade Procedure](https://docs.scylladb.com/stable/upgrade/)

### Pillar 4: Upgrade Performance Tracking

**Problem**: Rolling upgrade tests exist (`perf-regression-latency-650gb-upgrade.yaml`, `LatteStressLatencyComparison` in `sdcm/argus_results.py`) but each run is standalone -- no version-over-version trend.

**Enhancement**: Track upgrade impact factors in Argus with version metadata (source_version, target_version). Compute degradation percentages automatically. Integrate with Pillar 1 for trend detection across releases.

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

### Pillar 6: i8g ARM64 Migration

Incorporates the existing plan at `docs/plans/i8g-performance-jobs-migration.md` (Phase 1 complete, Phases 2-8 pending).

Pillar 1 (statistical baselines) directly simplifies Phases 4-6 (recalibrate thresholds) -- hardware-partitioned baselines auto-establish new normals after N validation runs on i8g.

### Pillar 7: Comparison & Efficiency Scoring

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

**Comparison Output Matrix**:

| Vendor | Instance | Raw Max OPS | Efficiency Score | Cost/Hour | Value Score |
|--------|----------|-------------|-----------------|-----------|-------------|
| AWS | i4i.4xlarge | 450k | 85,000 | $4.05 | 20,987 |
| AWS | i8g.4xlarge | 480k | 92,000 | $3.80 | 24,210 |
| GCE | n2-highmem-8 | 380k | 60,000 | $3.20 | 18,750 |

> **Reference**: Kleinrock, L. (1979), "Power and Deterministic Rules of Thumb for Probabilistic Problems in Computer Communications"
> **Reference**: [TPC Pricing Specification](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc_pricing_v3.5.0.pdf)

## 5. Test Categories

### Category 1: Throughput Staircase (Existing, Enhanced)
The predefined steps methodology. Enhanced with system metrics and statistical baselines.
Tests: `test_read_gradual_increase_load`, `test_write_gradual_increase_load`, `test_mixed_gradual_increase_load`, `test_read_disk_only_gradual_increase_load`.

### Category 2: Latency Under Load (Existing)
Steady-state latency tests at fixed throughput.

### Category 3: Performance Under Operations (New Framework)
Nemesis operations during sustained workload with per-operation SLA validation.
Operations: rolling restart, decommission, node replace, rolling upgrade, tablet operations, major compaction.

### Category 4: Upgrade Performance (Existing, Enhanced)
Before/after comparison with version-over-version trend tracking.

### Category 5: Max Throughput Discovery (Existing)
Binary-search optimization in `MaximumPerformanceSearchTest` (`performance_search_max_throughput_test.py`). Results seed staircase step definitions.

### Category 6: Customer Workload Reproduction (New)
Template-driven reproduction. Details in qa-internal.

## 6. Comparison & Regression Detection Methodology

### Level 1: Single-Run Validation (Current)
Each run validated against `fixed_limit` thresholds. Retained for SLA-critical limits.

### Level 2: Statistical Baseline Comparison (New)
Compared against sliding window of N=10 runs on same hardware.
Regression: `observed > mean + 3 * stdev` for latency, `observed < mean - 2 * stdev` for throughput.
Non-parametric alternative: Wilcoxon rank-sum test with p < 0.05 and Cliff's Delta for effect size.

### Level 3: Trend Detection (New)
Monotonic degradation over configurable window.
Alert when: 5 consecutive runs each degrade >1%, OR linear regression slope exceeds 0.5%/run over 10-run window.

### Level 4: Efficiency Scoring
Kleinrock's Power adaptation as described in Pillar 7.

### Level 5: Cross-Version Comparison
Upgrade tests: percentage change in metrics. Flag regression when degradation exceeds sensitivity threshold from Level 2.

## 7. Stress Tool Guidelines

| Tool | Language | Best For | Limitations |
|------|----------|----------|-------------|
| cassandra-stress | Java | Complex profiles, legacy compatibility | Thread-per-client model, GC overhead, max ~32 threads/process |
| scylla-bench | Go | High throughput (>500k OPS), zero GC | Limited profile support, different output format |
| latte | Rust | Low-latency measurement, Rune scripting | Incompatible with c-s YAML profiles |
| YCSB | Java | Industry-standard benchmarks, per-op latencies | Limited workload customization |

**Decision Tree**:
- Need complex workload profiles? -> cassandra-stress
- Need >500k OPS from minimal hardware? -> scylla-bench
- Need precise latency with custom logic? -> latte
- Need industry-standard comparison? -> YCSB

> **Reference**: [cassandra-stress docs](https://docs.scylladb.com/manual/master/operating-scylla/admin-tools/cassandra-stress.html)
> **Reference**: [scylla-bench](https://github.com/scylladb/scylla-bench)
> **Reference**: [latte](https://github.com/pkolaczk/latte), [SCT PR #13574: Adding latte support](https://github.com/scylladb/scylla-cluster-tests/pull/13574/)
> **Reference**: [Comparing Stress Tools for Cassandra](https://thelastpickle.com/blog/2020/04/06/comparing-stress-tools.html)

## 8. Cloud Tuning Guidelines

| Parameter | AWS (e.g., i4i) | GCP (e.g., c2d/n2) | Azure (e.g., Lsv3) | Logic |
|-----------|-----------------|---------------------|---------------------|-------|
| Loader Threads | High (800-1200) | Moderate (600-800) | High (800+) | AWS ENA requires higher parallelism for PPS limits |
| Connection Count | High | Low/Moderate | High | GCP creates overhead on high connection counts |
| Disk Type | Instance Store (NVMe) | Local SSD (Scratch) | Local NVMe | Never use network storage for perf tests |
| Step Multiplier | 1.0x - 1.2x | 0.8x - 1.0x | 1.0x | Adjust based on relative CPU strength |

**Hardware Coefficient**: Do not manually edit YAML for every instance type. Derive new targets as: `New_Targets[] = Baseline_Targets[] * Coefficient`.

## 9. Pipeline & Scheduling

Current pipeline architecture in `vars/perfRegressionParallelPipelinebyRegion.groovy` is well-designed for extension. New test types integrate as entries in the `testRegionMatrix` array.

**Current Schedule** (from `perfRegressionParallelPipelinebyRegion.groovy`):
- Weekly (Sundays): `master-weekly` -- most comprehensive (predefined throughput steps on i8g tablets, microbenchmarks)
- Every 3 weeks: `master-3weeks` -- rolling upgrade + nemesis latency tests
- Monthly: `master-monthly` -- VNode tests, nemesis latency on vnodes

**Scheduling for New Tests**:
- Nemesis performance: 3-weekly cadence (label: `master-3weeks`)
- Upgrade tracking: tied to release cadence, triggered per-release
- Customer reproduction: manual trigger only
- Efficiency scoring: post-processing step after staircase runs
