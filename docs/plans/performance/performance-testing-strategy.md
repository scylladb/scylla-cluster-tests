---
status: draft
domain: performance
created: 2026-04-06
last_updated: 2026-04-06
owner: fruch
---

# Performance Testing Strategy Implementation Plan

## 1. Problem Statement

SCT's performance regression testing relies on hardcoded `fixed_limit` thresholds in YAML files (`configurations/performance/latency-decorator-error-thresholds-*.yaml`) to detect regressions. This approach has several measurable limitations:

- **No drift detection**: A P99 latency creeping from 2ms to 5ms goes unnoticed when the threshold is 10ms. Gradual regressions accumulate across releases until they cross the fixed limit.
- **Manual threshold recalibration**: Every hardware migration (e.g., i4i to i8g) requires manually adjusting dozens of threshold values across multiple YAML files. The ongoing i8g migration (`docs/plans/i8g-performance-jobs-migration.md`) highlights this cost.
- **No root-cause attribution**: When a regression is detected, only latency and throughput are recorded. There is no system-level data (CPU utilization, disk IOPS, compaction pressure, memory) to diagnose whether the regression is compute-bound, I/O-bound, or memory-bound.
- **No automatic SLA comparison for nemesis tests**: Nemesis-latency tests (`test_latency_*_with_nemesis` in `performance_regression_test.py`) collect per-operation latency, but the SLA is not compared to a baseline automatically. The throughput used was aimed at 50% of maximum but has never been recalibrated. There is no per-phase breakdown (baseline, disruption, recovery), making it impossible to distinguish slow recovery from high-impact disruption.
- **No automatic trend detection across releases**: Upgrade tests (`LatteStressLatencyComparison` in `sdcm/argus_results.py`) store results in Argus with version metadata, but there is no automatic trend detection or degradation alerting across releases.

## 2. Current State

### Performance Test Infrastructure

- **Predefined steps test**: `PerformanceRegressionPredefinedStepsTest` in `performance_regression_gradual_grow_throughput.py` reads `perf_gradual_throttle_steps` from YAML configs and runs load at each step.
- **Step configurations**: `configurations/performance/cassandra_stress_gradual_load_steps*.yaml` and `configurations/performance/latte-perf-gradual-steps-*.yaml` define per-step throughput targets.
- **Threshold configs**: `configurations/performance/latency-decorator-error-thresholds-*.yaml` define `fixed_limit` values for latency validation.
- **Latency collection**: `sdcm/utils/latency.py` collects Prometheus-based latency metrics; `sdcm/utils/hdrhistogram.py` parses HDR histogram files for percentile data.
- **Decorator-based validation**: `sdcm/utils/decorators.py` wraps test methods with latency validation, Grafana screenshots, and reactor stall counting.
- **Max throughput discovery**: `MaximumPerformanceSearchTest` in `performance_search_max_throughput_test.py` uses binary search to find peak throughput.
- **Nemesis latency tests**: `test_latency_read_with_nemesis`, `test_latency_write_with_nemesis`, `test_latency_mixed_with_nemesis` in `performance_regression_test.py` (lines 680-699).
- **Upgrade comparison**: `LatteStressLatencyComparison` in `sdcm/argus_results.py` (line 254) reports upgrade test results to Argus.
- **Pipeline scheduling**: `vars/perfRegressionParallelPipelinebyRegion.groovy` manages weekly, 3-weekly, and monthly perf test scheduling.
- **Argus integration**: `sdcm/argus_results.py` defines result table schemas for Argus reporting.

### What Needs to Change

1. Regression detection needs statistical baselines alongside (not replacing) fixed limits.
2. System-level metrics (CPU, disk, memory, compaction) must be collected at each performance step.
3. Nemesis performance tests need structured phase measurement (baseline/disruption/recovery).
4. Upgrade results in Argus need automatic trend detection and degradation alerting across releases (version data already exists).
5. Efficiency scoring needs implementation for cross-hardware and cross-cloud comparison.

## 3. Goals

1. **Statistical regression detection**: Detect latency regressions exceeding `mean + 3 * stdev` over a 10-run sliding window, partitioned by (test_name, step_name, instance_type, architecture).
2. **System metrics at each step**: Collect CPU utilization, memory, disk IOPS, compaction throughput, and cache hit rate from Prometheus at each performance step and store in Argus.
3. **Nemesis performance framework**: Measure P99 latency in three phases (baseline, disruption, recovery) for at least 4 operation types, with per-operation impact factor reporting.
4. **Upgrade trend tracking**: Store source_version and target_version metadata in Argus results, enabling version-over-version trend queries.
5. **Trend alerting**: Alert when 5 consecutive runs each show >1% degradation, or linear regression slope exceeds 0.5%/run over a 10-run window.
6. **i8g migration simplification**: Hardware-partitioned baselines auto-establish new normals for i8g, eliminating manual threshold edits for Phases 4-6 of the i8g migration plan.
7. **Efficiency scoring**: Compute Kleinrock's Power metric (throughput/latency) and cost-normalized value score for cross-hardware comparison.

## 4. Implementation Phases

### Phase 1: Statistical Baselines (3-4 PRs)

**Importance**: High -- foundation for all other phases

**Dependencies**: None

**Description**: Argus already stores historical performance results and has the data model for baseline tracking. This phase adds statistical comparison logic on top of existing Argus data, replacing sole reliance on `fixed_limit` with a hybrid approach: statistical detection for drift + fixed limits for SLA backstops.

**Deliverables**:
- PR 1.1: Statistical comparison layer on top of existing Argus historical data, with partition key (test_name, step_name, instance_type, architecture)
- PR 1.2: Sliding window computation (mean, stdev, N=10) with configurable k-factor
- PR 1.3: Integration into `sdcm/utils/decorators.py` validation path -- run statistical check alongside existing `fixed_limit` check
- PR 1.4: Configuration parameters in `sdcm/sct_config.py` for window size, k-factor, and enable/disable toggle

**Definition of Done**:
- [ ] Baseline data persists across runs in Argus or local storage
- [ ] Statistical regression is flagged when `observed > mean + k * stdev`
- [ ] Existing `fixed_limit` checks remain functional and unchanged
- [ ] Unit tests cover baseline computation, sliding window, and edge cases (< N runs)
- [ ] Integration test validates end-to-end flow with a Docker-based perf run

### Phase 2: System Metrics Collection (2-3 PRs, parallel with Phase 1)

**Importance**: High -- enables root-cause attribution

**Dependencies**: None (can proceed in parallel with Phase 1)

**Description**: Extend the performance decorator to query Prometheus for system-level metrics (CPU, memory, disk IOPS, compaction, cache) at the start and end of each step, and store snapshots alongside latency results.

**Related Jira Items**:
- SCT-188: "Add grow/shrink operations to performance test cases" (related to expanding perf coverage)
- QAINFRA-59: "Auto generate complete test job from customer inputs" (related to customer workload)
- SCT-24: "Create and keep separate docs for performance tests" (docs improvement)
- QAINFRA-38: "Recurring SCT Performance Testing on xcloud backend" (xcloud perf)
- Note: Historical Jira items requesting CPU/memory metrics collection predate the current Jira project -- the requirement has been a known gap.

**Deliverables**:
- PR 2.1: Prometheus query helper in `sdcm/utils/latency.py` or new `sdcm/utils/perf_metrics.py` for system metric collection
- PR 2.2: Integration into step decorator in `sdcm/utils/decorators.py` to collect metrics per step
- PR 2.3: Argus result table extension to store system metrics alongside latency/throughput

**Definition of Done**:
- [ ] CPU utilization, memory, disk IOPS (read/write), compaction throughput, and cache hit rate collected at each step
- [ ] Metrics stored in Argus alongside existing latency/throughput data
- [ ] Unit tests for Prometheus query construction and response parsing
- [ ] No regression in existing performance test execution time (< 5% overhead)

### Phase 3: Nemesis Performance Framework (3-4 PRs)

**Importance**: Medium -- new capability

**Dependencies**: Phase 2 (system metrics collection)

**Description**: Build a structured nemesis performance measurement framework that segments each nemesis test into baseline, disruption, and recovery phases with separate latency tracking.

**Deliverables**:
- PR 3.1: Phase-segmented latency collector (baseline/disruption/recovery time windows)
- PR 3.2: Impact factor computation (P99_during / P99_baseline) and recovery time measurement
- PR 3.3: Nemesis SLA matrix configuration and validation (per-operation thresholds)
- PR 3.4: At least 4 operation types implemented: rolling restart, decommission, node replace, major compaction

**Definition of Done**:
- [ ] Each nemesis perf test reports: baseline_P99, disruption_P99, recovery_P99, impact_factor, recovery_time
- [ ] Impact factor validated against configurable per-operation SLA thresholds
- [ ] Results stored in Argus with operation type metadata
- [ ] Unit tests for phase segmentation and impact factor computation

### Phase 4: Upgrade Performance Tracking (2 PRs)

**Importance**: Medium -- enhances existing capability

**Dependencies**: Phase 1 (statistical baselines)

**Description**: Argus already has version tables that store source and target version metadata for upgrade tests. This phase adds automatic trend detection on top of the existing version data, enabling version-over-version degradation alerting without manual inspection.

**Deliverables**:
- PR 4.1: Automatic degradation percentage computation using existing Argus version table data
- PR 4.2: Integration with Phase 1 statistical baselines for version-over-version trend detection and alerting

**Definition of Done**:
- [ ] Degradation percentage computed automatically from existing Argus version tables
- [ ] Trend detection alerts when version-over-version degradation exceeds threshold
- [ ] Statistical baseline comparison works for upgrade results
- [ ] Unit tests for degradation computation and trend detection

### Phase 5: i8g Migration (Ongoing)

**Importance**: High -- active migration

**Dependencies**: Phase 1 simplifies Phases 4-6 of the existing i8g plan

**Description**: Continue the i8g migration per `docs/plans/i8g-performance-jobs-migration.md`. Phase 1 of this plan (statistical baselines) eliminates the need for manual threshold recalibration -- new baselines auto-establish after N validation runs on i8g hardware.

**Deliverables**:
- Integration of statistical baselines into i8g validation runs
- Hardware-partitioned baselines with `architecture=arm64` partition key

**Definition of Done**:
- [ ] i8g validation runs populate statistical baselines automatically
- [ ] No manual `fixed_limit` edits required for i8g threshold calibration
- [ ] Existing i4i baselines remain unaffected (partitioned by instance_type)

### Phase 6: Trend Detection & Efficiency Scoring (2-3 PRs)

**Importance**: Medium -- depends on baseline data accumulation

**Dependencies**: Phase 1 (statistical baselines)

**Description**: Implement trend detection alerts and Kleinrock's Power-based efficiency scoring for cross-hardware comparison.

**Deliverables**:

- PR 6.1: Trend detection algorithms
  - **Consecutive degradation detection**: Flag when M consecutive runs (default M=5) each show >D% degradation (default D=1%) compared to the previous run
  - **Linear regression over sliding window**: Compute slope over N-run window (default N=10); alert when slope exceeds S%/run (default S=0.5%)
  - **Implementation**: Use `numpy.polyfit` (degree 1) for slope computation on the existing Argus result time series
  - **Integration points**: Query Argus result tables for historical data; results partitioned by (test_name, step_name, instance_type, architecture)

- PR 6.2: Alerting integration
  - **Mechanism**: Implement as Argus validation rules that run post-result-upload
  - **Notification**: Generate SCT events (`PerformanceTrendAlert`) that flow through existing event pipeline to email reports and Argus UI
  - **Configuration**: Per-test overrides for M, D, N, S thresholds via `sct_config.py` parameters

- PR 6.3: Efficiency scoring and cross-hardware comparison
  - **Efficiency score**: `Step_Score = Target_Throughput_OPS / P99_Latency_ms`, `Total_Score = Sum(Step_Score_i)` (Kleinrock's Power metric adaptation)
  - **Value score**: `Value_Score = Total_Score / Hourly_Cluster_Cost` where cost includes both DB and loader nodes
  - **Cross-hardware comparison**: Generate comparison matrices across instance types and clouds using the same workload definition; normalize by cost for fair comparison
  - **Cross-hardware methodology**: Same test case YAML, same data size, same step definitions; only hardware and cloud vary. Results compared via efficiency and value scores, not raw throughput (which is expected to differ).

**Definition of Done**:
- [ ] Consecutive degradation detection flags M=5 consecutive >1% degrading runs
- [ ] Linear regression slope detection alerts when slope exceeds 0.5%/run over 10 runs
- [ ] Trend detection integrates with Argus result tables as data source
- [ ] Alerting produces SCT events visible in test reports and Argus UI
- [ ] Efficiency score and value score computed and stored in Argus
- [ ] Unit tests for trend detection algorithms (linear regression, consecutive detection)
- [ ] Unit tests for efficiency and value score computation
- [ ] Comparison report produces human-readable output matrix for at least 2 hardware configurations

## 5. Testing Requirements

### Unit Tests

| Phase | Test Focus | Location |
|-------|-----------|----------|
| 1 | Baseline computation, sliding window, k-factor thresholds, edge cases (N < window_size) | `unit_tests/test_perf_baselines.py` |
| 2 | Prometheus query construction, metric parsing, snapshot storage | `unit_tests/test_perf_metrics.py` |
| 3 | Phase segmentation, impact factor computation, SLA validation | `unit_tests/test_nemesis_perf.py` |
| 4 | Version metadata handling, degradation percentage | `unit_tests/test_upgrade_tracking.py` |
| 6 | Trend detection algorithms, efficiency scoring, value score | `unit_tests/test_trend_detection.py` |

### Integration Tests

- Phase 1: End-to-end baseline flow with Docker backend perf run
- Phase 2: Prometheus metric collection during Docker perf run
- Phase 3: Nemesis phase measurement with Docker backend

### Manual Testing

- Phase 1: Validate statistical regression detection against known regression (inject artificial latency increase)
- Phase 5: Run i8g staircase test and verify baselines populate correctly
- Phase 6: Verify comparison report output with real multi-hardware data

## 6. Success Criteria

- Statistical baselines detect a 15% latency regression within 3 runs (not waiting for fixed_limit breach)
- System metrics are available in Argus for every performance step, enabling root-cause triage without re-running tests
- Nemesis performance tests report per-phase metrics (baseline, disruption, recovery) for at least 4 operation types
- i8g threshold calibration requires zero manual YAML edits (auto-established by statistical baselines)
- Trend detection fires an alert on synthetic 5-run degradation sequence in unit tests
- Efficiency scoring produces valid comparison matrix for at least 2 hardware configurations

## 7. Risk Mitigation

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Insufficient historical data for statistical baselines (< N runs) | High (initial) | Medium | Fall back to `fixed_limit` when baseline window has fewer than N runs; gradually transition as data accumulates |
| Statistical false positives from noisy environments | Medium | High | Use k=3 (3-sigma) for conservative detection; add Wilcoxon rank-sum as non-parametric alternative; allow per-test k-factor override |
| Prometheus query overhead slows test execution | Low | Medium | Batch queries per step (single request with multiple metrics); measure overhead and enforce < 5% budget |
| Argus schema changes break existing result consumers | Medium | High | Add new fields as optional columns; maintain backward compatibility; version the result table schema |
| Nemesis phase boundaries imprecise (disruption start/end timing) | Medium | Medium | Use SCT event timestamps for precise phase boundaries; add configurable tolerance window |
| i8g baseline establishment takes too long (10 runs at weekly cadence = 10 weeks) | High | Medium | Run initial baseline establishment at higher frequency (daily for 2 weeks); accept wider confidence interval with N=5 initially |
